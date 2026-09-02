"""bridge 状态机 + SQLite outbox 测试（用 fake_hermes + 内存桥）。"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from src.adapter import HermesAdapter, HermesResult
from src.bridge import (
    Bridge,
    BridgeConfig,
    STATE_ACKED,
    STATE_EXECUTING,
    STATE_FAILED,
    STATE_RESULT_PENDING,
)
from src.protocol import LivisConfig

FAKE = str(Path(__file__).resolve().parents[1] / "scripts" / "fake_hermes.sh")


class FakeAdapter:
    """可控 adapter：手动设定返回结果，不真跑子进程。"""

    def __init__(self):
        self.result = HermesResult(ok=True, text="结果", exit_code=0, duration=0.5)
        self.calls: list[str] = []
        self.cancelled_jobs: list[str] = []

    async def run(self, content: str) -> HermesResult:
        self.calls.append(content)
        if "CANCEL" in content:
            self.cancelled_jobs.append(content)
        return self.result


def make_bridge(tmp_path, adapter=None) -> Bridge:
    cfg = BridgeConfig(
        hermes_bin=FAKE,
        db_path=tmp_path / "bridge.db",
    )
    livis_cfg = LivisConfig(data_dir=tmp_path)
    return Bridge(cfg, livis_cfg, adapter or FakeAdapter(), db_path=tmp_path / "bridge.db")


def exec_msg(content: str, job_id: str = "job-1") -> dict:
    return {
        "type": "exec",
        "metadata": {"job_id": job_id, "msg_id": "m-1", "agent_id": "a", "device_id": "d"},
        "payload": {"content": content},
    }


def ack_msg(job_id: str) -> dict:
    return {
        "type": "ack_send_result",
        "metadata": {"job_id": job_id, "ref_job_id": job_id, "msg_id": "m-2", "agent_id": "a", "device_id": "d"},
        "payload": {},
    }


@pytest.mark.asyncio
async def test_exec_to_acked_full_cycle(tmp_path):
    bridge = make_bridge(tmp_path)
    await bridge.start()
    try:
        await bridge.on_message(exec_msg("任务A", "job-1"))
        # 等 worker 执行
        for _ in range(20):
            if bridge._job("job-1") and bridge._job("job-1")["state"] == STATE_RESULT_PENDING:
                break
            await asyncio.sleep(0.05)
        job = bridge._job("job-1")
        assert job["state"] == STATE_RESULT_PENDING
        assert job["result"] == "结果"
        assert bridge._pending_results(), "outbox 应有待发结果"

        # 模拟服务端 ack
        await bridge.on_message(ack_msg("job-1"))
        assert bridge._job("job-1")["state"] == STATE_ACKED
        assert not bridge._pending_results()
    finally:
        await bridge.stop()


@pytest.mark.asyncio
async def test_serial_execution(tmp_path):
    """并发 exec 必须串行执行（同一时刻一个 job）。"""
    adapter = FakeAdapter()
    bridge = make_bridge(tmp_path, adapter)
    await bridge.start()
    try:
        await bridge.on_message(exec_msg("A", "job-a"))
        await bridge.on_message(exec_msg("B", "job-b"))
        for _ in range(30):
            done = [bridge._job(j)["state"] for j in ("job-a", "job-b") if bridge._job(j)]
            if done and all(s == STATE_RESULT_PENDING for s in done):
                break
            await asyncio.sleep(0.05)
        assert adapter.calls == ["A", "B"], f"必须串行: {adapter.calls}"
    finally:
        await bridge.stop()


@pytest.mark.asyncio
async def test_cancel_before_start(tmp_path):
    adapter = FakeAdapter()
    bridge = make_bridge(tmp_path, adapter)
    await bridge.start()
    try:
        await bridge.on_message(exec_msg("CANCEL任务", "job-x"))
        await bridge.on_message({"type": "cancel_chat", "metadata": {"job_id": "job-x"}, "payload": {}})
        for _ in range(20):
            if bridge._job("job-x") and bridge._job("job-x")["state"] in (STATE_FAILED, STATE_RESULT_PENDING):
                break
            await asyncio.sleep(0.05)
        job = bridge._job("job-x")
        assert job["state"] == STATE_FAILED
        assert "取消" in (job["error"] or "") or "cancelled" in (job["error"] or "")
    finally:
        await bridge.stop()


@pytest.mark.asyncio
async def test_failed_job_marks_state(tmp_path):
    adapter = FakeAdapter()
    adapter.result = HermesResult(ok=False, text="", exit_code=1, duration=0.1, error="boom")
    bridge = make_bridge(tmp_path, adapter)
    await bridge.start()
    try:
        await bridge.on_message(exec_msg("会失败", "job-f"))
        for _ in range(20):
            if bridge._job("job-f") and bridge._job("job-f")["state"] == STATE_FAILED:
                break
            await asyncio.sleep(0.05)
        job = bridge._job("job-f")
        assert job["state"] == STATE_FAILED
        assert job["error"] == "boom"
    finally:
        await bridge.stop()


@pytest.mark.asyncio
async def test_unknown_message_warns(tmp_path):
    bridge = make_bridge(tmp_path)
    # 未知类型不应崩
    await bridge.on_message({"type": "future_unknown_type", "metadata": {}, "payload": {}})
    assert bridge._pending_results() == []


@pytest.mark.asyncio
async def test_outbox_persists_across_restart(tmp_path):
    """result_pending 结果在重启后仍保留（断线不丢）。"""
    bridge1 = make_bridge(tmp_path)
    await bridge1.start()
    await bridge1.on_message(exec_msg("持久化", "job-p"))
    for _ in range(20):
        if bridge1._job("job-p") and bridge1._job("job-p")["state"] == STATE_RESULT_PENDING:
            break
        await asyncio.sleep(0.05)
    await bridge1.stop()

    # 重启（模拟崩溃/断线）
    bridge2 = make_bridge(tmp_path)
    pending = bridge2._pending_results()
    assert len(pending) == 1
    assert pending[0]["job_id"] == "job-p"
    assert "结果" in (pending[0]["result"] or "")
    await bridge2.stop()
