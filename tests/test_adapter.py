"""adapter 层测试：fake_hermes 子进程驱动。"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.adapter import HermesAdapter, HermesResult

FAKE = str(Path(__file__).resolve().parents[1] / "scripts" / "fake_hermes.sh")


@pytest.mark.asyncio
async def test_fake_ok(tmp_path):
    adapter = HermesAdapter(bin=FAKE, timeout=30)
    result = await adapter.run("你好")
    assert result.ok
    assert "FAKE-HERMES-OK" in result.text
    assert result.exit_code == 0
    assert result.usage and result.usage["total_tokens"] == 120


@pytest.mark.asyncio
async def test_fake_fail(tmp_path):
    adapter = HermesAdapter(bin=FAKE, timeout=30)
    result = await adapter.run("这个任务会 FAIL")
    assert not result.ok
    assert result.exit_code == 42
    assert "simulated failure" in result.error


@pytest.mark.asyncio
async def test_fake_slow_respects_timeout(tmp_path):
    adapter = HermesAdapter(bin=FAKE, timeout=1)
    result = await adapter.run("SLOW 任务")
    assert not result.ok
    assert "超时" in result.error


@pytest.mark.asyncio
async def test_as_send_result():
    ok = HermesResult(ok=True, text="  结果文本  \n", exit_code=0, duration=1.0)
    assert ok.as_send_result == "结果文本"
    bad = HermesResult(ok=False, text="", exit_code=42, duration=1.0, error="boom")
    assert "Hermes 执行失败" in bad.as_send_result
