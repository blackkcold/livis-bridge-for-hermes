"""Bridge 核心：Livis WS ↔ Hermes adapter 之间的状态机编排。

职责:
  - 接收 exec / send_message / cancel_chat
  - 串行执行 Hermes（同一时刻一个 job）
  - SQLite outbox: 结果落库 -> 发送 -> ack 后清账（断线不丢）
  - 重连对账: 恢复后重发未 ack 的结果
  - 协议变更检测: 未知消息类型/格式异常 -> 告警日志

状态机 (每 job):
  executing -> result_pending -> acked
  任一环节异常 -> failed（记录原因，不回传）
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .protocol import (
    MSG_ACK_SEND_RESULT,
    MSG_ACK_SEND_MESSAGE,
    MSG_CANCEL_CHAT,
    MSG_EXEC,
    MSG_SEND_MESSAGE,
    MSG_SEND_RESULT,
    LivisConfig,
    LivisWSClient,
    exec_content,
)

log = logging.getLogger("livis.bridge")

STATE_EXECUTING = "executing"
STATE_RESULT_PENDING = "result_pending"
STATE_ACKED = "acked"
STATE_FAILED = "failed"

# 已知消息类型（收到时不告警；未知类型=协议变更信号）
KNOWN_TYPES = frozenset({
    MSG_ACK_SEND_RESULT, MSG_CANCEL_CHAT, MSG_EXEC, MSG_SEND_MESSAGE,
    "connect", "connected", "heartbeat", "token_expiring", "token_refreshed",
    "token_refresh", "ack_send_message", "ack_cancel_chat", "pong",
})


@dataclass
class BridgeConfig:
    hermes_bin: str = os.environ.get("HERMES_BIN", "hermes")
    hermes_timeout: float = 600.0
    max_jobs_in_parallel: int = 1
    protocol_warn_on_unknown: bool = True
    db_path: Path = field(
        default_factory=lambda: Path(
            os.environ.get("LIVIS_DATA_DIR", "./data")
        ) / "bridge.db"
    )

    @classmethod
    def from_env(cls) -> "BridgeConfig":
        return cls(
            hermes_bin=os.environ.get("HERMES_BIN", "hermes"),
            hermes_timeout=float(os.environ.get("HERMES_TIMEOUT", "600")),
            db_path=Path(os.environ.get("LIVIS_DATA_DIR", "./data")) / "bridge.db",
        )


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            state TEXT NOT NULL,
            result TEXT,
            error TEXT,
            raw_msg TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)"
    )
    conn.commit()
    return conn


class Bridge:
    def __init__(
        self,
        cfg: BridgeConfig,
        livis_cfg: LivisConfig,
        adapter,
        db_path: Path | None = None,
    ):
        self.cfg = cfg
        self.livis_cfg = livis_cfg
        self.adapter = adapter
        self._conn = _connect(db_path or cfg.db_path)
        # 串行任务队列（同时一个 job）
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._worker: asyncio.Task | None = None
        self._active_job: str | None = None
        self._cancel_requested: set[str] = set()
        # WS client（server 模式下由外部注入，用于发送结果）
        self._client: LivisWSClient | None = None

    def set_client(self, client: LivisWSClient | None) -> None:
        """注入/清除 WS client 引用（server 模式）。"""
        self._client = client

    # ── 数据库 ──────────────────────────────────────────────────────

    def _insert_job(self, job_id: str, content: str, raw_msg: dict) -> None:
        now = time.time()
        self._conn.execute(
            "INSERT OR IGNORE INTO jobs (job_id, content, state, raw_msg, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (job_id, content, STATE_EXECUTING, json.dumps(raw_msg, ensure_ascii=False), now, now),
        )
        self._conn.commit()

    def _set_state(self, job_id: str, state: str, result: str | None = None, error: str | None = None) -> None:
        self._conn.execute(
            "UPDATE jobs SET state=?, result=?, error=?, updated_at=? WHERE job_id=?",
            (state, result, error, time.time(), job_id),
        )
        self._conn.commit()

    def _pending_results(self) -> list[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM jobs WHERE state=? ORDER BY created_at", (STATE_RESULT_PENDING,)
        ).fetchall()

    def _job(self, job_id: str) -> sqlite3.Row | None:
        return self._conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()

    # ── 消息处理（WS 回调）──────────────────────────────────────────

    async def _extract_inner(self, msg: dict[str, Any]) -> dict[str, Any]:
        """从 send_message.payload 提取内层消息。

        官方协议: payload.data 可能是对象或 JSON 字符串，
        内层含 type / content（exec / cancel_chat 都在内层）。
        """
        payload = msg.get("payload") or {}
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        if isinstance(data, str):
            try:
                inner = json.loads(data)
                return inner if isinstance(inner, dict) else {}
            except json.JSONDecodeError:
                log.warning("payload.data JSON 解析失败: %.200s", data)
                return {}
        return {}

    async def on_message(self, msg: dict[str, Any]) -> None:
        mtype = msg.get("type", "")
        meta = msg.get("metadata") or {}
        job_id = meta.get("job_id") or str(uuid.uuid4())

        if mtype == MSG_SEND_MESSAGE:
            # ✅ 立即回 ack_send_message（服务端靠它确认已送达）
            if self._client is not None:
                try:
                    await self._client.send_ack(MSG_ACK_SEND_MESSAGE, meta)
                    log.info("已回 ack_send_message job=%s", job_id)
                except Exception as e:
                    log.warning("回 ack_send_message 失败: %s", e)
            inner = await self._extract_inner(msg)
            itype = inner.get("type", "message")
            content = inner.get("content", "")
            if itype == "exec" and content:
                self._insert_job(job_id, content, msg)
                await self._queue.put(job_id)
                log.info("exec 入队 job=%s: %.60s", job_id, content)
            elif itype == "cancel_chat" or itype == "cancel":
                target = meta.get("job_id") or self._active_job
                if target:
                    self._cancel_requested.add(target)
                    log.info("取消请求 job=%s", target)
            else:
                log.info("send_message 非 exec 类型: %s (job=%s)", itype, job_id)
            return

        # 兼容：服务端可能直接发 exec 外层（与官方嵌套结构并存）
        if mtype == MSG_EXEC:
            try:
                content = exec_content(msg)
            except ValueError as e:
                log.error("exec 消息非法: %s", e)
                return
            self._insert_job(job_id, content, msg)
            await self._queue.put(job_id)
            log.info("exec(外层) 入队 job=%s: %.60s", job_id, content)
            return

        if mtype == MSG_CANCEL_CHAT:
            target = meta.get("job_id") or self._active_job
            if target:
                self._cancel_requested.add(target)
                log.info("取消请求 job=%s", target)
                # C1: 对齐官方协议——cancel 必须回 ack_cancel_chat
                if self._client is not None:
                    try:
                        await self._client.send_ack("ack_cancel_chat", meta)
                        log.info("已回 ack_cancel_chat job=%s", target)
                    except Exception as e:
                        log.warning("回 ack_cancel_chat 失败: %s", e)
            return

        if mtype == MSG_ACK_SEND_RESULT:
            self._handle_ack(msg)
            return

        if mtype in KNOWN_TYPES:
            return
        if self.cfg.protocol_warn_on_unknown:
            log.warning("协议未知消息: type=%s metadata=%s", mtype, json.dumps(meta, ensure_ascii=False)[:300])

    def _handle_ack(self, msg: dict[str, Any]) -> None:
        meta = msg.get("metadata") or {}
        ref = meta.get("ref_job_id") or meta.get("job_id") or meta.get("msg_id") or ""
        if not ref:
            log.warning("ack_send_result 无引用: %s", json.dumps(msg, ensure_ascii=False)[:200])
            return
        job = self._conn.execute(
            "SELECT * FROM jobs WHERE job_id=? OR raw_msg LIKE ?",
            (ref, f'%"job_id": "{ref}"%'),
        ).fetchone()
        if job:
            self._set_state(job["job_id"], STATE_ACKED)
            log.info("ack 达成 job=%s", job["job_id"])
        else:
            log.info("ack 无匹配 job=%s（可能已清）", ref)

    # ── 执行 worker（串行）─────────────────────────────────────────

    async def _worker_loop(self) -> None:
        while True:
            job_id = await self._queue.get()
            self._active_job = job_id
            try:
                await self._execute_job(job_id)
            except Exception:
                log.exception("job 执行异常 job=%s", job_id)
            finally:
                self._active_job = None

    async def _execute_job(self, job_id: str) -> None:
        job = self._job(job_id)
        if job is None or job["state"] == STATE_ACKED:
            return
        if job_id in self._cancel_requested:
            self._cancel_requested.discard(job_id)
            self._set_state(job_id, STATE_FAILED, error="cancelled before start")
            log.info("job 未执行即取消: %s", job_id)
            return

        result = await self.adapter.run(job["content"])
        if job_id in self._cancel_requested:
            self._cancel_requested.discard(job_id)
            self._set_state(job_id, STATE_FAILED, error="cancelled")
            log.info("job 已取消: %s", job_id)
            return

        if result.ok:
            self._set_state(job_id, STATE_RESULT_PENDING, result=result.as_send_result)
            log.info("job 完成 job=%s (%.1fs) tokens=%s", job_id, result.duration, (result.usage or {}).get("total_tokens"))
            # 立即发送（成功则等 ack 清账；失败留在 outbox，重连对账重发）
            sent = await self._flush_pending(self._client)
            if not sent:
                log.warning("结果未能发送 job=%s，留在 outbox 待重发", job_id)
        else:
            self._set_state(job_id, STATE_FAILED, error=result.error)
            log.error("job 失败 job=%s: %s", job_id, result.error)

    # ── outbox 发送 ────────────────────────────────────────────────

    async def _flush_pending(self, client: LivisWSClient | None = None) -> int:
        """发送所有 result_pending 结果（首次发送/断线重连对账共用）。

        返回成功发送条数。无 client 或发送失败则留库，等待下次对账。
        """
        pending = self._pending_results()
        if not pending:
            return 0
        log.info("outbox 待发送: %d 条", len(pending))
        sent = 0
        for row in pending:
            try:
                if client is not None:
                    await client.send_result(row["result"] or "", row["job_id"])
                    sent += 1
                    log.info("已发送结果 job=%s (%.60s)", row["job_id"], (row["result"] or "")[:60])
                else:
                    log.debug("无 WS client，结果留库 job=%s", row["job_id"])
            except Exception as e:
                log.warning("发送结果失败 job=%s: %s", row["job_id"], e)
        return sent

    async def on_reconnect(self, client: LivisWSClient) -> None:
        """重连对账：把所有 result_pending 重发。"""
        log.info("重连对账开始")
        await self._flush_pending(client)
        log.info("重连对账完成")

    # ── 生命周期 ───────────────────────────────────────────────────

    async def start(self) -> None:
        self._worker = asyncio.create_task(self._worker_loop())

    async def stop(self) -> None:
        if self._worker:
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
            self._worker = None
        self._conn.close()

    # ── 查询 ───────────────────────────────────────────────────────

    def status(self) -> dict[str, Any]:
        counts = {
            STATE_EXECUTING: 0,
            STATE_RESULT_PENDING: 0,
            STATE_ACKED: 0,
            STATE_FAILED: 0,
        }
        for row in self._conn.execute("SELECT state, COUNT(*) c FROM jobs GROUP BY state"):
            counts[row["state"]] = row["c"]
        return {
            "active_job": self._active_job,
            "counts": counts,
            "pending": [
                {"job_id": r["job_id"], "result": (r["result"] or "")[:80]}
                for r in self._pending_results()
            ],
        }
