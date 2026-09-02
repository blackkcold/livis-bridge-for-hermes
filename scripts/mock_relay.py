"""模拟 Livis 中继服务器（测试/开发用）。

行为:
  - WS 接收: 认证握手 (connected) -> exec 下发 -> 收 send_result -> 回 ack_send_result
  - 支持注入: 断线 / 延迟 / 未知消息类型
  - 可通过 HTTP 控制接口 (127.0.0.1:8765/admin) 触发 exec

用法:
  python -m scripts.mock_relay --port 8765
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import uuid
from typing import Any

import websockets

log = logging.getLogger("mock_relay")

# 客户端(桥)连入的 URL: ws://localhost:8765/api/v1/ws
# 控制接口:        http://localhost:8765/admin/send_exec?content=xxx
#                  http://localhost:8765/admin/close_all


class MockRelay:
    def __init__(self):
        self.clients: set[websockets.WebSocketServerProtocol] = set()
        self.received: list[dict[str, Any]] = []
        self.pending_acks: dict[str, str] = {}  # job_id -> raw send_result

    # ── 客户端处理 ──────────────────────────────────────────────────

    async def handle(self, ws: websockets.WebSocketServerProtocol) -> None:
        self.clients.add(ws)
        log.info("客户端接入: %s (共 %d)", ws.remote_address, len(self.clients))
        try:
            # 等客户端 connect 握手（协议要求先发 connect 再回 connected）
            first = await ws.recv()
            msg = json.loads(first)
            self.received.append(msg)
            if msg.get("type") != "connect":
                log.warning("首个消息不是 connect: %s", msg.get("type"))
            else:
                payload = msg.get("payload") or {}
                log.info(
                    "connect 握手: device_id=%s client=%s node_name=%s token=%s...",
                    payload.get("device_id"), payload.get("client"),
                    payload.get("node_name"), str(payload.get("token", ""))[:8],
                )
            await ws.send(json.dumps({"type": "connected", "metadata": {}, "payload": {}}))
            async for raw in ws:
                msg = json.loads(raw)
                self.received.append(msg)
                await self.dispatch(ws, msg)
        except websockets.ConnectionClosed:
            pass
        finally:
            self.clients.discard(ws)
            log.info("客户端断开: %s (剩 %d)", ws.remote_address, len(self.clients))

    # ── send_exec：模拟真实中继发嵌套 send_message ─────────────────

    async def send_exec(self, content: str, job_id: str | None = None) -> int:
        """按真实协议下发: 外层 send_message, payload.data 内层 {"type":"exec","content":...}。"""
        job_id = job_id or str(uuid.uuid4())
        msg = {
            "type": "send_message",
            "metadata": {
                "msg_id": str(uuid.uuid4()),
                "job_id": job_id,
                "agent_id": "mock-openclaw-11111111",
                "device_id": "mock-device-22222222",
                "timestamp": 0,
            },
            "payload": {
                "from_node_id": "mock-phone",
                "from_node_type": "phone",
                "data": json.dumps({"type": "exec", "content": content}, ensure_ascii=False),
            },
        }
        targets = list(self.clients)
        for ws in targets:
            await ws.send(json.dumps(msg, ensure_ascii=False))
        log.info("已下发 exec job=%s content=%.60s (客户端 %d)", job_id, content, len(targets))
        return len(targets)

    async def dispatch(self, ws: websockets.WebSocketServerProtocol, msg: dict[str, Any]) -> None:
        mtype = msg.get("type", "")
        meta = msg.get("metadata") or {}
        if mtype == "send_result":
            job_id = meta.get("job_id", "?")
            self.pending_acks[job_id] = json.dumps(msg, ensure_ascii=False)
            # 模拟服务端回 ack
            ack = {
                "type": "ack_send_result",
                "metadata": {
                    "msg_id": str(uuid.uuid4()),
                    "job_id": job_id,
                    "agent_id": meta.get("agent_id", ""),
                    "device_id": meta.get("device_id", ""),
                    "timestamp": 0,
                    "ref_job_id": job_id,
                },
                "payload": {},
            }
            await ws.send(json.dumps(ack, ensure_ascii=False))
            log.info("收到 send_result job=%s → 已回 ack", job_id)
        elif mtype == "heartbeat":
            await ws.send(json.dumps({"type": "pong", "metadata": {}, "payload": {}}))
        # 其余（ack_send_message 等）记录即可

    # ── 控制接口 ────────────────────────────────────────────────────

    async def close_all(self) -> None:
        for ws in list(self.clients):
            await ws.close()
        self.clients.clear()
        log.info("已断开全部客户端")


async def http_control(relay: MockRelay, port: int) -> None:
    """极简 HTTP 控制接口。"""
    import http.server

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):  # 静默
            pass

        def _send(self, code: int, body: str = ""):
            self.send_response(code)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(body.encode())

        def do_GET(self):  # noqa: N802
            from urllib.parse import urlparse, parse_qs

            path = urlparse(self.path).path
            qs = parse_qs(urlparse(self.path).query)
            if path == "/admin/send_exec":
                content = qs.get("content", [""])[0]
                asyncio.create_task(relay.send_exec(content))
                self._send(200, f"exec 已下发: {content[:60]}")
            elif path == "/admin/close_all":
                asyncio.create_task(relay.close_all())
                self._send(200, "已断开全部")
            else:
                self._send(404, "not found")

    server = http.server.ThreadingHTTPServer(("127.0.0.1", port + 1), Handler)
    log.info("控制接口: http://127.0.0.1:%d/admin/...", port + 1)
    await asyncio.to_thread(server.serve_forever)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    relay = MockRelay()

    async with websockets.serve(relay.handle, "0.0.0.0", args.port):
        log.info("mock 中继监听: ws://0.0.0.0:%d/api/v1/ws", args.port)
        await asyncio.gather(http_control(relay, args.port), asyncio.Future())


if __name__ == "__main__":
    asyncio.run(main())
