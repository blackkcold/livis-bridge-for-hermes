"""mock 全链路测试：mock_relay <-> bridge <-> fake_hermes。

起一个本地 WS mock 中继，bridge 连上去，通过控制接口下发 exec，
验证: exec -> 执行 -> send_result -> ack -> outbox 清账。
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from pathlib import Path

import pytest
import websockets

from src.adapter import HermesAdapter
from src.bridge import Bridge, BridgeConfig, STATE_ACKED, STATE_RESULT_PENDING
from src.protocol import LivisConfig, TokenSet

FAKE = str(Path(__file__).resolve().parents[1] / "scripts" / "fake_hermes.sh")
logging.basicConfig(level=logging.WARNING)


class RelayServer:
    """轻量测试中继（在测试进程内起）。"""

    def __init__(self):
        self.client_ws = None
        self.received: list[dict] = []
        self.received_results: list[dict] = []
        self.sent_acks: list[dict] = []

    async def handle(self, ws):
        self.client_ws = ws
        try:
            # 等客户端 connect 握手（与真实中继一致：先 connect 后 connected）
            first = await ws.recv()
            first_msg = json.loads(first)
            self.received.append(first_msg)
            assert first_msg["type"] == "connect", f"首个消息应为 connect, got: {first_msg.get('type')}"
            await ws.send(json.dumps({"type": "connected", "metadata": {}, "payload": {}}))
            async for raw in ws:
                msg = json.loads(raw)
                self.received.append(msg)
                if msg["type"] == "send_result":
                    self.received_results.append(msg)
                    ack = {
                        "type": "ack_send_result",
                        "metadata": {"job_id": msg["metadata"]["job_id"], "ref_job_id": msg["metadata"]["job_id"]},
                        "payload": {},
                    }
                    self.sent_acks.append(ack)
                    await ws.send(json.dumps(ack))
                elif msg["type"] == "heartbeat":
                    await ws.send(json.dumps({"type": "pong", "metadata": {}, "payload": {}}))
        except websockets.ConnectionClosed:
            pass

    async def send_exec(self, content: str, job_id: str | None = None):
        """按真实协议: 外层 send_message + 内层 payload.data JSON。"""
        job_id = job_id or str(uuid.uuid4())
        msg = {
            "type": "send_message",
            "metadata": {"job_id": job_id, "msg_id": str(uuid.uuid4())},
            "payload": {
                "from_node_id": "mock-phone",
                "from_node_type": "phone",
                "data": json.dumps({"type": "exec", "content": content}, ensure_ascii=False),
            },
        }
        await self.client_ws.send(json.dumps(msg))
        return job_id


@pytest.fixture
async def relay_and_bridge(tmp_path):
    relay = RelayServer()
    server = await websockets.serve(relay.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    livis_cfg = LivisConfig(data_dir=tmp_path, ws_url=f"ws://127.0.0.1:{port}/api/v1/ws")
    # 预置 token（跳过认证）
    livis_cfg.tokens_path().parent.mkdir(parents=True, exist_ok=True)
    livis_cfg.tokens_path().write_text(json.dumps(TokenSet(
        access_token="mock-token", expires_in=3600, obtained_at=__import__("time").time()
    ).to_dict()))

    bcfg = BridgeConfig(hermes_bin=FAKE, db_path=tmp_path / "bridge.db")
    bridge = Bridge(bcfg, livis_cfg, HermesAdapter(bin=FAKE, timeout=30), db_path=tmp_path / "bridge.db")

    yield relay, bridge, livis_cfg, port

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_e2e_mock_full_cycle(relay_and_bridge):
    relay, bridge, livis_cfg, port = relay_and_bridge

    from src.protocol import LivisWSClient
    client = LivisWSClient(livis_cfg, bridge.on_message, tokens=TokenSet(access_token="mock-token", expires_in=3600, obtained_at=__import__("time").time()))
    await client.connect()
    await bridge.start()
    bridge.set_client(client)  # 注入 WS client，结果才能发回 relay
    recv_task = asyncio.create_task(client.recv_loop())  # 启动收消息循环
    try:
        # 等握手
        await asyncio.sleep(0.2)
        job_id = await relay.send_exec("你好，帮我查一下天气")
        # 等待结果到达 relay
        for _ in range(50):
            if relay.received_results:
                break
            await asyncio.sleep(0.1)
        assert relay.received_results, "relay 应收到 send_result"
        result_msg = relay.received_results[0]
        # send_result.payload.data 应为 JSON 字符串 {"text": "..."}
        import json as _json
        data = result_msg["payload"]["data"]
        parsed = _json.loads(data) if isinstance(data, str) else data
        assert "FAKE-HERMES-OK" in parsed.get("text", ""), f"结果应含 FAKE-HERMES-OK: {data[:200]}"
        # ack 应已由 relay 回发 → bridge outbox 清账
        for _ in range(50):
            job = bridge._job(job_id)
            if job and job["state"] == STATE_ACKED:
                break
            await asyncio.sleep(0.1)
        job = bridge._job(job_id)
        assert job is not None and job["state"] == STATE_ACKED, f"应 acked, got {job['state'] if job else None}"
        assert not bridge._pending_results(), "outbox 应清空"
    finally:
        recv_task.cancel()
        await bridge.stop()
        await client.close()


@pytest.mark.asyncio
async def test_e2e_mock_send_message_type(relay_and_bridge):
    relay, bridge, livis_cfg, port = relay_and_bridge
    from src.protocol import LivisWSClient
    client = LivisWSClient(livis_cfg, bridge.on_message, tokens=TokenSet(access_token="mock-token", expires_in=3600, obtained_at=__import__("time").time()))
    await client.connect()
    await bridge.start()
    bridge.set_client(client)  # 注入 WS client，结果才能发回 relay
    recv_task = asyncio.create_task(client.recv_loop())  # 启动收消息循环
    try:
        await asyncio.sleep(0.2)
        # send_message 内层 exec 也应触发执行（与真实协议一致）
        msg = {
            "type": "send_message",
            "metadata": {"job_id": str(uuid.uuid4())},
            "payload": {"from_node_id": "mock", "data": json.dumps({"type": "exec", "content": "发送消息测试"}, ensure_ascii=False)},
        }
        await relay.client_ws.send(json.dumps(msg))
        for _ in range(50):
            if relay.received_results:
                break
            await asyncio.sleep(0.1)
        assert relay.received_results
    finally:
        recv_task.cancel()
        await bridge.stop()
        await client.close()
