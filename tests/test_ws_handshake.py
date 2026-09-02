"""WS 握手协议测试（新协议：connect 消息认证 + heartbeat 心跳）。"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pytest
import websockets

from src.protocol import (
    CLIENT_NAME,
    LivisConfig,
    LivisWSClient,
    TokenSet,
    load_agent_id,
)

FAKE = str(Path(__file__).resolve().parents[1] / "scripts" / "fake_hermes.sh")


class HandshakeRecorder:
    """记录客户端发来的第一条消息（connect 握手）。"""

    def __init__(self):
        self.first_msg = None
        self.connected_sent = False

    async def handle(self, ws):
        try:
            raw = await ws.recv()
            self.first_msg = json.loads(raw)
            await ws.send(json.dumps({"type": "connected", "metadata": {}, "payload": {}}))
            self.connected_sent = True
            # 保持连接直到关闭
            async for _raw in ws:
                pass
        except websockets.ConnectionClosed:
            pass


@pytest.fixture
async def recorder_server(tmp_path):
    recorder = HandshakeRecorder()
    server = await websockets.serve(recorder.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    cfg = LivisConfig(data_dir=tmp_path, ws_url=f"ws://127.0.0.1:{port}/api/v1/ws")
    cfg.tokens_path().parent.mkdir(parents=True, exist_ok=True)
    cfg.tokens_path().write_text(json.dumps(TokenSet(
        access_token="test-token-123",
        refresh_token="test-refresh-456",
        expires_in=3600,
        obtained_at=time.time(),
    ).to_dict()))

    yield recorder, cfg, port

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_connect_handshake_format(recorder_server):
    """验证客户端首个消息是 connect 且携带 token/device/agent 信息。"""
    recorder, cfg, port = recorder_server
    client = LivisWSClient(cfg, on_message=lambda m: None)
    await client.connect()
    try:
        await asyncio.sleep(0.3)
        assert recorder.first_msg is not None, "服务端应收到 connect 握手"
        msg = recorder.first_msg
        assert msg["type"] == "connect"
        meta = msg["metadata"]
        assert meta["agent_id"].startswith("openclaw-")
        payload = msg["payload"]
        assert payload["client"] == CLIENT_NAME
        assert payload["token"] == "test-token-123"
        assert payload["refresh_token"] == "test-refresh-456"
        assert payload["node_name"] == "我的电脑"
        assert "device_id" in payload
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_agent_id_saved(recorder_server):
    """agentId 应持久化到文件。"""
    recorder, cfg, port = recorder_server
    client = LivisWSClient(cfg, on_message=lambda m: None)
    await client.connect()
    try:
        await asyncio.sleep(0.3)
        saved = load_agent_id(cfg)
        assert saved and saved.startswith("openclaw-")
        assert saved == client.agent_id, "持久化的 agentId 应与 WS 使用的一致"
    finally:
        await client.close()
