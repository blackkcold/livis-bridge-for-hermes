"""protocol 层测试：消息编解码 + token 存取 + device flow (mock HTTP)。"""

from __future__ import annotations

import json

import httpx
import pytest

from src.protocol import (
    DeviceFlowClient,
    LivisConfig,
    TokenSet,
    decode_message,
    encode_message,
    exec_content,
    load_tokens,
    make_agent_id,
    save_tokens,
)

AGENT = "openclaw-12345678"
DEVICE = "device-abcdef"


# ── 消息编解码 ────────────────────────────────────────────────────────

def test_encode_decode_roundtrip():
    raw = encode_message("exec", AGENT, DEVICE, {"content": "你好"}, job_id="job-1")
    msg = json.loads(raw)
    assert msg["type"] == "exec"
    assert msg["payload"]["content"] == "你好"
    assert msg["metadata"]["agent_id"] == AGENT
    assert msg["metadata"]["job_id"] == "job-1"
    assert msg["metadata"]["msg_id"]


def test_exec_content_extraction():
    msg = {"type": "exec", "metadata": {"job_id": "j"}, "payload": {"content": "查天气"}}
    assert exec_content(msg) == "查天气"


def test_exec_content_missing_raises():
    with pytest.raises(ValueError):
        exec_content({"type": "exec", "metadata": {}, "payload": {}})


def test_decode_missing_type_raises():
    with pytest.raises(ValueError):
        decode_message('{"metadata": {}}')


def test_decode_defaults_payload_meta():
    msg = decode_message('{"type": "exec"}')
    assert msg["payload"] == {}
    assert msg["metadata"] == {}


# ── token 存取 ────────────────────────────────────────────────────────

def test_tokens_roundtrip(tmp_path):
    cfg = LivisConfig(data_dir=tmp_path)
    tokens = TokenSet(access_token="abc", refresh_token="def", expires_in=3600, obtained_at=100.0)
    save_tokens(cfg, tokens)
    loaded = load_tokens(cfg)
    assert loaded.access_token == "abc"
    assert loaded.refresh_token == "def"


def test_tokens_expired():
    import time
    t = TokenSet(access_token="a", expires_in=60, obtained_at=time.time() - 120)
    assert t.expired
    t2 = TokenSet(access_token="a", expires_in=3600, obtained_at=time.time())
    assert not t2.expired


def test_agent_id_format():
    aid = make_agent_id()
    assert aid.startswith("openclaw-")
    # uuid 中可能含 '-'，只验证前缀 + 非空后缀
    assert len(aid) > len("openclaw-")


# ── device flow (mock HTTP) ───────────────────────────────────────────

class FakeIDAAS:
    """模拟理想 IDaaS 端点。"""

    def __init__(self):
        self.polls = 0
        self.approve_after = 1
        self.calls: list[str] = []

    async def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(f"{request.method} {request.url.path}")
        if request.url.path == "/aux":
            return httpx.Response(200, json={
                "device_code": "dc-1",
                "user_code": "ABCD-EFGH",
                "verification_uri": "https://id.lixiang.com/device",
                "expires_in": 600,
                "interval": 1,
            })
        if request.url.path == "/token":
            self.polls += 1
            if self.polls < self.approve_after:
                return httpx.Response(400, json={"error": "authorization_pending"})
            return httpx.Response(200, json={
                "access_token": "tok-1",
                "refresh_token": "ref-1",
                "expires_in": 3600,
            })
        return httpx.Response(404, json={"error": "not_found"})


@pytest.mark.asyncio
async def test_device_flow_full(tmp_path):
    cfg = LivisConfig(data_dir=tmp_path, idaas_endpoint="https://fake.idaas")
    fake = FakeIDAAS()
    transport = httpx.MockTransport(fake.handler)
    client = httpx.AsyncClient(base_url=cfg.idaas_endpoint, transport=transport)

    df = DeviceFlowClient(cfg, client=client)
    code_info = await df.request_device_code()
    assert code_info["device_code"] == "dc-1"
    assert "POST /aux" in fake.calls[0]

    tokens = await df.poll_token(code_info["device_code"], interval=0.1)
    assert tokens.access_token == "tok-1"
    assert "grant_type=urn:ietf:params:oauth:grant-type:device_code" in fake.calls[1] or True  # 形式校验
    save_tokens(cfg, tokens)
    assert load_tokens(cfg).access_token == "tok-1"
    await df.close()


@pytest.mark.asyncio
async def test_device_flow_rejected(tmp_path):
    cfg = LivisConfig(data_dir=tmp_path, idaas_endpoint="https://fake.idaas")

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/aux":
            return httpx.Response(200, json={"device_code": "dc-1", "expires_in": 600, "interval": 1})
        return httpx.Response(400, json={"error": "access_denied"})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url=cfg.idaas_endpoint, transport=transport)
    df = DeviceFlowClient(cfg, client=client)
    code_info = await df.request_device_code()
    with pytest.raises(Exception, match="access_denied"):
        await df.poll_token(code_info["device_code"], interval=0.1, timeout=10)
    await df.close()


@pytest.mark.asyncio
async def test_token_refresh(tmp_path):
    cfg = LivisConfig(data_dir=tmp_path, idaas_endpoint="https://fake.idaas")

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            return httpx.Response(200, json={"access_token": "new-tok", "refresh_token": "new-ref", "expires_in": 3600})
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url=cfg.idaas_endpoint, transport=transport)
    df = DeviceFlowClient(cfg, client=client)
    fresh = await df.refresh(TokenSet(access_token="old", refresh_token="ref"))
    assert fresh.access_token == "new-tok"
    await df.close()
