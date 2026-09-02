"""Livis (理想AI眼镜) 协议层。

逆向自官方 openclaw 插件 (release-2.0.0-7287b4fc):
  - 认证: OAuth2 Device Flow (RFC 8628) @ https://id.lixiang.com/api
  - 中继: WebSocket @ wss://livis-pc-kit-gateway.livis.com/api/v1/ws
  - 消息: connected / send_message / exec / send_result / ack_* / ping/pong

本文件只做协议编解码与传输，不含业务状态机（见 bridge.py）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Awaitable

import httpx

log = logging.getLogger("livis.protocol")

# ── 逆向参数（官方 openclaw 插件 v2.0.0 公开客户端参数）────────────
# 以下参数来自理想官方分发的公开插件包，属公开客户端标识（OAuth public client），
# 非个人凭据；个人凭据（access/refresh token）只存在于运行时 data/ 目录。
IDAAS_ENDPOINT = "https://id.lixiang.com/api"
CLIENT_ID = "6qxd1MLZhAtdWipnmXe1dd"
APP_AUDIENCE = "rZgT0SETDNueMVAhfRN10"
APP_SCOPE = "super"

DEFAULT_WS_URL = "wss://livis-pc-kit-gateway.livis.com/api/v1/ws"

# 消息类型
MSG_CONNECTED = "connected"
MSG_SEND_MESSAGE = "send_message"
MSG_SEND_RESULT = "send_result"
MSG_EXEC = "exec"
MSG_CANCEL_CHAT = "cancel_chat"
MSG_ACK_SEND_MESSAGE = "ack_send_message"
MSG_ACK_SEND_RESULT = "ack_send_result"
MSG_PING = "ping"
MSG_PONG = "pong"
MSG_TOKEN_EXPIRED = "token_expired"


@dataclass
class LivisConfig:
    """协议级配置（可被环境变量覆盖，便于测试）。"""

    idaas_endpoint: str = IDAAS_ENDPOINT
    client_id: str = CLIENT_ID
    audience: str = APP_AUDIENCE
    scope: str = APP_SCOPE
    ws_url: str = DEFAULT_WS_URL
    data_dir: Path = field(
        default_factory=lambda: Path(os.environ.get("LIVIS_DATA_DIR", "./data"))
    )

    def tokens_path(self) -> Path:
        return self.data_dir / "livis-pc-kit-tokens.json"

    def agent_id_path(self) -> Path:
        return self.data_dir / "livis-agent.id"

    @classmethod
    def from_env(cls) -> "LivisConfig":
        return cls(
            ws_url=os.environ.get("LIVIS_WS_URL", DEFAULT_WS_URL),
            data_dir=Path(os.environ.get("LIVIS_DATA_DIR", "./data")),
        )


# ── Token 存取 ────────────────────────────────────────────────────────

@dataclass
class TokenSet:
    access_token: str
    refresh_token: str = ""
    expires_in: int = 0
    obtained_at: float = 0.0

    @property
    def expired(self) -> bool:
        return time.time() >= self.obtained_at + self.expires_in - 60

    def to_dict(self) -> dict:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_in": self.expires_in,
            "obtained_at": self.obtained_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TokenSet":
        return cls(
            access_token=d.get("access_token", ""),
            refresh_token=d.get("refresh_token", ""),
            expires_in=d.get("expires_in", 0),
            obtained_at=d.get("obtained_at", 0.0),
        )


def load_tokens(cfg: LivisConfig) -> TokenSet | None:
    p = cfg.tokens_path()
    if not p.exists():
        return None
    try:
        return TokenSet.from_dict(json.loads(p.read_text()))
    except Exception:
        log.warning("token 文件损坏: %s", p)
        return None


def save_tokens(cfg: LivisConfig, tokens: TokenSet) -> None:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    tmp = cfg.tokens_path().with_suffix(".tmp")
    tmp.write_text(json.dumps(tokens.to_dict(), indent=2))
    tmp.replace(cfg.tokens_path())
    log.info("tokens 已保存: %s", cfg.tokens_path())


def load_agent_id(cfg: LivisConfig) -> str | None:
    p = cfg.agent_id_path()
    return p.read_text().strip() if p.exists() else None


def save_agent_id(cfg: LivisConfig, agent_id: str) -> None:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    cfg.agent_id_path().write_text(agent_id + "\n")
    log.info("agent_id 已保存: %s", agent_id)


def device_id_path(cfg: LivisConfig) -> Path:
    return cfg.data_dir / "livis-device.id"


def load_device_id(cfg: LivisConfig) -> str | None:
    p = device_id_path(cfg)
    return p.read_text().strip() if p.exists() else None


def save_device_id(cfg: LivisConfig, device_id: str) -> None:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    device_id_path(cfg).write_text(device_id + "\n")
    log.info("device_id 已保存: %s", device_id)


def get_or_create_device_id(cfg: LivisConfig) -> str:
    """device_id 必须持久化（官方同款: ~/.openclaw/livis-device.id）。

    服务端按 device_id 维护连接路由；每次重连都换新 id 会导致消息路由失败。
    """
    existing = load_device_id(cfg)
    if existing:
        return existing
    new_id = str(uuid.uuid4())
    save_device_id(cfg, new_id)
    return new_id


def make_agent_id(prefix: str = "openclaw") -> str:
    """与官方一致的 agentId 格式: <前缀>-<uuid>。"""
    return f"{prefix}-{uuid.uuid4()}"


# ── Device Flow 认证 ─────────────────────────────────────────────────

class DeviceFlowError(Exception):
    pass


class DeviceFlowClient:
    """标准 OAuth2 Device Flow (RFC 8628)。

    流程: request_device_code -> 用户在手机 App 确认 -> poll token。
    """

    def __init__(self, cfg: LivisConfig, client: httpx.AsyncClient | None = None):
        self.cfg = cfg
        self._http = client or httpx.AsyncClient(base_url=cfg.idaas_endpoint)

    async def request_device_code(self) -> dict[str, Any]:
        resp = await self._http.post(
            "/aux",
            data={
                "client_id": self.cfg.client_id,
                "scope": f"{self.cfg.scope} offline_access",
                "audience": self.cfg.audience,
                "offline_access": "true",
            },
        )
        resp.raise_for_status()
        return resp.json()

    def _unwrap(self, data: dict) -> dict:
        """官方响应可能嵌套在 data[appAudience] 下。"""
        nested = data.get(self.cfg.audience)
        return nested if isinstance(nested, dict) else data

    async def poll_token(self, device_code: str, interval: float = 5.0, timeout: float = 600.0) -> TokenSet:
        """轮询授权结果，直到成功/拒绝/超时。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = await self._http.post(
                "/token",
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "device_code": device_code,
                    "client_id": self.cfg.client_id,
                },
            )
            if resp.status_code == 200:
                body = self._unwrap(resp.json())
                if not body.get("access_token"):
                    raise DeviceFlowError(f"响应缺 access_token: {resp.text[:200]}")
                return TokenSet(
                    access_token=body["access_token"],
                    refresh_token=body.get("refresh_token", ""),
                    expires_in=body.get("expires_in", 3600),
                    obtained_at=time.time(),
                )
            err = resp.json().get("error", "unknown")
            if err == "authorization_pending":
                await asyncio.sleep(interval)
                continue
            if err in ("slow_down",):
                await asyncio.sleep(interval + 5)
                continue
            raise DeviceFlowError(f"device flow 失败: {err} ({resp.text[:200]})")
        raise DeviceFlowError("device flow 超时")

    async def refresh(self, tokens: TokenSet) -> TokenSet:
        if not tokens.refresh_token:
            raise DeviceFlowError("无 refresh_token")
        resp = await self._http.post(
            "/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": tokens.refresh_token,
            },
        )
        resp.raise_for_status()
        body = self._unwrap(resp.json())
        if not body.get("access_token"):
            raise DeviceFlowError(f"refresh 响应缺 access_token: {resp.text[:200]}")
        return TokenSet(
            access_token=body["access_token"],
            refresh_token=body.get("refresh_token", tokens.refresh_token),
            expires_in=body.get("expires_in", 3600),
            obtained_at=time.time(),
        )

    async def close(self) -> None:
        await self._http.aclose()


# ── 消息编解码 ────────────────────────────────────────────────────────

def _meta(agent_id: str, device_id: str, job_id: str | None = None) -> dict[str, Any]:
    return {
        "msg_id": str(uuid.uuid4()),
        "job_id": job_id or str(uuid.uuid4()),
        "agent_id": agent_id,
        "device_id": device_id,
        "timestamp": int(time.time() * 1000),
    }


def encode_message(
    msg_type: str,
    agent_id: str,
    device_id: str,
    payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    job_id: str | None = None,
) -> str:
    msg = {
        "type": msg_type,
        "metadata": metadata or _meta(agent_id, device_id, job_id),
        "payload": payload or {},
    }
    return json.dumps(msg, ensure_ascii=False)


def decode_message(raw: str) -> dict[str, Any]:
    msg = json.loads(raw)
    if "type" not in msg:
        raise ValueError(f"消息缺少 type: {raw[:200]}")
    msg.setdefault("metadata", {})
    msg.setdefault("payload", {})
    return msg


def exec_content(msg: dict[str, Any]) -> str:
    """从 exec 消息提取用户指令。"""
    payload = msg.get("payload") or {}
    content = payload.get("content", "")
    if not isinstance(content, str) or not content.strip():
        raise ValueError("exec payload.content 缺失或为空")
    return content


# ── WS 客户端 ─────────────────────────────────────────────────────────

MessageHandler = Callable[[dict[str, Any]], Awaitable[None]]

# 官方常量（逆向自 bundle.js channel.ts）
CLIENT_NAME = "openclaw"
PERSONL_DEVICE = "personl-device"
NODE_NAME = "我的电脑"
NODE_DESC = f"{PERSONL_DEVICE} {NODE_NAME}"
HB_INTERVAL = 30      # 心跳间隔 30s
PONG_TIMEOUT = 60     # 心跳超时 60s → 强制重连
MSG_CONNECT = "connect"
MSG_HEARTBEAT = "heartbeat"


class LivisWSClient:
    """Livis 中继 WebSocket 客户端。

    官方握手（逆向自 bundle.js）:
      1. URL: {ws_url}?protocol_version=1  （无 headers）
      2. onopen 后发 type="connect" 消息，payload 携带 token/refresh_token/device_id
      3. 心跳: 每 30s 发 ws ping + type="heartbeat" 应用消息；
         60s 无 pong → 强制重连
    所有发送消息自动附加 metadata.client 与 payload.nodeType。
    """

    def __init__(
        self,
        cfg: LivisConfig,
        on_message: MessageHandler,
        tokens: TokenSet | None = None,
        agent_id: str | None = None,
        device_id: str | None = None,
        node_name: str = NODE_NAME,
    ):
        self.cfg = cfg
        self.on_message = on_message
        self.tokens = tokens
        self.agent_id = agent_id or load_agent_id(cfg) or make_agent_id()
        self.device_id = device_id or get_or_create_device_id(cfg)
        self.node_name = node_name
        if agent_id is None:
            save_agent_id(cfg, self.agent_id)
        self._ws = None
        self._running = False
        self._hb_task: asyncio.Task | None = None
        self._last_pong_at = 0.0
        self._connected_at: float | None = None
        self._last_msg_at = 0.0  # C3: 路由失联监控
        self._inactivity_warned = False

    async def connect(self) -> None:
        import websockets

        if self.tokens is None:
            self.tokens = load_tokens(self.cfg)
        if self.tokens is None or not self.tokens.access_token:
            raise DeviceFlowError("未认证: 请先执行 device-flow 认证")

        url = f"{self.cfg.ws_url}?protocol_version=1"
        log.info("连接中继: %s (agent=%s)", url, self.agent_id)
        self._ws = await websockets.connect(url)
        self._running = True
        self._connected_at = __import__("time").time()
        self._last_pong_at = __import__("time").time()
        await self._send_connect_handshake()
        self._hb_task = asyncio.create_task(self._heartbeat_loop())
        log.info("中继已连接（connect 握手已发送）")

    async def _send_connect_handshake(self) -> None:
        assert self.tokens is not None
        msg = {
            "type": MSG_CONNECT,
            "metadata": _meta(self.agent_id, self.device_id),
            "payload": {
                "device_id": self.device_id,
                "node_name": self.node_name,
                "node_desc": f"{PERSONL_DEVICE} {self.node_name}",
                "client": CLIENT_NAME,
                "token": self.tokens.access_token,
                "refresh_token": self.tokens.refresh_token,
            },
        }
        await self._ws.send(json.dumps(msg, ensure_ascii=False))
        log.info("connect 握手已发送 (device_id=%s)", self.device_id)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                # websockets.ping() 会等待服务端 pong —— 收到即健康；
                # 超时才重连（修复: 之前用 last_pong_at 判断，但应用层
                # JSON pong 服务端从不发，导致每 80s 误判超时强制重连）
                if self._ws is not None:
                    await asyncio.wait_for(self._ws.ping(), timeout=10)
                await self.send_raw(MSG_HEARTBEAT, {})
            except asyncio.TimeoutError:
                log.warning("pong 超时 (10s)，强制重连")
                await self.close()
                return
            except Exception as e:
                log.warning("心跳异常: %s → 重连", e)
                await self.close()
                return
            await asyncio.sleep(HB_INTERVAL)

    async def send_raw(self, msg_type: str, payload: dict[str, Any], job_id: str | None = None) -> str:
        text = encode_message(msg_type, self.agent_id, self.device_id, payload, job_id=job_id)
        # 官方 sendMessage: metadata.client + payload.nodeType
        outgoing = json.loads(text)
        outgoing["metadata"]["client"] = CLIENT_NAME
        outgoing.setdefault("payload", {})
        outgoing["payload"]["nodeType"] = PERSONL_DEVICE
        raw = json.dumps(outgoing, ensure_ascii=False)
        if self._ws is None:
            raise ConnectionError("WS 未连接")
        # 诊断快照：发送的 send_result 原文
        if msg_type == MSG_SEND_RESULT and self.cfg.data_dir:
            try:
                self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
                (self.cfg.data_dir / "diag-send_result.json").write_text(raw)
            except Exception:
                pass
        await self._ws.send(raw)
        return raw

    async def send_result(self, data: str, job_id: str) -> str:
        # 官方: payload.data 是 JSON 字符串 {"text": "..."}（见 deliverResult）
        return await self.send_raw(MSG_SEND_RESULT, {"data": json.dumps({"text": data}, ensure_ascii=False)}, job_id)

    async def send_ack(self, ack_type: str, ref: dict[str, Any]) -> str:
        """回执（ack_send_message / ack_send_result / ack_cancel_chat）。

        ★ 关键：job_id 必须用原始 job_id（绝不能新生成 uuid）！
        服务端 ack_send_result 匹配顺序: payload.ref_msg_id
                                    → metadata.job_id → metadata.msg_id
        用新 uuid 会导致服务端永远匹配不到 pending job → 反复重投 → App 卡"处理中"。
        """
        original_job = ref.get("job_id") or ref.get("ref_job_id") or ""
        original_msg = ref.get("msg_id") or ref.get("ref_msg_id") or ""
        meta = {
            "msg_id": str(uuid.uuid4()),
            "job_id": original_job,  # ★ 原始 job_id，不是新的！
            "agent_id": self.agent_id,
            "device_id": self.device_id,
            "timestamp": int(time.time() * 1000),
            "client": CLIENT_NAME,
        }
        payload: dict[str, Any] = {"nodeType": PERSONL_DEVICE}
        if original_msg:
            payload["ref_msg_id"] = original_msg  # ★ 服务端优先读这里
        if original_job:
            payload["ref_job_id"] = original_job
        text = json.dumps({"type": ack_type, "metadata": meta, "payload": payload}, ensure_ascii=False)
        if self._ws is None:
            raise ConnectionError("WS 未连接")
        await self._ws.send(text)
        return text

    async def handle_token_expiring(self) -> None:
        """服务端提示 token 将过期 → 立即 refresh + 上报新 token（官方协议）。"""
        log.info("服务端 token_expiring → 立即刷新")

        assert self.tokens is not None
        df = DeviceFlowClient(self.cfg)
        try:
            fresh = await df.refresh(self.tokens)
            self.tokens = fresh
            save_tokens(self.cfg, fresh)
            await self.send_raw(
                "token_refresh",
                {"token": fresh.access_token, "refresh_token": fresh.refresh_token},
            )
            log.info("token 已刷新并上报 token_refresh")
        except Exception as e:
            log.error("token 刷新失败: %s", e)
        finally:
            await df.close()

    async def recv_loop(self) -> None:
        """接收消息循环（遇连接断开抛异常，由上层重连）。"""
        import time as _time

        assert self._ws is not None
        # C3: 路由失联监控——连接正常但长时间无服务端消息 = 消息路由不到我们
        self._last_msg_at = _time.time()
        self._inactivity_warned = False

        async def _inactivity_watcher():
            while self._running:
                await asyncio.sleep(60)
                idle = _time.time() - self._last_msg_at
                if idle > 120 and not self._inactivity_warned:
                    log.warning(
                        "⚠️ 路由失联疑似: 连接正常但 %ds 无任何服务端消息（token/heartbeat 除外）。"
                        "可能原因: App 对话目标节点与该连接 device_id 不匹配，或消息路由异常。",
                        idle,
                    )
                    self._inactivity_warned = True

        watcher = asyncio.create_task(_inactivity_watcher())
        try:
            async for raw in self._ws:
                try:
                    msg = decode_message(raw)
                except ValueError as e:
                    log.warning("消息解析失败: %s", e)
                    continue
                self._last_msg_at = _time.time()
                self._inactivity_warned = False
                # 诊断快照：保存关键消息原文到数据目录（不受日志截断影响）
                if msg["type"] in ("send_message", "ack_send_result", "connected") and self.cfg.data_dir:
                    try:
                        self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
                        (self.cfg.data_dir / f"diag-{msg['type']}.json").write_text(raw)
                    except Exception:
                        pass
                if msg["type"] == MSG_PONG:
                    self._last_pong_at = _time.time()
                    continue
                if msg["type"] in (MSG_CONNECTED, MSG_CONNECT):
                    log.info("服务端 %s", msg["type"])
                    continue
                if msg["type"] == "token_expiring":
                    log.info("token expiring at %s", (msg.get("payload") or {}).get("expires_at"))
                    await self.handle_token_expiring()
                    continue
                # 已知服务端控制消息（token_refreshed 等）→ 静默,但计入活跃
                try:
                    await self.on_message(msg)
                except Exception:
                    log.exception("消息处理异常: type=%s", msg["type"])
        finally:
            watcher.cancel()

    async def close(self) -> None:
        self._running = False
        if self._hb_task:
            self._hb_task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None


# ── 顶层便捷流程 ──────────────────────────────────────────────────────

async def run_device_flow(cfg: LivisConfig) -> TokenSet:
    """完整 device flow：请求 code → 打印授权指引 → 轮询。"""
    df = DeviceFlowClient(cfg)
    try:
        code_info = await df.request_device_code()
        verification = code_info.get("verification_uri_complete") or code_info.get("verification_uri", "")
        print(f"\n请在手机「理想同学」App 中完成授权:\n  {verification}\n")
        print(f"(手动码: {code_info.get('user_code', '')} — 有效期 {code_info.get('expires_in', 0)}s)\n")
        tokens = await df.poll_token(code_info["device_code"], interval=code_info.get("interval", 5))
        save_tokens(cfg, tokens)
        print("✅ 认证成功，tokens 已保存")
        return tokens
    finally:
        await df.close()


async def ensure_token(cfg: LivisConfig) -> TokenSet:
    """取 token；过期则自动 refresh；无 token 则抛错提示先认证。"""
    tokens = load_tokens(cfg)
    if tokens is None:
        raise DeviceFlowError("未认证: 请先运行 `python -m src.cli device-flow`")
    if not tokens.expired:
        return tokens
    df = DeviceFlowClient(cfg)
    try:
        fresh = await df.refresh(tokens)
        save_tokens(cfg, fresh)
        return fresh
    finally:
        await df.close()
