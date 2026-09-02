"""WebUI 状态服务测试（mock bridge，验证接口与隔离性）。"""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from src.bridge import Bridge, BridgeConfig
from src.protocol import LivisConfig
from src.webui import LogBuffer, WebUIServer, WebUIState


class FakeBridge:
    """最小 bridge 替身（不跑真逻辑）。"""

    def __init__(self, tmp_path):
        cfg = BridgeConfig(db_path=tmp_path / "bridge.db")
        self.cfg = cfg
        livis_cfg = LivisConfig(data_dir=tmp_path)
        self._conn = None
        import sqlite3
        self._conn = sqlite3.connect(tmp_path / "bridge.db")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY, content TEXT, state TEXT,
                result TEXT, error TEXT, raw_msg TEXT,
                created_at REAL, updated_at REAL
            )
        """)
        self._conn.execute(
            "INSERT INTO jobs VALUES ('job-1','你好','acked','结果文本','','{}',100,101)"
        )
        self._conn.execute(
            "INSERT INTO jobs VALUES ('job-2','失败任务','failed','','boom','{}',102,103)"
        )
        self._conn.commit()
        self._active_job = "job-1"
        self._queue = __import__("asyncio").Queue()
        self._queue.put_nowait("job-3")


class FakeClient:
    """最小 WS client 替身。"""

    def __init__(self):
        self._ws = object()
        self._connected_at = time.time() - 100
        self._last_pong_at = time.time() - 5
        self._last_msg_at = time.time() - 30
        self.agent_id = "openclaw-test-1234"
        self.device_id = "device-test-5678"


@pytest.fixture
def webui(tmp_path):
    bridge = FakeBridge(tmp_path)
    client = FakeClient()
    livis_cfg = LivisConfig(data_dir=tmp_path)
    state = WebUIState(bridge=bridge, client=client, livis_cfg=livis_cfg)
    log_buffer = LogBuffer()
    server = WebUIServer(state=state, host="127.0.0.1", port=0, log_buffer=log_buffer)
    # port=0 → 自动分配
    server.port = 0
    assert server.start()
    yield server, state
    server.stop()


def _get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())


def test_status_endpoint(webui):
    server, state = webui
    base = f"http://127.0.0.1:{server.actual_port}"
    data = _get(f"{base}/api/status")
    assert data["ok"] is True
    assert data["ws"]["state"] == "connected"
    assert data["identity"]["agent_id"] == "openclaw-test-1234"
    assert data["identity"]["device_id"] == "device-test-5678"
    assert data["process"]["pid"] > 0
    assert data["process"]["version"] == "0.1.0"
    assert data["bridge"]["active_job"] == "job-1"
    assert data["bridge"]["queue_size"] == 1
    # token 不暴露明文
    assert "access_token" not in json.dumps(data)


def test_jobs_endpoint(webui):
    server, state = webui
    base = f"http://127.0.0.1:{server.actual_port}"
    data = _get(f"{base}/api/jobs")
    assert data["total"] == 2
    assert data["acked"] == 1
    assert data["failed"] == 1
    assert len(data["recent"]) == 2
    assert data["recent"][0]["job_id"] == "job-2"  # 按时间倒序


def test_logs_endpoint(webui):
    server, state = webui
    base = f"http://127.0.0.1:{server.actual_port}"
    data = _get(f"{base}/api/logs")
    assert "lines" in data
    assert isinstance(data["lines"], list)


def test_index_html(webui):
    server, state = webui
    base = f"http://127.0.0.1:{server.actual_port}"
    with urllib.request.urlopen(f"{base}/", timeout=5) as resp:
        html = resp.read().decode()
    assert "Livis Bridge 看板" in html
    assert "api/status" in html


def test_404(webui):
    server, state = webui
    base = f"http://127.0.0.1:{server.actual_port}"
    import urllib.error
    with pytest.raises(urllib.error.HTTPError) as e:
        _get(f"{base}/api/nonexistent")
    assert e.value.code == 404


def test_auth_token_required(tmp_path):
    """带 token 时未授权返回 401。"""
    bridge = FakeBridge(tmp_path)
    state = WebUIState(bridge=bridge)
    server = WebUIServer(state=state, host="127.0.0.1", port=0, token="secret-token")
    assert server.start()
    try:
        base = f"http://127.0.0.1:{server.actual_port}"
        import urllib.error
        with pytest.raises(urllib.error.HTTPError) as e:
            _get(f"{base}/api/status")
        assert e.value.code == 401
        # 带正确 token 通过
        req = urllib.request.Request(f"{base}/api/status", headers={"Authorization": "Bearer secret-token"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        assert data["ok"] is True
    finally:
        server.stop()


def test_port_conflict_auto_increment(tmp_path):
    """端口冲突自动 +1。"""
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    occupied = s.getsockname()[1]
    # 保持 socket 占用（不 close），模拟端口被占

    bridge = FakeBridge(tmp_path)
    state = WebUIState(bridge=bridge)
    server = WebUIServer(state=state, host="127.0.0.1", port=occupied)
    assert server.start()
    try:
        assert server.actual_port == occupied + 1
    finally:
        server.stop()
        s.close()
