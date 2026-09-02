"""WebUI 状态服务（内嵌于 bridge 进程，独立线程）。

设计原则（不影响 bridge 主链路）:
  - 独立 daemon 线程 + 标准库 http.server，零新增依赖
  - 只读 bridge 状态（内存 + SQLite），不持有锁、不写任何状态
  - 任何异常只记日志，绝不抛出到主线程
  - 默认绑定 127.0.0.1，可选 token 鉴权（远程访问时）

接口:
  GET  /api/status      → 连接/身份/token/进程状态
  GET  /api/jobs       → 会话统计 + 最近记录
  GET  /api/logs       → 日志尾部（进程内环形缓冲）
  POST /api/reconnect  → 触发 WS 重连（可选）
  GET  /               → 看板页面
"""

from __future__ import annotations

import json
import logging
import os
import platform
import socket
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

log = logging.getLogger("livis.webui")

VERSION = "0.1.0"
LOG_BUFFER_MAX = 200


class LogBuffer(logging.Handler):
    """进程内日志环形缓冲（WebUI 读取，不碰日志文件）。

    增强:
      - enabled: 记录开关（关闭后不再接收新日志，但保留已有）
      - max_age: 自动清理（读取时过滤超过 max_age 秒的条目）
      - clear(): 清空缓冲
    """

    def __init__(self, maxlen: int = LOG_BUFFER_MAX, max_age: int = 3600):
        super().__init__()
        self.buffer: deque[tuple[float, str]] = deque(maxlen=maxlen)
        self.enabled = True
        self.max_age = max_age  # 秒；0 = 不过期

    def emit(self, record: logging.LogRecord) -> None:
        if not self.enabled:
            return
        try:
            self.buffer.append((time.time(), self.format(record)))
        except Exception:
            pass

    def lines(self, max_lines: int = 50) -> list[str]:
        """读取（自动清理过期条目）。"""
        now = time.time()
        if self.max_age > 0:
            # 从尾部保留未过期条目
            fresh = [t for t in self.buffer if now - t[0] <= self.max_age]
            if len(fresh) != len(self.buffer):
                self.buffer.clear()
                self.buffer.extend(fresh)
        return [text for _, text in list(self.buffer)[-max_lines:]]

    def clear(self) -> None:
        self.buffer.clear()

    def config(self) -> dict:
        return {"enabled": self.enabled, "max_age": self.max_age, "size": len(self.buffer)}


class WebUIState:
    """bridge 状态快照提供者（由 bridge 注入引用，只读）。"""

    def __init__(self, bridge=None, client=None, livis_cfg=None):
        self.bridge = bridge
        self.client = client
        self.livis_cfg = livis_cfg
        self.started_at = time.time()

    def snapshot(self) -> dict[str, Any]:
        """组装 /api/status 响应。任何异常都降级为部分数据。"""
        out: dict[str, Any] = {"ok": True, "ts": time.time()}
        # 连接状态
        ws_state = "disconnected"
        connected_at = None
        last_pong = None
        last_msg = None
        if self.client is not None:
            ws_state = "connected" if getattr(self.client, "_ws", None) is not None else "reconnecting"
            connected_at = getattr(self.client, "_connected_at", None)
            last_pong = getattr(self.client, "_last_pong_at", None) or None
            last_msg = getattr(self.client, "_last_msg_at", None) or None
        out["ws"] = {
            "state": ws_state,
            "connected_at": connected_at,
            "last_pong_at": last_pong,
            "last_msg_at": last_msg,
        }
        # 身份
        agent_id = getattr(self.client, "agent_id", None) if self.client else None
        device_id = getattr(self.client, "device_id", None) if self.client else None
        if not agent_id and self.livis_cfg is not None:
            from .protocol import load_agent_id
            agent_id = load_agent_id(self.livis_cfg)
        if not device_id and self.livis_cfg is not None:
            from .protocol import load_device_id
            device_id = load_device_id(self.livis_cfg)
        out["identity"] = {"agent_id": agent_id, "device_id": device_id}
        # token（只暴露过期时间，绝不暴露 token 本身）
        token_info: dict[str, Any] = {"expires_at": None, "expired": None}
        if self.livis_cfg is not None:
            try:
                from .protocol import load_tokens
                tokens = load_tokens(self.livis_cfg)
                if tokens:
                    token_info["expires_at"] = tokens.obtained_at + tokens.expires_in
                    token_info["expired"] = tokens.expired
            except Exception:
                pass
        out["token"] = token_info
        # 进程
        out["process"] = {
            "pid": os.getpid(),
            "uptime_s": int(time.time() - self.started_at),
            "version": VERSION,
            "python": platform.python_version(),
            "host": socket.gethostname(),
        }
        # bridge 内部
        active_job = None
        queue_size = 0
        if self.bridge is not None:
            active_job = getattr(self.bridge, "_active_job", None)
            queue_size = getattr(self.bridge, "_queue", None).qsize() if getattr(self.bridge, "_queue", None) else 0
        out["bridge"] = {"active_job": active_job, "queue_size": queue_size}
        return out

    def jobs(self, limit: int = 10, offset: int = 0) -> dict[str, Any]:
        """会话统计 + 最近记录（独立只读连接，避免跨线程复用 bridge 连接）。

        limit: 每页条数（默认 10，前端默认取 5）
        offset: 分页偏移（加载更多用）
        """
        out: dict[str, Any] = {
            "total": 0, "executing": 0, "result_pending": 0, "acked": 0, "failed": 0,
            "recent": [], "offset": offset, "limit": limit, "has_more": False,
        }
        if self.bridge is None:
            return out
        import sqlite3

        db_path = getattr(self.bridge, "cfg", None).db_path if getattr(self.bridge, "cfg", None) else None
        if db_path is None:
            return out
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            try:
                for row in conn.execute("SELECT state, COUNT(*) c FROM jobs GROUP BY state"):
                    key = row["state"]
                    if key in out:
                        out[key] = row["c"]
                out["total"] = sum(out[k] for k in ("executing", "result_pending", "acked", "failed"))
                limit = max(1, min(limit, 50))
                offset = max(0, offset)
                rows = conn.execute(
                    "SELECT job_id, content, state, result, error, created_at, updated_at FROM jobs ORDER BY created_at DESC LIMIT ? OFFSET ?",
                    (limit + 1, offset),  # 多取 1 条判断 has_more
                ).fetchall()
                has_more = len(rows) > limit
                rows = rows[:limit]
                out["has_more"] = has_more
                out["recent"] = [
                    {
                        "job_id": r["job_id"],
                        "content": (r["content"] or "")[:60],
                        "state": r["state"],
                        "result": (r["result"] or "")[:80],
                        "error": (r["error"] or "")[:80],
                        "created_at": r["created_at"],
                        "updated_at": r["updated_at"],
                    }
                    for r in rows
                ]
            finally:
                conn.close()
        except Exception as e:
            log.warning("jobs 查询失败: %s", e)
        return out


class WebUIHandler(BaseHTTPRequestHandler):
    """HTTP 请求处理。线程安全：每请求独立线程，只读状态。"""

    state: WebUIState | None = None  # 类级注入
    auth_token: str | None = None
    log_buffer: LogBuffer | None = None
    reconnect_cb = None

    # ── 工具 ──────────────────────────────────────────────────────

    def log_message(self, fmt: str, *args) -> None:
        # 静默访问日志（避免刷屏）
        pass

    def _send_json(self, data: dict, code: int = 200) -> None:
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self) -> bool:
        if not self.auth_token:
            return True
        header = self.headers.get("Authorization", "")
        return header == f"Bearer {self.auth_token}"

    # ── 路由 ───────────────────────────────────────────────────────

    def do_GET(self) -> None:  # noqa: N802
        if not self._check_auth():
            self._send_json({"error": "unauthorized"}, 401)
            return
        path = self.path.split("?")[0]
        if path == "/api/status":
            self._send_json(self.state.snapshot() if self.state else {"ok": False})
        elif path == "/api/jobs":
            limit, offset = 10, 0
            try:
                qs = self.path.split("?", 1)[1]
                params = dict(p.split("=") for p in qs.split("&") if "=" in p)
                limit = int(params.get("limit", "10"))
                offset = int(params.get("offset", "0"))
            except Exception:
                pass
            self._send_json(self.state.jobs(limit, offset) if self.state else {"ok": False})
        elif path == "/api/logs":
            lines = self.log_buffer.lines(50) if self.log_buffer else []
            cfg = self.log_buffer.config() if self.log_buffer else {}
            self._send_json({"lines": lines, "config": cfg})
        elif path in ("/", "/index.html"):
            self._send_html(INDEX_HTML)
        else:
            self._send_json({"error": "not found"}, 404)

    def do_POST(self) -> None:  # noqa: N802
        if not self._check_auth():
            self._send_json({"error": "unauthorized"}, 401)
            return
        path = self.path.split("?")[0]
        if path == "/api/reconnect" and self.reconnect_cb:
            try:
                self.reconnect_cb()
                self._send_json({"ok": True, "msg": "reconnect 已触发"})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, 500)
        elif path == "/api/logs/clear" and self.log_buffer:
            self.log_buffer.clear()
            self._send_json({"ok": True, "msg": "日志已清空"})
        elif path == "/api/logs/config" and self.log_buffer:
            try:
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length).decode() or "{}") if length else {}
                if "enabled" in body:
                    self.log_buffer.enabled = bool(body["enabled"])
                if "max_age" in body:
                    self.log_buffer.max_age = max(0, int(body["max_age"]))
                self._send_json({"ok": True, "config": self.log_buffer.config()})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, 400)
        else:
            self._send_json({"error": "not found"}, 404)


class WebUIServer:
    """WebUI 服务（独立 daemon 线程）。"""

    def __init__(
        self,
        state: WebUIState,
        host: str = "127.0.0.1",
        port: int = 8766,
        token: str | None = None,
        log_buffer: LogBuffer | None = None,
        reconnect_cb=None,
    ):
        self.state = state
        self.host = host
        self.port = port
        self.token = token
        self.log_buffer = log_buffer
        self.reconnect_cb = reconnect_cb
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.actual_port: int | None = None

    def start(self) -> bool:
        """启动服务。端口被占时自动尝试 +1（最多 10 次）。"""
        for attempt in range(10):
            port = self.port + attempt
            try:
                self._server = ThreadingHTTPServer((self.host, port), WebUIHandler)
                # port=0 时取实际分配端口
                self.actual_port = self._server.server_address[1]
                break
            except OSError as e:
                log.warning("端口 %d 被占用: %s", port, e)
        if self._server is None:
            log.error("WebUI 无法绑定端口（尝试 10 次失败），跳过启动")
            return False

        WebUIHandler.state = self.state
        WebUIHandler.auth_token = self.token
        WebUIHandler.log_buffer = self.log_buffer
        WebUIHandler.reconnect_cb = self.reconnect_cb

        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True, name="webui")
        self._thread.start()
        log.info("WebUI 看板已启动: http://%s:%d", self.host, self.actual_port)
        return True

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None


# ── 看板页面（单文件，深色主题，3s 轮询）────────────────────────────

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Livis Bridge 看板</title>
<style>
:root {
  --bg: #0d1117; --card: #161b22; --border: #30363d;
  --fg: #e6edf3; --muted: #8b949e; --accent: #58a6ff;
  --green: #3fb950; --yellow: #d29922; --red: #f85149;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { background: var(--bg); color: var(--fg); font: 14px/1.5 -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif; padding: 20px; }
h1 { font-size: 18px; margin-bottom: 4px; }
.sub { color: var(--muted); font-size: 12px; margin-bottom: 16px; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }
.card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.card h2 { font-size: 13px; color: var(--muted); margin-bottom: 10px; font-weight: 600; }
.row { display: flex; justify-content: space-between; padding: 3px 0; font-size: 13px; }
.row .k { color: var(--muted); }
.row .v { font-family: ui-monospace, Menlo, monospace; font-size: 12px; }
.dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
.dot.green { background: var(--green); box-shadow: 0 0 6px var(--green); }
.dot.yellow { background: var(--yellow); box-shadow: 0 0 6px var(--yellow); }
.dot.red { background: var(--red); box-shadow: 0 0 6px var(--red); }
.bar { display: flex; height: 8px; border-radius: 4px; overflow: hidden; margin: 8px 0; }
.bar div { transition: width .4s; }
.bar .exec { background: var(--accent); }
.bar .pend { background: var(--yellow); }
.bar .acked { background: var(--green); }
.bar .fail { background: var(--red); }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
th, td { text-align: left; padding: 4px 6px; border-bottom: 1px solid var(--border); }
th { color: var(--muted); font-weight: 500; }
td.mono { font-family: ui-monospace, Menlo, monospace; font-size: 11px; }
.state-badge { padding: 1px 8px; border-radius: 10px; font-size: 11px; }
.state-badge.acked { background: rgba(63,185,80,.15); color: var(--green); }
.state-badge.executing { background: rgba(88,166,255,.15); color: var(--accent); }
.state-badge.result_pending { background: rgba(210,153,34,.15); color: var(--yellow); }
.state-badge.failed { background: rgba(248,81,73,.15); color: var(--red); }
.logs { background: #010409; border: 1px solid var(--border); border-radius: 6px; padding: 8px; font: 11px/1.6 ui-monospace, Menlo, monospace; max-height: 220px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; }
.btn { background: var(--card); color: var(--fg); border: 1px solid var(--border); border-radius: 6px; padding: 4px 12px; cursor: pointer; font-size: 12px; }
.btn:hover { border-color: var(--accent); }
.btn:disabled { opacity: .5; cursor: not-allowed; }
#conn-banner { display: flex; align-items: center; gap: 8px; padding: 8px 12px; border-radius: 6px; margin-bottom: 12px; font-size: 13px; }
#conn-banner.connected { background: rgba(63,185,80,.1); border: 1px solid rgba(63,185,80,.3); }
#conn-banner.reconnecting { background: rgba(210,153,34,.1); border: 1px solid rgba(210,153,34,.3); }
#conn-banner.disconnected { background: rgba(248,81,73,.1); border: 1px solid rgba(248,81,73,.3); }
</style>
</head>
<body>
<h1>🦞 Livis Bridge 看板</h1>
<div class="sub" id="sub">加载中…</div>

<div id="conn-banner"><span class="dot" id="banner-dot"></span><span id="banner-text">连接中…</span></div>

<div class="grid">
  <div class="card">
    <h2>🔌 连接状态</h2>
    <div class="row"><span class="k">WS 状态</span><span class="v" id="ws-state">-</span></div>
    <div class="row"><span class="k">连接时长</span><span class="v" id="ws-uptime">-</span></div>
    <div class="row"><span class="k">最后心跳</span><span class="v" id="ws-pong">-</span></div>
    <div class="row"><span class="k">最后消息</span><span class="v" id="ws-msg">-</span></div>
  </div>
  <div class="card">
    <h2>🪪 在线身份</h2>
    <div class="row"><span class="k">Agent ID</span><span class="v" id="id-agent">-</span></div>
    <div class="row"><span class="k">Device ID</span><span class="v" id="id-device">-</span></div>
    <div class="row"><span class="k">Token 过期</span><span class="v" id="id-token">-</span></div>
    <div class="row"><span class="k">当前任务</span><span class="v" id="id-active">-</span></div>
  </div>
  <div class="card">
    <h2>📊 会话统计</h2>
    <div class="row"><span class="k">总任务</span><span class="v" id="jobs-total">-</span></div>
    <div class="bar" id="jobs-bar"></div>
    <div class="row"><span class="k">执行中 / 待回传 / 完成 / 失败</span><span class="v" id="jobs-counts">-</span></div>
  </div>
  <div class="card">
    <h2>⚙️ 运行情况</h2>
    <div class="row"><span class="k">进程 PID</span><span class="v" id="proc-pid">-</span></div>
    <div class="row"><span class="k">运行时长</span><span class="v" id="proc-uptime">-</span></div>
    <div class="row"><span class="k">版本 / Python</span><span class="v" id="proc-ver">-</span></div>
    <div class="row"><span class="k">主机</span><span class="v" id="proc-host">-</span></div>
  </div>
</div>

<div class="card" style="margin-top:12px">
  <h2>🕘 最近任务 <button class="btn" id="btn-load-more" style="float:right;display:none">加载更多</button></h2>
  <table>
    <thead><tr><th>时间</th><th>内容</th><th>状态</th><th>结果/错误</th></tr></thead>
    <tbody id="jobs-recent"><tr><td colspan="4" style="color:var(--muted)">暂无数据</td></tr></tbody>
  </table>
</div>

<div class="card" style="margin-top:12px">
  <h2>📜 日志尾部
    <span style="float:right">
      <label style="font-size:12px;color:var(--muted);margin-right:8px"><input type="checkbox" id="log-enabled" checked> 记录</label>
      <button class="btn" id="btn-log-clear" style="margin-right:4px">🗑 清空</button>
      <button class="btn" id="btn-reconnect">🔄 触发重连</button>
    </span>
  </h2>
  <div style="font-size:11px;color:var(--muted);margin-bottom:6px" id="log-config">-</div>
  <div class="logs" id="logs">加载中…</div>
</div>

<script>
const $ = id => document.getElementById(id);
const fmtTime = ts => ts ? new Date(ts * 1000).toLocaleTimeString('zh-CN', {hour12:false}) : '-';
const fmtDur = s => { if (s == null) return '-'; s = Math.floor(s); return s < 60 ? s + 's' : Math.floor(s/60) + 'm' + (s%60) + 's'; };
const fmtAgo = ts => { if (!ts) return '-'; const d = Date.now()/1000 - ts; return d < 5 ? '刚刚' : d < 60 ? Math.floor(d)+'s 前' : d < 3600 ? Math.floor(d/60)+'m 前' : Math.floor(d/3600)+'h 前'; };

function renderStatus(s) {
  const ws = s.ws || {};
  const state = ws.state || 'disconnected';
  const banner = $('conn-banner');
  banner.className = state;
  $('banner-dot').className = 'dot ' + (state === 'connected' ? 'green' : state === 'reconnecting' ? 'yellow' : 'red');
  $('banner-text').textContent = state === 'connected' ? '已连接中继' : state === 'reconnecting' ? '重连中…' : '已断开';
  $('ws-state').textContent = state;
  $('ws-uptime').textContent = ws.connected_at ? fmtDur(Date.now()/1000 - ws.connected_at) : '-';
  $('ws-pong').textContent = fmtAgo(ws.last_pong_at);
  $('ws-msg').textContent = fmtAgo(ws.last_msg_at);
  const id = s.identity || {};
  $('id-agent').textContent = id.agent_id || '-';
  $('id-device').textContent = id.device_id || '-';
  const tok = s.token || {};
  $('id-token').textContent = tok.expires_at ? fmtTime(tok.expires_at) + (tok.expired ? ' ⚠️' : '') : '-';
  $('id-active').textContent = (s.bridge && s.bridge.active_job) ? String(s.bridge.active_job).slice(0, 20) : '无';
  const p = s.process || {};
  $('proc-pid').textContent = p.pid || '-';
  $('proc-uptime').textContent = fmtDur(p.uptime_s);
  $('proc-ver').textContent = (p.version || '-') + ' / ' + (p.python || '-');
  $('proc-host').textContent = p.host || '-';
  $('sub').textContent = 'agent: ' + (id.agent_id || '?') + ' · 刷新于 ' + new Date().toLocaleTimeString('zh-CN', {hour12:false});
}

let jobsOffset = 0;
const JOBS_PAGE = 5;

function renderJobs(j, append) {
  $('jobs-total').textContent = j.total;
  $('jobs-counts').textContent = j.executing + ' / ' + j.result_pending + ' / ' + j.acked + ' / ' + j.failed;
  const bar = $('jobs-bar');
  bar.innerHTML = '';
  const parts = [['exec', j.executing], ['pend', j.result_pending], ['acked', j.acked], ['fail', j.failed]];
  for (const [cls, n] of parts) {
    if (n > 0 && j.total > 0) {
      const d = document.createElement('div');
      d.className = cls;
      d.style.width = (n / j.total * 100) + '%';
      bar.appendChild(d);
    }
  }
  const tbody = $('jobs-recent');
  if (!append) tbody.innerHTML = '';
  if (!j.recent || !j.recent.length) {
    if (!append) tbody.innerHTML = '<tr><td colspan="4" style="color:var(--muted)">暂无数据</td></tr>';
  }
  for (const r of j.recent) {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td class="mono">' + fmtTime(r.created_at) + '</td>'
      + '<td>' + (r.content || '').replace(/</g, '&lt;') + '</td>'
      + '<td><span class="state-badge ' + r.state + '">' + r.state + '</span></td>'
      + '<td class="mono">' + (r.result || r.error || '').replace(/</g, '&lt;').slice(0, 60) + '</td>';
    tbody.appendChild(tr);
  }
  // 加载更多按钮
  const btn = $('btn-load-more');
  if (j.has_more) {
    btn.style.display = 'inline-block';
  } else {
    btn.style.display = 'none';
  }
}

async function loadJobs(append) {
  try {
    const r = await fetch('/api/jobs?limit=' + JOBS_PAGE + '&offset=' + jobsOffset).then(r => r.json());
    renderJobs(r, append);
    jobsOffset += r.recent.length;
  } catch (e) {
    console.error('jobs 加载失败', e);
  }
}

function renderLogs(l) {
  $('logs').textContent = (l.lines || []).join('\n') || '(空)';
  const el = $('logs');
  el.scrollTop = el.scrollHeight;
  const cfg = l.config || {};
  $('log-config').textContent = '缓冲 ' + (cfg.size ?? '-') + ' 条 · 保留 ' + (cfg.max_age ? cfg.max_age + 's' : '永久') + (cfg.enabled ? '' : ' · 已暂停记录');
  $('log-enabled').checked = cfg.enabled !== false;
}

async function refresh() {
  try {
    const [s, l] = await Promise.all([
      fetch('/api/status').then(r => r.json()),
      fetch('/api/logs').then(r => r.json()),
    ]);
    renderStatus(s); renderLogs(l);
  } catch (e) {
    $('banner-text').textContent = '看板数据获取失败: ' + e;
  }
}

$('btn-load-more').addEventListener('click', () => loadJobs(true));

$('btn-log-clear').addEventListener('click', async () => {
  try {
    await fetch('/api/logs/clear', {method: 'POST'});
    $('logs').textContent = '(已清空)';
  } catch (e) {
    $('logs').textContent = '清空失败: ' + e;
  }
});

$('log-enabled').addEventListener('change', async (e) => {
  try {
    const r = await fetch('/api/logs/config', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({enabled: e.target.checked}),
    });
    const d = await r.json();
    if (d.config) $('log-config').textContent = '缓冲 ' + d.config.size + ' 条 · 保留 ' + (d.config.max_age ? d.config.max_age + 's' : '永久') + (d.config.enabled ? '' : ' · 已暂停记录');
  } catch (err) {
    console.error('日志开关失败', err);
  }
});

$('btn-reconnect').addEventListener('click', async () => {
  const btn = $('btn-reconnect');
  btn.disabled = true;
  try {
    const r = await fetch('/api/reconnect', {method: 'POST'});
    const d = await r.json();
    $('logs').textContent = '→ ' + (d.msg || JSON.stringify(d)) + '\n' + $('logs').textContent;
  } catch (e) {
    $('logs').textContent = '→ 重连触发失败: ' + e + '\n' + $('logs').textContent;
  }
  btn.disabled = false;
});

refresh();
loadJobs(false);
setInterval(refresh, 3000);
</script>
</body>
</html>
"""
