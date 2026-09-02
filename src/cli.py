"""Livis Bridge CLI。

用法:
  python -m src.cli device-flow          # 首次认证（OAuth2 device flow）
  python -m src.cli run '<cmd>'          # 本地冒烟：走 bridge 执行一条指令（连 mock 或真中继）
  python -m src.cli server               # 启动 bridge（连 WS 中继，循环监听）
  python -m src.cli status               # 查看 bridge.db 状态
  python -m src.cli uninstall            # 清空数据（tokens/agent.id/db）
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .adapter import HermesAdapter
from .bridge import Bridge, BridgeConfig
from .protocol import (
    LivisConfig,
    LivisWSClient,
    ensure_token,
    load_agent_id,
    make_agent_id,
    run_device_flow,
)
from .bridge import STATE_RESULT_PENDING

log = logging.getLogger("livis.cli")


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


# ── device-flow ───────────────────────────────────────────────────────

async def cmd_device_flow(livis_cfg: LivisConfig, verbose: bool) -> int:
    _setup_logging(verbose)
    await run_device_flow(livis_cfg)

    # 生成 agentId（若尚无）
    agent_id = load_agent_id(livis_cfg) or make_agent_id()
    if not load_agent_id(livis_cfg):
        from .protocol import save_agent_id
        save_agent_id(livis_cfg, agent_id)
    print(f"\nAgent ID: {agent_id}")
    print("请在手机「理想同学」App 的绑定页输入上述 Agent ID 完成绑定。")
    print("（认证与绑定相互独立：认证走浏览器手机号+短信，绑定走 App。）")
    return 0


# ── run（本地冒烟）───────────────────────────────────────────────────

async def cmd_run(content: str, livis_cfg: LivisConfig, verbose: bool) -> int:
    _setup_logging(verbose)
    cfg = BridgeConfig.from_env()
    bridge = Bridge(cfg, livis_cfg, HermesAdapter(bin=cfg.hermes_bin, timeout=cfg.hermes_timeout))
    await bridge.start()
    try:
        import uuid as _uuid
        job_id = f"local-{_uuid.uuid4().hex[:8]}"
        bridge._insert_job(job_id, content, {"type": "exec", "metadata": {"job_id": job_id}, "payload": {"content": content}})
        await bridge._execute_job(job_id)
        job = bridge._job(job_id)
        if job and job["state"] == STATE_RESULT_PENDING:
            print(job["result"])
            return 0
        print(f"[失败] state={job['state'] if job else '?'} error={job['error'] if job else '?'}")
        return 1
    finally:
        await bridge.stop()


# ── server（主循环）───────────────────────────────────────────────────

async def cmd_server(livis_cfg: LivisConfig, verbose: bool, webui_port: int = 8766, webui_host: str = "127.0.0.1", webui_token: str | None = None) -> int:
    _setup_logging(verbose)
    cfg = BridgeConfig.from_env()
    adapter = HermesAdapter(bin=cfg.hermes_bin, timeout=cfg.hermes_timeout)
    bridge = Bridge(cfg, livis_cfg, adapter)

    # ── WebUI（独立线程，异常不影响主链路）────────────────────────
    from .webui import LogBuffer, WebUIServer, WebUIState

    log_buffer = LogBuffer()
    logging.getLogger().addHandler(log_buffer)
    webui_state = WebUIState(bridge=bridge, livis_cfg=livis_cfg)
    webui = WebUIServer(
        state=webui_state,
        host=webui_host,
        port=webui_port,
        token=webui_token,
        log_buffer=log_buffer,
    )
    webui.start()

    while True:
        try:
            tokens = await ensure_token(livis_cfg)
            client = LivisWSClient(livis_cfg, bridge.on_message, tokens=tokens)
            await client.connect()
            await bridge.start()
            bridge.set_client(client)  # 注入 WS client，结果才能发出
            webui_state.client = client  # WebUI 读取连接状态
            await bridge.on_reconnect(client)  # 启动即对账一次
            log.info("bridge 运行中 (agent=%s)", client.agent_id)
            try:
                await client.recv_loop()
            finally:
                bridge.set_client(None)
                webui_state.client = None
                await client.close()
        except asyncio.CancelledError:
            log.info("bridge 停止")
            await bridge.stop()
            webui.stop()
            return 0
        except Exception as e:
            log.error("连接异常: %s — 5s 后重连", e)
            await asyncio.sleep(5)


# ── status ────────────────────────────────────────────────────────────

async def cmd_status(livis_cfg: LivisConfig, verbose: bool) -> int:
    _setup_logging(verbose)
    cfg = BridgeConfig.from_env()
    bridge = Bridge(cfg, livis_cfg, HermesAdapter(bin=cfg.hermes_bin))
    try:
        st = bridge.status()
        print(f"active_job: {st['active_job']}")
        print(f"counts:      executing={st['counts']['executing']} pending={st['counts']['result_pending']} acked={st['counts']['acked']} failed={st['counts']['failed']}")
        if st["pending"]:
            print("pending results:")
            for p in st["pending"]:
                print(f"  {p['job_id']}: {p['result']}")
        return 0
    finally:
        await bridge.stop()


# ── uninstall ─────────────────────────────────────────────────────────

async def cmd_uninstall(livis_cfg: LivisConfig, verbose: bool) -> int:
    _setup_logging(verbose)
    import shutil

    data_dir = livis_cfg.data_dir
    if not data_dir.exists():
        print("无数据目录，无需卸载")
        return 0
    for f in data_dir.iterdir():
        if f.name in ("livis-pc-kit-tokens.json", "livis-agent.id", "bridge.db", "bridge.db-wal", "bridge.db-shm"):
            f.unlink(missing_ok=True)
            print(f"已删除: {f}")
    print("卸载完成（数据文件已清空）")
    return 0


# ── main ──────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(prog="livis-bridge", description="Livis ↔ Hermes bridge")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("device-flow", help="OAuth2 device flow 认证")
    sub.add_parser("status", help="查看状态")
    sub.add_parser("uninstall", help="清空本地数据")

    p_run = sub.add_parser("run", help="本地冒烟执行")
    p_run.add_argument("content", help="指令文本")

    p_server = sub.add_parser("server", help="运行 bridge 主循环")
    p_server.add_argument("--ws-url", default=None, help="覆盖 WS URL（默认连真实中继）")
    p_server.add_argument("--webui-port", type=int, default=8766, help="WebUI 端口（默认 8766，冲突自动 +1）")
    p_server.add_argument("--webui-host", default="127.0.0.1", help="WebUI 绑定地址（默认仅本机）")
    p_server.add_argument("--webui-token", default=None, help="WebUI 访问 token（远程访问时必填）")

    args = parser.parse_args()
    livis_cfg = LivisConfig.from_env()
    if getattr(args, "ws_url", None):
        livis_cfg.ws_url = args.ws_url

    if args.cmd == "device-flow":
        return asyncio.run(cmd_device_flow(livis_cfg, args.verbose))
    if args.cmd == "run":
        return asyncio.run(cmd_run(args.content, livis_cfg, args.verbose))
    if args.cmd == "server":
        return asyncio.run(cmd_server(
            livis_cfg, args.verbose,
            webui_port=args.webui_port, webui_host=args.webui_host, webui_token=args.webui_token,
        ))
    if args.cmd == "status":
        return asyncio.run(cmd_status(livis_cfg, args.verbose))
    if args.cmd == "uninstall":
        return asyncio.run(cmd_uninstall(livis_cfg, args.verbose))
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
