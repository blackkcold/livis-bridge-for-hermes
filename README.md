# Livis Bridge for Hermes — 理想 AI 眼镜 × Hermes Agent

> 让理想 AI 眼镜 Livis（「理想同学」App）通过理想官方中继服务器，远程驱动 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 完成问答与任务执行。

**这不是理想汽车的官方项目，也不是 Hermes 官方项目。** 这是一个社区逆向实现：协议层通过分析理想官方 openclaw 插件的行为重建，仅供个人学习与实验使用。

---

## ✨ 功能特性

- 🔌 **协议兼容**：完整实现 Livis PC Kit 的 WebSocket 中继协议（`connect` 握手、`send_message`/`exec`、`send_result`、心跳、token 自动刷新）
- 🤖 **驱动任何 agent**：默认对接 Hermes Agent（`hermes -z`），可替换为任意支持"单轮命令 → 文本输出"的程序
- 💾 **可靠投递**：SQLite outbox 模式——断线不丢结果，重连自动对账重发
- 🔄 **自动重连**：网络抖动/服务端断开自动重连，心跳超时检测
- 🔑 **标准认证**：OAuth2 Device Flow（浏览器手机号+短信登录），token 自动续期
- 🐳 **容器化**：Docker / docker-compose 一键开发测试，`fake_hermes.sh` + `mock_relay.py` 全离线测试

## 🏗️ 架构

```
┌────────────┐       ┌──────────────┐       ┌─────────────────────────┐
│ 理想AI眼镜   │ 语音   │ 理想同学 App  │ 云端  │ 理想 Livis 中继（官方）    │
│  (Livis)   │──────►│   (你手机)    │──────►│ wss://livis-pc-kit-     │
└────────────┘       └──────────────┘       │ gateway.livis.com/api/v1/ws
        ▲                                   └───────────┬─────────────┘
        │ 结果显示                                    │ exec / send_result
        └──────────────────────────────────────────────┘
                                                    │
                                        ┌───────────▼─────────────┐
                                        │     livis-bridge-for-hermes │
                                        │  protocol.py   WS+认证   │
                                        │  bridge.py     状态机/outbox│
                                        │  adapter.py    子进程驱动   │
                                        └─────────┬───────────────┘
                                                  │ hermes -z '<指令>'
                                        ┌─────────▼───────────────┐
                                        │  Hermes Agent (任意端)   │
                                        └─────────────────────────┘
```

**核心语义映射**：眼镜/App 的指令文本 → 中继 `exec` → `hermes -z` 子进程 → stdout 文本 → `send_result` 回传眼镜端。

## 🚀 快速开始

### 前置要求

- Python ≥ 3.11
- 理想 AI 眼镜 Livis + 手机「理想同学」App
- 本机已安装 Hermes Agent（`hermes` 命令可用），或任何可被 `hermes -z` 替代的程序

### 安装

```bash
git clone https://github.com/blackkcold/livis-bridge-for-hermes.git
cd livis-bridge-for-hermes
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

### 第一步：认证（浏览器 device flow）

```bash
.venv/bin/python -m src.cli device-flow
```

会打开浏览器 → 手机号 + 短信验证码登录 → 自动保存 token 到 `./data/`。

### 第二步：启动 bridge

```bash
.venv/bin/python -m src.cli server
```

bridge 连接中继后，会打印 Agent ID（如 `openclaw-xxxx-xxxx`）。

### 第三步：App 绑定（⚠️ 关键顺序）

> **必须先启动 bridge，再在 App 里绑定 Agent ID。** 离线状态下绑定会导致服务端路由目标失效，消息永远无法送达。

在「理想同学」App 的设备绑定页输入 bridge 打印的 Agent ID。

### 第四步：对话

对眼镜说（或 App 对话）"你好" → 回复来自 Hermes。

### 完整命令

| 命令 | 说明 |
|---|---|
| `python -m src.cli device-flow` | 首次认证（浏览器短信登录） |
| `python -m src.cli server` | 运行 bridge 主循环 |
| `python -m src.cli status` | 查看任务/outbox 状态 |
| `python -m src.cli run '<cmd>'` | 本地冒烟：直接用 Hermes 跑一条 |
| `python -m src.cli uninstall` | 清空本地数据（tokens/身份） |

## 🧪 开发与测试

```bash
# 容器内跑全套测试（mock 环境，零外部依赖）
docker build -t livis-bridge-for-hermes .
docker run --rm -v "$PWD/scripts:/app/scripts" \
  -e LIVIS_DATA_DIR=/tmp/test-data \
  -e HERMES_BIN=/app/scripts/fake_hermes.sh \
  livis-bridge-for-hermes python -m pytest tests/ -v

# 本地跑
.venv/bin/pip install -e ".[dev]"
HERMES_BIN=$PWD/scripts/fake_hermes.sh .venv/bin/python -m pytest tests/ -v
```

测试套件完全离线：`fake_hermes.sh` 模拟 Hermes 子进程，`mock_relay.py` 模拟理想中继，不消耗任何 API 请求。

## ⚠️ 免责声明

1. **非官方项目**：本项目的协议层基于对理想官方 openclaw 插件（`livis-pc-kit`）行为的外部观察重建，理想可能随时变更协议导致本项目失效。
2. **个人使用**：请仅用于个人学习与实验。未经理想官方授权，请勿用于商业用途或大规模分发。
3. **风险自担**：使用本项目涉及真实账号认证与远程控制能力，请妥善保管 token 文件（`data/` 目录）；因使用本项目产生的任何直接或间接损失，作者不承担责任。
4. **安全提醒**：`data/` 目录包含你的登录 token 与设备身份，**严禁提交到版本库、严禁分享**。部署到服务器时请确保文件权限（建议 `chmod 600`）。
5. **合规**：请遵守理想汽车服务条款及当地法律法规。如有疑问，请以理想官方渠道说明为准。

## 📄 版权与许可

- 本项目代码：MIT License（见 [LICENSE](LICENSE)）
- 本项目与理想汽车、Hermes Agent 无任何隶属关系；提及的产品名与商标归各自所有者所有
- 协议逆向仅用于互操作研究

## 🤝 贡献

欢迎提交 Issue 与 PR。请先阅读 [CONTRIBUTING.md](CONTRIBUTING.md)；发现安全相关问题请通过 [SECURITY.md](SECURITY.md) 的渠道私密报告。

## 📦 版本记录

- **v0.1.0**（2026-09-02）：首个可运行版本。完整协议实现（认证/握手/心跳/消息/outbox），本地 mock 全链路测试通过，真实设备端到端验证通过。

---

**相关链接**：[Hermes Agent](https://github.com/NousResearch/hermes-agent) · [理想汽车](https://www.lixiang.com)
