# Livis ↔ Hermes Bridge 完整方案

> 目标：让理想 AI 眼镜 Livis（手机"理想同学"App）通过理想中继服务器，远程驱动本机/远端 Hermes Agent。
> 状态：阶段 0（协议逆向 + Hermes CLI 验证）已完成 ✅

---

## 0. 架构总览

```
眼镜语音 ──► 理想同学 App ──► wss://livis-pc-kit-gateway.livis.com/api/v1/ws
                                    │  exec{content} / send_result{data}
                              ┌─────▼─────┐
                              │  bridge   │  (Python, 容器内运行)
                              │  状态机    │
                              │  SQLite   │── outbox（断线不丢结果）
                              └─────┬─────┘
                                    │  adapter.spawn(content)
                              ┌─────▼─────┐
                              │ hermes -z │  (宿主机或远端, 子进程)
                              └─────┬─────┘
                                    │  stdout + --usage-file
                              ┌─────▼─────┐
                              │ send_result│──► ack_send_result ──► outbox 清账
                              └──────────┘
```

**核心语义映射**（已从官方插件逆向确认）：

```
exec.content ──► hermes -z '<content>' ──► stdout ──► send_result.payload.data
cancel_chat   ──► proc.kill()（无优雅信号，硬杀可接受）
```

---

## 1. 协议逆向成果（阶段 0，已完成 ✅）

来源：官方插件包 `release-2.0.0-7287b4fc.tar.gz`（v2.0.0），从 CDN 拉取拆解。

### 1.1 认证（标准 OAuth2 Device Flow，RFC 8628）

| 参数 | 值 |
|---|---|
| 认证端点 | `https://id.lixiang.com/api` |
| client_id | `6qxd1MLZhAtdWipnmXe1dd` |
| appAudience | `rZgT0SETDNueMVAhfRN10` |
| appScope | `super` + `offline_access` |
| 流程 | `requestDeviceCode → poll → grant_type=urn:ietf:params:oauth:grant-type:device_code` |
| Token 存储 | 本地 JSON（`livis-pc-kit-tokens.json`），含 refresh_token |

### 1.2 WebSocket 中继

| 参数 | 值 |
|---|---|
| WS URL | `wss://livis-pc-kit-gateway.livis.com/api/v1/ws`（见 `protocol.py` 常量） |

### 1.3 认证与绑定（两个独立环节）

> ⚠️ 官方插件源码确认：**认证不走 App**。`loginWithDeviceFlow()` 直接打开浏览器访问 `verification_uri_complete`，用**手机号+短信验证码**登录理想 IDaaS。App 里只有 agentId 绑定入口，没有授权场景。

| 环节 | 方式 | 产出 |
|---|---|---|
| 认证 | 浏览器打开 verification_uri → 手机号+短信 | `access_token` + `refresh_token` |
| 绑定 | App「设备绑定页」输入 agentId | 服务端绑定 agent↔账号 |

### 1.4 消息类型

| 方向 | 类型 | 说明 |
|---|---|---|
| 服务端→客户端 | `connected` | WS 握手成功 |
| 服务端→客户端 | `send_message` | 文本消息（含 job_id）→ 回 `ack_send_message` |
| 服务端→客户端 | `exec` | **执行指令**，`payload.content` 即 agent 指令 |
| 服务端→客户端 | `cancel_chat` | 取消执行 |
| 客户端→服务端 | `send_result` | 执行结果，`payload.data` 文本 |
| 客户端→服务端 | `ack_send_message` / `ack_send_result` | 回执，清 outbox |
| 双向 | `ping` / `pong` | 心跳 |

### 1.4 消息结构

```json
{
  "type": "exec",
  "metadata": {
    "msg_id": "<uuid>",
    "job_id": "<uuid>",
    "agent_id": "<openclaw-xxx>",
    "device_id": "<uuid>",
    "timestamp": 1750000000000
  },
  "payload": { "content": "用户指令文本" }
}
```

### 1.5 绑定

- agentId 格式：`openclaw-<uuid>`（写入 `~/.openclaw/livis-agent.id`）
- **⚠️ 最大不确定性**：App 绑定页是否强制校验 `openclaw-` 前缀（阶段 3 第一件事验证；若强校验需确认服务端是否只查前缀）

---

## 2. Hermes 侧验证（阶段 0，已完成 ✅）

| 假设 | 实测结果 |
|---|---|
| `hermes -z '<cmd>'` 单轮执行 | ✅ 15.7s 返回，exit 0，stdout 纯净（"收到"） |
| `--usage-file PATH` | ✅ JSON：tokens/cost/session_id/completed |
| 取消语义 | ⚠️ 无优雅信号 → `cancel_chat` 用 `proc.kill()` 硬杀 |
| 版本 | v0.20.5 (git install) |

**adapter 规则**：
- 只读 stdout 文本；`--usage-file` 仅用于统计/调试（可留可去）
- 超时（如 10min）→ kill + 报错结果
- 并发：同一时刻只跑一个 job（串行队列），job_id 为键

---

## 3. 容器化策略

**Docker Desktop 已装（v29.2.1, arm64），daemon 就绪。**

| 测试项 | 容器内 | 宿主机 |
|---|---|---|
| protocol 单测（mock WS） | ✅ | — |
| bridge 状态机 / outbox / 重连对账 | ✅ | — |
| adapter 单测（fake hermes 脚本） | ✅ | — |
| mock 全链路（mock-relay + fake hermes） | ✅ | — |
| adapter 集成（真 `hermes -z`） | ⚠️ 可选 | ✅ 冒烟 1-2 次 |
| 真实 WS + 账号绑定（阶段 3） | ✅ | 等价 |
| 端到端（眼镜 → Hermes） | ✅ | 等价 |

**核心设计**：容器内置 `fake_hermes.sh`（模拟 stdout/退出码/延迟），自动测试零 API 消耗、离线可跑。真 Hermes 集成测试仅在宿主机冒烟（避免每次 -z 烧真实 API 请求）。

---

## 4. 文件结构

```
livis-bridge/
├── PLAN.md                    # 本文档
├── Dockerfile                 # python:3.11-slim + websockets/httpx/sqlite3
├── docker-compose.yml         # 服务: bridge + mock-relay（测试用）
├── pyproject.toml
├── src/
│   ├── __init__.py
│   ├── protocol.py            # device flow + WS 编解码 + 心跳
│   ├── adapter.py             # hermes -z 子进程驱动（spawn/stdout/kill/usage）
│   ├── bridge.py              # 状态机 + SQLite outbox + 重连对账
│   └── cli.py                 # run / status / uninstall
├── tests/
│   ├── test_protocol.py       # 编解码 + device flow (mock HTTP)
│   ├── test_adapter.py        # fake_hermes 子进程
│   ├── test_bridge.py         # 状态机 + outbox 幂等 + 断线重连
│   └── test_e2e_mock.py       # mock_relay 全链路
├── scripts/
│   ├── mock_relay.py          # 模拟理想中继（exec 下发 + ack 回执 + 断线注入）
│   └── fake_hermes.sh         # 模拟 hermes -z
└── data/                      # 运行时: tokens.json / bridge.db / agent.id
```

---

## 5. 阶段计划与门禁

### 阶段 1：本地骨架（纯容器，无真实账号）

1. **1a 容器骨架**：Dockerfile / compose / pyproject / 目录结构
2. **1b protocol.py**：device flow 认证 + WS 消息编解码 + 心跳
3. **1c adapter.py**：`hermes -z` 子进程驱动（spawn / stdout / kill / usage-file）
4. **1d bridge.py**：状态机 `idle → executing → result → ack`；SQLite outbox；重连对账；串行队列
5. **1e cli.py**：`run '<cmd>'`（本地冒烟链路）/ `status` / `uninstall`

**产出**：容器内 `pytest` 全绿；host 上 `cli run` 冒烟过一次真 Hermes。

### 阶段 2：mock 全链路（容器内，门禁 G1）

- `mock_relay.py` 模拟中继：exec 下发、ack 回执、断线注入、延迟注入
- 演练：mock-relay → bridge → adapter(fake_hermes) → send_result → ack → outbox 清账
- 专项：断线重连对账、outbox 幂等、取消、WAL 崩溃恢复

**门禁 G1**：全绿 → 才允许碰真实账号。

### 阶段 3：真实账号绑定（需用户，门禁 G2）

1. 跑 device flow → 手机理想同学 App 确认授权
2. 生成 agentId → App 绑定页输入
3. **⚠️ 第一验证**：App 是否强制 `openclaw-` 前缀

**门禁 G2**：绑定成功。

### 阶段 4：真实端到端（门禁 G3）

1. 连真实 WS，验证 `connected` 握手 + token 自动刷新
2. 眼镜语音 → App → 中继 → bridge → Hermes → 结果回传 → 眼镜显示
3. **验证眼镜端显示格式**（markdown? 纯文本? 长度限制?）→ 决定是否加结果清洗层
4. 往返成功后**冻结 protocol.py**

**门禁 G3**：往返成功。

### 阶段 5：部署守护

- 部署目标：本地 Mac（launchd）或远端 Linux 服务器（systemd）二选一
- 日志轮转、崩溃自启、token 自动刷新
- **协议变更检测**：WS 握手失败 / 消息格式异常 → 告警（防理想改协议）

### 阶段 6：收尾

- 使用文档 + 卸载脚本
- 沉淀 skill（逆向参数、device flow 流程、坑位）

---

## 5.5 部署经验（2026-09-03 实测沉淀）

### 远端部署（43.129.241.95）

- systemd 服务 `livis-bridge`，`Restart=always`，日志 append 到 `data/bridge.log`
- wrapper `scripts/hermes-livis.sh`：`exec hermes -p livis "$@"`（profile 隔离）
- **⚠️ 部署后必须清理手动启动的残留实例**：`ps -ef | grep 'src.cli'`，双实例同连一 agentId → 服务端广播 → 双进程双会话双 token（症状：state.db 每任务 2 会话、token 翻倍、延迟升高）

### Profile 优化（livis）

- 模型：deepseek-v4-flash:0731 + ollama-cloud
- 关闭 thinking：`agent.reasoning_overrides: {deepseek-v4-flash:0731: none}`（ollama-cloud 仅认 reasoning_effort:none，extra_body.thinking:disabled 被忽略）
- toolsets 精简：`hermes -p livis tools disable <ts>`（顶层 `toolsets` 键不生效，-z 读 `platform_toolsets.cli`）
- 效果：input tokens 15.9K → 5.7K（-64%），延迟 9.3s → 4.9s

### 延迟基线

| 场景 | 延迟 |
|---|---|
| 简单问答（优化后） | ~4-5s |
| 含 web_search | ~7-9s（工具结果进上下文） |
| 优化前 | 9.3s |

---

## 6. 风险与缓解

| 风险 | 等级 | 缓解 |
|---|---|---|
| 协议闭源，理想可能变更 | 中 | 版本锁定 + 变更检测告警 |
| App 绑定校验 `openclaw-` 前缀 | 中 | 阶段 3 首验；备选：自建 agentId 同格式伪装 |
| 眼镜端显示格式未知 | 中 | 阶段 4 验证，可能需要结果清洗 |
| 绑定需真人 App 操作 | 低 | 一次性流程 |
| 合规（个人逆向使用） | 低 | 不分发、不商用，仅个人 bridge |

---

## 7. 测试策略总表

| 层 | 工具 | 位置 | 依赖 |
|---|---|---|---|
| 编解码/认证 | pytest + mock HTTP | 容器 | 无 |
| adapter | pytest + fake_hermes | 容器 | 无 |
| bridge | pytest + tmp SQLite | 容器 | 无 |
| 全链路 | mock_relay | 容器 | 无 |
| 真 Hermes | `cli run` | 宿主机 | 本机 hermes |
| 真协议 | bridge 连真实 WS | 容器/宿主 | 账号+App |
