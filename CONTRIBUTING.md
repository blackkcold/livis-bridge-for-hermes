# Contributing Guidelines

感谢你考虑为这个项目做贡献！以下是一些约定，让协作更顺畅。

## 项目性质提醒

这是一个**个人实验性质**的社区项目：
- 协议层基于逆向观察，非官方支持
- 涉及真实账号认证，请勿在贡献中泄露任何个人 token / agentId / deviceId
- PR 与 Issue 是公开的，**永远不要**粘贴 `data/` 目录内容或其他敏感文件内容

## 如何贡献

### 报告 Bug / 提需求

1. 先搜索是否已有相同 Issue
2. 创建 Issue，使用仓库内置模板（Bug report / Feature request）
3. 描述尽量包含：
   - 版本（`git log -1 --oneline` 或 Release tag）
   - 运行环境（macOS/Linux/Docker，Python 版本）
   - 日志片段（脱敏后）
   - 复现步骤

### 提交代码

1. Fork 仓库，创建 feature branch：`git checkout -b feat/your-feature`
2. 遵循现有代码风格（模块 docstring、类型标注、日志规范）
3. **新增/修改必须配套测试**（`tests/` 下，保持离线可跑）
4. 本地跑通：
   ```bash
   HERMES_BIN=$PWD/scripts/fake_hermes.sh .venv/bin/python -m pytest tests/ -v
   ```
5. 提交信息用约定式（conventional commits）：
   - `feat: 新增 xxx`
   - `fix: 修复 xxx`
   - `docs: 更新文档`
   - `test: 补充测试`
6. 发起 PR 到 `main`，描述改动动机

### 协议相关贡献（重点）

如果发现了新的协议细节（消息结构、时序、字段），**不要放具体 token 值**，用占位符与脱敏示例：
```json
{"type": "exec", "metadata": {"job_id": "<UUID>"}, "payload": {"content": "..."}}
```
并在 PR 描述里注明"外部观察所得"。

## 代码结构

```
src/
├── protocol.py   # 协议层（认证/WS/编解码）—— 只做协议，不含业务
├── adapter.py    # 子进程适配器（hermes -z 驱动）
├── bridge.py     # 状态机 + outbox + 重连编排
└── cli.py        # 命令入口
```

分层职责要清晰：protocol 不懂业务，bridge 不懂协议细节，adapter 只负责拉进程拉输出。

## 行为准则

- 友善、就事论事
- 尊重"个人实验项目"的维护边界——维护者可能无法及时响应所有 PR
- 未经授权，请勿将本项目用于商业用途

感谢贡献！🎉
