# Security Policy

## Supported Versions

Only the latest release is actively supported with security fixes.

| Version | Supported |
|---------|-----------|
| latest (v0.1.x) | ✅ |
| older | ❌ |

## Reporting a Vulnerability

**请不要公开提交包含敏感凭据的安全问题（Issue 是公开的）。**

由于本项目是个人维护的社区项目，安全问题的报告方式：

1. **涉及敏感凭据泄露**（token、agentId、deviceId 等被意外提交到仓库）：
   - 立刻在 GitHub 仓库页面创建 **Private vulnerability report**（仓库 → Security → Report a vulnerability），或
   - 直接发邮件到仓库主页显示的维护者邮箱
2. **一般安全问题**（协议缺陷、注入风险等）：
   - 创建 Issue 时在标题前加 `[SECURITY]`，公开讨论即可（不涉及敏感数据）

### 报告内容模板

```
## 描述
（问题的具体表现）

## 影响范围
（哪个模块/哪个版本）

## 复现步骤
（如何触发）

## 建议修复
（可选）
```

## 安全注意事项（给使用者）

- `data/` 目录包含 **登录 token 与设备身份**，属最高敏感数据
- 部署到服务器时：`chmod 600 data/*`，确保只有运行用户可读
- 不要把 `data/` 提交到任何版本库（`.gitignore` 已排除）
- token 通过 refresh_token 自动续期；一旦泄露，请立即在 App 端解绑并重新认证

## 本项目的安全设计

- token 仅存本地文件，不经由任何第三方中转
- 子进程调用 `hermes -z` 使用参数传递（无 shell 拼接），避免命令注入
- 指令文本按原样透传（这是"远程控制"的本质能力，使用者需自行评估信任边界）
