# iMessage 适配器插件

通过 Photon Spectrum 云端将 MaiBot 接入 Apple iMessage，实现消息双向收发。

## 架构

```
MaiBot (plugin.py)  ←─本地 WebSocket─→  Node.js 侧车  ←─spectrum-ts SDK─→  Photon Cloud  ←─→  Apple iMessage
   127.0.0.1:18763                     (sidecar/)                           (云端 Mac 服务器)
```

- **Python 端**：负责 WebSocket Server、侧车进程管理、MaiBot SDK 组件注册
- **Node.js 侧车**：薄壳，只调 `spectrum-ts` 官方 SDK，不写任何 Photon 协议代码

## 前置条件

| 依赖 | 最低版本 | 用途 |
|------|----------|------|
| Node.js | ≥ 18 | 侧车运行时 |
| npm | ≥ 9 | 侧车依赖管理 |
| Photon 账号 | — | iMessage 云端服务，在 [app.photon.codes](https://app.photon.codes) 注册 |
| MaiBot | ≥ 1.0.0 | 插件宿主 |

## 安装

1. 将插件目录放入 MaiBot 的 `plugins/` 下：

   ```
   plugins/iMessage-Adapter/
   ```

2. 安装侧车依赖：

   ```bash
   cd plugins/iMessage-Adapter/sidecar
   npm install
   npm run build
   ```

## 配置

在 MaiBot 的 WebUI 或通过 `config.toml` 配置：

```toml
[photon]
project_id = "your-project-id"        # 在 app.photon.codes 获取
project_secret = "your-project-secret" # 项目密钥

[bridge]
ws_port = 18763                       # 本地桥接端口（一般不需要改）
max_retries = 3                       # 侧车崩溃最大重启次数
retry_interval = 3.0                  # 重启间隔（秒）
```

## 使用

### 管理命令

| 命令 | 说明 |
|------|------|
| `/imessage_status` | 查看连接状态、侧车 PID、重启次数 |
| `/imessage_reconnect` | 手动重连侧车和 Photon |

### 消息收发

- **收消息**：他人通过 iMessage 发给你 → 自动注入 MaiBot 消息管道 → LLM 回复
- **发消息**：MaiBot 生成的回复 → 自动通过 iMessage 发送

## 故障排查

| 症状 | 可能原因 | 解决 |
|------|----------|------|
| `/imessage_status` 显示未连接 | 侧车未启动 | 检查 `sidecar/node_modules/` 是否存在，确保已执行 `npm install && npm run build` |
| Photon 认证失败 | project_id/project_secret 错误 | 检查 `config.toml` 中的凭证是否正确 |
| 侧车反复重启 | 网络问题或 Photon 服务异常 | 查看 MaiBot 日志中的 `[侧车]` 前缀消息 |
| 能收不能发 | 网关未就绪 | 等待 Photon 完全连接后重试，或用 `/imessage_status` 确认状态 |
| 端口冲突 | 18763 被其他程序占用 | 修改 `bridge.ws_port` 配置项 |

## 开发

```bash
# Python 端
pip install maibot-plugin-sdk websockets

# Node.js 侧车
cd sidecar
npm install
npm run build    # 编译 TypeScript
node dist/index.js  # 手动启动（调试时由 Python 环境变量传参）
```
