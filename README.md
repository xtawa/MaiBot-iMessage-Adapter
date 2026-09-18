# MaiBot-iMessage-Adapter

**MaiBot 的 iMessage 平台适配器插件**

通过 Photon Spectrum 云端将 [MaiBot](https://github.com/Mai-with-u/MaiBot) 接入 Apple iMessage，实现消息双向收发。

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-GPL--3.0-green.svg)](LICENSE)
[![maibot-plugin-sdk](https://img.shields.io/badge/SDK-maibot--plugin--sdk-orange)](https://github.com/Mai-with-u/maibot-plugin-sdk)


📖 **完整详细教程请访问：**  
**[MaiBot-iMessage-Adapter 详细使用教程](https://www.galeros.xyz/2026/07/07/imessage-adapter/)**

### 安装

将本仓库克隆到 MaiBot 的 `plugins/` 下即可。

```
cd /path/to/MaiBot/plugins
git clone https://github.com/xtawa/MaiBot-iMessage-Adapter.git
```

### 前置条件

| 依赖 | 最低版本 | 用途 |
|------|----------|------|
| nodeenv (PyPI) | ≥ 1.10.0 | 系统无 Node.js 时自动安装到插件目录下 |
| Node.js | 20.18.1+ 或 ≥ 22 | 侧车运行时。插件会校验系统 Node.js 版本；版本不满足当前锁定依赖要求时，会自动通过 nodeenv 安装隔离的 LTS 到插件目录下的 `.nodeenv/` |
| Photon 账号 | — | iMessage 云端服务，在 [app.photon.codes](https://app.photon.codes) 注册 |
| MaiBot | ≥ 1.0.0 | 插件宿主 |


首次启用时，插件会优先使用满足 Node.js 20.18.1+ 或 ≥ 22 的系统运行时；若系统版本不满足要求或不可用，则通过 `nodeenv` 自动提供隔离的 LTS。随后会优先执行 `npm ci`（存在 lockfile 时）安装侧车依赖并编译 TypeScript。后续启动在编译产物有效时会跳过此步骤。


## 架构

```
MaiBot (plugin.py)  ←─本地 WebSocket─→  Node.js 侧车  ←─spectrum-ts SDK─→  Photon Cloud  ←─→  Apple iMessage
   127.0.0.1:18763                     (sidecar/)                           (云端 Mac 服务器)
```

- **Python 端**：负责 WebSocket Server、侧车进程管理、MaiBot SDK 组件注册
- **Node.js 侧车**：薄壳，调用 `spectrum-ts` 官方 SDK发送接收消息



## 配置

插件配置可通过 MaiBot WebUI 的插件配置页面修改。

### 配置项说明

| 配置节 | 字段 | 类型 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `[plugin]` | `enabled` | bool | false | 是否启用适配器 |
| `[plugin]` | `config_version` | str | 1.0.0 | 配置版本（无需变更） |
| `[photon]` | `project_id` | str | "" | Photon 项目 ID |
| `[photon]` | `project_secret` | str | "" | Photon 项目密钥 |
| `[bridge]` | `ws_port` | int | 18763 | 本地桥接 WebSocket 端口 |
| `[bridge]` | `max_retries` | int | 3 | 侧车崩溃最大重启次数 |
| `[bridge]` | `retry_interval` | float | 3.0 | 重启间隔（秒） |
| `[bridge]` | `max_attachment_size_mb` | int | 10 | 最大附件大小（MB） |

### 使用 (MaiBot配置)
MaiBot Core 仍会用主配置里的 bot 平台账号识别“机器人自己”。因此在启用此插件后，必须在MaiBot配置文件夹下的bot_config.toml中的 `[bot]` 部分中的 `platforms = []` 这个配置项加入以下信息:
```toml
[bot]
platform = "" # 这项保持为空(如果你没有主要的平台)
qq_account = "" # 和上个参数要求一致
platforms = ["imessage:+10000000000"] # 重要: 格式要求为 "imessage:+10000000000"
# "imessage:" 后跟的+1数字是你的Photon项目所分配的号码，请通过[Photon Dashboard] (https://app.photon.codes/dashboard/)获取。
nickname = "麦麦" # 根据你的要求来改
alias_names = [] 
```

当然，如果你不喜欢直接编辑配置文件，也可以在WebUI中设置，具体方法是:`麦麦设置-基础-平台账号右边的加号-平台imessage,账号就是+10000000000` (请填写自己的项目信息)。
不设置将无法正常发送信息。

初次使用必须先通过iMessage向Photon平台提供的号码发送信息，否则因平台限制将会出现`AuthenticationError: [spectrum-imessage] Target not allowed for this project`错误。
Photon免费的计划不支持电子邮件地址的iMessage !

### 管理命令

| 命令 | 说明 |
|------|------|
| `/imessage_status` | 查看连接状态、侧车 PID、重启次数 |
| `/imessage_reconnect` | 手动重连侧车和 Photon |

### 消息收发

- **收消息**：他人通过 iMessage 发给你 → 自动注入 MaiBot 消息管道 → LLM 回复
- **发消息**：MaiBot 生成的回复 → 自动通过 iMessage 发送

## 注意事项

### 不支持的消息类型

受限于 Photon 云端服务和MaiBot能力，以下消息类型**无法**正常收发，插件会自动拦截并记录日志：

| 消息类型 | 说明 |
|----------|------|
| 语音消息 (`.caf`) | Photon/Spectrum 目前仍可能把原生 iMessage 语音暴露为普通 CAF 附件；适配器暂不注入 MaiBot |
| 实况图片 (`.heic` / `.heif`) | Apple 实况图片格式无法被麦麦解析，已在侧车层拦截 |
| 联系人名片 (vCard) | 会提取姓名、电话、邮箱并转换为可读文本；照片等扩展字段暂不展开 |

### 其他限制

- **免费版 Photon 不支持电子邮件地址的 iMessage**，仅支持手机号
- **免费版 Photon 不支持主动发起会话**，必须先由对方通过 iMessage 向 Photon 号码发送首条消息后，才能回复
- **附件大小**受 WebUI「最大附件大小」配置项控制（默认 10 MB）；桥接层会为 Base64/JSON 膨胀自动预留帧空间

## 故障排查

| 症状 | 可能原因 | 解决 |
|------|----------|------|
| `/imessage_status` 显示未连接 | 侧车未安装依赖或编译失败 | 插件会优先执行 `npm ci` 并编译 TypeScript，查看日志确认是否成功 |
| Photon 认证失败 | project_id/project_secret 配置错误 | 在 WebUI 插件配置页面检查凭证是否正确 |
| 侧车反复重启 | 网络问题或 Photon 服务异常 | 查看 MaiBot 日志中的 `[侧车]` 前缀消息 |
| 能收不能发 | 网关未就绪 | 等待 Photon 完全连接后重试，或用 `/imessage_status` 确认状态 |
| 端口冲突 | 18763 被其他程序占用 | 在 WebUI 中修改「桥接端口」配置 |



## 许可证

本项目基于 GPL-3.0 许可证开源。