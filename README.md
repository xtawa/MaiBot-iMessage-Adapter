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
- **Node.js 侧车**：通过 `spectrum-ts` 官方 SDK 连接 Photon，负责 iMessage 原生内容、附件、事件和结构化 Action 的转换



## 配置

插件配置可通过 MaiBot WebUI 的插件配置页面修改。

### 配置项说明

| 配置节 | 字段 | 类型 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `[plugin]` | `enabled` | bool | false | 是否启用适配器 |
| `[plugin]` | `inbound_reaction_emoji` | str | 👀 | 入站消息进入 MaiBot 后发送的 iMessage 表情反应；留空关闭 |
| `[plugin]` | `config_version` | str | 1.2.0 | 配置版本（无需变更） |
| `[photon]` | `project_id` | str | "" | Photon 项目 ID |
| `[photon]` | `project_secret` | str | "" | Photon 项目密钥 |
| `[photon]` | `line_phones` | list[str] | [] | 此项目绑定的 iMessage 号码；多号码项目用于回程选路 |
| `[photon]` | `additional_projects` | list[PhotonProjectConfig] | [] | 可选的额外 Photon 项目，每个项目有 `project_id`、`project_secret`、`lines` |
| `[bridge]` | `ws_port` | int | 18763 | 本地桥接 WebSocket 端口 |
| `[bridge]` | `max_retries` | int | 3 | 侧车崩溃最大重启次数 |
| `[bridge]` | `retry_interval` | float | 3.0 | 重启间隔（秒） |
| `[bridge]` | `max_attachment_size_mb` | int | 10 | 单个附件大小上限（MB） |
| `[bridge]` | `max_message_size_mb` | int | 20 | 单条消息内所有附件的总大小上限（MB） |

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
- **表情反应**：入站消息成功进入 MaiBot 后，默认以 `👀` 反应原消息；可在 WebUI 修改为任意表情，留空关闭
- **多媒体顺序**：文本、语音、图片和普通文件按桥接事件中的原始顺序传递；一条消息可包含多个附件
- **原生事件**：Spectrum 暴露的 Reaction、Poll、Reply 等内容会在 `imessage_event` 消息段中保留结构化 metadata；MaiBot 不认识的已暴露事件会附带可读系统消息

### iMessage 能力与边界

| 能力 | 适配行为 |
|------|----------|
| 文本、图片、普通附件、多附件 | 收发；多段内容按原顺序组成 Spectrum multipart 消息；图片与附件遵守单附件和单消息总大小限制 |
| 原生语音 | 收到的 CAF/音频附件转为 `voice` 消息段；出站 `send_audio_message` 使用 Spectrum 的语音 builder |
| 已读与投递状态 | 侧车尝试调用 Photon `message.read()` 并保留 `read_status`；发送回执只表示 Photon 接收请求，不代表对方设备已投递或已读。锁定的 Spectrum provider 未向 `app.messages` 暴露远端 read/delivery receipts |
| Tapback / Emoji Reaction | 保留入站原始事件；出站 `send_reaction`，以及已有的入站自动 Reaction |
| Reply 引用回复 | MaiBot `reply` 段可携带目标消息 ID；也可用 `send_reply` Action |
| Unsend 撤回 | `unsend_message` 先查侧车缓存，必要时向 Photon 查询目标并确认由本账号发送；当前 Spectrum provider 不会把入站 `message.unsent` 事件转成 `app.messages` 消息 |
| Message Effects | `send_effect` 支持 Spectrum 暴露的气泡和屏幕效果名称，如 `gentle`、`slam`、`confetti`、`fireworks` |
| Poll | 收到投票创建及投票/撤销投票变化作为结构化事件；支持 `create_poll`、`vote_poll`、`unvote_poll` 和 `add_poll_option`。Spectrum 当前不转发 `optionAdded` 变更事件 |
| Chat Background | `set_chat_background` 设置图片背景；`clear: true` 清除 |
| Link Card | `send_link_card` 接受 HTTP(S) URL 并创建链接卡片 |
| Location Card | `send_location` 发送 Apple Maps 链接卡片，并在 `action_metadata` 标明这是链接降级 |
| vCard | `send_vcard` 发送联系人卡片 |
| HEIC / HEIF / Live Photo | HEIC/HEIF 按原格式收发；Live Photo 入站通过 Photon companion stream 保留主图与视频，出站使用 `send_live_photo` 单独发送配对附件 |
| 手写消息 | `send_handwriting` 可将 MaiBot 提供的 PNG 等静态图片作为附件发送；没有原生手写 API 时会标记静态图片降级 |
| Digital Touch | 当前锁定的 Spectrum 公共 API 没有发送入口，Action 会返回明确的不支持错误 |
| 多 Photon Project / 多号码 | 可配置多个 Photon 项目和号码映射；每条入站事件携带项目与线路 metadata，用于出站选路 |
| 主动消息与会话缓存 | 已知会话优先使用缓存，未缓存时查询 Photon；`open_dm` 可请求创建 DM，是否允许由 Photon 项目和目标地址的权限决定 |
| 重连、去重与顺序 | 入站事件按到达顺序串行处理并按项目、号码、事件 ID 去重；Photon 流异常会触发侧车重启重连 |

### 结构化 Action

需要调用 iMessage 原生操作时，在网关消息中传入 `imessage_action` 对象，或使用 `raw_message` 中的 `imessage_action` 段。目标会话仍由 `session_id` 或 `platform_io_target_user_id` 指定；多项目场景可通过 `platform_io_project_id` 和 `platform_io_account_id` 选路。

```json
{
  "session_id": "any;-;+15550000000",
  "imessage_action": {
    "action": "send_reply",
    "reply_to_message_id": "photon-message-id",
    "text": "引用回复内容"
  }
}
```

| Action | 主要字段 |
|--------|----------|
| `send_reply` | `reply_to_message_id`、`text`、`attachments` |
| `unsend_message` | `message_id` |
| `send_reaction` | `target_message_id`、`emoji` |
| `send_audio_message` | `data_base64`、`mime_type`、`name`、`duration` |
| `send_live_photo` | `data_base64`、`name`（`.HEIC`/`.HEIF`）、`mime_type`、`companion_data_base64` |
| `send_effect` | `effect`、`text` 或单个附件；效果名见上表 |
| `create_poll` | `title`、`options`（字符串数组） |
| `vote_poll` | `poll_message_id`、`option_id` |
| `unvote_poll` | `poll_message_id` |
| `add_poll_option` | `poll_message_id`、`title` |
| `send_link_card` | `url`（HTTP 或 HTTPS） |
| `send_location` | `latitude`、`longitude`，可选 `label` 或 `address` |
| `send_vcard` | `contact` 或 `vcard` 对象 |
| `set_chat_background` | `data_base64`、`mime_type`；`clear: true` 可清除背景 |
| `send_handwriting` | `image_base64` 或 `data_base64`，可选 `mime_type`、`name` |
| `open_dm` | `recipient`，多号码项目还需线路账号 metadata |
| `send_digital_touch` | 当前 SDK 未公开对应发送 API，会返回明确失败回执 |

Action 名称大小写不敏感；其余字段按 Action 语义校验。Poll 投票和添加选项复用 Spectrum 管理的 Photon 高级客户端，仍沿用同一项目与线路认证。当前未被 SDK 支持的操作会返回失败回执，不会静默伪装成功。

## 注意事项

### 其他限制

- **免费版 Photon 不支持电子邮件地址的 iMessage**，仅支持手机号
- **主动发起会话**：`open_dm` 会请求 Spectrum 创建会话；Photon 计划、允许列表和 Apple 侧限制仍可能拒绝该操作
- **附件大小**受 WebUI「最大附件大小」配置项控制（默认 10 MB）；桥接层会按“单附件 + 单消息附件总量”双重限制，并为 Base64/JSON 膨胀自动预留帧空间
- **附件完整性**：侧车会拒绝损坏的 Base64、超过单附件上限或超过单条消息总上限的出站附件，不会静默截断后发送
- **回执语义**：Spectrum 发送成功仅表示 Photon 接受了请求。锁定版本没有公开对方设备投递回执流，故 `delivery_confirmed` 保持 `false`
- **事件覆盖**：锁定版本的 Spectrum provider 不会把入站撤回、编辑、远端 read receipt 和 Poll `optionAdded` 变更转成 `app.messages` 项；侧车只抽象 SDK 实际暴露的原生事件
- **平台原生能力**：数字触控等操作以 Spectrum 当前公共 API 为准；不支持时会返回明确错误

为遵守参考仓库的许可证，本项目没有复制或移植 Uranus iMessage 的源码；适配实现使用 Spectrum 已公开的接口，并保留 MaiBot Adapter 原有的 Python 插件与 Node.js 侧车架构。

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
