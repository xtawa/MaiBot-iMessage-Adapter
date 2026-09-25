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
| maibot-plugin-sdk (PyPI) | 2.3.0–2.8.2 | 插件组件注册；CI 对此范围内已发布的每个版本运行 Python 与 MaiBot Host 组件回环测试 |
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
| `[plugin]` | `auto_typing_indicator` | bool | true | 收到入站 iMessage 消息后自动展示正在输入（Typing Indicator）气泡状态 |
| `[plugin]` | `parse_inline_action_tags` | bool | true | 允许在回复文本中使用内联标签（如 `[effect:烟花]`、`[music:歌名]`、`[transfer:520]` 等）触发原生动作 |
| `[plugin]` | `forward_native_events_to_maibot` | bool | true | 将 iMessage 撤回、编辑、投票、贴纸、已读、群变更、聊天背景变化等原生事件注入 MaiBot |
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
| `/imessage_status` | 查看连接状态、侧车 PID、重启次数及活跃会话缓存 |
| `/imessage_reconnect` | 手动重连侧车和 Photon |
| `/imessage_check <手机号或邮箱>` | 检测指定手机号或 Apple ID 邮箱是否开通 iMessage 蓝泡泡 |
| `/imessage_enroll <邮箱>` | 将指定邮箱注册到 Photon Shared Instance 实例并获取绑定引导 |

### MaiBot LLM 原生工具（`@Tool`）

插件向 MaiBot 的 LLM 规划器注册了 7 个专用 `@Tool`，让麦麦可以在聊天过程中自主决策并调用 iMessage 原生交互：

| 工具名称 | 功能说明 |
|----------|----------|
| `imessage_send_reaction` | 对消息发送或移除 Tapback 点按表情反应（支持 ❤️/👍/👎/😂/‼️/❓ 及任意自定义 Emoji） |
| `imessage_reply_or_edit_message` | 引用回复指定消息、编辑上一条已发消息、撤回已发消息或执行已读不回 |
| `imessage_send_effect` | 发送带全屏特效（烟花、激光、气球、五彩纸屑、爱心、流星、聚光灯、回声、欢庆）、气泡特效（震撼、放大、缩小、隐形墨水）或 iOS 18 文字动效（抖动、点头、爆炸、波纹、绽放等）的消息 |
| `imessage_poll` | 发起 iMessage 原生交互式投票、为现有投票投出一票、或向投票追加新选项 |
| `imessage_send_card` | 发送音乐卡片（Apple Music / 网易云音乐双源检索）、虚拟转账收款卡片（对方双击点按气泡即可收款，自动撤回/编辑原“待收款”气泡并替换为“✓ 已收款”）、富链接预览卡片或个人名片 |
| `imessage_chat_and_group` | 管理 iMessage 群聊或会话（修改群名、拉人/踢人、展示带心跳与回复结束自动停止的输入中气泡、穿透勿扰模式强制提醒） |
| `imessage_location_and_check` | 发送 Apple Maps 定位卡片（降级模式）、主动刷新并查询 Find My 实时位置、或检测号码是否支持 iMessage 蓝泡泡 |

> **多会话 Tool 路由**：当前 MaiBot Host 向插件 Tool 注入的是 Host 会话哈希。适配器会依据入站消息的用户或群组、线路号码和项目标识建立哈希到 iMessage `chat_id` 的映射；多个会话并行时可自动选中调用方会话。映射缺失时会拒绝执行并提示显式传入 `chat_id`，避免猜测目标。

### 内联动作标签（Inline Action Tags）

当 `parse_inline_action_tags = true` 时，人设提示词或 LLM 回复中也可以直接使用内联标签触发 iMessage 动作：
- `[effect:烟花] 新年快乐！`（全屏/气泡特效，支持中英文别名）
- `[text_effect:爆炸] 太离谱了！`（iOS 18 文字动效）
- `[react:❤️]`（对最新入站消息发送 Tapback 反应）
- `[music:周杰伦-晴天]`（自动检索 Apple Music / 网易云音乐并发送带封面的富链接音乐卡片）
- `[transfer:520:拿去买奶茶]`（发送虚拟转账 MiniApp 卡片，用户点按气泡即可触发收款，自动撤回/编辑原待收款气泡并替换为已收款卡片）
- `[location:南宁万象城]` 或 `[location:22.8152,108.3669|万象城]`（发送 Apple Maps 定位预览卡片）
- `[poll:今晚吃什么|火锅|烧烤|日料]` / `[vote:A]` / `[poll_add:小龙虾]`（发起投票 / 投票 / 加选项）
- `[reply:我也觉得]` / `[edit:更正后的文本]` / `[unsend]` / `[leave_on_read]`

### iMessage 能力分类与 MaiBot 兼容细节（29 项原生动作 + 2 项降级动作）

| 类别 | 数量 / 范围 | 适配说明 |
|------|-------------|----------|
| **原生结构化动作 (`NATIVE_STRUCTURED_ACTIONS`)** | **29 项** | `send`、`open_dm`、`send_reply`、`send_reaction`、`remove_reaction`、`edit_message`、`unsend_message`、`send_effect`、`send_audio_message`、`place_sticker`、`send_live_photo`、`send_link_card`、`send_music_card`、`send_transfer_card`、`update_transfer_card`、`share_my_contact`、`send_vcard`、`find_my_location`、`create_poll`、`vote_poll`、`unvote_poll`、`add_poll_option`、`set_typing`、`mark_read`、`notify_silenced`、`set_chat_background`、`manage_group`、`check_imessage_availability`、`enroll_shared_user` |
| **降级结构化动作 (`FALLBACK_STRUCTURED_ACTIONS`)** | **2 项** (`fallback_mode: true`) | `send_location`（通过 `https://maps.apple.com/` 富链接卡片 + Google Maps 静态预览图发送，非 CLLocation 原生数据包）、`send_handwriting`（服务端渲染手写笔迹 PNG 图片附件发送，回执中显式标记 `fallback_mode: true, native: false`） |
| **仅支持入站解析的能力** | Digital Touch / 第三方扩展气泡 | 入站消息自动调用 `photon.messages.getEmbeddedMedia` 提取 Apple 手写与 Digital Touch 内嵌媒体，并解析 Apple Cash、Find My、网易云音乐、QQ 音乐、B 站、小红书等 15+ 种扩展气泡；**出站不支持 `send_digital_touch`**（调用时会显式报错拒绝） |
| **MaiBot 标准消息段对齐** | `voice` / `file` / `reply` / `additional_config` | `voice.data` 为字符串，二进制放在顶层 `binary_data_base64`；视频作为 `type="file"` 且 `mime_type="video/..."` 传入 `FileComponent`；`reply.data` 提供 `target_message_id`、`target_message_content`、`target_message_sender_*`；原生事件保存在 `message_info.additional_config["imessage_event"]` |
| **底层 gRPC 5 路实时事件流订阅** | `messages` / `chats` / `polls` / `groups` / `locations` | 侧车同时订阅 `AdvancedIMessage` 的 `messages`、`chats`、`polls`、`groups` 与 `locations.watch()` 实时流，完整捕获入站撤回（含缓存原文回溯）、消息编辑（含修改前后对比）、已读回执、贴纸放置、聊天背景变更、投票选项追加、群变更及 Find My 位置更新 (`location.updated`) |
| **Typing 与投递核验** | 4s 心跳 + 约 20s 上限 + 12s 核验 | 收到消息后开启 Typing Indicator，侧车每 4 秒保活，最长约 20 秒；`send_to_imessage` 完成或报错时发送 `set_typing(false)`。若思考超过上限或没有出站消息，状态会提前或依超时结束。出站 12 秒后核验投递状态；大附件下载内置 4 次指数退避重试 |

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
