"""iMessage 适配器配置模型。"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, Optional

from maibot_sdk import Field, PluginConfigBase

SUPPORTED_CONFIG_VERSION = "1.2.0"
# 供插件作者方便追踪插件版本
PLUGIN_VERSION = "0.2.0"

DEFAULT_WS_PORT = 18763
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_INTERVAL = 3.0
DEFAULT_MAX_ATTACHMENT_SIZE_MB = 10
DEFAULT_MAX_MESSAGE_SIZE_MB = 20


def _schema_i18n(
    *,
    label_en: str,
    label_ja: str,
    label_ko: str,
    hint_en: Optional[str] = None,
    hint_ja: Optional[str] = None,
    hint_ko: Optional[str] = None,
    placeholder_en: Optional[str] = None,
    placeholder_ja: Optional[str] = None,
    placeholder_ko: Optional[str] = None,
) -> Dict[str, Dict[str, str]]:
    """构造 WebUI 配置项的多语言说明文本。"""

    i18n: Dict[str, Dict[str, str]] = {
        "en_US": {"label": label_en},
        "ja_JP": {"label": label_ja},
        "ko_KR": {"label": label_ko},
    }
    if hint_en is not None:
        i18n["en_US"]["hint"] = hint_en
    if hint_ja is not None:
        i18n["ja_JP"]["hint"] = hint_ja
    if hint_ko is not None:
        i18n["ko_KR"]["hint"] = hint_ko
    if placeholder_en is not None:
        i18n["en_US"]["placeholder"] = placeholder_en
    if placeholder_ja is not None:
        i18n["ja_JP"]["placeholder"] = placeholder_ja
    if placeholder_ko is not None:
        i18n["ko_KR"]["placeholder"] = placeholder_ko
    return i18n


class IMessagePluginOptions(PluginConfigBase):
    """插件级配置。"""

    __ui_label__: ClassVar[str] = "插件设置"
    __ui_order__: ClassVar[int] = 0

    enabled: bool = Field(
        default=False,
        description="是否启用 iMessage 适配器。",
        json_schema_extra={
            "hint": "关闭后插件会保持空闲，不会启动侧车进程或连接 Photon。",
            "i18n": _schema_i18n(
                label_en="Enable adapter",
                label_ja="アダプターを有効化",
                label_ko="어댑터 활성화",
                hint_en="When disabled, the plugin stays idle and will not launch the sidecar process or connect to Photon.",
                hint_ja="無効にすると、プラグインは待機状態のままになり、サイドカープロセスを起動せず、Photon にも接続しません。",
                hint_ko="비활성화하면 플러그인이 대기 상태로 유지되며 사이드카 프로세스를 시작하지 않고 Photon에 연결하지 않습니다.",
            ),
            "label": "启用适配器",
            "order": 0,
        },
    )
    inbound_reaction_emoji: str = Field(
        default="👀",
        description="收到可注入 MaiBot 的 iMessage 后自动发送的表情反应；留空可关闭。",
        json_schema_extra={
            "hint": "默认发送 👀。仅对文字、图片等会进入 MaiBot 消息管道的入站消息生效；留空即可关闭。",
            "i18n": _schema_i18n(
                label_en="Inbound reaction emoji",
                label_ja="受信メッセージへのリアクション",
                label_ko="수신 메시지 반응 이모지",
                hint_en="Sends this emoji as a reaction after an inbound iMessage is accepted for MaiBot. Leave empty to disable.",
                hint_ja="MaiBot に受信として取り込まれた iMessage にこの絵文字でリアクションします。空欄で無効化できます。",
                hint_ko="MaiBot에 수신 메시지로 전달된 iMessage에 이 이모지로 반응합니다. 비워 두면 비활성화됩니다.",
                placeholder_en="👀",
                placeholder_ja="👀",
                placeholder_ko="👀",
            ),
            "label": "入站消息表情反应",
            "order": 1,
            "placeholder": "👀",
        },
    )
    auto_typing_indicator: bool = Field(
        default=True,
        description="收到入站 iMessage 消息后自动触发正在输入（Typing Indicator）气泡状态。",
        json_schema_extra={
            "hint": "开启后，收到用户消息并在 MaiBot 思考回复期间自动向对方展示 iMessage 输入中指示器。",
            "i18n": _schema_i18n(
                label_en="Auto typing indicator",
                label_ja="自動入力中インジケーター",
                label_ko="자동 입력 중 표시",
                hint_en="Automatically shows the iMessage typing indicator while MaiBot prepares a reply.",
                hint_ja="MaiBot が返信を準備している間、iMessage の入力中インジケーターを自動表示します。",
                hint_ko="MaiBot이 답장을 준비하는 동안 iMessage 입력 중 표시기를 자동으로 보여줍니다.",
            ),
            "label": "自动输入中指示器",
            "order": 2,
        },
    )
    parse_inline_action_tags: bool = Field(
        default=True,
        description="允许在回复文本中使用内联动作标签（如 [effect:烟花]、[react:❤️]、[music:歌名]、[location:地点]、[poll:标题|选项A|选项B] 等）触发原生 iMessage 动作。",
        json_schema_extra={
            "hint": "支持人设提示词或 LLM 直接输出内联标签触发屏幕特效、文字动效、音乐卡片、定位、投票、转账卡片等能力。",
            "i18n": _schema_i18n(
                label_en="Parse inline action tags",
                label_ja="インラインアクションタグ解析",
                label_ko="인라인 액션 태그 파싱",
                hint_en="Allows inline tags like [effect:fireworks], [react:❤️], [music:...], [location:...] in outbound text to trigger native iMessage actions.",
                hint_ja="[effect:fireworks] や [react:❤️]、[music:...] などのインラインタグを解析して iMessage ネイティブ機能を発動します。",
                hint_ko="[effect:fireworks], [react:❤️], [music:...] 등의 인라인 태그를 파싱하여 네이티브 iMessage 기능을 실행합니다.",
            ),
            "label": "解析内联动作标签",
            "order": 3,
        },
    )
    forward_native_events_to_maibot: bool = Field(
        default=True,
        description="将 iMessage 原生事件（撤回、编辑、投票、贴纸、已读、群变更、聊天背景变化等）注入 MaiBot 消息流。",
        json_schema_extra={
            "hint": "开启后 MaiBot 可感知对方撤回消息、编辑消息、发起或参与投票、贴纸及转账收款等事件。",
            "i18n": _schema_i18n(
                label_en="Forward native iMessage events",
                label_ja="ネイティブイベントを MaiBot に転送",
                label_ko="네이티브 이벤트를 MaiBot으로 전달",
            ),
            "label": "转发原生事件到 MaiBot",
            "order": 4,
        },
    )
    config_version: str = Field(
        default=SUPPORTED_CONFIG_VERSION,
        description="当前配置结构版本。",
        json_schema_extra={
            "disabled": True,
            "hidden": True,
            "i18n": _schema_i18n(label_en="Config version", label_ja="設定バージョン", label_ko="설정 버전"),
            "label": "配置版本",
            "order": 99,
        },
    )
    plugin_version: str = Field(
        default=PLUGIN_VERSION,
        description="插件版本标识（兼容旧配置；侧车构建版本不再写回 config.toml）。",
        json_schema_extra={
            "disabled": True,
            "hidden": True,
            "i18n": _schema_i18n(label_en="Plugin version", label_ja="プラグインバージョン", label_ko="플러그인 버전"),
            "label": "插件版本",
            "order": 100,
        },
    )

    def should_connect(self) -> bool:
        """判断当前配置下是否应当启动连接。"""
        return self.enabled


class PhotonProjectConfig(PluginConfigBase):
    """额外 Photon 项目及其可选的 iMessage 号码映射。"""

    project_id: str = Field(default="", description="Photon 项目 ID。")
    project_secret: str = Field(
        default="",
        description="Photon 项目密钥。",
        json_schema_extra={"input_type": "password"},
    )
    lines: list[str] = Field(
        default_factory=list,
        description="此项目下的 iMessage 号码；用于项目未随会话元数据传回时辅助选路。",
    )


class PhotonServerConfig(PluginConfigBase):
    """Photon Spectrum 云端连接配置。"""

    __ui_label__: ClassVar[str] = "Photon 云端"
    __ui_order__: ClassVar[int] = 1

    project_id: str = Field(
        default="",
        description="Photon 项目 ID，用于标识你的 Photon 项目。",
        json_schema_extra={
            "hint": "在 Photon 控制台 (app.photon.codes) 创建项目后获取。",
            "i18n": _schema_i18n(
                label_en="Project ID",
                label_ja="プロジェクト ID",
                label_ko="프로젝트 ID",
                hint_en="Obtain from the Photon dashboard (app.photon.codes) after creating a project.",
                hint_ja="Photon ダッシュボード (app.photon.codes) でプロジェクト作成後に取得します。",
                hint_ko="Photon 대시보드(app.photon.codes)에서 프로젝트 생성 후 확인할 수 있습니다.",
                placeholder_en="PROJECT_ID",
                placeholder_ja="PROJECT_ID",
                placeholder_ko="PROJECT_ID",
            ),
            "label": "项目 ID",
            "order": 0,
            "placeholder": "PROJECT_ID",
        },
    )
    project_secret: str = Field(
        default="",
        description="Photon 项目密钥，用于认证你的 Photon 项目身份。",
        json_schema_extra={
            "hint": "与项目 ID 配套的密钥，请妥善保管，切勿泄露。",
            "i18n": _schema_i18n(
                label_en="Project secret",
                label_ja="プロジェクトシークレット",
                label_ko="프로젝트 시크릿",
                hint_en="The secret paired with the project ID. Keep it safe and never expose it.",
                hint_ja="プロジェクト ID とペアになるシークレットです。安全に保管し、決して公開しないでください。",
                hint_ko="프로젝트 ID와 페어링되는 시크릿입니다. 안전하게 보관하고 절대 공개하지 마십시오.",
                placeholder_en="PROJECT_SECRET",
                placeholder_ja="PROJECT_SECRET",
                placeholder_ko="PROJECT_SECRET",
            ),
            "input_type": "password",
            "label": "项目密钥",
            "order": 1,
            "placeholder": "PROJECT_SECRET",
        },
    )
    line_phones: list[str] = Field(
        default_factory=list,
        description="当前 Photon 项目下的 iMessage 号码。多个号码会按会话所属线路选路。",
        json_schema_extra={
            "label": "iMessage 号码",
            "hint": "多号码时填写项目分配的全部号码，使用国际格式，例如 +15551234567。",
            "order": 2,
        },
    )
    additional_projects: list[PhotonProjectConfig] = Field(
        default_factory=list,
        description="可选的其他 Photon 项目；每个项目可绑定多个 iMessage 号码。",
        json_schema_extra={
            "label": "其他 Photon 项目",
            "hint": "用于连接多个 Photon Project。保留项目 ID 与密钥为空的条目会导致启动失败。",
            "order": 3,
        },
    )

    def configured_projects(self) -> list[dict[str, Any]]:
        """Return validated credentials for all non-empty Photon projects."""

        entries: list[dict[str, Any]] = []
        primary_id = self.project_id.strip()
        primary_secret = self.project_secret.strip()
        if primary_id or primary_secret:
            if not primary_id or not primary_secret:
                raise ValueError("主 Photon 项目必须同时填写 project_id 和 project_secret")
            entries.append(
                {
                    "project_id": primary_id,
                    "project_secret": primary_secret,
                    "lines": [value.strip() for value in self.line_phones if value.strip()],
                }
            )

        for index, project in enumerate(self.additional_projects, start=1):
            project_id = project.project_id.strip()
            project_secret = project.project_secret.strip()
            if not project_id and not project_secret:
                continue
            if not project_id or not project_secret:
                raise ValueError(f"第 {index} 个额外 Photon 项目必须填写 ID 和密钥")
            entries.append(
                {
                    "project_id": project_id,
                    "project_secret": project_secret,
                    "lines": [value.strip() for value in project.lines if value.strip()],
                }
            )

        project_ids = [entry["project_id"] for entry in entries]
        if not entries:
            raise ValueError("至少配置一个 Photon 项目")
        if len(project_ids) != len(set(project_ids)):
            raise ValueError("Photon 项目 ID 不得重复")
        return entries


class SidecarBridgeConfig(PluginConfigBase):
    """本地 Node.js 侧车桥接配置。"""

    __ui_label__: ClassVar[str] = "本地桥接"
    __ui_order__: ClassVar[int] = 2

    ws_port: int = Field(
        default=DEFAULT_WS_PORT,
        description="Python 侧 WebSocket Server 的监听端口，侧车进程会连接此端口。",
        json_schema_extra={
            "hint": "仅监听 127.0.0.1，不会暴露到外网。如端口冲突可更换为其他未占用端口。",
            "i18n": _schema_i18n(
                label_en="Bridge port",
                label_ja="ブリッジポート",
                label_ko="브리지 포트",
                hint_en="Listens on 127.0.0.1 only. Change to another free port if it conflicts.",
                hint_ja="127.0.0.1 のみで待受します。ポートが競合する場合は他の空きポートに変更してください。",
                hint_ko="127.0.0.1에서만 수신 대기합니다. 포트가 충돌하면 다른 사용 가능한 포트로 변경하십시오.",
            ),
            "label": "桥接端口",
            "order": 0,
        },
    )
    max_retries: int = Field(
        default=DEFAULT_MAX_RETRIES,
        description="侧车进程异常退出后的最大自动重启次数。",
        json_schema_extra={
            "hint": "超过此次数后插件将放弃重启，需手动执行 /imessage_reconnect 或重载插件。",
            "i18n": _schema_i18n(
                label_en="Max retries",
                label_ja="最大リトライ回数",
                label_ko="최대 재시도 횟수",
                hint_en="After exceeding this count the plugin stops restarting. Run /imessage_reconnect or reload the plugin manually.",
                hint_ja="この回数を超えるとプラグインは再起動を停止します。手動で /imessage_reconnect を実行するか、プラグインを再読み込みしてください。",
                hint_ko="이 횟수를 초과하면 플러그인이 재시작을 중지합니다. 수동으로 /imessage_reconnect를 실행하거나 플러그인을 다시 로드하십시오.",
            ),
            "label": "最大重启次数",
            "order": 1,
        },
    )
    retry_interval: float = Field(
        default=DEFAULT_RETRY_INTERVAL,
        description="侧车进程异常退出后，等待多少秒再尝试重启。",
        json_schema_extra={
            "hint": "建议不低于 2 秒，给 Photon 服务端断开检测留出时间。",
            "i18n": _schema_i18n(
                label_en="Retry interval (sec)",
                label_ja="リトライ間隔（秒）",
                label_ko="재시도 간격(초)",
                hint_en="Suggested at least 2 seconds to allow Photon to detect the disconnect.",
                hint_ja="Photon サーバー側の切断検出のため、2 秒以上を推奨します。",
                hint_ko="Photon 서버 측의 연결 해제 감지를 위해 2초 이상을 권장합니다.",
            ),
            "label": "重启间隔（秒）",
            "order": 2,
            "step": 0.5,
        },
    )
    max_attachment_size_mb: int = Field(
        default=DEFAULT_MAX_ATTACHMENT_SIZE_MB,
        description="最大附件大小（MB），超出此大小的附件将被拒绝，仅限图片等支持的媒体类型。",
        json_schema_extra={
            "hint": "设置过大可能导致 WebSocket 帧超限或内存占用过高。建议 1–50 MB。",
            "i18n": _schema_i18n(
                label_en="Max attachment size (MB)",
                label_ja="最大添付ファイルサイズ (MB)",
                label_ko="최대 첨부 파일 크기(MB)",
                hint_en="Overly large values may cause WebSocket frame overflow or high memory usage. Recommended: 1–50 MB.",
                hint_ja="大きすぎると WebSocket フレーム超過やメモリ使用量の増加を招く可能性があります。推奨: 1～50 MB。",
                hint_ko="너무 큰 값은 WebSocket 프레임 초과 또는 높은 메모리 사용량을 초래할 수 있습니다. 권장: 1~50MB.",
            ),
            "label": "最大附件大小（MB）",
            "order": 3,
        },
    )
    max_message_size_mb: int = Field(
        default=DEFAULT_MAX_MESSAGE_SIZE_MB,
        description="单条桥接消息允许携带的附件总大小（MB）。多个附件会共享此额度。",
        json_schema_extra={
            "hint": "用于限制多图/多附件消息的总内存占用。建议不小于单附件上限，默认 20 MB。",
            "i18n": _schema_i18n(
                label_en="Max message attachments (MB)",
                label_ja="1メッセージの添付合計上限 (MB)",
                label_ko="메시지당 첨부파일 총 한도(MB)",
                hint_en="Limits the total decoded attachment bytes carried by one bridged message. Default: 20 MB.",
                hint_ja="1 件のブリッジメッセージに含める添付ファイルの合計サイズを制限します。既定値: 20 MB。",
                hint_ko="하나의 브리지 메시지에 포함되는 첨부파일의 총 디코딩 크기를 제한합니다. 기본값: 20MB.",
            ),
            "label": "单条消息附件总上限（MB）",
            "order": 4,
        },
    )




class IMessageAdapterConfig(PluginConfigBase):
    """iMessage 适配器插件完整配置。"""

    plugin: IMessagePluginOptions = Field(default_factory=IMessagePluginOptions)
    photon: PhotonServerConfig = Field(default_factory=PhotonServerConfig)
    bridge: SidecarBridgeConfig = Field(default_factory=SidecarBridgeConfig)
