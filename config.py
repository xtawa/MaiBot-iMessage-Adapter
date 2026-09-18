"""iMessage 适配器配置模型。"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, Optional

from maibot_sdk import Field, PluginConfigBase

SUPPORTED_CONFIG_VERSION = "1.0.0"
# 供插件作者方便追踪插件版本
PLUGIN_VERSION = "0.1.12"

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
