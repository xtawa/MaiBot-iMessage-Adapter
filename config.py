"""iMessage 适配器配置模型。"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, Optional

from maibot_sdk import Field, PluginConfigBase

SUPPORTED_CONFIG_VERSION = "1.0.0"

DEFAULT_WS_PORT = 18763
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_INTERVAL = 3.0


def _schema_i18n(
    *,
    label_en: str,
    label_ja: str,
    hint_en: Optional[str] = None,
    hint_ja: Optional[str] = None,
    placeholder_en: Optional[str] = None,
    placeholder_ja: Optional[str] = None,
) -> Dict[str, Dict[str, str]]:
    """构造 WebUI 配置项的多语言说明文本。"""

    i18n: Dict[str, Dict[str, str]] = {
        "en_US": {"label": label_en},
        "ja_JP": {"label": label_ja},
    }
    if hint_en is not None:
        i18n["en_US"]["hint"] = hint_en
    if hint_ja is not None:
        i18n["ja_JP"]["hint"] = hint_ja
    if placeholder_en is not None:
        i18n["en_US"]["placeholder"] = placeholder_en
    if placeholder_ja is not None:
        i18n["ja_JP"]["placeholder"] = placeholder_ja
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
                hint_en="When disabled, the plugin stays idle and will not launch the sidecar process or connect to Photon.",
                hint_ja="無効にすると、プラグインは待機状態のままになり、サイドカープロセスを起動せず、Photon にも接続しません。",
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
            "i18n": _schema_i18n(label_en="Config version", label_ja="設定バージョン"),
            "label": "配置版本",
            "order": 99,
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
                hint_en="Obtain from the Photon dashboard (app.photon.codes) after creating a project.",
                hint_ja="Photon ダッシュボード (app.photon.codes) でプロジェクト作成後に取得します。",
                placeholder_en="proj_xxxxxxxx",
                placeholder_ja="proj_xxxxxxxx",
            ),
            "label": "项目 ID",
            "order": 0,
            "placeholder": "proj_xxxxxxxx",
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
                hint_en="The secret paired with the project ID. Keep it safe and never expose it.",
                hint_ja="プロジェクト ID とペアになるシークレットです。安全に保管し、決して公開しないでください。",
                placeholder_en="sk_xxxxxxxx",
                placeholder_ja="sk_xxxxxxxx",
            ),
            "input_type": "password",
            "label": "项目密钥",
            "order": 1,
            "placeholder": "sk_xxxxxxxx",
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
                hint_en="Listens on 127.0.0.1 only. Change to another free port if it conflicts.",
                hint_ja="127.0.0.1 のみで待受します。ポートが競合する場合は他の空きポートに変更してください。",
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
                hint_en="After exceeding this count the plugin stops restarting. Run /imessage_reconnect or reload the plugin manually.",
                hint_ja="この回数を超えるとプラグインは再起動を停止します。手動で /imessage_reconnect を実行するか、プラグインを再読み込みしてください。",
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
                hint_en="Suggested at least 2 seconds to allow Photon to detect the disconnect.",
                hint_ja="Photon サーバー側の切断検出のため、2 秒以上を推奨します。",
            ),
            "label": "重启间隔（秒）",
            "order": 2,
            "step": 0.5,
        },
    )


class IMessageAdapterConfig(PluginConfigBase):
    """iMessage 适配器插件完整配置。"""

    plugin: IMessagePluginOptions = Field(default_factory=IMessagePluginOptions)
    photon: PhotonServerConfig = Field(default_factory=PhotonServerConfig)
    bridge: SidecarBridgeConfig = Field(default_factory=SidecarBridgeConfig)
