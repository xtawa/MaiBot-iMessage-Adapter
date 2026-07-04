"""iMessage 适配器配置模型。"""

from __future__ import annotations

from maibot_sdk import Field, PluginConfigBase


class IMessagePluginOptions(PluginConfigBase):
    """插件级配置。"""

    __ui_label__ = "插件设置"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(
        default=False,
        description="是否启用 iMessage 适配器。",
        json_schema_extra={
            "hint": "关闭后插件会保持空闲，不会启动侧车进程或连接 Photon。",
            "label": "启用适配器",
            "order": 0,
        },
    )

    def should_connect(self) -> bool:
        """判断当前配置下是否应当启动连接。"""
        return self.enabled


class PhotonServerConfig(PluginConfigBase):
    """Photon Spectrum 云端连接配置。"""

    __ui_label__ = "Photon 云端"
    __ui_icon__ = "cloud"
    __ui_order__ = 1

    project_id: str = Field(
        default="",
        description="Photon 项目 ID，用于标识你的 Photon 项目。",
        json_schema_extra={
            "hint": "在 Photon 控制台 (app.photon.codes) 创建项目后获取。",
            "label": "项目 ID",
            "order": 0,
            "placeholder": "proj_xxxxxxxx",
        },
    )
    project_secret: str = Field(
        default="",
        description="Photon 项目密钥，用于认证你的 Photon 项目身份。",
        json_schema_extra={
            "hint": "与项目 ID 配套的密钥，请勿泄露。",
            "input_type": "password",
            "label": "项目密钥",
            "order": 1,
            "placeholder": "sk_xxxxxxxx",
        },
    )


class SidecarBridgeConfig(PluginConfigBase):
    """本地 Node.js 侧车桥接配置。"""

    __ui_label__ = "本地桥接"
    __ui_icon__ = "link"
    __ui_order__ = 2

    ws_port: int = Field(
        default=18763,
        description="Python 侧 WebSocket Server 的监听端口，侧车进程会连接此端口。",
        json_schema_extra={
            "hint": "仅监听 127.0.0.1，不会暴露到外网。如端口冲突可更换为其他未占用端口。",
            "label": "桥接端口",
            "order": 0,
        },
    )
    max_retries: int = Field(
        default=3,
        description="侧车进程异常退出后的最大自动重启次数。",
        json_schema_extra={
            "hint": "超过此次数后插件将放弃重启，需手动执行 /imessage_reconnect 或重载插件。",
            "label": "最大重启次数",
            "order": 1,
        },
    )
    retry_interval: float = Field(
        default=3.0,
        description="侧车进程异常退出后，等待多少秒再尝试重启。",
        json_schema_extra={
            "hint": "建议不低于 2 秒，给 Photon 服务端断开检测留出时间。",
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
