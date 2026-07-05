"""iMessage 适配器调试工具。

插件初始化完毕后自动向指定号码发送一条测试消息。
将下方常量填写完成后，重启插件即可自动发送。
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

# ═══════════════════════════════════════════════════════════════
# 调试常量 — 按需修改
# ═══════════════════════════════════════════════════════════════

# 是否启用调试发送（设为 False 可临时关闭）
ENABLED = True

# 目标 chat_id（iMessage 的 space.id，即手机号，带国家代码）
# 格式示例: "+8613800138000"（sidecar 会自动补 any;-; 前缀）
TARGET_CHAT_ID = "+447591971774"

# 要发送的文本内容
MESSAGE_TEXT = "Hello from MaiBot iMessage Adapter (debug)"

# 网关就绪后延迟多少秒再发送（确保 Photon 完全初始化）
DELAY_SECONDS = 3

# ═══════════════════════════════════════════════════════════════
# 发送逻辑 — 无需修改
# ═══════════════════════════════════════════════════════════════

logger = logging.getLogger("plugin.com.galeros.imessage_adapter.debug")


def _validate() -> bool:
    """校验调试常量是否合法。"""
    if not ENABLED:
        return False
    if not TARGET_CHAT_ID.strip():
        logger.warning("[debug] TARGET_CHAT_ID 为空，跳过调试发送")
        return False
    if not MESSAGE_TEXT.strip():
        logger.warning("[debug] MESSAGE_TEXT 为空，跳过调试发送")
        return False
    return True


async def send_debug_message(
    *,
    bridge_ws: Any,
    gateway_ready: bool,
) -> None:
    """在网关就绪后向目标号码发送一条调试消息。

    在 ``on_load`` 中、iMessage 网关就绪后调用。
    满足以下条件才会发送：
    - ``ENABLED`` 为 True
    - ``TARGET_CHAT_ID`` 已填写
    - 网关已就绪（``gateway_ready`` 为 True）
    - ``bridge_ws`` 可用

    Args:
        bridge_ws: 已认证的 WebSocket 连接对象。
        gateway_ready: 网关是否已就绪。
    """

    if not _validate():
        return

    if not gateway_ready:
        logger.warning("[debug] 网关未就绪，跳过调试发送")
        return

    if bridge_ws is None:
        logger.warning("[debug] bridge_ws 为空，跳过调试发送")
        return

    if DELAY_SECONDS > 0:
        logger.info("[debug] 等待 %s 秒后发送调试消息…", DELAY_SECONDS)
        await asyncio.sleep(DELAY_SECONDS)

    payload = {
        "type": "send",
        "data": {
            "chat_id": TARGET_CHAT_ID.strip(),
            "text": MESSAGE_TEXT.strip(),
            "attachments": [],
        },
    }

    try:
        await bridge_ws.send(json.dumps(payload, ensure_ascii=False))
        logger.info(
            "[debug] 调试消息已发送 → %s: %s",
            TARGET_CHAT_ID,
            MESSAGE_TEXT,
        )
    except Exception as exc:
        logger.error("[debug] 调试消息发送失败: %s", exc)
