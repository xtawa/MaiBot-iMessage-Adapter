"""MaiBot-facing serialization for the local iMessage bridge protocol."""

from __future__ import annotations

from typing import Any


_STRUCTURED_ACTIONS = {
    "send",
    "send_reply",
    "unsend_message",
    "send_reaction",
    "send_audio_message",
    "send_live_photo",
    "send_effect",
    "create_poll",
    "vote_poll",
    "unvote_poll",
    "add_poll_option",
    "send_location",
    "send_link_card",
    "send_vcard",
    "set_chat_background",
    "send_handwriting",
    "send_digital_touch",
    "open_dm",
}


def extract_structured_action(message: dict[str, Any]) -> dict[str, Any] | None:
    """Read an explicit adapter Action from the gateway payload or raw segments."""

    candidate = message.get("imessage_action")
    if candidate is None:
        raw_message = message.get("raw_message")
        if isinstance(raw_message, list):
            for segment in raw_message:
                if not isinstance(segment, dict):
                    continue
                if str(segment.get("type", "")).lower() not in {
                    "imessage_action",
                    "imessage-action",
                }:
                    continue
                candidate = segment.get("data")
                break
    if candidate is None:
        return None
    if not isinstance(candidate, dict):
        raise ValueError("imessage_action 必须是对象")

    action = str(candidate.get("action", "")).strip().lower()
    if action not in _STRUCTURED_ACTIONS:
        raise ValueError(f"不支持的 iMessage Action: {action or '<empty>'}")
    result = dict(candidate)
    result["action"] = action
    return result


def extract_reply_target(raw_message: Any) -> str:
    """Return the first quoted iMessage ID represented by a MaiBot reply segment."""

    if not isinstance(raw_message, list):
        return ""

    def find_id(value: Any, depth: int = 0) -> str:
        if depth > 4 or not isinstance(value, dict):
            return ""
        for key in ("target_message_id", "message_id", "external_message_id", "id"):
            result = str(value.get(key, "") or "").strip()
            if result:
                return result
        for key in ("target", "message", "reply", "data"):
            result = find_id(value.get(key), depth + 1)
            if result:
                return result
        return ""

    for segment in raw_message:
        if not isinstance(segment, dict):
            continue
        if str(segment.get("type", "")).lower() in {"reply", "quote"}:
            result = find_id(segment)
            if result:
                return result
    return ""


def native_event_summary(event: Any) -> str:
    """Make a short visible system message while retaining the full event below."""

    if not isinstance(event, dict):
        return "iMessage 收到无法解析的原生事件"
    kind = str(event.get("event_type", event.get("content_type", "unknown")))
    metadata = event.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    if kind in {"reaction.added", "reaction"}:
        reaction = metadata.get("emoji", "")
        target = metadata.get("target")
        target_id = target.get("id", "") if isinstance(target, dict) else ""
        suffix = f"（目标消息 {target_id}）" if target_id else ""
        return f"[iMessage 系统事件] 收到表情反应 {reaction}{suffix}".strip()
    if kind in {"poll.created", "poll"}:
        title = str(metadata.get("title", "未命名投票"))
        return f"[iMessage 系统事件] 创建了投票：{title}"
    if kind in {"poll.vote", "poll_option"}:
        selected = metadata.get("selected")
        action = "投票" if selected is not False else "撤销投票"
        option = metadata.get("option")
        option_title = option.get("title", "投票选项") if isinstance(option, dict) else metadata.get("title", "投票选项")
        poll = metadata.get("poll")
        poll_title = poll.get("title", "") if isinstance(poll, dict) else ""
        context = f"（{poll_title}）" if poll_title else ""
        return f"[iMessage 系统事件] 有人{action}：{option_title}{context}"
    if kind == "read.updated":
        return "[iMessage 系统事件] 会话已读状态已更新"
    if kind == "delivery.updated":
        return f"[iMessage 系统事件] 消息投递状态：{metadata.get('state', 'unknown')}"
    if kind == "message.unsent":
        return "[iMessage 系统事件] 对方撤回了一条消息"
    if kind == "message.edited":
        return "[iMessage 系统事件] 对方编辑了一条消息"
    return f"[iMessage 系统事件] 收到原生内容：{kind}"


def to_mai_message_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Convert one sidecar envelope without discarding ordered parts or metadata."""

    sender = data.get("sender")
    if not isinstance(sender, dict):
        sender = {}
    chat_id = str(data.get("chat_id", "") or "")
    line_phone = str(data.get("line_phone", "") or "").strip()
    project_id = str(data.get("project_id", "") or "").strip()
    event = data.get("native_event")
    is_system_event = bool(data.get("is_system_event", False))
    text = str(data.get("text", "") or "")
    attachments = data.get("attachments")
    if not isinstance(attachments, list):
        attachments = []

    raw_message: list[dict[str, Any]] = []

    def append_attachment(attachment: Any) -> None:
        if not isinstance(attachment, dict):
            return
        att_type = str(attachment.get("type", "file")).strip().lower()
        data_base64 = str(attachment.get("data_base64", "") or "")
        if att_type == "image":
            image_segment = {
                "type": "image",
                "data": "",
                "binary_data_base64": data_base64,
                "mime_type": attachment.get("mime_type", "image/*"),
                "name": attachment.get("name", ""),
                "hash": "",
            }
            companion_base64 = attachment.get("companion_data_base64")
            if companion_base64:
                image_segment["live_photo_companion_base64"] = companion_base64
                image_segment["live_photo_companion_name"] = attachment.get("companion_name", "")
                image_segment["live_photo_companion_mime_type"] = attachment.get(
                    "companion_mime_type", "video/quicktime"
                )
            raw_message.append(image_segment)
        elif att_type == "voice":
            raw_message.append(
                {
                    "type": "voice",
                    "data": {
                        "binary_data_base64": data_base64,
                        "mime_type": attachment.get("mime_type", "audio/mp4"),
                        "name": attachment.get("name", ""),
                        "duration": attachment.get("duration"),
                    },
                }
            )
        else:
            raw_message.append({"type": "file", "data": dict(attachment)})

    parts = data.get("parts")
    used_parts = isinstance(parts, list) and bool(parts)
    if used_parts:
        for part in parts:
            if not isinstance(part, dict):
                continue
            part_type = str(part.get("type", "")).lower()
            if part_type == "text":
                part_text = str(part.get("text", "") or "")
                if part_text:
                    raw_message.append({"type": "text", "data": part_text})
            elif part_type in {"attachment", "voice"}:
                index = part.get("attachment_index")
                if isinstance(index, int) and 0 <= index < len(attachments):
                    append_attachment(attachments[index])
    else:
        if text:
            raw_message.append({"type": "text", "data": text})
        for attachment in attachments:
            append_attachment(attachment)

    if is_system_event and not text:
        summary = native_event_summary(event)
        raw_message.append({"type": "text", "data": summary})
    if isinstance(event, dict):
        raw_message.append({"type": "imessage_event", "data": event})
    if not raw_message:
        raw_message.append({"type": "text", "data": ""})

    additional_config: dict[str, Any] = {}
    if line_phone not in {"", "shared"}:
        additional_config["platform_io_account_id"] = line_phone
    if project_id:
        additional_config["platform_io_project_id"] = project_id
    if is_system_event:
        additional_config["imessage_system_event"] = True

    group_info = None
    if str(data.get("space_type", "")).lower() == "group":
        group_info = {
            "group_id": chat_id,
            "group_name": str(data.get("space_name", "") or chat_id),
        }
    message_info: dict[str, Any] = {
        "user_info": {
            "user_id": str(sender.get("address", "unknown") or "unknown"),
            "user_nickname": str(
                "iMessage 系统事件"
                if is_system_event
                else sender.get("name", "unknown") or "unknown"
            ),
        },
        "additional_config": additional_config,
    }
    if group_info is not None:
        message_info["group_info"] = group_info

    return {
        "message_id": data.get("message_id") or data.get("event_id", ""),
        "platform": "imessage",
        "session_id": chat_id,
        "message_info": message_info,
        "raw_message": raw_message,
    }
