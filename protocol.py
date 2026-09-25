"""MaiBot-facing serialization and inline tag parsing for the local iMessage bridge protocol."""

from __future__ import annotations

import re
from typing import Any


NATIVE_STRUCTURED_ACTIONS = frozenset(
    {
        "send",
        "send_reply",
        "edit_message",
        "unsend_message",
        "send_reaction",
        "remove_reaction",
        "send_audio_message",
        "send_live_photo",
        "send_effect",
        "create_poll",
        "vote_poll",
        "unvote_poll",
        "add_poll_option",
        "send_link_card",
        "send_music_card",
        "send_transfer_card",
        "update_transfer_card",
        "send_vcard",
        "share_my_contact",
        "set_chat_background",
        "place_sticker",
        "set_typing",
        "mark_read",
        "notify_silenced",
        "manage_group",
        "find_my_location",
        "check_imessage_availability",
        "enroll_shared_user",
        "open_dm",
    }
)

FALLBACK_STRUCTURED_ACTIONS = frozenset(
    {
        "send_location",
        "send_handwriting",
    }
)

_STRUCTURED_ACTIONS = NATIVE_STRUCTURED_ACTIONS | FALLBACK_STRUCTURED_ACTIONS

_XML_BLOCK_RE = re.compile(
    r"<(thinking|thought|analysis|scratchpad)\b[^>]*>.*?</\1>",
    re.IGNORECASE | re.DOTALL,
)

_INLINE_TAG_RE = re.compile(
    r"[\[\［【]\s*(?:"
    r"(?:message_effect|effect|消息特效|特效|效果)\s*[:：]\s*(?P<effect>[^\]\］】]{1,40})|"
    r"(?:text_effect|文字动效|字效)\s*[:：]\s*(?P<text_effect>[^\]\］】]{1,40})|"
    r"(?:reaction|react|tapback|回应)\s*[:：]\s*(?P<react>[^\]\］】]{1,80})|"
    r"(?:music|song|音乐|歌曲|点歌)\s*[:：]\s*(?P<music>[^\]\］】]{1,120})|"
    r"(?:share_location|location|位置|定位|共享位置)\s*[:：]\s*(?P<location>[^\]\］】]{1,160})|"
    r"(?:card|link_card|卡片|链接卡片)\s*[:：]\s*(?P<card>https?://[^\]\］】\s]{1,500})|"
    r"(?:transfer_money|transfer|转账|转钱)\s*[:：]\s*(?P<transfer>\d[^\]\］】]{0,80})|"
    r"(?:poll_add|add_option|加选项|添加选项)\s*[:：]\s*(?P<poll_add>[^\]\］】]{1,120})|"
    r"(?:poll_vote|vote|投票|投)\s*[:：]\s*(?P<vote>[^\]\］】]{1,60})|"
    r"(?:create_poll|poll|发起投票|投票发起)\s*[:：]\s*(?P<poll>[^\]\］】]{1,400})|"
    r"(?:reply|引用回复|引用)\s*[:：]\s*(?P<reply>[^\]\］】]{1,200})|"
    r"(?:edit|编辑消息|修改消息)\s*[:：]\s*(?P<edit>[^\]\］】]{1,400})|"
    r"(?P<unsend>(?:undosend|undo_send|unsend|撤回消息|撤回)(?:\s*[:：]\s*[^\]\］】]{0,80})?)|"
    r"(?P<leave_on_read>leave[\s_-]*on[\s_-]*read|已读不回)"
    r")\s*[\]\］】]",
    re.IGNORECASE,
)


def extract_structured_action(message: dict[str, Any]) -> dict[str, Any] | None:
    """Read an explicit adapter Action from the gateway payload or raw segments."""

    candidate = message.get("imessage_action")
    if candidate is None:
        message_info = message.get("message_info")
        if isinstance(message_info, dict):
            add_cfg = message_info.get("additional_config")
            if isinstance(add_cfg, dict):
                candidate = add_cfg.get("imessage_action")
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
    if action == "send_digital_touch":
        raise ValueError(
            "Digital Touch 出站发送未获 Photon/Spectrum SDK 公共接口支持（仅支持入站识别与内嵌图像提取）"
        )
    if action not in _STRUCTURED_ACTIONS:
        raise ValueError(f"不支持的 iMessage Action: {action or '<empty>'}")
    result = dict(candidate)
    result["action"] = action
    if action in FALLBACK_STRUCTURED_ACTIONS:
        result.setdefault("fallback_mode", True)
    return result


def extract_inline_imessage_action(text: str) -> tuple[str, dict[str, Any] | None]:
    """Parse optional inline iMessage bracket tags in outbound text while skipping <thinking> blocks."""

    if not text or ("[" not in text and "［" not in text and "【" not in text):
        return text, None

    xml_spans = [m.span() for m in _XML_BLOCK_RE.finditer(text)]

    def in_xml(pos: int) -> bool:
        return any(start <= pos < end for start, end in xml_spans)

    extracted_action: dict[str, Any] | None = None
    out_chunks: list[str] = []
    cursor = 0

    for match in _INLINE_TAG_RE.finditer(text):
        if in_xml(match.start()):
            continue
        out_chunks.append(text[cursor : match.start()])
        cursor = match.end()

        if extracted_action is not None:
            continue

        groups = match.groupdict()
        if groups.get("effect"):
            extracted_action = {
                "action": "send_effect",
                "effect": groups["effect"].strip(),
            }
        elif groups.get("text_effect"):
            extracted_action = {
                "action": "send",
                "text_effect": groups["text_effect"].strip().lower(),
            }
        elif groups.get("react"):
            body = groups["react"].strip()
            parts = re.split(r"[:：]", body, maxsplit=1)
            emoji = parts[0].strip()
            target = parts[1].strip() if len(parts) > 1 else "last"
            extracted_action = {
                "action": "send_reaction",
                "emoji": emoji,
                "target_message_id": target,
            }
        elif groups.get("music"):
            extracted_action = {
                "action": "send_music_card",
                "query": groups["music"].strip(),
            }
        elif groups.get("location"):
            body = groups["location"].strip()
            last_colon = max(body.rfind(":"), body.rfind("："))
            tail = body[last_colon + 1 :].strip() if last_colon >= 0 else ""
            coord_match = re.match(
                r"^(-?\d{1,3}(?:\.\d+)?)\s*[,，]\s*(-?\d{1,3}(?:\.\d+)?)$",
                tail,
            )
            if coord_match and last_colon >= 0:
                extracted_action = {
                    "action": "send_location",
                    "label": body[:last_colon].strip(),
                    "latitude": float(coord_match.group(1)),
                    "longitude": float(coord_match.group(2)),
                }
            else:
                extracted_action = {
                    "action": "send_location",
                    "label": body,
                }
        elif groups.get("card"):
            extracted_action = {
                "action": "send_link_card",
                "url": groups["card"].strip(),
            }
        elif groups.get("transfer"):
            body = groups["transfer"].strip()
            parts = re.split(r"[:：]", body, maxsplit=1)
            amount = parts[0].strip()
            note = parts[1].strip() if len(parts) > 1 else "转账"
            extracted_action = {
                "action": "send_transfer_card",
                "amount": amount,
                "note": note,
            }
        elif groups.get("poll_add"):
            extracted_action = {
                "action": "add_poll_option",
                "title": groups["poll_add"].strip(),
            }
        elif groups.get("vote"):
            extracted_action = {
                "action": "vote_poll",
                "option_id": groups["vote"].strip(),
            }
        elif groups.get("poll"):
            segs = [
                s.strip()
                for s in re.split(r"[|｜]", groups["poll"].strip())
                if s.strip()
            ]
            if len(segs) >= 2:
                extracted_action = {
                    "action": "create_poll",
                    "title": segs[0],
                    "options": [{"title": item} for item in segs[1:]],
                }
        elif groups.get("reply"):
            extracted_action = {
                "action": "send_reply",
                "reply_to_message_id": groups["reply"].strip(),
            }
        elif groups.get("edit"):
            extracted_action = {
                "action": "edit_message",
                "new_text": groups["edit"].strip(),
            }
        elif groups.get("unsend"):
            raw_unsend = groups["unsend"].strip()
            parts = re.split(r"[:：]", raw_unsend, maxsplit=1)
            target = parts[1].strip() if len(parts) > 1 and parts[1].strip() else "last"
            extracted_action = {
                "action": "unsend_message",
                "message_id": target,
            }
        elif groups.get("leave_on_read"):
            extracted_action = {
                "action": "mark_read",
            }

    out_chunks.append(text[cursor:])
    cleaned_text = "".join(out_chunks).strip()
    if extracted_action is not None and extracted_action["action"] in {
        "send",
        "send_reply",
        "send_effect",
        "send_music_card",
        "send_link_card",
        "send_location",
        "send_transfer_card",
    }:
        if cleaned_text:
            extracted_action["text"] = cleaned_text

    return cleaned_text, extracted_action


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

    if kind == "transfer.claimed" or isinstance(metadata.get("transfer_claimed"), dict):
        claimed = metadata.get("transfer_claimed") if isinstance(metadata.get("transfer_claimed"), dict) else metadata
        amount = claimed.get("amount", "")
        note = claimed.get("note", "")
        return f"[iMessage 系统事件] 对方收下了你的转账 {amount}{f'（{note}）' if note else ''}".strip()

    if kind in {"reaction.added", "reaction"}:
        reaction = metadata.get("emoji", "")
        target = metadata.get("target")
        target_id = target.get("id", "") if isinstance(target, dict) else ""
        suffix = f"（目标消息 {target_id}）" if target_id else ""
        return f"[iMessage 系统事件] 收到表情反应 {reaction}{suffix}".strip()

    if kind == "reaction.removed":
        reaction = metadata.get("emoji", "")
        return f"[iMessage 系统事件] 对方移除了表情反应 {reaction}".strip()

    if kind in {"poll.created", "poll"}:
        title = str(metadata.get("title", "未命名投票"))
        options = metadata.get("options")
        if isinstance(options, list) and options:
            opt_texts = []
            for idx, opt in enumerate(options):
                if isinstance(opt, dict):
                    letter = opt.get("letter") or chr(65 + idx)
                    lbl = opt.get("title") or opt.get("text") or ""
                    if lbl:
                        opt_texts.append(f"{letter}.{lbl}")
            if opt_texts:
                return f"[iMessage 系统事件] 创建了投票：{title}（选项：{' / '.join(opt_texts)}）"
        return f"[iMessage 系统事件] 创建了投票：{title}"

    if kind == "poll.option_added":
        title = str(metadata.get("title", "投票"))
        options = metadata.get("options")
        if isinstance(options, list) and options:
            last_opt = options[-1]
            last_title = last_opt.get("title", "") if isinstance(last_opt, dict) else ""
            if last_title:
                return f"[iMessage 系统事件] 投票「{title}」新增了选项：{last_title}"
        return f"[iMessage 系统事件] 投票「{title}」新增了选项"

    if kind in {"poll.vote", "poll_option"}:
        selected = metadata.get("selected")
        action = "投票" if selected is not False else "撤销投票"
        option = metadata.get("option")
        option_title = (
            option.get("title", "投票选项")
            if isinstance(option, dict)
            else metadata.get("title", "投票选项")
        )
        poll = metadata.get("poll")
        poll_title = poll.get("title", "") if isinstance(poll, dict) else ""
        context = f"（{poll_title}）" if poll_title else ""
        return f"[iMessage 系统事件] 有人{action}：{option_title}{context}"

    if kind == "read.updated":
        return "[iMessage 系统事件] 会话已读状态已更新"

    if kind == "delivery.updated":
        state = metadata.get("state", "unknown")
        peer_avail = metadata.get("peer_imessage_available")
        if peer_avail is False:
            return f"[iMessage 系统事件] 消息投递告警：{state}（对方号码未开启 iMessage 或仅支持普通短信）"
        return f"[iMessage 系统事件] 消息投递状态：{state}"

    if kind == "message.unsent":
        unsent_text = str(metadata.get("unsent_text", "") or "").strip()
        if unsent_text:
            return f"[iMessage 系统事件] 对方撤回了一条消息（撤回前内容：“{unsent_text}”）"
        return "[iMessage 系统事件] 对方撤回了一条消息"

    if kind == "message.edited":
        new_text = str(metadata.get("new_text", "") or "").strip()
        prev_text = str(metadata.get("previous_text", "") or "").strip()
        if new_text and prev_text:
            return f"[iMessage 系统事件] 对方将消息“{prev_text}”编辑为：“{new_text}”"
        if new_text:
            return f"[iMessage 系统事件] 对方编辑了一条消息为：“{new_text}”"
        return "[iMessage 系统事件] 对方编辑了一条消息"

    if kind == "chat.background_changed":
        return "[iMessage 系统事件] 对方更换了当前聊天背景壁纸"

    if kind == "chat.background_removed":
        return "[iMessage 系统事件] 对方移除了当前聊天背景壁纸"

    if kind == "group.changed":
        change = metadata.get("change")
        if isinstance(change, dict):
            change_type = change.get("type", "")
            if change_type == "displayNameChanged":
                return f"[iMessage 系统事件] 群聊名称已修改为：{change.get('displayName', '')}"
            if change_type == "participantAdded":
                return "[iMessage 系统事件] 有新成员加入了群聊"
            if change_type in {"participantRemoved", "participantLeft"}:
                return "[iMessage 系统事件] 有成员离开了群聊"
            if change_type in {"iconChanged", "iconRemoved"}:
                return "[iMessage 系统事件] 群聊头像已更新"
        return "[iMessage 系统事件] 群聊状态已更新"

    if kind == "sticker.placed":
        return "[iMessage 系统事件] 对方在消息气泡上贴了一张贴纸"

    if kind == "location.updated":
        loc = metadata.get("location") if isinstance(metadata.get("location"), dict) else metadata
        handle = str(loc.get("handle") or loc.get("address") or loc.get("sender") or "").strip()
        lat = loc.get("latitude")
        lng = loc.get("longitude")
        coord_str = f" ({lat}, {lng})" if lat is not None and lng is not None else ""
        return f"[iMessage Find My 位置更新] {handle or '联系人'}更新了实时位置{coord_str}"

    return f"[iMessage 系统事件] 收到原生内容：{kind}"


def extract_native_event(message: dict[str, Any]) -> dict[str, Any] | None:
    """Extract the structured iMessage native event from additional_config (or legacy raw_message)."""
    if not isinstance(message, dict):
        return None
    message_info = message.get("message_info")
    if isinstance(message_info, dict):
        additional_config = message_info.get("additional_config")
        if isinstance(additional_config, dict):
            ev = additional_config.get("imessage_event")
            if isinstance(ev, dict):
                return ev
    raw_message = message.get("raw_message")
    if isinstance(raw_message, list):
        for seg in raw_message:
            if isinstance(seg, dict) and seg.get("type") == "imessage_event" and isinstance(seg.get("data"), dict):
                return seg["data"]
    return None


def to_mai_message_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Convert one sidecar envelope into a MaiBot-compatible message dictionary."""

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

    # ReplyComponent reads its sender metadata from the nested data payload.
    reply_to = data.get("reply_to")
    if not isinstance(reply_to, dict) and isinstance(event, dict):
        ev_meta = event.get("metadata")
        if (
            event.get("content_type") == "reply"
            and isinstance(ev_meta, dict)
            and isinstance(ev_meta.get("target"), dict)
        ):
            target_obj = ev_meta["target"]
            target_id = str(target_obj.get("id", "") or "").strip()
            if target_id:
                reply_to = {
                    "target_message_id": target_id,
                    "text": str(target_obj.get("text", "") or "").strip(),
                }
    if isinstance(reply_to, dict):
        target_id = str(
            reply_to.get("target_message_id")
            or reply_to.get("message_id")
            or ""
        ).strip()
        if target_id:
            quoted_text = str(
                reply_to.get("target_message_content")
                or reply_to.get("content")
                or reply_to.get("text")
                or ""
            ).strip()
            reply_sender = reply_to.get("sender") if isinstance(reply_to.get("sender"), dict) else {}
            reply_sender_id = str(
                reply_to.get("target_message_sender_id")
                or reply_to.get("target_user_id")
                or reply_to.get("sender_id")
                or reply_to.get("user_id")
                or reply_sender.get("address")
                or ""
            ).strip()
            reply_sender_name = str(
                reply_to.get("target_message_sender_nickname")
                or reply_to.get("target_user_nickname")
                or reply_to.get("sender_name")
                or reply_to.get("nickname")
                or reply_sender.get("name")
                or reply_sender_id
                or ""
            ).strip()
            reply_sender_cardname = str(
                reply_to.get("target_message_sender_cardname")
                or reply_to.get("target_user_cardname")
                or reply_to.get("sender_cardname")
                or reply_sender.get("cardname")
                or ""
            ).strip()
            reply_seg_data: dict[str, Any] = {
                "target_message_id": target_id,
                "message_id": target_id,
                "id": target_id,
                "target_message_content": quoted_text,
            }
            if quoted_text:
                reply_seg_data["content"] = quoted_text
                reply_seg_data["text"] = quoted_text
            if reply_sender_id:
                reply_seg_data["target_message_sender_id"] = reply_sender_id
            if reply_sender_name:
                reply_seg_data["target_message_sender_nickname"] = reply_sender_name
            if reply_sender_cardname:
                reply_seg_data["target_message_sender_cardname"] = reply_sender_cardname
            raw_message.append(
                {
                    "type": "reply",
                    "target_message_id": target_id,
                    "target_message_content": quoted_text,
                    "data": reply_seg_data,
                }
            )

    def append_attachment(attachment: Any) -> None:
        if not isinstance(attachment, dict):
            return
        att_type = str(attachment.get("type", "file")).strip().lower()
        data_base64 = str(
            attachment.get("data_base64")
            or attachment.get("base64")
            or attachment.get("binary_data_base64")
            or ""
        )
        mime_type = str(
            attachment.get("mime_type")
            or (
                "image/png"
                if att_type == "image"
                else "audio/mp4"
                if att_type == "voice"
                else "video/mp4"
                if att_type == "video"
                else "application/octet-stream"
            )
        )
        file_name = str(attachment.get("name", "") or "")
        if att_type == "image":
            image_segment = {
                "type": "image",
                "data": "",
                "binary_data_base64": data_base64,
                "base64": data_base64,
                "mime_type": mime_type,
                "name": file_name,
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
            # MaiBot reads data as VoiceComponent.content and binary_data_base64 as its bytes.
            duration = attachment.get("duration")
            voice_segment: dict[str, Any] = {
                "type": "voice",
                "binary_data_base64": data_base64,
                "base64": data_base64,
                "mime_type": mime_type,
                "name": file_name,
                "duration": duration,
                "data": "",
            }
            raw_message.append(voice_segment)
        else:
            # MaiBot has no video segment branch; video uses FileComponent with a video MIME type.
            file_payload = dict(attachment)
            file_payload["base64"] = data_base64
            file_payload["binary_data_base64"] = data_base64
            file_payload["data_base64"] = data_base64
            file_payload["mime_type"] = mime_type
            file_payload["name"] = file_name
            raw_message.append(
                {
                    "type": "file",
                    "name": file_name,
                    "mime_type": mime_type,
                    "base64": data_base64,
                    "binary_data_base64": data_base64,
                    "data": file_payload,
                }
            )

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
    if not raw_message:
        raw_message.append({"type": "text", "data": ""})

    additional_config: dict[str, Any] = {}
    if line_phone not in {"", "shared"}:
        additional_config["platform_io_account_id"] = line_phone
    if project_id:
        additional_config["platform_io_project_id"] = project_id
    if chat_id:
        additional_config["imessage_chat_id"] = chat_id
    resolved_msg_id = str(data.get("message_id") or data.get("event_id", "") or "")
    if resolved_msg_id:
        additional_config["imessage_message_id"] = resolved_msg_id
    if is_system_event:
        additional_config["imessage_system_event"] = True
    # Issue 9: 将原生事件稳定保存在 message_info.additional_config["imessage_event"] 中，
    # 避免 MaiBot 将未知 raw_message type="imessage_event" 降级为 DictComponent 后丢失类型标识。
    if isinstance(event, dict):
        additional_config["imessage_event"] = event
        additional_config["imessage_event_type"] = str(event.get("event_type", "") or "")

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
        "message_id": resolved_msg_id,
        "platform": "imessage",
        "session_id": chat_id,
        "message_info": message_info,
        "raw_message": raw_message,
    }
