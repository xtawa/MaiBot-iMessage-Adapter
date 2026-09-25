import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from plugin import IMessageAdapterPlugin, create_plugin
from protocol import (
    FALLBACK_STRUCTURED_ACTIONS,
    NATIVE_STRUCTURED_ACTIONS,
    extract_inline_imessage_action,
    extract_native_event,
    extract_reply_target,
    extract_structured_action,
    native_event_summary,
    to_mai_message_dict,
)


class AdapterProtocolTests(unittest.TestCase):
    def test_ordered_text_and_multiple_media_parts_are_preserved(self):
        event = {
            "schema_version": 1,
            "event_id": "project%3Aline%3Amessage-1",
            "event_type": "message.group",
            "content_type": "group",
            "metadata": {"title": "group title"},
        }
        converted = to_mai_message_dict(
            {
                "message_id": "message-1",
                "project_id": "project-a",
                "line_phone": "+15551234567",
                "chat_id": "any;-;+15550000000",
                "space_type": "group",
                "sender": {"name": "Alice", "address": "+15550000001"},
                "text": "before\nafter",
                "attachments": [
                    {
                        "type": "image",
                        "mime_type": "image/heic",
                        "data_base64": "aW1hZ2U=",
                        "companion_data_base64": "dmlkZW8=",
                        "companion_name": "photo.MOV",
                    },
                    {
                        "type": "voice",
                        "mime_type": "audio/x-caf",
                        "name": "voice.caf",
                        "duration": 3.5,
                        "data_base64": "YXVkaW8=",
                    },
                    {
                        "type": "file",
                        "mime_type": "application/pdf",
                        "name": "doc.pdf",
                        "data_base64": "ZmlsZQ==",
                    },
                    {
                        "type": "video",
                        "mime_type": "video/mp4",
                        "name": "clip.mp4",
                        "data_base64": "dmlkZW8=",
                    },
                ],
                "parts": [
                    {"type": "text", "text": "before"},
                    {"type": "attachment", "attachment_index": 0},
                    {"type": "voice", "attachment_index": 1},
                    {"type": "text", "text": "after"},
                    {"type": "attachment", "attachment_index": 2},
                    {"type": "attachment", "attachment_index": 3},
                ],
                "native_event": event,
            }
        )

        segments = converted["raw_message"]
        # raw_message only contains standard MaiBot segment types; native_event is in additional_config
        self.assertEqual(
            [segment["type"] for segment in segments],
            ["text", "image", "voice", "text", "file", "video"],
        )
        self.assertEqual(segments[0]["data"], "before")
        self.assertEqual(segments[1]["mime_type"], "image/heic")
        self.assertEqual(segments[1]["live_photo_companion_base64"], "dmlkZW8=")
        self.assertEqual(segments[1]["live_photo_companion_name"], "photo.MOV")

        # Issue 1: Voice segment must expose binary_data_base64 at top-level for MaiBot _build_binary_component
        voice_seg = segments[2]
        self.assertEqual(voice_seg["type"], "voice")
        self.assertEqual(voice_seg["binary_data_base64"], "YXVkaW8=")
        self.assertEqual(voice_seg["data"]["binary_data_base64"], "YXVkaW8=")
        self.assertEqual(voice_seg["data"]["base64"], "YXVkaW8=")

        self.assertEqual(segments[3]["data"], "after")

        # Issue 2: File & video segments must expose base64 in data for MaiBot FileComponent.from_payload
        file_seg = segments[4]
        self.assertEqual(file_seg["type"], "file")
        self.assertEqual(file_seg["data"]["base64"], "ZmlsZQ==")
        self.assertEqual(file_seg["binary_data_base64"], "ZmlsZQ==")

        video_seg = segments[5]
        self.assertEqual(video_seg["type"], "video")
        self.assertEqual(video_seg["data"]["base64"], "dmlkZW8=")
        self.assertEqual(video_seg["binary_data_base64"], "dmlkZW8=")

        # Issue 9: native_event is preserved losslessly in additional_config["imessage_event"]
        add_cfg = converted["message_info"]["additional_config"]
        self.assertEqual(add_cfg["platform_io_account_id"], "+15551234567")
        self.assertEqual(add_cfg["platform_io_project_id"], "project-a")
        self.assertEqual(add_cfg["imessage_event"], event)
        self.assertEqual(add_cfg["imessage_event_type"], "message.group")
        self.assertEqual(extract_native_event(converted), event)
        self.assertEqual(converted["message_info"]["group_info"]["group_id"], "any;-;+15550000000")

    def test_native_event_is_visible_and_full_metadata_is_retained(self):
        event = {
            "event_type": "poll.created",
            "content_type": "poll",
            "metadata": {"title": "Dinner", "options": [{"title": "Pizza"}]},
        }
        converted = to_mai_message_dict(
            {
                "message_id": "",
                "event_id": "poll-event-1",
                "chat_id": "any;-;+15550000000",
                "sender": {"address": "+15550000001"},
                "native_event": event,
                "is_system_event": True,
            }
        )
        self.assertEqual(converted["message_id"], "poll-event-1")
        self.assertIn("Dinner", converted["raw_message"][0]["data"])
        self.assertEqual(converted["message_info"]["additional_config"]["imessage_event"], event)
        self.assertEqual(extract_native_event(converted), event)
        self.assertTrue(converted["message_info"]["additional_config"]["imessage_system_event"])
        self.assertIn(
            "Pizza",
            native_event_summary(
                {
                    "event_type": "poll.vote",
                    "metadata": {
                        "selected": True,
                        "option": {"title": "Pizza"},
                        "poll": {"title": "Dinner"},
                    },
                }
            ),
        )
        self.assertIn(
            "22.8152",
            native_event_summary(
                {
                    "event_type": "location.updated",
                    "metadata": {"handle": "+15550000001", "latitude": 22.8152, "longitude": 108.3669},
                }
            ),
        )

    def test_structured_action_and_reply_target_extraction(self):
        self.assertEqual(len(NATIVE_STRUCTURED_ACTIONS), 29)
        self.assertEqual(FALLBACK_STRUCTURED_ACTIONS, {"send_location", "send_handwriting"})

        action = extract_structured_action(
            {
                "raw_message": [
                    {"type": "text", "data": "hello"},
                    {"type": "imessage_action", "data": {"action": "send_effect", "effect": "confetti"}},
                ]
            }
        )
        self.assertEqual(action, {"action": "send_effect", "effect": "confetti"})

        # Fallback actions are explicitly marked with fallback_mode=True
        handwriting_action = extract_structured_action(
            {"imessage_action": {"action": "send_handwriting", "text": "Hi"}}
        )
        self.assertEqual(
            handwriting_action,
            {"action": "send_handwriting", "text": "Hi", "fallback_mode": True},
        )

        # Unsupported Digital Touch outbound action is rejected
        with self.assertRaises(ValueError):
            extract_structured_action({"imessage_action": {"action": "send_digital_touch"}})

        with self.assertRaises(ValueError):
            extract_structured_action({"imessage_action": {"action": "run_shell"}})

        self.assertEqual(
            extract_reply_target(
                [
                    {"type": "reply", "data": {"target": {"message_id": "quoted-42"}}},
                    {"type": "text", "data": "reply"},
                ]
            ),
            "quoted-42",
        )

    def test_inbound_reply_to_prepends_standard_reply_segment(self):
        converted = to_mai_message_dict(
            {
                "message_id": "msg-200",
                "chat_id": "iMessage;-;+15550000001",
                "sender": {"name": "Bob", "address": "+15550000001"},
                "text": "我也觉得！",
                "reply_to": {
                    "message_id": "msg-100",
                    "text": "今晚去吃火锅吗？",
                    "target_user_id": "+15550000002",
                    "target_user_nickname": "Alice",
                },
            }
        )
        segments = converted["raw_message"]
        self.assertEqual(segments[0]["type"], "reply")
        # Issue 10: Both top-level and data include target_message_content for MaiBot ReplyComponent
        self.assertEqual(segments[0]["target_message_id"], "msg-100")
        self.assertEqual(segments[0]["target_message_content"], "今晚去吃火锅吗？")
        self.assertEqual(segments[0]["target_user_id"], "+15550000002")
        self.assertEqual(segments[0]["target_user_nickname"], "Alice")
        self.assertEqual(segments[0]["data"]["target_message_id"], "msg-100")
        self.assertEqual(segments[0]["data"]["target_message_content"], "今晚去吃火锅吗？")
        self.assertEqual(segments[0]["data"]["text"], "今晚去吃火锅吗？")
        self.assertEqual(segments[1], {"type": "text", "data": "我也觉得！"})

    def test_inline_imessage_action_tags_parsing(self):
        cleaned, action = extract_inline_imessage_action("新年快乐！[effect:烟花]")
        self.assertEqual(cleaned, "新年快乐！")
        self.assertEqual(action, {"action": "send_effect", "effect": "烟花", "text": "新年快乐！"})

        cleaned, action = extract_inline_imessage_action("[music:周杰伦-晴天] 送你一首歌")
        self.assertEqual(cleaned, "送你一首歌")
        self.assertEqual(action, {"action": "send_music_card", "query": "周杰伦-晴天", "text": "送你一首歌"})

        cleaned, action = extract_inline_imessage_action("[transfer:520:拿去买奶茶]")
        self.assertEqual(cleaned, "")
        self.assertEqual(action, {"action": "send_transfer_card", "amount": "520", "note": "拿去买奶茶"})

        cleaned, action = extract_inline_imessage_action("[poll:宵夜吃什么|烧烤|火锅|小龙虾]")
        self.assertEqual(cleaned, "")
        self.assertEqual(
            action,
            {
                "action": "create_poll",
                "title": "宵夜吃什么",
                "options": [{"title": "烧烤"}, {"title": "火锅"}, {"title": "小龙虾"}],
            },
        )

        cleaned, action = extract_inline_imessage_action("<thinking>Maybe use [effect:fireworks]</thinking>普通回复")
        self.assertIsNone(action)

    def test_enhanced_native_event_summaries(self):
        self.assertIn(
            "刚刚撤回的内容",
            native_event_summary(
                {
                    "event_type": "message.unsent",
                    "metadata": {"unsent_text": "刚刚撤回的内容"},
                }
            ),
        )
        self.assertIn(
            "修改后文本",
            native_event_summary(
                {
                    "event_type": "message.edited",
                    "metadata": {"previous_text": "旧文本", "new_text": "修改后文本"},
                }
            ),
        )
        self.assertIn(
            "¥520.00",
            native_event_summary(
                {
                    "event_type": "transfer.claimed",
                    "metadata": {"amount": "¥520.00", "note": "红包"},
                }
            ),
        )

    def test_real_maibot_sdk_components_and_multi_session_safety(self):
        plugin: IMessageAdapterPlugin = create_plugin()
        components = plugin.get_components()
        component_names = {
            c.get("name") if isinstance(c, dict) else getattr(c, "name", None)
            for c in components
        }
        expected_tools = {
            "imessage_send_reaction",
            "imessage_reply_or_edit_message",
            "imessage_send_effect",
            "imessage_poll",
            "imessage_send_card",
            "imessage_chat_and_group",
            "imessage_location_and_check",
        }
        self.assertTrue(expected_tools.issubset(component_names))
        self.assertIn("imessage", component_names)
        self.assertIsInstance(plugin.get_default_config(), dict)

        # Single active session: auto-resolves safely
        plugin._chat_states = {
            "iMessage;-;+8613800138000": {
                "chat_id": "iMessage;-;+8613800138000",
                "line_phone": "+15550001111",
                "project_id": "proj-1",
                "last_inbound_message_id": "msg-a1",
            }
        }
        single_ctx = plugin._resolve_active_chat_context("")
        self.assertEqual(single_ctx["chat_id"], "iMessage;-;+8613800138000")

        # Multiple active sessions: refuses to guess when chat_id is empty (Issue 3)
        plugin._chat_states["iMessage;-;+8613900139000"] = {
            "chat_id": "iMessage;-;+8613900139000",
            "line_phone": "+15550001111",
            "project_id": "proj-1",
            "last_inbound_message_id": "msg-b1",
        }
        with self.assertRaises(ValueError):
            plugin._resolve_active_chat_context("")

        # Explicit phone suffix or full chat_id resolves accurately even with multiple sessions
        matched_ctx = plugin._resolve_active_chat_context("+8613900139000")
        self.assertEqual(matched_ctx["chat_id"], "iMessage;-;+8613900139000")
        self.assertEqual(matched_ctx["last_inbound_message_id"], "msg-b1")

        # Tool invocation with omitted chat_id under multiple sessions returns explicit refusal message
        res_text = asyncio.run(plugin.tool_send_reaction(reaction="❤️", chat_id=""))
        self.assertIn("多会话串台", res_text)

    def test_tool_poll_and_find_my_and_typing_stop_lifecycle(self):
        plugin: IMessageAdapterPlugin = create_plugin()
        plugin._gateway_ready = True
        plugin._chat_states = {
            "iMessage;-;+8613800138000": {
                "chat_id": "iMessage;-;+8613800138000",
                "line_phone": "+15550001111",
                "project_id": "proj-1",
                "last_inbound_message_id": "msg-a1",
                "latest_poll_message_id": "poll-123",
            }
        }
        sent_frames = []

        async def fake_send(raw_json: str):
            import json as _json

            frame = _json.loads(raw_json)
            sent_frames.append(frame)
            req_id = frame.get("request_id")
            fut = plugin._pending_sends.get(req_id)
            if fut and not fut.done():
                fut.set_result({"success": True, "external_message_id": "out-1", "action_metadata": {"ok": True}})

        mock_ws = MagicMock()
        mock_ws.send = AsyncMock(side_effect=fake_send)
        plugin._bridge_ws = mock_ws
        plugin._context = MagicMock()
        plugin.set_plugin_config(plugin.get_default_config())

        # Issue 4: tool_poll add_option sends title, option, and option_text
        asyncio.run(
            plugin.tool_poll(
                operation="add_option",
                title_or_option="小龙虾",
                chat_id="iMessage;-;+8613800138000",
            )
        )
        poll_frame = sent_frames[-1]["data"]
        self.assertEqual(poll_frame["action"], "add_poll_option")
        self.assertEqual(poll_frame["title"], "小龙虾")
        self.assertEqual(poll_frame["option"], "小龙虾")
        self.assertEqual(poll_frame["option_text"], "小龙虾")

        # Issue 6: tool_location_and_check find_my sends both refresh=True and operation="refresh"
        asyncio.run(
            plugin.tool_location_and_check(
                operation="find_my",
                target_or_place="+8613800138000",
                chat_id="iMessage;-;+8613800138000",
            )
        )
        find_my_frame = sent_frames[-1]["data"]
        self.assertEqual(find_my_frame["action"], "find_my_location")
        self.assertTrue(find_my_frame["refresh"])
        self.assertEqual(find_my_frame["operation"], "refresh")

        # Issue 8: Auto typing indicator is stopped in send_to_imessage finally block
        asyncio.run(plugin._start_typing_indicator("iMessage;-;+8613800138000"))
        self.assertIn("iMessage;-;+8613800138000", plugin._active_typing_chats)
        asyncio.run(
            plugin.send_to_imessage(
                {
                    "session_id": "iMessage;-;+8613800138000",
                    "raw_message": [{"type": "text", "data": "思考完毕的回复"}],
                }
            )
        )
        self.assertNotIn("iMessage;-;+8613800138000", plugin._active_typing_chats)
        stop_frame = sent_frames[-1]["data"]
        self.assertEqual(stop_frame["action"], "set_typing")
        self.assertFalse(stop_frame["typing"])


if __name__ == "__main__":
    unittest.main()
