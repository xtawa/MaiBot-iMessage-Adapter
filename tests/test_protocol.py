import unittest

from protocol import (
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
                    {"type": "voice", "mime_type": "audio/x-caf", "data_base64": "YXVkaW8="},
                    {"type": "file", "mime_type": "application/pdf", "data_base64": "ZmlsZQ=="},
                ],
                "parts": [
                    {"type": "text", "text": "before"},
                    {"type": "attachment", "attachment_index": 0},
                    {"type": "voice", "attachment_index": 1},
                    {"type": "text", "text": "after"},
                    {"type": "attachment", "attachment_index": 2},
                ],
                "native_event": event,
            }
        )

        segments = converted["raw_message"]
        self.assertEqual(
            [segment["type"] for segment in segments],
            ["text", "image", "voice", "text", "file", "imessage_event"],
        )
        self.assertEqual(segments[0]["data"], "before")
        self.assertEqual(segments[1]["mime_type"], "image/heic")
        self.assertEqual(segments[1]["live_photo_companion_base64"], "dmlkZW8=")
        self.assertEqual(segments[1]["live_photo_companion_name"], "photo.MOV")
        self.assertEqual(segments[2]["type"], "voice")
        self.assertEqual(segments[3]["data"], "after")
        self.assertEqual(segments[-1]["data"], event)
        self.assertEqual(converted["message_info"]["additional_config"], {
            "platform_io_account_id": "+15551234567",
            "platform_io_project_id": "project-a",
        })
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
        self.assertEqual(converted["raw_message"][-1], {"type": "imessage_event", "data": event})
        self.assertTrue(converted["message_info"]["additional_config"]["imessage_system_event"])
        self.assertIn("Pizza", native_event_summary({
            "event_type": "poll.vote",
            "metadata": {
                "selected": True,
                "option": {"title": "Pizza"},
                "poll": {"title": "Dinner"},
            },
        }))

    def test_structured_action_and_reply_target_extraction(self):
        action = extract_structured_action({
            "raw_message": [
                {"type": "text", "data": "hello"},
                {"type": "imessage_action", "data": {"action": "send_effect", "effect": "confetti"}},
            ]
        })
        self.assertEqual(action, {"action": "send_effect", "effect": "confetti"})
        self.assertEqual(
            extract_structured_action({"imessage_action": {"action": "send_live_photo"}}),
            {"action": "send_live_photo"},
        )
        self.assertEqual(
            extract_reply_target([
                {"type": "reply", "data": {"target": {"message_id": "quoted-42"}}},
                {"type": "text", "data": "reply"},
            ]),
            "quoted-42",
        )
        with self.assertRaises(ValueError):
            extract_structured_action({"imessage_action": {"action": "run_shell"}})


if __name__ == "__main__":
    unittest.main()
