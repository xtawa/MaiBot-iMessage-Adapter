"""Exercise the pinned MaiBot Host parser and component implementations.

CI checks out the Host source at MAIBOT_HOST_SOURCE. Only import-time dependencies
are replaced; the component and parser class bodies come from the Host checkout.
"""

import ast
import asyncio
import base64
import hashlib
import json
import os
from abc import ABC, abstractmethod
from copy import deepcopy
from pathlib import Path
import unittest
from unittest.mock import MagicMock
from unittest.mock import AsyncMock

from plugin import create_plugin
from protocol import to_mai_message_dict


HOST_SOURCE = os.environ.get("MAIBOT_HOST_SOURCE", "")


def _host_classes(path: Path, names: set[str], namespace: dict) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name in names]
    found = {node.name for node in classes}
    if found != names:
        raise AssertionError(f"MaiBot Host classes changed: missing {names - found}")
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *classes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), "exec"), namespace)


@unittest.skipUnless(HOST_SOURCE, "set MAIBOT_HOST_SOURCE to run Host round-trip tests")
class MaiBotHostRoundtripTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(HOST_SOURCE)
        component_path = root / "src/common/data_models/message_component_data_model.py"
        parser_path = root / "src/plugin_runtime/host/message_utils.py"
        session_path = root / "src/common/utils/utils_session.py"
        for path in (component_path, parser_path, session_path):
            if not path.is_file():
                raise AssertionError(f"MaiBot Host source missing: {path}")
        namespace = {
            "ABC": ABC, "abstractmethod": abstractmethod, "base64": base64,
            "hashlib": hashlib, "deepcopy": deepcopy, "MagicMock": MagicMock,
            "logger": MagicMock(), "Seg": MagicMock(),
        }
        component_names = {
            "BaseMessageComponentModel", "ByteComponent", "TextComponent", "ImageComponent",
            "EmojiComponent", "VoiceComponent", "FileComponent", "AtComponent",
            "ReplyComponent", "ForwardNodeComponent", "DictComponent",
            "ForwardComponent", "MessageSequence",
        }
        _host_classes(component_path, component_names, namespace)
        _host_classes(parser_path, {"PluginMessageUtils"}, namespace)
        _host_classes(session_path, {"SessionUtils"}, namespace)
        cls.host = namespace

    def test_voice_file_video_and_reply_survive_host_roundtrip(self):
        converted = to_mai_message_dict({
            "message_id": "incoming-1", "chat_id": "iMessage;-;+15550000001",
            "sender": {"address": "+15550000001"}, "text": "response",
            "reply_to": {
                "message_id": "quoted-1", "text": "quoted text",
                "target_user_id": "+15550000002", "target_user_nickname": "Alice",
                "target_user_cardname": "Alice C",
            },
            "attachments": [
                {"type": "voice", "name": "voice.caf", "mime_type": "audio/x-caf", "data_base64": "YXVkaW8="},
                {"type": "file", "name": "doc.pdf", "mime_type": "application/pdf", "data_base64": "ZmlsZQ=="},
                {"type": "video", "name": "clip.mp4", "mime_type": "video/mp4", "data_base64": "dmlkZW8="},
            ],
        })
        utils = self.host["PluginMessageUtils"]
        sequence = utils._message_sequence_from_dict(converted["raw_message"])
        reply, text, voice, file, video = sequence.components
        self.assertIsInstance(reply, self.host["ReplyComponent"])
        self.assertEqual(reply.target_message_id, "quoted-1")
        self.assertEqual(reply.target_message_content, "quoted text")
        self.assertEqual(reply.target_message_sender_id, "+15550000002")
        self.assertEqual(reply.target_message_sender_nickname, "Alice")
        self.assertEqual(reply.target_message_sender_cardname, "Alice C")
        self.assertIsInstance(text, self.host["TextComponent"])
        self.assertIsInstance(voice, self.host["VoiceComponent"])
        self.assertEqual(voice.content, "")
        self.assertEqual(voice.binary_data, b"audio")
        for component, expected_name, expected_mime, expected_base64 in (
            (file, "doc.pdf", "application/pdf", "ZmlsZQ=="),
            (video, "clip.mp4", "video/mp4", "dmlkZW8="),
        ):
            self.assertIsInstance(component, self.host["FileComponent"])
            self.assertEqual(component.name, expected_name)
            self.assertEqual(component.mime_type, expected_mime)
            self.assertEqual(component.base64_data, expected_base64)
        serialized = utils._message_sequence_to_dict(sequence)
        self.assertEqual(serialized[2]["data"], "")
        self.assertEqual(serialized[2]["binary_data_base64"], "YXVkaW8=")
        self.assertEqual(serialized[4]["data"]["mime_type"], "video/mp4")
        self.assertEqual(serialized[0]["data"]["target_message_sender_id"], "+15550000002")

        plugin = create_plugin()
        plugin._gateway_ready = True
        plugin._context = MagicMock()
        plugin.set_plugin_config(plugin.get_default_config())
        plugin._pending_sends = {}
        plugin._active_typing_chats = set()
        frames = []

        async def acknowledge(frame_json):
            frame = json.loads(frame_json)
            frames.append(frame)
            plugin._pending_sends[frame["request_id"]].set_result({"success": True})

        plugin._bridge_ws = MagicMock(send=AsyncMock(side_effect=acknowledge))
        receipt = asyncio.run(plugin.send_to_imessage({
            "session_id": "iMessage;-;+15550000001",
            "raw_message": [serialized[4]],
        }))
        self.assertTrue(receipt["success"])
        self.assertEqual(frames[0]["data"]["attachments"][0]["type"], "video")

    def test_host_tool_session_routes_to_correct_imessage_chat(self):
        plugin = create_plugin()
        plugin._chat_states = {}
        plugin._host_session_chat_ids = {}
        host_session = self.host["SessionUtils"].calculate_session_id
        for chat_id, user_id, message_id in (
            ("iMessage;-;+15550000001", "+15550000001", "msg-1"),
            ("iMessage;-;+15550000002", "+15550000002", "msg-2"),
        ):
            message = to_mai_message_dict({
                "message_id": message_id, "chat_id": chat_id,
                "line_phone": "+15550009999", "project_id": "project-1",
                "sender": {"address": user_id}, "text": "hello",
            })
            expected = host_session("imessage", user_id=user_id, account_id="+15550009999", scope="project-1")
            calculated = plugin._host_session_id_for_inbound(message, "+15550009999", "project-1")
            self.assertEqual(calculated, expected)
            plugin._host_session_chat_ids[calculated] = chat_id
            plugin._chat_states[chat_id] = {"chat_id": chat_id, "last_inbound_message_id": message_id}
        plugin._execute_sidecar_action = AsyncMock(return_value={"success": True})
        second_hash = host_session("imessage", user_id="+15550000002", account_id="+15550009999", scope="project-1")
        group_chat_id = "iMessage;+;group-1"
        group_message = to_mai_message_dict({
            "message_id": "group-msg", "chat_id": group_chat_id, "space_type": "group",
            "line_phone": "+15550009999", "project_id": "project-1",
            "sender": {"address": "+15550000003"}, "text": "group hello",
        })
        group_hash = host_session("imessage", group_id=group_chat_id, account_id="+15550009999", scope="project-1")
        self.assertEqual(plugin._host_session_id_for_inbound(group_message, "+15550009999", "project-1"), group_hash)
        result = asyncio.run(plugin.tool_send_reaction(chat_id=second_hash))
        self.assertIn("msg-2", result)
        self.assertEqual(plugin._execute_sidecar_action.await_args.kwargs["chat_id"], "iMessage;-;+15550000002")
        frames = []

        async def acknowledge(frame_json):
            frame = json.loads(frame_json)
            frames.append(frame)
            plugin._pending_sends[frame["request_id"]].set_result({"success": True})

        plugin._execute_sidecar_action = type(plugin)._execute_sidecar_action.__get__(plugin)
        plugin._pending_sends = {}
        plugin._bridge_ws = MagicMock(send=AsyncMock(side_effect=acknowledge))
        plugin._gateway_ready = True
        receipt = asyncio.run(plugin._execute_sidecar_action({"action": "send_reaction", "chat_id": second_hash}))
        self.assertTrue(receipt["success"])
        self.assertEqual(frames[0]["data"]["chat_id"], "iMessage;-;+15550000002")
        with self.assertRaises(ValueError):
            plugin._resolve_active_chat_context("0" * 32)


if __name__ == "__main__":
    unittest.main()
