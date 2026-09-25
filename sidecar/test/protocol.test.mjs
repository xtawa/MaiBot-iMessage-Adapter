import assert from "node:assert/strict";
import test from "node:test";

import {
  BRIDGE_PROTOCOL_VERSION,
  STRUCTURED_ACTIONS,
  decodeBase64Attachment,
  messageContentMetadata,
  messageDedupeKey,
  messageEventType,
  normalizeStructuredAction,
  toJsonSafeMetadata,
} from "../dist/protocol.js";

test("structured action protocol is versioned and rejects unknown actions", () => {
  assert.equal(BRIDGE_PROTOCOL_VERSION, 2);
  assert.deepEqual(normalizeStructuredAction({ action: "send_reply", target_message_id: "m1" }), {
    action: "send_reply",
    target_message_id: "m1",
  });
  assert.ok(STRUCTURED_ACTIONS.has("vote_poll"));
  assert.ok(STRUCTURED_ACTIONS.has("unvote_poll"));
  assert.ok(STRUCTURED_ACTIONS.has("add_poll_option"));
  assert.throws(() => normalizeStructuredAction({ action: "delete_everything" }), /不支持/);
});

test("attachment decoding validates base64 and per-file bounds", () => {
  assert.deepEqual(decodeBase64Attachment("aGVsbG8=", 5), Buffer.from("hello"));
  assert.throws(() => decodeBase64Attachment("%%%", 100), /Base64/);
  assert.throws(() => decodeBase64Attachment("aGVsbG8=", 4), /大小限制/);
  assert.throws(() => decodeBase64Attachment("", 10), /非空/);
});

test("metadata serializer keeps human-readable values and removes SDK methods", () => {
  const value = {
    title: "Dinner poll",
    summary: "Choose a place",
    timestamp: new Date("2026-09-24T00:00:00.000Z"),
    count: 4n,
    image: Buffer.from("abc"),
    read() {},
    nested: { stream() {}, value: true },
  };
  assert.deepEqual(toJsonSafeMetadata(value), {
    title: "Dinner poll",
    summary: "Choose a place",
    timestamp: "2026-09-24T00:00:00.000Z",
    count: "4",
    image: { binary: true, size: 3 },
    nested: { value: true },
  });
});

test("event keys are project and line scoped", () => {
  assert.notEqual(
    messageDedupeKey("project-a", "+1555", "same-id"),
    messageDedupeKey("project-b", "+1555", "same-id"),
  );
  assert.equal(messageEventType("reactionAdded"), "reaction.added");
  assert.equal(messageEventType("poll_option"), "poll.vote");
});

test("poll vote events retain identifiers required by vote and add-option actions", () => {
  const metadata = messageContentMetadata(
    "poll_option",
    "any;-;poll-42:+15551234567:option-9:selected:1234",
    { type: "poll_option", option: { title: "Pizza" }, poll: { title: "Dinner" }, selected: true },
  );
  assert.deepEqual(metadata, {
    type: "poll_option",
    option: { title: "Pizza" },
    poll: { title: "Dinner" },
    selected: true,
    poll_message_id: "any;-;poll-42",
    option_id: "option-9",
  });
});
