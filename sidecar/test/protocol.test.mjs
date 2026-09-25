import assert from "node:assert/strict";
import test from "node:test";

import {
  BRIDGE_PROTOCOL_VERSION,
  STRUCTURED_ACTIONS,
  buildAppleMapsUrl,
  decodeBase64Attachment,
  extractTextEffects,
  formatTransferAmount,
  isTransientGrpcError,
  matchPollOption,
  messageContentMetadata,
  messageDedupeKey,
  messageEventType,
  normalizeStructuredAction,
  parseAppleMapsUrl,
  parseBalloonBundle,
  resolveEffectDescriptor,
  sniffMediaMime,
  summarizeMiniAppLayout,
  toJsonSafeMetadata,
} from "../dist/protocol.js";

test("structured action protocol is versioned and supports extended iMessage actions", () => {
  assert.equal(BRIDGE_PROTOCOL_VERSION, 3);
  assert.deepEqual(normalizeStructuredAction({ action: "send_reply", target_message_id: "m1" }), {
    action: "send_reply",
    target_message_id: "m1",
  });
  for (const act of [
    "vote_poll",
    "unvote_poll",
    "add_poll_option",
    "edit_message",
    "remove_reaction",
    "send_music_card",
    "send_transfer_card",
    "update_transfer_card",
    "set_typing",
    "mark_read",
    "manage_group",
    "find_my_location",
    "check_imessage_availability",
    "enroll_shared_user",
  ]) {
    assert.ok(STRUCTURED_ACTIONS.has(act), `expected ${act} in STRUCTURED_ACTIONS`);
  }
  assert.throws(() => normalizeStructuredAction({ action: "delete_everything" }), /不支持/);
});

test("attachment decoding validates base64 and per-file bounds", () => {
  assert.deepEqual(decodeBase64Attachment("aGVsbG8=", 5), Buffer.from("hello"));
  assert.deepEqual(decodeBase64Attachment("data:image/png;base64,aGVsbG8=", 5), Buffer.from("hello"));
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
  assert.equal(messageEventType("backgroundChanged"), "chat.background_changed");
});

test("poll vote events retain identifiers and matchPollOption resolves letter/index/text", () => {
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

  const options = [
    { optionIdentifier: "opt-1", text: "麻辣烫" },
    { optionIdentifier: "opt-2", text: "海底捞火锅" },
  ];
  assert.equal(matchPollOption(options, "A")?.optionIdentifier, "opt-1");
  assert.equal(matchPollOption(options, "2")?.optionIdentifier, "opt-2");
  assert.equal(matchPollOption(options, "海底捞")?.optionIdentifier, "opt-2");
});

test("effects, balloons, maps URLs, transfer amounts, and media sniffing work accurately", () => {
  assert.equal(resolveEffectDescriptor("烟花")?.key, "fireworks");
  assert.equal(resolveEffectDescriptor("com.apple.MobileSMS.expressivesend.invisibleink")?.key, "invisible");

  const wordAnims = extractTextEffects("我超级喜欢你", [
    { type: "effect", effectName: "explode", start: 1, length: 2 },
  ]);
  assert.equal(wordAnims.length, 1);
  assert.equal(wordAnims[0]?.snippet, "超级");
  assert.equal(wordAnims[0]?.label, "爆炸");

  const neteaseBalloon = parseBalloonBundle(
    "com.apple.messages.MSMessageExtensionBalloonPlugin:TEAMID:com.netease.cloudmusic.iMessageExtension",
  );
  assert.equal(neteaseBalloon?.appName, "网易云音乐");
  assert.equal(
    summarizeMiniAppLayout({ caption: "晴天", subcaption: "周杰伦" }),
    "晴天 · 周杰伦",
  );

  const mapsUrl = buildAppleMapsUrl("南宁万象城", 22.817, 108.3665);
  assert.ok(mapsUrl.includes("coordinate=22.817,108.3665"));
  const parsedMaps = parseAppleMapsUrl(mapsUrl);
  assert.equal(parsedMaps?.name, "南宁万象城");
  assert.equal(parsedMaps?.coordinates, "22.817,108.3665");

  assert.equal(formatTransferAmount("520", "￥"), "￥520.00");
  assert.equal(
    sniffMediaMime(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 0])).mimeType,
    "image/png",
  );
  assert.equal(isTransientGrpcError(new Error("14 UNAVAILABLE: Connection dropped")), true);
});
