/** JSON-safe, versioned bridge helpers shared by the sidecar and tests. */

export const BRIDGE_PROTOCOL_VERSION = 2;

export const STRUCTURED_ACTIONS = new Set([
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
]);

const FUNCTION_FIELDS = new Set([
  "read",
  "stream",
  "toJSON",
]);

export interface StructuredAction {
  action: string;
  [key: string]: unknown;
}

export function normalizeStructuredAction(value: unknown): StructuredAction {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("结构化 Action 必须是对象");
  }
  const input = value as Record<string, unknown>;
  const action = typeof input.action === "string" ? input.action.trim() : "";
  if (!STRUCTURED_ACTIONS.has(action)) {
    throw new Error(`不支持的 iMessage Action: ${action || "<empty>"}`);
  }
  return { ...input, action };
}

/** Remove SDK methods and non-JSON values while retaining native metadata. */
export function toJsonSafeMetadata(value: unknown, depth = 0): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return { binary: true, size: value.byteLength };
  }
  if (typeof value === "function" || typeof value === "undefined") {
    return undefined;
  }
  if (depth >= 6) {
    return "[metadata depth limit]";
  }
  if (Array.isArray(value)) {
    return value.slice(0, 100).map((entry) => toJsonSafeMetadata(entry, depth + 1));
  }
  if (typeof value === "object") {
    const result: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value)) {
      if (FUNCTION_FIELDS.has(key) || typeof entry === "function") {
        continue;
      }
      const normalized = toJsonSafeMetadata(entry, depth + 1);
      if (normalized !== undefined) {
        result[key] = normalized;
      }
    }
    return result;
  }
  return String(value);
}

export function messageEventType(contentType: unknown): string {
  const type = typeof contentType === "string" ? contentType.toLowerCase() : "unknown";
  const known: Record<string, string> = {
    reaction: "reaction.added",
    reactionadded: "reaction.added",
    poll: "poll.created",
    poll_option: "poll.vote",
    reply: "message.reply",
    edit: "message.edited",
    unsend: "message.unsent",
    voice: "message.voice",
    contact: "contact.received",
    richlink: "link.received",
    typing: "typing.changed",
    read: "read.updated",
  };
  return known[type] ?? `message.${type}`;
}

/** Add poll identifiers omitted by Spectrum's normalized poll-option content. */
export function messageContentMetadata(
  contentType: unknown,
  messageId: unknown,
  content: unknown,
): unknown {
  const metadata = toJsonSafeMetadata(content);
  if (contentType !== "poll_option" || !metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return metadata;
  }
  const match = String(messageId ?? "").match(/^(.+):([^:]+):([^:]+):(selected|deselected):(\d+)$/);
  if (!match) return metadata;
  return {
    ...(metadata as Record<string, unknown>),
    poll_message_id: match[1],
    option_id: match[3],
  };
}

export function messageDedupeKey(
  projectId: string,
  linePhone: string,
  eventId: string,
): string {
  return [projectId || "default", linePhone || "shared", eventId]
    .map((part) => encodeURIComponent(part))
    .join(":");
}

export function decodeBase64Attachment(
  value: unknown,
  maxBytes: number,
): Buffer {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error("附件 data_base64 必须是非空字符串");
  }
  const encoded = value.trim();
  const pattern = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
  if (!pattern.test(encoded)) {
    throw new Error("附件 data_base64 不是有效 Base64");
  }
  const buffer = Buffer.from(encoded, "base64");
  if (buffer.length === 0) {
    throw new Error("附件解码后为空");
  }
  if (buffer.length > maxBytes) {
    throw new Error(`附件超过单附件大小限制: ${buffer.length} > ${maxBytes} bytes`);
  }
  return buffer;
}
