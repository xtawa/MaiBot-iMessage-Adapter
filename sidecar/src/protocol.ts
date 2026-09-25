/** JSON-safe, versioned bridge helpers shared by the sidecar and tests. */

export const BRIDGE_PROTOCOL_VERSION = 3;

export const STRUCTURED_ACTIONS = new Set([
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
  "send_location",
  "send_link_card",
  "send_music_card",
  "send_transfer_card",
  "update_transfer_card",
  "send_vcard",
  "share_my_contact",
  "set_chat_background",
  "send_handwriting",
  "send_digital_touch",
  "place_sticker",
  "set_typing",
  "mark_read",
  "notify_silenced",
  "manage_group",
  "find_my_location",
  "check_imessage_availability",
  "enroll_shared_user",
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
  const action = typeof input.action === "string" ? input.action.trim().toLowerCase() : "";
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
    reactionremoved: "reaction.removed",
    poll: "poll.created",
    poll_option: "poll.vote",
    optionadded: "poll.option_added",
    reply: "message.reply",
    edit: "message.edited",
    unsend: "message.unsent",
    voice: "message.voice",
    contact: "contact.received",
    richlink: "link.received",
    typing: "typing.changed",
    read: "read.updated",
    sticker: "sticker.placed",
    backgroundchanged: "chat.background_changed",
    backgroundremoved: "chat.background_removed",
    groupchanged: "group.changed",
    locationupdated: "location.updated",
    transferclaimed: "transfer.claimed",
    card: "card.received",
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
  const encoded = value.trim().replace(/^data:[^;]+;base64,/i, "").replace(/\s+/g, "");
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

// ---------------------------------------------------------------------------
// iMessage Effects (Screen, Bubble & iOS 18 Word Effects)
// ---------------------------------------------------------------------------

export const MESSAGE_EFFECT_IDS: Record<string, string> = {
  balloons: "com.apple.messages.effect.CKBalloonEffect",
  celebration: "com.apple.messages.effect.CKHappyBirthdayEffect",
  confetti: "com.apple.messages.effect.CKConfettiEffect",
  echo: "com.apple.messages.effect.CKEchoEffect",
  fireworks: "com.apple.messages.effect.CKFireworksEffect",
  gentle: "com.apple.MobileSMS.expressivesend.gentle",
  heart: "com.apple.messages.effect.CKHeartEffect",
  invisible: "com.apple.MobileSMS.expressivesend.invisibleink",
  lasers: "com.apple.messages.effect.CKLasersEffect",
  loud: "com.apple.MobileSMS.expressivesend.loud",
  slam: "com.apple.MobileSMS.expressivesend.impact",
  sparkles: "com.apple.messages.effect.CKSparklesEffect",
  spotlight: "com.apple.messages.effect.CKSpotlightEffect",
};

export const MESSAGE_EFFECT_LABELS: Record<string, string> = {
  balloons: "气球",
  celebration: "庆典",
  confetti: "五彩纸屑",
  echo: "回声",
  fireworks: "烟花",
  gentle: "轻柔",
  heart: "爱心",
  invisible: "隐形墨水",
  lasers: "镭射激光",
  loud: "放大呼喊",
  slam: "震撼重击",
  sparkles: "闪光",
  spotlight: "聚光灯",
};

const SCREEN_EFFECT_SET = new Set([
  "balloons",
  "celebration",
  "confetti",
  "echo",
  "fireworks",
  "heart",
  "lasers",
  "sparkles",
  "spotlight",
]);

const EFFECT_ALIASES: Record<string, string> = {
  balloon: "balloons",
  love: "heart",
  invisibleink: "invisible",
  invisible_ink: "invisible",
  impact: "slam",
  happybirthday: "celebration",
  shootingstar: "sparkles",
  shooting_star: "sparkles",
  气球: "balloons",
  生日: "celebration",
  生日快乐: "celebration",
  庆典: "celebration",
  彩纸: "confetti",
  五彩纸屑: "confetti",
  回声: "echo",
  烟花: "fireworks",
  爱心: "heart",
  心: "heart",
  镭射: "lasers",
  激光: "lasers",
  闪光: "sparkles",
  流星: "sparkles",
  聚光灯: "spotlight",
  聚光: "spotlight",
  轻轻地: "gentle",
  轻轻: "gentle",
  轻柔: "gentle",
  大声: "loud",
  放大: "loud",
  用力: "slam",
  重锤: "slam",
  震撼: "slam",
  隐形墨水: "invisible",
  隐形: "invisible",
};

export interface EffectDescriptor {
  key: string;
  id: string;
  label: string;
  scope: "screen" | "bubble";
}

export function resolveEffectDescriptor(input: unknown): EffectDescriptor | null {
  const raw = String(input ?? "").trim();
  if (!raw) return null;
  const normalized = raw.toLowerCase().replace(/[\s-]+/g, "_");
  const key = Object.hasOwn(MESSAGE_EFFECT_IDS, normalized)
    ? normalized
    : EFFECT_ALIASES[normalized] ?? EFFECT_ALIASES[raw] ?? "";
  if (key && MESSAGE_EFFECT_IDS[key]) {
    return {
      key,
      id: MESSAGE_EFFECT_IDS[key]!,
      label: MESSAGE_EFFECT_LABELS[key] ?? key,
      scope: SCREEN_EFFECT_SET.has(key) ? "screen" : "bubble",
    };
  }
  for (const [candidateKey, bundleId] of Object.entries(MESSAGE_EFFECT_IDS)) {
    if (bundleId.toLowerCase() === raw.toLowerCase()) {
      return {
        key: candidateKey,
        id: bundleId,
        label: MESSAGE_EFFECT_LABELS[candidateKey] ?? candidateKey,
        scope: SCREEN_EFFECT_SET.has(candidateKey) ? "screen" : "bubble",
      };
    }
  }
  return null;
}

export const WORD_EFFECT_LABELS: Record<string, string> = {
  big: "变大",
  small: "变小",
  shake: "抖动",
  nod: "点头",
  explode: "爆炸",
  ripple: "涟漪",
  bloom: "绽放",
  jitter: "颤抖",
};

export interface ExtractedTextEffect {
  type: string;
  effect: string;
  label: string;
  start: number;
  length: number;
  snippet: string;
}

export function extractTextEffects(
  text: unknown,
  formatting: unknown,
): ExtractedTextEffect[] {
  const rawText = typeof text === "string" ? text : "";
  if (!Array.isArray(formatting) || formatting.length === 0) return [];
  const results: ExtractedTextEffect[] = [];
  for (const item of formatting) {
    if (!item || typeof item !== "object") continue;
    const entry = item as Record<string, unknown>;
    const rawEffect = String(entry.effectName ?? entry.effect ?? entry.animation ?? entry.style ?? "").trim().toLowerCase();
    const rawType = String(entry.type ?? "").trim().toLowerCase();
    const range = Array.isArray(entry.range) ? entry.range : undefined;
    const start = typeof entry.start === "number"
      ? entry.start
      : (typeof range?.[0] === "number" ? range[0] : 0);
    const length = typeof entry.length === "number"
      ? entry.length
      : (typeof range?.[1] === "number" ? range[1] : 0);
    const snippet = rawText && length > 0 && start >= 0
      ? rawText.slice(start, start + length)
      : "";
    if (rawEffect && Object.hasOwn(WORD_EFFECT_LABELS, rawEffect)) {
      results.push({
        type: "text_effect",
        effect: rawEffect,
        label: WORD_EFFECT_LABELS[rawEffect]!,
        start,
        length,
        snippet,
      });
    } else if (["bold", "italic", "underline", "strikethrough"].includes(rawType)) {
      results.push({
        type: rawType,
        effect: rawType,
        label: rawType,
        start,
        length,
        snippet,
      });
    }
  }
  return results;
}

// ---------------------------------------------------------------------------
// Balloon / MiniApp / Location / Transfer Card Helpers
// ---------------------------------------------------------------------------

const EXT_PREFIX = "com.apple.messages.MSMessageExtensionBalloonPlugin";

const APPLE_BALLOONS: Record<string, { kind: "url" | "handwriting" | "digital_touch" | "apple_cash"; label: string }> = {
  "com.apple.messages.URLBalloonProvider": { kind: "url", label: "富链接" },
  "com.apple.Handwriting.HandwritingProvider": { kind: "handwriting", label: "手写消息" },
  "com.apple.DigitalTouchBalloonProvider": { kind: "digital_touch", label: "Digital Touch" },
  "com.apple.messages.MSMessageExtensionBalloonPlugin:0000000000:com.apple.PassbookUIService.PeerPaymentMessagesExtension": {
    kind: "apple_cash",
    label: "Apple Cash 转账",
  },
};

const KNOWN_EXTENSION_APPS: Record<string, string> = {
  "com.netease.cloudmusic": "网易云音乐",
  "com.tencent.qqmusic": "QQ音乐",
  "com.apple.music": "Apple Music",
  "com.spotify.client": "Spotify",
  "com.google.ios.youtube": "YouTube",
  "com.ss.iphone.ugc.aweme": "抖音",
  "com.zhiliaoapp.musically": "TikTok",
  "com.xingin.discover": "小红书",
  "com.tencent.xin": "微信",
  "com.taobao.taobao": "淘宝",
  "com.meituan.imeituan": "美团",
  "com.sankuai.meituan": "美团",
  "com.dianping.dpscope": "大众点评",
  "com.jingdong.app.mall": "京东",
  "com.bilibili.mobile": "哔哩哔哩",
  "tv.danmaku.bili": "哔哩哔哩",
  "com.burbn.instagram": "Instagram",
  "codes.photon.spectrum": "转账/卡片",
};

const LOCATION_SHARE_PREFIXES = [
  "com.apple.findmy",
  "com.apple.mobileme.fmf1",
  "com.apple.mobileme.fmf",
  "com.apple.friendfinder",
];

export interface BalloonParseResult {
  kind: "url" | "handwriting" | "digital_touch" | "apple_cash" | "find_my" | "extension" | "apple_other";
  bundleId: string;
  appName: string;
  supportsEmbeddedMedia: boolean;
}

export function parseBalloonBundle(balloonBundleId: unknown): BalloonParseResult | null {
  const raw = String(balloonBundleId ?? "").trim();
  if (!raw) return null;

  if (Object.hasOwn(APPLE_BALLOONS, raw)) {
    const hit = APPLE_BALLOONS[raw]!;
    return {
      kind: hit.kind,
      bundleId: raw,
      appName: hit.label,
      supportsEmbeddedMedia: hit.kind === "handwriting" || hit.kind === "digital_touch",
    };
  }

  let bundleId = raw;
  if (raw.startsWith(EXT_PREFIX)) {
    const rest = raw.slice(EXT_PREFIX.length).replace(/^:/, "");
    bundleId = rest.includes(":") ? rest.slice(rest.indexOf(":") + 1) : rest;
  }

  const lower = bundleId.toLowerCase();
  if (LOCATION_SHARE_PREFIXES.some((prefix) => lower === prefix || lower.startsWith(`${prefix}.`))) {
    return {
      kind: "find_my",
      bundleId,
      appName: "查找 (Find My) 位置共享",
      supportsEmbeddedMedia: false,
    };
  }

  for (const [prefix, name] of Object.entries(KNOWN_EXTENSION_APPS)) {
    if (lower === prefix || lower.startsWith(`${prefix}.`)) {
      return {
        kind: "extension",
        bundleId,
        appName: name,
        supportsEmbeddedMedia: false,
      };
    }
  }

  return {
    kind: raw.startsWith("com.apple.") ? "apple_other" : "extension",
    bundleId,
    appName: "",
    supportsEmbeddedMedia: false,
  };
}

export function summarizeMiniAppLayout(layout: unknown): string {
  if (!layout || typeof layout !== "object") return "";
  const record = layout as Record<string, unknown>;
  const candidates = [
    record.caption,
    record.imageTitle,
    record.subcaption,
    record.imageSubtitle,
    record.trailingCaption,
    record.trailingSubcaption,
    record.summary,
  ];
  const seen = new Set<string>();
  const picked: string[] = [];
  for (const raw of candidates) {
    const text = String(raw ?? "").trim();
    if (!text || seen.has(text)) continue;
    seen.add(text);
    picked.push(text.length > 60 ? `${text.slice(0, 60)}…` : text);
    if (picked.length >= 3) break;
  }
  return picked.join(" · ");
}

const PIN_SPAN = "0.028033,0.037066";
const MAPS_HOSTS = new Set(["maps.apple.com", "maps.apple", "beta.maps.apple.com"]);

export function buildAppleMapsUrl(
  label?: unknown,
  latitude?: unknown,
  longitude?: unknown,
): string {
  const name = String(label ?? "").trim();
  const lat = typeof latitude === "number" ? latitude : Number(latitude);
  const lon = typeof longitude === "number" ? longitude : Number(longitude);
  const hasCoords =
    latitude !== undefined &&
    longitude !== undefined &&
    String(latitude).trim() !== "" &&
    String(longitude).trim() !== "" &&
    Number.isFinite(lat) &&
    Number.isFinite(lon) &&
    Math.abs(lat) <= 90 &&
    Math.abs(lon) <= 180;

  if (!hasCoords) {
    if (!name) {
      throw new Error("send_location 需要提供有效的经纬度坐标或地点名称 label/address");
    }
    const url = new URL("https://maps.apple.com/");
    url.searchParams.set("q", name);
    return url.toString();
  }

  const url = new URL("https://maps.apple.com/place");
  url.searchParams.set("coordinate", `${lat},${lon}`);
  url.searchParams.set("name", name || `${lat},${lon}`);
  url.searchParams.set("span", PIN_SPAN);
  return url.toString().replace(/%2C/gi, ",");
}

export interface ParsedMapsLocation {
  url: string;
  name: string;
  coordinates: string;
  formatted: string;
}

export function parseAppleMapsUrl(rawUrl: unknown): ParsedMapsLocation | null {
  const trimmed = String(rawUrl ?? "").trim();
  if (!trimmed) return null;
  let u: URL;
  try {
    u = new URL(trimmed);
  } catch {
    return null;
  }
  if (u.protocol !== "http:" && u.protocol !== "https:") return null;
  if (!MAPS_HOSTS.has(u.hostname.toLowerCase())) return null;

  const p = u.searchParams;
  const rawCoord = String(p.get("coordinate") ?? p.get("ll") ?? p.get("sll") ?? "").trim();
  const coordMatch = /^(-?\d{1,3}(?:\.\d+)?)\s*[,，]\s*(-?\d{1,3}(?:\.\d+)?)$/.exec(rawCoord);
  let coordinates = "";
  if (coordMatch) {
    const lat = Number(coordMatch[1]);
    const lon = Number(coordMatch[2]);
    if (Math.abs(lat) <= 90 && Math.abs(lon) <= 180) {
      coordinates = `${lat},${lon}`;
    }
  }
  const name = String(
    p.get("name") ?? p.get("q") ?? p.get("address") ?? p.get("daddr") ?? "",
  ).trim();
  if (!name && !coordinates) return null;
  const displayName = name || "地图位置";
  const formatted = `[位置: ${displayName}${coordinates ? ` (${coordinates})` : ""}]`;
  return {
    url: trimmed,
    name: displayName,
    coordinates,
    formatted,
  };
}

export function formatTransferAmount(rawAmount: unknown, currency?: unknown): string {
  const text = String(rawAmount ?? "").trim();
  const sym = String(currency ?? "").trim() || "￥";
  if (!text) return `${sym}0.00`;
  const cleaned = text
    .replace(/\p{Sc}/gu, "")
    .replaceAll(sym, "")
    .replace(/[,，\s]/g, "");
  if (!/^-?\d+(?:\.\d+)?$/.test(cleaned)) {
    return text.startsWith(sym) ? text : `${sym}${text}`;
  }
  const num = Number(cleaned);
  return `${sym}${num.toLocaleString("zh-CN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

export function sniffMediaMime(
  buffer: Buffer,
  fallbackMime = "",
  fileName = "",
): { mimeType: string; extension: string } {
  if (buffer.length >= 12) {
    if (
      buffer[0] === 0x89 &&
      buffer[1] === 0x50 &&
      buffer[2] === 0x4e &&
      buffer[3] === 0x47
    ) {
      return { mimeType: "image/png", extension: "png" };
    }
    if (buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) {
      return { mimeType: "image/jpeg", extension: "jpg" };
    }
    if (
      buffer[0] === 0x47 &&
      buffer[1] === 0x49 &&
      buffer[2] === 0x46 &&
      buffer[3] === 0x38
    ) {
      return { mimeType: "image/gif", extension: "gif" };
    }
    if (
      buffer.subarray(0, 4).toString("ascii") === "RIFF" &&
      buffer.subarray(8, 12).toString("ascii") === "WEBP"
    ) {
      return { mimeType: "image/webp", extension: "webp" };
    }
    if (
      buffer.subarray(0, 4).toString("ascii") === "RIFF" &&
      buffer.subarray(8, 12).toString("ascii") === "WAVE"
    ) {
      return { mimeType: "audio/wav", extension: "wav" };
    }
    if (buffer.subarray(0, 4).toString("ascii") === "caff") {
      return { mimeType: "audio/x-caf", extension: "caf" };
    }
    if (buffer.subarray(0, 4).toString("ascii") === "OggS") {
      return { mimeType: "audio/ogg", extension: "ogg" };
    }
    if (buffer.subarray(0, 5).toString("ascii") === "%PDF-") {
      return { mimeType: "application/pdf", extension: "pdf" };
    }
    if (buffer.subarray(4, 8).toString("ascii") === "ftyp") {
      const brand = buffer.subarray(8, 12).toString("ascii").toLowerCase();
      if (["heic", "heix", "hevc", "mif1", "msf1"].includes(brand)) {
        return { mimeType: "image/heic", extension: "heic" };
      }
      if (brand === "avif") {
        return { mimeType: "image/avif", extension: "avif" };
      }
      if (["m4a ", "m4b ", "mp4a"].includes(brand)) {
        return { mimeType: "audio/mp4", extension: "m4a" };
      }
      if (brand === "qt  ") {
        return { mimeType: "video/quicktime", extension: "mov" };
      }
      return { mimeType: "video/mp4", extension: "mp4" };
    }
  }
  const extMatch = /\.([a-z0-9]{1,8})$/i.exec(fileName.trim());
  const ext = extMatch ? extMatch[1]!.toLowerCase() : "";
  return {
    mimeType: fallbackMime || "application/octet-stream",
    extension: ext || "bin",
  };
}

export function matchPollOption(
  options: readonly { optionIdentifier: string; text: string }[],
  query: unknown,
): { optionIdentifier: string; text: string } | null {
  const raw = String(query ?? "").trim();
  if (!raw || !Array.isArray(options) || options.length === 0) return null;

  // 1. Direct optionIdentifier match
  const exactId = options.find((o) => o.optionIdentifier === raw);
  if (exactId) return exactId;

  // 2. Single letter A-Z match
  if (/^[A-Za-z]$/.test(raw)) {
    const idx = raw.toUpperCase().charCodeAt(0) - 65;
    if (idx >= 0 && idx < options.length) return options[idx]!;
  }

  // 3. 1-based numeric index match ("1", "2", ...)
  if (/^\d{1,2}$/.test(raw)) {
    const idx = Number(raw) - 1;
    if (idx >= 0 && idx < options.length) return options[idx]!;
  }

  // 4. Exact or substring option text match
  const lower = raw.toLowerCase();
  const exactText = options.find((o) => o.text.trim().toLowerCase() === lower);
  if (exactText) return exactText;
  const partialText = options.find(
    (o) =>
      o.text.trim().toLowerCase().includes(lower) ||
      lower.includes(o.text.trim().toLowerCase()),
  );
  return partialText ?? null;
}

export function isTransientGrpcError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const anyErr = err as Record<string, unknown>;
  const name = String(anyErr.constructor?.name ?? anyErr.name ?? "");
  if (name === "ConnectionError" || name === "TimeoutError") return true;
  if (anyErr.retryable === true) return true;
  const text = `${anyErr.message ?? ""} ${(anyErr.cause as any)?.message ?? ""}`;
  return /Connection dropped|UNAVAILABLE|DEADLINE_EXCEEDED|socket hang up|ECONNRESET|ETIMEDOUT|EPIPE/i.test(
    text,
  );
}
