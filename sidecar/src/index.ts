/**
 * MaiBot iMessage Adapter — Sidecar
 *
 * 任务:
 * 初始化 spectrum-ts SDK
 * 连接 Python WebSocket Server，通过本地桥接转发消息
 * 把 SDK 的 Message/Space 翻译成简化 JSON
 * 
 * Made BY Galeros
 * 
 */

import {
  Spectrum,
  attachment,
  contact,
  group,
  option,
  poll,
  reaction,
  reply,
  richlink,
  text,
  unsend,
  voice,
} from "spectrum-ts";
import {
  background,
  effect,
  imessage,
} from "spectrum-ts/providers/imessage";
import type { AdvancedIMessage } from "@photon-ai/advanced-imessage";
import { WebSocket } from "ws";
import {
  BRIDGE_PROTOCOL_VERSION,
  decodeBase64Attachment,
  messageDedupeKey,
  messageContentMetadata,
  messageEventType,
  normalizeStructuredAction,
  toJsonSafeMetadata,
} from "./protocol.js";

// ---------------------------------------------------------------------------
// 1. 从 Python 通过环境变量接收配置
// ---------------------------------------------------------------------------

const PORT = parseInt(process.env.BRIDGE_WS_PORT ?? "", 10);
if (!PORT || PORT <= 0) {
  console.error("[sidecar] 缺少 BRIDGE_WS_PORT 环境变量");
  process.exit(1);
}

const TOKEN = (process.env.BRIDGE_WS_TOKEN ?? "").trim();
if (!TOKEN) {
  console.error("[sidecar] 缺少 BRIDGE_WS_TOKEN 环境变量");
  process.exit(1);
}

interface PhotonProjectCredential {
  project_id: string;
  project_secret: string;
  lines: string[];
}

function readProjectCredentials(): PhotonProjectCredential[] {
  const configured = (process.env.PHOTON_PROJECTS ?? "").trim();
  let input: unknown;
  if (configured) {
    try {
      input = JSON.parse(configured);
    } catch {
      throw new Error("PHOTON_PROJECTS 必须是有效 JSON 数组");
    }
  } else {
    input = [{
      project_id: process.env.PHOTON_PROJECT_ID,
      project_secret: process.env.PHOTON_PROJECT_SECRET,
      lines: [],
    }];
  }
  if (!Array.isArray(input) || input.length === 0 || input.length > 20) {
    throw new Error("至少需要一个 Photon 项目，最多支持 20 个项目");
  }
  const projects = input.map((entry, index) => {
    if (!entry || typeof entry !== "object") {
      throw new Error(`Photon 项目 ${index + 1} 必须是对象`);
    }
    const value = entry as Record<string, unknown>;
    const projectId = typeof value.project_id === "string" ? value.project_id.trim() : "";
    const projectSecret = typeof value.project_secret === "string" ? value.project_secret.trim() : "";
    const lines = Array.isArray(value.lines)
      ? value.lines.filter((line): line is string => typeof line === "string").map((line) => line.trim()).filter(Boolean)
      : [];
    if (!projectId || !projectSecret) {
      throw new Error(`Photon 项目 ${index + 1} 必须同时提供项目 ID 和密钥`);
    }
    return { project_id: projectId, project_secret: projectSecret, lines };
  });
  const ids = projects.map((project) => project.project_id);
  if (new Set(ids).size !== ids.length) {
    throw new Error("Photon 项目 ID 不得重复");
  }
  return projects;
}

let PROJECTS: PhotonProjectCredential[];
try {
  PROJECTS = readProjectCredentials();
} catch (err) {
  console.error("[sidecar] Photon 配置无效:", err);
  process.exit(1);
}

const MAX_ATTACHMENT_MB = parseInt(process.env.MAX_ATTACHMENT_SIZE_MB ?? "", 10);
const MAX_ATTACHMENT_BYTES = (Number.isFinite(MAX_ATTACHMENT_MB) && MAX_ATTACHMENT_MB > 0)
  ? MAX_ATTACHMENT_MB * 1024 * 1024
  : 10 * 1024 * 1024; // fallback 默认 10 MB

const MAX_MESSAGE_MB = parseInt(process.env.MAX_MESSAGE_SIZE_MB ?? "", 10);
const MAX_MESSAGE_BYTES = (Number.isFinite(MAX_MESSAGE_MB) && MAX_MESSAGE_MB > 0)
  ? MAX_MESSAGE_MB * 1024 * 1024
  : 20 * 1024 * 1024; // fallback 默认 20 MB

// 整条消息按附件总量限流；base64 会放大约 4/3，并预留 JSON 元数据空间。
const MAX_PAYLOAD = 4 * Math.ceil(MAX_MESSAGE_BYTES / 3) + 256 * 1024;

// 空字符串会关闭自动反应；保留完整 Emoji 字符串，不限制为固定的 Tapback 集合。
const INBOUND_REACTION_EMOJI = (process.env.INBOUND_REACTION_EMOJI ?? "").trim();
const MAX_SPACE_CACHE_ENTRIES = 256;
const MAX_MESSAGE_CACHE_ENTRIES = 2_048;
const MAX_SEEN_EVENT_ENTRIES = 10_000;

// ---------------------------------------------------------------------------
// 2. 初始化 spectrum-ts 官方 SDK
// ---------------------------------------------------------------------------

type SpectrumApp = Awaited<ReturnType<typeof Spectrum>>;
type MessageSpace = SpectrumApp["messages"] extends AsyncIterable<infer Item>
  ? Item extends [infer Space, unknown] ? Space : never
  : never;
type ProjectRuntime = {
  projectId: string;
  lines: string[];
  app: SpectrumApp;
  im: any;
  currentSpace: MessageSpace | null;
  spaces: Map<string, MessageSpace>;
  messageHandles: Map<string, any>;
  outboundMessageKeys: Set<string>;
};

const runtimes = new Map<string, ProjectRuntime>();
const failedProjectIds: string[] = [];
for (const project of PROJECTS) {
  try {
    const projectApp = await Spectrum({
      projectId: project.project_id,
      projectSecret: project.project_secret,
      providers: [imessage.config()],
    });
    runtimes.set(project.project_id, {
      projectId: project.project_id,
      lines: project.lines,
      app: projectApp,
      im: imessage(projectApp) as any,
      currentSpace: null,
      spaces: new Map<string, MessageSpace>(),
      messageHandles: new Map<string, any>(),
      outboundMessageKeys: new Set<string>(),
    });
    console.log("[sidecar] spectrum-ts 已连接项目: %s", project.project_id);
  } catch (err) {
    failedProjectIds.push(project.project_id);
    console.error("[sidecar] Photon 项目初始化失败 (%s): %s", project.project_id, String(err));
  }
}
if (runtimes.size === 0) {
  console.error("[sidecar] 没有可用的 Photon 项目");
  process.exit(2);
}

// ---------------------------------------------------------------------------
// 3. 连接 Python WebSocket Server
// ---------------------------------------------------------------------------

const pyWs = new WebSocket(`ws://127.0.0.1:${PORT}`, {
  maxPayload: MAX_PAYLOAD,
});
console.log(
  "[sidecar] 连接到 Python WebSocket，单附件: %d MB，单消息总附件: %d MB，帧上限: %d MB",
  Math.round(MAX_ATTACHMENT_BYTES / 1024 / 1024),
  Math.round(MAX_MESSAGE_BYTES / 1024 / 1024),
  Math.ceil(MAX_PAYLOAD / 1024 / 1024),
);

// 3a. 认证握手
try {
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("auth timeout")),
      10_000,
    );

    pyWs.on("open", () => {
      console.log("[sidecar] 已连接 Python WebSocket，发送认证…");
      pyWs.send(JSON.stringify({ type: "auth", token: TOKEN }));
    });

    pyWs.once("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        clearTimeout(timeout);
        if (msg.type === "auth_ok") {
          resolve();
        } else {
          reject(new Error(`收到非预期消息: ${msg.type}`));
        }
      } catch (err) {
        clearTimeout(timeout);
        reject(new Error(`认证响应不是有效 JSON: ${String(err)}`));
      }
    });

    pyWs.on("error", (err) => {
      clearTimeout(timeout);
      reject(err);
    });
  });
  console.log("[sidecar] Python 认证通过");
} catch (err) {
  console.error("[sidecar] Python WebSocket 认证失败:", err);
  process.exit(1);
}

// 3b. Photon SDK 就绪后通知 Python
pyWs.send(JSON.stringify({
  type: "ready",
  protocol_version: BRIDGE_PROTOCOL_VERSION,
  project_ids: [...runtimes.keys()],
  failed_project_ids: failedProjectIds,
}));
console.log("[sidecar] 已向 Python 发送 ready 信号");

// ---------------------------------------------------------------------------
// 4. 消费 Photon 消息流 → 翻译 → 发给 Python
// ---------------------------------------------------------------------------

function lineForSpace(runtime: ProjectRuntime, space: any): string {
  try {
    return String(runtime.im(space)?.phone ?? space?.phone ?? "");
  } catch {
    return "";
  }
}

function spaceCacheKey(chatId: string, linePhone: string): string {
  return `${linePhone || "shared"}\u0000${chatId}`;
}

function messageCacheKey(messageId: string, chatId: string, linePhone: string): string {
  return `${linePhone || "shared"}\u0000${chatId || "unknown"}\u0000${messageId}`;
}

function rememberSpace(runtime: ProjectRuntime, space: MessageSpace): void {
  runtime.currentSpace = space;
  const key = spaceCacheKey(space.id, lineForSpace(runtime, space));
  runtime.spaces.delete(key);
  runtime.spaces.set(key, space);
  if (runtime.spaces.size > MAX_SPACE_CACHE_ENTRIES) {
    const oldest = runtime.spaces.keys().next().value;
    if (typeof oldest === "string") runtime.spaces.delete(oldest);
  }
}

function rememberMessage(runtime: ProjectRuntime, message: any, fallbackSpace?: MessageSpace): void {
  if (!message || typeof message.id !== "string") return;
  const space = (message.space ?? fallbackSpace) as MessageSpace | undefined;
  const chatId = String(space?.id ?? "");
  const linePhone = space ? lineForSpace(runtime, space) : "";
  const key = messageCacheKey(message.id, chatId, linePhone);
  runtime.messageHandles.delete(key);
  runtime.messageHandles.set(key, message);
  if (runtime.messageHandles.size > MAX_MESSAGE_CACHE_ENTRIES) {
    const oldest = runtime.messageHandles.keys().next().value;
    if (typeof oldest === "string") runtime.messageHandles.delete(oldest);
  }
  if (message.content?.type === "group" && Array.isArray(message.content.items)) {
    for (const item of message.content.items) rememberMessage(runtime, item, space);
  }
  if (message.content?.type === "reply" && message.content.target) {
    rememberMessage(runtime, message.content.target, space);
  }
}

function decodeOutboundAttachment(dataBase64: unknown): Buffer {
  return decodeBase64Attachment(dataBase64, MAX_ATTACHMENT_BYTES);
}

async function collectInboundContent(
  runtime: ProjectRuntime,
  space: MessageSpace,
  content: any,
  senderId: string,
  textParts: string[],
  attachments: Record<string, unknown>[],
  parts: Record<string, unknown>[],
  budget: { usedBytes: number },
): Promise<boolean> {
  if (!content || typeof content.type !== "string") {
    return false;
  }

  switch (content.type) {
    case "text":
      if (typeof content.text === "string" && content.text.length > 0) {
        textParts.push(content.text);
        parts.push({ type: "text", text: content.text });
        return true;
      }
      return false;

    case "attachment":
    case "voice": {
      const mime = String(content.mimeType ?? "");
      const cname = String(content.name ?? "");
      const isVoice = content.type === "voice" || mime.startsWith("audio/") || cname.toLowerCase().endsWith(".caf");

      try {
        let buf: Buffer;
        let companion: Buffer | undefined;
        let companionMimeType = "video/quicktime";
        let companionName = "";
        const isHeif = /^image\/hei[cf](?:$|;)/i.test(mime);
        if (isHeif && typeof content.id === "string" && content.id) {
          try {
            const frames = getManagedPhotonClient(runtime, space).attachments.downloadStream(content.id);
            const primaryChunks: Buffer[] = [];
            const companionChunks: Buffer[] = [];
            try {
              for await (const frame of frames) {
                if (frame.type === "header" && frame.companionInfo) {
                  companionMimeType = frame.companionInfo.mimeType;
                  companionName = frame.companionInfo.fileName;
                } else if (frame.type === "primaryChunk") {
                  primaryChunks.push(Buffer.from(frame.data));
                } else if (frame.type === "companionChunk") {
                  companionChunks.push(Buffer.from(frame.data));
                }
              }
            } finally {
              await frames.close();
            }
            buf = primaryChunks.length > 0
              ? Buffer.concat(primaryChunks)
              : await content.read();
            if (companionChunks.length > 0) companion = Buffer.concat(companionChunks);
          } catch (error) {
            console.warn("[sidecar] Live Photo 附件流读取失败，回退到主图片: %s", String(error));
            buf = await content.read();
            companion = undefined;
          }
        } else {
          buf = await content.read();
        }

        const totalAttachmentBytes = buf.length + (companion?.length ?? 0);
        if (totalAttachmentBytes > MAX_ATTACHMENT_BYTES) {
          console.warn(
            `[sidecar] 附件超过大小限制，已跳过: ${(totalAttachmentBytes / 1024 / 1024).toFixed(2)} MB > ${Math.round(MAX_ATTACHMENT_BYTES / 1024 / 1024)} MB`,
          );
          return false;
        }

        if (budget.usedBytes + totalAttachmentBytes > MAX_MESSAGE_BYTES) {
          console.warn(
            "[sidecar] 跳过附件：单条消息附件总量将超过 %d MB",
            Math.round(MAX_MESSAGE_BYTES / 1024 / 1024),
          );
          return false;
        }
        budget.usedBytes += totalAttachmentBytes;

        const att: Record<string, unknown> = {
          type: isVoice ? "voice" : mime.startsWith("image/") ? "image" : "file",
          mime_type: mime || (isVoice ? "audio/x-caf" : "application/octet-stream"),
          data_base64: buf.toString("base64"),
        };
        if (cname) att.name = cname;
        if (content.size != null) att.size = content.size;
        if (content.duration != null) att.duration = content.duration;
        if (companion) {
          att.companion_data_base64 = companion.toString("base64");
          att.companion_mime_type = companionMimeType;
          if (companionName) att.companion_name = companionName;
        }
        const attachmentIndex = attachments.length;
        attachments.push(att);
        parts.push({
          type: isVoice ? "voice" : "attachment",
          attachment_index: attachmentIndex,
        });
        return true;
      } catch (err) {
        console.error("[sidecar] 读取附件内容失败（发送者: %s）: %s", senderId, String(err));
        return false;
      }
    }

    case "group": {
      let accepted = false;
      for (const item of content.items ?? []) {
        accepted =
          (await collectInboundContent(
            runtime,
            space,
            item?.content ?? item,
            senderId,
            textParts,
            attachments,
            parts,
            budget,
          )) || accepted;
      }
      return accepted;
    }

    case "reply":
      // 正文进入常规消息段；引用目标保存在 native_event.metadata 中。
      return await collectInboundContent(
        runtime,
        space,
        content.content,
        senderId,
        textParts,
        attachments,
        parts,
        budget,
      );

    case "richlink":
      if (typeof content.url === "string" && content.url.length > 0) {
        textParts.push(content.url);
        parts.push({ type: "text", text: content.url });
        return true;
      }
      return false;

    case "contact": {
      const displayName =
        content.name?.formatted ||
        [content.name?.first, content.name?.last].filter(Boolean).join(" ");
      const phones = Array.isArray(content.phones)
        ? content.phones.map((phone: any) => phone?.value).filter(Boolean)
        : [];
      const emails = Array.isArray(content.emails)
        ? content.emails.map((email: any) => email?.value).filter(Boolean)
        : [];
      const details = [displayName, ...phones, ...emails].filter(Boolean).join(" · ");
      const summary = details ? `[联系人] ${details}` : "[联系人名片]";
      textParts.push(summary);
      parts.push({ type: "text", text: summary });
      return true;
    }

    // reaction / poll_option / typing / read 等事件不应被当成一次普通用户发言触发 LLM。
    default:
      console.log("[sidecar] 忽略不作为普通聊天注入的内容类型: %s", content.type);
      return false;
  }
}

let eventSequence = 0;
const seenEvents = new Map<string, number>();
let inboundQueue: Promise<void> = Promise.resolve();

function rememberEvent(key: string): boolean {
  const now = Date.now();
  for (const [oldKey, timestamp] of seenEvents) {
    if (now - timestamp < 30 * 60 * 1000) break;
    seenEvents.delete(oldKey);
  }
  if (seenEvents.has(key)) return false;
  seenEvents.set(key, now);
  while (seenEvents.size > MAX_SEEN_EVENT_ENTRIES) {
    const oldest = seenEvents.keys().next().value;
    if (typeof oldest !== "string") break;
    seenEvents.delete(oldest);
  }
  return true;
}

async function handleInboundMessage(
  runtime: ProjectRuntime,
  space: MessageSpace,
  message: any,
): Promise<void> {
  if (message.direction === "outbound") {
    console.log("[sidecar] 忽略自身 outbound 回声: message_id=%s", message.id);
    return;
  }
  const linePhone = lineForSpace(runtime, space);
  const sourceEventId = String(message.id ?? "").trim()
    || `unknown-${Date.now()}-${eventSequence + 1}`;
  const eventKey = messageDedupeKey(runtime.projectId, linePhone, sourceEventId);
  if (!rememberEvent(eventKey)) {
    console.log("[sidecar] 忽略重复 iMessage 事件: %s", eventKey);
    return;
  }

  rememberSpace(runtime, space);
  rememberMessage(runtime, message, space);

  let readStatus = "not_requested";
  try {
    await message.read();
    readStatus = "marked_chat_read";
  } catch (error) {
    readStatus = "failed";
    console.warn("[sidecar] 标记 iMessage 会话已读失败: %s", String(error));
  }

  const textParts: string[] = [];
  const attachments: Record<string, unknown>[] = [];
  const parts: Record<string, unknown>[] = [];
  const budget = { usedBytes: 0 };
  const accepted = await collectInboundContent(
    runtime,
    space,
    message.content,
    message.sender?.id ?? "未知",
    textParts,
    attachments,
    parts,
    budget,
  );

  if (accepted && INBOUND_REACTION_EMOJI) {
    try {
      await space.send(reaction(INBOUND_REACTION_EMOJI, message));
      console.log("[sidecar] 已发送 iMessage 表情反应 %s: message_id=%s", INBOUND_REACTION_EMOJI, message.id);
    } catch (error) {
      console.warn("[sidecar] 发送 iMessage 表情反应失败: message_id=%s error=%s", message.id, String(error));
    }
  }

  const nativeEvent = {
    schema_version: 1,
    event_id: eventKey,
    sequence: ++eventSequence,
    event_type: messageEventType(message.content?.type),
    content_type: String(message.content?.type ?? "unknown"),
    project_id: runtime.projectId,
    line_phone: linePhone,
    message_id: String(message.id ?? ""),
    chat_id: String(space.id ?? ""),
    space_type: String((space as any).type ?? ""),
    sender: toJsonSafeMetadata(message.sender),
    timestamp: message.timestamp?.getTime?.() ?? Date.now(),
    direction: String(message.direction ?? "inbound"),
    read_status: readStatus,
    metadata: messageContentMetadata(message.content?.type, message.id, message.content),
  };

  const data = {
    message_id: String(message.id ?? ""),
    event_id: eventKey,
    project_id: runtime.projectId,
    chat_id: String(space.id ?? ""),
    space_type: String((space as any).type ?? ""),
    line_phone: linePhone,
    sender: {
      name: String(message.sender?.id ?? "未知"),
      address: String(message.sender?.id ?? ""),
    },
    text: textParts.join("\n"),
    timestamp: message.timestamp?.getTime?.() ?? Date.now(),
    is_from_me: false,
    read_status: readStatus,
    attachments,
    parts,
    native_event: nativeEvent,
  };

  try {
    pyWs.send(JSON.stringify({ type: accepted ? "message" : "native_event", data }));
  } catch (error) {
    console.error("[sidecar] 转发 iMessage 到 MaiBot 失败: %s", String(error));
  }
}

function enqueueInboundMessage(
  runtime: ProjectRuntime,
  space: MessageSpace,
  message: any,
): void {
  inboundQueue = inboundQueue
    .then(() => handleInboundMessage(runtime, space, message))
    .catch((error: unknown) => {
      console.error("[sidecar] 入站消息处理失败，队列继续: %s", String(error));
    });
}

async function consumeProjectMessages(runtime: ProjectRuntime): Promise<void> {
  try {
    for await (const [space, message] of runtime.app.messages) {
      enqueueInboundMessage(runtime, space, message);
    }
  } catch (err) {
    console.error("[sidecar] Photon 消息循环异常退出 (%s): %s", runtime.projectId, String(err));
    try {
      pyWs.send(JSON.stringify({
        type: "error",
        code: "PHOTON_DISCONNECTED",
        project_id: runtime.projectId,
        message: String(err),
        fatal: true,
      }));
    } catch {
      // WebSocket 可能已断开。
    }
    process.exit(1);
  }
}

for (const runtime of runtimes.values()) {
  void consumeProjectMessages(runtime);
}

// ---------------------------------------------------------------------------
// 5. 接收 Python 指令
// ---------------------------------------------------------------------------

function resolveRuntime(projectId: string, linePhone: string): ProjectRuntime {
  if (projectId) {
    const runtime = runtimes.get(projectId);
    if (!runtime) throw new Error(`未配置 Photon 项目: ${projectId}`);
    return runtime;
  }
  if (linePhone) {
    const matches = [...runtimes.values()].filter((runtime) => runtime.lines.includes(linePhone));
    if (matches.length === 1) return matches[0]!;
    if (matches.length > 1) throw new Error(`号码 ${linePhone} 对应多个 Photon 项目，请携带 project_id`);
  }
  if (runtimes.size === 1) return runtimes.values().next().value!;
  throw new Error("多项目模式必须在会话元数据中提供 project_id 或可唯一匹配的 line_phone");
}

function normalizeChatId(value: string): string {
  const target = value.trim();
  if (!target) return "";
  if (target.startsWith("any;-;") || target.startsWith("any;+;") || target.includes(";+;")) return target;
  return `any;-;${target}`;
}

async function resolveSpace(
  runtime: ProjectRuntime,
  targetId: string,
  linePhone = "",
): Promise<MessageSpace | null> {
  const target = normalizeChatId(targetId);
  const current = runtime.currentSpace;
  if (
    current
    && current.id === target
    && (linePhone
      ? lineForSpace(runtime, current) === linePhone
      : runtime.lines.length <= 1)
  ) {
    return current;
  }
  if (linePhone) {
    const cached = runtime.spaces.get(spaceCacheKey(target, linePhone));
    if (cached) {
      rememberSpace(runtime, cached);
      return cached;
    }
  } else {
    const matches = [...runtime.spaces.values()].filter((space) => space.id === target);
    if (matches.length === 1) {
      rememberSpace(runtime, matches[0]!);
      return matches[0]!;
    }
    if (matches.length > 1) throw new Error(`会话 ${target} 属于多个号码，请提供 line_phone`);
  }

  if (!linePhone && runtime.lines.length > 1) {
    throw new Error(`项目 ${runtime.projectId} 有多个 iMessage 号码，冷发送必须提供 line_phone`);
  }
  console.log("[sidecar] 冷发送到: project=%s chat_id=%s line=%s", runtime.projectId, target, linePhone || "auto");
  const space = linePhone
    ? await runtime.im.space.get(target, { phone: linePhone })
    : await runtime.im.space.get(target);
  if (!space) {
    console.warn("[sidecar] im.space.get 返回 null，无法发送到 %s", target);
    return null;
  }
  rememberSpace(runtime, space as MessageSpace);
  return space as MessageSpace;
}

async function createDirectSpace(
  runtime: ProjectRuntime,
  recipient: string,
  linePhone: string,
): Promise<MessageSpace> {
  const normalizedRecipient = recipient.trim();
  if (!normalizedRecipient) throw new Error("open_dm 缺少 recipient");
  if (!linePhone && runtime.lines.length > 1) {
    throw new Error("open_dm 在多号码项目中必须提供 line_phone");
  }
  const params = linePhone ? { phone: linePhone } : undefined;
  const space = await runtime.im.space.create([{ id: normalizedRecipient }], params);
  if (!space) throw new Error(`Photon 未返回新会话: ${normalizedRecipient}`);
  rememberSpace(runtime, space as MessageSpace);
  return space as MessageSpace;
}

async function resolveMessageHandle(
  runtime: ProjectRuntime,
  space: MessageSpace,
  messageId: string,
): Promise<any> {
  const id = messageId.trim();
  if (!id) throw new Error("缺少目标 message_id");
  const key = messageCacheKey(id, space.id, lineForSpace(runtime, space));
  const cached = runtime.messageHandles.get(key);
  if (cached) return cached;
  const fetched = await runtime.im.getMessage(space, id);
  if (!fetched) throw new Error(`找不到目标消息: ${id}`);
  rememberMessage(runtime, fetched, space);
  return fetched;
}

/** Reuse the remote client owned and authenticated by spectrum-ts. */
function getManagedPhotonClient(
  runtime: ProjectRuntime,
  space: MessageSpace,
): AdvancedIMessage {
  const platform = runtime.app.__internal.platforms.get("iMessage") as
    | { client?: unknown }
    | undefined;
  const clients = platform?.client;
  if (!Array.isArray(clients)) {
    throw new Error("Spectrum 没有暴露当前 iMessage 项目的远程客户端");
  }
  const phone = lineForSpace(runtime, space);
  const managed = clients.find((entry: any) => entry?.phone === phone);
  if (!managed?.client) {
    throw new Error(`Spectrum 项目 ${runtime.projectId} 没有线路 ${phone || "unknown"} 的客户端`);
  }
  return managed.client as AdvancedIMessage;
}

function outboundItems(result: unknown): any[] {
  if (Array.isArray(result)) return result.filter(Boolean);
  return result ? [result] : [];
}

function rememberOutbound(runtime: ProjectRuntime, result: unknown, space: MessageSpace): string {
  const items = outboundItems(result);
  for (const item of items) {
    rememberMessage(runtime, item, space);
    if (typeof item?.id === "string") {
      const itemSpace = (item.space ?? space) as MessageSpace;
      const key = messageCacheKey(item.id, itemSpace.id, lineForSpace(runtime, itemSpace));
      runtime.outboundMessageKeys.delete(key);
      runtime.outboundMessageKeys.add(key);
    }
  }
  while (runtime.outboundMessageKeys.size > MAX_MESSAGE_CACHE_ENTRIES) {
    const oldest = runtime.outboundMessageKeys.values().next().value;
    if (typeof oldest !== "string") break;
    runtime.outboundMessageKeys.delete(oldest);
  }
  return items.find((item) => typeof item?.id === "string")?.id ?? "";
}

function buildContentBuilders(data: Record<string, any>): any[] {
  const contents: any[] = [];
  const atts = data.attachments ?? [];
  if (!Array.isArray(atts)) throw new Error("attachments 必须是数组");

  let attachmentBytesUsed = 0;
  const addAttachment = (raw: unknown, index: number): void => {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
      throw new Error(`第 ${index + 1} 个附件必须是对象`);
    }
    const att = raw as Record<string, unknown>;
    if (att.type === "live_photo") {
      throw new Error("Live Photo 必须使用 send_live_photo Action 单独发送");
    }
    const buf = decodeOutboundAttachment(att.data_base64);
    attachmentBytesUsed += buf.length;
    if (attachmentBytesUsed > MAX_MESSAGE_BYTES) {
      throw new Error(`单条消息附件总量超过限制: ${Math.round(MAX_MESSAGE_BYTES / 1024 / 1024)} MB`);
    }
    const mimeType = typeof att.mime_type === "string" && att.mime_type
      ? att.mime_type
      : att.type === "voice" ? "audio/mp4" : "application/octet-stream";
    const name = typeof att.name === "string" && att.name ? att.name : undefined;
    if (att.type === "voice" || mimeType.toLowerCase().startsWith("audio/")) {
      contents.push(voice(buf, {
        mimeType: mimeType.toLowerCase().startsWith("audio/") ? mimeType : "audio/mp4",
        name: name ?? "voice.m4a",
        duration: typeof att.duration === "number" ? att.duration : undefined,
      }));
    } else {
      contents.push(attachment(buf, { mimeType, name }));
    }
  };

  const parts = Array.isArray(data.parts) ? data.parts : [];
  if (parts.length > 0) {
    for (const [index, part] of parts.entries()) {
      if (!part || typeof part !== "object") continue;
      if (part.type === "text") {
        if (typeof part.text === "string" && part.text.trim()) contents.push(text(part.text));
      } else if (part.type === "attachment" || part.type === "voice") {
        const attachmentIndex = part.attachment_index;
        if (!Number.isInteger(attachmentIndex) || attachmentIndex < 0 || attachmentIndex >= atts.length) {
          throw new Error(`第 ${index + 1} 个内容片段引用了无效附件`);
        }
        addAttachment(atts[attachmentIndex], attachmentIndex);
      }
    }
  } else {
    const msgText = typeof data.text === "string" ? data.text : "";
    if (msgText.trim()) contents.push(text(msgText));
    for (const [index, raw] of atts.entries()) addAttachment(raw, index);
  }
  return contents;
}

type DispatchResult = {
  messageId?: string;
  deliveryStatus?: Record<string, unknown>;
  actionMetadata?: Record<string, unknown>;
};

async function dispatchAction(
  runtime: ProjectRuntime,
  space: MessageSpace,
  actionData: Record<string, any>,
): Promise<DispatchResult> {
  const action = String(actionData.action ?? "send");
  const spaceAny = space as any;
  if (action === "send" || action === "send_reply") {
    const builders = buildContentBuilders(actionData);
    if (builders.length === 0) throw new Error(`${action} 没有可发送内容`);
    let sent: unknown;
    if (action === "send_reply") {
      const targetId = String(actionData.reply_to_message_id ?? actionData.target_message_id ?? "");
      const target = await resolveMessageHandle(runtime, space, targetId);
      sent = await spaceAny.send(...builders.map((builder) => reply(builder, target)));
    } else {
      const textPartCount = Array.isArray(actionData.parts)
        ? actionData.parts.filter((part: any) => part?.type === "text").length
        : (String(actionData.text ?? "").trim() ? 1 : 0);
      if (builders.length > 1 && textPartCount <= 1) {
        sent = await spaceAny.send(group(builders[0], builders[1], ...builders.slice(2)));
      } else if (builders.length > 1) {
        // Spectrum multipart allows one text item. Keep unusual interleaved
        // text/media sequences ordered by sending their items serially.
        const sentItems: unknown[] = [];
        for (const builder of builders) sentItems.push(await spaceAny.send(builder));
        sent = sentItems;
      } else {
        sent = await spaceAny.send(builders[0]);
      }
    }
    return { messageId: rememberOutbound(runtime, sent, space) };
  }

  if (action === "unsend_message") {
    const target = await resolveMessageHandle(runtime, space, String(actionData.message_id ?? actionData.target_message_id ?? ""));
    const targetKey = messageCacheKey(target.id, space.id, lineForSpace(runtime, space));
    if (!runtime.outboundMessageKeys.has(targetKey) && target.direction !== "outbound") {
      throw new Error("只能撤回 Spectrum 确认为本账号发送的消息");
    }
    await spaceAny.send(unsend(target));
    runtime.outboundMessageKeys.delete(targetKey);
    return {};
  }

  if (action === "send_reaction") {
    const target = await resolveMessageHandle(runtime, space, String(actionData.target_message_id ?? actionData.message_id ?? ""));
    const emoji = String(actionData.emoji ?? "").trim();
    if (!emoji) throw new Error("send_reaction 缺少 emoji");
    const sent = await spaceAny.send(reaction(emoji, target));
    return { messageId: rememberOutbound(runtime, sent, space) };
  }

  if (action === "send_live_photo") {
    if (String(actionData.text ?? "").trim()) {
      throw new Error("send_live_photo 目前只接受单独的 Live Photo，不接受附带文本");
    }
    const source = actionData.data_base64
      ? actionData
      : (Array.isArray(actionData.attachments)
        ? actionData.attachments.find((entry: any) => entry?.type === "live_photo")
        : undefined);
    if (!source) throw new Error("send_live_photo 缺少 HEIC/HEIF 主图片");
    const name = String(source.name ?? "live-photo.HEIC");
    const mimeType = String(source.mime_type ?? "image/heic").toLowerCase();
    if (!/^image\/hei[cf](?:$|;)/i.test(mimeType) || !/\.(heic|heif)$/i.test(name)) {
      throw new Error("Live Photo 主附件必须是 HEIC/HEIF 图片");
    }
    const primary = decodeOutboundAttachment(source.data_base64);
    const companion = decodeOutboundAttachment(source.companion_data_base64);
    if (primary.length + companion.length > MAX_MESSAGE_BYTES) {
      throw new Error(`Live Photo 主图和视频合计超过单条消息限制: ${Math.round(MAX_MESSAGE_BYTES / 1024 / 1024)} MB`);
    }
    if (primary.length + companion.length > MAX_ATTACHMENT_BYTES) {
      throw new Error(`Live Photo 主图和视频合计超过单附件限制: ${Math.round(MAX_ATTACHMENT_BYTES / 1024 / 1024)} MB`);
    }

    const photon = getManagedPhotonClient(runtime, space);
    const uploaded = await photon.attachments.upload({
      fileName: name,
      data: primary,
      companion: { data: companion },
    });
    const sent = await photon.messages.sendAttachment(space.id, uploaded.attachment.guid);
    const message = {
      id: sent.guid,
      direction: "outbound",
      content: { type: "attachment", name, mimeType, size: primary.length },
      space,
      timestamp: sent.dateCreated,
    };
    const messageId = rememberOutbound(runtime, message, space) || sent.guid;
    return {
      messageId,
      actionMetadata: {
        native: true,
        media: "live_photo",
        companion_name: uploaded.companion?.fileName,
        companion_mime_type: uploaded.companion?.mimeType,
      },
    };
  }

  if (action === "send_audio_message") {
    const source = actionData.data_base64
      ? actionData
      : (Array.isArray(actionData.attachments)
        ? actionData.attachments.find((entry: any) => entry?.type === "voice")
        : undefined);
    if (!source) throw new Error("send_audio_message 缺少 data_base64");
    const bytes = decodeOutboundAttachment(source.data_base64);
    const mimeType = String(source.mime_type ?? "audio/mp4");
    if (!mimeType.toLowerCase().startsWith("audio/")) throw new Error("语音 MIME 类型必须以 audio/ 开头");
    const builder = voice(bytes, {
      mimeType,
      name: String(source.name ?? "voice.m4a"),
      duration: typeof source.duration === "number" ? source.duration : undefined,
    });
    const targetId = String(actionData.reply_to_message_id ?? "");
    const sent = targetId
      ? await spaceAny.send(reply(builder, await resolveMessageHandle(runtime, space, targetId)))
      : await spaceAny.send(builder);
    return { messageId: rememberOutbound(runtime, sent, space) };
  }

  if (action === "send_effect") {
    const builders = buildContentBuilders({ text: actionData.text, attachments: actionData.attachments });
    if (builders.length !== 1) throw new Error("send_effect 每次只能包装一段文本或一个附件");
    const effectName = String(actionData.effect ?? "").trim();
    const effectValues = (imessage as any).effect?.message ?? {};
    const effectValue = effectValues[effectName] ?? Object.values(effectValues).find((value) => value === effectName);
    if (typeof effectValue !== "string") throw new Error(`未知 Message Effect: ${effectName}`);
    const sent = await spaceAny.send(effect(builders[0], effectValue as Parameters<typeof effect>[1]));
    return { messageId: rememberOutbound(runtime, sent, space) };
  }

  if (action === "create_poll") {
    const title = String(actionData.title ?? "").trim();
    const options = actionData.options;
    if (!title || !Array.isArray(options) || options.length === 0) {
      throw new Error("create_poll 需要 title 和至少一个 options 选项");
    }
    const choices = options.map((value: unknown) => {
      const label = typeof value === "string" ? value : String((value as any)?.title ?? "");
      if (!label.trim()) throw new Error("投票选项不能为空");
      return option(label.trim());
    });
    const sent = await spaceAny.send(poll(title, choices));
    return { messageId: rememberOutbound(runtime, sent, space) };
  }

  if (action === "send_link_card") {
    const url = String(actionData.url ?? "").trim();
    const parsed = new URL(url);
    if (parsed.protocol !== "https:" && parsed.protocol !== "http:") throw new Error("链接卡片只接受 HTTP(S) URL");
    const sent = await spaceAny.send(richlink(parsed.toString()));
    return { messageId: rememberOutbound(runtime, sent, space) };
  }

  if (action === "send_vcard") {
    const contactData = actionData.contact ?? actionData.vcard;
    if (!contactData) throw new Error("send_vcard 缺少 contact 或 vcard 数据");
    const sent = await spaceAny.send(contact(contactData));
    return { messageId: rememberOutbound(runtime, sent, space) };
  }

  if (action === "set_chat_background") {
    if (actionData.clear === true) {
      await spaceAny.send(background("clear"));
      return {};
    }
    const bytes = decodeOutboundAttachment(actionData.data_base64);
    const mimeType = String(actionData.mime_type ?? "image/jpeg").toLowerCase();
    if (!new Set(["image/jpeg", "image/png", "image/heic", "image/heif"]).has(mimeType)) {
      throw new Error("Chat Background 仅接受 JPEG、PNG、HEIC 或 HEIF 图片");
    }
    await spaceAny.send(background(bytes, { mimeType }));
    return {};
  }

  if (action === "send_location") {
    const latitude = Number(actionData.latitude);
    const longitude = Number(actionData.longitude);
    if (!Number.isFinite(latitude) || latitude < -90 || latitude > 90 || !Number.isFinite(longitude) || longitude < -180 || longitude > 180) {
      throw new Error("send_location 需要有效的 latitude / longitude");
    }
    const label = String(actionData.label ?? actionData.address ?? "").trim();
    const query = encodeURIComponent(label || `${latitude},${longitude}`);
    const url = `https://maps.apple.com/?ll=${latitude},${longitude}&q=${query}`;
    const sent = await spaceAny.send(richlink(url));
    return {
      messageId: rememberOutbound(runtime, sent, space),
      actionMetadata: { native: false, fallback: "apple_maps_link", latitude, longitude },
    };
  }

  if (action === "send_handwriting") {
    const base64 = actionData.image_base64 ?? actionData.data_base64;
    if (typeof base64 !== "string") throw new Error("当前 Spectrum API 没有原生手写接口；请提供 image_base64 发送静态手写图片");
    const bytes = decodeOutboundAttachment(base64);
    const mimeType = String(actionData.mime_type ?? "image/png");
    if (!mimeType.toLowerCase().startsWith("image/")) {
      throw new Error("静态手写回退需要 image/* MIME 类型");
    }
    const sent = await spaceAny.send(attachment(bytes, {
      mimeType,
      name: String(actionData.name ?? "handwriting.png"),
    }));
    return {
      messageId: rememberOutbound(runtime, sent, space),
      actionMetadata: { native: false, fallback: "static_image" },
    };
  }

  if (action === "open_dm") {
    throw new Error("open_dm 必须在 resolveSpace 前处理");
  }
  if (action === "vote_poll" || action === "unvote_poll" || action === "add_poll_option") {
    const pollMessageId = String(
      actionData.poll_message_id ?? actionData.target_message_id ?? actionData.message_id ?? "",
    ).trim();
    if (!pollMessageId) throw new Error(`${action} 缺少 poll_message_id`);
    const photon = getManagedPhotonClient(runtime, space);
    let pollState: unknown;
    if (action === "vote_poll") {
      const optionId = String(actionData.option_id ?? actionData.option_identifier ?? "").trim();
      if (!optionId) throw new Error("vote_poll 缺少 option_id");
      pollState = await photon.polls.vote(pollMessageId, optionId);
    } else if (action === "unvote_poll") {
      pollState = await photon.polls.unvote(pollMessageId);
    } else {
      const title = String(actionData.title ?? actionData.option ?? "").trim();
      if (!title) throw new Error("add_poll_option 缺少 title");
      pollState = await photon.polls.addOption(pollMessageId, title);
    }
    return {
      actionMetadata: {
        native: true,
        action,
        poll_message_id: pollMessageId,
        poll: toJsonSafeMetadata(pollState),
      },
    };
  }
  if (action === "send_digital_touch") {
    throw new Error("Digital Touch 当前没有已锁定的 spectrum-ts 公共发送 API");
  }
  throw new Error(`未实现的 iMessage Action: ${action}`);
}

async function handleBridgeMessage(raw: WebSocket.RawData): Promise<void> {
  let msg: any;
  try {
    msg = JSON.parse(raw.toString());
  } catch (err) {
    console.warn("[sidecar] 收到无效的 Python WebSocket JSON，已忽略: %s", String(err));
    return;
  }

  if (msg.type === "shutdown") {
    console.log("[sidecar] 收到 Python shutdown 请求，正在关闭…");
    await stopAllRuntimes();
    pyWs.close();
    process.exit(0);
  }
  if (msg.type !== "send" && msg.type !== "action") return;

  const requestId = typeof msg.request_id === "string" ? msg.request_id : "";
  const sendResult = (
    success: boolean,
    error = "",
    result: DispatchResult = {},
  ): void => {
    const deliveryStatus = success && result.messageId
      ? {
          state: "accepted_by_photon",
          delivery_confirmed: false,
          observed_at: new Date().toISOString(),
        }
      : undefined;
    pyWs.send(JSON.stringify({
      type: "send_result",
      request_id: requestId,
      success,
      error,
      external_message_id: result.messageId ?? "",
      delivery_status: deliveryStatus,
      action_metadata: result.actionMetadata,
    }));
  };

  try {
    const data = msg.data && typeof msg.data === "object" ? msg.data as Record<string, any> : {};
    let actionData: Record<string, any> = data;
    if (msg.type === "action") actionData = normalizeStructuredAction(data);
    else actionData = { action: "send", ...data };

    const linePhone = String(data.line_phone ?? "").trim();
    const projectId = String(data.project_id ?? "").trim();
    const runtime = resolveRuntime(projectId, linePhone);
    let targetId = String(data.chat_id ?? "").trim();
    let space: MessageSpace;
    if (actionData.action === "open_dm") {
      space = await createDirectSpace(runtime, String(actionData.recipient ?? targetId), linePhone);
    } else {
      if (!targetId) throw new Error("缺少目标 chat_id");
      targetId = normalizeChatId(targetId);
      const resolved = await resolveSpace(runtime, targetId, linePhone);
      if (!resolved) throw new Error(`无法解析目标空间: ${targetId}`);
      space = resolved;
    }

    const result = await dispatchAction(runtime, space, actionData);
    sendResult(true, "", result);
  } catch (err) {
    const error = String(err) || "Photon 操作失败";
    console.error("[sidecar] iMessage Action 失败: %s", error);
    sendResult(false, error);
  }
}

async function stopAllRuntimes(): Promise<void> {
  await Promise.all([...runtimes.values()].map(async (runtime) => {
    try {
      await runtime.app.stop();
    } catch (error) {
      console.warn("[sidecar] Photon 项目关闭失败 (%s): %s", runtime.projectId, String(error));
    }
  }));
}

let actionQueue: Promise<void> = Promise.resolve();
pyWs.on("message", (raw) => {
  actionQueue = actionQueue
    .then(() => handleBridgeMessage(raw))
    .catch((error: unknown) => {
      console.error("[sidecar] Python 指令处理失败，队列继续: %s", String(error));
    });
});

// ---------------------------------------------------------------------------
// 6. 进程级错误处理
// ---------------------------------------------------------------------------

pyWs.on("close", (code, reason) => {
  console.log(`[sidecar] Python WebSocket 已关闭: code=${code} reason=${reason}`);
  // Python 端主动断开通常意味着 on_unload，正常退出
  if (code !== 1000) {
    process.exit(1);
  }
});

pyWs.on("error", (err) => {
  console.error("[sidecar] Python WebSocket 错误:", err);
});

process.on("SIGTERM", async () => {
  console.log("[sidecar] 收到 SIGTERM，正在关闭…");
  await stopAllRuntimes();
  pyWs.close();
  process.exit(0);
});

process.on("SIGINT", async () => {
  console.log("[sidecar] 收到 SIGINT，正在关闭…");
  await stopAllRuntimes();
  pyWs.close();
  process.exit(0);
});
