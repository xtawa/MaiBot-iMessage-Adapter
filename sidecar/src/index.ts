/**
 * MaiBot iMessage Adapter — Sidecar
 *
 * 任务:
 * 初始化 spectrum-ts SDK 与底层 @photon-ai/advanced-imessage 客户端
 * 连接 Python WebSocket Server，通过本地桥接转发消息与全量原生事件
 * 把 SDK 的 Message/Space/Poll/Chat/Group/Location/MiniApp 翻译成结构化 JSON
 *
 * Made BY Galeros
 */

import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { inflateRawSync } from "node:zlib";

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
import type { AdvancedIMessage, TextEffect, TextFormatInput } from "@photon-ai/advanced-imessage";
import { WebSocket } from "ws";
import {
  BRIDGE_PROTOCOL_VERSION,
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
  : 10 * 1024 * 1024;

const MAX_MESSAGE_MB = parseInt(process.env.MAX_MESSAGE_SIZE_MB ?? "", 10);
const MAX_MESSAGE_BYTES = (Number.isFinite(MAX_MESSAGE_MB) && MAX_MESSAGE_MB > 0)
  ? MAX_MESSAGE_MB * 1024 * 1024
  : 20 * 1024 * 1024;

const MAX_PAYLOAD = 4 * Math.ceil(MAX_MESSAGE_BYTES / 3) + 256 * 1024;
const INBOUND_REACTION_EMOJI = (process.env.INBOUND_REACTION_EMOJI ?? "").trim();

const MAX_SPACE_CACHE_ENTRIES = 256;
const MAX_MESSAGE_CACHE_ENTRIES = 2_048;
const MAX_SEEN_EVENT_ENTRIES = 10_000;
const READ_RETRY_DELAYS_MS = [800, 2_000, 5_000, 10_000];
const DELIVERY_CHECK_DELAY_MS = 12_000;
const POLL_TITLE_ECHO_TTL_MS = 30_000;

// ---------------------------------------------------------------------------
// 2. 初始化 spectrum-ts 官方 SDK 与运行时缓存
// ---------------------------------------------------------------------------

type SpectrumApp = Awaited<ReturnType<typeof Spectrum>>;
type MessageSpace = SpectrumApp["messages"] extends AsyncIterable<infer Item>
  ? Item extends [infer Space, unknown] ? Space : never
  : never;

interface CachedPollRecord {
  pollMessageGuid: string;
  chatGuid: string;
  title: string;
  options: { optionIdentifier: string; text: string }[];
  updatedAt: number;
}

interface ActiveTransferRecord {
  messageGuid: string;
  chatGuid: string;
  amount: string;
  formattedAmount: string;
  note: string;
  currency: string;
  appName: string;
  state: "pending" | "received";
  createdAt: number;
}

type ProjectRuntime = {
  projectId: string;
  projectSecret: string;
  lines: string[];
  app: SpectrumApp;
  im: any;
  currentSpace: MessageSpace | null;
  spaces: Map<string, MessageSpace>;
  messageHandles: Map<string, any>;
  messageTextById: Map<string, string>;
  outboundMessageKeys: Set<string>;
  lastOutboundMessageByChat: Map<string, string>;
  lastInboundMessageByChat: Map<string, string>;
  pollsByChat: Map<string, CachedPollRecord>;
  pollsById: Map<string, CachedPollRecord>;
  recentPollTitles: Map<string, { title: string; timestamp: number }>;
  transfersByMessageId: Map<string, ActiveTransferRecord>;
  streamClosers: (() => Promise<void>)[];
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
      projectSecret: project.project_secret,
      lines: project.lines,
      app: projectApp,
      im: imessage(projectApp) as any,
      currentSpace: null,
      spaces: new Map<string, MessageSpace>(),
      messageHandles: new Map<string, any>(),
      messageTextById: new Map<string, string>(),
      outboundMessageKeys: new Set<string>(),
      lastOutboundMessageByChat: new Map<string, string>(),
      lastInboundMessageByChat: new Map<string, string>(),
      pollsByChat: new Map<string, CachedPollRecord>(),
      pollsById: new Map<string, CachedPollRecord>(),
      recentPollTitles: new Map<string, { title: string; timestamp: number }>(),
      transfersByMessageId: new Map<string, ActiveTransferRecord>(),
      streamClosers: [],
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

pyWs.send(JSON.stringify({
  type: "ready",
  protocol_version: BRIDGE_PROTOCOL_VERSION,
  project_ids: [...runtimes.keys()],
  failed_project_ids: failedProjectIds,
}));
console.log("[sidecar] 已向 Python 发送 ready 信号");

// ---------------------------------------------------------------------------
// 4. 辅助工具：底层客户端获取、断流重试、音频转码、文档解析与音乐/号码查询
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

function extractPlainTextFromContent(content: any): string {
  if (!content || typeof content !== "object") return "";
  if (typeof content.text === "string" && content.text.trim()) return content.text.trim();
  if (content.type === "reply" && content.content) {
    return extractPlainTextFromContent(content.content);
  }
  if (content.type === "group" && Array.isArray(content.items)) {
    return content.items
      .map((item: any) => extractPlainTextFromContent(item?.content ?? item))
      .filter(Boolean)
      .join("\n");
  }
  return "";
}

function rememberMessage(
  runtime: ProjectRuntime,
  message: any,
  fallbackSpace?: MessageSpace,
  explicitText?: string,
): void {
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
  const msgText = (explicitText ?? extractPlainTextFromContent(message.content)).trim();
  if (msgText) {
    runtime.messageTextById.delete(message.id);
    runtime.messageTextById.set(message.id, msgText);
    while (runtime.messageTextById.size > MAX_MESSAGE_CACHE_ENTRIES) {
      const oldest = runtime.messageTextById.keys().next().value;
      if (typeof oldest !== "string") break;
      runtime.messageTextById.delete(oldest);
    }
  }
  if (message.content?.type === "group" && Array.isArray(message.content.items)) {
    for (const item of message.content.items) rememberMessage(runtime, item, space);
  }
  if (message.content?.type === "reply" && message.content.target) {
    rememberMessage(runtime, message.content.target, space);
  }
}

function findCachedMessageById(runtime: ProjectRuntime, messageId: string): any | undefined {
  const target = messageId.trim();
  if (!target) return undefined;
  for (const [key, handle] of runtime.messageHandles.entries()) {
    if (key.endsWith(`\u0000${target}`) || handle?.id === target) {
      return handle;
    }
  }
  return undefined;
}

function getManagedPhotonClients(runtime: ProjectRuntime): { phone: string; client: AdvancedIMessage }[] {
  const platform = runtime.app.__internal?.platforms?.get("iMessage") as
    | { client?: unknown }
    | undefined;
  const clients = platform?.client;
  if (!Array.isArray(clients)) return [];
  return clients
    .filter((entry: any) => entry && entry.client)
    .map((entry: any) => ({
      phone: String(entry.phone ?? ""),
      client: entry.client as AdvancedIMessage,
    }));
}

function getManagedPhotonClient(
  runtime: ProjectRuntime,
  space?: MessageSpace | null,
  preferredPhone = "",
): AdvancedIMessage {
  const entries = getManagedPhotonClients(runtime);
  if (entries.length === 0) {
    throw new Error("Spectrum 没有暴露当前 iMessage 项目的远程客户端");
  }
  const phone = preferredPhone || (space ? lineForSpace(runtime, space) : "");
  if (phone) {
    const matched = entries.find((entry) => entry.phone === phone);
    if (matched) return matched.client;
  }
  return entries[0]!.client;
}

async function readBytesWithRetry(
  reader: () => Promise<Buffer>,
  label: string,
): Promise<Buffer> {
  let lastError: unknown;
  for (let attempt = 0; attempt <= READ_RETRY_DELAYS_MS.length; attempt += 1) {
    if (attempt > 0) {
      const waitMs = READ_RETRY_DELAYS_MS[attempt - 1]!;
      console.warn(
        "[sidecar] 读取 %s 遇到临时流中断，%.1fs 后第 %d 次重试: %s",
        label,
        waitMs / 1000,
        attempt,
        String(lastError),
      );
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
    try {
      return await reader();
    } catch (err) {
      lastError = err;
      if (!isTransientGrpcError(err)) throw err;
    }
  }
  throw lastError;
}

function decodeOutboundAttachment(dataBase64: unknown): Buffer {
  return decodeBase64Attachment(dataBase64, MAX_ATTACHMENT_BYTES);
}

/**
 * 使用系统 ffmpeg（若可用）将出站语音转码为带 `+faststart` 的 AAC M4A 并提取时长，
 * 彻底解决 iOS iMessage 语音气泡显示 `0:00` 或重进会话后气泡消失的问题。
 */
async function transcodeVoiceToFaststartM4a(
  inputBuffer: Buffer,
  fallbackDuration?: number,
): Promise<{ buffer: Buffer; mimeType: string; name: string; duration?: number }> {
  const id = randomUUID();
  const inPath = join(tmpdir(), `maibot-imsg-voice-${id}.in`);
  const outPath = join(tmpdir(), `maibot-imsg-voice-${id}.m4a`);
  try {
    await fs.writeFile(inPath, inputBuffer);
    const stderrText = await new Promise<string>((resolve, reject) => {
      const proc = spawn("ffmpeg", [
        "-y",
        "-i",
        inPath,
        "-vn",
        "-c:a",
        "aac",
        "-b:a",
        "64k",
        "-movflags",
        "+faststart",
        "-f",
        "ipod",
        outPath,
      ], { stdio: ["ignore", "ignore", "pipe"] });

      let errChunks = "";
      proc.stderr?.on("data", (chunk) => {
        errChunks += chunk.toString("utf8");
      });
      const timer = setTimeout(() => {
        proc.kill("SIGKILL");
        reject(new Error("ffmpeg timeout"));
      }, 15_000);
      timer.unref?.();

      proc.on("error", (err) => {
        clearTimeout(timer);
        reject(err);
      });
      proc.on("close", (code) => {
        clearTimeout(timer);
        if (code === 0) resolve(errChunks);
        else reject(new Error(`ffmpeg exited with code ${code}`));
      });
    });

    const outBuffer = await fs.readFile(outPath);
    let duration = fallbackDuration;
    const durMatch = /Duration:\s*(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)/i.exec(stderrText);
    if (durMatch) {
      const parsedSeconds =
        Number(durMatch[1]) * 3600 +
        Number(durMatch[2]) * 60 +
        Number(durMatch[3]);
      if (Number.isFinite(parsedSeconds) && parsedSeconds > 0) {
        duration = Math.round(parsedSeconds * 10) / 10;
      }
    }
    if (outBuffer.length > 0 && outBuffer.length <= MAX_ATTACHMENT_BYTES) {
      return {
        buffer: outBuffer,
        mimeType: "audio/mp4",
        name: "voice.m4a",
        duration,
      };
    }
  } catch {
    // 系统无 ffmpeg 或转码失败时平滑回退到原始音频数据
  } finally {
    await fs.unlink(inPath).catch(() => {});
    await fs.unlink(outPath).catch(() => {});
  }
  const sniffed = sniffMediaMime(inputBuffer, "audio/mp4", "voice.m4a");
  return {
    buffer: inputBuffer,
    mimeType: sniffed.mimeType.startsWith("audio/") ? sniffed.mimeType : "audio/mp4",
    name: "voice.m4a",
    duration: fallbackDuration,
  };
}

/**
 * 从入站纯文本或 .docx 文档附件中提取可读文本，方便 MaiBot LLM 直接阅读文件内容。
 */
function extractDocumentText(buf: Buffer, fileName: string, mimeType: string): string {
  const lowerName = fileName.toLowerCase();
  const lowerMime = mimeType.toLowerCase();
  const maxChars = 2_500;

  if (
    /\.(txt|md|markdown|json|csv|log|yaml|yml|xml|ini|toml|py|js|ts|html|css)$/i.test(lowerName) ||
    lowerMime === "text/plain" ||
    lowerMime === "text/markdown" ||
    lowerMime === "application/json" ||
    lowerMime === "text/csv"
  ) {
    const raw = buf.toString("utf8").replace(/^\uFEFF/, "").replace(/\r\n?/g, "\n").trim();
    if (!raw) return "";
    return raw.length > maxChars ? `${raw.slice(0, maxChars)}…(已截断)` : raw;
  }

  // 轻量免依赖解析 .docx (ZIP 内 word/document.xml)
  if (lowerName.endsWith(".docx") || lowerMime.endsWith("wordprocessingml.document")) {
    try {
      let offset = 0;
      while (offset + 30 <= buf.length) {
        if (buf.readUInt32LE(offset) !== 0x04034b50) break;
        const compression = buf.readUInt16LE(offset + 8);
        const compressedSize = buf.readUInt32LE(offset + 18);
        const nameLen = buf.readUInt16LE(offset + 26);
        const extraLen = buf.readUInt16LE(offset + 28);
        const entryName = buf.subarray(offset + 30, offset + 30 + nameLen).toString("utf8");
        const dataStart = offset + 30 + nameLen + extraLen;
        const dataEnd = dataStart + compressedSize;
        if (dataEnd > buf.length) break;
        if (entryName === "word/document.xml" && compressedSize > 0) {
          const slice = buf.subarray(dataStart, dataEnd);
          const xmlBytes = compression === 8 ? inflateRawSync(slice) : compression === 0 ? slice : null;
          if (xmlBytes) {
            const xml = xmlBytes.toString("utf8");
            const extracted = xml
              .replace(/<\/w:p>/g, "\n")
              .replace(/<[^>]+>/g, "")
              .replace(/&amp;/g, "&")
              .replace(/&lt;/g, "<")
              .replace(/&gt;/g, ">")
              .replace(/&quot;/g, "\"")
              .replace(/\n{3,}/g, "\n\n")
              .trim();
            if (extracted) {
              return extracted.length > maxChars ? `${extracted.slice(0, maxChars)}…(已截断)` : extracted;
            }
          }
          break;
        }
        offset = dataEnd;
      }
    } catch {
      // ignore malformed docx
    }
  }
  return "";
}

/**
 * 音乐搜索与卡片链接解析（同时检索 Apple Music TW 与网易云音乐，借鉴 Uranus music.js 评分策略）。
 */
async function resolveMusicCardTrack(
  query: string,
  preferredSource = "apple",
): Promise<{ title: string; artist: string; url: string; source: string } | null> {
  const cleaned = query.trim().replace(/\s+/g, " ");
  if (!cleaned) return null;

  const normalizeToken = (s: string) =>
    s
      .toLowerCase()
      .replace(/[\s　]+/g, "")
      .replace(/[·・'’‘"“”()（）[\]【】{}<>《》,，.。!！?？:：;；、\-—–~～/\\|_*#@&+]/g, "");

  const splitMatch = /^(.{1,80}?)\s*[-–—－_|/]\s*(.{1,80})$/.exec(cleaned);
  const terms = splitMatch
    ? [splitMatch[1]!.trim(), splitMatch[2]!.trim()].filter(Boolean)
    : [cleaned];
  const coverRegex = /原唱|翻唱|伴奏|纯音乐|口琴|八音盒|铃声|钢琴|吉他|cover|instrumental|karaoke/i;

  const scoreCandidate = (item: { title: string; artist: string }, rank: number) => {
    const nTitle = normalizeToken(item.title);
    const nArtist = normalizeToken(item.artist);
    let score = 0;
    const titleHit = terms.some((t) => {
      const nt = normalizeToken(t);
      return nt && (nTitle === nt || (nt.length >= 2 && nTitle.includes(nt)));
    });
    if (titleHit) score += 2;
    const artistHit = terms.some((t) => {
      const nt = normalizeToken(t);
      if (!nt) return false;
      if (nArtist === nt || (nt.length >= 2 && nArtist.includes(nt))) return true;
      // 简繁同字亲和度匹配
      const setA = new Set(nt);
      const setB = new Set(nArtist);
      let shared = 0;
      for (const ch of setA) if (setB.has(ch)) shared += 1;
      return shared / Math.max(1, Math.min(setA.size, setB.size)) >= 0.6;
    });
    if (artistHit) score += 1;
    else if (coverRegex.test(item.title)) score -= 1;
    else if (rank === 0 && titleHit) score += 0.5;
    return score;
  };

  const searchApple = async (): Promise<{ title: string; artist: string; url: string; source: string }[]> => {
    try {
      const u = new URL("https://itunes.apple.com/search");
      u.searchParams.set("term", terms.join(" "));
      u.searchParams.set("country", "TW");
      u.searchParams.set("media", "music");
      u.searchParams.set("entity", "song");
      u.searchParams.set("limit", "6");
      const res = await fetch(u, { signal: AbortSignal.timeout(6_000) });
      if (!res.ok) return [];
      const data = (await res.json()) as any;
      return (Array.isArray(data?.results) ? data.results : [])
        .filter((r: any) => r?.trackViewUrl && r?.trackName)
        .map((r: any) => ({
          title: String(r.trackName),
          artist: String(r.artistName ?? ""),
          url: String(r.trackViewUrl),
          source: "apple",
        }));
    } catch {
      return [];
    }
  };

  const searchNetease = async (): Promise<{ title: string; artist: string; url: string; source: string }[]> => {
    try {
      const body = new URLSearchParams({
        s: terms.join(" "),
        type: "1",
        offset: "0",
        total: "true",
        limit: "6",
      });
      const res = await fetch("https://music.163.com/api/search/get/web", {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Referer: "https://music.163.com",
        },
        body,
        signal: AbortSignal.timeout(6_000),
      });
      if (!res.ok) return [];
      const data = (await res.json()) as any;
      const songs = data?.result?.songs;
      return (Array.isArray(songs) ? songs : [])
        .filter((s: any) => s?.id && s?.name)
        .map((s: any) => ({
          title: String(s.name),
          artist: Array.isArray(s.artists)
            ? s.artists.map((a: any) => a?.name).filter(Boolean).join("/")
            : "",
          url: `https://music.163.com/song?id=${s.id}`,
          source: "netease",
        }));
    } catch {
      return [];
    }
  };

  const [appleHits, neteaseHits] = await Promise.all([searchApple(), searchNetease()]);
  const all = [
    ...appleHits.map((item, idx) => ({ item, score: scoreCandidate(item, idx) + (preferredSource === "apple" ? 0.1 : 0) })),
    ...neteaseHits.map((item, idx) => ({ item, score: scoreCandidate(item, idx) + (preferredSource === "netease" ? 0.1 : 0) })),
  ].sort((a, b) => b.score - a.score);

  const best = all[0];
  if (!best || best.score < 1.5) return null;
  return best.item;
}

/**
 * Photon 管理 REST API：查询或登记共享号码用户并返回分配的 iMessage 线路号码。
 */
async function enrollPhotonSharedUser(
  projectId: string,
  projectSecret: string,
  rawPhone: string,
): Promise<{ assignedPhoneNumber: string; userId: string; alreadyEnrolled: boolean }> {
  const digits = rawPhone.replace(/\D/g, "");
  const phoneNumber = digits ? `+${digits}` : "";
  if (!/^\+[1-9]\d{6,14}$/.test(phoneNumber)) {
    throw new Error("手机号格式无效，需要 E.164 国际格式（例如 +8613800138000）");
  }
  const baseUrl = (process.env.SPECTRUM_CLOUD_URL ?? "https://spectrum.photon.codes").replace(/\/+$/, "");
  const auth = Buffer.from(`${projectId}:${projectSecret}`).toString("base64");

  // 1. 先查是否已经登记过
  try {
    const getRes = await fetch(`${baseUrl}/projects/${projectId}/users/`, {
      headers: { Authorization: `Basic ${auth}` },
      signal: AbortSignal.timeout(8_000),
    });
    if (getRes.ok) {
      const body = (await getRes.json()) as any;
      const users = body?.data?.users ?? body?.data ?? [];
      if (Array.isArray(users)) {
        const hit = users.find((u: any) => u?.phoneNumber === phoneNumber && u?.assignedPhoneNumber);
        if (hit) {
          return {
            assignedPhoneNumber: String(hit.assignedPhoneNumber),
            userId: String(hit.id ?? ""),
            alreadyEnrolled: true,
          };
        }
      }
    }
  } catch {
    // 忽略查询错误，继续尝试 POST 登记
  }

  // 2. 登记共享用户
  const postRes = await fetch(`${baseUrl}/projects/${projectId}/users/`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Basic ${auth}`,
    },
    body: JSON.stringify({ type: "shared", phoneNumber }),
    signal: AbortSignal.timeout(10_000),
  });
  const textBody = await postRes.text();
  let jsonBody: any = null;
  try {
    jsonBody = JSON.parse(textBody);
  } catch {
    // ignore
  }
  if (!postRes.ok) {
    const detail = jsonBody?.message ?? jsonBody?.error ?? textBody.slice(0, 200);
    throw new Error(`Photon 登记共享用户失败 (HTTP ${postRes.status}): ${detail}`);
  }
  const data = jsonBody?.data ?? jsonBody;
  const assigned = String(data?.assignedPhoneNumber ?? "").trim();
  if (!assigned) {
    throw new Error("Photon 响应未返回 assignedPhoneNumber");
  }
  return {
    assignedPhoneNumber: assigned,
    userId: String(data?.id ?? ""),
    alreadyEnrolled: false,
  };
}

// ---------------------------------------------------------------------------
// 5. 消费 Photon 消息流与原生 gRPC 事件流 → 翻译 → 发给 Python
// ---------------------------------------------------------------------------

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

function sendNativeEventToPython(
  runtime: ProjectRuntime,
  linePhone: string,
  chatId: string,
  senderAddress: string,
  eventType: string,
  contentType: string,
  sourceEventId: string,
  metadata: Record<string, unknown>,
  messageId = "",
): void {
  const eventKey = messageDedupeKey(runtime.projectId, linePhone, sourceEventId);
  if (!rememberEvent(eventKey)) return;

  const isGroup = chatId.includes(";+;");
  const nativeEvent = {
    schema_version: 2,
    event_id: eventKey,
    sequence: ++eventSequence,
    event_type: eventType,
    content_type: contentType,
    project_id: runtime.projectId,
    line_phone: linePhone,
    message_id: messageId,
    chat_id: chatId,
    space_type: isGroup ? "group" : "dm",
    sender: { id: senderAddress },
    timestamp: Date.now(),
    direction: "inbound",
    read_status: "not_requested",
    metadata: toJsonSafeMetadata(metadata),
  };

  const data = {
    message_id: messageId,
    event_id: eventKey,
    project_id: runtime.projectId,
    chat_id: chatId,
    space_type: isGroup ? "group" : "dm",
    line_phone: linePhone,
    sender: {
      name: senderAddress || "iMessage 系统事件",
      address: senderAddress,
    },
    text: "",
    timestamp: Date.now(),
    is_from_me: false,
    is_system_event: true,
    read_status: "not_requested",
    attachments: [],
    parts: [],
    native_event: nativeEvent,
  };

  try {
    pyWs.send(JSON.stringify({ type: "native_event", data }));
  } catch (err) {
    console.error("[sidecar] 转发原生事件到 Python 失败: %s", String(err));
  }
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
        let processedText = content.text;
        // 识别正文中的 Apple Maps 链接并补充可读位置说明
        const urlMatches = processedText.match(/https?:\/\/[^\s<>「」【】()（）]+/gi) ?? [];
        for (const rawUrl of urlMatches) {
          const parsedLoc = parseAppleMapsUrl(rawUrl.replace(/[.,;:!?。，；：！？]+$/, ""));
          if (parsedLoc && !processedText.includes(parsedLoc.formatted)) {
            processedText += `\n${parsedLoc.formatted}`;
          }
        }
        textParts.push(processedText);
        parts.push({ type: "text", text: processedText });
        return true;
      }
      return false;

    case "attachment":
    case "voice":
    case "sticker": {
      const rawMime = String(content.mimeType ?? "");
      const cname = String(content.name ?? "");
      const isVoice =
        content.type === "voice" ||
        rawMime.startsWith("audio/") ||
        cname.toLowerCase().endsWith(".caf");

      try {
        let buf: Buffer;
        let companion: Buffer | undefined;
        let companionMimeType = "video/quicktime";
        let companionName = "";
        const isHeif = /^image\/hei[cf](?:$|;)/i.test(rawMime) || /\.(heic|heif)$/i.test(cname);

        if (isHeif && typeof content.id === "string" && content.id) {
          try {
            const streamResult = await readBytesWithRetry(async () => {
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
              if (companionChunks.length > 0) {
                companion = Buffer.concat(companionChunks);
              }
              return primaryChunks.length > 0
                ? Buffer.concat(primaryChunks)
                : await content.read();
            }, `LivePhoto(${cname || content.id})`);
            buf = streamResult;
          } catch (error) {
            console.warn("[sidecar] Live Photo 附件流读取失败，回退到主图片: %s", String(error));
            buf = await readBytesWithRetry(() => content.read(), `附件(${cname || "image"})`);
            companion = undefined;
          }
        } else {
          buf = await readBytesWithRetry(() => content.read(), `附件(${cname || content.type})`);
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

        const sniffed = sniffMediaMime(buf, rawMime, cname);
        const mime = rawMime && rawMime !== "application/octet-stream" ? rawMime : sniffed.mimeType;
        const isImage = content.type === "sticker" || mime.startsWith("image/");
        const isVideo = !isVoice && !isImage && mime.startsWith("video/");

        const att: Record<string, unknown> = {
          type: isVoice ? "voice" : isImage ? "image" : isVideo ? "video" : "file",
          mime_type: mime || (isVoice ? "audio/x-caf" : "application/octet-stream"),
          data_base64: buf.toString("base64"),
        };
        if (cname) att.name = cname;
        else if (content.type === "sticker") att.name = `sticker.${sniffed.extension}`;
        if (content.size != null) att.size = content.size;
        if (content.duration != null) att.duration = content.duration;
        if (companion) {
          att.companion_data_base64 = companion.toString("base64");
          att.companion_mime_type = companionMimeType;
          if (companionName) att.companion_name = companionName;
        }

        // 若是文本/文档类文件，自动抽取正文摘要供 MaiBot 阅读
        if (!isVoice && !isImage && !isVideo) {
          const docText = extractDocumentText(buf, cname, mime);
          if (docText) {
            att.extracted_text = docText;
            const docNotice = `[文件「${cname || "文档"}」内容]:\n${docText}`;
            textParts.push(docNotice);
            parts.push({ type: "text", text: docNotice });
          }
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
        const parsedLoc = parseAppleMapsUrl(content.url);
        const linkSummary = parsedLoc
          ? `${parsedLoc.formatted} ${content.url}`
          : content.title
            ? `[链接卡片: ${content.title}] ${content.url}`
            : content.url;
        textParts.push(linkSummary);
        parts.push({ type: "text", text: linkSummary });
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

    default:
      return false;
  }
}

/**
 * 尝试解析入站消息上的 Balloon / MiniApp 卡片、手写消息、Digital Touch、Apple Cash 或消息动效。
 */
async function enrichInboundWithNativeFeatures(
  runtime: ProjectRuntime,
  space: MessageSpace,
  message: any,
  textParts: string[],
  attachments: Record<string, unknown>[],
  parts: Record<string, unknown>[],
  budget: { usedBytes: number },
): Promise<{ acceptedExtra: boolean; extraMetadata: Record<string, unknown> }> {
  let acceptedExtra = false;
  const extraMetadata: Record<string, unknown> = {};
  const msgId = String(message.id ?? "").trim();
  const chatId = String(space.id ?? "").trim();

  // 1. 尝试获取 balloonBundleId / expressiveSendStyleId / formatting
  let balloonBundleId = String(message.balloonBundleId ?? message.content?.balloonBundleId ?? "").trim();
  let styleId = String(message.expressiveSendStyleId ?? message.content?.expressiveSendStyleId ?? "").trim();
  let formatting = message.formatting ?? message.content?.formatting;
  let nativeMsg: any = null;

  const isUnsupportedOrEmpty =
    textParts.length === 0 &&
    attachments.length === 0 &&
    !["reaction", "poll", "poll_option", "typing", "read"].includes(String(message.content?.type ?? ""));

  if ((balloonBundleId || isUnsupportedOrEmpty) && msgId) {
    try {
      const photon = getManagedPhotonClient(runtime, space);
      nativeMsg = await photon.messages.get(msgId);
      if (nativeMsg?.content) {
        if (!balloonBundleId && nativeMsg.content.balloonBundleId) {
          balloonBundleId = String(nativeMsg.content.balloonBundleId);
        }
        if (!styleId && nativeMsg.content.expressiveSendStyleId) {
          styleId = String(nativeMsg.content.expressiveSendStyleId);
        }
        if (!formatting && nativeMsg.content.formatting) {
          formatting = nativeMsg.content.formatting;
        }
      }
    } catch {
      // ignore if native message lookup fails
    }
  }

  // 2. 处理 Balloon / MiniApp / 手写 / Digital Touch / Find My / Apple Cash
  const balloonInfo = parseBalloonBundle(balloonBundleId);
  if (balloonInfo && balloonInfo.kind !== "url") {
    extraMetadata.balloon = balloonInfo;
    if (balloonInfo.supportsEmbeddedMedia && msgId && chatId) {
      try {
        const photon = getManagedPhotonClient(runtime, space);
        const embedded = await readBytesWithRetry(async () => {
          const res = await photon.messages.getEmbeddedMedia(chatId, msgId);
          return Buffer.from(res.data);
        }, `${balloonInfo.appName}(${msgId})`);
        if (
          embedded.length > 0 &&
          embedded.length <= MAX_ATTACHMENT_BYTES &&
          budget.usedBytes + embedded.length <= MAX_MESSAGE_BYTES
        ) {
          budget.usedBytes += embedded.length;
          const sniffed = sniffMediaMime(embedded, "image/png", `${balloonInfo.kind}.png`);
          const isImage = sniffed.mimeType.startsWith("image/");
          const attIndex = attachments.length;
          attachments.push({
            type: isImage ? "image" : "file",
            mime_type: sniffed.mimeType,
            name: `${balloonInfo.kind}.${sniffed.extension}`,
            data_base64: embedded.toString("base64"),
            size: embedded.length,
          });
          parts.push({ type: "attachment", attachment_index: attIndex });
          const labelText = `[iMessage ${balloonInfo.appName}]`;
          textParts.push(labelText);
          parts.push({ type: "text", text: labelText });
          acceptedExtra = true;
        }
      } catch (err) {
        console.warn("[sidecar] 提取内嵌媒体 (%s) 失败: %s", balloonInfo.appName, String(err));
      }
      if (!acceptedExtra) {
        const fallbackText = `[iMessage ${balloonInfo.appName}]`;
        textParts.push(fallbackText);
        parts.push({ type: "text", text: fallbackText });
        acceptedExtra = true;
      }
    } else if (balloonInfo.kind === "find_my") {
      const findMyText = "[位置共享] 对方通过「查找 (Find My)」向你共享了实时位置";
      textParts.push(findMyText);
      parts.push({ type: "text", text: findMyText });
      acceptedExtra = true;
    } else {
      const miniApp = (nativeMsg as any)?.content?.miniApp ?? (message as any)?.miniApp;
      const layoutText = summarizeMiniAppLayout(miniApp?.layout);
      const appTitle = balloonInfo.appName || String(miniApp?.appName ?? "").trim() || "iMessage 扩展";
      const cardUrl = String(miniApp?.url ?? "").trim();
      const summary = `[卡片分享: ${appTitle}${layoutText ? ` · ${layoutText}` : ""}${cardUrl ? ` (${cardUrl})` : ""}]`;
      extraMetadata.mini_app = {
        app_name: appTitle,
        summary: layoutText,
        url: cardUrl,
      };
      textParts.push(summary);
      parts.push({ type: "text", text: summary });
      acceptedExtra = true;
    }
  }

  // 3. 处理全屏/气泡特效 (expressiveSendStyleId)
  if (styleId) {
    const effectDesc = resolveEffectDescriptor(styleId);
    if (effectDesc) {
      extraMetadata.message_effect = effectDesc;
      const scopeLabel = effectDesc.scope === "screen" ? "全屏特效" : "气泡特效";
      const effectNotice = `[消息特效: ${effectDesc.label}（${scopeLabel}）]`;
      textParts.push(effectNotice);
      parts.push({ type: "text", text: effectNotice });
    }
  }

  // 4. 处理 iOS 18 逐词文字动效 (formatting)
  const rawCombinedText = textParts.join("\n") || String(nativeMsg?.content?.text ?? "");
  const wordEffects = extractTextEffects(rawCombinedText, formatting);
  if (wordEffects.length > 0) {
    extraMetadata.text_effects = wordEffects;
    const animations = wordEffects.filter((e) => e.type === "text_effect");
    if (animations.length > 0) {
      const descList = animations
        .map((a) => (a.snippet ? `“${a.snippet}”(${a.label})` : a.label))
        .join("、");
      const animNotice = `[文字动效: ${descList}]`;
      textParts.push(animNotice);
      parts.push({ type: "text", text: animNotice });
    }
  }

  return { acceptedExtra, extraMetadata };
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
  const chatId = String(space.id ?? "");
  const msgId = String(message.id ?? "").trim();
  const sourceEventId = msgId || `unknown-${Date.now()}-${eventSequence + 1}`;
  const eventKey = messageDedupeKey(runtime.projectId, linePhone, sourceEventId);
  if (!rememberEvent(eventKey)) {
    console.log("[sidecar] 忽略重复 iMessage 事件: %s", eventKey);
    return;
  }

  // 抑制 iOS 发起投票时顺带发送的同名纯文本标题回声
  if (message.content?.type === "text" && typeof message.content?.text === "string") {
    const recentPoll = runtime.recentPollTitles.get(chatId);
    if (
      recentPoll &&
      Date.now() - recentPoll.timestamp < POLL_TITLE_ECHO_TTL_MS &&
      recentPoll.title === message.content.text.trim()
    ) {
      runtime.recentPollTitles.delete(chatId);
      console.log("[sidecar] 已抑制 iOS 投票标题重复文本回声: %s", recentPoll.title);
      return;
    }
  }

  rememberSpace(runtime, space);

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

  let accepted = await collectInboundContent(
    runtime,
    space,
    message.content,
    message.sender?.id ?? "未知",
    textParts,
    attachments,
    parts,
    budget,
  );

  const { acceptedExtra, extraMetadata } = await enrichInboundWithNativeFeatures(
    runtime,
    space,
    message,
    textParts,
    attachments,
    parts,
    budget,
  );
  accepted = accepted || acceptedExtra;

  rememberMessage(runtime, message, space, textParts.join("\n"));
  if (accepted && msgId && chatId) {
    runtime.lastInboundMessageByChat.set(chatId, msgId);
  }

  // 若是对方给转账卡片贴了 Tapback/Emoji，自动将转账标记为「已收款」并改写气泡
  if (message.content?.type === "reaction") {
    const targetId = String(message.content?.target?.id ?? "").trim();
    const transfer = targetId ? runtime.transfersByMessageId.get(targetId) : undefined;
    if (transfer && transfer.state === "pending") {
      transfer.state = "received";
      extraMetadata.transfer_claimed = {
        message_id: targetId,
        amount: transfer.formattedAmount,
        note: transfer.note,
      };
    }
  }

  // 解析引用回复的目标消息 ID 与被引原文
  let replyToInfo: Record<string, unknown> | undefined;
  if (message.content?.type === "reply" && message.content?.target) {
    const targetId = String(message.content.target.id ?? "").trim();
    const targetText =
      extractPlainTextFromContent(message.content.target.content) ||
      runtime.messageTextById.get(targetId) ||
      "";
    if (targetId) {
      replyToInfo = {
        target_message_id: targetId,
        text: targetText,
      };
    }
  }

  if (accepted && INBOUND_REACTION_EMOJI) {
    try {
      await space.send(reaction(INBOUND_REACTION_EMOJI, message));
      console.log("[sidecar] 已发送 iMessage 表情反应 %s: message_id=%s", INBOUND_REACTION_EMOJI, message.id);
    } catch (error) {
      console.warn("[sidecar] 发送 iMessage 表情反应失败: message_id=%s error=%s", message.id, String(error));
    }
  }

  const baseMetadata = messageContentMetadata(message.content?.type, message.id, message.content);
  const mergedMetadata =
    baseMetadata && typeof baseMetadata === "object" && !Array.isArray(baseMetadata)
      ? { ...(baseMetadata as Record<string, unknown>), ...extraMetadata }
      : { value: baseMetadata, ...extraMetadata };

  const nativeEvent = {
    schema_version: 2,
    event_id: eventKey,
    sequence: ++eventSequence,
    event_type: extraMetadata.transfer_claimed
      ? "transfer.claimed"
      : messageEventType(message.content?.type),
    content_type: String(message.content?.type ?? "unknown"),
    project_id: runtime.projectId,
    line_phone: linePhone,
    message_id: msgId,
    chat_id: chatId,
    space_type: String((space as any).type ?? (chatId.includes(";+;") ? "group" : "dm")),
    sender: toJsonSafeMetadata(message.sender),
    timestamp: message.timestamp?.getTime?.() ?? Date.now(),
    direction: String(message.direction ?? "inbound"),
    read_status: readStatus,
    metadata: mergedMetadata,
  };

  const data: Record<string, unknown> = {
    message_id: msgId,
    event_id: eventKey,
    project_id: runtime.projectId,
    chat_id: chatId,
    space_type: String((space as any).type ?? (chatId.includes(";+;") ? "group" : "dm")),
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
  if (replyToInfo) {
    data.reply_to = replyToInfo;
  }

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

/**
 * 订阅底层 AdvancedIMessage 的 live streams：
 * 1. messages.subscribeEvents() -> 捕获撤回 (message.unsent 含原文回溯)、编辑 (message.edited)、已读回执 (message.read)、贴纸 (message.stickerPlaced)
 * 2. chats.subscribeEvents()    -> 捕获对方更换或移除聊天背景 (chat.backgroundChanged / chat.backgroundRemoved)
 * 3. polls.subscribeEvents()    -> 捕获投票创建 (created)、加选项 (optionAdded)、投票/撤票并自动回源补全空标题
 * 4. groups.subscribeEvents()   -> 捕获群名称修改、成员增减、群头像变更
 * 5. locations.watch()          -> 捕获 Find My 共享位置实时更新
 */
function startNativeEventStreams(runtime: ProjectRuntime): void {
  const managedClients = getManagedPhotonClients(runtime);
  for (const { phone, client } of managedClients) {
    // 1) Message Events (unsend / edit / read / sticker)
    try {
      const msgStream = client.messages.subscribeEvents();
      runtime.streamClosers.push(() => msgStream.close().catch(() => {}));
      (async () => {
        try {
          for await (const ev of msgStream) {
            if ((ev as any).isFromMe) continue;
            const chatId = String(ev.chatGuid ?? "");
            const actor = String(ev.actor?.address ?? "");
            if (ev.type === "message.unsent") {
              const unsentText = runtime.messageTextById.get(ev.messageGuid) ?? "";
              sendNativeEventToPython(
                runtime,
                phone,
                chatId,
                actor,
                "message.unsent",
                "unsend",
                `unsend:${ev.messageGuid}:${ev.sequence}`,
                {
                  message_id: ev.messageGuid,
                  unsent_text: unsentText,
                  retracted_at: ev.retractedAt,
                },
                ev.messageGuid,
              );
            } else if (ev.type === "message.edited") {
              const previousText = runtime.messageTextById.get(ev.messageGuid) ?? "";
              const newText = String(ev.content?.text ?? "").trim();
              if (newText) runtime.messageTextById.set(ev.messageGuid, newText);
              sendNativeEventToPython(
                runtime,
                phone,
                chatId,
                actor,
                "message.edited",
                "edit",
                `edit:${ev.messageGuid}:${ev.sequence}`,
                {
                  message_id: ev.messageGuid,
                  previous_text: previousText,
                  new_text: newText,
                  edited_at: ev.editedAt,
                },
                ev.messageGuid,
              );
            } else if (ev.type === "message.read") {
              sendNativeEventToPython(
                runtime,
                phone,
                chatId,
                actor,
                "read.updated",
                "read",
                `read:${ev.messageGuid}:${ev.sequence}`,
                {
                  message_id: ev.messageGuid,
                  read_at: ev.readAt,
                },
                ev.messageGuid,
              );
            } else if (ev.type === "message.stickerPlaced") {
              sendNativeEventToPython(
                runtime,
                phone,
                chatId,
                actor,
                "sticker.placed",
                "sticker",
                `sticker:${ev.messageGuid}:${ev.sequence}`,
                {
                  message_id: ev.messageGuid,
                  sticker: ev.sticker,
                  placement: ev.placement,
                },
                ev.messageGuid,
              );
            }
          }
        } catch (err) {
          console.debug("[sidecar] message 事件流已结束 (%s): %s", phone || runtime.projectId, String(err));
        }
      })();
    } catch {
      // ignore if stream unsupported
    }

    // 2) Chat Events (backgroundChanged / backgroundRemoved)
    try {
      const chatStream = client.chats.subscribeEvents();
      runtime.streamClosers.push(() => chatStream.close().catch(() => {}));
      (async () => {
        try {
          for await (const ev of chatStream) {
            if (ev.isFromMe) continue;
            if (ev.type === "chat.backgroundChanged" || ev.type === "chat.backgroundRemoved") {
              const isChanged = ev.type === "chat.backgroundChanged";
              sendNativeEventToPython(
                runtime,
                phone,
                String(ev.chatGuid ?? ""),
                String(ev.actor?.address ?? ""),
                isChanged ? "chat.background_changed" : "chat.background_removed",
                isChanged ? "backgroundChanged" : "backgroundRemoved",
                `chatbg:${ev.chatGuid}:${ev.sequence}`,
                {
                  action: isChanged ? "changed" : "removed",
                  chat_guid: ev.chatGuid,
                },
              );
            }
          }
        } catch (err) {
          console.debug("[sidecar] chat 事件流已结束 (%s): %s", phone || runtime.projectId, String(err));
        }
      })();
    } catch {
      // ignore
    }

    // 3) Poll Events (created / optionAdded / voted / unvoted + 解决 Spectrum ZodError 空标题问题)
    try {
      const pollStream = client.polls.subscribeEvents();
      runtime.streamClosers.push(() => pollStream.close().catch(() => {}));
      (async () => {
        try {
          for await (const ev of pollStream) {
            if (ev.isFromMe) continue;
            const pollGuid = String(ev.pollMessageGuid ?? "");
            const chatGuid = String(ev.chatGuid ?? "");
            const delta = ev.delta;
            if (!pollGuid || !delta) continue;

            let cached = runtime.pollsById.get(pollGuid);
            let title = "title" in delta ? String(delta.title ?? "").trim() : cached?.title ?? "";
            let options =
              "options" in delta && Array.isArray(delta.options)
                ? delta.options.map((o) => ({
                    optionIdentifier: String(o.optionIdentifier ?? ""),
                    text: String(o.text ?? "").trim(),
                  }))
                : cached?.options ?? [];

            if (!title || options.length === 0) {
              try {
                const fetched = await client.polls.get(pollGuid);
                if (fetched) {
                  title = String(fetched.title ?? title).trim();
                  if (Array.isArray(fetched.options) && fetched.options.length > 0) {
                    options = fetched.options.map((o) => ({
                      optionIdentifier: String(o.optionIdentifier ?? ""),
                      text: String(o.text ?? "").trim(),
                    }));
                  }
                }
              } catch {
                // ignore
              }
            }

            cached = {
              pollMessageGuid: pollGuid,
              chatGuid,
              title: title || "未命名投票",
              options,
              updatedAt: Date.now(),
            };
            runtime.pollsById.set(pollGuid, cached);
            if (chatGuid) runtime.pollsByChat.set(chatGuid, cached);

            if (delta.type === "created" && title && chatGuid) {
              runtime.recentPollTitles.set(chatGuid, { title, timestamp: Date.now() });
            }

            if (delta.type === "created" || delta.type === "optionAdded") {
              sendNativeEventToPython(
                runtime,
                phone,
                chatGuid,
                String(ev.actor?.address ?? ""),
                delta.type === "created" ? "poll.created" : "poll.option_added",
                delta.type === "created" ? "poll" : "optionAdded",
                `poll:${pollGuid}:${delta.type}:${ev.sequence}`,
                {
                  poll_message_id: pollGuid,
                  title: cached.title,
                  options: cached.options.map((o, idx) => ({
                    letter: String.fromCharCode(65 + idx),
                    option_id: o.optionIdentifier,
                    title: o.text,
                  })),
                },
                pollGuid,
              );
            } else if (delta.type === "voted" || delta.type === "unvoted") {
              const matchedOpt = cached.options.find(
                (o) => o.optionIdentifier === delta.optionIdentifier,
              );
              sendNativeEventToPython(
                runtime,
                phone,
                chatGuid,
                String(ev.actor?.address ?? ""),
                "poll.vote",
                "poll_option",
                `poll:${pollGuid}:${delta.type}:${delta.optionIdentifier}:${ev.sequence}`,
                {
                  poll_message_id: pollGuid,
                  option_id: delta.optionIdentifier,
                  selected: delta.type === "voted",
                  option: {
                    id: delta.optionIdentifier,
                    title: matchedOpt?.text ?? delta.optionIdentifier,
                  },
                  poll: {
                    id: pollGuid,
                    title: cached.title,
                  },
                },
                pollGuid,
              );
            }
          }
        } catch (err) {
          console.debug("[sidecar] poll 事件流已结束 (%s): %s", phone || runtime.projectId, String(err));
        }
      })();
    } catch {
      // ignore
    }

    // 4) Group Events
    try {
      const groupStream = client.groups.subscribeEvents();
      runtime.streamClosers.push(() => groupStream.close().catch(() => {}));
      (async () => {
        try {
          for await (const ev of groupStream) {
            if (ev.isFromMe) continue;
            sendNativeEventToPython(
              runtime,
              phone,
              String(ev.chatGuid ?? ""),
              String(ev.actor?.address ?? ""),
              "group.changed",
              "groupChanged",
              `group:${ev.chatGuid}:${ev.sequence}`,
              {
                change: ev.change,
              },
            );
          }
        } catch (err) {
          console.debug("[sidecar] group 事件流已结束 (%s): %s", phone || runtime.projectId, String(err));
        }
      })();
    } catch {
      // ignore
    }
  }
}

/**
 * 启动时探活本项目的 iMessage 线路注册状态（借鉴 Uranus delivery.js:checkLineRegistered）。
 */
async function verifyRuntimeLines(runtime: ProjectRuntime): Promise<void> {
  const managed = getManagedPhotonClients(runtime);
  for (const { phone, client } of managed) {
    const targetPhone = phone || runtime.lines[0] || "";
    if (!targetPhone) continue;
    try {
      const available = await client.addresses.isIMessageAvailable(targetPhone);
      if (!available) {
        console.warn(
          "[sidecar] ⚠ 线路号码 %s 当前在 Apple 侧显示未激活 iMessage，收发可能受限",
          targetPhone,
        );
      } else {
        console.log("[sidecar] 线路号码 %s iMessage 探活正常", targetPhone);
      }
    } catch {
      // ignore probe errors on startup
    }
  }
}

/**
 * 出站消息发送后 12 秒异步回查投递回执（借鉴 Uranus delivery.js:watchDelivery）。
 */
function scheduleOutboundDeliveryCheck(
  runtime: ProjectRuntime,
  space: MessageSpace,
  messageId: string,
): void {
  if (!messageId) return;
  const linePhone = lineForSpace(runtime, space);
  const chatId = String(space.id ?? "");
  const timer = setTimeout(async () => {
    try {
      const photon = getManagedPhotonClient(runtime, space, linePhone);
      const msg = await photon.messages.get(messageId);
      if (!msg) return;
      const delivered = Boolean(msg.isDelivered) || Boolean(msg.dateDelivered);
      const errorCode = Number(msg.sendErrorCode ?? 0);
      if (errorCode !== 0 || !delivered) {
        let imessageAvailable: boolean | null = null;
        const peerMatch = chatId.includes(";-;") ? chatId.slice(chatId.indexOf(";-;") + 3).trim() : "";
        if (peerMatch) {
          try {
            imessageAvailable = await photon.addresses.isIMessageAvailable(peerMatch);
          } catch {
            imessageAvailable = null;
          }
        }
        console.warn(
          "[sidecar] 出站投递诊断: message_id=%s chat=%s delivered=%s sendErrorCode=%d peer_imessage=%s",
          messageId,
          chatId,
          String(delivered),
          errorCode,
          String(imessageAvailable),
        );
        if (errorCode !== 0 || imessageAvailable === false) {
          sendNativeEventToPython(
            runtime,
            linePhone,
            chatId,
            peerMatch,
            "delivery.updated",
            "delivery",
            `delivery:${messageId}:${errorCode}`,
            {
              message_id: messageId,
              state: errorCode !== 0 ? `error_${errorCode}` : "peer_imessage_unavailable",
              delivered,
              send_error_code: errorCode,
              peer_imessage_available: imessageAvailable,
            },
            messageId,
          );
        }
      }
    } catch {
      // ignore background diagnostic errors
    }
  }, DELIVERY_CHECK_DELAY_MS);
  timer.unref?.();
}

for (const runtime of runtimes.values()) {
  void consumeProjectMessages(runtime);
  startNativeEventStreams(runtime);
  void verifyRuntimeLines(runtime);
}

// ---------------------------------------------------------------------------
// 6. 接收 Python 指令与分发执行
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
  if (
    target.startsWith("any;-;") ||
    target.startsWith("any;+;") ||
    target.startsWith("iMessage;-;") ||
    target.startsWith("iMessage;+;") ||
    target.includes(";+;")
  ) {
    return target;
  }
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
    current &&
    current.id === target &&
    (linePhone
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
  fallbackDirection: "inbound" | "outbound" = "inbound",
): Promise<any> {
  let id = messageId.trim();
  if (!id || id.toLowerCase() === "last" || id.toLowerCase() === "latest") {
    id =
      (fallbackDirection === "outbound"
        ? runtime.lastOutboundMessageByChat.get(space.id)
        : runtime.lastInboundMessageByChat.get(space.id)) ?? "";
  }
  if (!id) throw new Error("缺少目标 message_id，且当前会话无最近消息缓存");
  const key = messageCacheKey(id, space.id, lineForSpace(runtime, space));
  const cached = runtime.messageHandles.get(key) ?? findCachedMessageById(runtime, id);
  if (cached) return cached;
  const fetched = await runtime.im.getMessage(space, id);
  if (!fetched) throw new Error(`找不到目标消息: ${id}`);
  rememberMessage(runtime, fetched, space);
  return fetched;
}

function outboundItems(result: unknown): any[] {
  if (Array.isArray(result)) return result.filter(Boolean);
  return result ? [result] : [];
}

function rememberOutbound(
  runtime: ProjectRuntime,
  result: unknown,
  space: MessageSpace,
  explicitText?: string,
): string {
  const items = outboundItems(result);
  for (const item of items) {
    rememberMessage(runtime, item, space, explicitText);
    if (typeof item?.id === "string") {
      const itemSpace = (item.space ?? space) as MessageSpace;
      const key = messageCacheKey(item.id, itemSpace.id, lineForSpace(runtime, itemSpace));
      runtime.outboundMessageKeys.delete(key);
      runtime.outboundMessageKeys.add(key);
      runtime.lastOutboundMessageByChat.set(itemSpace.id, item.id);
      scheduleOutboundDeliveryCheck(runtime, itemSpace, item.id);
    }
  }
  while (runtime.outboundMessageKeys.size > MAX_MESSAGE_CACHE_ENTRIES) {
    const oldest = runtime.outboundMessageKeys.values().next().value;
    if (typeof oldest !== "string") break;
    runtime.outboundMessageKeys.delete(oldest);
  }
  return items.find((item) => typeof item?.id === "string")?.id ?? "";
}

async function buildContentBuilders(data: Record<string, any>): Promise<any[]> {
  const contents: any[] = [];
  const atts = data.attachments ?? [];
  if (!Array.isArray(atts)) throw new Error("attachments 必须是数组");

  let attachmentBytesUsed = 0;
  const addAttachment = async (raw: unknown, index: number): Promise<void> => {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
      throw new Error(`第 ${index + 1} 个附件必须是对象`);
    }
    const att = raw as Record<string, unknown>;
    if (att.type === "live_photo") {
      throw new Error("Live Photo 必须使用 send_live_photo Action 单独发送");
    }
    const rawBuf = decodeOutboundAttachment(att.data_base64);
    attachmentBytesUsed += rawBuf.length;
    if (attachmentBytesUsed > MAX_MESSAGE_BYTES) {
      throw new Error(`单条消息附件总量超过限制: ${Math.round(MAX_MESSAGE_BYTES / 1024 / 1024)} MB`);
    }
    const sniffed = sniffMediaMime(
      rawBuf,
      typeof att.mime_type === "string" ? att.mime_type : "",
      typeof att.name === "string" ? att.name : "",
    );
    const mimeType =
      typeof att.mime_type === "string" && att.mime_type && att.mime_type !== "application/octet-stream"
        ? att.mime_type
        : sniffed.mimeType;
    const rawName = typeof att.name === "string" && att.name ? att.name : undefined;

    if (att.type === "voice" || mimeType.toLowerCase().startsWith("audio/")) {
      const transcoded = await transcodeVoiceToFaststartM4a(
        rawBuf,
        typeof att.duration === "number" ? att.duration : undefined,
      );
      contents.push(voice(transcoded.buffer, {
        mimeType: transcoded.mimeType,
        name: transcoded.name,
        duration: transcoded.duration,
      }));
    } else {
      // 确保图片/表情包后缀与真实字节格式一致，防止 iOS 渲染为空白文件图标
      let finalName = rawName;
      if (mimeType.startsWith("image/") && finalName && !/\.[a-z0-9]{2,5}$/i.test(finalName)) {
        finalName = `${finalName}.${sniffed.extension}`;
      } else if (mimeType.startsWith("image/") && finalName?.endsWith(".png") && sniffed.extension !== "png" && sniffed.extension !== "bin") {
        finalName = finalName.replace(/\.png$/i, `.${sniffed.extension}`);
      }
      contents.push(attachment(rawBuf, { mimeType, name: finalName }));
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
        await addAttachment(atts[attachmentIndex], attachmentIndex);
      }
    }
  } else {
    const msgText = typeof data.text === "string" ? data.text : "";
    if (msgText.trim()) contents.push(text(msgText));
    for (const [index, raw] of atts.entries()) await addAttachment(raw, index);
  }
  return contents;
}

type DispatchResult = {
  messageId?: string;
  deliveryStatus?: Record<string, unknown>;
  actionMetadata?: Record<string, unknown>;
};

const TRANSFER_TEAM_ID = "P8XT6232SL";
const TRANSFER_BUNDLE_ID = "codes.photon.Spectrum.MessagesExtension";
const TRANSFER_APP_STORE_ID = 6777616651;
const TRANSFER_URL = "https://photon.codes";

async function dispatchAction(
  runtime: ProjectRuntime,
  space: MessageSpace,
  actionData: Record<string, any>,
): Promise<DispatchResult> {
  const action = String(actionData.action ?? "send");
  const spaceAny = space as any;

  if (action === "send" || action === "send_reply") {
    // 若指定了 iOS 18 文字格式/动效 (text_effect / formatting) 或主题行 (subject)，优先调用底层 photon.messages.sendText
    const textContent = String(actionData.text ?? "").trim();
    const hasNoAttachments = !Array.isArray(actionData.attachments) || actionData.attachments.length === 0;
    const textEffectName = String(actionData.text_effect ?? "").trim().toLowerCase();
    const rawFormatting = Array.isArray(actionData.formatting) ? actionData.formatting : [];
    const subjectLine = String(actionData.subject ?? "").trim();

    if (
      action === "send" &&
      textContent &&
      hasNoAttachments &&
      (textEffectName || rawFormatting.length > 0 || subjectLine)
    ) {
      const photon = getManagedPhotonClient(runtime, space);
      const formattingInputs: TextFormatInput[] = [];
      if (textEffectName && ["big", "small", "shake", "nod", "explode", "ripple", "bloom", "jitter"].includes(textEffectName)) {
        formattingInputs.push({
          type: "effect",
          effect: textEffectName as TextEffect,
          start: 0,
          length: textContent.length,
        });
      }
      for (const f of rawFormatting) {
        if (f && typeof f === "object" && typeof f.type === "string") {
          formattingInputs.push(f as TextFormatInput);
        }
      }
      const effectDesc = resolveEffectDescriptor(actionData.effect);
      const sent = await photon.messages.sendText(space.id, textContent, {
        ...(formattingInputs.length > 0 ? { formatting: formattingInputs } : {}),
        ...(subjectLine ? { subject: subjectLine } : {}),
        ...(effectDesc ? { effect: effectDesc.id as any } : {}),
      });
      const messageId = rememberOutbound(runtime, { id: sent.guid, space }, space, textContent) || sent.guid;
      return {
        messageId,
        actionMetadata: {
          native: true,
          text_effect: textEffectName || undefined,
          subject: subjectLine || undefined,
        },
      };
    }

    const builders = await buildContentBuilders(actionData);
    if (builders.length === 0) throw new Error(`${action} 没有可发送内容`);
    let sent: unknown;
    if (action === "send_reply") {
      const targetId = String(actionData.reply_to_message_id ?? actionData.target_message_id ?? "last");
      const target = await resolveMessageHandle(runtime, space, targetId, "inbound");
      sent = await spaceAny.send(...builders.map((builder) => reply(builder, target)));
    } else {
      const textPartCount = Array.isArray(actionData.parts)
        ? actionData.parts.filter((part: any) => part?.type === "text").length
        : (textContent ? 1 : 0);
      if (builders.length > 1 && textPartCount <= 1) {
        sent = await spaceAny.send(group(builders[0], builders[1], ...builders.slice(2)));
      } else if (builders.length > 1) {
        const sentItems: unknown[] = [];
        for (const builder of builders) sentItems.push(await spaceAny.send(builder));
        sent = sentItems;
      } else {
        sent = await spaceAny.send(builders[0]);
      }
    }
    return { messageId: rememberOutbound(runtime, sent, space, textContent) };
  }

  if (action === "edit_message") {
    const newText = String(actionData.new_text ?? actionData.text ?? "").trim();
    if (!newText) throw new Error("edit_message 缺少 new_text");
    const rawTargetId = String(actionData.message_id ?? actionData.target_message_id ?? "last");
    const target = await resolveMessageHandle(runtime, space, rawTargetId, "outbound");
    const photon = getManagedPhotonClient(runtime, space);
    const partIndex = typeof actionData.part_index === "number" ? actionData.part_index : undefined;
    const edited = await photon.messages.edit(space.id, target.id, newText, {
      ...(partIndex !== undefined ? { partIndex } : {}),
    });
    runtime.messageTextById.set(target.id, newText);
    return {
      messageId: edited?.guid ?? target.id,
      actionMetadata: { native: true, action: "edit_message", message_id: target.id, new_text: newText },
    };
  }

  if (action === "unsend_message") {
    const rawTargetId = String(actionData.message_id ?? actionData.target_message_id ?? "last");
    const target = await resolveMessageHandle(runtime, space, rawTargetId, "outbound");
    const targetKey = messageCacheKey(target.id, space.id, lineForSpace(runtime, space));
    if (!runtime.outboundMessageKeys.has(targetKey) && target.direction !== "outbound") {
      throw new Error("只能撤回 Spectrum 确认为本账号发送的消息");
    }
    const delayMs = Number(actionData.delay_ms ?? 0);
    if (Number.isFinite(delayMs) && delayMs > 0 && delayMs <= 5_000) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
    await spaceAny.send(unsend(target));
    runtime.outboundMessageKeys.delete(targetKey);
    return {
      actionMetadata: { native: true, action: "unsend_message", message_id: target.id },
    };
  }

  if (action === "send_reaction" || action === "remove_reaction") {
    const rawTargetId = String(actionData.target_message_id ?? actionData.message_id ?? "last");
    const target = await resolveMessageHandle(runtime, space, rawTargetId, "inbound");
    const emoji = String(actionData.emoji ?? "❤️").trim();
    if (!emoji) throw new Error(`${action} 缺少 emoji`);
    if (action === "remove_reaction" || actionData.remove === true) {
      const photon = getManagedPhotonClient(runtime, space);
      await photon.messages.setReaction(space.id, target.id, { kind: "emoji", emoji }, false);
      return {
        actionMetadata: { native: true, action: "remove_reaction", target_message_id: target.id, emoji },
      };
    }
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
    const rawBytes = decodeOutboundAttachment(source.data_base64);
    const mimeType = String(source.mime_type ?? "audio/mp4");
    if (!mimeType.toLowerCase().startsWith("audio/")) throw new Error("语音 MIME 类型必须以 audio/ 开头");
    const transcoded = await transcodeVoiceToFaststartM4a(
      rawBytes,
      typeof source.duration === "number" ? source.duration : undefined,
    );
    const builder = voice(transcoded.buffer, {
      mimeType: transcoded.mimeType,
      name: String(source.name ?? transcoded.name),
      duration: transcoded.duration,
    });
    const targetId = String(actionData.reply_to_message_id ?? "");
    const sent = targetId
      ? await spaceAny.send(reply(builder, await resolveMessageHandle(runtime, space, targetId)))
      : await spaceAny.send(builder);
    return { messageId: rememberOutbound(runtime, sent, space) };
  }

  if (action === "send_effect") {
    const builders = await buildContentBuilders({ text: actionData.text, attachments: actionData.attachments });
    if (builders.length !== 1) throw new Error("send_effect 每次只能包装一段文本或一个附件");
    const rawEffectName = String(actionData.effect ?? "").trim();
    const effectDesc = resolveEffectDescriptor(rawEffectName);
    const effectValues = (imessage as any).effect?.message ?? {};
    const effectValue =
      effectDesc?.id ??
      effectValues[rawEffectName] ??
      Object.values(effectValues).find((value) => value === rawEffectName);
    if (typeof effectValue !== "string") throw new Error(`未知 Message Effect: ${rawEffectName}`);
    const sent = await spaceAny.send(effect(builders[0], effectValue as Parameters<typeof effect>[1]));
    return {
      messageId: rememberOutbound(runtime, sent, space, String(actionData.text ?? "")),
      actionMetadata: effectDesc
        ? { native: true, effect: effectDesc.key, label: effectDesc.label, scope: effectDesc.scope }
        : { native: true, effect: rawEffectName },
    };
  }

  if (action === "create_poll") {
    const title = String(actionData.title ?? "").trim();
    const options = actionData.options;
    if (!title || !Array.isArray(options) || options.length === 0) {
      throw new Error("create_poll 需要 title 和至少一个 options 选项");
    }
    const choiceLabels: string[] = [];
    const choices = options.map((value: unknown) => {
      const label = typeof value === "string" ? value : String((value as any)?.title ?? "");
      if (!label.trim()) throw new Error("投票选项不能为空");
      choiceLabels.push(label.trim());
      return option(label.trim());
    });
    const sent = await spaceAny.send(poll(title, choices));
    const messageId = rememberOutbound(runtime, sent, space, title);
    if (messageId) {
      const pollRecord: CachedPollRecord = {
        pollMessageGuid: messageId,
        chatGuid: space.id,
        title,
        options: choiceLabels.map((lbl, idx) => ({
          optionIdentifier: `${idx + 1}`,
          text: lbl,
        })),
        updatedAt: Date.now(),
      };
      runtime.pollsByChat.set(space.id, pollRecord);
      runtime.pollsById.set(messageId, pollRecord);
    }
    return {
      messageId,
      actionMetadata: { native: true, title, options: choiceLabels },
    };
  }

  if (action === "send_link_card") {
    const url = String(actionData.url ?? "").trim();
    const parsed = new URL(url);
    if (parsed.protocol !== "https:" && parsed.protocol !== "http:") throw new Error("链接卡片只接受 HTTP(S) URL");
    const sent = await spaceAny.send(richlink(parsed.toString()));
    return { messageId: rememberOutbound(runtime, sent, space, parsed.toString()) };
  }

  if (action === "send_music_card") {
    const query = String(
      actionData.query ??
        ([actionData.artist, actionData.title ?? actionData.song].filter(Boolean).join("-")),
    ).trim();
    if (!query) throw new Error("send_music_card 需要提供 query（如“歌手-歌名”）或 title");
    const preferredSource = String(actionData.source ?? "apple").toLowerCase();
    const resolved = await resolveMusicCardTrack(query, preferredSource);
    if (!resolved) {
      // 未检索到曲库链接时降级为纯文本分享，避免静默丢失
      const fallbackText = `🎵 推荐歌曲：${query}`;
      const sent = await spaceAny.send(text(fallbackText));
      return {
        messageId: rememberOutbound(runtime, sent, space, fallbackText),
        actionMetadata: { native: false, fallback: "plain_text", query },
      };
    }
    const sent = await spaceAny.send(richlink(resolved.url));
    return {
      messageId: rememberOutbound(runtime, sent, space, resolved.url),
      actionMetadata: {
        native: true,
        media: "music_card",
        title: resolved.title,
        artist: resolved.artist,
        url: resolved.url,
        source: resolved.source,
      },
    };
  }

  if (action === "send_transfer_card" || action === "update_transfer_card") {
    const photon = getManagedPhotonClient(runtime, space);
    const rawAmount = actionData.amount ?? "0";
    const currency = String(actionData.currency ?? "￥").trim() || "￥";
    const formattedAmount = formatTransferAmount(rawAmount, currency);
    const note = String(actionData.note ?? actionData.memo ?? "转账").trim();
    const appName = String(actionData.app_name ?? "转账").trim() || "转账";
    const state: "pending" | "received" =
      action === "update_transfer_card" || actionData.state === "received"
        ? "received"
        : "pending";
    const stateLabel = state === "received" ? "已收款" : "待收款";
    const imageBytes = actionData.image_base64
      ? decodeOutboundAttachment(actionData.image_base64)
      : undefined;

    const sent = await photon.messages.sendCustomizedMiniApp(space.id, {
      appName,
      appStoreId: TRANSFER_APP_STORE_ID,
      extensionBundleId: TRANSFER_BUNDLE_ID,
      teamId: TRANSFER_TEAM_ID,
      url: TRANSFER_URL,
      layout: {
        caption: formattedAmount,
        ...(note ? { subcaption: note } : {}),
        trailingCaption: stateLabel,
        ...(imageBytes ? { image: imageBytes, imageTitle: appName } : {}),
        summary: `转账 ${formattedAmount}${note ? ` · ${note}` : ""}（${stateLabel}）`,
      },
    });
    const messageId = rememberOutbound(runtime, { id: sent.guid, space }, space, formattedAmount) || sent.guid;
    runtime.transfersByMessageId.set(messageId, {
      messageGuid: messageId,
      chatGuid: space.id,
      amount: String(rawAmount),
      formattedAmount,
      note,
      currency,
      appName,
      state,
      createdAt: Date.now(),
    });
    return {
      messageId,
      actionMetadata: {
        native: true,
        media: "transfer_card",
        amount: formattedAmount,
        note,
        state: stateLabel,
      },
    };
  }

  if (action === "send_vcard") {
    const contactData = actionData.contact ?? actionData.vcard;
    if (!contactData) throw new Error("send_vcard 缺少 contact 或 vcard 数据");
    const sent = await spaceAny.send(contact(contactData));
    return { messageId: rememberOutbound(runtime, sent, space) };
  }

  if (action === "share_my_contact") {
    const photon = getManagedPhotonClient(runtime, space);
    await photon.chats.shareContactInfo(space.id);
    return { actionMetadata: { native: true, action: "share_my_contact" } };
  }

  if (action === "set_chat_background") {
    if (actionData.clear === true) {
      await spaceAny.send(background("clear"));
      return { actionMetadata: { native: true, action: "clear_chat_background" } };
    }
    const bytes = decodeOutboundAttachment(actionData.data_base64);
    const mimeType = String(actionData.mime_type ?? "image/jpeg").toLowerCase();
    if (!new Set(["image/jpeg", "image/png", "image/heic", "image/heif"]).has(mimeType)) {
      throw new Error("Chat Background 仅接受 JPEG、PNG、HEIC 或 HEIF 图片");
    }
    await spaceAny.send(background(bytes, { mimeType }));
    return { actionMetadata: { native: true, action: "set_chat_background" } };
  }

  if (action === "send_location") {
    const label = String(actionData.label ?? actionData.address ?? actionData.name ?? "").trim();
    const url = buildAppleMapsUrl(label, actionData.latitude, actionData.longitude);
    const sent = await spaceAny.send(richlink(url));
    return {
      messageId: rememberOutbound(runtime, sent, space, url),
      actionMetadata: {
        native: true,
        media: "apple_maps_card",
        url,
        label,
        latitude: actionData.latitude,
        longitude: actionData.longitude,
      },
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

  if (action === "place_sticker") {
    const rawTargetId = String(actionData.target_message_id ?? actionData.message_id ?? "last");
    const target = await resolveMessageHandle(runtime, space, rawTargetId, "inbound");
    const bytes = decodeOutboundAttachment(actionData.data_base64 ?? actionData.image_base64);
    const sniffed = sniffMediaMime(bytes, String(actionData.mime_type ?? "image/png"), String(actionData.name ?? "sticker.png"));
    const photon = getManagedPhotonClient(runtime, space);
    const uploaded = await photon.attachments.upload({
      fileName: `sticker.${sniffed.extension}`,
      data: bytes,
    });
    const sent = await photon.messages.placeSticker(space.id, target.id, uploaded.attachment.guid, {
      x: typeof actionData.x === "number" ? actionData.x : 0.5,
      y: typeof actionData.y === "number" ? actionData.y : 0.5,
      scale: typeof actionData.scale === "number" ? actionData.scale : 1.0,
      rotation: typeof actionData.rotation === "number" ? actionData.rotation : 0,
    });
    return {
      messageId: sent?.guid ?? target.id,
      actionMetadata: { native: true, action: "place_sticker", target_message_id: target.id },
    };
  }

  if (action === "set_typing") {
    const isTyping = actionData.typing !== false && actionData.is_typing !== false;
    const photon = getManagedPhotonClient(runtime, space);
    await photon.chats.setTyping(space.id, isTyping);
    return { actionMetadata: { native: true, action: "set_typing", is_typing: isTyping } };
  }

  if (action === "mark_read") {
    const photon = getManagedPhotonClient(runtime, space);
    await photon.chats.markRead(space.id);
    return { actionMetadata: { native: true, action: "mark_read", chat_id: space.id } };
  }

  if (action === "notify_silenced") {
    const rawTargetId = String(actionData.message_id ?? actionData.target_message_id ?? "last");
    const target = await resolveMessageHandle(runtime, space, rawTargetId, "outbound");
    const photon = getManagedPhotonClient(runtime, space);
    await photon.messages.notifySilenced(space.id, target.id);
    return { actionMetadata: { native: true, action: "notify_silenced", message_id: target.id } };
  }

  if (action === "manage_group") {
    const photon = getManagedPhotonClient(runtime, space);
    const operation = String(actionData.operation ?? actionData.sub_action ?? "rename").trim().toLowerCase();
    if (operation === "rename" || operation === "set_display_name") {
      const displayName = String(actionData.display_name ?? actionData.name ?? "").trim();
      if (!displayName) throw new Error("manage_group rename 缺少 display_name");
      const updated = await photon.groups.setDisplayName(space.id, displayName);
      return { actionMetadata: { native: true, operation, chat: toJsonSafeMetadata(updated) } };
    }
    if (operation === "add_participants" || operation === "remove_participants") {
      const addresses = Array.isArray(actionData.participants ?? actionData.addresses)
        ? (actionData.participants ?? actionData.addresses).map((a: unknown) => String(a).trim()).filter(Boolean)
        : String(actionData.participants ?? actionData.addresses ?? "")
            .split(/[,，\s]+/)
            .map((a) => a.trim())
            .filter(Boolean);
      if (addresses.length === 0) throw new Error(`manage_group ${operation} 缺少 participants 号码列表`);
      const updated =
        operation === "add_participants"
          ? await photon.groups.addParticipants(space.id, addresses)
          : await photon.groups.removeParticipants(space.id, addresses);
      return { actionMetadata: { native: true, operation, participants: addresses, chat: toJsonSafeMetadata(updated) } };
    }
    if (operation === "leave") {
      await photon.groups.leave(space.id);
      return { actionMetadata: { native: true, operation: "leave" } };
    }
    if (operation === "set_icon") {
      const bytes = decodeOutboundAttachment(actionData.data_base64 ?? actionData.image_base64);
      await photon.groups.setIcon(space.id, bytes);
      return { actionMetadata: { native: true, operation: "set_icon" } };
    }
    if (operation === "remove_icon" || operation === "clear_icon") {
      await photon.groups.removeIcon(space.id);
      return { actionMetadata: { native: true, operation: "remove_icon" } };
    }
    throw new Error(`未知的 manage_group 操作: ${operation}`);
  }

  if (action === "find_my_location") {
    const photon = getManagedPhotonClient(runtime, space);
    const operation = String(actionData.operation ?? "get").trim().toLowerCase();
    const peerAddress =
      String(actionData.address ?? "").trim() ||
      (space.id.includes(";-;") ? space.id.slice(space.id.indexOf(";-;") + 3).trim() : "");
    if (operation === "list") {
      const list = await photon.locations.list();
      return { actionMetadata: { native: true, operation: "list", locations: toJsonSafeMetadata(list) } };
    }
    if (operation === "request") {
      if (!peerAddress) throw new Error("find_my_location request 缺少目标 address");
      const receipt = await photon.locations.request(space.id, peerAddress);
      return { actionMetadata: { native: true, operation: "request", receipt: toJsonSafeMetadata(receipt) } };
    }
    if (!peerAddress) throw new Error("find_my_location get 缺少目标 address");
    const loc = await photon.locations.get(peerAddress);
    return { actionMetadata: { native: true, operation: "get", location: toJsonSafeMetadata(loc) } };
  }

  if (action === "open_dm") {
    throw new Error("open_dm 必须在 resolveSpace 前处理");
  }

  if (action === "vote_poll" || action === "unvote_poll" || action === "add_poll_option") {
    const photon = getManagedPhotonClient(runtime, space);
    let pollMessageId = String(
      actionData.poll_message_id ?? actionData.target_message_id ?? actionData.message_id ?? "",
    ).trim();
    if (!pollMessageId || pollMessageId.toLowerCase() === "last" || pollMessageId.toLowerCase() === "latest") {
      pollMessageId = runtime.pollsByChat.get(space.id)?.pollMessageGuid ?? "";
    }
    if (!pollMessageId) throw new Error(`${action} 缺少 poll_message_id，且当前会话无已知的投票缓存`);

    let pollState: any;
    if (action === "vote_poll") {
      const rawOptionQuery = String(
        actionData.option_id ?? actionData.option_identifier ?? actionData.option ?? actionData.choice ?? "",
      ).trim();
      if (!rawOptionQuery) throw new Error("vote_poll 缺少 option_id 或选项字母/名称");

      let resolvedOptionId = rawOptionQuery;
      let cachedPoll = runtime.pollsById.get(pollMessageId) ?? runtime.pollsByChat.get(space.id);
      if (!cachedPoll || cachedPoll.options.length === 0) {
        try {
          const fetched = await photon.polls.get(pollMessageId);
          if (fetched && Array.isArray(fetched.options)) {
            cachedPoll = {
              pollMessageGuid: pollMessageId,
              chatGuid: space.id,
              title: String(fetched.title ?? ""),
              options: fetched.options.map((o) => ({
                optionIdentifier: String(o.optionIdentifier ?? ""),
                text: String(o.text ?? ""),
              })),
              updatedAt: Date.now(),
            };
            runtime.pollsById.set(pollMessageId, cachedPoll);
            runtime.pollsByChat.set(space.id, cachedPoll);
          }
        } catch {
          // ignore
        }
      }
      if (cachedPoll && cachedPoll.options.length > 0) {
        const matched = matchPollOption(cachedPoll.options, rawOptionQuery);
        if (matched) resolvedOptionId = matched.optionIdentifier;
      }
      pollState = await photon.polls.vote(pollMessageId, resolvedOptionId);
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

    // 不依赖现有聊天 Space 的全局诊断/管理操作
    if (actionData.action === "check_imessage_availability") {
      const address = String(actionData.address ?? actionData.recipient ?? data.chat_id ?? "")
        .replace(/^(?:any|iMessage);-;/i, "")
        .trim();
      if (!address) throw new Error("check_imessage_availability 缺少目标 address");
      const photon = getManagedPhotonClient(runtime, null, linePhone);
      const available = await photon.addresses.isIMessageAvailable(address);
      let services: readonly string[] = [];
      let country: string | null = null;
      let focusSilenced: boolean | null = null;
      try {
        const info = await photon.addresses.get(address);
        services = info.services ?? [];
        country = info.country ?? null;
      } catch {
        // ignore
      }
      try {
        focusSilenced = await photon.addresses.isFocusSilenced(address);
      } catch {
        // ignore
      }
      sendResult(true, "", {
        actionMetadata: {
          native: true,
          action: "check_imessage_availability",
          address,
          imessage_available: available,
          services,
          country,
          focus_silenced: focusSilenced,
        },
      });
      return;
    }

    if (actionData.action === "enroll_shared_user") {
      const phone = String(actionData.phone_number ?? actionData.address ?? actionData.recipient ?? "").trim();
      if (!phone) throw new Error("enroll_shared_user 缺少 phone_number");
      const enrolled = await enrollPhotonSharedUser(runtime.projectId, runtime.projectSecret, phone);
      sendResult(true, "", {
        actionMetadata: {
          native: true,
          action: "enroll_shared_user",
          phone_number: phone,
          assigned_phone_number: enrolled.assignedPhoneNumber,
          user_id: enrolled.userId,
          already_enrolled: enrolled.alreadyEnrolled,
        },
      });
      return;
    }

    let targetId = String(data.chat_id ?? "").trim();
    let space: MessageSpace;
    if (actionData.action === "open_dm") {
      space = await createDirectSpace(runtime, String(actionData.recipient ?? targetId), linePhone);
      sendResult(true, "", {
        actionMetadata: {
          native: true,
          action: "open_dm",
          chat_id: space.id,
        },
      });
      return;
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
    for (const closeStream of runtime.streamClosers) {
      await closeStream().catch(() => {});
    }
    runtime.streamClosers = [];
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
// 7. 进程级错误处理
// ---------------------------------------------------------------------------

pyWs.on("close", (code, reason) => {
  console.log(`[sidecar] Python WebSocket 已关闭: code=${code} reason=${reason}`);
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
