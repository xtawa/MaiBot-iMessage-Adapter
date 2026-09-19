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

import { Spectrum, attachment, reaction, text } from "spectrum-ts";
import { imessage } from "spectrum-ts/providers/imessage";
import { WebSocket } from "ws";

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

const PROJECT_ID = (process.env.PHOTON_PROJECT_ID ?? "").trim();
if (!PROJECT_ID) {
  console.error("[sidecar] 缺少 PHOTON_PROJECT_ID 环境变量");
  process.exit(1);
}

const PROJECT_SECRET = (process.env.PHOTON_PROJECT_SECRET ?? "").trim();
if (!PROJECT_SECRET) {
  console.error("[sidecar] 缺少 PHOTON_PROJECT_SECRET 环境变量");
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

// ---------------------------------------------------------------------------
// 2. 初始化 spectrum-ts 官方 SDK
// ---------------------------------------------------------------------------

let app: Awaited<ReturnType<typeof Spectrum>>;
try {
  app = await Spectrum({
    projectId: PROJECT_ID,
    projectSecret: PROJECT_SECRET,
    providers: [imessage.config()],
  });
  console.log("[sidecar] spectrum-ts SDK 初始化完成");
} catch (err) {
  console.error("[sidecar] Photon 认证失败:", err);
  process.exit(2); // exit code 2 = 认证失败，Python 端不重启
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
pyWs.send(JSON.stringify({ type: "ready" }));
console.log("[sidecar] 已向 Python 发送 ready 信号");

// ---------------------------------------------------------------------------
// 4. 消费 Photon 消息流 → 翻译 → 发给 Python
// ---------------------------------------------------------------------------

// 用于发送消息时引用当前的 space 上下文
// spectrum-ts 的消息循环中 [space, message] 是一个整体
// 但我们在 'message' 事件中异步接收 Python 的发送指令，
// 需要一个共享的 space 引用。这里使用一个简单的闭包捕获。
let currentSpace: Awaited<ReturnType<typeof Spectrum>>["messages"] extends AsyncIterable<infer T>
  ? T extends [infer S, unknown] ? S : never
  : never;

// 缓存已见过的 space（按 id），支持冷发送到之前会话中出现过的 chat_id
const spaceCache = new Map<string, typeof currentSpace>();

function rememberSpace(space: typeof currentSpace): void {
  // Map 的插入顺序可作为轻量 LRU 使用，避免长期运行时会话缓存无限增长。
  spaceCache.delete(space.id);
  spaceCache.set(space.id, space);

  if (spaceCache.size <= MAX_SPACE_CACHE_ENTRIES) {
    return;
  }

  const oldestSpaceId = spaceCache.keys().next().value;
  if (typeof oldestSpaceId === "string") {
    spaceCache.delete(oldestSpaceId);
  }
}

function decodeOutboundAttachment(dataBase64: unknown): Buffer {
  if (typeof dataBase64 !== "string") {
    throw new Error("附件 data_base64 必须是字符串");
  }

  const encoded = dataBase64.trim();
  if (!encoded) {
    throw new Error("附件 data_base64 不能为空");
  }

  // Buffer.from 会容忍非法 Base64 并静默截断，必须在桥接边界明确拒绝损坏数据。
  const base64Pattern = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
  if (!base64Pattern.test(encoded)) {
    throw new Error("附件 data_base64 不是有效 Base64");
  }

  const buffer = Buffer.from(encoded, "base64");
  if (buffer.length === 0) {
    throw new Error("附件解码后为空");
  }
  if (buffer.length > MAX_ATTACHMENT_BYTES) {
    throw new Error(
      `附件超过单附件大小限制: ${(buffer.length / 1024 / 1024).toFixed(2)} MB > ${Math.round(MAX_ATTACHMENT_BYTES / 1024 / 1024)} MB`,
    );
  }
  return buffer;
}

async function collectInboundContent(
  content: any,
  senderId: string,
  textParts: string[],
  attachments: Record<string, unknown>[],
  budget: { usedBytes: number },
): Promise<boolean> {
  if (!content || typeof content.type !== "string") {
    return false;
  }

  switch (content.type) {
    case "text":
      if (typeof content.text === "string" && content.text.length > 0) {
        textParts.push(content.text);
        return true;
      }
      return false;

    case "attachment": {
      const mime = String(content.mimeType ?? "");
      const cname = String(content.name ?? "");

      // Photon 当前仍可能把原生 iMessage 语音作为 octet-stream + .caf 暴露。
      if (mime.startsWith("audio/") || cname.toLowerCase().endsWith(".caf")) {
        console.log(
          "[sidecar] 收到音频附件，当前 MaiBot 适配层不处理，已跳过该部分（发送者: %s）",
          senderId,
        );
        return false;
      }

      if (
        mime === "image/heic" ||
        mime === "image/heif" ||
        cname.toLowerCase().endsWith(".heic") ||
        cname.toLowerCase().endsWith(".heif")
      ) {
        console.log(
          "[sidecar] 收到 HEIC/HEIF 图片，当前 MaiBot 适配层不处理，已跳过该部分（发送者: %s）",
          senderId,
        );
        return false;
      }

      try {
        const buf: Buffer = await content.read();
        if (buf.length > MAX_ATTACHMENT_BYTES) {
          console.warn(
            `[sidecar] 附件超过大小限制，已跳过: ${(buf.length / 1024 / 1024).toFixed(2)} MB > ${Math.round(MAX_ATTACHMENT_BYTES / 1024 / 1024)} MB`,
          );
          return false;
        }

        if (budget.usedBytes + buf.length > MAX_MESSAGE_BYTES) {
          console.warn(
            "[sidecar] 跳过附件：单条消息附件总量将超过 %d MB",
            Math.round(MAX_MESSAGE_BYTES / 1024 / 1024),
          );
          return false;
        }
        budget.usedBytes += buf.length;

        const att: Record<string, unknown> = {
          type: mime.startsWith("image/") ? "image" : "file",
          mime_type: mime || "application/octet-stream",
          data_base64: buf.toString("base64"),
        };
        if (cname) att.name = cname;
        if (content.size != null) att.size = content.size;
        attachments.push(att);
        return true;
      } catch (err) {
        console.error("[sidecar] 读取附件内容失败:", err);
        return false;
      }
    }

    case "group": {
      let accepted = false;
      for (const item of content.items ?? []) {
        accepted =
          (await collectInboundContent(
            item?.content ?? item,
            senderId,
            textParts,
            attachments,
            budget,
          )) || accepted;
      }
      return accepted;
    }

    case "reply":
      // 保留回复正文；target 由 MaiBot 当前 MessageDict 无法完整表达，暂不伪造引用关系。
      return await collectInboundContent(
        content.content,
        senderId,
        textParts,
        attachments,
        budget,
      );

    case "richlink":
      if (typeof content.url === "string" && content.url.length > 0) {
        textParts.push(content.url);
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
      textParts.push(details ? `[联系人] ${details}` : "[联系人名片]");
      return true;
    }

    case "voice":
      console.log(
        "[sidecar] 收到 voice 内容，当前 MaiBot 适配层不处理，已跳过该部分（发送者: %s）",
        senderId,
      );
      return false;

    // reaction / poll_option / typing / read 等事件不应被当成一次普通用户发言触发 LLM。
    default:
      console.log("[sidecar] 忽略不作为普通聊天注入的内容类型: %s", content.type);
      return false;
  }
}

// 启动消息消费循环（异步，不阻塞事件循环）
(async () => {
  try {
    for await (const [space, message] of app.messages) {
      if (message.direction !== "inbound") {
        console.log("[sidecar] 忽略自身 outbound 回声: message_id=%s", message.id);
        continue;
      }

      try {
        await message.read();
        console.log("[sidecar] 已发送 iMessage 已读回执: message_id=%s", message.id);
      } catch (error) {
        console.warn("[sidecar] 发送 iMessage 已读回执失败: %s", String(error));
      }

      currentSpace = space;
      rememberSpace(space);

      // Spectrum 会把同一条 iMessage 的文字 + 多附件封装为 group，
      // reply 也可能再包装一层正文，因此递归展开可被 MaiBot 表达的内容。
      const textParts: string[] = [];
      const attachments: Record<string, unknown>[] = [];
      const budget = { usedBytes: 0 };
      const accepted = await collectInboundContent(
        message.content,
        message.sender?.id ?? "未知",
        textParts,
        attachments,
        budget,
      );
      if (!accepted) {
        continue;
      }

      if (INBOUND_REACTION_EMOJI) {
        try {
          const reactionMessage = await space.send(reaction(INBOUND_REACTION_EMOJI, message));
          if (reactionMessage) {
            console.log(
              "[sidecar] 已发送 iMessage 表情反应 %s: message_id=%s",
              INBOUND_REACTION_EMOJI,
              message.id,
            );
          } else {
            console.warn(
              "[sidecar] iMessage 平台未发送表情反应 %s: message_id=%s",
              INBOUND_REACTION_EMOJI,
              message.id,
            );
          }
        } catch (error) {
          console.warn(
            "[sidecar] 发送 iMessage 表情反应失败: message_id=%s error=%s",
            message.id,
            String(error),
          );
        }
      }
      const textContent = textParts.join("\n");

      let linePhone = "";
      try {
        linePhone = String((imessage(space as any) as any).phone ?? "");
      } catch {
        // 非 iMessage space 理论上不会出现在本侧车；保持空值即可。
      }

      pyWs.send(
        JSON.stringify({
          type: "message",
          data: {
            message_id: message.id,
            chat_id: space.id,
            line_phone: linePhone,
            sender: {
              name: message.sender?.id ?? "未知",
              address: message.sender?.id ?? "",
            },
            text: textContent,
            timestamp: message.timestamp?.getTime() ?? Date.now(),
            is_from_me: false,
            attachments,
          },
        }),
      );
    }
  } catch (err) {
    console.error("[sidecar] Photon 消息循环异常退出:", err);
    // 通知 Python 后退出，由 Python 端决定是否重启
    try {
      pyWs.send(
        JSON.stringify({
          type: "error",
          code: "PHOTON_DISCONNECTED",
          message: String(err),
          fatal: true,
        }),
      );
    } catch {
      // WebSocket 可能已断开
    }
    process.exit(1);
  }
})();

// ---------------------------------------------------------------------------
// 5. 接收 Python 指令
// ---------------------------------------------------------------------------

async function resolveSpace(
  targetId: string,
  linePhone = "",
): Promise<typeof currentSpace | null> {
  if (currentSpace && currentSpace.id === targetId) {
    return currentSpace;
  }
  if (spaceCache.has(targetId)) {
    const cachedSpace = spaceCache.get(targetId)!;
    rememberSpace(cachedSpace);
    return cachedSpace;
  }

  // Cold send. Dedicated 多线项目必须把原会话所属 phone 带回 space.get。
  console.log(
    "[sidecar] 冷发送到: chat_id=%s line=%s",
    targetId,
    linePhone || "auto",
  );
  const im = imessage(app);
  const space = linePhone && linePhone !== "shared"
    ? await im.space.get(targetId, { phone: linePhone })
    : await im.space.get(targetId);
  if (!space) {
    console.warn("[sidecar] im.space.get 返回 null，无法发送到 " + targetId);
    return null;
  }
  rememberSpace(space as typeof currentSpace);
  return space as typeof currentSpace;
}

pyWs.on("message", async (raw) => {
  let msg: any;
  try {
    msg = JSON.parse(raw.toString());
  } catch (err) {
    console.warn("[sidecar] 收到无效的 Python WebSocket JSON，已忽略:", err);
    return;
  }

  if (msg.type === "send") {
    const requestId = typeof msg.request_id === "string" ? msg.request_id : "";
    const sendResult = (
      success: boolean,
      error = "",
      externalMessageId = "",
    ): void => {
      pyWs.send(JSON.stringify({
        type: "send_result",
        request_id: requestId,
        success,
        error,
        external_message_id: externalMessageId,
      }));
    };

    try {
      let targetId = String(msg.data?.chat_id ?? "").trim();
      if (!targetId) {
        sendResult(false, "缺少目标 chat_id");
        return;
      }
      // 兼容旧格式：裸号码自动补 DM 前缀
      if (!targetId.startsWith("any;-;") && !targetId.startsWith("any;+;")) {
        targetId = `any;-;${targetId}`;
      }

      const linePhone = String(msg.data?.line_phone ?? "").trim();
      const space = await resolveSpace(targetId, linePhone);
      if (!space) {
        sendResult(false, "无法解析目标空间: " + targetId);
        return;
      }

      // Build content array for space.send()
      const contents: any[] = [];

      // Text (if any)
      const msgText = (msg.data.text ?? "").trim();
      if (msgText) {
        contents.push(text(msgText));
      }

      // Attachments (if any)
      const atts = msg.data.attachments ?? [];
      if (!Array.isArray(atts)) {
        sendResult(false, "attachments 必须是数组");
        return;
      }
      let attachmentBytesUsed = 0;
      for (const att of atts) {
        if (!att || typeof att !== "object") {
          sendResult(false, "附件必须是对象");
          return;
        }
        if (att.type === "voice") {
          console.log("[sidecar] 不支持发送语音消息，已跳过");
          continue;
        }
        const mimeType = typeof att.mime_type === "string" && att.mime_type
          ? att.mime_type
          : "application/octet-stream";
        const buf = decodeOutboundAttachment(att.data_base64);
        if (attachmentBytesUsed + buf.length > MAX_MESSAGE_BYTES) {
          sendResult(
            false,
            `单条消息附件总量超过限制: ${Math.round(MAX_MESSAGE_BYTES / 1024 / 1024)} MB`,
          );
          return;
        }
        attachmentBytesUsed += buf.length;
        const opts: Record<string, unknown> = { mimeType };
        if (typeof att.name === "string" && att.name) opts.name = att.name;
        contents.push(attachment(buf, opts as any));
      }

      if (contents.length === 0) {
        sendResult(false, "没有可发送的内容");
        return;
      }

      console.log(
        "[sidecar] 正在发送 iMessage：文字=%d，附件=%d",
        msgText.length,
        atts.length,
      );
      const sent = await (space as any).send(...contents);
      const sentItems = Array.isArray(sent) ? sent : [sent];
      const externalMessageId = sentItems
        .map((item: any) => typeof item?.id === "string" ? item.id : "")
        .find((id: string) => id.length > 0) ?? "";
      sendResult(true, "", externalMessageId);
    } catch (err) {
      console.error("[sidecar] 发送消息失败:", err);
      sendResult(false, String(err) || "Photon 发送失败");
    }
  } else if (msg.type === "shutdown") {
    console.log("[sidecar] 收到 Python shutdown 请求，正在关闭…");
    try {
      await app.stop();
    } catch {
      // SDK 关闭时可能抛异常，忽略
    }
    pyWs.close();
    process.exit(0);
  }
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
  try {
    await app.stop();
  } catch {
    // 忽略关闭错误
  }
  pyWs.close();
  process.exit(0);
});

process.on("SIGINT", async () => {
  console.log("[sidecar] 收到 SIGINT，正在关闭…");
  try {
    await app.stop();
  } catch {
    // 忽略关闭错误
  }
  pyWs.close();
  process.exit(0);
});
