/**
 * MaiBot iMessage Adapter — Sidecar
 *
 * 薄壳（Thin Shell）：不写任何 Photon 协议层代码。
 * 全部 Photon 功能（认证、Fusor WebSocket、心跳、重连、消息收发）
 * 由 spectrum-ts 官方 npm 包内置处理。
 *
 * 侧车只做三件事：
 *   1. 初始化 spectrum-ts SDK
 *   2. 连接 Python WebSocket Server，通过本地桥接转发消息
 *   3. 把 SDK 的 Message/Space 翻译成简化 JSON
 */

import { Spectrum, text, attachment, voice } from "spectrum-ts";
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

// ---------------------------------------------------------------------------
// 2. 初始化 spectrum-ts 官方 SDK
//    SDK 内部自动完成: JWT 认证、Fusor WS 连接、心跳、重连、token 刷新
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

const pyWs = new WebSocket(`ws://127.0.0.1:${PORT}`);

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

    pyWs.on("message", (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "auth_ok") {
        clearTimeout(timeout);
        resolve();
      } else {
        clearTimeout(timeout);
        reject(new Error(`收到非预期消息: ${msg.type}`));
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
//    app.messages 由 spectrum-ts 提供，是标准的 AsyncIterable
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

// 启动消息消费循环（异步，不阻塞事件循环）
(async () => {
  try {
    for await (const [space, message] of app.messages) {
      currentSpace = space;
      spaceCache.set(space.id, space);

      // Build the uniform message data object
      let textContent = "";
      const attachments: Record<string, unknown>[] = [];

      switch (message.content.type) {
        case "text":
          textContent = message.content.text;
          break;
        case "attachment":
        case "voice": {
          try {
            const buf: Buffer = await (message.content as any).read();
            const att: Record<string, unknown> = {
              type: message.content.type === "voice" ? "voice" : "image",
              mime_type: message.content.mimeType,
              data_base64: buf.toString("base64"),
            };
            if (message.content.name) att.name = message.content.name;
            if ((message.content as any).size != null) att.size = (message.content as any).size;
            if (message.content.type === "voice" && (message.content as any).duration != null) {
              att.duration = (message.content as any).duration;
            }
            attachments.push(att);
          } catch (err) {
            console.error("[sidecar] 读取附件内容失败:", err);
          }
          break;
        }
        default:
          console.log("[sidecar] 未处理的消息类型:", message.content.type);
          break;
      }

      pyWs.send(
        JSON.stringify({
          type: "message",
          data: {
            message_id: message.id,
            chat_id: space.id,
            sender: {
              name: message.sender?.id ?? "未知",
              address: message.sender?.id ?? "",
            },
            text: textContent,
            timestamp: message.timestamp?.getTime() ?? Date.now(),
            is_from_me: message.direction === "outbound",
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

async function resolveSpace(targetId: string): Promise<typeof currentSpace | null> {
  if (currentSpace && currentSpace.id === targetId) {
    return currentSpace;
  }
  if (spaceCache.has(targetId)) {
    return spaceCache.get(targetId)!;
  }
  // Cold send
  console.log("[sidecar] 冷发送到: chat_id=" + targetId);
  const im = imessage(app);
  const space = await im.space.get(targetId);
  if (!space) {
    console.warn("[sidecar] im.space.get 返回 null，无法发送到 " + targetId);
    return null;
  }
  spaceCache.set(space.id, space as typeof currentSpace);
  return space as typeof currentSpace;
}

pyWs.on("message", async (raw) => {
  const msg = JSON.parse(raw.toString());

  if (msg.type === "send") {
    try {
      let targetId = msg.data.chat_id.trim();
      // 兼容旧格式：裸号码自动补 DM 前缀
      if (!targetId.startsWith("any;-;") && !targetId.startsWith("any;+;")) {
        targetId = `any;-;${targetId}`;
      }

      const space = await resolveSpace(targetId);
      if (!space) {
        pyWs.send(
          JSON.stringify({
            type: "error",
            code: "SEND_FAILED",
            message: "无法解析目标空间: " + targetId,
            fatal: false,
          }),
        );
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
      const atts: any[] = msg.data.attachments ?? [];
      for (const att of atts) {
        const mimeType = att.mime_type || "application/octet-stream";
        const buf = Buffer.from(att.data_base64, "base64");

        if (att.type === "voice") {
          const opts: Record<string, unknown> = { mimeType };
          if (att.name) opts.name = att.name;
          if (att.duration != null) opts.duration = att.duration;
          contents.push(voice(buf, opts as any));
        } else {
          // Default to attachment for "image" and any other type
          const opts: Record<string, unknown> = { mimeType };
          if (att.name) opts.name = att.name;
          contents.push(attachment(buf, opts as any));
        }
      }

      if (contents.length === 0) {
        pyWs.send(
          JSON.stringify({
            type: "error",
            code: "SEND_FAILED",
            message: "没有可发送的内容",
            fatal: false,
          }),
        );
        return;
      }

      await (space as any).send(...contents);
    } catch (err) {
      console.error("[sidecar] 发送消息失败:", err);
      pyWs.send(
        JSON.stringify({
          type: "error",
          code: "SEND_FAILED",
          message: String(err),
          fatal: false,
        }),
      );
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
