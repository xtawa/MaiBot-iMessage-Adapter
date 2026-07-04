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

import { Spectrum } from "spectrum-ts";
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

// 启动消息消费循环（异步，不阻塞事件循环）
(async () => {
  try {
    for await (const [space, message] of app.messages) {
      currentSpace = space;

      if (message.content.type === "text") {
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
              text: message.content.text,
              timestamp: message.timestamp?.getTime() ?? Date.now(),
              is_from_me: message.direction === "outbound",
              attachments: [],
            },
          }),
        );
      }
      // 后续版本可扩展: attachment / reaction / group 等 content.type
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

pyWs.on("message", async (raw) => {
  const msg = JSON.parse(raw.toString());

  if (msg.type === "send") {
    // 调 SDK 的发送 API：发送纯文本
    try {
      // 需要根据 chat_id 找到对应的 space
      // 注意: spectrum-ts 的 space.send() 是发送到当前循环中的 space
      // 对于出站场景，需要通过 app 查找或使用当前已知的 space
      // 第一版简化处理：如果 chat_id 匹配当前 space，直接发送
      if (currentSpace && currentSpace.id === msg.data.chat_id) {
        const { text } = await import("spectrum-ts");
        await currentSpace.send(text(msg.data.text));
      } else {
        // 跨 space 发送 — spectrum-ts 支持通过 app 获取 space
        // 第一版暂不支持；记录错误
        console.warn("[sidecar] 跨 space 发送暂不支持: chat_id=", msg.data.chat_id);
      }
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
