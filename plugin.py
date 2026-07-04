"""iMessage 适配器插件

通过 Photon Spectrum 云端将 MaiBot 接入 iMessage。
采用侧车（Sidecar）模式：
Python 插件 ←─本地 WebSocket─→ Node.js 进程 ←─spectrum-ts SDK─→ Photon Cloud → Apple iMessage
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
from pathlib import Path
from typing import Any

from maibot_sdk import Command, MaiBotPlugin, MessageGateway
from maibot_sdk.types import MessageGatewayRouteType

from .config import IMessageAdapterConfig


# —————————————————————————————————————————————————————————————————————————————
# 插件主类
# —————————————————————————————————————————————————————————————————————————————


class IMessageAdapterPlugin(MaiBotPlugin):
    """iMessage 适配器插件"""

    config_model = IMessageAdapterConfig

    # ── 运行时状态 ──
    _sidecar_process: asyncio.subprocess.Process | None = None
    _ws_server: object | None = None  # websockets.Server
    _bridge_ws: object | None = None  # websockets.WebSocketServerProtocol
    _retry_count: int = 0
    _monitor_task: asyncio.Task | None = None
    _ws_connected: asyncio.Event | None = None
    _gateway_ready: bool = False
    _bridge_token: str = ""
    _recv_task: asyncio.Task | None = None

    # ── 生命周期 ──

    async def on_load(self) -> None:
        """插件加载：启动侧车 + WebSocket Server。"""

        self.ctx.logger.info("iMessage 适配器正在加载…")

        # 检查是否启用
        if not self.config.plugin.should_connect():
            self.ctx.logger.info("插件未启用，跳过加载")
            return

        # 生成一次性认证 token
        self._bridge_token = secrets.token_hex(32)
        ws_port = self.config.bridge.ws_port

        # 1. 启动 WebSocket Server
        self._ws_connected = asyncio.Event()
        self._ws_server = await self._start_ws_server(ws_port)
        self.ctx.logger.info("WebSocket Server 已启动: 127.0.0.1:%d", ws_port)

        # 2. 启动 Node.js 侧车
        sidecar_dir = Path(__file__).parent / "sidecar"
        await self._launch_sidecar(sidecar_dir, ws_port)

        # 3. 等待侧车连接 + 认证 + ready
        try:
            await asyncio.wait_for(self._ws_connected.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            self.ctx.logger.error("侧车未在 30 秒内连接，启动失败")
            await self._kill_sidecar()
            return

        self.ctx.logger.info("侧车已连接，等待 Photon 就绪…")

        # _gateway_ready 由 _recv_loop 收到 "ready" 消息后设置
        # 给侧车额外的 30s 连接 Photon
        for _ in range(30):
            if self._gateway_ready:
                break
            await asyncio.sleep(1)

        if self._gateway_ready:
            await self.ctx.gateway.update_state(
                "imessage",
                ready=True,
                platform="imessage",
                protocol="photon",
            )
            self.ctx.logger.info("iMessage 网关已就绪")
        else:
            self.ctx.logger.warning("Photon 未在 30s 内就绪，网关标记为未就绪")

        # 4. 启动进程监控
        self._monitor_task = asyncio.create_task(self._monitor_sidecar())
        self._retry_count = 0

    async def on_unload(self) -> None:
        """插件卸载：关闭侧车 + WebSocket Server。"""

        self.ctx.logger.info("iMessage 适配器正在卸载…")

        # 未启用时无需清理
        if not self.config.plugin.should_connect():
            return

        # 1. 取消监控和接收任务
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            self._monitor_task = None
        if self._recv_task is not None:
            self._recv_task.cancel()
            self._recv_task = None

        # 2. 通知侧车关闭
        if self._bridge_ws is not None:
            try:
                await self._bridge_ws.send(json.dumps({"type": "shutdown"}))
            except Exception:
                pass

        # 3. 等待进程退出
        if self._sidecar_process is not None:
            try:
                await asyncio.wait_for(self._sidecar_process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self.ctx.logger.warning("侧车未在 5s 内退出，强制终止")
                await self._kill_sidecar()

        # 4. 关闭 WebSocket Server
        if self._ws_server is not None:
            self._ws_server.close()
            await self._ws_server.wait_closed()

        # 5. 上报网关离线
        await self.ctx.gateway.update_state("imessage", ready=False)
        self._gateway_ready = False
        self.ctx.logger.info("iMessage 适配器已卸载")

    async def on_config_update(self, scope: str, config_data: dict, version: str) -> None:
        """配置热重载回调。"""

        if scope == "self":
            self.ctx.logger.info("iMessage 适配器配置已更新: version=%s", version)
            # 如果 Photon 凭证变更，需要重启侧车
            # 第一版简化处理：仅记录日志，用户需手动重连

    # ── WebSocket Server ──

    async def _start_ws_server(self, port: int) -> object:
        """启动本地 WebSocket Server，仅接受一个客户端。"""

        import websockets

        async def ws_handler(websocket):
            # 只允许一个客户端
            if self._bridge_ws is not None:
                self.ctx.logger.warning("已有侧车连接，拒绝新连接")
                await websocket.close(4001, "已有侧车连接")
                return

            # 认证
            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                msg = json.loads(raw)
            except asyncio.TimeoutError:
                self.ctx.logger.warning("侧车认证超时")
                await websocket.close(4002, "认证超时")
                return
            except Exception:
                self.ctx.logger.warning("侧车发送了无效的认证消息")
                await websocket.close(4000, "无效消息")
                return

            if not isinstance(msg, dict) or msg.get("type") != "auth":
                self.ctx.logger.warning("侧车认证消息格式错误")
                await websocket.close(4000, "格式错误")
                return

            if not secrets.compare_digest(str(msg.get("token", "")), self._bridge_token):
                self.ctx.logger.warning("侧车认证 token 不匹配")
                await websocket.close(4401, "token 不匹配")
                return

            await websocket.send(json.dumps({"type": "auth_ok"}))
            self._bridge_ws = websocket
            self._ws_connected.set()
            self.ctx.logger.info("侧车认证通过")

            # 启动消息接收循环
            self._recv_task = asyncio.create_task(self._recv_loop(websocket))

        server = await websockets.serve(
            ws_handler,
            "127.0.0.1",
            port,
        )
        return server

    async def _recv_loop(self, websocket) -> None:
        """接收侧车消息的循环。"""

        import websockets

        try:
            async for raw in websocket:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    self.ctx.logger.warning("侧车发送了无效 JSON")
                    continue

                tp = msg.get("type", "")
                if tp == "ready":
                    self._gateway_ready = True
                    await self.ctx.gateway.update_state(
                        "imessage",
                        ready=True,
                        platform="imessage",
                        protocol="photon",
                    )
                    self.ctx.logger.info("Photon 已就绪")

                elif tp == "message":
                    data = msg.get("data", {})
                    mai_msg = self._to_mai_message_dict(data)
                    try:
                        accepted = await self.ctx.gateway.route_message(
                            gateway_name="imessage",
                            message=mai_msg,
                            external_message_id=str(data.get("message_id", "")),
                        )
                        if not accepted:
                            self.ctx.logger.debug("Host 未接收入站消息: %s", data.get("message_id"))
                    except Exception as exc:
                        self.ctx.logger.error("注入入站消息失败: %s", exc)

                elif tp == "error":
                    code = msg.get("code", "UNKNOWN")
                    message = msg.get("message", "")
                    fatal = msg.get("fatal", False)
                    if fatal:
                        self.ctx.logger.error("侧车致命错误 [%s]: %s", code, message)
                    else:
                        self.ctx.logger.warning("侧车错误 [%s]: %s", code, message)

                elif tp == "status":
                    self.ctx.logger.info(
                        "侧车状态: %s — %s",
                        msg.get("connection", "unknown"),
                        msg.get("details", ""),
                    )
                else:
                    self.ctx.logger.debug("侧车未知消息类型: %s", tp)

        except websockets.exceptions.ConnectionClosed:
            self.ctx.logger.info("侧车 WebSocket 连接已关闭")
        finally:
            self._bridge_ws = None
            self._gateway_ready = False

    # ── 进程管理 ──

    async def _launch_sidecar(self, sidecar_dir: Path, ws_port: int) -> None:
        """启动 Node.js 侧车子进程。"""

        env = {
            **os.environ,
            "BRIDGE_WS_PORT": str(ws_port),
            "BRIDGE_WS_TOKEN": self._bridge_token,
            "PHOTON_PROJECT_ID": self.config.photon.project_id,
            "PHOTON_PROJECT_SECRET": self.config.photon.project_secret,
        }

        self._sidecar_process = await asyncio.create_subprocess_exec(
            "node",
            "dist/index.js",
            cwd=str(sidecar_dir),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        self.ctx.logger.info("侧车进程已启动: PID=%d", self._sidecar_process.pid)

        # 启动 stdout/stderr 读取任务（非阻塞）
        if self._sidecar_process.stdout is not None:
            asyncio.create_task(self._read_sidecar_stdout())
        if self._sidecar_process.stderr is not None:
            asyncio.create_task(self._read_sidecar_stderr())

    async def _read_sidecar_stdout(self) -> None:
        """读取侧车 stdout 并转发到日志。"""

        if self._sidecar_process is None or self._sidecar_process.stdout is None:
            return
        try:
            while True:
                line = await self._sidecar_process.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                if text:
                    self.ctx.logger.info("[侧车] %s", text)
        except Exception:
            pass

    async def _read_sidecar_stderr(self) -> None:
        """读取侧车 stderr 并转发到日志。"""

        if self._sidecar_process is None or self._sidecar_process.stderr is None:
            return
        try:
            while True:
                line = await self._sidecar_process.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                if text:
                    self.ctx.logger.warning("[侧车] %s", text)
        except Exception:
            pass

    async def _restart_sidecar(self) -> None:
        """重启侧车进程。"""

        self.ctx.logger.info("正在重启侧车（第 %d/%d 次）…",
                             self._retry_count + 1,
                             self.config.bridge.max_retries)

        # 清理旧连接
        await self._kill_sidecar()
        self._bridge_ws = None
        self._gateway_ready = False
        self._ws_connected = asyncio.Event()

        # 重新生成 token
        self._bridge_token = secrets.token_hex(32)

        sidecar_dir = Path(__file__).parent / "sidecar"
        ws_port = self.config.bridge.ws_port

        await self.ctx.gateway.update_state("imessage", ready=False)
        await self._launch_sidecar(sidecar_dir, ws_port)

        try:
            await asyncio.wait_for(self._ws_connected.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            self.ctx.logger.error("侧车重连超时")
            return

        self.ctx.logger.info("侧车已重新连接")

    async def _kill_sidecar(self) -> None:
        """强制终止侧车进程。"""

        if self._sidecar_process is None:
            return
        try:
            self._sidecar_process.kill()
            await self._sidecar_process.wait()
        except Exception:
            pass
        self._sidecar_process = None

    async def _monitor_sidecar(self) -> None:
        """监控侧车进程，崩溃时自动重启。"""

        while self._sidecar_process is not None:
            exit_code = await self._sidecar_process.wait()
            self.ctx.logger.info("侧车进程退出: exit_code=%d", exit_code or 0)

            self._sidecar_process = None
            self._bridge_ws = None
            self._gateway_ready = False

            if exit_code == 0:
                # 正常退出（用户发 /imessage_reconnect 或 on_unload）
                return
            if exit_code == 2:
                # 认证失败，不重启
                self.ctx.logger.error("Photon 认证失败，请检查 project_id/project_secret")
                await self.ctx.gateway.update_state("imessage", ready=False)
                return

            if self._retry_count >= self.config.bridge.max_retries:
                self.ctx.logger.error("侧车已崩溃超过最大重试次数 (%d)", self.config.bridge.max_retries)
                await self.ctx.gateway.update_state("imessage", ready=False)
                return

            self._retry_count += 1
            await asyncio.sleep(self.config.bridge.retry_interval)
            await self._restart_sidecar()

    # ── 消息格式转换 ──

    @staticmethod
    def _to_mai_message_dict(data: dict) -> dict:
        """将侧车的简化 JSON 转换为 MaiBot 标准消息字典。

        chat_id 直接作为 MaiBot 的 session_id，
        保证同一个 iMessage 对话始终映射到同一个会话。

        raw_message 必须是一个 list（MessageSequence 的反序列化格式），
        字段名 "data" 对齐 SDK 的 _component_from_dict 解析规则。
        """
        sender = data.get("sender", {})
        chat_id = str(data.get("chat_id", ""))
        text = str(data.get("text", ""))

        return {
            "message_id": data.get("message_id", ""),
            "platform": "imessage",
            "session_id": chat_id,
            "message_info": {
                "user_info": {
                    "user_id": str(sender.get("address", "unknown")),
                    "user_nickname": str(sender.get("name", "unknown")),
                },
                "additional_config": {},
            },
            "raw_message": [{"type": "text", "data": text}],
        }

    # ── @MessageGateway 组件 ──

    @MessageGateway(
        route_type="duplex",
        name="imessage",
        platform="imessage",
        protocol="photon",
        description="iMessage 消息收发网关（通过 Photon Spectrum 云端）",
    )
    async def send_to_imessage(
        self,
        message: dict[str, Any],
        route: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """出站：将 Host 消息通过侧车发送到 iMessage。"""

        del route, metadata, kwargs

        # Host 传出的消息字典来自 SessionMessage._session_message_to_dict()
        # - raw_message 是 MessageSequence 序列化后的嵌套 dict，不是纯文本
        # - processed_plain_text 才是 LLM 处理后待发送的纯文本
        # - session_id 是路由目标（对应 iMessage 的 chat_id）
        raw_text = str(message.get("processed_plain_text", "") or "")
        session_id = str(message.get("session_id", "") or "")

        if not raw_text or not session_id:
            return {"success": False, "error": "缺少消息内容或目标"}

        if self._bridge_ws is None or not self._gateway_ready:
            return {"success": False, "error": "iMessage 网关未就绪"}

        try:
            payload = {
                "type": "send",
                "data": {
                    "chat_id": session_id,
                    "text": raw_text,
                    "attachments": [],
                },
            }
            await self._bridge_ws.send(json.dumps(payload, ensure_ascii=False))
            return {"success": True}
        except Exception as exc:
            self.ctx.logger.error("发送消息到侧车失败: %s", exc)
            return {"success": False, "error": str(exc)}

    # ── 管理命令 ──

    @Command(
        "imessage_status",
        description="查看 iMessage 适配器运行状态",
        pattern=r"^/imessage_status$",
    )
    async def handle_status(self, stream_id: str = "", **kwargs: Any) -> tuple:
        """返回侧车进程状态、Photon 连接状态。"""

        del kwargs

        pid = "—"
        if self._sidecar_process is not None:
            pid = str(self._sidecar_process.pid)

        status_lines = [
            "📱 iMessage 适配器状态",
            f"侧车 PID: {pid}",
            f"Photon 连接: {'✅ 已就绪' if self._gateway_ready else '❌ 未连接'}",
            f"重启次数: {self._retry_count}/{self.config.bridge.max_retries}",
            f"桥接端口: {self.config.bridge.ws_port}",
            f"Photon 项目: {self.config.photon.project_id or '未配置'}",
        ]
        await self.ctx.send.text("\n".join(status_lines), stream_id)
        return True, "状态已显示", True

    @Command(
        "imessage_reconnect",
        description="手动重连 iMessage 适配器",
        pattern=r"^/imessage_reconnect$",
    )
    async def handle_reconnect(self, stream_id: str = "", **kwargs: Any) -> tuple:
        """手动触发侧车重启。"""

        del kwargs

        self._retry_count = 0
        await self._restart_sidecar()
        await self.ctx.send.text("🔄 iMessage 适配器已触发重连", stream_id)
        return True, "已重连", True


# ---------------------------------------------------------------------------
# 工厂函数
# ---------------------------------------------------------------------------


def create_plugin() -> IMessageAdapterPlugin:
    """创建 iMessage 适配器插件实例。"""

    return IMessageAdapterPlugin()
