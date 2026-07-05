"""iMessage 适配器插件。

将 MaiBot 接入 Apple iMessage，实现消息双向收发。
采用侧车（Sidecar）模式：
Python 插件 ←─本地 WebSocket─→ Node.js 进程 ←─spectrum-ts SDK─→ Photon Cloud → Apple iMessage
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from maibot_sdk import Command, MaiBotPlugin, MessageGateway
from maibot_sdk.types import MessageGatewayRouteType

from .config import IMessageAdapterConfig

if TYPE_CHECKING:
    from maibot_sdk import PluginConfigBase


# ── 模块常量 ──

# nodeenv 安装路径（插件目录下的 .nodeenv/）
_NODEENV_DIR = Path(__file__).parent / ".nodeenv"
# Windows: Scripts, Linux/macOS: bin
_NODEENV_BIN = "Scripts" if os.name == "nt" else "bin"


class IMessageAdapterPlugin(MaiBotPlugin):
    """iMessage 消息网关适配器插件。"""

    config_model: ClassVar[type[PluginConfigBase] | None] = IMessageAdapterConfig

    # ── 运行时状态 ──
    _sidecar_process: asyncio.subprocess.Process | None = None
    _ws_server: object | None = None  # websockets.Server
    _bridge_ws: object | None = None  # websockets.WebSocketServerProtocol
    _retry_count: int = 0
    _monitor_task: asyncio.Task | None = None
    _ws_connected: asyncio.Event | None = None
    _gateway_ready: bool = False
    _bridge_token: str = ""
    _shutting_down: bool = False

    # ── 生命周期 ──

    async def on_load(self) -> None:
        """在插件加载时根据配置决定是否启动连接。"""

        await self._restart_connection_if_needed()

    async def on_unload(self) -> None:
        """在插件卸载时关闭连接。"""

        self._shutting_down = True
        await self._stop_connection()

        # 关闭 WebSocket Server
        if self._ws_server is not None:
            self._ws_server.close()
            await self._ws_server.wait_closed()
            self._ws_server = None
        self._bridge_ws = None
        self._bridge_token = ""

        self.ctx.logger.info("iMessage 适配器已卸载")

    async def on_config_update(self, scope: str, config_data: dict, version: str) -> None:
        """在配置更新后重载连接状态。

        Args:
            scope: 配置变更范围。
            config_data: 最新的配置数据。
            version: 配置版本号。
        """

        if scope != "self":
            return

        self.set_plugin_config(config_data)
        if version:
            self.ctx.logger.debug("iMessage 适配器收到配置更新通知: %s", version)
        await self._restart_connection_if_needed()

    # ── 连接管理 ──

    async def _restart_connection_if_needed(self) -> None:
        """根据当前配置重启连接。"""
        await self._stop_connection()

        if not self.config.plugin.should_connect():
            self.ctx.logger.info("iMessage 适配器保持空闲状态，因为插件未启用")
            return

        self._shutting_down = False

        # 生成一次性认证 token
        self._bridge_token = secrets.token_hex(32)
        ws_port = self.config.bridge.ws_port

        # 启动 WebSocket Server
        self._ws_connected = asyncio.Event()
        self._ws_server = await self._start_ws_server(ws_port)
        self.ctx.logger.info("WebSocket Server 已启动: 127.0.0.1:%d", ws_port)

        # 启动 Node.js 侧车
        sidecar_dir = Path(__file__).parent / "sidecar"
        await self._launch_sidecar(sidecar_dir, ws_port)

        # 等待侧车连接 + 认证
        try:
            await asyncio.wait_for(self._ws_connected.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            self.ctx.logger.error("侧车未在 30 秒内连接，启动失败")
            await self._kill_sidecar()
            return

        self.ctx.logger.info("侧车已连接，等待 Photon 就绪…")

        # 等待 Photon ready（最多 30s）
        for _ in range(30):
            if self._gateway_ready:
                break
            await asyncio.sleep(1)

        if self._gateway_ready:
            await self.ctx.gateway.update_state(
                "imessage",
                ready=True,
                platform="imessage",
                metadata={"protocol": "photon"},
            )
            self.ctx.logger.info("iMessage 网关已就绪")

            # 调试发送
            try:
                from .debug import send_debug_message

                await send_debug_message(
                    bridge_ws=self._bridge_ws,
                    gateway_ready=self._gateway_ready,
                )
            except Exception as exc:
                self.ctx.logger.warning("[debug] 调试发送异常: %s", exc)
        else:
            self.ctx.logger.warning("Photon 未在 30s 内就绪，网关标记为未就绪")

        # 启动进程监控
        self._monitor_task = asyncio.create_task(self._monitor_sidecar())
        self._retry_count = 0

    async def _stop_connection(self) -> None:
        """停止当前连接并清理资源。"""
        # 先标记关闭中，阻止 _monitor_sidecar 再次重启
        self._shutting_down = True

        # 取消监控任务
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            self._monitor_task = None

        # 通知侧车关闭
        if self._bridge_ws is not None:
            try:
                await self._bridge_ws.send(json.dumps({"type": "shutdown"}))
            except Exception:
                pass
            # 关闭 websocket 连接，让 _recv_loop 中的 async for 自然退出
            try:
                await self._bridge_ws.close()
            except Exception:
                pass

        # 等待进程退出
        if self._sidecar_process is not None:
            try:
                await asyncio.wait_for(self._sidecar_process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self.ctx.logger.warning("侧车未在 5s 内退出，强制终止")
                await self._kill_sidecar()

        # 上报网关离线
        if self._gateway_ready:
            await self.ctx.gateway.update_state("imessage", ready=False)
            self._gateway_ready = False

        self._bridge_ws = None

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

            # 直接 await 接收循环，保持 ws_handler 存活
            # （create_task 会让 handler 立刻返回 → websockets 框架关闭连接）
            await self._recv_loop(websocket)

        server = await websockets.serve(ws_handler, "127.0.0.1", port)
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
                        metadata={"protocol": "photon"},
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
        except Exception as exc:
            self.ctx.logger.error("_recv_loop 异常退出: %s", exc, exc_info=True)
        finally:
            self._bridge_ws = None
            self._gateway_ready = False

    # ── 进程管理 ──

    @staticmethod
    def _node_bin_dir() -> Path:
        """返回 .nodeenv 中 node/npm/npx 所在的 bin 目录。"""
        return _NODEENV_DIR / _NODEENV_BIN

    @staticmethod
    def _nodeenv_node() -> Path:
        """返回 .nodeenv 中的 node 可执行文件路径。"""
        ext = ".exe" if os.name == "nt" else ""
        return _NODEENV_DIR / _NODEENV_BIN / f"node{ext}"

    @staticmethod
    def _nodeenv_npm() -> Path:
        """返回 .nodeenv 中的 npm 可执行文件路径。"""
        ext = ".cmd" if os.name == "nt" else ""
        return _NODEENV_DIR / _NODEENV_BIN / f"npm{ext}"

    @staticmethod
    def _nodeenv_npx() -> Path:
        """返回 .nodeenv 中的 npx 可执行文件路径。"""
        ext = ".cmd" if os.name == "nt" else ""
        return _NODEENV_DIR / _NODEENV_BIN / f"npx{ext}"

    async def _resolve_node_binaries(self) -> tuple[str, str, str]:
        """解析 node、npm、npx 的可执行文件路径。

        查找顺序:
        1. 系统 PATH（零开销，最优先）
        2. 插件目录下的 .nodeenv/ 缓存
        3. 都不存在 → 在线程池中调用 _install_nodeenv() 安装

        Returns:
            (node_path, npm_path, npx_path) — 可执行文件路径字符串。
        """

        system_node = shutil.which("node")
        system_npm = shutil.which("npm")
        if system_node and system_npm:
            npm_dir = Path(system_npm).parent
            npx_name = "npx.cmd" if os.name == "nt" else "npx"
            system_npx = str(npm_dir / npx_name)
            return system_node, system_npm, system_npx

        cached_node = self._nodeenv_node()
        cached_npm = self._nodeenv_npm()
        cached_npx = self._nodeenv_npx()
        if cached_node.exists() and cached_npm.exists():
            return str(cached_node), str(cached_npm), str(cached_npx)

        self.ctx.logger.info("系统中未找到 Node.js，正在通过 nodeenv 安装到 %s…", _NODEENV_DIR)
        await asyncio.to_thread(self._install_nodeenv)
        self.ctx.logger.info("Node.js 安装完成")
        return str(cached_node), str(cached_npm), str(cached_npx)

    @staticmethod
    def _install_nodeenv() -> None:
        """同步安装 nodeenv 到插件目录（在线程池中执行）。"""
        import nodeenv

        nodeenv_dir = _NODEENV_DIR
        if nodeenv_dir.exists():
            return

        # nodeenv 的 src_base_url 默认 None，仅在 CLI main() 中初始化。
        # 直接调用 API 需手动设置，否则 get_last_lts_node_version()
        # 会因 "None/index.json" 而失败。
        if nodeenv.src_base_url is None:
            nodeenv.src_base_url = "https://nodejs.org/download/release"

        node_version = nodeenv.get_last_lts_node_version()
        args = nodeenv.make_parser().parse_args([
            "--prebuilt",
            "--node", node_version,
            "--clean-src",
            str(nodeenv_dir),
        ])
        nodeenv.create_environment(str(nodeenv_dir), args)

    async def _ensure_sidecar_built(self, sidecar_dir: Path) -> bool:
        """确保侧车已编译，必要时自动执行 npm install + npm run build。

        Returns:
            bool: 编译产物是否存在且可运行。
        """

        dist_file = sidecar_dir / "dist" / "index.js"
        if dist_file.exists():
            return True

        node_path, npm_path, npx_path = await self._resolve_node_binaries()

        # 将 node 所在目录注入 PATH，这样 npm postinstall 脚本
        # 启动的子进程也能找到 node。
        node_bin_dir = str(Path(node_path).parent)
        env = os.environ.copy()
        env["PATH"] = node_bin_dir + os.pathsep + env.get("PATH", "")

        node_modules = sidecar_dir / "node_modules"
        # 用 node_modules/.package-lock.json（npm 安装成功的标记）来判断
        # 而非目录是否存在，避免上一次失败遗留的空壳导致跳过安装。
        npm_installed = (node_modules / ".package-lock.json").exists()
        if not npm_installed:
            self.ctx.logger.info("侧车依赖尚未安装，正在执行 npm install…")
            try:
                process = await asyncio.create_subprocess_exec(
                    npm_path,
                    "install",
                    cwd=str(sidecar_dir),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                )
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120.0)
                if process.returncode != 0:
                    out_text = (stdout or b"").decode("utf-8", errors="replace")
                    err_text = (stderr or b"").decode("utf-8", errors="replace")
                    combined = (out_text + "\n" + err_text).strip()
                    self.ctx.logger.error(
                        "npm install 失败: %s",
                        combined,
                    )
                    return False
            except asyncio.TimeoutError:
                self.ctx.logger.error("npm install 超时")
                return False
            except Exception as exc:
                self.ctx.logger.error("npm install 异常: %s", exc)
                return False

        self.ctx.logger.info("正在编译侧车 TypeScript…")
        try:
            process = await asyncio.create_subprocess_exec(
                npx_path,
                "tsc",
                cwd=str(sidecar_dir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60.0)
            if process.returncode != 0:
                # tsc 编译错误走 stdout，不是 stderr，两者都要输出
                out_text = (stdout or b"").decode("utf-8", errors="replace")
                err_text = (stderr or b"").decode("utf-8", errors="replace")
                combined = (out_text + "\n" + err_text).strip()
                self.ctx.logger.error("tsc 编译失败: %s", combined)
                return False
        except asyncio.TimeoutError:
            self.ctx.logger.error("tsc 编译超时")
            return False
        except Exception as exc:
            self.ctx.logger.error("tsc 编译异常: %s", exc)
            return False

        if not dist_file.exists():
            self.ctx.logger.error("tsc 编译完成后仍未找到 dist/index.js")
            return False

        self.ctx.logger.info("侧车编译完成")
        return True

    async def _launch_sidecar(self, sidecar_dir: Path, ws_port: int) -> None:
        """启动 Node.js 侧车子进程。"""

        if not await self._ensure_sidecar_built(sidecar_dir):
            self.ctx.logger.error("侧车编译失败，无法启动")
            return
        node_path, _, _ = await self._resolve_node_binaries()
        env = {
            **os.environ,
            "BRIDGE_WS_PORT": str(ws_port),
            "BRIDGE_WS_TOKEN": self._bridge_token,
            "PHOTON_PROJECT_ID": self.config.photon.project_id,
            "PHOTON_PROJECT_SECRET": self.config.photon.project_secret,
        }

        self._sidecar_process = await asyncio.create_subprocess_exec(
            node_path,
            "dist/index.js",
            cwd=str(sidecar_dir),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        self.ctx.logger.info("侧车进程已启动: PID=%d", self._sidecar_process.pid)

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
        self.ctx.logger.info(
            "正在重启侧车（第 %d/%d 次）…",
            self._retry_count + 1,
            self.config.bridge.max_retries,
        )

        await self._kill_sidecar()
        self._bridge_ws = None
        self._gateway_ready = False
        self._ws_connected = asyncio.Event()
        self._bridge_token = secrets.token_hex(32)

        sidecar_dir = Path(__file__).parent / "sidecar"
        await self.ctx.gateway.update_state("imessage", ready=False)
        await self._launch_sidecar(sidecar_dir, self.config.bridge.ws_port)

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

            if self._shutting_down:
                self.ctx.logger.info("插件正在关闭，侧车不再重启")
                return
            if exit_code == 0:
                return
            if exit_code == 2:
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
        """将侧车的简化 JSON 转换为 MaiBot 标准消息字典。"""
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
