"""
MaiBot iMessage Adapter — Main Program

采用侧车模式: Python 插件通过本地 WebSocket 与 Node.js 进程通信
Node.js 侧车通过 spectrum-ts SDK 连接 Photon Cloud 实现 iMessage 收发

Made BY Galeros

"""

# 更新日志: 增加侧车版本检测，防止本地编译文件不同步 26/7/9 19:14

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

from .config import IMessageAdapterConfig, PLUGIN_VERSION

if TYPE_CHECKING:
    from maibot_sdk import PluginConfigBase

"""nodeenv 安装时的目录变量"""
_NODEENV_DIR = Path(__file__).parent / ".nodeenv"
_NODEENV_BIN = "Scripts" if os.name == "nt" else "bin"

_MIN_NODE_VERSION = (20, 18, 1)
_WS_FRAME_OVERHEAD_BYTES = 256 * 1024
_SEND_ACK_TIMEOUT_SECONDS = 30.0


def _bridge_frame_limit_bytes(max_attachment_size_mb: int) -> int:
    """Return a WebSocket frame limit large enough for a base64 attachment."""
    attachment_bytes = max(1, max_attachment_size_mb) * 1024 * 1024
    base64_bytes = 4 * ((attachment_bytes + 2) // 3)
    return base64_bytes + _WS_FRAME_OVERHEAD_BYTES


def _base64_decoded_size(encoded: str) -> int:
    """Estimate decoded byte size without allocating another copy."""
    value = encoded.strip()
    if not value:
        return 0
    padding = 2 if value.endswith("==") else 1 if value.endswith("=") else 0
    return max(0, (len(value) * 3) // 4 - padding)


class IMessageAdapterPlugin(MaiBotPlugin):
    """iMessage 消息网关适配器插件。"""

    config_model: ClassVar[type[PluginConfigBase] | None] = IMessageAdapterConfig

    _sidecar_process: asyncio.subprocess.Process | None = None
    _ws_server: object | None = None
    _bridge_ws: object | None = None
    _retry_count: int = 0
    _monitor_task: asyncio.Task | None = None
    _ws_connected: asyncio.Event | None = None
    _gateway_ready: bool = False
    _bridge_token: str = ""
    _shutting_down: bool = False
    _pending_sends: dict[str, asyncio.Future] = {}

    """═══════════════════════════════════════════════
    生命周期
    ═══════════════════════════════════════════════"""

    async def on_load(self) -> None:
        self._pending_sends = {}
        await self._restart_connection_if_needed()

    async def on_unload(self) -> None:
        self._shutting_down = True
        await self._stop_connection()

        self._bridge_ws = None
        self._bridge_token = ""

        self.ctx.logger.info("iMessage 适配器已卸载")

    async def on_config_update(self, scope: str, config_data: dict, version: str) -> None:
        if scope != "self":
            return

        self.ctx.logger.warning("iMessage 适配器收到配置更新通知，正在应用新配置…")
        self.set_plugin_config(config_data)
        if version:
            self.ctx.logger.debug("iMessage 适配器收到配置更新通知: %s", version)
        await self._restart_connection_if_needed()

    """═══════════════════════════════════════════════
    连接管理：WebSocket Server + 侧车生命周期
    ═══════════════════════════════════════════════"""

    async def _restart_connection_if_needed(self) -> None:
        """根据当前配置启动完整的连接管线。"""
        await self._stop_connection()

        if not self.config.plugin.should_connect():
            self.ctx.logger.info("iMessage 适配器保持空闲状态，因为插件未启用")
            return

        self._shutting_down = False

        self._bridge_token = secrets.token_hex(32)
        ws_port = self.config.bridge.ws_port

        self._ws_connected = asyncio.Event()
        self._ws_server = await self._start_ws_server(ws_port)

        sidecar_dir = Path(__file__).parent / "sidecar"
        await self._launch_sidecar(sidecar_dir, ws_port)

        try:
            await asyncio.wait_for(self._ws_connected.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            self.ctx.logger.error("侧车未在 30 秒内连接，启动失败")
            await self._kill_sidecar()
            return

        self.ctx.logger.info("侧车已连接，等待 Photon 就绪…")

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

        self._monitor_task = asyncio.create_task(self._monitor_sidecar())
        self._retry_count = 0

    async def _stop_connection(self) -> None:
        """停止侧车进程并清理 WebSocket 连接。"""
        self._shutting_down = True

        if self._monitor_task is not None:
            self._monitor_task.cancel()
            self._monitor_task = None

        if self._bridge_ws is not None:
            try:
                await self._bridge_ws.send(json.dumps({"type": "shutdown"}))
            except Exception:
                pass
            try:
                await self._bridge_ws.close()
            except Exception:
                pass

        if self._sidecar_process is not None:
            try:
                await asyncio.wait_for(self._sidecar_process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                self.ctx.logger.warning("侧车未在 5s 内退出，强制终止")
                await self._kill_sidecar()

        if self._gateway_ready:
            await self.ctx.gateway.update_state("imessage", ready=False)
            self._gateway_ready = False

        if self._ws_server is not None:
            self._ws_server.close()
            await self._ws_server.wait_closed()
            self._ws_server = None

        self._bridge_ws = None
        self._fail_pending_sends("iMessage 连接已关闭")

    def _fail_pending_sends(self, reason: str) -> None:
        """让所有等待 Photon 回执的发送任务立即失败。"""
        pending = list(self._pending_sends.values())
        self._pending_sends.clear()
        for future in pending:
            if not future.done():
                future.set_result({
                    "success": False,
                    "error": reason,
                    "external_message_id": "",
                })

    """=══════════════════════════════════════════════
    WebSocket Server：接收侧车连接与消息
    ═══════════════════════════════════════════════"""

    async def _start_ws_server(self, port: int) -> object:
        """启动本地 WebSocket Server，接受侧车认证连接。"""
        import websockets

        async def ws_handler(websocket):
            if self._bridge_ws is not None:
                self.ctx.logger.warning("已有侧车连接，拒绝新连接")
                await websocket.close(4001, "已有侧车连接")
                return

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

            await self._recv_loop(websocket)

        max_frame_bytes = _bridge_frame_limit_bytes(self.config.bridge.max_attachment_size_mb)
        server = await websockets.serve(
            ws_handler, "127.0.0.1", port,
            max_size=max_frame_bytes,
        )
        self.ctx.logger.info(
            "WebSocket Server 已启动: 127.0.0.1:%d，最大附件大小: %d MB，帧上限: %.1f MB",
            port,
            self.config.bridge.max_attachment_size_mb,
            max_frame_bytes / 1024 / 1024,
        )
        return server

    async def _recv_loop(self, websocket) -> None:
        """接收侧车消息并分发处理：ready / message / error / status。"""
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
                    if bool(data.get("is_from_me", False)):
                        self.ctx.logger.debug("忽略自身 iMessage 回声: %s", data.get("message_id"))
                        continue

                    message_id = str(data.get("message_id", "") or "")
                    line_phone = str(data.get("line_phone", "") or "").strip()
                    mai_msg = self._to_mai_message_dict(data)
                    route_metadata = None
                    if line_phone and line_phone != "shared":
                        route_metadata = {"self_id": line_phone}
                    try:
                        accepted = await self.ctx.gateway.route_message(
                            gateway_name="imessage",
                            message=mai_msg,
                            route_metadata=route_metadata,
                            external_message_id=message_id,
                            dedupe_key=message_id,
                        )
                        if not accepted:
                            self.ctx.logger.debug("Host 未接收入站消息: %s", message_id)
                    except Exception as exc:
                        self.ctx.logger.error("注入入站消息失败: %s", exc)

                elif tp == "send_result":
                    request_id = str(msg.get("request_id", "") or "")
                    future = self._pending_sends.pop(request_id, None)
                    if future is None:
                        self.ctx.logger.debug("收到未知或已过期的发送回执: %s", request_id)
                    elif not future.done():
                        future.set_result({
                            "success": bool(msg.get("success", False)),
                            "error": str(msg.get("error", "") or ""),
                            "external_message_id": str(msg.get("external_message_id", "") or ""),
                        })

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
            self.ctx.logger.error("_recv_loop 异常退出: %s", exc)
        finally:
            was_ready = self._gateway_ready
            self._bridge_ws = None
            self._gateway_ready = False
            self._fail_pending_sends("iMessage 侧车连接已断开")
            if was_ready:
                try:
                    await self.ctx.gateway.update_state("imessage", ready=False)
                except Exception as exc:
                    self.ctx.logger.warning("同步 iMessage 网关离线状态失败: %s", exc)

    """╔══════════════════════════════════════════════
    Node.js 运行时解析 + 侧车进程管理
    ╚══════════════════════════════════════════════"""

    @staticmethod
    async def _detect_node_version(node_path: str) -> tuple[int, int, int] | None:
        """读取 Node.js 版本；无法读取时返回 None。"""
        try:
            process = await asyncio.create_subprocess_exec(
                node_path,
                "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=5.0)
            if process.returncode != 0:
                return None
            raw = (stdout or b"").decode("utf-8", errors="replace").strip().lstrip("v")
            parts = raw.split("-", 1)[0].split(".")
            if len(parts) < 3:
                return None
            return int(parts[0]), int(parts[1]), int(parts[2])
        except (asyncio.TimeoutError, OSError, ValueError):
            return None

    @staticmethod
    def _node_version_supported(version: tuple[int, int, int] | None) -> bool:
        """匹配当前锁定依赖的 Node.js engines：20.18.1+ 或 22+。"""
        if version is None:
            return False
        if version[0] == 20:
            return version >= _MIN_NODE_VERSION
        return version[0] >= 22

    @staticmethod
    def _node_bin_dir() -> Path:
        return _NODEENV_DIR / _NODEENV_BIN

    @staticmethod
    def _nodeenv_node() -> Path:
        ext = ".exe" if os.name == "nt" else ""
        return _NODEENV_DIR / _NODEENV_BIN / f"node{ext}"

    @staticmethod
    def _nodeenv_npm() -> Path:
        ext = ".cmd" if os.name == "nt" else ""
        return _NODEENV_DIR / _NODEENV_BIN / f"npm{ext}"

    @staticmethod
    def _nodeenv_npx() -> Path:
        ext = ".cmd" if os.name == "nt" else ""
        return _NODEENV_DIR / _NODEENV_BIN / f"npx{ext}"

    async def _resolve_node_binaries(self) -> tuple[str, str, str]:
        """查找 node 可执行文件路径。

        优先级: 系统 PATH → .nodeenv 缓存 → nodeenv 自动安装。
        """
        system_node = shutil.which("node")
        system_npm = shutil.which("npm")
        system_npx = shutil.which("npx")
        if system_node and system_npm and system_npx:
            version = await self._detect_node_version(system_node)
            if self._node_version_supported(version):
                return system_node, system_npm, system_npx
            version_text = ".".join(map(str, version)) if version is not None else "未知"
            self.ctx.logger.warning(
                "系统 Node.js 版本不可用（检测到 %s，要求 20.18.1+ 或 22+），将改用隔离 nodeenv",
                version_text,
            )

        cached_node = self._nodeenv_node()
        cached_npm = self._nodeenv_npm()
        cached_npx = self._nodeenv_npx()
        if cached_node.exists() and cached_npm.exists() and cached_npx.exists():
            version = await self._detect_node_version(str(cached_node))
            if self._node_version_supported(version):
                return str(cached_node), str(cached_npm), str(cached_npx)
            version_text = ".".join(map(str, version)) if version is not None else "未知"
            self.ctx.logger.warning(
                "缓存的 Node.js 版本不可用（检测到 %s，要求 20.18.1+ 或 22+），将重新安装",
                version_text,
            )

        if _NODEENV_DIR.exists():
            await asyncio.to_thread(shutil.rmtree, _NODEENV_DIR, ignore_errors=True)

        self.ctx.logger.warning("=" * 60)
        self.ctx.logger.warning("⚠ 系统中未找到 Node.js，即将自动下载安装")
        self.ctx.logger.warning("   下载源: https://nodejs.org/download/release")
        self.ctx.logger.warning("   安装位置: %s", _NODEENV_DIR)
        self.ctx.logger.warning("   此过程需要联网，可能需要 1-2 分钟，请耐心等待…")
        self.ctx.logger.warning("=" * 60)
        self.ctx.logger.info("系统中未找到 Node.js，正在通过 nodeenv 安装到 %s…", _NODEENV_DIR)
        await asyncio.to_thread(self._install_nodeenv)
        self.ctx.logger.info("Node.js 安装完成")
        return str(cached_node), str(cached_npm), str(cached_npx)

    @staticmethod
    def _install_nodeenv() -> None:
        """在线程池中执行 nodeenv 安装，下载预编译 Node.js LTS 到插件目录。"""
        import nodeenv

        nodeenv_dir = _NODEENV_DIR
        if nodeenv_dir.exists():
            return

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

    """╔══════════════════════════════════════════════
    侧车构建与启动
    ╚══════════════════════════════════════════════"""

    async def _ensure_sidecar_built(self, sidecar_dir: Path) -> bool:
        """确保侧车编译产物与当前插件版本一致，必要时重新构建。"""
        dist_dir = sidecar_dir / "dist"
        dist_file = dist_dir / "index.js"
        version_file = dist_dir / ".adapter-version"

        built_version = ""
        if version_file.exists():
            try:
                built_version = version_file.read_text(encoding="utf-8").strip()
            except OSError:
                built_version = ""

        if dist_file.exists() and built_version == PLUGIN_VERSION:
            return True

        if dist_dir.exists():
            self.ctx.logger.warning(
                "侧车编译产物需要刷新（built=%s current=%s），正在清理 dist",
                built_version or "unknown",
                PLUGIN_VERSION,
            )
            await asyncio.to_thread(shutil.rmtree, dist_dir, ignore_errors=True)

        node_path, npm_path, npx_path = await self._resolve_node_binaries()

        node_bin_dir = str(Path(node_path).parent)
        env = os.environ.copy()
        env["PATH"] = node_bin_dir + os.pathsep + env.get("PATH", "")

        node_modules = sidecar_dir / "node_modules"
        npm_installed = (node_modules / ".package-lock.json").exists()
        if not npm_installed:
            npm_command = "ci" if (sidecar_dir / "package-lock.json").exists() else "install"
            self.ctx.logger.info("侧车依赖尚未安装，正在执行 npm %s…", npm_command)
            try:
                process = await asyncio.create_subprocess_exec(
                    npm_path,
                    npm_command,
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
                    self.ctx.logger.error("npm %s 失败: %s", npm_command, combined)
                    return False
            except asyncio.TimeoutError:
                self.ctx.logger.error("npm %s 超时", npm_command)
                return False
            except Exception as exc:
                self.ctx.logger.error("npm %s 异常: %s", npm_command, exc)
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

        try:
            version_file.write_text(PLUGIN_VERSION + "\n", encoding="utf-8")
        except OSError as exc:
            self.ctx.logger.warning("无法写入侧车构建版本标记 %s: %s", version_file, exc)

        self.ctx.logger.info("侧车编译完成: adapter=%s", PLUGIN_VERSION)
        return True

    async def _launch_sidecar(self, sidecar_dir: Path, ws_port: int) -> None:
        """启动 Node.js 侧车子进程，并开始读取其 stdout/stderr。"""
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
            "MAX_ATTACHMENT_SIZE_MB": str(self.config.bridge.max_attachment_size_mb),
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
        """将侧车 stdout 逐行转发到框架日志。"""
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
        """将侧车 stderr 逐行转发到框架日志。"""
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

    """╔══════════════════════════════════════════════
    侧车容灾：崩溃重启 + 进程监控
    ╚══════════════════════════════════════════════"""

    async def _restart_sidecar(self) -> None:
        """终止并重新启动侧车进程。"""
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
        """监控侧车进程退出状态，按策略自动重启或弃疗。"""
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

    """╔══════════════════════════════════════════════
    消息格式转换
    ╚══════════════════════════════════════════════"""

    @staticmethod
    def _to_mai_message_dict(data: dict) -> dict:
        """将侧车 JSON 转换为 MaiBot 标准的入站消息字典。"""
        sender = data.get("sender", {})
        chat_id = str(data.get("chat_id", ""))
        text = str(data.get("text", "") or "")

        raw_message: list[dict] = []

        if text:
            raw_message.append({"type": "text", "data": text})

        for att in data.get("attachments", []) or []:
            att_type = str(att.get("type", "")).strip().lower()
            data_base64 = att.get("data_base64", "")

            if att_type == "image":
                raw_message.append({
                    "type": "image",
                    "data": "",
                    "binary_data_base64": data_base64,
                    "hash": "",
                })
            else:
                raw_message.append({"type": "dict", "data": att})

        if not raw_message:
            raw_message = [{"type": "text", "data": ""}]

        return {
            "message_id": data.get("message_id", ""),
            "platform": "imessage",
            "session_id": chat_id,
            "message_info": {
                "user_info": {
                    "user_id": str(sender.get("address", "unknown")),
                    "user_nickname": str(sender.get("name", "unknown")),
                },
                "additional_config": (
                    {"platform_io_account_id": str(data.get("line_phone", "")).strip()}
                    if str(data.get("line_phone", "")).strip() not in {"", "shared"}
                    else {}
                ),
            },
            "raw_message": raw_message,
        }

    """╔══════════════════════════════════════════════
    出站：MaiBot → iMessage
    ╚══════════════════════════════════════════════"""

    @MessageGateway(
        route_type="duplex",
        name="imessage",
        platform="imessage",
        protocol="photon",
        description="iMessage 消息收发网关（通过 Photon Spectrum 云端）",
        timeout_ms=40000,
    )
    async def send_to_imessage(
        self,
        message: dict[str, Any],
        route: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """出站入口：将 MaiBot 回复通过侧车发送到 iMessage。"""
        del metadata, kwargs

        session_id = str(message.get("session_id", "") or "")

        if not session_id:
            return {"success": False, "error": "缺少目标会话"}

        message_info = message.get("message_info", {})
        additional_config = message_info.get("additional_config", {}) if isinstance(message_info, dict) else {}
        target_user_id = str(additional_config.get("platform_io_target_user_id", "") or "").strip()
        chat_id = target_user_id if target_user_id else session_id

        route_account_id = ""
        if isinstance(route, dict):
            route_account_id = str(route.get("account_id", "") or "").strip()
        inherited_account_id = str(additional_config.get("platform_io_account_id", "") or "").strip()
        line_phone = route_account_id or inherited_account_id
        if line_phone == "shared":
            line_phone = ""

        if self._bridge_ws is None or not self._gateway_ready:
            return {"success": False, "error": "iMessage 网关未就绪"}

        # Build text and attachments from raw_message
        raw_message = message.get("raw_message", [])
        if not isinstance(raw_message, list):
            raw_message = []

        payload_text_parts: list[str] = []
        attachments: list[dict] = []

        for component in raw_message:
            if not isinstance(component, dict):
                continue
            comp_type = str(component.get("type", "")).strip().lower()

            if comp_type == "text":
                payload_text_parts.append(str(component.get("data", "")))
            elif comp_type == "image":
                b64 = str(component.get("binary_data_base64", "") or "")
                if b64:
                    max_attachment_bytes = self.config.bridge.max_attachment_size_mb * 1024 * 1024
                    decoded_size = _base64_decoded_size(b64)
                    if decoded_size > max_attachment_bytes:
                        self.ctx.logger.warning(
                            "跳过过大的出站图片: %.2f MB > %d MB",
                            decoded_size / 1024 / 1024,
                            self.config.bridge.max_attachment_size_mb,
                        )
                        continue
                    attachments.append({
                        "type": "image",
                        "mime_type": "image/png",
                        "data_base64": b64,
                    })
            elif comp_type == "voice":
                # 语音不支持（Photon AttachmentService 对 iMessage 语音附件下载有 bug）
                self.ctx.logger.warning("不支持发送语音消息，已跳过")

        payload_text = "".join(payload_text_parts)

        # Fallback: if raw_message was empty, use legacy processed_plain_text
        if not payload_text and not attachments:
            payload_text = str(message.get("processed_plain_text", "") or "")

        if not payload_text and not attachments:
            return {"success": False, "error": "缺少消息内容或目标"}

        request_id = secrets.token_hex(16)
        loop = asyncio.get_running_loop()
        receipt_future = loop.create_future()
        self._pending_sends[request_id] = receipt_future
        bridge_ws = self._bridge_ws

        try:
            payload = {
                "type": "send",
                "request_id": request_id,
                "data": {
                    "chat_id": chat_id,
                    "line_phone": line_phone,
                    "text": payload_text,
                    "attachments": attachments,
                },
            }
            await bridge_ws.send(json.dumps(payload, ensure_ascii=False))
            receipt = await asyncio.wait_for(
                asyncio.shield(receipt_future),
                timeout=_SEND_ACK_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            self.ctx.logger.error("等待 Photon 发送回执超时: request_id=%s", request_id)
            return {"success": False, "error": "Photon 发送确认超时（30 秒）"}
        except Exception as exc:
            self.ctx.logger.error("发送消息到侧车失败: %s", exc)
            return {"success": False, "error": str(exc) or "发送消息到侧车失败"}
        finally:
            self._pending_sends.pop(request_id, None)

        if not receipt.get("success", False):
            error = str(receipt.get("error", "") or "").strip() or "Photon 未提供发送失败原因"
            self.ctx.logger.error("Photon 发送失败: %s", error)
            return {"success": False, "error": error}

        result: dict[str, Any] = {"success": True}
        external_message_id = str(receipt.get("external_message_id", "") or "").strip()
        if external_message_id:
            result["external_message_id"] = external_message_id
        return result

    """╔══════════════════════════════════════════════
    管理命令
    ╚══════════════════════════════════════════════"""

    @Command(
        "imessage_status",
        description="查看 iMessage 适配器运行状态",
        pattern=r"^/imessage_status$",
    )
    async def handle_status(self, stream_id: str = "", **kwargs: Any) -> tuple:
        """返回侧车 PID、Photon 连接状态等运行时信息。"""
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
        # 完整停止连接管线会先取消后台 monitor，避免手动重连与自动重启并发。
        await self._restart_connection_if_needed()
        await self.ctx.send.text("🔄 iMessage 适配器已触发重连", stream_id)
        return True, "已重连", True


def create_plugin() -> IMessageAdapterPlugin:
    """MaiBot 插件工厂函数。"""
    return IMessageAdapterPlugin()
