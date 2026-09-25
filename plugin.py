"""
MaiBot iMessage Adapter — Main Program

采用侧车模式: Python 插件通过本地 WebSocket 与 Node.js 进程通信
Node.js 侧车通过 spectrum-ts SDK 连接 Photon Cloud 实现 iMessage 收发

Made BY Galeros

"""

# 更新日志: 增加侧车版本检测，防止本地编译文件不同步 26/7/9 19:14

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from maibot_sdk import Command, MaiBotPlugin, MessageGateway
from maibot_sdk.types import MessageGatewayRouteType

from .config import IMessageAdapterConfig, PLUGIN_VERSION
from .protocol import (
    extract_reply_target,
    extract_structured_action,
    to_mai_message_dict,
)

if TYPE_CHECKING:
    from maibot_sdk import PluginConfigBase

"""nodeenv 安装时的目录变量"""
_NODEENV_DIR = Path(__file__).parent / ".nodeenv"
_NODEENV_BIN = "Scripts" if os.name == "nt" else "bin"

_MIN_NODE_VERSION = (20, 18, 1)
_WS_FRAME_OVERHEAD_BYTES = 256 * 1024
_SEND_ACK_TIMEOUT_SECONDS = 30.0


def _bridge_frame_limit_bytes(max_message_size_mb: int) -> int:
    """Return a WebSocket frame limit large enough for one bounded message."""
    message_bytes = max(1, max_message_size_mb) * 1024 * 1024
    base64_bytes = 4 * ((message_bytes + 2) // 3)
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

        try:
            projects = self.config.photon.configured_projects()
        except (AttributeError, TypeError, ValueError) as exc:
            self.ctx.logger.error("iMessage Photon 配置无效: %s", exc)
            return
        if len(projects) > 20:
            self.ctx.logger.error("iMessage 最多支持 20 个 Photon 项目")
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
            monitor_task = self._monitor_task
            self._monitor_task = None
            monitor_task.cancel()
            await asyncio.gather(monitor_task, return_exceptions=True)

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

        max_frame_bytes = _bridge_frame_limit_bytes(self.config.bridge.max_message_size_mb)
        server = await websockets.serve(
            ws_handler, "127.0.0.1", port,
            max_size=max_frame_bytes,
        )
        self.ctx.logger.info(
            "WebSocket Server 已启动: 127.0.0.1:%d，单附件: %d MB，单消息总附件: %d MB，帧上限: %.1f MB",
            port,
            self.config.bridge.max_attachment_size_mb,
            self.config.bridge.max_message_size_mb,
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
                    project_ids = [
                        str(value).strip()
                        for value in msg.get("project_ids", [])
                        if str(value).strip()
                    ]
                    failed_project_ids = [
                        str(value).strip()
                        for value in msg.get("failed_project_ids", [])
                        if str(value).strip()
                    ]
                    await self.ctx.gateway.update_state(
                        "imessage",
                        ready=True,
                        platform="imessage",
                        metadata={
                            "protocol": "photon",
                            "project_ids": project_ids,
                            "failed_project_ids": failed_project_ids,
                            "bridge_protocol_version": msg.get("protocol_version"),
                        },
                    )
                    self.ctx.logger.info(
                        "Photon 已就绪：%d 个项目", len(project_ids) or 1
                    )
                    if failed_project_ids:
                        self.ctx.logger.warning(
                            "部分 Photon 项目未能连接: %s",
                            ", ".join(failed_project_ids),
                        )

                elif tp in {"message", "native_event"}:
                    data = msg.get("data", {})
                    if not isinstance(data, dict):
                        self.ctx.logger.warning("侧车事件 data 不是对象")
                        continue
                    data = dict(data)
                    if tp == "native_event":
                        data["is_system_event"] = True
                    if bool(data.get("is_from_me", False)):
                        self.ctx.logger.debug("忽略自身 iMessage 回声: %s", data.get("message_id"))
                        continue

                    message_id = str(data.get("message_id", "") or "")
                    line_phone = str(data.get("line_phone", "") or "").strip()
                    mai_msg = self._to_mai_message_dict(data)
                    project_id = str(data.get("project_id", "") or "").strip()
                    route_metadata: dict[str, str] = {}
                    if line_phone and line_phone != "shared":
                        route_metadata["self_id"] = line_phone
                    if project_id:
                        route_metadata["connection_id"] = project_id
                    native_event = data.get("native_event")
                    event_id = ""
                    if isinstance(native_event, dict):
                        event_id = str(native_event.get("event_id", "") or "")
                    external_id = event_id or message_id
                    dedupe_key = json.dumps(
                        [project_id, line_phone, external_id],
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    try:
                        accepted = await self.ctx.gateway.route_message(
                            gateway_name="imessage",
                            message=mai_msg,
                            route_metadata=route_metadata or None,
                            external_message_id=external_id,
                            dedupe_key=dedupe_key,
                        )
                        if not accepted:
                            self.ctx.logger.debug("Host 未接收入站消息: %s", external_id)
                    except Exception as exc:
                        self.ctx.logger.error("注入 iMessage 入站事件失败: %s", exc)

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
                            "delivery_status": msg.get("delivery_status"),
                            "action_metadata": msg.get("action_metadata"),
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

    @staticmethod
    def _sha256_file(path: Path) -> str:
        """返回文件 SHA-256；文件不存在或不可读时返回空字符串。"""
        try:
            digest = hashlib.sha256()
            with path.open("rb") as file:
                for chunk in iter(lambda: file.read(1024 * 1024), b""):
                    digest.update(chunk)
            return digest.hexdigest()
        except OSError:
            return ""

    async def _ensure_sidecar_built(self, sidecar_dir: Path) -> bool:
        """确保侧车依赖与编译产物都和当前仓库状态一致。"""
        dist_dir = sidecar_dir / "dist"
        dist_file = dist_dir / "index.js"
        version_file = dist_dir / ".adapter-version"

        lock_file = sidecar_dir / "package-lock.json"
        package_file = sidecar_dir / "package.json"
        dependency_source = lock_file if lock_file.exists() else package_file
        dependency_hash = self._sha256_file(dependency_source)

        node_modules = sidecar_dir / "node_modules"
        dependency_marker = node_modules / ".adapter-deps.sha256"
        installed_dependency_hash = ""
        if dependency_marker.exists():
            try:
                installed_dependency_hash = dependency_marker.read_text(encoding="utf-8").strip()
            except OSError:
                installed_dependency_hash = ""
        dependencies_current = (
            node_modules.exists()
            and bool(dependency_hash)
            and installed_dependency_hash == dependency_hash
        )

        built_version = ""
        if version_file.exists():
            try:
                built_version = version_file.read_text(encoding="utf-8").strip()
            except OSError:
                built_version = ""

        build_current = dist_file.exists() and built_version == PLUGIN_VERSION
        if build_current and dependencies_current:
            return True

        if dist_dir.exists():
            self.ctx.logger.warning(
                "侧车编译产物需要刷新（built=%s current=%s deps=%s），正在清理 dist",
                built_version or "unknown",
                PLUGIN_VERSION,
                "current" if dependencies_current else "stale",
            )
            await asyncio.to_thread(shutil.rmtree, dist_dir, ignore_errors=True)

        node_path, npm_path, npx_path = await self._resolve_node_binaries()

        node_bin_dir = str(Path(node_path).parent)
        env = os.environ.copy()
        env["PATH"] = node_bin_dir + os.pathsep + env.get("PATH", "")

        if not dependencies_current:
            npm_command = "ci" if lock_file.exists() else "install"
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

                dependency_hash = self._sha256_file(dependency_source)
                if dependency_hash:
                    try:
                        dependency_marker.write_text(dependency_hash + "\n", encoding="utf-8")
                    except OSError as exc:
                        self.ctx.logger.warning("无法写入依赖版本标记 %s: %s", dependency_marker, exc)
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
            "PHOTON_PROJECTS": json.dumps(
                self.config.photon.configured_projects(), ensure_ascii=False
            ),
            "MAX_ATTACHMENT_SIZE_MB": str(self.config.bridge.max_attachment_size_mb),
            "MAX_MESSAGE_SIZE_MB": str(self.config.bridge.max_message_size_mb),
            "INBOUND_REACTION_EMOJI": self.config.plugin.inbound_reaction_emoji,
        }

        self._sidecar_process = await asyncio.create_subprocess_exec(
            node_path,
            "dist/index.js",
            cwd=str(sidecar_dir),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        process = self._sidecar_process
        self.ctx.logger.info("侧车进程已启动: PID=%d", process.pid)

        if process.stdout is not None:
            asyncio.create_task(self._read_sidecar_stdout(process))
        if process.stderr is not None:
            asyncio.create_task(self._read_sidecar_stderr(process))

    async def _read_sidecar_stdout(self, process: asyncio.subprocess.Process) -> None:
        """将指定侧车进程的 stdout 逐行转发到框架日志。"""
        if process.stdout is None:
            return
        try:
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                if text:
                    self.ctx.logger.info("[侧车:%d] %s", process.pid, text)
        except (asyncio.CancelledError, Exception) as exc:
            if isinstance(exc, asyncio.CancelledError):
                raise

    async def _read_sidecar_stderr(self, process: asyncio.subprocess.Process) -> None:
        """将指定侧车进程的 stderr 逐行转发到框架日志。"""
        if process.stderr is None:
            return
        try:
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                if text:
                    self.ctx.logger.warning("[侧车:%d] %s", process.pid, text)
        except (asyncio.CancelledError, Exception) as exc:
            if isinstance(exc, asyncio.CancelledError):
                raise

    """╔══════════════════════════════════════════════
    侧车容灾：崩溃重启 + 进程监控
    ╚══════════════════════════════════════════════"""

    async def _restart_sidecar(self) -> None:
        """终止并重新启动侧车进程。"""
        self.ctx.logger.info(
            "正在重启侧车（第 %d/%d 次）…",
            self._retry_count,
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
        """将 Sidecar 事件完整转换为 MaiBot 标准消息结构。"""
        return to_mai_message_dict(data)

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

        message_info = message.get("message_info", {})
        additional_config = (
            message_info.get("additional_config", {})
            if isinstance(message_info, dict)
            else {}
        )
        if not isinstance(additional_config, dict):
            additional_config = {}
        raw_message = message.get("raw_message", [])
        if not isinstance(raw_message, list):
            raw_message = []
        try:
            structured_action = extract_structured_action(message)
        except ValueError as exc:
            return {"success": False, "error": str(exc)}

        session_id = str(message.get("session_id", "") or "").strip()
        target_user_id = str(
            additional_config.get("platform_io_target_user_id", "") or ""
        ).strip()
        chat_id = target_user_id or session_id
        action_name = str((structured_action or {}).get("action", ""))
        if not chat_id and action_name == "open_dm":
            chat_id = str((structured_action or {}).get("recipient", "") or "").strip()
        if not chat_id:
            return {"success": False, "error": "缺少目标会话或收件人"}

        route_account_id = ""
        route_project_id = ""
        if isinstance(route, dict):
            route_account_id = str(
                route.get("account_id", route.get("self_id", "")) or ""
            ).strip()
            route_project_id = str(
                route.get("connection_id", route.get("project_id", "")) or ""
            ).strip()
        inherited_account_id = str(
            additional_config.get("platform_io_account_id", "") or ""
        ).strip()
        line_phone = route_account_id or inherited_account_id
        if line_phone == "shared":
            line_phone = ""
        project_id = route_project_id or str(
            additional_config.get("platform_io_project_id", "") or ""
        ).strip()

        if self._bridge_ws is None or not self._gateway_ready:
            return {"success": False, "error": "iMessage 网关未就绪"}

        payload_text_parts: list[str] = []
        attachments: list[dict[str, Any]] = []
        parts: list[dict[str, Any]] = []
        attachment_bytes_used = 0
        max_attachment_bytes = self.config.bridge.max_attachment_size_mb * 1024 * 1024
        max_message_bytes = self.config.bridge.max_message_size_mb * 1024 * 1024

        for component in raw_message:
            if not isinstance(component, dict):
                continue
            comp_type = str(component.get("type", "")).strip().lower()
            if comp_type == "text":
                text_part = str(component.get("data", ""))
                payload_text_parts.append(text_part)
                if text_part:
                    if parts and parts[-1].get("type") == "text":
                        parts[-1]["text"] += text_part
                    else:
                        parts.append({"type": "text", "text": text_part})
                continue
            if comp_type in {"imessage_action", "imessage-action", "reply", "quote", "imessage_event"}:
                continue

            if comp_type == "emoji":
                component_data = component.get("data")
                emoji_text = component_data if isinstance(component_data, str) else ""
                if isinstance(component_data, dict):
                    emoji_text = str(component_data.get("emoji", component_data.get("name", "")) or "")
                if emoji_text:
                    payload_text_parts.append(emoji_text)
                    if parts and parts[-1].get("type") == "text":
                        parts[-1]["text"] += emoji_text
                    else:
                        parts.append({"type": "text", "text": emoji_text})

            if comp_type in {"image", "emoji", "voice", "record", "audio", "file"}:
                component_data = component.get("data")
                nested_data = component_data if isinstance(component_data, dict) else {}
                b64 = str(
                    component.get("binary_data_base64")
                    or nested_data.get("binary_data_base64")
                    or nested_data.get("data_base64")
                    or ""
                )
                if not b64:
                    if comp_type in {"voice", "record", "audio"}:
                        self.ctx.logger.warning("跳过没有音频数据的语音段")
                    continue
                decoded_size = _base64_decoded_size(b64)
                companion_b64 = str(
                    component.get("live_photo_companion_base64")
                    or nested_data.get("live_photo_companion_base64")
                    or nested_data.get("companion_data_base64")
                    or ""
                )
                companion_size = _base64_decoded_size(companion_b64)
                total_size = decoded_size + companion_size
                if total_size > max_attachment_bytes:
                    self.ctx.logger.warning(
                        "跳过过大的出站附件: %.2f MB > %d MB",
                        total_size / 1024 / 1024,
                        self.config.bridge.max_attachment_size_mb,
                    )
                    continue
                if attachment_bytes_used + total_size > max_message_bytes:
                    self.ctx.logger.warning(
                        "跳过附件：单条消息附件总量将超过 %d MB",
                        self.config.bridge.max_message_size_mb,
                    )
                    continue
                attachment_bytes_used += total_size

                is_voice = comp_type in {"voice", "record", "audio"}
                is_image = comp_type in {"image", "emoji"}
                mime_type = str(
                    component.get("mime_type")
                    or nested_data.get("mime_type")
                    or ("audio/mp4" if is_voice else "image/png" if is_image else "application/octet-stream")
                )
                is_live_photo = bool(companion_b64) and is_image
                attachment_index = len(attachments)
                attachment = {
                    "type": "live_photo" if is_live_photo else "voice" if is_voice else "image" if is_image else "file",
                    "mime_type": mime_type,
                    "name": str(
                        component.get("name")
                        or nested_data.get("name")
                        or ("voice.m4a" if is_voice else "maibot-image.png")
                    ),
                    "duration": nested_data.get("duration", component.get("duration")),
                    "data_base64": b64,
                }
                if is_live_photo:
                    attachment["companion_data_base64"] = companion_b64
                    attachment["companion_name"] = str(
                        component.get("live_photo_companion_name")
                        or nested_data.get("live_photo_companion_name")
                        or nested_data.get("companion_name")
                        or ""
                    )
                    attachment["companion_mime_type"] = str(
                        component.get("live_photo_companion_mime_type")
                        or nested_data.get("live_photo_companion_mime_type")
                        or nested_data.get("companion_mime_type")
                        or "video/quicktime"
                    )
                attachments.append(attachment)
                parts.append({
                    "type": "voice" if is_voice else "attachment",
                    "attachment_index": attachment_index,
                })

        payload_text = "".join(payload_text_parts)
        if not payload_text and not attachments:
            payload_text = str(message.get("processed_plain_text", "") or "")

        reply_target = extract_reply_target(raw_message)
        live_photo_attachments = [
            item for item in attachments if item.get("type") == "live_photo"
        ]
        request_type = "send"
        action_data: dict[str, Any] | None = None
        if structured_action is not None:
            if live_photo_attachments and action_name != "send_live_photo":
                return {
                    "success": False,
                    "error": "Live Photo 需要使用 send_live_photo 单独发送，不能与文本或其他附件混合",
                }
            request_type = "action"
            action_data = dict(structured_action)
            if action_name in {"send", "send_reply", "send_audio_message", "send_effect"}:
                action_data.setdefault("text", payload_text)
                action_data.setdefault("attachments", attachments)
                action_data.setdefault("parts", parts)
            if action_name == "send_reply" and not action_data.get("reply_to_message_id"):
                action_data["reply_to_message_id"] = action_data.get(
                    "target_message_id", action_data.get("message_id", "")
                )
            if action_name == "send_audio_message" and not action_data.get("data_base64"):
                voice_attachment = next(
                    (item for item in attachments if item.get("type") == "voice"),
                    None,
                )
                if voice_attachment is not None:
                    action_data.update(voice_attachment)
            if action_name == "send_live_photo":
                live_photo = live_photo_attachments[0] if live_photo_attachments else None
                if live_photo is not None:
                    action_data.setdefault("data_base64", live_photo["data_base64"])
                    action_data.setdefault("mime_type", live_photo["mime_type"])
                    action_data.setdefault("name", live_photo["name"])
                    action_data.setdefault(
                        "companion_data_base64", live_photo["companion_data_base64"]
                    )
                    action_data.setdefault("companion_name", live_photo.get("companion_name", ""))
            if action_name == "send_live_photo" and (
                payload_text or len(live_photo_attachments) > 1 or len(attachments) > 1
            ):
                return {
                    "success": False,
                    "error": "Live Photo 目前需要单独发送，不能与文本或其他附件混合",
                }
        elif live_photo_attachments:
            if len(live_photo_attachments) != 1 or len(attachments) != 1 or payload_text:
                return {
                    "success": False,
                    "error": "Live Photo 目前需要单独发送，不能与文本或其他附件混合",
                }
            live_photo = live_photo_attachments[0]
            request_type = "action"
            action_data = {
                "action": "send_live_photo",
                "data_base64": live_photo["data_base64"],
                "mime_type": live_photo["mime_type"],
                "name": live_photo["name"],
                "companion_data_base64": live_photo["companion_data_base64"],
                "companion_name": live_photo.get("companion_name", ""),
            }
        elif reply_target:
            request_type = "action"
            action_data = {
                "action": "send_reply",
                "reply_to_message_id": reply_target,
                "text": payload_text,
                "attachments": attachments,
            }

        if request_type == "send" and not payload_text and not attachments:
            return {"success": False, "error": "缺少消息内容或目标"}

        request_id = secrets.token_hex(16)
        loop = asyncio.get_running_loop()
        receipt_future = loop.create_future()
        self._pending_sends[request_id] = receipt_future
        bridge_ws = self._bridge_ws

        try:
            payload = {
                "type": request_type,
                "request_id": request_id,
                "data": {
                    "chat_id": chat_id,
                    "line_phone": line_phone,
                    "project_id": project_id,
                    **(
                        action_data
                        if action_data is not None
                        else {
                            "text": payload_text,
                            "attachments": attachments,
                            "parts": parts,
                        }
                    ),
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
        delivery_status = receipt.get("delivery_status")
        if isinstance(delivery_status, dict):
            result["delivery_status"] = delivery_status
        action_metadata = receipt.get("action_metadata")
        if isinstance(action_metadata, dict):
            result["action_metadata"] = action_metadata
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
