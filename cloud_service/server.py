"""The Cloud Service: robot registry, heartbeats, session orchestration (the control plane).

Started by: cloud_service/__main__.py  ->  CloudService(...).start()
Explained in Notebook 2 (sections 6 and 10) and Notebook 3 (sections 5-7).

What runs once start() is called:
  - a wss:// server. For every client that connects, the websockets library calls _route(),
    which hands the connection to robot_handler() (/robot) or user_handler() (/user).
  - _watchdog(), a background loop that marks robots offline when heartbeats stop or a
    dropped robot doesn't come back within RECONNECT_GRACE.

Security: every robot must send its token in `register` (including when reconnecting), every user
must send `hello` with its token first. Flooding: a connection that doesn't identify itself within
FIRST_MESSAGE_TIMEOUT is closed, and at most MAX_CONNECTIONS are open at once.

Robot states:  online -> in_session -> (reconnecting) -> offline
"""
import asyncio
import contextlib
import logging
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from cloud_service.player_process import (
    DEFAULT_PLAYER_MODULE,
    PlayerProcess,
    PlayerSpawnError,
    spawn_player,
)
from common import config
from common.config import HEARTBEAT_INTERVAL, cloud_url
from common.protocol import ROBOT_PATH, USER_PATH, msg, parse
from common.security import TokenStore, load_identity, server_tls_context

log = logging.getLogger("Cloud")

# Module-level copies, looked up each time they're used, so tests can shorten them with
# monkeypatch.setattr(cloud_service.server, "HEARTBEAT_TIMEOUT", ...).
HEARTBEAT_TIMEOUT = config.HEARTBEAT_TIMEOUT
RECONNECT_GRACE = config.RECONNECT_GRACE
FIRST_MESSAGE_TIMEOUT = config.FIRST_MESSAGE_TIMEOUT
MAX_CONNECTIONS = config.MAX_CONNECTIONS


@dataclass
class Session:
    session_id: str
    robot_id: str
    user_ws: object
    user_name: str
    player: PlayerProcess


@dataclass
class RobotRecord:
    # field names follow GRID's PartyInstance type (robot_name, state, last_seen_ms, endpoint)
    robot_id: str
    ws: object
    state: str = "online"
    last_seen: float = field(default_factory=time.monotonic)
    session: Session | None = None
    disconnected_at: float | None = None


class CloudService:
    def __init__(self, host: str, port: int, pki_dir, player_module: str = DEFAULT_PLAYER_MODULE):
        self.host = host
        self.port = port
        self.pki_dir = Path(pki_dir)
        self.player_module = player_module
        self.tokens = TokenStore.load(self.pki_dir / "tokens.json")
        self.tls = server_tls_context(load_identity(self.pki_dir, "cloud"))
        self.robots: dict[str, RobotRecord] = {}
        self.open_connections = 0
        self._server = None
        self._watchdog_task: asyncio.Task | None = None

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        self._server = await serve(self._route, self.host, self.port, ssl=self.tls)
        self._watchdog_task = asyncio.create_task(self._watchdog())
        log.info("listening on %s", cloud_url(self.host, self.port))

    async def stop(self) -> None:
        for rec in list(self.robots.values()):
            await self.end_session(rec, "cloud_shutdown")
        if self._watchdog_task:
            self._watchdog_task.cancel()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        log.info("stopped")

    async def _route(self, ws) -> None:
        """Called by the websockets library once per connecting client."""
        if self.open_connections >= MAX_CONNECTIONS:
            await ws.close(code=1013, reason="too many connections")   # 1013 = try again later
            return
        self.open_connections += 1
        try:
            path = ws.request.path
            if path == ROBOT_PATH:
                await self.robot_handler(ws)
            elif path == USER_PATH:
                await self.user_handler(ws)
            else:
                await ws.close(code=1008, reason="unknown path")
        finally:
            self.open_connections -= 1

    async def _first_message(self, ws) -> dict | None:
        """The first message, or None if it's missing, malformed or too slow (connection closed)."""
        try:
            data = parse(await asyncio.wait_for(ws.recv(), FIRST_MESSAGE_TIMEOUT))
        except asyncio.TimeoutError:
            await ws.close(code=1008, reason="no identification in time")
            return None
        except ConnectionClosed:
            return None
        return data

    async def _refuse(self, ws, reason: str) -> None:
        await ws.send(msg("error", reason=reason))
        await ws.close(code=1008, reason=reason)       # 1008 = policy violation

    # ------------------------------------------------------------------ robots
    async def robot_handler(self, ws) -> None:
        """One robot's whole connection: register, then heartbeats until it ends."""
        first = await self._first_message(ws)
        if first is None:
            return
        robot_id = first.get("robot_id")
        if first["type"] != "register" or not robot_id:
            await self._refuse(ws, "must_register_first")
            return
        if not self.tokens.check_robot(robot_id, first.get("token")):
            log.warning("rejected registration for %s: bad or missing token", robot_id)
            await self._refuse(ws, "unauthorized")
            return

        existing = self.robots.get(robot_id)
        if existing and existing.state in ("online", "in_session"):
            log.warning("rejected duplicate registration for %s", robot_id)
            await ws.send(msg("error", reason="duplicate_registration"))
            return

        if existing and existing.state == "reconnecting":
            rec = await self._resume(existing, ws)
        else:
            rec = RobotRecord(robot_id, ws)
            self.robots[robot_id] = rec
            await ws.send(msg("registered", robot_id=robot_id))
            log.info("registered %s", robot_id)

        try:
            async for raw in ws:
                data = parse(raw)
                if not data:
                    continue
                if data["type"] == "heartbeat":
                    rec.last_seen = time.monotonic()
                elif data["type"] == "goodbye":
                    await self.mark_offline(rec, "robot_shutdown")
                    return
        except ConnectionClosed:
            pass
        finally:
            # dropped without a goodbye: hold its place instead of ending the session
            if rec.ws is ws and rec.state in ("online", "in_session"):
                rec.state = "reconnecting"
                rec.disconnected_at = time.monotonic()
                log.warning("%s connection dropped; holding it for %.0fs", robot_id, RECONNECT_GRACE)
                await self._notify_user(rec, "reconnecting")

    async def _resume(self, rec: RobotRecord, ws) -> RobotRecord:
        """Same robot (same token) back inside the grace period: reattach it to its existing session."""
        rec.ws, rec.disconnected_at, rec.last_seen = ws, None, time.monotonic()
        rec.state = "in_session" if rec.session else "online"
        await ws.send(msg("registered", robot_id=rec.robot_id, resumed=True))
        log.info("%s reconnected within the grace period", rec.robot_id)
        if rec.session:
            await ws.send(msg("join_session", player_endpoint=rec.session.player.endpoint))
            await self._notify_user(rec, "in_session")
        return rec

    async def mark_offline(self, rec: RobotRecord, why: str) -> None:
        rec.state = "offline"
        log.warning("%s is offline (%s)", rec.robot_id, why)
        await self.end_session(rec, f"robot_offline:{why}")
        if why == "heartbeat_timeout":
            # a silent robot must reconnect and register again to be available
            await rec.ws.close(code=1001, reason="heartbeat_timeout")

    async def _watchdog(self) -> None:
        """Background loop started by start(): the heartbeat 'AFK timer'."""
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL / 2)
            now = time.monotonic()
            for rec in list(self.robots.values()):
                if rec.state in ("online", "in_session") and now - rec.last_seen > HEARTBEAT_TIMEOUT:
                    await self.mark_offline(rec, "heartbeat_timeout")
                elif rec.state == "reconnecting" and now - rec.disconnected_at > RECONNECT_GRACE:
                    await self.mark_offline(rec, "reconnect_timeout")

    async def _notify_user(self, rec: RobotRecord, state: str) -> None:
        if rec.session:
            with contextlib.suppress(ConnectionClosed):
                await rec.session.user_ws.send(msg("robot_status", robot_id=rec.robot_id, state=state))

    # ------------------------------------------------------------------ users
    async def user_handler(self, ws) -> None:
        """One user's whole connection: hello, then requests until it ends."""
        first = await self._first_message(ws)
        if first is None:
            return
        name = first.get("name")
        if first["type"] != "hello" or not self.tokens.check_user(name, first.get("token")):
            log.warning("rejected user %r: bad hello or token", name)
            await self._refuse(ws, "unauthorized")
            return
        await ws.send(msg("welcome"))
        log.info("user %s connected", name)

        handlers = {"list_robots": self.handle_list, "connect": self.handle_connect,
                    "disconnect": self.handle_disconnect}
        try:
            async for raw in ws:
                data = parse(raw)
                handler = handlers.get(data["type"]) if data else None
                if handler is None:
                    await ws.send(msg("error", reason="bad_request",
                                      request_id=data.get("request_id") if data else None))
                    continue
                await handler(ws, name, data)
        except ConnectionClosed:
            pass
        finally:
            for rec in list(self.robots.values()):
                if rec.session and rec.session.user_ws is ws:
                    await self.end_session(rec, "user_disconnected")

    async def handle_list(self, ws, name: str, data: dict) -> None:
        robots = [{"robot_id": r.robot_id, "state": r.state} for r in self.robots.values()]
        await ws.send(msg("robots", robots=robots, request_id=data.get("request_id")))

    async def handle_connect(self, ws, name: str, data: dict) -> None:
        rid, req = data.get("robot_id"), data.get("request_id")
        rec = self.robots.get(rid)
        refusal = None
        if rec is None or rec.state == "offline":
            refusal = "robot_not_found"
        elif rec.session and rec.session.user_ws is ws:
            refusal = "already_connected"
        elif rec.state == "in_session":
            # ASSUMPTION A3: one User controls a robot at a time (a User may hold several robots).
            refusal = "robot_busy"
        elif rec.state == "reconnecting":
            refusal = "robot_reconnecting"
        if refusal:
            await ws.send(msg("error", reason=refusal, robot_id=rid, request_id=req))
            return
        await self.start_session(rec, ws, name, req)

    async def handle_disconnect(self, ws, name: str, data: dict) -> None:
        rid, req = data.get("robot_id"), data.get("request_id")
        rec = self.robots.get(rid)
        if rec is None or rec.session is None or rec.session.user_ws is not ws:
            await ws.send(msg("error", reason="not_connected", robot_id=rid, request_id=req))
            return
        await self.end_session(rec, "user_disconnected", notify_user=False)
        await ws.send(msg("disconnected", robot_id=rid, request_id=req))

    # ------------------------------------------------------------------ sessions
    async def start_session(self, rec: RobotRecord, user_ws, user_name: str, request_id) -> None:
        # ASSUMPTION A1: the Player is spawned when a User connects (the Functional Requirements),
        # not when the robot turns on (the Overview). The assignment says both.
        rec.state = "in_session"   # claim first: the spawn below waits, and other requests run meanwhile
        try:
            player = await asyncio.to_thread(spawn_player, rec.robot_id, self.host,
                                             self.pki_dir, self.player_module)
        except PlayerSpawnError as err:
            rec.state = "online"
            log.error("spawn failed: %s", err)
            await user_ws.send(msg("error", reason="player_spawn_failed", robot_id=rec.robot_id,
                                   request_id=request_id))
            return

        rec.session = Session(uuid.uuid4().hex[:8], rec.robot_id, user_ws, user_name, player)
        log.info("session %s: %s <-> %s via Player at %s",
                 rec.session.session_id, user_name, rec.robot_id, player.endpoint)
        await rec.ws.send(msg("join_session", player_endpoint=player.endpoint))
        await user_ws.send(msg("session_ready", robot_id=rec.robot_id,
                               player_endpoint=player.endpoint, request_id=request_id))

    async def end_session(self, rec: RobotRecord, reason: str, notify_user: bool = True) -> None:
        """Every way a session can end comes through here, so cleanup lives in one place."""
        s = rec.session
        if s is None:
            return
        rec.session = None
        if rec.state == "in_session":
            rec.state = "online"
        await asyncio.to_thread(s.player.stop)
        log.info("session %s ended (%s), Player stopped", s.session_id, reason)
        targets = [rec.ws] + ([s.user_ws] if notify_user else [])
        for ws in targets:
            with contextlib.suppress(ConnectionClosed):
                await ws.send(msg("session_ended", robot_id=rec.robot_id, reason=reason))
