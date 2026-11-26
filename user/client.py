"""The User program's logic: requests to the Cloud Service, and one triangle per connected robot.

Used by: user/cli.py (the command prompt calls these methods).
Explained in Notebook 2 section 8, Notebook 3 (security), Notebook 4 section 4 (the connect() wait).

start() connects to wss://.../user, sends hello with this user's token, and starts _read_loop.
Requests (list_robots, connect, disconnect) carry a request_id; _read_loop matches each reply to
the request waiting for it. Anything else the Cloud pushes (session_ended, robot_status) is logged.

If the connection to the Cloud Service ends (Cloud restarted, network gone), the User keeps
running: it ends its robot sessions (the Cloud ends them on its side too), says so, and reconnects
in the background with the same backoff as the Robot. ASSUMPTION A12: sessions are not resumed
after the User reconnects; the operator connects again.

A User can hold sessions with several robots at once. Each session is its own Zenoh session,
because each robot's triangle goes through its own Player.
"""
import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field

from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from common.config import (COMMAND_PATH_TIMEOUT, RECONNECT_BACKOFF_MAX, RECONNECT_BACKOFF_START,
                           REQUEST_TIMEOUT)
from common.mesh import open_session, topic
from common.protocol import USER_PATH, msg, parse, status_msg, wait_for_subscriber
from common.security import client_tls_context, load_identity, read_token

log = logging.getLogger("User")


class ControlError(Exception):
    """The Cloud Service refused a request; the message is its reason (e.g. robot_not_found)."""


class NotInSession(Exception):
    pass


@dataclass
class RobotSession:
    """This user's side of one robot's triangle."""
    robot_id: str
    zsession: object
    command_pub: object
    status_pub: object
    subs: list = field(default_factory=list)

    def close(self) -> None:
        for s in self.subs:
            s.undeclare()
        self.command_pub.undeclare()
        self.status_pub.undeclare()
        self.zsession.close()


class UserClient:
    def __init__(self, name: str, cloud_url: str, host: str, pki_dir):
        self.name = name
        self.cloud_url = cloud_url
        self.host = host                  # address this user's Zenoh sessions listen on
        # Loaded once, here, so a missing file fails immediately.
        self.identity = load_identity(pki_dir, "users", name)
        self.token = read_token(pki_dir, "users", name)
        self.ws = None                    # None while not connected to the Cloud Service
        self.closing = False              # set by shutdown(): don't reconnect
        self.pending: dict[str, asyncio.Future] = {}
        self.reader: asyncio.Task | None = None
        self.reconnector: asyncio.Task | None = None
        self.sessions: dict[str, RobotSession] = {}
        # Called with a short text for rare session events (session ended, robot reconnecting),
        # which the operator needs to see. user/__main__.py points it at the terminal.
        self.on_notice = lambda text: None

    # ------------------------------------------------------------------ control plane
    async def start(self) -> None:
        """Connect and identify. Raises ControlError("unauthorized") if the token is rejected."""
        await self._open()

    async def _open(self) -> None:
        ws = await ws_connect(self.cloud_url + USER_PATH, ssl=client_tls_context(self.identity.ca))
        await ws.send(msg("hello", name=self.name, token=self.token))
        try:
            reply = parse(await ws.recv())
        except ConnectionClosed:
            reply = None
        if not reply or reply["type"] != "welcome":
            await ws.close()
            raise ControlError(reply.get("reason", "refused") if reply else "connection closed")
        self.ws = ws
        self.reader = asyncio.create_task(self._read_loop(ws))

    @property
    def online(self) -> bool:
        """True while connected to the Cloud Service."""
        return self.ws is not None

    async def _read_loop(self, ws) -> None:
        try:
            async for raw in ws:
                data = parse(raw)
                if not data:
                    continue
                fut = self.pending.pop(data.get("request_id"), None)
                if fut and not fut.done():
                    fut.set_result(data)                  # a reply to one of our requests
                elif data["type"] == "session_ended":
                    self._notice(f"session with {data.get('robot_id')} ended: {data.get('reason')}")
                    await self._close_session(data.get("robot_id"))
                elif data["type"] == "robot_status":
                    self._notice(f"{data.get('robot_id')} is now {data.get('state')}")
        except ConnectionClosed:
            pass
        # The connection is over, whether it was closed cleanly (Cloud shut down) or dropped.
        self.ws = None
        for fut in self.pending.values():
            if not fut.done():
                fut.set_exception(ControlError("cloud_connection_lost"))
        self.pending.clear()
        if self.closing:
            return
        for robot_id in list(self.sessions):
            self._notice(f"session with {robot_id} ended: cloud_connection_lost")
            await self._close_session(robot_id)
        self._notice("lost the Cloud Service; reconnecting")
        self.reconnector = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        delay = RECONNECT_BACKOFF_START
        while not self.closing:
            await asyncio.sleep(delay)
            try:
                await self._open()
            except ControlError as err:          # the Cloud answered but refused us: retrying won't help
                self._notice(f"the Cloud Service refused us ({err}); not reconnecting")
                return
            except (OSError, WebSocketException) as err:
                log.info("can't reach the Cloud Service (%s)", err)
                delay = min(delay * 2, RECONNECT_BACKOFF_MAX)
                continue
            self._notice("reconnected to the Cloud Service")
            return

    def _notice(self, text: str) -> None:
        log.warning(text)
        self.on_notice(text)

    async def _request(self, type_: str, **fields) -> dict:
        if self.ws is None:
            raise ControlError("not connected to the Cloud Service (reconnecting)")
        rid = uuid.uuid4().hex[:8]
        fut = asyncio.get_running_loop().create_future()
        self.pending[rid] = fut
        try:
            await self.ws.send(msg(type_, request_id=rid, **fields))
        except ConnectionClosed:
            self.pending.pop(rid, None)
            raise ControlError("cloud_connection_lost") from None
        try:
            reply = await asyncio.wait_for(fut, REQUEST_TIMEOUT)
        finally:
            self.pending.pop(rid, None)
        if reply["type"] == "error":
            raise ControlError(reply.get("reason"))
        return reply

    async def list_robots(self) -> list[dict]:
        return (await self._request("list_robots"))["robots"]

    async def connect(self, robot_id: str) -> None:
        """Start a session and return only once commands can actually reach the robot."""
        if robot_id in self.sessions:
            raise ControlError("already_connected")
        reply = await self._request("connect", robot_id=robot_id)
        ep = reply["player_endpoint"]
        zsession = open_session(self.host, self.identity, [ep])
        session = RobotSession(robot_id, zsession,
                               command_pub=zsession.declare_publisher(topic(robot_id, "command")),
                               status_pub=zsession.declare_publisher(topic(robot_id, "status")))
        session.subs = [zsession.declare_subscriber(topic(robot_id, t), self._on_message)
                        for t in ("sensor", "processed", "status")]
        self.sessions[robot_id] = session

        # Zenoh drops a put if the sender doesn't yet know of any matching subscriber. Wait until the
        # Robot's command subscription (and someone's status subscription) has reached us, so the
        # first command isn't lost and "joined" is heard.
        for pub in (session.command_pub, session.status_pub):
            if not await wait_for_subscriber(pub, COMMAND_PATH_TIMEOUT):
                await self._close_session(robot_id)
                raise ControlError("robot_unreachable")
        session.status_pub.put(status_msg(self.name, "joined"))   # the Player replies with its current state
        log.info("connected to %s via Player at %s", robot_id, ep)

    async def disconnect(self, robot_id: str) -> None:
        if robot_id not in self.sessions:
            raise NotInSession(robot_id)
        await self._close_session(robot_id)
        if self.ws is not None:                    # offline, the Cloud has already ended it
            await self._request("disconnect", robot_id=robot_id)

    # ------------------------------------------------------------------ data plane
    def _on_message(self, sample) -> None:
        # Runs on a Zenoh thread; the assignment wants every received message printed.
        raw = sample.payload.to_string()
        if str(sample.key_expr).endswith("/status"):
            try:
                if json.loads(raw).get("from") == self.name:
                    return                  # don't print our own status messages
            except (json.JSONDecodeError, AttributeError):
                pass
        log.info("got %s: %s", sample.key_expr, raw)

    def _targets(self, robot_id: str) -> list[RobotSession]:
        """One robot by id, or every connected robot for "all"."""
        if robot_id == "all":
            if not self.sessions:
                raise NotInSession("all")
            return list(self.sessions.values())
        if robot_id not in self.sessions:
            raise NotInSession(robot_id)
        return [self.sessions[robot_id]]

    def send_command(self, robot_id: str, name: str) -> None:
        # ASSUMPTION A10: "all" sends the same command to each connected robot, one message per
        # robot. They're sent together, not executed at the same instant.
        for s in self._targets(robot_id):
            s.command_pub.put(json.dumps({"command": name}))

    def send_status(self, robot_id: str, text: str) -> None:
        # ASSUMPTION A4: the assignment's "User publishes data, Player receives data" uses the status
        # topic, the only one its topic table lets the User publish for the Player to receive.
        for s in self._targets(robot_id):
            s.status_pub.put(status_msg(self.name, text))

    # ------------------------------------------------------------------ cleanup
    async def _close_session(self, robot_id) -> None:
        session = self.sessions.pop(robot_id, None)
        if session is not None:
            # Closing a Zenoh session can block for seconds (about 2 s seen on Windows). Do it in a
            # worker thread so the prompt and the Cloud connection keep working meanwhile.
            await asyncio.to_thread(session.close)

    async def shutdown(self) -> None:
        self.closing = True
        if self.reconnector:
            self.reconnector.cancel()
        for robot_id in list(self.sessions):
            await self._close_session(robot_id)
        if self.ws is not None:
            await self.ws.close()       # the Cloud ends any sessions we still owned
        if self.reader is not None:
            self.reader.cancel()
