"""The Robot program's logic: control plane (Cloud Service) and data plane (the triangle).

Started by: robot/__main__.py  ->  RobotClient(...).run()
Explained in Notebook 2 section 7 (control), Notebook 1 (Zenoh), Notebook 3 (security).

run() loops forever:
  connect to wss://.../robot -> register with token -> heartbeat in the background
  -> handle control messages (join_session / session_ended) until the connection ends
  -> wait (1s, 2s, 4s ... up to RECONNECT_BACKOFF_MAX) and try again.

On join_session it opens a mutual-TLS Zenoh session pointed at the Player, subscribes to
command + status, publishes a sine-wave sensor reading every SENSOR_INTERVAL, and reports
what it does on status (see common/protocol.py).
"""
import asyncio
import json
import logging

from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from common.config import RECONNECT_BACKOFF_MAX, RECONNECT_BACKOFF_START, SENSOR_INTERVAL
import common.config as config
from common.mesh import open_session, topic
from common.protocol import ROBOT_PATH, msg, parse, status_msg, wait_for_subscriber
from common.security import client_tls_context, load_identity, read_token
from robot.jetbot import FakeJetbot
from robot.sensor import SineSensor

log = logging.getLogger("Robot")

# Looked up on every heartbeat, so tests can change it with monkeypatch.
HEARTBEAT_INTERVAL = config.HEARTBEAT_INTERVAL
STATUS_PATH_TIMEOUT = config.COMMAND_PATH_TIMEOUT    # how long to wait for a status subscriber before "joined"


class RegistrationRefused(Exception):
    pass


class RobotClient:
    def __init__(self, robot_id: str, cloud_url: str, host: str, pki_dir, sensor: SineSensor | None = None):
        self.robot_id = robot_id
        self.cloud_url = cloud_url
        self.host = host                  # address this robot's Zenoh session listens on
        self.pki_dir = pki_dir
        # Loaded once, here, so a missing file fails immediately instead of inside the retry loop.
        self.identity = load_identity(pki_dir, "robots", robot_id)
        self.token = read_token(pki_dir, "robots", robot_id)
        self.sensor = sensor or SineSensor()
        self.jetbot = FakeJetbot()
        self.commands = {                 # command name (from the message) -> jetbot method
            "stop": self.jetbot.stop,
            "forward": self.jetbot.forward,
            "backward": self.jetbot.backward,
            "left": self.jetbot.left,
            "right": self.jetbot.right,
        }
        self.ws = None
        self.zsession = None
        self.status_pub = None
        self.subs = []
        self.sensor_task: asyncio.Task | None = None

    # ------------------------------------------------------------------ main loop
    async def run(self) -> None:
        """Stay connected to the Cloud Service, reconnecting with backoff. Raises RegistrationRefused."""
        delay = RECONNECT_BACKOFF_START
        while True:
            try:
                await self._connect_once()
                delay = RECONNECT_BACKOFF_START       # we were connected; reset the backoff
            except (OSError, WebSocketException) as err:
                log.warning("can't reach the Cloud Service (%s)", err)
            except (RegistrationRefused, asyncio.CancelledError):
                raise                                 # refused, or shutting down: stop trying
            except Exception:
                # Anything unexpected in one attempt must not end the robot's life: log it and retry.
                log.exception("unexpected error; will reconnect")
            log.info("reconnecting in %.0fs", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_BACKOFF_MAX)

    async def _connect_once(self) -> None:
        self.ws = await ws_connect(self.cloud_url + ROBOT_PATH, ssl=client_tls_context(self.identity.ca))
        await self.ws.send(msg("register", robot_id=self.robot_id, token=self.token))
        try:
            reply = parse(await self.ws.recv())
        except ConnectionClosed:
            reply = None
        if not reply or reply["type"] != "registered":
            await self.ws.close()
            raise RegistrationRefused(reply.get("reason") if reply else "connection closed")
        log.info("%s registered%s", self.robot_id, " (resumed)" if reply.get("resumed") else "")

        heartbeat = asyncio.create_task(self._heartbeat_loop())
        reason = "cloud_connection_lost"
        try:
            await self._control_loop()
        except asyncio.CancelledError:
            reason = "robot_shutdown"            # Ctrl+C: shutdown() says goodbye next
            raise
        finally:
            heartbeat.cancel()
            await self._leave_session(reason)

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await self.ws.send(msg("heartbeat"))
                await asyncio.sleep(HEARTBEAT_INTERVAL)
        except ConnectionClosed:
            pass   # _control_loop notices the drop too, and run() reconnects

    async def _control_loop(self) -> None:
        handlers = {"join_session": self._on_join_session, "session_ended": self._on_session_ended}
        try:
            async for raw in self.ws:
                data = parse(raw)
                handler = handlers.get(data["type"]) if data else None
                if handler:
                    await handler(data)
        except ConnectionClosed:
            pass
        log.warning("connection to the Cloud Service ended")

    # ------------------------------------------------------------------ data plane
    async def _on_join_session(self, data: dict) -> None:
        ep = data["player_endpoint"]
        self.zsession = open_session(self.host, self.identity, [ep])
        self.status_pub = self.zsession.declare_publisher(topic(self.robot_id, "status"))
        self.subs = [
            self.zsession.declare_subscriber(topic(self.robot_id, "command"), self._on_command),
            self.zsession.declare_subscriber(topic(self.robot_id, "status"), self._on_status),
        ]
        self.sensor_task = asyncio.create_task(self._sensor_loop())
        log.info("joined session via Player at %s", ep)
        # Announce only once someone can hear it (the Player answers with its current state).
        if not await wait_for_subscriber(self.status_pub, STATUS_PATH_TIMEOUT):
            log.warning("no status subscriber yet; 'joined' may not be seen")
        self._publish_status("joined")

    async def _on_session_ended(self, data: dict) -> None:
        await self._leave_session(data.get("reason"))

    async def _sensor_loop(self) -> None:
        sensor_topic = topic(self.robot_id, "sensor")
        while self.zsession is not None:
            self.zsession.put(sensor_topic, json.dumps({"state": self.sensor.read()}))
            await asyncio.sleep(SENSOR_INTERVAL)

    def _publish_status(self, event: str, **fields) -> None:
        # Called from Zenoh callback threads too, while _leave_session may be tearing down on the
        # main thread: take a local copy, and treat a publish during teardown as best effort.
        pub = self.status_pub
        if pub is None:
            return
        try:
            pub.put(status_msg(self.robot_id, event, **fields))
        except Exception as err:
            log.debug("status %r not sent (%s)", event, err)

    def _on_command(self, sample) -> None:
        # Runs on a Zenoh thread.
        # ASSUMPTION A6: commands come straight from the User (topic table); the Player doesn't relay them.
        raw = sample.payload.to_string()
        log.info("got %s: %s", sample.key_expr, raw)
        try:
            name = json.loads(raw)["command"]
            action = self.commands[name]
        except (json.JSONDecodeError, KeyError, TypeError):
            log.warning("rejected command %r", raw)
            self._publish_status("rejected_command", command=raw)
            return
        action()
        if name == "stop":
            self._publish_status("stopped")
        else:
            self._publish_status("moving", command=name)

    def _on_status(self, sample) -> None:
        raw = sample.payload.to_string()
        try:
            data = json.loads(raw)
            if data.get("from") == self.robot_id:
                return                      # don't print our own status messages
        except (json.JSONDecodeError, AttributeError):
            data = {}
        log.info("got %s: %s", sample.key_expr, raw)
        if data.get("status") == "joined":
            # Someone just joined: tell them our current state, since our earlier status
            # messages were sent before they could hear them.
            moving = self.jetbot.left_motor != 0.0 or self.jetbot.right_motor != 0.0
            self._publish_status("ready", motors="moving" if moving else "stopped")

    async def _leave_session(self, reason) -> None:
        # ASSUMPTION A7: safety first. Whenever the robot leaves a session or loses the Cloud,
        # it stops its motors, so it never keeps driving on a stale command.
        self.jetbot.stop()
        if self.zsession is None:
            return
        self._publish_status("leaving", reason=reason)
        if self.sensor_task:
            self.sensor_task.cancel()
        # Detach everything first, then release it. Each release is attempted even if another fails,
        # so a half-closed session can never block the next join or reconnect.
        subs, self.subs = self.subs, []
        pub, self.status_pub = self.status_pub, None
        z, self.zsession = self.zsession, None
        for release in [s.undeclare for s in subs] + ([pub.undeclare] if pub else []):
            try:
                release()
            except Exception:
                log.exception("cleanup step failed while leaving the session")
        try:
            # Closing a Zenoh session can block for seconds (about 2 s seen on Windows). Do it in a
            # worker thread so heartbeats and control messages keep flowing meanwhile.
            await asyncio.to_thread(z.close)
        except Exception:
            log.exception("cleanup step failed while leaving the session")
        log.info("left session (%s)", reason)

    # ------------------------------------------------------------------ shutdown
    async def shutdown(self) -> None:
        """Deliberate exit: say goodbye so the Cloud skips the reconnect grace period."""
        if self.ws is not None:
            try:
                await self.ws.send(msg("goodbye"))
                await self.ws.close()
            except ConnectionClosed:
                pass
        await self._leave_session("robot_shutdown")
