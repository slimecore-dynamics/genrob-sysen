"""Integration tests: real Cloud Service, Robot, User and Player processes over real (encrypted) connections.

Fixtures (cloud, make_robot, make_user, pki) are in conftest.py. Each test gets a fresh Cloud Service
on a free port and cleans up after itself. Timeouts are shortened per test with monkeypatch.
"""
import asyncio
import json
import logging
import os
import signal
import sys
import time

import pytest

import robot.client as robot_client
from cloud_service import server
from cloud_service.player_process import spawn_player
from cloud_service.server import CloudService
from common.config import cloud_url
from common.mesh import free_port, open_session, topic
from common.protocol import status_msg, wait_for_subscriber
from common.security import load_identity
from robot.client import RegistrationRefused, RobotClient
from robot.sensor import SineSensor
from tests.helpers import HOST, capture_messages, statuses, wait_until
from user import client as user_client
from user.client import ControlError, UserClient

HIGH = SineSensor(center=35.0, amplitude=0.0)     # always "high"
NORMAL = SineSensor(center=20.0, amplitude=0.0)   # always "normal"


# ================================================================ Functional Requirements 1-2
@pytest.mark.asyncio
async def test_robot_registers_and_is_listed(cloud, make_robot, make_user):
    await make_robot()
    user = await make_user()
    assert await user.list_robots() == [{"robot_id": "robot-1", "state": "online"}]


# ================================================================ Functional Requirements 3-5: the triangle
@pytest.mark.asyncio
async def test_full_triangle(cloud, make_robot, make_user):
    """The assignment's demonstration: sensor -> Player -> processed, User -> command -> Robot."""
    bot, _ = await make_robot()
    user = await make_user()
    received = capture_messages(user)
    await user.connect("robot-1")
    assert cloud.robots["robot-1"].state == "in_session"

    # Wait for both: the Robot's first reading can be dropped while it doesn't yet know of our
    # subscription, even though the Player (which knew of us sooner) relays its processed copy.
    await wait_until(lambda: {t.rsplit("/", 1)[1] for t, _ in received} >= {"sensor", "processed"})
    sensor = next(d for t, d in received if t.endswith("/sensor"))
    assert set(sensor) == {"state"}
    for t, d in received:
        if t.endswith("/processed"):
            assert d["status"] == ("high" if d["state"] > 30.0 else "normal")

    user.send_command("robot-1", "forward")
    await wait_until(lambda: bot.jetbot.left_motor == 0.5)
    user.send_command("robot-1", "stop")
    await wait_until(lambda: bot.jetbot.left_motor == 0.0)


@pytest.mark.asyncio
async def test_robot_and_user_are_linked_directly(cloud, make_robot, make_user):
    """The triangle's Robot-User side is a direct Zenoh link, not a relay through the Player."""
    bot, _ = await make_robot()
    user = await make_user()
    await user.connect("robot-1")
    user_zid = str(user.sessions["robot-1"].zsession.info.zid())
    await wait_until(lambda: user_zid in {str(z) for z in bot.zsession.info.peers_zid()})


@pytest.mark.asyncio
async def test_every_entity_publishes_and_hears_status(cloud, make_robot, make_user, caplog):
    """Topic table: status is 'Any -> All'. Robot, Player and User each publish, and each hears the others."""
    caplog.set_level(logging.INFO, logger="Player")
    bot, _ = await make_robot(sensor=HIGH)                  # a high reading makes the Player raise an alert
    robot_heard = []
    original = bot._on_status                               # record what the Robot hears, then let it react

    def recording_on_status(sample):
        robot_heard.append(sample.payload.to_string())
        original(sample)
    bot._on_status = recording_on_status

    user = await make_user()
    received = capture_messages(user)
    await user.connect("robot-1")
    user.send_status("robot-1", "operator here")

    # Robot and Player both announced themselves before the User joined; their replies to the
    # User's "joined" still give the User their current state.
    await wait_until(lambda: any(d["status"] == "ready" and d.get("motors") == "stopped"
                                 for d in statuses(received, "robot-1")))       # User hears the Robot
    await wait_until(lambda: any(d["status"] == "ready" and d.get("level") == "high"
                                 for d in statuses(received, "player")))        # User hears the Player
    await wait_until(lambda: any('"player"' in m for m in robot_heard))          # Robot hears the Player
    await wait_until(lambda: any("operator here" in m for m in robot_heard))     # Robot hears the User
    await wait_until(lambda: any("operator here" in r.getMessage() for r in caplog.records))  # Player hears the User
    await wait_until(lambda: any('"joined"' in r.getMessage() for r in caplog.records))       # Player hears the Robot


@pytest.mark.asyncio
async def test_player_never_claims_a_level_it_has_not_measured(pki):
    """Found on Windows: a User joined before the Player's first sensor reading, and the Player's
    "ready" reply said level "normal" with no data behind it. This forces that order every time:
    a User joins a fresh Player, and only then does the first reading (35.0, high) arrive."""
    player = spawn_player("robot-1", HOST, pki)
    user = open_session(HOST, load_identity(pki, "users", "user"), [player.endpoint])
    robot = open_session(HOST, load_identity(pki, "robots", "robot-1"), [player.endpoint])
    heard = []
    sub = user.declare_subscriber(topic("robot-1", "status"),
                                  lambda s: heard.append(json.loads(s.payload.to_string())))
    status_pub = user.declare_publisher(topic("robot-1", "status"))
    sensor_pub = robot.declare_publisher(topic("robot-1", "sensor"))
    try:
        assert await wait_for_subscriber(status_pub, 5)
        status_pub.put(status_msg("user", "joined"))                    # join before any reading
        await asyncio.sleep(1.0)
        assert not any(d.get("level") for d in heard)                   # no level claimed without data

        assert await wait_for_subscriber(sensor_pub, 5)
        sensor_pub.put(json.dumps({"state": 35.0}))                     # first reading: high
        await wait_until(lambda: any(d["status"] == "ready" for d in heard))
        assert {"from": "player", "status": "ready", "level": "high"} in heard   # the held reply
        assert any(d["status"] == "alert" and d["level"] == "high" for d in heard)
    finally:
        for p in (sub, status_pub, sensor_pub):
            p.undeclare()
        user.close()
        robot.close()
        player.stop()


@pytest.mark.asyncio
async def test_player_alerts_when_level_changes(cloud, make_robot, make_user):
    # Center 30, amplitude 5, period 4 s, one reading per second: the reading crosses 30 in both
    # directions every cycle. Which crossing comes first depends on where in the wave the robot starts.
    await make_robot(sensor=SineSensor(center=30.0, amplitude=5.0, period=4.0))
    user = await make_user()
    received = capture_messages(user)
    await user.connect("robot-1")
    await wait_until(lambda: {d["level"] for d in statuses(received, "player") if d["status"] == "alert"}
                     == {"high", "normal"}, timeout=12)


@pytest.mark.asyncio
async def test_robot_reports_commands_on_status(cloud, make_robot, make_user):
    await make_robot()
    user = await make_user()
    received = capture_messages(user)
    await user.connect("robot-1")
    for command in ("forward", "stop", "fly"):
        user.send_command("robot-1", command)
    # Only the command reports, in order. (The Robot's "ready" reply to the User's "joined" can land
    # anywhere among them: status and commands arrive on different topics, handled on separate threads.)
    command_reports = ("moving", "stopped", "rejected_command")
    await wait_until(lambda: [d["status"] for d in statuses(received, "robot-1")
                              if d["status"] in command_reports] == list(command_reports))


@pytest.mark.asyncio
async def test_player_prints_what_it_receives(cloud, make_robot, make_user, caplog):
    """'Each entity should print all data it receives': the Player's output reaches the Cloud's log."""
    caplog.set_level(logging.INFO, logger="Player")
    await make_robot()
    user = await make_user()
    await user.connect("robot-1")
    await wait_until(lambda: any("got robot/robot-1/sensor" in r.getMessage() for r in caplog.records))


@pytest.mark.asyncio
async def test_first_command_after_connect_is_not_lost(cloud, make_robot, make_user):
    """Regression test. A command sent the instant connect() returned was sometimes dropped:
    Zenoh discards a put when the sender doesn't yet know of a matching subscriber.
    connect() now waits for the Robot's command subscription before returning."""
    bot, _ = await make_robot()
    user = await make_user()
    for _ in range(5):                        # several fresh sessions, to give the race a chance to show
        await user.connect("robot-1")
        user.send_command("robot-1", "forward")      # immediately, no waiting
        await wait_until(lambda: bot.jetbot.left_motor == 0.5, timeout=3)
        user.send_command("robot-1", "stop")
        await wait_until(lambda: bot.jetbot.left_motor == 0.0, timeout=3)
        await user.disconnect("robot-1")
        await wait_until(lambda: cloud.robots["robot-1"].session is None)


# ================================================================ multiple robots, multiple users (Bonus 2)
@pytest.mark.asyncio
async def test_one_user_controls_several_robots(cloud, make_robot, make_user):
    r1, _ = await make_robot("robot-1")
    r2, _ = await make_robot("robot-2")
    user = await make_user()
    await user.connect("robot-1")
    await user.connect("robot-2")
    assert set(user.sessions) == {"robot-1", "robot-2"}

    user.send_command("robot-2", "forward")                       # one robot by name
    await wait_until(lambda: r2.jetbot.left_motor == 0.5)
    await asyncio.sleep(0.3)
    assert r1.jetbot.left_motor == 0.0                            # the other didn't move

    user.send_command("all", "backward")                          # every connected robot
    await wait_until(lambda: r1.jetbot.left_motor == -0.5 and r2.jetbot.left_motor == -0.5)


@pytest.mark.asyncio
async def test_two_users_two_robots_are_isolated(cloud, make_robot, make_user):
    r1, _ = await make_robot("robot-1")
    r2, _ = await make_robot("robot-2")
    first, second = await make_user("first"), await make_user("second")
    seen_first, seen_second = capture_messages(first), capture_messages(second)
    await first.connect("robot-1")
    await second.connect("robot-2")

    await wait_until(lambda: seen_first and seen_second)
    await asyncio.sleep(1.5)
    assert all(t.startswith("robot/robot-1/") for t, _ in seen_first)
    assert all(t.startswith("robot/robot-2/") for t, _ in seen_second)

    first.send_command("robot-1", "forward")
    await wait_until(lambda: r1.jetbot.left_motor == 0.5)
    await asyncio.sleep(0.3)
    assert r2.jetbot.left_motor == 0.0


# ================================================================ edge cases named in the assignment
@pytest.mark.asyncio
async def test_robot_not_found(cloud, make_user):
    user = await make_user()
    with pytest.raises(ControlError, match="robot_not_found"):
        await user.connect("robot-99")


@pytest.mark.asyncio
async def test_duplicate_registration_refused(cloud, make_robot, pki):
    await make_robot("robot-1")
    second = RobotClient("robot-1", cloud_url(HOST, cloud.port), HOST, pki)   # same id, valid token
    with pytest.raises(RegistrationRefused, match="duplicate_registration"):
        await second._connect_once()


@pytest.mark.asyncio
async def test_player_spawn_failure(cloud, make_robot, make_user):
    cloud.player_module = "no_such_player_module"   # the spawned process exits immediately
    await make_robot()
    user = await make_user()
    with pytest.raises(ControlError, match="player_spawn_failed"):
        await user.connect("robot-1")
    assert cloud.robots["robot-1"].state == "online"   # free to try again

@pytest.mark.asyncio
async def test_player_exiting_mid_session_ends_the_session(cloud, make_robot, make_user):
    bot, _ = await make_robot()
    user = await make_user()
    await user.connect("robot-1")
    cloud.robots["robot-1"].session.player.proc.kill()
    await wait_until(lambda: cloud.robots["robot-1"].state == "online")
    await wait_until(lambda: "robot-1" not in user.sessions and bot.zsession is None)
    await user.connect("robot-1")                       # a new session can start
    

# ================================================================ other failure handling
@pytest.mark.asyncio
async def test_second_user_gets_robot_busy(cloud, make_robot, make_user):
    await make_robot()
    first, second = await make_user("first"), await make_user("second")
    await first.connect("robot-1")
    with pytest.raises(ControlError, match="robot_busy"):
        await second.connect("robot-1")


@pytest.mark.asyncio
async def test_bad_request_is_refused(cloud, make_user):
    user = await make_user()
    with pytest.raises(ControlError, match="bad_request"):
        await user._request("make_coffee")
    assert await user.list_robots() == []          # the connection is still usable


@pytest.mark.asyncio
async def test_user_leaving_ends_session_and_stops_player(cloud, make_robot, make_user):
    bot, _ = await make_robot()
    user = await make_user()
    await user.connect("robot-1")
    player_proc = cloud.robots["robot-1"].session.player.proc
    await user.shutdown()
    await wait_until(lambda: cloud.robots["robot-1"].session is None)
    await wait_until(lambda: player_proc.poll() is not None)        # Player process exited
    await wait_until(lambda: bot.zsession is None)                  # Robot left the triangle
    assert cloud.robots["robot-1"].state == "online"


@pytest.mark.asyncio
async def test_heartbeat_timeout_ends_session(cloud, make_robot, make_user, monkeypatch):
    bot, _ = await make_robot()
    user = await make_user()
    await user.connect("robot-1")
    user.send_command("robot-1", "forward")
    await wait_until(lambda: bot.jetbot.left_motor == 0.5)          # session is up and the robot is moving
    # Only now "freeze" the robot, so the timeout can't fire during setup.
    monkeypatch.setattr(robot_client, "HEARTBEAT_INTERVAL", 60)    # Robot now beats once a minute...
    monkeypatch.setattr(server, "HEARTBEAT_TIMEOUT", 0.5)          # ...and the Cloud gives up after 0.5 s
    await wait_until(lambda: "robot-1" not in user.sessions)       # User was told the session ended
    await wait_until(lambda: bot.jetbot.left_motor == 0.0)         # Robot stopped its motors


@pytest.mark.asyncio
async def test_drop_and_reconnect_resumes_same_session(cloud, make_robot, make_user):
    bot, _ = await make_robot()
    user = await make_user()
    await user.connect("robot-1")
    session_id = cloud.robots["robot-1"].session.session_id
    await bot.ws.close()                     # connection drops without a goodbye; run() reconnects on its own
    await wait_until(lambda: cloud.robots["robot-1"].state == "reconnecting")
    await wait_until(lambda: cloud.robots["robot-1"].state == "in_session")
    assert cloud.robots["robot-1"].session.session_id == session_id   # same session, same Player
    await wait_until(lambda: bot.zsession is not None)


@pytest.mark.asyncio
async def test_drop_without_return_ends_session(cloud, make_robot, make_user, monkeypatch):
    monkeypatch.setattr(server, "RECONNECT_GRACE", 0.5)
    bot, task = await make_robot()
    user = await make_user()
    await user.connect("robot-1")
    task.cancel()                            # the robot stops trying...
    await bot.ws.close()                     # ...and its connection drops, with no goodbye
    await wait_until(lambda: cloud.robots["robot-1"].state == "offline")
    await wait_until(lambda: "robot-1" not in user.sessions)


@pytest.mark.asyncio
async def test_connect_while_robot_reconnecting(cloud, make_robot, make_user):
    bot, task = await make_robot()
    task.cancel()
    await bot.ws.close()
    await wait_until(lambda: cloud.robots["robot-1"].state == "reconnecting")
    user = await make_user()
    with pytest.raises(ControlError, match="robot_reconnecting"):
        await user.connect("robot-1")


@pytest.mark.asyncio
async def test_robot_ctrl_c_ends_session_with_robot_shutdown(cloud, make_robot, make_user):
    bot, task = await make_robot()
    user = await make_user()
    notices = []
    user.on_notice = notices.append
    received = capture_messages(user)
    await user.connect("robot-1")
    task.cancel()                            # what Ctrl+C does to the Robot's main task
    await bot.shutdown()
    await wait_until(lambda: "session with robot-1 ended: robot_offline:robot_shutdown" in notices, timeout=2)
    leaving = [d for d in statuses(received, "robot-1") if d["status"] == "leaving"]
    assert leaving and leaving[0]["reason"] == "robot_shutdown"


@pytest.mark.asyncio
async def test_user_survives_cloud_restart(pki):
    """The Cloud Service goes away while a User is in a session: the User keeps running, says so,
    refuses requests clearly while offline, and reconnects by itself when the Cloud is back."""
    port = free_port(HOST)
    first = CloudService(HOST, port, pki)
    await first.start()
    bot = RobotClient("robot-1", cloud_url(HOST, port), HOST, pki)
    bot_task = asyncio.create_task(bot.run())
    user = UserClient("user", cloud_url(HOST, port), HOST, pki)
    notices = []
    user.on_notice = notices.append
    second = None
    try:
        await wait_until(lambda: "robot-1" in first.robots)
        await user.start()
        await user.connect("robot-1")
        await first.stop()
        await wait_until(lambda: "lost the Cloud Service; reconnecting" in notices)
        assert "session with robot-1 ended: cloud_shutdown" in notices
        assert user.sessions == {} and not user.online
        with pytest.raises(ControlError, match="not connected"):
            await user.list_robots()

        second = CloudService(HOST, port, pki)
        await second.start()
        await wait_until(lambda: user.online, timeout=10)
        assert "reconnected to the Cloud Service" in notices
        await wait_until(lambda: second.robots.get("robot-1") is not None
                         and second.robots["robot-1"].state == "online", timeout=10)
        await user.connect("robot-1")        # and it works again
        user.send_command("robot-1", "forward")
        await wait_until(lambda: bot.jetbot.left_motor > 0)
    finally:
        await user.shutdown()
        bot_task.cancel()
        await bot.shutdown()
        if second is not None:
            await second.stop()


@pytest.mark.skipif(sys.platform == "win32", reason="sends a POSIX SIGINT; Windows uses a new process group instead")
def test_ctrl_c_does_not_reach_the_player(pki):
    """Ctrl+C in the Cloud Service's terminal must not interrupt its Players: the Cloud stops them."""
    player = spawn_player("robot-1", HOST, pki)
    try:
        assert os.getpgid(player.proc.pid) != os.getpgid(0)   # its own process group
        os.kill(player.proc.pid, signal.SIGINT)               # even sent straight to it...
        time.sleep(0.5)
        assert player.proc.poll() is None                     # ...it keeps running
    finally:
        player.stop()
    assert player.proc.returncode == 0                        # and still stops cleanly when told to


class SlowClose:
    """A Zenoh session whose close() takes `delay` seconds, like the ~2 s closes seen on Windows."""
    def __init__(self, session, delay):
        self._session, self._delay = session, delay

    def close(self):
        time.sleep(self._delay)
        self._session.close()

    def __getattr__(self, name):
        return getattr(self._session, name)


@pytest.mark.asyncio
async def test_slow_zenoh_close_does_not_freeze_the_robot(cloud, make_robot, make_user, monkeypatch):
    """Closing a Zenoh session can block for seconds (seen on Windows). It must not stop the Robot's
    event loop: heartbeats, the goodbye and everything else keep running while it closes."""
    real_open = robot_client.open_session
    monkeypatch.setattr(robot_client, "open_session", lambda *a: SlowClose(real_open(*a), 3.0))
    bot, task = await make_robot()
    user = await make_user()
    notices = []
    user.on_notice = notices.append
    await user.connect("robot-1")
    task.cancel()
    stopping = asyncio.create_task(bot.shutdown())             # takes 3 s to close Zenoh
    # Meanwhile the goodbye still reaches the Cloud, and the User hears about it, right away.
    await wait_until(lambda: "session with robot-1 ended: robot_offline:robot_shutdown" in notices, timeout=1.0)
    assert not stopping.done()                                 # (the close really was still going on)
    await stopping


@pytest.mark.asyncio
async def test_slow_zenoh_close_does_not_freeze_the_user(cloud, make_robot, make_user, monkeypatch):
    real_open = user_client.open_session
    monkeypatch.setattr(user_client, "open_session", lambda *a: SlowClose(real_open(*a), 3.0))
    await make_robot()
    user = await make_user()
    await user.connect("robot-1")
    closing = asyncio.create_task(user.disconnect("robot-1"))   # takes 3 s to close
    await asyncio.sleep(0.1)
    started = time.monotonic()
    assert await user.list_robots()            # meanwhile, the User still answers right away
    assert time.monotonic() - started < 1.0
    assert not closing.done()                  # (the close really was still going on)
    await closing


@pytest.mark.asyncio
async def test_user_waits_for_leaving_that_arrives_after_session_ended(cloud, make_robot, make_user):
    """"leaving" (over Zenoh) and session_ended (over the Cloud's WebSocket) are separate connections,
    so "leaving" can arrive second. Force that by delaying it, and check it isn't lost."""
    bot, task = await make_robot()
    user = await make_user()
    order = []
    user.on_notice = lambda text: order.append("notice")
    received = capture_messages(user)
    real_dispatch = user._dispatch
    def slow_dispatch(session, sample):
        if '"leaving"' in sample.payload.to_string():
            time.sleep(0.3)                            # this one arrives late
            order.append("leaving")
        real_dispatch(session, sample)
    user._dispatch = slow_dispatch
    await user.connect("robot-1")
    task.cancel()
    await bot.shutdown()
    await wait_until(lambda: "notice" in order, timeout=3)
    assert order.index("leaving") < order.index("notice")
    assert [d for d in statuses(received, "robot-1") if d["status"] == "leaving"]


@pytest.mark.asyncio
async def test_own_subscriber_does_not_count_as_a_listener(pki):
    """The Robot and User subscribe and publish on the same status topic. Their own subscriber must
    not satisfy wait_for_subscriber, or they publish before anyone else can hear (found on Windows)."""
    session = open_session(HOST, load_identity(pki, "users", "user"), [])      # no other peers
    sub = session.declare_subscriber(topic("robot-1", "status"), lambda s: None)
    pub = session.declare_publisher(topic("robot-1", "status"))
    try:
        assert not await wait_for_subscriber(pub, 0.5)
    finally:
        sub.undeclare()
        pub.undeclare()
        session.close()


# ================================================================ connect and Player-start failure
def slow_spawn(real_spawn, delay):
    """A spawn_player that takes `delay` seconds longer, to open a window while the Player starts."""
    def spawn(*args):
        time.sleep(delay)
        return real_spawn(*args)
    return spawn


@pytest.mark.asyncio
async def test_failed_connect_ends_the_cloud_session(cloud, make_robot, make_user, monkeypatch):
    """connect() fails with robot_unreachable: the Cloud's session and Player end too, so a retry works."""
    monkeypatch.setattr(user_client, "COMMAND_PATH_TIMEOUT", 1.0)
    original = RobotClient._on_join_session
    calls = []

    async def join_second_time_only(self, data):          # the first join_session is ignored
        calls.append(data)
        if len(calls) > 1:
            await original(self, data)
    monkeypatch.setattr(RobotClient, "_on_join_session", join_second_time_only)

    await make_robot()
    user = await make_user()
    with pytest.raises(ControlError, match="robot_unreachable"):
        await user.connect("robot-1")
    await wait_until(lambda: cloud.robots["robot-1"].session is None)
    assert cloud.robots["robot-1"].state == "online"
    await user.connect("robot-1")                         # the retry is accepted and works


@pytest.mark.asyncio
async def test_robot_dropping_during_player_start_leaves_the_user_connected(cloud, make_robot, make_user,
                                                                            monkeypatch):
    """The robot's socket closes while its Player starts. That must not end the User's connection."""
    monkeypatch.setattr(user_client, "COMMAND_PATH_TIMEOUT", 1.0)
    monkeypatch.setattr(server, "spawn_player", slow_spawn(server.spawn_player, 1.0))
    _, task = await make_robot()
    user = await make_user()
    connecting = asyncio.create_task(user.connect("robot-1"))
    await asyncio.sleep(0.3)
    task.cancel()                                         # the robot is gone, without a goodbye
    await cloud.robots["robot-1"].ws.close()
    with pytest.raises(ControlError, match="robot_unreachable"):
        await connecting
    assert user.online                                    # still connected to the Cloud Service
    await wait_until(lambda: cloud.robots["robot-1"].session is None)   # and the Player was stopped


@pytest.mark.asyncio
async def test_connect_that_times_out_does_not_leave_a_player_running(cloud, make_robot, make_user,
                                                                      monkeypatch):
    """The User gives up waiting (request_timeout); the session that succeeds afterwards is ended."""
    monkeypatch.setattr(user_client, "REQUEST_TIMEOUT", 0.3)
    monkeypatch.setattr(server, "spawn_player", slow_spawn(server.spawn_player, 1.0))
    await make_robot()
    user = await make_user()
    with pytest.raises(ControlError, match="request_timeout"):
        await user.connect("robot-1")
    rec = cloud.robots["robot-1"]
    await wait_until(lambda: rec.session is None and rec.state == "online")

@pytest.mark.asyncio
async def test_robot_is_starting_while_its_player_spawns(cloud, make_robot, make_user, monkeypatch):
    """While the Player starts, the robot is "starting": claimed, so a second User gets robot_busy."""
    monkeypatch.setattr(server, "spawn_player", slow_spawn(server.spawn_player, 1.0))
    await make_robot()
    first, second = await make_user("first"), await make_user("second")
    connecting = asyncio.create_task(first.connect("robot-1"))
    await wait_until(lambda: cloud.robots["robot-1"].state == "starting")
    assert {"robot_id": "robot-1", "state": "starting"} in await second.list_robots()
    with pytest.raises(ControlError, match="robot_busy"):
        await second.connect("robot-1")
    await connecting
    assert cloud.robots["robot-1"].state == "in_session"
