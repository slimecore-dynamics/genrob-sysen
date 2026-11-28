"""Security tests (Bonus 1) and flooding limits. Each one tries an attack and checks it fails.
"""
import asyncio
import json
import ssl
import stat
import sys

import pytest
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed, InvalidMessage

from cloud_service import server
from common.config import cloud_url
from common.mesh import open_session, topic
from common.pki import make_ca, make_cert
from common.protocol import msg
from common.security import Identity, client_tls_context, load_identity
from robot.client import RegistrationRefused, RobotClient
from tests.helpers import HOST, wait_until
from tools.setup_security import setup
from user.client import ControlError, UserClient


# ---------------------------------------------------------------- tokens on the control plane
@pytest.mark.asyncio
async def test_robot_with_wrong_token_is_refused(cloud, pki):
    bot = RobotClient("robot-1", cloud_url(HOST, cloud.port), HOST, pki)
    bot.token = "guessed-token"
    with pytest.raises(RegistrationRefused, match="unauthorized"):
        await bot._connect_once()
    assert "robot-1" not in cloud.robots


@pytest.mark.asyncio
async def test_robot_cannot_use_another_robots_token(cloud, pki):
    bot = RobotClient("robot-2", cloud_url(HOST, cloud.port), HOST, pki)
    bot.robot_id = "robot-1"                      # robot-2's token, claiming to be robot-1
    with pytest.raises(RegistrationRefused, match="unauthorized"):
        await bot._connect_once()


@pytest.mark.asyncio
async def test_user_with_wrong_token_is_refused(cloud, pki):
    user = UserClient("user", cloud_url(HOST, cloud.port), HOST, pki)
    user.token = "guessed-token"
    with pytest.raises(ControlError, match="unauthorized"):
        await user.start()


@pytest.mark.asyncio
async def test_identity_added_while_cloud_runs_can_log_in(cloud, pki):
    """setup_security run while the Cloud Service is up: the new user is accepted without a restart."""
    setup(pki, users=["late-user"])
    user = UserClient("late-user", cloud_url(HOST, cloud.port), HOST, pki)
    await user.start()
    try:
        assert await user.list_robots() == []
    finally:
        await user.shutdown()


@pytest.mark.asyncio
async def test_impostor_cannot_take_over_during_reconnect_grace(cloud, make_robot, make_user, pki):
    """A robot that drops is held for RECONNECT_GRACE. Resuming needs its token."""
    bot, task = await make_robot("robot-1")
    user = await make_user()
    await user.connect("robot-1")
    session_id = cloud.robots["robot-1"].session.session_id
    task.cancel()
    await bot.ws.close()                                         # real robot drops
    await wait_until(lambda: cloud.robots["robot-1"].state == "reconnecting")

    impostor = RobotClient("robot-1", cloud_url(HOST, cloud.port), HOST, pki)
    impostor.token = "guessed-token"
    with pytest.raises(RegistrationRefused, match="unauthorized"):
        await impostor._connect_once()
    assert cloud.robots["robot-1"].state == "reconnecting"     # still waiting for the real one

    real_again = RobotClient("robot-1", cloud_url(HOST, cloud.port), HOST, pki)
    task2 = asyncio.create_task(real_again.run())
    try:
        await wait_until(lambda: cloud.robots["robot-1"].state == "in_session")
        assert cloud.robots["robot-1"].session.session_id == session_id
    finally:
        task2.cancel()
        await real_again.shutdown()


# ---------------------------------------------------------------- TLS on the control plane
@pytest.mark.asyncio
async def test_plain_websocket_is_rejected(cloud):
    with pytest.raises((InvalidMessage, ConnectionClosed, OSError)):
        async with ws_connect(f"ws://{HOST}:{cloud.port}/robot", open_timeout=3):
            pass


@pytest.mark.asyncio
async def test_client_refuses_a_server_it_does_not_trust(cloud):
    """A client that only trusts the system's normal CAs won't talk to ours, and ours won't talk to an impostor."""
    with pytest.raises(ssl.SSLCertVerificationError):
        async with ws_connect(f"wss://{HOST}:{cloud.port}/robot", ssl=ssl.create_default_context()):
            pass


# ---------------------------------------------------------------- mutual TLS on the data plane
@pytest.mark.asyncio
async def test_untrusted_zenoh_peer_cannot_join_or_command(cloud, make_robot, make_user, tmp_path):
    bot, _ = await make_robot()
    user = await make_user()
    await user.connect("robot-1")
    player_ep = cloud.robots["robot-1"].session.player.endpoint

    attacker_ca = make_ca(tmp_path / "attacker_ca", name="Attacker CA")
    fake = make_cert(attacker_ca, tmp_path / "fake-user", "user")   # claims to be "user"
    attacker = open_session(HOST, Identity(fake / "cert.pem", fake / "key.pem", attacker_ca / "cert.pem"),
                            [player_ep])
    try:
        await asyncio.sleep(1.5)
        attacker.put(topic("robot-1", "command"), json.dumps({"command": "forward"}))
        await asyncio.sleep(0.5)
        assert len(attacker.info.peers_zid()) == 0              # nobody accepted it
        assert bot.jetbot.left_motor == 0.0                     # the robot didn't move
    finally:
        attacker.close()


@pytest.mark.asyncio
async def test_unencrypted_zenoh_peer_cannot_join(cloud, make_robot, make_user):
    await make_robot()
    user = await make_user()
    await user.connect("robot-1")
    player_ep = cloud.robots["robot-1"].session.player.endpoint
    plain = open_session(HOST, None, [player_ep.replace("tls/", "tcp/")])
    try:
        await asyncio.sleep(1.5)
        assert len(plain.info.peers_zid()) == 0
    finally:
        plain.close()


# ---------------------------------------------------------------- flooding limits
@pytest.mark.asyncio
async def test_silent_connection_is_closed(cloud, pki, monkeypatch):
    monkeypatch.setattr(server, "FIRST_MESSAGE_TIMEOUT", 0.3)
    async with ws_connect(f"wss://{HOST}:{cloud.port}/robot", ssl=client_tls_context(pki / "ca" / "cert.pem")) as ws:
        with pytest.raises(ConnectionClosed) as closed:
            await asyncio.wait_for(ws.recv(), 3)                # we never send anything
    assert closed.value.rcvd.code == 1008


@pytest.mark.asyncio
async def test_connection_cap(cloud, pki, monkeypatch):
    monkeypatch.setattr(server, "MAX_CONNECTIONS", 2)
    ctx = client_tls_context(pki / "ca" / "cert.pem")
    url = f"wss://{HOST}:{cloud.port}/robot"
    first, second = await ws_connect(url, ssl=ctx), await ws_connect(url, ssl=ctx)   # two silent connections
    try:
        await wait_until(lambda: cloud.open_connections == 2)
        async with ws_connect(url, ssl=ctx) as third:
            with pytest.raises(ConnectionClosed) as closed:
                await asyncio.wait_for(third.recv(), 3)
        assert closed.value.rcvd.code == 1013                    # "try again later"
    finally:
        await first.close()
        await second.close()
    await wait_until(lambda: cloud.open_connections == 0)       # slots are released


@pytest.mark.asyncio
async def test_register_message_must_come_first(cloud, pki):
    async with ws_connect(f"wss://{HOST}:{cloud.port}/robot", ssl=client_tls_context(pki / "ca" / "cert.pem")) as ws:
        await ws.send(msg("heartbeat"))
        reply = json.loads(await ws.recv())
    assert reply == {"type": "error", "reason": "must_register_first"}


@pytest.mark.skipif(sys.platform == "win32", reason="Windows ignores file modes; files inherit the folder's permissions")
def test_private_keys_and_tokens_are_owner_only(pki):
    for path in [pki / "ca" / "key.pem", pki / "robots" / "robot-1" / "key.pem",
                 pki / "robots" / "robot-1" / "token"]:
        assert stat.S_IMODE(path.stat().st_mode) == 0o600, path


@pytest.mark.parametrize("name", ["robot/1", "robot-*", "../cloud"])   # topic level, wildcard, path
def test_names_that_could_escape_a_folder_or_topic_are_refused(pki, name):
    with pytest.raises(ValueError):
        setup(pki, robots=[name])


@pytest.mark.asyncio
async def test_another_robots_certificate_cannot_command_this_robot(cloud, make_robot, make_user, pki):
    """Authorization, not just authentication: these certificates are genuine (signed by our CA),
    so mutual TLS lets them in, but the Robot's ACL accepts commands only from the session's User.
    One of them is a robot named "user", like the session's User: certificate names carry their
    kind (common.security.cert_name), so sharing a name grants nothing."""
    setup(pki, robots=["user"])
    bot, _ = await make_robot()
    user = await make_user()
    await user.connect("robot-1")
    player_ep = cloud.robots["robot-1"].session.player.endpoint
    robot_zid = bot.zsession.info.zid()
    for attacker in ("robot-2", "user"):
        other = open_session(HOST, load_identity(pki, "robots", attacker), [player_ep])
        try:
            await wait_until(lambda o=other: robot_zid in list(o.info.peers_zid()))   # linked to the robot...
            other.put(topic("robot-1", "command"), json.dumps({"command": "forward"}))
            await asyncio.sleep(0.5)
            assert bot.jetbot.left_motor == 0.0, attacker                          # ...but not obeyed
        finally:
            other.close()
    user.send_command("robot-1", "forward")                                    # the session's User is
    await wait_until(lambda: bot.jetbot.left_motor > 0)
