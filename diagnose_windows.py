"""Windows diagnostics for two open questions. Not part of the repo: run it from the repo folder,
    py diagnose_windows.py
and paste the whole output back. Takes about a minute. Uses throwaway certificates (like the
benchmark), so it doesn't touch your pki/ folder.

Part 1: after wait_for_subscriber() returns, does a single message get through?
        Tried 100 times with the current wait (Zenoh notification) and 100 times with the old
        wait (check every 10 ms). If the current wait loses messages and the old one doesn't,
        that explains the failing test, which publishes its sensor reading exactly once.

Part 2: where do the ~250 ms of "time to session" go on this machine?
        10 connects, each split into steps.

Part 3: the Robot and User only find each other through the Player ("gossip"), then open a
        direct link. How long does that take here, and how long until the User can see the
        Robot's command subscription? 20 tries.
"""
import asyncio
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path

import cloud_service.server as server
import user.client as user_client
from cloud_service.player_process import spawn_player
from cloud_service.server import CloudService
from common.config import cloud_url
from common.mesh import free_port, open_session
from common.protocol import wait_for_subscriber
from common.security import load_identity
from robot.client import RobotClient
from tools.setup_security import setup
from user.client import UserClient

HOST = "127.0.0.1"


async def wait_polling(publisher, timeout: float) -> bool:
    """The old wait_for_subscriber: check every 10 ms."""
    deadline = time.monotonic() + timeout
    while not publisher.matching_status.matching:
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(0.01)
    return True


async def single_put_trials(pki: Path, waiter, trials: int) -> tuple[int, int]:
    player = spawn_player("robot-1", HOST, pki)
    lost = timeouts = 0
    try:
        for i in range(trials):
            a = open_session(HOST, load_identity(pki, "robots", "robot-1"), [player.endpoint])
            b = open_session(HOST, load_identity(pki, "users", "user"), [player.endpoint])
            got = threading.Event()
            pub = a.declare_publisher(f"diag/{i}")
            sub = b.declare_subscriber(f"diag/{i}", lambda s, got=got: got.set())
            if not await waiter(pub, 5):
                timeouts += 1
            pub.put("x")
            if not await asyncio.to_thread(got.wait, 2):
                lost += 1
            sub.undeclare()
            pub.undeclare()
            a.close()
            b.close()
    finally:
        player.stop()
    return lost, timeouts


async def part1(pki: Path) -> None:
    print("Part 1: one message published right after the subscriber wait returns (100 tries each)")
    for name, waiter in (("current wait (notification)", wait_for_subscriber),
                         ("old wait (every 10 ms)", wait_polling)):
        lost, timeouts = await single_put_trials(pki, waiter, 100)
        print(f"  {name:30} lost {lost:3} of 100   (wait timed out {timeouts} times)")


async def part2(pki: Path) -> None:
    print("\nPart 2: time to session, split into steps (10 connects; median, then max, in ms)")
    steps: dict[str, list[float]] = {}
    current: dict[str, float] = {}
    joined_sends = []

    def timed(name, fn):
        def wrapper(*args, **kwargs):
            t = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                current[name] = current.get(name, 0.0) + (time.perf_counter() - t) * 1000
        return wrapper

    def timed_async(name, fn):
        async def wrapper(*args, **kwargs):
            t = time.perf_counter()
            try:
                return await fn(*args, **kwargs)
            finally:
                current[name] = current.get(name, 0.0) + (time.perf_counter() - t) * 1000
        return wrapper

    real_status_msg = user_client.status_msg

    def counting_status_msg(sender, event, **fields):
        if event == "joined":
            current["joined sent"] = current.get("joined sent", 0) + 1
        return real_status_msg(sender, event, **fields)

    server.spawn_player = timed("  Cloud: start the Player", server.spawn_player)
    user_client.open_session = timed("User: open Zenoh session", user_client.open_session)
    user_client.wait_for_subscriber = timed_async("User: wait for Robot's subscriptions",
                                                  user_client.wait_for_subscriber)
    user_client.status_msg = counting_status_msg
    real_request = UserClient._request
    UserClient._request = timed_async("User: Cloud request (incl. Player start)", real_request)

    cloud = CloudService(HOST, free_port(HOST), pki)
    await cloud.start()
    url = cloud_url(HOST, cloud.port)
    bot = RobotClient("robot-1", url, HOST, pki)
    bot_task = asyncio.create_task(bot.run())
    while "robot-1" not in cloud.robots:
        await asyncio.sleep(0.05)
    user = UserClient("user", url, HOST, pki)
    await user.start()
    try:
        for _ in range(10):
            current.clear()
            t0 = time.perf_counter()
            await user.connect("robot-1")
            total = (time.perf_counter() - t0) * 1000
            joined_sends.append(current.pop("joined sent", 0))
            known = sum(v for k, v in current.items() if not k.startswith("  "))
            current["User: wait for Robot's 'ready' (the rest)"] = total - known
            current["TOTAL"] = total
            for k, v in current.items():
                steps.setdefault(k, []).append(v)
            await user.disconnect("robot-1")
            while cloud.robots["robot-1"].session is not None:
                await asyncio.sleep(0.05)
    finally:
        await user.shutdown()
        bot_task.cancel()
        await bot.shutdown()
        await cloud.stop()

    order = ["User: Cloud request (incl. Player start)", "  Cloud: start the Player",
             "User: open Zenoh session", "User: wait for Robot's subscriptions",
             "User: wait for Robot's 'ready' (the rest)", "TOTAL"]
    for k in order:
        v = steps.get(k, [0.0])
        print(f"  {k:45} {statistics.median(v):8.1f} {max(v):8.1f}")
    print(f"  'joined' sent per connect: {joined_sends}   (more than 1 means the first was lost "
          f"and a 0.25 s retry happened)")


async def part3(pki: Path) -> None:
    print("\nPart 3: User connects; time until the direct Robot-User link and the Robot's subscription"
          " (20 tries; median, then max, in ms)")
    player = spawn_player("robot-1", HOST, pki)
    links, subs = [], []
    try:
        for _ in range(20):
            robot = open_session(HOST, load_identity(pki, "robots", "robot-1"), [player.endpoint])
            cmd_sub = robot.declare_subscriber("robot/robot-1/command", lambda s: None)
            robot_zid = str(robot.info.zid())
            await asyncio.sleep(0.2)                        # Robot settled, like in a real session
            t0 = time.perf_counter()
            user = open_session(HOST, load_identity(pki, "users", "user"), [player.endpoint])
            pub = user.declare_publisher("robot/robot-1/command")
            link = sub_seen = None
            while (link is None or sub_seen is None) and time.perf_counter() - t0 < 5:
                now = (time.perf_counter() - t0) * 1000
                if link is None and robot_zid in {str(z) for z in user.info.peers_zid()}:
                    link = now
                if sub_seen is None and pub.matching_status.matching:
                    sub_seen = now
                await asyncio.sleep(0.001)
            links.append(link if link is not None else 5000.0)
            subs.append(sub_seen if sub_seen is not None else 5000.0)
            pub.undeclare()
            user.close()
            cmd_sub.undeclare()
            robot.close()
    finally:
        player.stop()
    print(f"  {'direct Robot-User link appears':45} {statistics.median(links):8.1f} {max(links):8.1f}")
    print(f"  {'User sees Robot command subscription':45} {statistics.median(subs):8.1f} {max(subs):8.1f}")
    print(f"  all link times: {[round(x) for x in links]}")


def main() -> None:
    print(f"Python {sys.version.split()[0]} on {sys.platform}\n")
    with tempfile.TemporaryDirectory() as tmp:
        pki = Path(tmp) / "pki"
        setup(pki, robots=["robot-1"], users=["user"])
        print()
        asyncio.run(part1(pki))
        asyncio.run(part2(pki))
        asyncio.run(part3(pki))


if __name__ == "__main__":
    main()
