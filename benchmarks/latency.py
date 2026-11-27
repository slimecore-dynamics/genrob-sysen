"""Latency benchmark.  Run from the repo root:  python -m benchmarks.latency [--count N] [--sessions N]

Measures, all on this one machine (localhost), and reports percentiles:

1. Zenoh round trip, plain TCP vs. mutual TLS (data plane, one hop each way)
   Session A publishes a ping, session B echoes it back, A times the round trip.
   One-way latency is roughly half of this. Plain TCP is measured only to show what TLS costs;
   the programs always use TLS.

2. Sensor -> User, direct vs. through the Player (the real, encrypted triangle)
   A real Player process is spawned. A robot-side session publishes readings stamped with the time;
   the user-side session records when the raw reading (1 hop) and the Player's processed version
   (2 hops + the Player's work) arrive. Both timestamps come from this one process, so the clock matches.

3. Time to session (control plane)
   How long user.connect() takes: request -> Cloud Service spawns a Player -> both sides join the
   triangle -> the command path is confirmed.

It creates its own throwaway certificates and tokens in a temporary folder.

Method notes
  - Closed loop: each message waits for the previous reply before the next is sent, so the numbers
    show latency with no queueing, not behavior under load.
  - Warm-up messages are sent first and not counted.
  - Percentiles, not averages: latency is right-skewed, so the mean hides the slow tail.
  - Localhost numbers show the software's own overhead. Over a real network, add the network's round trip.
"""
import argparse
import asyncio
import json
import logging
import statistics
import tempfile
import threading
import time
from pathlib import Path

import zenoh

from cloud_service.player_process import spawn_player
from cloud_service.server import CloudService
from common.config import cloud_url
from common.mesh import endpoint, free_port, make_config, open_session, topic
from common.security import load_identity
from robot.client import RobotClient
from tools.setup_security import setup
from user.client import UserClient

HOST = "127.0.0.1"
WARMUP = 50
ROBOT_ID, USER_NAME = "bench-robot", "bench-user"


# ---------------------------------------------------------------- reporting
def summarize(name: str, samples_ms: list[float]) -> dict:
    q = statistics.quantiles(samples_ms, n=100, method="inclusive")   # q[k-1] = k-th percentile
    return {"measurement": name, "n": len(samples_ms), "min": min(samples_ms),
            "p50": q[49], "p95": q[94], "p99": q[98], "max": max(samples_ms)}


def print_table(rows: list[dict]) -> None:
    print(f"\n{'measurement':44} {'n':>5} {'min':>8} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8}   (ms)")
    for r in rows:
        print(f"{r['measurement']:44} {r['n']:>5} {r['min']:8.3f} {r['p50']:8.3f} "
              f"{r['p95']:8.3f} {r['p99']:8.3f} {r['max']:8.3f}")


# ---------------------------------------------------------------- 1. Zenoh round trip
def zenoh_round_trip(count: int, ident_a, ident_b) -> list[float]:
    port_a = free_port(HOST)
    a = zenoh.open(make_config(HOST, port_a, ident_a))
    b = zenoh.open(make_config(HOST, free_port(HOST), ident_b, [endpoint(port_a, HOST, ident_a is not None)]))
    time.sleep(1)

    got_reply = threading.Event()
    echo = b.declare_subscriber("bench/ping", lambda s: b.put("bench/pong", s.payload.to_bytes()))
    pong = a.declare_subscriber("bench/pong", lambda s: got_reply.set())
    time.sleep(0.5)

    samples = []
    for i in range(WARMUP + count):
        got_reply.clear()
        t0 = time.perf_counter_ns()
        a.put("bench/ping", b"x" * 32)
        if not got_reply.wait(timeout=2):
            raise RuntimeError("no reply within 2 s")
        if i >= WARMUP:
            samples.append((time.perf_counter_ns() - t0) / 1e6)

    echo.undeclare()
    pong.undeclare()
    a.close()
    b.close()
    return samples


# ---------------------------------------------------------------- 2. through the real Player
def sensor_paths(count: int, pki: Path) -> tuple[list[float], list[float]]:
    player = spawn_player(ROBOT_ID, HOST, pki)
    robot = open_session(HOST, load_identity(pki, "robots", ROBOT_ID), [player.endpoint])
    user = open_session(HOST, load_identity(pki, "users", USER_NAME), [player.endpoint])
    time.sleep(1.5)

    direct, via_player = [], []
    processed_arrived = threading.Event()

    def on_sensor(sample):
        data = json.loads(sample.payload.to_string())
        if data["seq"] >= WARMUP:
            direct.append((time.perf_counter_ns() - data["t0"]) / 1e6)

    def on_processed(sample):
        data = json.loads(sample.payload.to_string())
        if data["seq"] >= WARMUP:
            via_player.append((time.perf_counter_ns() - data["t0"]) / 1e6)
        processed_arrived.set()

    subs = [user.declare_subscriber(topic(ROBOT_ID, "sensor"), on_sensor),
            user.declare_subscriber(topic(ROBOT_ID, "processed"), on_processed)]
    time.sleep(0.5)

    sensor = topic(ROBOT_ID, "sensor")
    for seq in range(WARMUP + count):
        processed_arrived.clear()
        robot.put(sensor, json.dumps({"state": 25.5, "seq": seq, "t0": time.perf_counter_ns()}))
        if not processed_arrived.wait(timeout=2):
            raise RuntimeError("Player did not answer within 2 s")

    for s in subs:
        s.undeclare()
    robot.close()
    user.close()
    player.stop()
    return direct, via_player


# ---------------------------------------------------------------- 3. time to session
async def time_to_session(runs: int, pki: Path) -> list[float]:
    cloud = CloudService(HOST, free_port(HOST), pki)
    await cloud.start()
    url = cloud_url(HOST, cloud.port)
    bot = RobotClient(ROBOT_ID, url, HOST, pki)
    bot_task = asyncio.create_task(bot.run())
    while ROBOT_ID not in cloud.robots:
        await asyncio.sleep(0.05)

    user = UserClient(USER_NAME, url, HOST, pki)
    await user.start()
    samples = []
    for _ in range(runs):
        t0 = time.perf_counter()
        await user.connect(ROBOT_ID)
        samples.append((time.perf_counter() - t0) * 1000)
        await user.disconnect(ROBOT_ID)
        while cloud.robots[ROBOT_ID].session is not None:     # wait for teardown before the next run
            await asyncio.sleep(0.05)

    await user.shutdown()
    bot_task.cancel()
    await bot.shutdown()
    await cloud.stop()
    return samples


def main() -> None:
    parser = argparse.ArgumentParser(description="Latency benchmark (localhost)")
    parser.add_argument("--count", type=int, default=1000, help="measured messages per data-plane test")
    parser.add_argument("--sessions", type=int, default=10, help="connect() runs for time to session")
    args = parser.parse_args()
    logging.basicConfig(level=logging.ERROR)   # keep component logs out of the results table
    zenoh.init_log_from_env_or("error")

    with tempfile.TemporaryDirectory() as tmp:
        pki = Path(tmp)
        setup(pki, robots=[ROBOT_ID], users=[USER_NAME])
        robot_id = load_identity(pki, "robots", ROBOT_ID)
        user_id = load_identity(pki, "users", USER_NAME)
        print()

        rows = [summarize("Zenoh round trip, plain TCP", zenoh_round_trip(args.count, None, None)),
                summarize("Zenoh round trip, mutual TLS", zenoh_round_trip(args.count, robot_id, user_id))]
        direct, via_player = sensor_paths(args.count, pki)
        rows.append(summarize("sensor -> User, direct (1 hop, TLS)", direct))
        rows.append(summarize("sensor -> Player -> User (TLS)", via_player))
        rows.append(summarize("time to session (connect -> ready)", asyncio.run(time_to_session(args.sessions, pki))))
        print_table(rows)


if __name__ == "__main__":
    main()
