"""Small helpers shared by the test files."""
import asyncio
import json
import time

HOST = "127.0.0.1"

# Identities created once per test run (see the pki fixture in conftest.py).
ROBOTS = ("robot-1", "robot-2", "robot-3")
USERS = ("user", "first", "second")


async def wait_until(condition, timeout=8.0, interval=0.05):
    """Poll until condition() is true. Waiting for a condition, not a fixed sleep, keeps tests
    fast when things are quick and reliable when the machine is slow."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")


def capture_messages(user) -> list:
    """Replace a User's receive callback with one that records (topic, data). Call before connect()."""
    received = []
    user._on_message = lambda s: received.append((str(s.key_expr), json.loads(s.payload.to_string())))
    return received


def statuses(received, sender=None) -> list[dict]:
    """Status messages from `received`, optionally only those from one sender."""
    return [d for t, d in received if t.endswith("/status") and (sender is None or d.get("from") == sender)]
