"""Unit tests: one function or class at a time, no network, no processes. Fast."""
import json
import math

import pytest

from cloud_service.player import PlayerState, classify
from common.mesh import topic
from common.protocol import parse
from common.security import TokenStore, fingerprint
from robot.client import RobotClient
from robot.sensor import SineSensor
from tests.helpers import HOST
from user.cli import parse_line


# ---------------------------------------------------------------- protocol
@pytest.mark.parametrize("raw", ["not json", '{"robot_id": "x"}', '["register"]'])
def test_parse_rejects_malformed(raw):
    assert parse(raw) is None


def test_topic_names_match_spec():
    assert topic("robot-1", "sensor") == "robot/robot-1/sensor"
    assert topic("robot-1", "command") == "robot/robot-1/command"


# ---------------------------------------------------------------- Player logic
def test_classify_normal_and_high():
    assert classify({"state": 25.5}) == {"state": 25.5, "status": "normal"}
    assert classify({"state": 30.0}) == {"state": 30.0, "status": "normal"}   # "above" 30, not "at"
    assert classify({"state": 30.01}) == {"state": 30.01, "status": "high"}


def test_classify_missing_state_raises():
    with pytest.raises(KeyError):
        classify({"temperature": 1.0})


def alerts(events):
    return [e["level"] for e in events if e["status"] == "alert"]


def test_alerts_only_when_level_changes():
    state = PlayerState()
    fired = [alerts(state.on_reading(level, 0.0))
             for level in ["normal", "normal", "high", "high", "normal", "high"]]
    assert fired == [[], [], ["high"], [], ["normal"], ["high"]]


def test_first_reading_high_raises_an_alert():
    state = PlayerState()
    assert alerts(state.on_reading("high", 35.0)) == ["high"]


# ---------------------------------------------------------------- sine-wave sensor
class FakeClock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


def test_sine_sensor_follows_the_wave():
    clock = FakeClock()
    sensor = SineSensor(center=25.5, amplitude=8.0, period=20.0, clock=clock)
    expected = {0: 25.5, 5: 33.5, 10: 25.5, 15: 17.5}           # quarter-period steps
    for seconds, value in expected.items():
        clock.now = 100.0 + seconds
        assert math.isclose(sensor.read(), value, abs_tol=0.01)


# ---------------------------------------------------------------- tokens
def test_token_store_checks_tokens():
    store = TokenStore(robots={"robot-1": fingerprint("right")})
    assert store.check_robot("robot-1", "right")
    assert not store.check_robot("robot-1", "wrong")
    assert not store.check_robot("robot-1", None)
    assert not store.check_robot("robot-99", "right")


def test_token_store_file_holds_no_tokens(pki):
    token = (pki / "robots" / "robot-1" / "token").read_text()
    assert token not in (pki / "tokens.json").read_text()
    assert TokenStore.load(pki / "tokens.json").check_robot("robot-1", token)


# ---------------------------------------------------------------- operator command prompt
@pytest.mark.parametrize("line, expected", [
    ("connect robot-1", ("connect", ("robot-1",))),                        # keyword with an argument
    ("connect", ("error", ("usage: connect <robot_id>",))),               # keyword missing it
    ("robot-2 forward", ("command", ("robot-2", "forward"))),             # <robot_id> <command>
    ("all status hello there", ("status", ("all", "hello there"))),       # all status <text>
    ("robot-1 go fast", ("error", ("usage: <robot_id|all> <command>",))), # anything else
    ("wait 2.5", ("wait", (2.5,))),                                        # the demos' pause
])
def test_parse_line(line, expected):
    assert parse_line(line) == expected


# ---------------------------------------------------------------- Robot command handling
class FakePayload:
    def __init__(self, text):
        self.text = text

    def to_string(self):
        return self.text


class FakeSample:
    """Stands in for a Zenoh sample: only the two parts the Robot's callback uses."""
    def __init__(self, text, key_expr="robot/robot-1/command"):
        self.payload = FakePayload(text)
        self.key_expr = key_expr


@pytest.fixture
def robot(pki):
    # constructing a RobotClient reads its files but doesn't connect to anything
    return RobotClient("robot-1", cloud_url="wss://unused", host=HOST, pki_dir=pki)


def test_forward_and_stop_drive_motors(robot):
    robot._on_command(FakeSample(json.dumps({"command": "forward"})))
    assert (robot.jetbot.left_motor, robot.jetbot.right_motor) == (0.5, 0.5)
    robot._on_command(FakeSample(json.dumps({"command": "stop"})))
    assert (robot.jetbot.left_motor, robot.jetbot.right_motor) == (0.0, 0.0)


@pytest.mark.parametrize("payload", [
    "not json",
    json.dumps({"sensor": "ultrasonic"}),   # no "command" key
    json.dumps({"command": "fly"}),         # unknown command
    json.dumps(["forward"]),                # wrong shape
])
def test_bad_commands_are_ignored(robot, payload):
    robot.jetbot.forward()
    robot._on_command(FakeSample(payload))  # must not raise
    assert robot.jetbot.left_motor == 0.5   # and must not change the motors


def test_missing_identity_fails_fast(pki):
    with pytest.raises(FileNotFoundError):
        RobotClient("robot-unprovisioned", cloud_url="wss://unused", host=HOST, pki_dir=pki)
