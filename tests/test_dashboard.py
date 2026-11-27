"""Tests for the read-only dashboard (dashboard/): reading the logs, and protecting "clear logs"."""
import http.client
import json
import time

from dashboard.logwatch import LogWatcher, Readings
from dashboard.server import Dashboard


def stamp(t: float) -> str:
    """A log timestamp (local time of day) for Unix time t, as the programs write it."""
    return time.strftime("%H:%M:%S", time.localtime(t)) + f".{int(t * 1000) % 1000:03d}"


def test_a_reading_in_several_logs_is_plotted_once_with_the_players_status(tmp_path):
    now = time.time()
    t1, t2 = now - 5.0, now - 4.0          # two readings a second apart, with the same value
    (tmp_path / "cloud.log").write_text(
        f'{stamp(t1)} [Player ] (robot-1) got robot/robot-1/sensor: {{"state": 35.0}}\n'
        f'{stamp(t2)} [Player ] (robot-1) got robot/robot-1/sensor: {{"state": 35.0}}\n')
    (tmp_path / "user-user.log").write_text(
        f'{stamp(t1 + 0.002)} [User   ] got robot/robot-1/sensor: {{"state": 35.0}}\n'
        f'{stamp(t1 + 0.003)} [User   ] got robot/robot-1/processed: {{"state": 35.0, "status": "high"}}\n')
    (tmp_path / "robot-1.log").write_text(
        f'{stamp(t1 + 0.004)} [Robot  ] got robot/robot-1/status: '
        f'{{"from": "player", "status": "alert", "level": "high", "state": 35.0}}\n')
    readings = Readings()
    LogWatcher(tmp_path, readings, clock=lambda: now).poll()

    points = readings.snapshot(now, 60)["robots"]["robot-1"]
    assert [(round(t), v, s) for t, v, s in points] == [(-5, 35.0, "high"), (-4, 35.0, "high")]
    # t1 came from 3 log lines in 2 files and is plotted once, with the status from "processed";
    # t2 has no "processed" line, so its status comes from the Player's last alert.


def request(port, method, path, host=None, token=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    headers = {"Host": host or f"127.0.0.1:{port}"}
    if token:
        headers["X-Dashboard-Token"] = token
    conn.request(method, path, headers=headers)
    res = conn.getresponse()
    body = res.read()
    conn.close()
    return res.status, body


def test_clear_logs_needs_the_token_and_a_local_host_name(tmp_path):
    log = tmp_path / "cloud.log"
    log.write_text("something worth keeping\n")
    dashboard = Dashboard(tmp_path, port=0)
    dashboard.start()
    try:
        port = dashboard.port
        assert request(port, "POST", "/api/clear-logs")[0] == 403                     # no token
        assert request(port, "POST", "/api/clear-logs", token="wrong")[0] == 403
        assert request(port, "GET", "/api/state", host="evil.example:80")[0] == 403    # DNS rebinding
        assert request(port, "POST", "/api/clear-logs", host="evil.example:80", token=dashboard.token)[0] == 403
        assert log.read_text() == "something worth keeping\n"

        status, body = request(port, "POST", "/api/clear-logs", token=dashboard.token)
        assert status == 200 and json.loads(body) == {"cleared": ["cloud.log"]}
        assert log.read_text() == ""
    finally:
        dashboard.stop()
