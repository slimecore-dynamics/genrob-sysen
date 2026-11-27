"""Read the programs' log files as they grow, and keep the recent sensor readings per robot.

Used by: dashboard/server.py.

Every program logs what it receives, one line per message, for example:
    23:17:51.188 [User   ] got robot/robot-1/sensor: {"state": 17.61}
    23:17:51.188 [User   ] got robot/robot-1/processed: {"state": 17.61, "status": "normal"}
    23:17:59.259 [Robot  ] got robot/robot-1/status: {"from": "player", "status": "alert", "level": "high", ...}
    23:17:51.188 [Player ] (robot-1) got robot/robot-1/sensor: {"state": 17.61}

The same reading usually appears in several logs (the Player's line in cloud.log, the User's log),
so readings of the same robot with the same value less than MERGE_WINDOW apart are one reading.
Each reading's normal/high comes from the Player: its "processed" message for that reading if one
was logged, otherwise the level from the Player's most recent "ready" or "alert" status.
"""
import json
import re
import threading
import time
from pathlib import Path

TAIL_BYTES = 512 * 1024    # how much of an existing log to read at startup (the end of it)
KEEP_SECONDS = 600         # how long readings are kept in memory
MERGE_WINDOW = 0.5         # seconds: same robot, same value, closer than this -> the same reading

_STAMP = r"(\d{2}):(\d{2}):(\d{2})\.(\d{3})"
MESSAGE = re.compile(_STAMP + r" \[[^\]]*\] (?:\([^)]*\) )?got robot/([A-Za-z0-9_-]+)/"
                     r"(sensor|processed|status): (.*)$")
SESSION_STARTED = re.compile(_STAMP + r" \[Cloud\s*\] session (\w+): (\S+) <-> (\S+) via Player at (\S+)$")
SESSION_ENDED = re.compile(_STAMP + r" \[Cloud\s*\] session (\w+) ended \(([^)]*)\)")


def to_epoch(h, m, s, ms, now: float) -> float:
    """A log time of day (local time, no date) as a Unix time, assuming it is from the last 24 h."""
    local = time.localtime(now)
    midnight = now - (now % 1) - (local.tm_hour * 3600 + local.tm_min * 60 + local.tm_sec)
    t = midnight + int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000
    return t - 86400 if t > now + 60 else t


class Readings:
    """Recent readings and Player levels per robot, and the sessions seen in cloud.log. Thread-safe."""

    def __init__(self):
        self.lock = threading.Lock()
        self.clear()

    def clear(self) -> None:
        self.points: dict[str, list] = {}    # robot -> [[t, value, status or None], ...] by time
        self.levels: dict[str, list] = {}    # robot -> [(t, level), ...] from the Player's status
        self.sessions: dict[str, dict] = {}  # session id -> {robot, user, player, since}

    def add_reading(self, robot: str, t: float, value: float, status: str | None) -> None:
        series = self.points.setdefault(robot, [])
        for point in reversed(series[-5:]):          # merge with the same reading from another log
            if point[1] == value and abs(point[0] - t) < MERGE_WINDOW:
                point[2] = point[2] or status
                return
        series.append([t, value, status])
        if len(series) > 1 and series[-2][0] > t:     # logs are read in turn, so keep time order
            series.sort(key=lambda p: p[0])

    def add_level(self, robot: str, t: float, level: str) -> None:
        levels = self.levels.setdefault(robot, [])
        levels.append((t, level))
        levels.sort()

    def level_at(self, robot: str, t: float) -> str | None:
        level = None
        for when, lv in self.levels.get(robot, []):
            if when > t + MERGE_WINDOW:
                break
            level = lv
        return level

    def prune(self, now: float) -> None:
        cutoff = now - KEEP_SECONDS
        for table in (self.points, self.levels):
            for robot in list(table):
                kept = [p for p in table[robot] if p[0] >= cutoff]
                if kept:
                    table[robot] = kept
                else:
                    del table[robot]

    def snapshot(self, now: float, window: float) -> dict:
        """The readings of the last `window` seconds, each with its normal/high status."""
        with self.lock:
            robots = {}
            for robot, series in self.points.items():
                recent = [[round(t - now, 3), v, s or self.level_at(robot, t)]
                          for t, v, s in series if t >= now - window - 2]
                if recent:
                    robots[robot] = recent
            sessions = sorted(self.sessions.values(), key=lambda s: s["robot"])
        return {"robots": robots, "sessions": sessions}


class LogWatcher:
    """Follows every *.log file in a folder: new files, new lines, and files that were cleared."""

    def __init__(self, logs_dir, readings: Readings, clock=time.time):
        self.logs_dir = Path(logs_dir)
        self.readings = readings
        self.clock = clock
        self.offsets: dict[Path, int] = {}
        self.partial: dict[Path, bytes] = {}

    def poll(self) -> None:
        now = self.clock()
        for path in sorted(self.logs_dir.glob("*.log")):
            for line in self._new_lines(path):
                self._parse(line, now)
        with self.readings.lock:
            self.readings.prune(now)

    def reset(self) -> None:
        """Forget everything read so far (after the logs were cleared)."""
        self.offsets.clear()
        self.partial.clear()
        with self.readings.lock:
            self.readings.clear()

    def _new_lines(self, path: Path) -> list[str]:
        try:
            size = path.stat().st_size
            if path not in self.offsets:                 # first time: only the end of the file
                self.offsets[path] = max(0, size - TAIL_BYTES)
                self.partial[path] = b"" if self.offsets[path] == 0 else None   # None: skip a cut line
            if size < self.offsets[path]:                # the file was cleared: start over
                self.offsets[path], self.partial[path] = 0, b""
            if size == self.offsets[path]:
                return []
            with open(path, "rb") as f:
                f.seek(self.offsets[path])
                data = f.read(size - self.offsets[path])
        except OSError:
            return []
        self.offsets[path] += len(data)
        head = self.partial[path]
        chunks = ((head or b"") + data).split(b"\n")
        self.partial[path] = chunks.pop()                # an unfinished last line waits for the rest
        if head is None and chunks:
            chunks.pop(0)                                # the first line was cut by TAIL_BYTES
        return [c.decode("ascii", "replace").rstrip("\r") for c in chunks]

    def _parse(self, line: str, now: float) -> None:
        if m := MESSAGE.match(line):
            h, mi, s, ms, robot, kind, payload = m.groups()
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                return
            if not isinstance(data, dict):
                return
            t = to_epoch(h, mi, s, ms, now)
            with self.readings.lock:
                if kind in ("sensor", "processed") and isinstance(data.get("state"), int | float):
                    self.readings.add_reading(robot, t, float(data["state"]), data.get("status"))
                elif kind == "status" and data.get("from") == "player" and data.get("level"):
                    self.readings.add_level(robot, t, data["level"])
        elif m := SESSION_STARTED.match(line):
            h, mi, s, ms, sid, user, robot, player = m.groups()
            with self.readings.lock:
                self.readings.sessions[sid] = {"robot": robot, "user": user, "player": player,
                                               "since": to_epoch(h, mi, s, ms, now)}
        elif m := SESSION_ENDED.match(line):
            with self.readings.lock:
                self.readings.sessions.pop(m.group(5), None)
