"""Player: the cloud-side program spawned once per session (the robot's cloud "brain").

Started by: cloud_service/player_process.py (spawn_player), as
    python -m cloud_service.player --robot-id ID --host HOST --port PORT --pki-dir PKI
Never run by hand in normal use.

What it does (assignment topic table, plus status):
  subscribes  robot/{id}/sensor, robot/{id}/status
  publishes   robot/{id}/processed   ({"state": x} -> {"state": x, "status": "normal"|"high"})
              robot/{id}/status      ready {level} (in reply to each "joined"; held until the first
                                     reading if there isn't one yet) / alert (when the level changes,
                                     or the first reading is high) / stopping
The rules for those status events live in PlayerState, which is unit tested on its own.
It prints everything it receives (the Cloud Service re-logs its output as [Player]).

Contract with the Cloud Service:
  - prints "READY <endpoint>" once its Zenoh session is open
  - exits cleanly when its stdin is closed
Explained in Notebook 2 section 5; mutual TLS in Notebook 3 section 8.
"""
import argparse
import json
import logging
import signal
import sys
import threading
from pathlib import Path

import zenoh

from common.config import HIGH_THRESHOLD
from common.mesh import endpoint, make_config, topic
from common.protocol import status_msg
from common.security import load_identity

log = logging.getLogger("Player")
SENDER = "player"


def classify(data: dict) -> dict:
    """Add a status to a sensor reading. Returns a new dict; extra fields are kept."""
    out = dict(data)
    out["status"] = "high" if data["state"] > HIGH_THRESHOLD else "normal"
    return out


class PlayerState:
    """Decides which status events the Player publishes. Holds no Zenoh objects, so it's unit-testable.

    - The level is unknown (None) until the first reading. The Player never claims a level it hasn't
      measured.
    - alert: when the level changes between normal and high, or the very first reading is high.
      (Every reading's level is already on the processed topic; status is for changes.)
    - ready {level}: the reply to someone's "joined". If they join before the first reading, the reply
      is held and sent with the first reading, so it always carries a real level.

    Zenoh runs the sensor and status callbacks on different threads, hence the lock.
    """

    def __init__(self):
        self.level: str | None = None
        self.ready_owed = False
        self._lock = threading.Lock()

    def on_reading(self, level: str, state: float) -> list[dict]:
        with self._lock:
            events = []
            previous, self.level = self.level, level
            if (previous is None and level == "high") or (previous is not None and level != previous):
                events.append({"status": "alert", "level": level, "state": state})
            if self.ready_owed:
                events.append({"status": "ready", "level": level})
                self.ready_owed = False
            return events

    def on_joined(self) -> list[dict]:
        with self._lock:
            if self.level is None:
                self.ready_owed = True          # no reading yet: answer once there is one
                return []
            return [{"status": "ready", "level": self.level}]


def main() -> None:
    parser = argparse.ArgumentParser(description="Cloud-side Player (spawned by the Cloud Service)")
    parser.add_argument("--robot-id", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--pki-dir", type=Path, required=True)
    args = parser.parse_args()

    # Only the Cloud Service decides when a Player stops (by closing our stdin). It also starts us
    # in our own process group (player_process.py); ignoring Ctrl+C here covers any other route.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Plain lines on stdout; the Cloud Service re-logs each one with its own timestamp.
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    zenoh.init_log_from_env_or("error")

    ident = load_identity(args.pki_dir, "player")
    session = zenoh.open(make_config(args.host, args.port, ident))
    sensor, processed, status = (topic(args.robot_id, t) for t in ("sensor", "processed", "status"))
    state = PlayerState()

    def publish_events(events: list[dict]) -> None:
        for event in events:
            fields = {k: v for k, v in event.items() if k != "status"}
            session.put(status, status_msg(SENDER, event["status"], **fields))

    def on_sensor(sample):
        raw = sample.payload.to_string()
        log.info("got %s: %s", sample.key_expr, raw)
        try:
            result = classify(json.loads(raw))
        except (json.JSONDecodeError, KeyError, TypeError):
            log.warning("rejected sensor message %r", raw)
            return
        session.put(processed, json.dumps(result))
        publish_events(state.on_reading(result["status"], result["state"]))

    def on_status(sample):
        raw = sample.payload.to_string()
        try:
            data = json.loads(raw)
            if data.get("from") == SENDER:
                return                      # don't print our own status messages
        except (json.JSONDecodeError, AttributeError):
            data = {}
        log.info("got %s: %s", sample.key_expr, raw)
        if data.get("status") == "joined":
            # Someone just joined: tell them the current state. Anything announced before they
            # joined never reached them (Zenoh drops messages nobody is subscribed to yet).
            publish_events(state.on_joined())

    subs = [session.declare_subscriber(sensor, on_sensor),
            session.declare_subscriber(status, on_status)]

    print(f"READY {endpoint(args.port, args.host)}", flush=True)

    for _ in sys.stdin:   # blocks until the Cloud Service closes our stdin
        pass

    session.put(status, status_msg(SENDER, "stopping"))
    for s in subs:
        s.undeclare()
    session.close()
    log.info("shut down cleanly")


if __name__ == "__main__":
    main()
