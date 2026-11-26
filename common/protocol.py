"""Message formats, plus one Zenoh delivery helper.

Used by: cloud_service/, robot/, user/.
Explained in Notebook 2 section 2 (control messages) and Notebook 4 section 4 (wait_for_subscriber).

ASSUMPTION A2: the control-plane message names and shapes below are this project's design;
the assignment describes the behaviors only.

Control plane (WebSocket, wss://, each program <-> Cloud Service). Every message is JSON with "type":

  Robot -> Cloud   register {robot_id, token}            heartbeat     goodbye
  Cloud -> Robot   registered {robot_id, resumed?}       error {reason}
                   join_session {player_endpoint}        session_ended {robot_id, reason}

  User  -> Cloud   hello {name, token}   (must be first)
                   list_robots {request_id}              connect {robot_id, request_id}
                   disconnect {robot_id, request_id}
  Cloud -> User    welcome                               error {reason, request_id?}
                   robots {robots, request_id}           session_ready {robot_id, player_endpoint, request_id}
                   disconnected {robot_id, request_id}   session_ended {robot_id, reason}
                   robot_status {robot_id, state}

Data plane (Zenoh topics from the assignment):
  robot/{id}/sensor     Robot  -> Player, User   {"state": 25.5}
  robot/{id}/command    User   -> Robot          {"command": "forward"}
  robot/{id}/processed  Player -> User           {"state": 25.5, "status": "normal"}
  robot/{id}/status     any    -> all            {"from": <who>, "status": <event>, ...extra fields}

ASSUMPTION A9: what each entity says on status (the assignment says "Status updates from any entity"):
  Robot:  joined, ready {motors} (in reply to every other "joined"), moving {command}, stopped,
          rejected_command {command}, leaving
  User:   joined, then free text typed by the operator
  Player: ready {level} (in reply to every "joined"; if it has no reading yet, the reply waits for
          the first one, so the level is always measured, never assumed), alert {level, state} (when
          the level changes between normal and high, or the first reading is high), stopping

The "ready" replies mean whoever joins last still learns everyone's current state, even though
anything announced before they joined never reached them.

"joined" is sent only once the sender's status publisher matches a subscriber, because Zenoh
silently drops a message published before the sender knows anyone is subscribed.
"""
import asyncio
import json
import time

ROBOT_PATH = "/robot"
USER_PATH = "/user"


def msg(type_: str, **fields) -> str:
    """Build a control message: msg("register", robot_id="robot-1", token=...)."""
    return json.dumps({"type": type_, **fields})


def parse(raw) -> dict | None:
    """Decode a control message. Returns None for anything that isn't a JSON object with a "type"."""
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict) or "type" not in data:
        return None
    return data


def status_msg(sender: str, event: str, **fields) -> str:
    """Build a data-plane status message: status_msg("robot-1", "moving", command="forward")."""
    return json.dumps({"from": sender, "status": event, **fields})


async def wait_for_subscriber(publisher, timeout: float) -> bool:
    """Wait until a Zenoh publisher knows of at least one matching subscriber. True if it does in time.

    Zenoh drops a put when the sender doesn't yet know of any subscriber for that key, and news of a
    new peer's subscriptions takes a few milliseconds to arrive.
    """
    deadline = time.monotonic() + timeout
    while not publisher.matching_status.matching:
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(0.01)
    return True
