"""Default settings shared by every program.

Used by: every __main__.py (as argparse defaults) and the clients/server.
Host, port, ids and the PKI folder can be overridden on the command line; see each program's --help.
Lines marked ASSUMPTION are choices where the assignment was ambiguous (all listed in README.md).
"""
from pathlib import Path

DEFAULT_HOST = "127.0.0.1"
DEFAULT_CLOUD_PORT = 8700
DEFAULT_PKI_DIR = Path("pki")     # created by: python -m tools.setup_security

# Heartbeats (Robot -> Cloud Service)
HEARTBEAT_INTERVAL = 1.0          # seconds between heartbeats
MISSED_HEARTBEATS_ALLOWED = 3     # consecutive misses before a robot is offline
HEARTBEAT_TIMEOUT = HEARTBEAT_INTERVAL * MISSED_HEARTBEATS_ALLOWED

# Connection drops
RECONNECT_GRACE = 5.0             # seconds the Cloud holds a dropped robot's session open
RECONNECT_BACKOFF_START = 1.0     # robot's first wait before retrying the Cloud
RECONNECT_BACKOFF_MAX = 10.0      # longest wait between retries

# Flooding limits (Cloud Service)
FIRST_MESSAGE_TIMEOUT = 5.0       # a new connection must register / say hello within this many seconds
MAX_CONNECTIONS = 200             # open WebSocket connections the Cloud accepts at once

# Player
PLAYER_READY_TIMEOUT = 10.0       # seconds to wait for a new Player to print READY
HIGH_THRESHOLD = 30.0             # ASSUMPTION A5: sensor state above this is "high", else "normal"

# Robot sensor  (ASSUMPTION A8: a sine wave centered on the assignment's example value)
SENSOR_INTERVAL = 1.0             # assignment: "Robot publishes sensor data every second"
SENSOR_CENTER = 25.5              # the assignment's example reading
SENSOR_AMPLITUDE = 8.0            # swings 17.5 .. 33.5, so it crosses HIGH_THRESHOLD each cycle
SENSOR_PERIOD = 20.0              # seconds per full cycle

# Zenoh
# Zenoh's default scouting delay (500 ms) can make zenoh.open() wait up to half a second for peers.
# Peers here always get an explicit address, so a short wait is enough. Measured with benchmarks/latency.py:
# time to session p50 was ~540 ms at the default, ~42 ms at 10 ms; the triangle formed every time.
ZENOH_SCOUTING_DELAY_MS = 10

# Requests (User -> Cloud Service)
REQUEST_TIMEOUT = 15.0            # longer than PLAYER_READY_TIMEOUT, since connect waits for a spawn
COMMAND_PATH_TIMEOUT = 5.0        # after session_ready, how long the User waits for the Robot's command subscriber


def cloud_url(host: str, port: int) -> str:
    return f"wss://{host}:{port}"
