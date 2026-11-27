# Robot / User / Player Data Channel

A Cloud Service tracks connected robots. When an operator (the User) connects to a robot, the Cloud Service starts a Player process for that session. The Robot, User and Player then form a peer-to-peer triangle: each one publishes to the other two and subscribes to them, with no broker in between.

```
        Player (cloud)
         /        \
     Robot -------- User
```

Control traffic (registration, heartbeats, session setup) goes over a WebSocket to the Cloud Service. Session data (sensor readings, commands, processed results, status) goes over Zenoh, directly between the three peers.

The optional items from the assignment (security, scalability, error handling, latency benchmark, testing) each have a section below.

## Requirements

- Python 3.11 or newer (tested on 3.11, 3.13 and 3.14)
- Dependencies in `requirements.txt`: `eclipse-zenoh`, `websockets`, `cryptography`, `pytest`, `pytest-asyncio`

## Setup

```bash
pip install -r requirements.txt
python -m tools.setup_security --robots robot-1 robot-2 --users user
```

The second command creates a private certificate authority, a certificate for each program, and a token for each robot and user. Everything is written to `pki/`, which is excluded from git. Robots and users can be added later by running the command again; the Cloud Service picks them up without a restart.

## Running

Start each program in its own terminal, from the repository root:

```bash
python -m cloud_service                       # Cloud Service
python -m robot --robot-id robot-1            # Robot
python -m user                                # User (interactive prompt)
```

Example User session:

```
list
connect robot-1
robot-1 status operator here
robot-1 forward
robot-1 stop
quit
```

### User commands

| Command | Description |
|---|---|
| `list` | List robots and their state |
| `sessions` | List robots this User is connected to |
| `connect <robot_id>` | Start a session with a robot (a User can hold several) |
| `disconnect <robot_id>` | End that session |
| `<robot_id> <command>` | Send `forward`, `backward`, `left`, `right` or `stop` |
| `all <command>` | Send a command to every connected robot |
| `<robot_id\|all> status <text>` | Publish a status message in that robot's session |
| `wait <seconds>` | Pause (used by the scripted demos) |
| `help`, `quit` | |

### Logs

The User terminal shows only commands and their results. Everything the User receives (sensor, processed and status messages) is written to `logs/user-<name>.log`. To follow it live:

```
Get-Content logs\user-user.log -Wait      # PowerShell
tail -f logs/user-user.log                # macOS/Linux
```

The Cloud Service logs to `logs/cloud.log`, including the output of every Player, and each Robot logs to `logs/<robot-id>.log`. Use `--log-file` to change the location.

### Dashboard

```bash
python -m dashboard        # opens http://127.0.0.1:8800
```

A read-only, live view of the log files: one line per robot over the last 60 seconds, with readings the Player classified as high marked, a SOLO button per robot, and the active sessions. It only reads the logs; it never starts, stops or talks to the programs. "Clear logs" empties the log files. The dashboard listens on 127.0.0.1 only, rejects requests that don't name this machine in their Host header, and requires a per-run token for clearing logs.

### Scripted demos

The demos need the Cloud Service and the robots they use already running (`robot-1` for `demo.txt`; `robot-1` and `robot-2` for `demo_multi.txt`). The Player is started by the Cloud Service when the demo connects; it isn't started by hand.

```bash
python -m user < demo.txt          # one robot (robot-1)
python -m user < demo_multi.txt    # one User, two robots (start robot-2 as well)
```

To see the Player classify two robots differently, give them different sensor ranges:

```bash
python -m robot --robot-id robot-1 --amplitude 3    # never reaches "high"
python -m robot --robot-id robot-2 --amplitude 8    # crosses "high" every cycle
```

## Functional requirements

| # | Requirement | Implementation |
|---|---|---|
| 1 | When a robot starts, it pings the cloud server and starts sending heartbeat messages. | `RobotClient._connect_once` registers with the robot's token; `_heartbeat_loop` sends a heartbeat every second (`robot/client.py`). |
| 2 | The cloud service maintains the connection and status of the robot. | `CloudService.robot_handler` and `_watchdog` track each robot as `online`, `in_session`, `reconnecting` or `offline` (`cloud_service/server.py`). |
| 3 | The user connects to the cloud server and can list connected robots and connect to one. | `UserClient.start`, `list_robots` and `connect` (`user/client.py`); the `list` and `connect` commands. |
| 4 | On a connect request, the cloud server spawns a pub/sub server, then directs the user and robot to start their own with its details. | `CloudService.start_session` starts the Player (`cloud_service/player_process.py`), waits for it to report `READY <endpoint>`, and sends that endpoint to the Robot (`join_session`) and the User (`session_ready`). Each opens its own Zenoh session, listening on its own port and connected to the Player. |
| 5 | The three pub/sub servers form a triangle mesh and publish and subscribe to each other directly. | Once the Robot and User are both connected to the Player, Zenoh's peer gossip connects them to each other. `test_robot_and_user_are_linked_directly` verifies the direct link. |

The demonstration scenario from the assignment maps as follows:

| Assignment | Implementation |
|---|---|
| Robot publishes sensor data every second | A sine wave centered on 25.5 (assumption A8) |
| Player processes it and republishes with a status | `classify()` in `cloud_service/player.py`, published on `processed` |
| User publishes data that the Player receives | `<robot_id> status <text>`, on the status topic (assumption A4) |
| User publishes `stop` and `forward` commands | `robot-1 stop`, `robot-1 forward`, or `all <command>` |
| Robot executes commands, jetbot-style | `FakeJetbot` (`robot/jetbot.py`) uses the method names of jetbot's `Robot` class |
| Each entity prints all data it receives | Each program's log; `test_player_prints_what_it_receives` |

## Architecture

| | Control plane | Data plane |
|---|---|---|
| Messages | register, heartbeat, list, connect, disconnect, session start and end | sensor, command, processed, status |
| Between | Each program and the Cloud Service | Robot, User and Player, peer to peer |
| Transport | WebSocket over TLS (`wss://`, paths `/robot` and `/user`) | Zenoh in peer mode, mutual TLS |

**Zenoh** provides peer-to-peer publish/subscribe without a broker in the data path. The Robot and User are only given the Player's address; Zenoh's gossip introduces them to each other, which produces the triangle the assignment describes. General Robotics' GRID platform also uses Zenoh: its client's `connect()` takes a combined `{"player": ..., "edge": ...}` Zenoh configuration.

**WebSocket** is used for control because the Cloud Service needs to push messages to robots (for example, "join this session"), not only answer requests. GRID's client similarly uses a signaling WebSocket, with separate URLs for robots, users and players.

### Session setup

1. The Robot connects to `/robot`, registers with its token, and sends a heartbeat every second.
2. The User connects to `/user`, identifies itself with its token, and sends `list_robots` and `connect robot-1`.
3. The Cloud Service starts `python -m cloud_service.player` on a free port and waits for `READY <endpoint>`.
4. The Cloud Service sends the endpoint to the Robot (`join_session`) and the User (`session_ready`).
5. The Robot and User open Zenoh sessions connected to the Player and announce `joined` on the status topic; the other peers reply with `ready` and their current state. The User's `connect()` returns once messages can flow in both directions: its commands can reach the Robot, and the Robot has answered its `joined`.

### Topics

| Topic | Publisher | Subscribers | Example payload |
|---|---|---|---|
| `robot/{id}/sensor` | Robot | Player, User | `{"state": 31.2}` |
| `robot/{id}/command` | User | Robot | `{"command": "forward"}` |
| `robot/{id}/processed` | Player | User | `{"state": 31.2, "status": "high"}` |
| `robot/{id}/status` | All | All | `{"from": "player", "status": "alert", "level": "high", "state": 31.2}` |

Data-plane messages are only delivered to other peers, never back to the session that sent them.

Sensor types could each get their own topic under the same prefix (for example `robot/{id}/sensor/camera`), and `robot/{id}/sensor/**` would subscribe to all of them. GRID's Isaac Sim bridge uses this one-topic-per-sensor layout.

### Status messages

Assumption A9 defines what each entity publishes on the status topic:

| Entity | Events |
|---|---|
| Robot | `joined`; `ready {motors}` in reply to another peer's `joined`; `moving {command}`; `stopped`; `rejected_command {command}`; `leaving {reason}`, always sent before its goodbye to the Cloud Service |
| Player | `ready {level}` in reply to each `joined`, held until it has a sensor reading; `alert {level, state}` when the level changes between normal and high, or when the first reading is already high; `stopping` |
| User | `joined`, repeated until the Robot answers `ready`; then any text the operator sends |

Zenoh drops a message if the sender does not yet know of any subscriber for it. The `joined`/`ready` exchange makes sure that whoever joins last still learns the current state of the others.

## Security

```bash
python -m tools.setup_security --robots robot-1 --users user
```

This creates a private certificate authority, a certificate for each program, and a token for each robot and user, under `pki/`.

| Threat | Mitigation | Test |
|---|---|---|
| Eavesdropping on control traffic | `wss://` only; plain `ws://` is refused | `test_plain_websocket_is_rejected` |
| Impersonating the Cloud Service | Clients trust only the project's CA | `test_client_refuses_a_server_it_does_not_trust` |
| Impersonating a robot or user | Each must present its token. The Cloud Service stores only SHA-256 fingerprints and compares them in constant time. | `test_robot_with_wrong_token_is_refused`, `test_robot_cannot_use_another_robots_token`, `test_user_with_wrong_token_is_refused` |
| Taking over a session during the reconnect grace period | Resuming a session also requires the robot's token | `test_impostor_cannot_take_over_during_reconnect_grace` |
| Joining a session and sending commands | Zenoh mutual TLS: every peer must present a certificate signed by the project's CA | `test_untrusted_zenoh_peer_cannot_join_or_command`, `test_unencrypted_zenoh_peer_cannot_join` |
| Holding connections open without identifying | Closed after `FIRST_MESSAGE_TIMEOUT` (5 s) | `test_silent_connection_is_closed` |
| Opening too many connections | Refused beyond `MAX_CONNECTIONS` (close code 1013, "try again later") | `test_connection_cap` |
| Other local users reading keys | Private keys and tokens are written with mode 600 (Linux and macOS) | `test_private_keys_and_tokens_are_owner_only` |
| Names that escape a directory or topic (`../x`, `robot-*`, `a/b`) | Robot and user names are limited to letters, digits, `-` and `_` | `test_names_that_could_escape_a_folder_or_topic_are_refused` |

### Known gaps

- **Isolation between sessions.** Any certificate signed by the CA could join another session if it learned that session's Player address. The Cloud Service only gives addresses to the matching Robot and User, but a full fix needs authorization: Zenoh access-control rules keyed to certificate names (for example, only `user` may publish to `robot/robot-1/command`), or per-session credentials.
- **Revocation.** Removing a fingerprint from `tokens.json` blocks new logins immediately, but does not disconnect a robot or user that is already connected. Certificates remain valid until they expire (90 days). Short-lived certificates or a revocation list would close this gap.
- **Rate limiting.** Connections are capped, but an authenticated client can send messages at any rate.
- **Key storage.** `pki/ca/key.pem` can sign any identity. In a real deployment it would be kept offline, and each robot would receive only its own folder.

## Scalability

Currently supported and tested:

- Many robots, each with its own session and its own Player process. Topics are per robot, so sessions do not see each other's data (`test_two_users_two_robots_are_isolated`).
- One User controlling several robots: `connect robot-1`, `connect robot-2`, then `robot-2 forward` or `all stop` (`test_one_user_controls_several_robots`). `all` sends one message per robot; they are sent together but not executed at the same instant.
- One User per robot at a time; a second User receives `robot_busy` (assumption A3).

Costs that grow with scale:

- One Player process per session. Starting it is the largest part of connection time (see Performance).
- One heartbeat per robot per second, and one open WebSocket per robot and per user, at the Cloud Service.

Possible next steps:

1. **Run Players on other machines.** `spawn_player` is the only code that starts them. Replacing it with "start a container" (for example, one Kubernetes pod per session) would spread Players across machines without changing the protocol.
2. **Run more than one Cloud Service.** The robot registry is held in memory. Moving it to a shared store such as Redis, with each robot's WebSocket pinned to one instance, would let several instances share the load.
3. **Robots on other networks.** Peer-to-peer links require each peer to reach the others' addresses. Across NAT, Zenoh routers or a relay would carry the traffic.
4. **Several Users on one robot** (one controlling, others observing) would require admitting more than one User to a session, with access-control rules deciding who may publish commands.

## Error handling and resource management

| Situation | Behavior | Test |
|---|---|---|
| Unknown robot | `robot_not_found` | `test_robot_not_found` |
| Duplicate registration | The second robot is refused (`duplicate_registration`) | `test_duplicate_registration_refused` |
| Player fails to start | The Player is killed, the User receives `player_spawn_failed`, and the robot returns to `online` | `test_player_spawn_failure` |
| Robot already in a session | The second User receives `robot_busy` | `test_second_user_gets_robot_busy` |
| Robot reconnecting | `robot_reconnecting` | `test_connect_while_robot_reconnecting` |
| Heartbeats stop | Offline after 3 missed heartbeats; the session ends, the Player is stopped, both sides are notified, and the motors stop | `test_heartbeat_timeout_ends_session` |
| Robot connection drops | Held as `reconnecting` for 5 s; if the robot re-registers in time, it resumes the same session and Player | `test_drop_and_reconnect_resumes_same_session`, `test_drop_without_return_ends_session` |
| Robot shut down (Ctrl+C) during a session | The robot says goodbye and goes offline immediately; the User is told `robot_offline:robot_shutdown` | `test_robot_ctrl_c_ends_session_with_robot_shutdown` |
| Cloud Service restarts | The Robot and User reconnect with backoff (1, 2, 4, up to 10 s); the User's sessions end and it reports this (assumption A12) | `test_user_survives_cloud_restart` |
| User disconnects | Its sessions end and their Players are stopped | `test_user_leaving_ends_session_and_stops_player` |
| Ctrl+C in the Cloud Service's terminal | Only the Cloud Service is interrupted; it then stops each Player in order (Players run in their own process group) | `test_ctrl_c_does_not_reach_the_player` (Linux and macOS) |
| Robot or user added while the Cloud Service is running | Accepted without a restart; the Cloud Service re-reads `tokens.json` when it changes | `test_identity_added_while_cloud_runs_can_log_in` |
| Malformed or unexpected message | Logged and refused (`bad_request`, `must_register_first`, `rejected_command`) without crashing | `test_bad_request_is_refused`, `test_register_message_must_come_first`, `test_bad_commands_are_ignored` |
| Unexpected error in the Robot | Logged with a traceback; the Robot keeps reconnecting | |

Every session ends through `CloudService.end_session`, which stops the Player by closing its stdin and kills it if it has not exited within 3 s. Zenoh subscribers, publishers and sessions are released on every exit path, and closing a Zenoh session runs on a worker thread so it cannot block the program. The test fixtures stop every server and process, including when a test fails.

## Performance

```bash
python -m benchmarks.latency
```

All measurements are on a single machine (localhost), closed loop (each message waits for its reply), with 50 warm-up messages discarded. Results are reported as percentiles.

Windows 11, Python 3.14.6:

| Measurement | p50 | p95 | p99 |
|---|---|---|---|
| Zenoh round trip, plain TCP (reference) | 0.080 ms | 0.124 ms | 0.157 ms |
| Zenoh round trip, mutual TLS | 0.120 ms | 0.206 ms | 0.428 ms |
| Sensor to User, direct (TLS) | 0.108 ms | 0.177 ms | 0.339 ms |
| Sensor to User through the Player (TLS) | 0.184 ms | 0.285 ms | 0.466 ms |
| Time to session (connect to ready, including Player start) | 293 ms | 324 ms | 330 ms |

Linux (cloud VM), Python 3.13:

| Measurement | p50 | p95 | p99 |
|---|---|---|---|
| Zenoh round trip, plain TCP (reference) | 0.165 ms | 0.206 ms | 0.267 ms |
| Zenoh round trip, mutual TLS | 0.162 ms | 0.200 ms | 0.226 ms |
| Sensor to User, direct (TLS) | 0.099 ms | 0.208 ms | 0.258 ms |
| Sensor to User through the Player (TLS) | 0.204 ms | 0.305 ms | 0.357 ms |
| Time to session (connect to ready, including Player start) | 65 ms | 82 ms | 90 ms |

Message latency is well under a millisecond on both platforms; routing through the Player adds about 0.08 ms (Windows) to 0.1 ms (Linux) at the median. On Linux, TLS and plain TCP were within measurement noise of each other. Sub-millisecond results vary by a few hundredths of a millisecond between runs, and time to session by tens of milliseconds.

### Connection time

Connecting is the slowest operation, and it is roughly four times slower on Windows. Median of 10 connections, measured with an instrumented client:

| Step | Windows | Linux |
|---|---|---|
| Starting the Player process | 164 ms | 51 ms |
| Rest of the Cloud Service request | 22 ms | 5 ms |
| Opening the User's Zenoh session | 14 ms | 4 ms |
| Waiting for the direct Robot-User link and the Robot's subscriptions | 82 ms | 0.4 ms |
| Waiting for the Robot's `ready` | 13 ms | 1.3 ms |
| **Total** | **298 ms** | **68 ms** |

The direct Robot-User link is established only after both peers have connected to the Player and the Player has introduced them, which takes about 78 ms on Windows and 6 ms on Linux. Two ways to reduce connection time, not implemented:

- Keep one Player started ahead of time, removing the largest step at the cost of an idle process.
- Give the User the Robot's address up front, so the direct link does not wait for the Player's introduction.

### Findings

- `connect()` originally took about 540 ms, almost entirely spent in `zenoh.open()` waiting out Zenoh's default 500 ms peer-scouting delay. Every peer here is given an explicit address, so the delay is set to 10 ms (`ZENOH_SCOUTING_DELAY_MS` in `common/config.py`).
- Waiting for a subscriber originally polled every 10 ms. It now uses Zenoh's matching notification, which fires as soon as a subscriber appears (0.7 ms instead of 10.7 ms per wait, measured in isolation).

## Testing

```bash
python -m pytest        # 71 tests
python -m pytest -v     # list each test by name
```

On Windows, 69 tests pass and 2 are skipped: the tests for Ctrl+C isolation of the Player and for file permissions only run on Linux and macOS.

Every push runs the suite on GitHub Actions (Linux with Python 3.11 and 3.14, Windows with Python 3.14). macOS can be run manually from the Actions tab. See `.github/workflows/tests.yml`.

| File | Contents |
|---|---|
| `tests/test_units.py` | Message parsing, Player classification and alerts, the sine sensor (with a fake clock), tokens, the command parser, and Robot command handling (with a fake Zenoh sample) |
| `tests/test_integration.py` | Real Cloud Service, Robot, User and Player processes: the triangle, status from every entity, multiple robots and users, and every failure case listed above |
| `tests/test_security.py` | Each attack in the security table, and the connection limits |
| `tests/test_repo.py` | Every text file is ASCII; received data goes to the log, not the User's terminal |
| `tests/test_dashboard.py` | Merging one reading logged by several programs, and the protections on clearing logs |

Each test starts its own Cloud Service on a free port with throwaway certificates and cleans up after itself. Timeouts are shortened per test with `monkeypatch`.

The code was also checked with ruff (lint), bandit (security patterns), pip-audit (known vulnerabilities in dependencies) and vulture (unused code). Issues they reported were fixed; the remaining findings are intentional and explained in comments.

### Issues found during testing

- **Lost first command.** After the scouting delay was lowered, a command sent immediately after `connect()` was sometimes dropped, because the User published before it knew of the Robot's subscription. `connect()` now waits for that subscription. (`test_first_command_after_connect_is_not_lost`)
- **Player reported a level without data.** The Player answered `joined` with `level: normal` before receiving any reading. It now holds that reply until its first reading. (`test_player_never_claims_a_level_it_has_not_measured`)
- **User did not recover from a Cloud Service restart.** The User did not detect that its connection had closed, and its next request failed. It now ends its sessions, reports it, and reconnects. (`test_user_survives_cloud_restart`)
- **Ctrl+C reached the Player.** Ctrl+C in a terminal is delivered to every process started from it, so Players were interrupted and then killed after the 3 s timeout. Players now run in their own process group. (`test_ctrl_c_does_not_reach_the_player`)
- **New identities required a restart.** The Cloud Service read `tokens.json` only at startup. It now re-reads the file when it changes. (`test_identity_added_while_cloud_runs_can_log_in`)
- **Closing a Zenoh session could block the program.** Closing a session sometimes takes about 2 s on Windows, and it ran on the event loop, which also stopped a Robot's heartbeats. It now runs on a worker thread. (`test_slow_zenoh_close_does_not_freeze_the_robot`, `test_slow_zenoh_close_does_not_freeze_the_user`)
- **Messages lost at the start and end of a session.** `connect()` only verified the User-to-Robot direction, so the Robot's first messages could be dropped. Separately, the Robot's `leaving` message could arrive just after the Cloud Service's session-ended notice, and was lost when the User closed its session. Both are fixed; the failure reproduced on Linux under CPU load in about 1 run in 4, and did not recur in 40 runs after the fix. (`test_robot_ctrl_c_ends_session_with_robot_shutdown`, `test_user_waits_for_leaving_that_arrives_after_session_ended`)
- **A session's own subscriber counted as a listener.** The Robot and User publish and subscribe on the status topic in the same Zenoh session, and Zenoh treats the session's own subscriber as a match, so the check for a listener passed before any other peer could receive the message. This caused 6 failures in 10 runs on Windows. Data-plane messages are now delivered to remote peers only (`common/mesh.py`). (`test_own_subscriber_does_not_count_as_a_listener`)

## Assumptions

The assignment asks for assumptions to be stated in the code. Each one is marked `ASSUMPTION A<n>` where it applies.

| | Assumption | Location |
|---|---|---|
| A1 | The Player is started when a User connects (as in Functional Requirements), not when the robot starts (as in the Overview); the assignment describes both. | `cloud_service/server.py`, `start_session` |
| A2 | Control-plane message names and formats are this project's own design. | `common/protocol.py` |
| A3 | One User controls a robot at a time; a User may control several robots. | `cloud_service/server.py`, `handle_connect` |
| A4 | "User publishes data, Player receives" uses the status topic, the only topic the assignment's table allows for it. | `user/client.py`, `send_status` |
| A5 | A sensor state above 30.0 is "high". | `common/config.py` |
| A6 | Commands go from the User to the Robot directly, as in the topic table; the Player does not relay them. | `robot/client.py`, `_on_command` |
| A7 | The Robot stops its motors whenever it leaves a session or loses the Cloud Service. | `robot/client.py`, `_leave_session` |
| A8 | The sensor is a sine wave centered on the assignment's example value of 25.5. | `robot/sensor.py`, `common/config.py` |
| A9 | The events each entity publishes on the status topic. | `common/protocol.py` |
| A10 | `all <command>` sends one message per connected robot. | `user/client.py`, `send_command` |
| A11 | Identities are issued in advance by `tools/setup_security.py` (in a real fleet, during onboarding). | `tools/setup_security.py` |
| A12 | After losing the Cloud Service, the User reconnects automatically but does not resume its sessions; the operator connects again. | `user/client.py`, module docstring |

## Project layout

Each file begins with a comment describing what it does and what uses it.

```
common/
  config.py          timings, limits and defaults
  protocol.py        message formats; wait_for_subscriber()
  mesh.py            Zenoh configuration (mutual TLS), endpoints, topic names
  security.py        identities, TLS contexts, tokens, name validation
  pki.py             CA and certificate creation
  logs.py            shared log format
cloud_service/
  __main__.py        entry point
  server.py          registry, heartbeats, watchdog, sessions, security checks
  player_process.py  starting and stopping Players
  player.py          the Player program (started by the Cloud Service)
dashboard/
  logwatch.py        reading and merging the logs
  server.py          web server
  page.html          the page
robot/
  __main__.py        entry point
  client.py          registration, heartbeat, reconnection, session handling, status
  jetbot.py          FakeJetbot, with the method names of NVIDIA jetbot's Robot class
  sensor.py          SineSensor
user/
  __main__.py        entry point
  client.py          Cloud Service requests; one Zenoh session per connected robot
  cli.py             the interactive prompt
tools/
  setup_security.py  creates pki/
tests/               conftest.py, helpers.py, test_units.py, test_integration.py,
                     test_security.py, test_repo.py
benchmarks/
  latency.py
```

## Known limitations

- Tested on Linux (Python 3.11, 3.13 and 3.14) and Windows 11 (Python 3.14). macOS is supported in CI but has not been run.
- All testing has been on a single machine. The programs accept host addresses (`--host`, `--cloud-host`) and `setup_security --ip` adds machine addresses to the certificates, but a multi-machine deployment has not been tested.
- See the known gaps under Security.