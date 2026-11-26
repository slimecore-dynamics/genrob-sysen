# Robot <-> User <-> Player real-time data channel

A Cloud Service keeps track of robots. When an operator (User) connects to a robot, the Cloud Service spawns a **Player** process for that session, and Robot, User and Player form a peer-to-peer triangle: each publishes to, and subscribes to, the other two directly.

```
        Player (cloud)
         /        \
     Robot -------- User
```

## Quick start

Python 3.11+.

```bash
pip install -r requirements.txt

# once: create a certificate authority, certificates and tokens (written to pki/, which git ignores)
python -m tools.setup_security --robots robot-1 robot-2 --users user
```

Then, in three terminals from this folder:

```bash
python -m cloud_service                       # 1. Cloud Service
python -m robot --robot-id robot-1            # 2. Robot (sine-wave sensor; see --amplitude, --period)
python -m user                                # 3. User: an interactive prompt (type help)
```

In the User prompt:

```
list
connect robot-1
robot-1 status operator here
robot-1 forward
robot-1 stop
quit
```

The User terminal only shows your commands and their results. Everything received (sensor, processed, status) is written to `logs/user-<name>.log`. To watch it live, in a second terminal:

```
Get-Content logs\user-user.log -Wait      # PowerShell
tail -f logs/user-user.log                # macOS/Linux
```

The Cloud Service and each Robot also log to `logs/cloud.log` and `logs/<robot-id>.log`. Use `--log-file` to change the location.

Scripted demos: `python -m user < demo.txt` (one robot), or start `robot-2` as well and run `python -m user < demo_multi.txt` (one user, two robots). To see the Player's processing differ between robots, give them different sensors, e.g. `python -m robot --robot-id robot-1 --amplitude 3` (never "high") and `python -m robot --robot-id robot-2 --amplitude 8` (crosses "high" every cycle).

Every program logs each message it receives. The Player's output appears in the Cloud Service's terminal as `[Player]`.

## The five functional requirements

| # | Requirement | Where it happens |
|---|---|---|
| 1 | "When a robot starts, it pings the cloud server and starts sending heartbeat messages" | `RobotClient._connect_once` sends `register` with its token; `_heartbeat_loop` sends `heartbeat` every second (`robot/client.py`) |
| 2 | "The cloud service should maintain connection and status of this robot" | `CloudService.robot_handler` and `_watchdog` keep each robot's state: `online`, `in_session`, `reconnecting`, `offline` (`cloud_service/server.py`) |
| 3 | "User connects to cloud server and can: list connected robots; connect to one of the robot" | `UserClient.start` / `list_robots` / `connect` (`user/client.py`); prompt commands `list`, `connect <robot_id>` |
| 4 | "When user requests to connect to a robot, cloud server spawns up a pub/sub server, after success, user and robot are also directed to spawn up pub/sub server respectively with information about the pub/sub server on the cloud side" | `CloudService.start_session` spawns the Player (`cloud_service/player_process.py`), waits for its `READY <endpoint>`, then sends that endpoint to the Robot (`join_session`) and the User (`session_ready`). Each opens its own Zenoh session, listening on its own port, connected to the Player. |
| 5 | "Three pub/sub servers should connect and form a triangle network mesh. Each entity should start to publish messages and subscribe to each other's publishing channel directly" | Zenoh peer gossip links Robot and User directly once both are connected to the Player. `test_robot_and_user_are_linked_directly` checks the direct link exists. |

### The demonstration

| Assignment | Here |
|---|---|
| Robot publishes sensor data every second: `{"state": 25.5}` | `{"state": <sine wave centered on 25.5>}` every second (assumption A8) |
| Player receives sensor data, processes, republishes: `{"state": 25.5, "status": "normal"}` | `classify()` in `cloud_service/player.py`, published on `processed` |
| User publishes data, Player receives data | `robot-1 status <text>`, on the status topic (assumption A4) |
| User publishes `{"command": "stop"}`, `{"command": "forward"}` | `robot-1 stop`, `robot-1 forward`, or `all <command>` |
| Robot receives and executes the command, jetbot-style | `FakeJetbot` (`robot/jetbot.py`) has jetbot's `Robot` method names |
| Each entity prints all data it receives | each program's log; `test_player_prints_what_it_receives` |

## Architecture

Two kinds of traffic, kept separate:

| | Control plane | Data plane |
|---|---|---|
| What | register, heartbeat, list, connect, disconnect, session start/end | sensor, command, processed, status |
| Between | each program <-> Cloud Service | Robot <-> User <-> Player, directly |
| Transport | WebSocket over TLS (`wss://`, paths `/robot`, `/user`) | Zenoh, peer mode, mutual TLS |

**Why Zenoh:** peer-to-peer pub/sub with no broker in the data path. Robot and User are only given the Player's endpoint; Zenoh's peer gossip then connects them directly, which forms the triangle the spec asks for. It's also what General Robotics' GRID platform uses: its client's `connect()` takes a combined `{"player": ..., "edge": ...}` Zenoh config.

**Why WebSocket for control:** the Cloud Service has to push to robots ("join this session now"), not just answer requests. GRID's client also uses a signaling WebSocket, with separate robot, user and player signaling URLs.

### Flow

1. Robot connects to `/robot` and sends `register` with its token, then `heartbeat` every second.
2. User connects to `/user`, sends `hello` with its token, then `list_robots` and `connect robot-1`.
3. The Cloud Service starts `python -m cloud_service.player` on a free port and waits for `READY <endpoint>`.
4. The endpoint goes to the Robot (`join_session`) and the User (`session_ready`).
5. Robot and User open Zenoh sessions pointed at the Player. Each announces `joined` on status once it can be heard; the others reply `ready` with their current state. The User's `connect()` returns only once its commands can reach the Robot.

### Topics

| Topic | Publisher | Subscriber(s) | Example |
|---|---|---|---|
| `robot/{id}/sensor` | Robot | Player, User | `{"state": 31.2}` |
| `robot/{id}/command` | User | Robot | `{"command": "forward"}` |
| `robot/{id}/processed` | Player | User | `{"state": 31.2, "status": "high"}` |
| `robot/{id}/status` | all | all | `{"from": "player", "status": "alert", "level": "high", "state": 31.2}` |

What each entity says on status (assumption A9):

| Who | Events |
|---|---|
| Robot | `joined`, `ready {motors}` (reply to others' `joined`), `moving {command}`, `stopped`, `rejected_command {command}`, `leaving` |
| Player | `ready {level}` (reply to every `joined`, sent once it has a reading), `alert {level, state}` when the level changes between normal and high, or when the first reading is high, `stopping` |
| User | `joined`, then anything the operator types |

The `ready` replies matter because Zenoh drops any message published before the sender knows of a subscriber. Anything announced before someone joined never reached them, so whoever joins last is told the current state.

Per-sensor topics (e.g. `robot/{id}/sensor/camera`) fit under the same prefix, and `robot/{id}/sensor/**` subscribes to all of them. That's the one-topic-per-sensor pattern GRID uses in its Isaac Sim bridge.

## Bonus 1: Security

```bash
python -m tools.setup_security --robots robot-1 --users user
```

That creates a private certificate authority (CA), one certificate per program, and one token per robot and user, under `pki/` (git-ignored).

| Threat | Protection | Test |
|---|---|---|
| Reading control traffic | `wss://` only; plain `ws://` is refused | `test_plain_websocket_is_rejected` |
| A fake Cloud Service | clients trust only our CA | `test_client_refuses_a_server_it_does_not_trust` |
| Pretending to be a robot or user | each must send its token; the Cloud stores only SHA-256 fingerprints and compares in constant time | `test_robot_with_wrong_token_is_refused`, `test_robot_cannot_use_another_robots_token`, `test_user_with_wrong_token_is_refused` |
| Taking over a session during the reconnect grace period | resuming needs the robot's token too | `test_impostor_cannot_take_over_during_reconnect_grace` |
| Joining a triangle and sending commands | Zenoh mutual TLS: every peer must present a certificate signed by our CA | `test_untrusted_zenoh_peer_cannot_join_or_command`, `test_unencrypted_zenoh_peer_cannot_join` |
| Holding connections open without identifying | closed after `FIRST_MESSAGE_TIMEOUT` (5 s) | `test_silent_connection_is_closed` |
| Opening too many connections | refused past `MAX_CONNECTIONS` (code 1013, "try again later") | `test_connection_cap` |

**Not covered, and how it would be:**
- **Isolation between sessions.** Any certificate from our CA could join another session's triangle if it learned that Player's address. The Cloud Service only gives addresses to the right Robot and User, but the fix is authorization: Zenoh access-control rules matching certificate common names (e.g. only `user` may publish `robot/robot-1/command`), or per-session credentials.
- **Revocation.** Deleting a token's fingerprint from tokens.json blocks new logins right away (no restart), but doesn't disconnect a robot or user that's already connected. Short-lived certificates or a revocation list would fix that.
- **Message rate limits.** Connections are capped, but an authenticated client can still send messages as fast as it likes.
- **Key storage.** `pki/ca/key.pem` can mint any identity; in a real deployment it lives offline, and each robot receives only its own folder.

## Bonus 2: Scalability: multiple robots for multiple users

**What works now (tested):**
- Many robots, each with its own session and its own Player process. Topics are per-robot, so sessions don't see each other's data (`test_two_users_two_robots_are_isolated`).
- One user controlling several robots: `connect robot-1`, `connect robot-2`, then `robot-2 forward` for one or `all stop` for every connected robot (`test_one_user_controls_several_robots`). `all` sends one message per robot. They're sent together, not executed at the same instant.
- One user per robot at a time; a second user gets `robot_busy` (assumption A3).

**Costs that grow with scale:**
- One Player process per session. Starting it is most of the time to session (Bonus 4): about 68 ms of 86 ms on Linux, and 127 ms of 221 ms on Windows.
- One heartbeat per robot per second to the Cloud Service, and one open WebSocket per robot and per user.

**Scaling further:**
1. **Players off the Cloud Service's machine.** `spawn_player` is the only thing that starts them. Swapping it for "start a container" (one pod per session in Kubernetes) spreads Players across machines without changing the protocol: the Player still reports `READY <endpoint>`.
2. **More than one Cloud Service.** The registry (`self.robots`) is in memory, so a second instance wouldn't see the first's robots. Moving it to a shared store (e.g. Redis), with each robot's WebSocket pinned to one instance, would let instances share the load.
3. **Robots on other networks.** Peer-to-peer links need each peer to reach the others' addresses. Across NATs, Zenoh routers (or a relay) would carry the traffic instead.
4. **Several users on one robot** (one controlling, others watching) would mean letting more than one User join a session's triangle, with the access-control rules above deciding who may publish commands.

## Bonus 3: Error handling and resource management

| Situation | Behavior | Test |
|---|---|---|
| Robot not found | `robot_not_found` | `test_robot_not_found` |
| Duplicate registration | second robot refused (`duplicate_registration`) | `test_duplicate_registration_refused` |
| Player spawn failure | Player killed, `player_spawn_failed`, robot back to `online` | `test_player_spawn_failure` |
| Robot already in a session | second user gets `robot_busy` | `test_second_user_gets_robot_busy` |
| Robot reconnecting | `robot_reconnecting` | `test_connect_while_robot_reconnecting` |
| Heartbeats stop | offline after 3 missed in a row; session ends, Player stopped, both sides told, motors stopped | `test_heartbeat_timeout_ends_session` |
| Robot connection drops | held as `reconnecting` for 5 s; if it re-registers in time it resumes the same session and Player | `test_drop_and_reconnect_resumes_same_session`, `test_drop_without_return_ends_session` |
| Robot shuts down on purpose | `goodbye`: offline immediately, no grace period | `test_goodbye_goes_offline_without_grace_period` |
| Cloud Service restarts | Robot and User both retry with backoff (1, 2, 4 ... 10 s) and reconnect; the User's sessions end and it says so (assumption A12) | `test_robot_reconnects_after_cloud_restart`, `test_user_survives_cloud_restart` |
| User leaves | its sessions end, Players stopped | `test_user_leaving_ends_session_and_stops_player` |
| Robot stopped with Ctrl+C mid-session | says goodbye; the User is told `robot_offline:robot_shutdown` | `test_robot_ctrl_c_ends_session_with_robot_shutdown` |
| Ctrl+C in the Cloud Service's terminal | only the Cloud Service is interrupted; it stops each Player in order (Players run in their own process group) | `test_ctrl_c_does_not_reach_the_player` (Linux/macOS) |
| Robot or user added while the Cloud Service runs | accepted without a restart (the Cloud re-reads tokens.json when it changes) | `test_identity_added_while_cloud_runs_can_log_in` |
| Malformed or unknown message | logged and refused (`bad_request`, `must_register_first`, `rejected_command`); never crashes a callback | `test_bad_request_is_refused`, `test_register_message_must_come_first`, `test_bad_commands_are_ignored` |
| Unexpected error in the Robot | logged with a traceback; the Robot keeps reconnecting | |


Resources: every session ends through one function (`CloudService.end_session`), which stops the Player politely (close its stdin), then forcefully if it hangs. Zenoh subscribers, publishers and sessions are released on every exit path, and test fixtures stop every server and process even when a test fails.

## Bonus 4: Latency benchmark

```bash
python -m benchmarks.latency
```

Localhost, closed loop (each message waits for its reply), 50 warm-up messages discarded, percentiles rather than averages.

Windows 11, Python 3.14.6:

| measurement | p50 | p95 | p99 |
|---|---|---|---|
| Zenoh round trip, plain TCP (for comparison only) | 0.050 ms | 0.069 ms | 0.096 ms |
| Zenoh round trip, mutual TLS | 0.092 ms | 0.116 ms | 0.170 ms |
| sensor -> User, direct (TLS) | 0.080 ms | 0.094 ms | 0.120 ms |
| sensor -> Player -> User (TLS) | 0.107 ms | 0.153 ms | 0.182 ms |
| time to session (connect -> ready, incl. starting a Player) | 221 ms | 240 ms | 241 ms |

Linux (cloud VM), Python 3.13:

| measurement | p50 | p95 | p99 |
|---|---|---|---|
| Zenoh round trip, plain TCP (for comparison only) | 0.13 ms | 0.16 ms | 0.18 ms |
| Zenoh round trip, mutual TLS | 0.15 ms | 0.20 ms | 0.29 ms |
| sensor -> User, direct (TLS) | 0.14 ms | 0.24 ms | 0.31 ms |
| sensor -> Player -> User (TLS) | 0.23 ms | 0.34 ms | 0.46 ms |
| time to session (connect -> ready, incl. starting a Player) | 86 ms | 104 ms | 114 ms |

Messages are fast on both: going through the Player adds about 0.03 ms (Windows) to 0.09 ms (Linux) at the median compared with the direct path. Time to session is the slow step on both, and it is about 2.5x slower on Windows. Starting the Player process alone (median of 5 starts) takes 127 ms on Windows and 68 ms on Linux, which accounts for 59 ms of the 135 ms difference; the rest hasn't been broken down. These are single-machine numbers: they show the software's own overhead. Over a real network, add the network's round trip.

**What the benchmark found:** `connect()` originally took about 540 ms, almost all of it in `zenoh.open()` waiting out Zenoh's default 500 ms peer-scouting delay. Every peer here gets an explicit address, so `ZENOH_SCOUTING_DELAY_MS` is 10 (`common/config.py`).

## Bonus 5: Testing

```bash
python -m pytest        # 79 tests; on Windows 78 pass and 1 is skipped (the Player Ctrl+C test runs only on Linux and macOS).
python -m pytest -v     # each test by name
```

Every push runs the tests on GitHub Actions (Linux with Python 3.11 and 3.14, Windows with 3.14); macOS runs on demand. See `.github/workflows/tests.yml`.

| File | What |
|---|---|
| `tests/test_units.py` | message parsing, Player classification and alerts, the sine sensor (with a fake clock), tokens, the prompt's parser, Robot command handling (with a fake Zenoh sample) |
| `tests/test_integration.py` | real Cloud Service, Robot, User and Player processes: the triangle, status from every entity, multiple robots and users, every failure case above |
| `tests/test_security.py` | each attack in the security table, and the flooding limits |
| `tests/test_repo.py` | every text file is ASCII; received data goes to the log, not the User's terminal |

Each test starts its own Cloud Service on a free port with throwaway certificates, and cleans up after itself. Timeouts are shortened per test with `monkeypatch`.

**Bugs testing on Windows found** (the first by pytest, the next three by hand, the last by GitHub Actions):
- **Player claimed a level it hadn't measured.** It answered `joined` with `level: normal` before any reading arrived. It now holds that reply until its first reading (`test_player_never_claims_a_level_it_has_not_measured`).
- **User stranded when the Cloud Service restarted.** It didn't notice the connection had closed; the next request failed. It now ends its sessions, says so, and reconnects (`test_user_survives_cloud_restart`).
- **Ctrl+C reached the Player.** Ctrl+C in a terminal goes to every process started from it, so the Player was interrupted mid-run and then force-killed after 3 s. It now runs in its own process group (`test_ctrl_c_does_not_reach_the_player`).
- **New users refused until a restart.** The Cloud Service read tokens.json only at startup. It now re-reads it when it changes (`test_identity_added_while_cloud_runs_can_log_in`).
- **A slow Zenoh close froze the program.** Closing a Zenoh session sometimes takes about 2 s on Windows, and it ran on the event loop, so nothing else could happen meanwhile (a frozen Robot also stops its heartbeats). It now runs in a worker thread (`test_slow_zenoh_close_does_not_freeze_the_robot`, `test_slow_zenoh_close_does_not_freeze_the_user`).

## Assumptions

The assignment asks for assumptions to be stated in the code. Each is marked `ASSUMPTION A<n>` where it applies:

| | Assumption | Where |
|---|---|---|
| A1 | The Player is spawned when a User connects (Functional Requirements), not when the robot turns on (Overview); the assignment says both | `cloud_service/server.py`, `start_session` |
| A2 | Control-plane message names and shapes are this project's design | `common/protocol.py` |
| A3 | One User controls a robot at a time; a User may hold several robots | `cloud_service/server.py`, `handle_connect` |
| A4 | "User publishes data, Player receives" uses the status topic, the only one the topic table allows | `user/client.py`, `send_status` |
| A5 | Sensor state above 30.0 is "high" | `common/config.py` |
| A6 | Commands go User -> Robot directly (topic table); the Player doesn't relay them | `robot/client.py`, `_on_command` |
| A7 | The Robot stops its motors whenever it leaves a session or loses the Cloud Service | `robot/client.py`, `_leave_session` |
| A8 | The sensor is a sine wave centered on the assignment's 25.5 | `robot/sensor.py`, `common/config.py` |
| A9 | What each entity publishes on status | `common/protocol.py` |
| A10 | `all <command>` sends one message per connected robot | `user/client.py`, `send_command` |
| A11 | Identities are issued ahead of time by `tools/setup_security.py` (in a real fleet, at onboarding) | `tools/setup_security.py` |
| A12 | After losing the Cloud Service, the User reconnects by itself but doesn't resume sessions; the operator connects again | `user/client.py`, module docstring |

## Code map

Each file starts with a comment saying what it does and what calls it.

```
common/
  config.py          every timing, limit and default
  protocol.py        message formats; wait_for_subscriber()
  mesh.py            Zenoh config (mutual TLS), endpoints, topic names
  security.py        identities, TLS contexts, tokens
  pki.py             CA and certificate creation
  logs.py            shared log format
cloud_service/
  __main__.py        entry point -> CloudService.start()
  server.py          registry, heartbeats, watchdog, sessions, security checks
  player_process.py  spawn_player() / PlayerProcess.stop()
  player.py          the Player program (spawned, never run by hand)
robot/
  __main__.py        entry point -> RobotClient.run()
  client.py          register, heartbeat, reconnect, join the triangle, report status
  jetbot.py          FakeJetbot, same method names as NVIDIA jetbot's Robot
  sensor.py          SineSensor
user/
  __main__.py        entry point -> UserClient.start() + run_cli()
  client.py          requests to the Cloud, one triangle per connected robot
  cli.py             the operator prompt (parse_line, execute)
tools/
  setup_security.py  creates pki/
tests/               conftest.py (fixtures), helpers.py, test_units.py, test_integration.py, test_security.py, test_repo.py
benchmarks/
  latency.py
```

## Known limitations

- Tested on Linux (Python 3.11, 3.13, 3.14) and Windows 11 (Python 3.14).
- Run on one machine. The code takes host addresses (`--host`, `--cloud-host`) and `setup_security --ip` adds machine addresses to the certificates, but a multi-machine run hasn't been done.
- See "Not covered" under Security.
