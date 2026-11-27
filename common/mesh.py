"""Zenoh helpers for the data plane (the Robot / User / Player triangle).

Used by: robot/client.py, user/client.py, cloud_service/player.py, benchmarks/latency.py.

Every program passes its Identity, so links are tls/ with mutual TLS: a peer must present a
certificate signed by our CA to connect. identity=None (plain tcp/) exists only so the
benchmark can measure what TLS costs; the programs never use it.
"""
import json
import socket

import zenoh

from common.config import ZENOH_SCOUTING_DELAY_MS
from common.security import Identity


def endpoint(port: int, host: str, tls: bool = True) -> str:
    """Zenoh address string, e.g. tls/127.0.0.1:7447."""
    return f"{'tls' if tls else 'tcp'}/{host}:{port}"


def free_port(host: str) -> int:
    """Ask the OS for an unused port. Small race: another program could take it before we bind."""
    with socket.socket() as s:
        s.bind((host, 0))
        return s.getsockname()[1]


def make_config(host: str, listen_port: int, identity: Identity | None, connect_to=()) -> zenoh.Config:
    """Peer-mode config: listen on one port, connect to the given endpoints.

    Multicast scouting is off, so nodes only find each other through addresses the Cloud
    Service hands out. Zenoh's gossip then links Robot and User directly.
    """
    cfg = zenoh.Config()
    cfg.insert_json5("mode", '"peer"')
    cfg.insert_json5("listen/endpoints", json.dumps([endpoint(listen_port, host, identity is not None)]))
    cfg.insert_json5("connect/endpoints", json.dumps(list(connect_to)))
    cfg.insert_json5("scouting/multicast/enabled", "false")
    cfg.insert_json5("scouting/delay", str(ZENOH_SCOUTING_DELAY_MS))
    # Publish robot/... messages to other peers only, never back to this session's own subscribers.
    # No program here needs its own messages, and it fixes a real bug: a session that subscribes and
    # publishes on the same topic (status) counted its own subscriber as a match, so
    # wait_for_subscriber() returned before any other peer could hear, and the next message could be
    # lost (found on Windows: test_player_never_claims_a_level_it_has_not_measured, 6 of 10 failed).
    cfg.insert_json5("qos/publication", json.dumps(
        [{"key_exprs": ["robot/**"], "config": {"allowed_destination": "remote"}}]))
    if identity is not None:
        key, cert = str(identity.key), str(identity.cert)
        cfg.insert_json5("transport/link/tls", json.dumps({
            "root_ca_certificate": str(identity.ca),
            "enable_mtls": True,                                  # the other side must present a cert too
            "listen_private_key": key, "listen_certificate": cert,
            "connect_private_key": key, "connect_certificate": cert,
        }))
    return cfg


def topic(robot_id: str, name: str) -> str:
    """Topic names from the assignment: robot/{robot_id}/{sensor|command|processed|status}."""
    return f"robot/{robot_id}/{name}"


def open_session(host: str, identity: Identity | None, connect_to=()) -> zenoh.Session:
    """Open a Zenoh session listening on a free port."""
    return zenoh.open(make_config(host, free_port(host), identity, connect_to))
