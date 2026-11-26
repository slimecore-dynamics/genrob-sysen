"""Identities, TLS contexts and tokens.

Used by: cloud_service/, robot/, user/, common/mesh.py.
Explained in Notebook 3, sections 5-8.

PKI folder layout (created by tools/setup_security.py):
    pki/ca/cert.pem                      the CA everyone trusts (ca/key.pem signs new identities)
    pki/cloud/{cert,key}.pem             Cloud Service (wss:// server)
    pki/player/{cert,key}.pem            every Player (Zenoh mutual TLS)
    pki/robots/<robot_id>/{cert,key}.pem, token
    pki/users/<name>/{cert,key}.pem, token
    pki/tokens.json                      the Cloud's fingerprints of every token (no actual tokens)
"""
import hashlib
import hmac
import json
import os
import secrets
import ssl
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Identity:
    """Where one program's certificate, private key, and the trusted CA certificate live."""
    cert: Path
    key: Path
    ca: Path


def identity_dir(pki_dir, kind: str, name: str = "") -> Path:
    """kind is 'cloud', 'player', 'robots' or 'users'; robots and users also need a name."""
    base = Path(pki_dir)
    return base / kind if kind in ("cloud", "player") else base / kind / name


def load_identity(pki_dir, kind: str, name: str = "") -> Identity:
    folder = identity_dir(pki_dir, kind, name)
    ident = Identity(folder / "cert.pem", folder / "key.pem", Path(pki_dir) / "ca" / "cert.pem")
    for path in (ident.cert, ident.key, ident.ca):
        if not path.is_file():
            raise FileNotFoundError(f"{path} is missing. Run: python -m tools.setup_security (see README)")
    return ident


def read_token(pki_dir, kind: str, name: str) -> str:
    path = identity_dir(pki_dir, kind, name) / "token"
    if not path.is_file():
        raise FileNotFoundError(f"{path} is missing. Run: python -m tools.setup_security (see README)")
    return path.read_text().strip()


# ---------------------------------------------------------------- TLS for the WebSocket control plane
def server_tls_context(ident: Identity) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(ident.cert, ident.key)
    return ctx


def client_tls_context(ca_cert: Path) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)     # checks the certificate and the host name
    ctx.load_verify_locations(ca_cert)
    return ctx


# ---------------------------------------------------------------- tokens
def new_token() -> str:
    return secrets.token_urlsafe(32)


def fingerprint(token: str) -> str:
    # A fast hash is fine because tokens are long and random (not human-chosen passwords).
    return hashlib.sha256(token.encode()).hexdigest()


class TokenStore:
    """What the Cloud Service keeps: id -> fingerprint of that id's token.

    A store loaded from a file re-reads it whenever the file changes, so a robot or user added with
    tools/setup_security.py while the Cloud Service is running can log in without a restart.
    """

    def __init__(self, robots: dict | None = None, users: dict | None = None):
        self.robots: dict[str, str] = robots or {}
        self.users: dict[str, str] = users or {}
        self.path: Path | None = None
        self._stamp = None              # (mtime, size) of the file when it was last read

    @classmethod
    def load(cls, path) -> "TokenStore":
        path = Path(path)
        if not path.is_file():
            raise FileNotFoundError(f"{path} is missing. Run: python -m tools.setup_security (see README)")
        store = cls()
        store.path = path
        store.refresh()
        return store

    def refresh(self) -> None:
        """Re-read the file if it changed since the last read. Keeps the old tables if it can't."""
        if self.path is None:
            return
        try:
            st = self.path.stat()
            stamp = (st.st_mtime_ns, st.st_size)
            if stamp == self._stamp:
                return
            data = json.loads(self.path.read_text())
        except (OSError, ValueError):
            return          # missing or half-written right now: keep what we had, try again next time
        self.robots = data.get("robots", {})
        self.users = data.get("users", {})
        self._stamp = stamp

    def save(self, path) -> None:
        # Write a temporary file and swap it in, so a reader never sees a half-written file.
        path = Path(path)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps({"robots": self.robots, "users": self.users}, indent=2))
        os.replace(tmp, path)

    @staticmethod
    def _check(table: dict, name, token) -> bool:
        expected = table.get(name)
        if expected is None or not isinstance(token, str):
            return False
        return hmac.compare_digest(fingerprint(token), expected)   # constant-time comparison

    def check_robot(self, robot_id, token) -> bool:
        self.refresh()
        return self._check(self.robots, robot_id, token)

    def check_user(self, name, token) -> bool:
        self.refresh()
        return self._check(self.users, name, token)
