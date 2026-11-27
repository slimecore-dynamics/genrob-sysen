"""The dashboard's web server: serves the page, the recent readings, and the "clear logs" action.

Used by: dashboard/__main__.py.

    GET  /                 the page (dashboard/page.html, with this run's token filled in)
    GET  /api/state        the last WINDOW seconds of readings, and the active sessions, as JSON
    POST /api/clear-logs   empty every *.log file in the logs folder (needs the token)

It listens on 127.0.0.1 only. Two checks keep other web pages open in the same browser from
using it: every request must name this machine in its Host header (which blocks DNS rebinding),
and the one action that changes anything must carry a random token that only the page knows.
"""
import json
import logging
import secrets
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from common.config import HIGH_THRESHOLD
from dashboard.logwatch import LogWatcher, Readings

log = logging.getLogger("Dashboard")

PAGE = Path(__file__).with_name("page.html")
WINDOW = 60.0           # seconds of readings shown on the graph
POLL_INTERVAL = 0.5     # seconds between checks of the log files
# A header name, not a secret (bandit B105 mistakes it for a password).
TOKEN_HEADER = "X-Dashboard-Token"  # nosec B105


class Dashboard:
    def __init__(self, logs_dir, host: str = "127.0.0.1", port: int = 8800):
        self.logs_dir = Path(logs_dir)
        self.token = secrets.token_urlsafe(32)
        self.readings = Readings()
        self.watcher = LogWatcher(self.logs_dir, self.readings)
        self.watch_lock = threading.Lock()          # the watcher and "clear logs" take turns
        self.stopping = threading.Event()
        self.httpd = ThreadingHTTPServer((host, port), self._handler_class())
        self.port = self.httpd.server_address[1]
        self.allowed_hosts = {f"127.0.0.1:{self.port}", f"localhost:{self.port}"}

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/"

    def start(self) -> None:
        """Serve and watch the logs in background threads."""
        threading.Thread(target=self._watch, daemon=True).start()
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self.stopping.set()
        self.httpd.shutdown()
        self.httpd.server_close()

    def _watch(self) -> None:
        while not self.stopping.is_set():
            with self.watch_lock:
                self.watcher.poll()
            self.stopping.wait(POLL_INTERVAL)

    def clear_logs(self) -> list[str]:
        """Empty every log file. Programs still running keep appending to them from the start."""
        cleared = []
        with self.watch_lock:
            for path in sorted(self.logs_dir.glob("*.log")):
                try:
                    with open(path, "r+b") as f:
                        f.truncate(0)
                    cleared.append(path.name)
                except OSError as err:
                    log.warning("couldn't clear %s: %s", path, err)
            self.watcher.reset()
        return cleared

    def state(self) -> dict:
        snap = self.readings.snapshot(time.time(), WINDOW)
        return {"window": WINDOW, "threshold": HIGH_THRESHOLD, **snap}

    def _handler_class(self):
        dashboard = self

        class Handler(BaseHTTPRequestHandler):
            # do_GET, do_POST and log_message are called by http.server, by name.
            def do_GET(self):
                if not self._from_this_machine():
                    return
                if self.path == "/":
                    page = PAGE.read_text(encoding="ascii").replace("__TOKEN__", dashboard.token)
                    self._send(HTTPStatus.OK, page.encode("ascii"), "text/html; charset=us-ascii")
                elif self.path == "/api/state":
                    self._send_json(HTTPStatus.OK, dashboard.state())
                elif self.path == "/favicon.ico":
                    self._send(HTTPStatus.NO_CONTENT, b"", "image/x-icon")
                else:
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

            def do_POST(self):
                if not self._from_this_machine():
                    return
                if self.path != "/api/clear-logs":
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                elif not secrets.compare_digest(self.headers.get(TOKEN_HEADER, ""), dashboard.token):
                    self._send_json(HTTPStatus.FORBIDDEN, {"error": "missing or wrong token"})
                else:
                    self._send_json(HTTPStatus.OK, {"cleared": dashboard.clear_logs()})

            def _from_this_machine(self) -> bool:
                if self.headers.get("Host", "") in dashboard.allowed_hosts:
                    return True
                self._send_json(HTTPStatus.FORBIDDEN, {"error": "unexpected Host header"})
                return False

            def _send_json(self, status, body) -> None:
                self._send(status, json.dumps(body).encode("ascii"), "application/json")

            def _send(self, status, body: bytes, content_type: str) -> None:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, fmt, *args):     # keep the terminal quiet; requests go to debug
                log.debug(fmt, *args)

        return Handler
