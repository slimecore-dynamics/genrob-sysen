"""Entry point: python -m dashboard [--logs-dir logs] [--port 8800] [--no-browser]

A read-only, live view of the programs' log files: one graph line per robot over the last
60 seconds, with readings the Player classified as high marked, a SOLO button per robot, and the
active sessions. It reads the logs only; it never starts, stops or talks to the programs.
Stop it with Ctrl+C.
"""
import argparse
import contextlib
import sys
import threading
import webbrowser
from pathlib import Path

from common import logs
from dashboard.server import Dashboard


def main() -> int:
    parser = argparse.ArgumentParser(description="Live graph of robot sensor readings, from the logs")
    parser.add_argument("--logs-dir", type=Path, default=logs.LOG_DIR)
    parser.add_argument("--port", type=int, default=8800)
    parser.add_argument("--no-browser", action="store_true", help="don't open a browser window")
    args = parser.parse_args()
    logs.setup(None)
    try:
        dashboard = Dashboard(args.logs_dir, port=args.port)
    except OSError as err:
        print(f"Can't listen on port {args.port} ({err}). Try --port with another number.", file=sys.stderr)
        return 1
    dashboard.start()
    print(f"Dashboard at {dashboard.url}  (reading {args.logs_dir.resolve()}; Ctrl+C to stop)")
    if not args.no_browser:
        webbrowser.open(dashboard.url)
    idle = threading.Event()
    with contextlib.suppress(KeyboardInterrupt):
        while not idle.wait(0.5):    # short waits: on Windows, Ctrl+C can't interrupt an endless one
            pass
    dashboard.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
