"""Entry point: python -m user [--name NAME] [--log-file FILE] [--host HOST] [--cloud-host H] [--cloud-port P] [--pki-dir PKI]

Connects UserClient (user/client.py) to the Cloud Service, then runs the command prompt (user/cli.py).
Needs its identity from: python -m tools.setup_security --users NAME (see README).

The terminal is only for typing commands and seeing their results. Everything received (sensor,
processed, status) goes to the log file, default logs/user-<name>.log. To watch it live, in a
second terminal:  Get-Content logs\\user-user.log -Wait   (PowerShell)  or  tail -f logs/user-user.log
"""
import argparse
import asyncio
import sys
from pathlib import Path

import zenoh

from common import logs
from common.config import DEFAULT_CLOUD_PORT, DEFAULT_HOST, DEFAULT_PKI_DIR, cloud_url
from user.cli import run_cli
from user.client import ControlError, UserClient


async def main(args) -> int:
    try:
        client = UserClient(args.name, cloud_url(args.cloud_host, args.cloud_port), args.host, args.pki_dir)
        await client.start()
    except (OSError, ControlError) as err:   # missing files, Cloud unreachable, or token refused
        print(f"Can't start: {err}", file=sys.stderr)
        return 1
    print(f"Received data is logged to {args.log_file}")
    try:
        await run_cli(client)
    finally:
        await client.shutdown()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Operator terminal")
    parser.add_argument("--name", default="user")
    parser.add_argument("--log-file", type=Path, default=None, help="default: logs/user-<name>.log")
    parser.add_argument("--host", default=DEFAULT_HOST, help="address this user's Zenoh sessions listen on")
    parser.add_argument("--cloud-host", default=DEFAULT_HOST)
    parser.add_argument("--cloud-port", type=int, default=DEFAULT_CLOUD_PORT)
    parser.add_argument("--pki-dir", type=Path, default=DEFAULT_PKI_DIR)
    args = parser.parse_args()
    args.log_file = args.log_file or logs.LOG_DIR / f"user-{args.name}.log"
    logs.setup(args.log_file, console=False)      # keep the terminal free for typing
    zenoh.init_log_from_env_or("error")
    try:
        sys.exit(asyncio.run(main(args)))
    except KeyboardInterrupt:
        pass
