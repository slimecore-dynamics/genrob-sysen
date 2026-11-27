"""Entry point: python -m cloud_service [--host HOST] [--port PORT] [--pki-dir PKI]

Starts CloudService (cloud_service/server.py) and runs until Ctrl+C,
then shuts down every running Player before exiting.
Needs pki/ from: python -m tools.setup_security (see README).
"""
import argparse
import asyncio
import contextlib
from pathlib import Path

import zenoh

from cloud_service.server import CloudService
from common import logs
from common.config import DEFAULT_CLOUD_PORT, DEFAULT_HOST, DEFAULT_PKI_DIR


async def main(args) -> None:
    cloud = CloudService(args.host, args.port, args.pki_dir)
    await cloud.start()
    try:
        await asyncio.Future()   # run until cancelled (Ctrl+C)
    finally:
        await cloud.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cloud Service: registry, heartbeats, Player spawning")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_CLOUD_PORT)
    parser.add_argument("--pki-dir", type=Path, default=DEFAULT_PKI_DIR)
    parser.add_argument("--log-file", type=Path, default=logs.LOG_DIR / "cloud.log")
    args = parser.parse_args()
    logs.setup(args.log_file)
    zenoh.init_log_from_env_or("error")
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main(args))
