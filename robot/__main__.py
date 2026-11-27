"""Entry point:
    python -m robot --robot-id ROBOT_ID [--amplitude A] [--period P] [--center C]
                    [--host HOST] [--cloud-host H] [--cloud-port P] [--pki-dir PKI]

Runs RobotClient (robot/client.py) until Ctrl+C, then says goodbye to the Cloud Service.
Needs its identity from: python -m tools.setup_security --robots ROBOT_ID (see README).
"""
import argparse
import asyncio
import contextlib
import sys
from pathlib import Path

import zenoh

from common import logs
from common.config import (
    DEFAULT_CLOUD_PORT,
    DEFAULT_HOST,
    DEFAULT_PKI_DIR,
    SENSOR_AMPLITUDE,
    SENSOR_CENTER,
    SENSOR_PERIOD,
    cloud_url,
)
from robot.client import RegistrationRefused, RobotClient
from robot.sensor import SineSensor


async def main(args) -> int:
    sensor = SineSensor(args.center, args.amplitude, args.period)
    try:
        robot = RobotClient(args.robot_id, cloud_url(args.cloud_host, args.cloud_port), args.host,
                            args.pki_dir, sensor)
    except FileNotFoundError as err:        # no certificate or token for this robot id
        print(err, file=sys.stderr)
        return 1
    try:
        await robot.run()
    except RegistrationRefused as err:
        print(f"Registration refused: {err}", file=sys.stderr)
        return 1
    finally:
        await robot.shutdown()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulated jetbot-style robot")
    parser.add_argument("--robot-id", required=True)
    parser.add_argument("--amplitude", type=float, default=SENSOR_AMPLITUDE, help="sine wave amplitude")
    parser.add_argument("--period", type=float, default=SENSOR_PERIOD, help="seconds per sine cycle")
    parser.add_argument("--center", type=float, default=SENSOR_CENTER, help="sine wave center value")
    parser.add_argument("--host", default=DEFAULT_HOST, help="address this robot's Zenoh session listens on")
    parser.add_argument("--cloud-host", default=DEFAULT_HOST)
    parser.add_argument("--cloud-port", type=int, default=DEFAULT_CLOUD_PORT)
    parser.add_argument("--pki-dir", type=Path, default=DEFAULT_PKI_DIR)
    parser.add_argument("--log-file", type=Path, default=None, help="default: logs/<robot-id>.log")
    args = parser.parse_args()
    logs.setup(args.log_file or logs.LOG_DIR / f"{args.robot_id}.log")
    zenoh.init_log_from_env_or("error")
    with contextlib.suppress(KeyboardInterrupt):
        sys.exit(asyncio.run(main(args)))
