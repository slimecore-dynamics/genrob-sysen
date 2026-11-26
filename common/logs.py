"""One log format for every program: time, who, message.

Used by: every __main__.py calls setup(); every module gets its own logger by name.
The assignment requires each entity to print what it receives; these log lines are that output.

Each program writes its log to a file under logs/ (git-ignored). The Cloud Service and Robot also
show it in their terminal. The User doesn't: its terminal is kept free for typing commands, and its
log file is where the received data goes (and what the dashboard will read).
"""
import logging
from pathlib import Path

LOG_DIR = Path("logs")
FORMAT = "%(asctime)s.%(msecs)03d [%(name)-7s] %(message)s"
DATE_FORMAT = "%H:%M:%S"


def setup(log_file: Path | None = None, console: bool = True, level: int = logging.INFO) -> None:
    handlers: list[logging.Handler] = []
    if console:
        handlers.append(logging.StreamHandler())
    if log_file is not None:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="ascii", errors="backslashreplace"))
    logging.basicConfig(level=level, format=FORMAT, datefmt=DATE_FORMAT, handlers=handlers, force=True)
    logging.getLogger("websockets").setLevel(logging.WARNING)   # hide the library's per-connection chatter
