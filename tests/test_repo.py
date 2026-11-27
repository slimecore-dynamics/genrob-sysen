"""Checks on the repository itself, and on the User program as a real process."""
import asyncio
import subprocess
import sys
from pathlib import Path

import pytest

from tests.helpers import HOST, wait_until

REPO_ROOT = Path(__file__).resolve().parent.parent
TEXT_SUFFIXES = {".py", ".md", ".txt", ".ini", ".cfg", ".toml", ".yml", ".yaml", ".json"}


def repo_text_files():
    skip = {"__pycache__", ".pytest_cache", "pki", "logs", ".git", ".venv", "venv"}
    for path in REPO_ROOT.rglob("*"):
        if (path.is_file() and not skip & set(path.relative_to(REPO_ROOT).parts)
                and (path.suffix in TEXT_SUFFIXES or path.name == ".gitignore")):
            yield path


def test_every_file_is_plain_ascii():
    offenders = []
    for path in repo_text_files():
        for line_no, line in enumerate(path.read_bytes().splitlines(), 1):
            if any(byte > 127 for byte in line):
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{line_no}")
    assert not offenders, "non-ASCII characters in: " + ", ".join(offenders)


@pytest.mark.asyncio
async def test_user_terminal_shows_only_prompt_output(cloud, make_robot, pki, tmp_path):
    """The User's terminal is for typing: received data goes to its log file, not the screen."""
    bot, _ = await make_robot()
    forward_calls = []
    real_forward = bot.commands["forward"]
    bot.commands["forward"] = lambda: (forward_calls.append(1), real_forward())   # record, then drive
    log_file = tmp_path / "user.log"
    script = "connect robot-1\nwait 2.5\nrobot-1 forward\nwait 1\nquit\n"
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "user", "--cloud-port", str(cloud.port), "--host", HOST,
        "--pki-dir", str(pki), "--log-file", str(log_file),
        cwd=REPO_ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, _ = await asyncio.wait_for(proc.communicate(script.encode()), timeout=30)
    terminal = out.decode()

    assert proc.returncode == 0, terminal
    assert "connected to robot-1" in terminal
    assert "sent forward to robot-1" in terminal
    assert "got robot/" not in terminal                       # no data on the screen...
    log = log_file.read_text()
    assert "got robot/robot-1/sensor" in log                  # ...it's all in the log
    assert "got robot/robot-1/processed" in log
    # The command reached the robot. (Its motors are stopped again by now: "quit" ended the
    # session, and the robot stops whenever a session ends.)
    await wait_until(lambda: forward_calls)
