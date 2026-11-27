"""Starting and stopping Player processes.

Used by: cloud_service/server.py (CloudService.start_session / end_session).

spawn_player() starts `python -m cloud_service.player ...` as a child process and waits for its
"READY <endpoint>" line. A thread keeps reading the child's output afterwards (an undrained pipe
would freeze the child) and logs each line as [Player].

The Player runs in its own process group. Ctrl+C in a terminal goes to every process in the
terminal's group, so without this the Player would be interrupted at the same moment as the Cloud
Service, instead of being stopped by it in order (stdin closed -> "stopping" -> clean exit).
"""
import logging
import queue
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

from common.config import PLAYER_READY_TIMEOUT
from common.mesh import free_port

player_log = logging.getLogger("Player")

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PLAYER_MODULE = "cloud_service.player"


class PlayerSpawnError(Exception):
    """The Player exited or never printed READY in time."""


@dataclass
class PlayerProcess:
    proc: subprocess.Popen
    endpoint: str

    def stop(self, timeout: float = 3.0) -> None:
        """Polite first (close stdin so the Player exits cleanly), forceful if it hangs."""
        if self.proc.poll() is not None:
            return
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()


def spawn_player(robot_id: str, host: str, pki_dir, module: str = DEFAULT_PLAYER_MODULE) -> PlayerProcess:
    """Start a Player for robot_id and wait for READY. Raises PlayerSpawnError on failure.

    Blocks while waiting, so the server calls it with asyncio.to_thread().
    `module` is swappable so tests can point at a Player that crashes on purpose.
    """
    port = free_port(host)
    if sys.platform == "win32":
        group = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    else:
        group = {"start_new_session": True}
    proc = subprocess.Popen(
        [sys.executable, "-u", "-m", module, "--robot-id", robot_id, "--host", host,
         "--port", str(port), "--pki-dir", str(Path(pki_dir).resolve())],
        cwd=REPO_ROOT,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        **group,
    )
    ready: queue.Queue = queue.Queue()

    def drain() -> None:
        for raw in proc.stdout:
            line = raw.rstrip()
            if line.startswith("READY "):
                ready.put(line.split(" ", 1)[1])
            elif line:
                player_log.info("(%s) %s", robot_id, line)
        ready.put(None)   # stdout closed: the process ended

    threading.Thread(target=drain, daemon=True, name=f"player-{robot_id}").start()

    try:
        ep = ready.get(timeout=PLAYER_READY_TIMEOUT)
    except queue.Empty:
        ep = None
    if ep is None:
        proc.kill()
        proc.wait()
        raise PlayerSpawnError(f"Player for {robot_id} never became ready (exit code {proc.returncode})")
    return PlayerProcess(proc, ep)
