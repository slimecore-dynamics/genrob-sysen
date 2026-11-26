"""The operator's command prompt.

Started by: user/__main__.py  ->  run_cli(client)
Works interactively or with piped input, e.g. a scripted demo:  python -m user < demo.txt

The terminal shows only the prompt, each command's result, and rare session events. Everything
received from robots goes to the log file (see user/__main__.py).

Grammar:
  list | sessions | help | quit | wait <seconds>
  connect <robot_id> | disconnect <robot_id>
  <robot_id|all> status <text>         publish on the robot's status topic
  <robot_id|all> <command>             send {"command": <command>}; the robot decides if it's valid

parse_line() turns a typed line into (action, args) without doing anything, so it can be unit tested.
"""
import asyncio
import logging

from user.client import ControlError, NotInSession, UserClient

log = logging.getLogger("User")

HELP = """commands:
  list                       list robots and their state
  sessions                   robots you're connected to
  connect <robot_id>         start a session with a robot (you can hold several)
  disconnect <robot_id>      end that session
  <robot_id> <command>       send a command, e.g.  robot-1 forward   (forward, backward, left, right, stop)
  all <command>              send the same command to every connected robot
  <robot_id|all> status <text>
                             publish a status message in that robot's session
  wait <seconds>             pause (for scripted demos)
  help                       show this
  quit                       exit"""

KEYWORDS = {"list", "sessions", "connect", "disconnect", "wait", "help", "quit"}


def parse_line(line: str) -> tuple[str, tuple]:
    """('noop'|'error'|keyword|'command'|'status', args). Pure: no side effects."""
    parts = line.split()
    if not parts:
        return "noop", ()
    head, rest = parts[0], parts[1:]
    if head in KEYWORDS:
        if head in ("connect", "disconnect"):
            return (head, (rest[0],)) if len(rest) == 1 else ("error", (f"usage: {head} <robot_id>",))
        if head == "wait":
            try:
                return "wait", (float(rest[0]) if rest else 1.0,)
            except ValueError:
                return "error", ("usage: wait <seconds>",)
        return head, ()
    # anything else is a target (a robot id or "all") followed by what to send it
    if not rest:
        return "error", (f"what should {head} do? (type help)",)
    if rest[0] == "status":
        if len(rest) < 2:
            return "error", ("usage: <robot_id|all> status <text>",)
        return "status", (head, " ".join(rest[1:]))
    if len(rest) != 1:
        return "error", ("usage: <robot_id|all> <command>",)
    return "command", (head, rest[0])


async def execute(client: UserClient, action: str, args: tuple) -> bool:
    """Run one parsed line. Returns False when the user asked to quit."""
    if action == "quit":
        return False
    if action == "noop":
        return True
    if action == "error":
        print(args[0])
    elif action == "help":
        print(HELP)
    elif action == "list":
        for r in await client.list_robots():
            print(f"  {r['robot_id']:20} {r['state']}")
    elif action == "sessions":
        print("  " + (", ".join(client.sessions) or "none"))
    elif action == "connect":
        print(f"  connecting to {args[0]} ...")
        await client.connect(args[0])
        print(f"  connected to {args[0]}")
    elif action == "disconnect":
        await client.disconnect(args[0])
        print(f"  disconnected from {args[0]}")
    elif action == "wait":
        await asyncio.sleep(args[0])
    elif action == "command":
        client.send_command(*args)
        print(f"  sent {args[1]} to {args[0]}")
    elif action == "status":
        client.send_status(*args)
        print(f"  sent status to {args[0]}")
    return True


PROMPT = "> "


async def run_cli(client: UserClient) -> None:
    waiting = False                  # True while the prompt is on screen and input() is waiting

    def show_notice(text: str) -> None:
        # Rare session events. They arrive while the prompt may already be showing, so put the
        # notice on its own line and show the prompt again, so it's clear you can keep typing.
        if waiting:
            print(f"\r  [{text}]\n{PROMPT}", end="", flush=True)
        else:
            print(f"  [{text}]", flush=True)

    client.on_notice = show_notice
    print(HELP)
    while True:
        waiting = True
        try:
            line = await asyncio.to_thread(input, PROMPT)
        except EOFError:          # end of piped input
            return
        finally:
            waiting = False
        try:
            if not await execute(client, *parse_line(line)):
                return
        except ControlError as err:
            print(f"refused: {err}")
        except NotInSession as err:
            print(f"not connected to {err}; use: connect <robot_id>")
