"""Keep a managed command bound to the lifetime of its lease wrapper."""

from __future__ import annotations

import argparse
import os
import select
import signal
import subprocess
import sys
import time

SPAWN_FAILED_EXIT = 127
APPROVAL_BYTE = b"A"


def _terminate_own_process_group(grace_seconds: float) -> None:
    """Terminate every command in this guard's process group."""

    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    os.killpg(os.getpgrp(), signal.SIGTERM)
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        time.sleep(0.05)
    os.killpg(os.getpgrp(), signal.SIGKILL)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-fd", type=int, required=True)
    parser.add_argument("--grace-seconds", type=float, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("command is required")

    if os.read(args.parent_fd, 1) != APPROVAL_BYTE:
        return SPAWN_FAILED_EXIT
    try:
        child = subprocess.Popen(command)
    except OSError:
        return SPAWN_FAILED_EXIT
    while True:
        if child.poll() is not None:
            return (
                child.returncode
                if child.returncode >= 0
                else 128 + abs(child.returncode)
            )
        readable, _, _ = select.select([args.parent_fd], [], [], 0.1)
        if readable and not os.read(args.parent_fd, 1):
            _terminate_own_process_group(args.grace_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
