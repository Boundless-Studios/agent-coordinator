"""Keep a managed command bound to the lifetime of its lease wrapper."""

from __future__ import annotations

import argparse
import ctypes
import os
import select
import signal
import subprocess
import sys
import time

SPAWN_FAILED_EXIT = 127
APPROVAL_BYTE = b"A"
SPAWN_SUCCEEDED_BYTE = b"S"
SPAWN_FAILED_BYTE = b"F"
TEARDOWN_FAILED_BYTE = b"T"
PARENT_POLL_SECONDS = 0.1
TEARDOWN_FAILED_EXIT = 76


def _adopt_orphaned_descendants() -> None:
    """Become a Linux child subreaper so killed descendants can be waited."""

    if sys.platform != "linux":
        return
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "could not become child subreaper")


def _reap_child_group(group_id: int) -> None:
    while True:
        try:
            waited_pid, _ = os.waitpid(-group_id, os.WNOHANG)
        except ChildProcessError:
            return
        if waited_pid == 0:
            return


def _terminate_child_group(
    child: subprocess.Popen[bytes],
    grace_seconds: float,
) -> None:
    """Stop the child group while keeping the guard alive to reap its leader."""

    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        try:
            child.wait(timeout=1)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("child leader could not be reaped") from exc
        _reap_child_group(child.pid)
        return
    except PermissionError as exc:
        raise RuntimeError("cannot signal child process group") from exc
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        child.poll()
        try:
            os.killpg(child.pid, 0)
        except ProcessLookupError:
            child.wait()
            return
        time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError as exc:
        raise RuntimeError("cannot signal child process group") from exc
    try:
        child.wait(timeout=1)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("child leader survived SIGKILL") from exc
    _reap_child_group(child.pid)
    try:
        os.killpg(child.pid, 0)
    except ProcessLookupError:
        return
    except PermissionError as exc:
        raise RuntimeError("cannot confirm child process group stopped") from exc
    raise RuntimeError("child process group survived SIGKILL")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-fd", type=int, required=True)
    parser.add_argument("--status-fd", type=int, required=True)
    parser.add_argument("--grace-seconds", type=float, required=True)
    parser.add_argument("--keepalive-timeout", type=float, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("command is required")

    authorization = os.read(args.parent_fd, 128)
    if not authorization.startswith(APPROVAL_BYTE):
        return SPAWN_FAILED_EXIT
    try:
        authorization_expires_at = float(authorization[1:].strip())
    except ValueError:
        return SPAWN_FAILED_EXIT
    if time.time() >= authorization_expires_at:
        return SPAWN_FAILED_EXIT
    _adopt_orphaned_descendants()

    terminate_requested = False

    def request_termination(_signum: int, _frame: object) -> None:
        nonlocal terminate_requested
        terminate_requested = True

    signal.signal(signal.SIGTERM, request_termination)
    keepalive_deadline = time.monotonic() + args.keepalive_timeout
    try:
        child = subprocess.Popen(command, start_new_session=True)
    except OSError:
        os.write(args.status_fd, SPAWN_FAILED_BYTE)
        return SPAWN_FAILED_EXIT
    if terminate_requested or time.time() >= authorization_expires_at:
        try:
            _terminate_child_group(child, args.grace_seconds)
        except RuntimeError:
            os.write(args.status_fd, TEARDOWN_FAILED_BYTE)
            return TEARDOWN_FAILED_EXIT
        return 128 + signal.SIGTERM
    try:
        os.write(args.status_fd, SPAWN_SUCCEEDED_BYTE)
    except BrokenPipeError:
        _terminate_child_group(child, args.grace_seconds)
        return 128 + signal.SIGTERM

    while True:
        child_exit = child.poll()
        if child_exit is not None:
            try:
                _terminate_child_group(child, args.grace_seconds)
            except RuntimeError:
                os.write(args.status_fd, TEARDOWN_FAILED_BYTE)
                return TEARDOWN_FAILED_EXIT
            return child_exit if child_exit >= 0 else 128 + abs(child_exit)
        if terminate_requested:
            try:
                _terminate_child_group(child, args.grace_seconds)
            except RuntimeError:
                os.write(args.status_fd, TEARDOWN_FAILED_BYTE)
                return TEARDOWN_FAILED_EXIT
            return 128 + signal.SIGTERM
        remaining = keepalive_deadline - time.monotonic()
        if remaining <= 0:
            try:
                _terminate_child_group(child, args.grace_seconds)
            except RuntimeError:
                os.write(args.status_fd, TEARDOWN_FAILED_BYTE)
                return TEARDOWN_FAILED_EXIT
            return 128 + signal.SIGTERM
        readable, _, _ = select.select(
            [args.parent_fd],
            [],
            [],
            min(PARENT_POLL_SECONDS, remaining),
        )
        if readable:
            if not os.read(args.parent_fd, 1):
                try:
                    _terminate_child_group(child, args.grace_seconds)
                except RuntimeError:
                    os.write(args.status_fd, TEARDOWN_FAILED_BYTE)
                    return TEARDOWN_FAILED_EXIT
                return 128 + signal.SIGTERM
            keepalive_deadline = time.monotonic() + args.keepalive_timeout


if __name__ == "__main__":
    raise SystemExit(main())
