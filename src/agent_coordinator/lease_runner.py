"""Exclusive resource leases for managed local commands."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable, Protocol

from .models import ClaimRecord, OwnerIdentity, TaskIdentity
from .service import ClaimConflictError, StaleClaimError, TaskCoordinator
from .store import JsonlClaimStore


LEASE_FINGERPRINT = "exclusive-resource-lease:v1"


@dataclass(frozen=True)
class LeaseKey:
    namespace: str
    resource_key: str

    def __post_init__(self) -> None:
        if not self.namespace:
            raise ValueError("namespace is required")
        if not self.resource_key:
            raise ValueError("resource_key is required")

    def task_identity(self) -> TaskIdentity:
        return TaskIdentity(
            task_type=self.namespace,
            task_id=self.resource_key,
            fingerprint=LEASE_FINGERPRINT,
        )


def canonical_worktree_resource(path: str | os.PathLike[str]) -> str:
    """Return the strict canonical identity for a worktree directory."""

    resolved = Path(path).resolve(strict=True)
    if not resolved.is_dir():
        raise NotADirectoryError(resolved)
    return str(resolved)


class LeaseRunState(str, Enum):
    EXITED = "exited"
    TIMED_OUT = "timed_out"
    INTERRUPTED = "interrupted"
    CONTENDED = "contended"
    LAUNCH_FAILED = "launch_failed"
    FENCED = "fenced"


@dataclass(frozen=True)
class LeaseRunRequest:
    key: LeaseKey
    command: tuple[str, ...]
    cwd: Path
    lease_seconds: int
    heartbeat_seconds: int
    timeout_seconds: float
    terminate_grace_seconds: float
    session_id: str
    agent: str

    def __post_init__(self) -> None:
        if not self.command:
            raise ValueError("command is required")
        if self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if self.heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be positive")
        if self.heartbeat_seconds >= self.lease_seconds:
            raise ValueError("heartbeat_seconds must be lower than lease_seconds")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.terminate_grace_seconds < 0:
            raise ValueError("terminate_grace_seconds must not be negative")
        if not self.session_id:
            raise ValueError("session_id is required")
        if not self.agent:
            raise ValueError("agent is required")


@dataclass(frozen=True)
class LeaseRunResult:
    state: LeaseRunState
    exit_code: int
    claim: ClaimRecord | None = None
    holder: ClaimRecord | None = None
    holder_age_seconds: float | None = None
    remediation: str | None = None
    release_error: str | None = None


class ManagedProcess(Protocol):
    pid: int
    returncode: int | None

    def poll(self) -> int | None: ...

    def wait(self, timeout: float | None = None) -> int: ...


def _terminate_process_group(
    process: ManagedProcess,
    grace_seconds: float,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = monotonic() + grace_seconds
    while process.poll() is None and monotonic() < deadline:
        sleep(min(0.05, max(0.0, deadline - monotonic())))
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=max(1.0, grace_seconds))
    except subprocess.TimeoutExpired:
        return


def run_with_lease(
    store: JsonlClaimStore,
    request: LeaseRunRequest,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    process_factory: Callable[..., ManagedProcess] = subprocess.Popen,
    teardown: Callable[[ManagedProcess, float], None] | None = None,
) -> LeaseRunResult:
    """Run a command under an exclusive, heartbeat-backed resource lease."""

    coordinator = TaskCoordinator(store, reclaim_dead_owners=False)
    owner = OwnerIdentity(
        session_id=request.session_id,
        pid=os.getpid(),
        agent=request.agent,
        worktree_path=str(request.cwd),
    )
    try:
        claim = coordinator.claim_task(
            request.key.task_identity(),
            owner,
            lease_seconds=request.lease_seconds,
            now=clock(),
        )
    except ClaimConflictError as exc:
        holder = exc.decision.claim
        age = (
            max(0.0, (clock() - holder.claimed_at).total_seconds())
            if holder is not None
            else None
        )
        return LeaseRunResult(
            state=LeaseRunState.CONTENDED,
            exit_code=3,
            holder=holder,
            holder_age_seconds=age,
            remediation=(
                "wait for the holder to finish, stop it normally, or after a crash "
                "wait for lease expiry"
            ),
        )

    process: ManagedProcess | None = None
    state = LeaseRunState.LAUNCH_FAILED
    exit_code = 127
    release_reason = "launch_failed"
    release_error = None
    stop_process = teardown or (
        lambda child, grace: _terminate_process_group(
            child,
            grace,
            monotonic,
            sleep,
        )
    )
    try:
        try:
            process = process_factory(
                request.command,
                cwd=request.cwd,
                start_new_session=True,
            )
        except OSError:
            return LeaseRunResult(
                state=state,
                exit_code=exit_code,
                claim=claim,
            )

        started = monotonic()
        deadline = started + request.timeout_seconds
        next_heartbeat = started + request.heartbeat_seconds
        while True:
            child_exit = process.poll()
            if child_exit is not None:
                state = LeaseRunState.EXITED
                exit_code = child_exit
                release_reason = "command_exited"
                break

            current = monotonic()
            if current >= deadline:
                stop_process(process, request.terminate_grace_seconds)
                state = LeaseRunState.TIMED_OUT
                exit_code = 124
                release_reason = "command_timed_out"
                break
            if current >= next_heartbeat:
                coordinator.heartbeat_claim(
                    claim.claim_id,
                    owner_session_id=request.session_id,
                    lease_epoch=claim.lease_epoch,
                    lease_seconds=request.lease_seconds,
                    now=clock(),
                )
                next_heartbeat = current + request.heartbeat_seconds
            sleep(
                min(
                    0.25,
                    max(0.0, deadline - current),
                    max(0.0, next_heartbeat - current),
                )
            )
    except StaleClaimError as exc:
        if process is not None:
            stop_process(process, request.terminate_grace_seconds)
        state = LeaseRunState.FENCED
        exit_code = 75
        release_reason = "lease_fenced"
        release_error = str(exc)
    except KeyboardInterrupt:
        if process is not None:
            stop_process(process, request.terminate_grace_seconds)
        state = LeaseRunState.INTERRUPTED
        exit_code = 130
        release_reason = "command_interrupted"
    finally:
        try:
            coordinator.release_claim(
                claim.claim_id,
                owner_session_id=request.session_id,
                lease_epoch=claim.lease_epoch,
                reason=release_reason,
                now=clock(),
            )
        except (KeyError, PermissionError, StaleClaimError, ValueError) as exc:
            release_error = str(exc)

    return LeaseRunResult(
        state=state,
        exit_code=exit_code,
        claim=claim,
        release_error=release_error,
    )
