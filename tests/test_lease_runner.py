import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest

from agent_coordinator import JsonlClaimStore, OwnerIdentity, TaskCoordinator
from agent_coordinator.lease_runner import (
    LeaseKey,
    LeaseRunRequest,
    LeaseRunState,
    run_with_lease,
)


BASE_TIME = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


def request(tmp_path, **changes) -> LeaseRunRequest:
    values = {
        "key": LeaseKey("local-frontend-test", str(tmp_path)),
        "command": (sys.executable, "-c", "raise SystemExit(0)"),
        "cwd": tmp_path,
        "lease_seconds": 30,
        "heartbeat_seconds": 5,
        "timeout_seconds": 10.0,
        "terminate_grace_seconds": 0.2,
        "session_id": "runner-1",
        "agent": "pytest",
    }
    values.update(changes)
    return LeaseRunRequest(**values)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("command", ()),
        ("lease_seconds", 0),
        ("heartbeat_seconds", 0),
        ("timeout_seconds", 0),
        ("terminate_grace_seconds", -1),
        ("session_id", ""),
        ("agent", ""),
    ],
)
def test_run_request_rejects_invalid_values(tmp_path, field, value) -> None:
    with pytest.raises(ValueError):
        request(tmp_path, **{field: value})


def test_contention_fails_fast_with_holder_age_and_remediation(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    coordinator = TaskCoordinator(store, reclaim_dead_owners=False)
    coordinator.claim_task(
        LeaseKey("local-frontend-test", str(tmp_path)).task_identity(),
        OwnerIdentity(session_id="holder", pid=4242, agent="vitest"),
        lease_seconds=60,
        now=BASE_TIME,
    )
    launches = 0

    def launch(*_args, **_kwargs):
        nonlocal launches
        launches += 1
        raise AssertionError("contended command must not launch")

    result = run_with_lease(
        store,
        request(tmp_path, session_id="contender"),
        clock=lambda: BASE_TIME + timedelta(seconds=12),
        process_factory=launch,
    )

    assert result.state is LeaseRunState.CONTENDED
    assert result.exit_code != 0
    assert result.holder is not None
    assert result.holder.owner.session_id == "holder"
    assert result.holder_age_seconds == 12
    assert "lease expiry" in (result.remediation or "")
    assert launches == 0


@pytest.mark.parametrize("child_exit", [0, 7])
def test_child_exit_is_preserved_and_lease_is_released(
    tmp_path,
    child_exit: int,
) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    result = run_with_lease(
        store,
        request(
            tmp_path,
            command=(sys.executable, "-c", f"raise SystemExit({child_exit})"),
        ),
    )

    assert result.state is LeaseRunState.EXITED
    assert result.exit_code == child_exit
    decision = TaskCoordinator(store).status(result.claim.task)
    assert decision.reclaimable is True
    assert decision.claim is not None
    assert decision.claim.release_reason == "command_exited"


def test_spawn_failure_releases_the_lease(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")

    def fail_spawn(*_args, **_kwargs):
        raise OSError("cannot spawn")

    result = run_with_lease(
        store,
        request(tmp_path),
        process_factory=fail_spawn,
    )

    assert result.state is LeaseRunState.LAUNCH_FAILED
    assert result.exit_code != 0
    assert result.claim is not None
    assert TaskCoordinator(store).status(result.claim.task).reclaimable is True


def test_timeout_terminates_process_group_and_releases(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    launched = []

    def launch(*args, **kwargs):
        process = subprocess.Popen(*args, **kwargs)
        launched.append(process)
        return process

    result = run_with_lease(
        store,
        request(
            tmp_path,
            command=(
                sys.executable,
                "-c",
                "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                "time.sleep(30)",
            ),
            timeout_seconds=0.05,
            terminate_grace_seconds=0.1,
        ),
        process_factory=launch,
    )

    assert result.state is LeaseRunState.TIMED_OUT
    assert result.exit_code != 0
    assert launched[0].returncode is not None
    assert result.claim is not None
    assert TaskCoordinator(store).status(result.claim.task).reclaimable is True


class InterruptingProcess:
    pid = os.getpid()
    returncode = None

    def poll(self):
        raise KeyboardInterrupt


def test_interrupt_tears_down_and_releases(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    torn_down = []

    result = run_with_lease(
        store,
        request(tmp_path),
        process_factory=lambda *_args, **_kwargs: InterruptingProcess(),
        teardown=lambda process, grace: torn_down.append((process.pid, grace)),
    )

    assert result.state is LeaseRunState.INTERRUPTED
    assert result.exit_code == 130
    assert torn_down == [(os.getpid(), 0.2)]
    assert result.claim is not None
    assert TaskCoordinator(store).status(result.claim.task).reclaimable is True


class PollingProcess:
    pid = os.getpid()
    returncode = None

    def __init__(self) -> None:
        self.polls = 0

    def poll(self):
        self.polls += 1
        if self.polls < 6:
            return None
        self.returncode = 0
        return 0


def test_long_command_heartbeats_before_lease_expiry(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    elapsed = 0.0

    def monotonic() -> float:
        return elapsed

    def sleep(seconds: float) -> None:
        nonlocal elapsed
        elapsed += seconds

    run_with_lease(
        store,
        request(
            tmp_path,
            lease_seconds=4,
            heartbeat_seconds=1,
        ),
        clock=lambda: BASE_TIME + timedelta(seconds=elapsed),
        monotonic=monotonic,
        sleep=sleep,
        process_factory=lambda *_args, **_kwargs: PollingProcess(),
    )

    event_types = [event["event"] for event in store.read_events()]
    assert "heartbeat" in event_types


def test_deposed_runner_terminates_child_when_heartbeat_is_fenced(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    elapsed = 0.0
    successor_claimed = False
    torn_down = []

    def clock() -> datetime:
        return BASE_TIME + timedelta(seconds=elapsed)

    def monotonic() -> float:
        nonlocal successor_claimed
        if elapsed >= 3 and not successor_claimed:
            TaskCoordinator(store, reclaim_dead_owners=False).claim_task(
                LeaseKey("local-frontend-test", str(tmp_path)).task_identity(),
                OwnerIdentity(session_id="successor", pid=9999, agent="pytest"),
                lease_seconds=30,
                now=clock(),
            )
            successor_claimed = True
        return elapsed

    def sleep(_seconds: float) -> None:
        nonlocal elapsed
        elapsed = 3.0

    result = run_with_lease(
        store,
        request(
            tmp_path,
            lease_seconds=2,
            heartbeat_seconds=1,
        ),
        clock=clock,
        monotonic=monotonic,
        sleep=sleep,
        process_factory=lambda *_args, **_kwargs: PollingProcess(),
        teardown=lambda process, grace: torn_down.append((process.pid, grace)),
    )

    assert result.state is LeaseRunState.FENCED
    assert result.exit_code != 0
    assert torn_down == [(os.getpid(), 0.2)]
    decision = TaskCoordinator(store, reclaim_dead_owners=False).status(
        LeaseKey("local-frontend-test", str(tmp_path)).task_identity(),
        now=clock(),
    )
    assert decision.claim is not None
    assert decision.claim.owner.session_id == "successor"
