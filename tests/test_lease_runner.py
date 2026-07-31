import math
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import pytest

import agent_coordinator.lease_runner as lease_runner
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
        ("lease_seconds", 10**20),
        ("heartbeat_seconds", 0),
        ("timeout_seconds", 0),
        ("terminate_grace_seconds", -1),
        ("timeout_seconds", math.nan),
        ("timeout_seconds", math.inf),
        ("terminate_grace_seconds", math.nan),
        ("terminate_grace_seconds", math.inf),
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


def test_overlapping_invocations_with_same_session_id_contend(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")

    class HoldingProcess:
        pid = 987654
        returncode = None

        def poll(self):
            if not attempted_overlap:
                attempted_overlap.append(
                    run_with_lease(
                        store,
                        request(tmp_path),
                        process_factory=lambda *_args, **_kwargs: pytest.fail(
                            "contending invocation must not launch"
                        ),
                    )
                )
            self.returncode = 0
            return 0

    attempted_overlap = []
    first = run_with_lease(
        store,
        request(tmp_path),
        process_factory=lambda *_args, **_kwargs: HoldingProcess(),
    )

    assert first.state is LeaseRunState.EXITED
    assert attempted_overlap[0].state is LeaseRunState.CONTENDED


def test_interrupt_after_acquisition_releases_unlaunched_claim(
    tmp_path,
    monkeypatch,
) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    original_claim = TaskCoordinator.claim_task

    def interrupt_after_claim(self, *args, **kwargs):
        original_claim(self, *args, **kwargs)
        raise KeyboardInterrupt

    monkeypatch.setattr(TaskCoordinator, "claim_task", interrupt_after_claim)

    result = run_with_lease(store, request(tmp_path))

    assert result.state is LeaseRunState.INTERRUPTED
    assert result.exit_code == 130
    assert result.claim is not None
    assert result.claim.release_reason == "acquisition_interrupted"


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
    assert result.claim is not None
    assert result.claim.release_reason == "command_exited"
    decision = TaskCoordinator(store).status(result.claim.task)
    assert decision.reclaimable is True
    assert decision.claim is not None
    assert decision.claim.release_reason == "command_exited"


def test_normal_leader_exit_cleans_up_surviving_process_group(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    torn_down = []

    class ExitedLeader:
        pid = 987654
        returncode = 0

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    result = run_with_lease(
        store,
        request(tmp_path),
        process_factory=lambda *_args, **_kwargs: ExitedLeader(),
        teardown=lambda process, grace: torn_down.append((process.pid, grace)),
    )

    assert result.state is LeaseRunState.EXITED
    assert torn_down == [(987654, 0.2)]


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


def test_spawn_failure_reports_a_release_storage_error(
    tmp_path,
    monkeypatch,
) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")

    def fail_spawn(*_args, **_kwargs):
        raise OSError("cannot spawn")

    def fail_release(*_args, **_kwargs):
        raise OSError("disk full during release")

    monkeypatch.setattr(TaskCoordinator, "release_claim", fail_release)

    result = run_with_lease(
        store,
        request(tmp_path),
        process_factory=fail_spawn,
    )

    assert result.state is LeaseRunState.LAUNCH_FAILED
    assert result.release_error == "disk full during release"


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
    pid = 987654
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


def test_wall_clock_resume_renews_before_polling_child_again(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    wall_elapsed = 0.0
    observations = []

    class SuspendedProcess:
        pid = 987654
        returncode = None
        polls = 0

        def poll(self):
            self.polls += 1
            observations.append(
                (
                    "poll",
                    len(
                        [
                            event
                            for event in store.read_events()
                            if event["event"] == "heartbeat"
                        ]
                    ),
                )
            )
            if self.polls == 2:
                self.returncode = 0
                return 0
            return None

        def wait(self, timeout=None):
            return self.returncode or 0

    def sleep(_seconds):
        nonlocal wall_elapsed
        wall_elapsed = 4.0

    run_with_lease(
        store,
        request(tmp_path, lease_seconds=3, heartbeat_seconds=1),
        clock=lambda: BASE_TIME + timedelta(seconds=wall_elapsed),
        monotonic=lambda: 0.0,
        sleep=sleep,
        process_factory=lambda *_args, **_kwargs: SuspendedProcess(),
    )

    assert observations[1] == ("poll", 2)


def test_delayed_spawn_revalidates_before_polling_child(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    wall_elapsed = 0.0
    torn_down = []

    class UnpolledProcess:
        pid = 987654
        returncode = None
        polls = 0

        def poll(self):
            self.polls += 1
            return None

        def wait(self, timeout=None):
            return 0

    process = UnpolledProcess()

    def delayed_spawn(*_args, **_kwargs):
        nonlocal wall_elapsed
        wall_elapsed = 4.0
        TaskCoordinator(store, reclaim_dead_owners=False).claim_task(
            LeaseKey("local-frontend-test", str(tmp_path)).task_identity(),
            OwnerIdentity(session_id="successor", pid=9999, agent="pytest"),
            lease_seconds=30,
            now=BASE_TIME + timedelta(seconds=wall_elapsed),
        )
        return process

    result = run_with_lease(
        store,
        request(tmp_path, lease_seconds=3, heartbeat_seconds=1),
        clock=lambda: BASE_TIME + timedelta(seconds=wall_elapsed),
        process_factory=delayed_spawn,
        teardown=lambda child, grace: torn_down.append((child.pid, grace)),
    )

    assert result.state is LeaseRunState.FENCED
    assert process.polls == 0
    assert torn_down == [(987654, 0.2)]


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
    assert torn_down == [(987654, 0.2)]
    decision = TaskCoordinator(store, reclaim_dead_owners=False).status(
        LeaseKey("local-frontend-test", str(tmp_path)).task_identity(),
        now=clock(),
    )
    assert decision.claim is not None
    assert decision.claim.owner.session_id == "successor"


def test_heartbeat_storage_failure_tears_down_before_propagating(
    tmp_path,
    monkeypatch,
) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    elapsed = 0.0
    torn_down = []

    def sleep(_seconds: float) -> None:
        nonlocal elapsed
        elapsed = 2.0

    def fail_heartbeat(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(TaskCoordinator, "heartbeat_claim", fail_heartbeat)

    with pytest.raises(OSError, match="disk full"):
        run_with_lease(
            store,
            request(tmp_path, heartbeat_seconds=1),
            monotonic=lambda: elapsed,
            sleep=sleep,
            process_factory=lambda *_args, **_kwargs: PollingProcess(),
            teardown=lambda process, grace: torn_down.append((process.pid, grace)),
        )

    assert torn_down == [(987654, 0.2)]


def test_teardown_escalates_when_leader_exits_but_process_group_survives(
    monkeypatch,
) -> None:
    from agent_coordinator.lease_runner import _terminate_process_group

    signals = []
    killed = False

    def killpg(_pid, sig):
        nonlocal killed
        if sig == 0 and killed:
            raise ProcessLookupError
        if sig != 0:
            signals.append(sig)
        if sig == signal.SIGKILL:
            killed = True

    class ExitedLeader:
        pid = 1234
        returncode = 0

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    elapsed = 0.0

    def monotonic():
        return elapsed

    def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    monkeypatch.setattr(os, "killpg", killpg)
    _terminate_process_group(ExitedLeader(), 0.1, monotonic, sleep)

    assert signals == [signal.SIGTERM, signal.SIGKILL]


def test_teardown_keeps_renewing_after_a_transient_callback_error(
    monkeypatch,
) -> None:
    from agent_coordinator.lease_runner import _terminate_process_group

    callback_calls = 0
    elapsed = 0.0
    killed = False

    def killpg(_pid, sent_signal):
        nonlocal killed
        if sent_signal == 0 and killed:
            raise ProcessLookupError
        if sent_signal == signal.SIGKILL:
            killed = True

    def callback():
        nonlocal callback_calls
        callback_calls += 1
        if callback_calls == 1:
            raise OSError("transient heartbeat failure")

    def monotonic():
        return elapsed

    def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    class StubbornProcess:
        pid = 1234
        returncode = None

        def poll(self):
            return None

        def wait(self, timeout=None):
            return -signal.SIGKILL

    monkeypatch.setattr(os, "killpg", killpg)

    with pytest.raises(OSError, match="transient heartbeat failure"):
        _terminate_process_group(
            StubbornProcess(),
            0.2,
            monotonic,
            sleep,
            callback,
        )

    assert callback_calls > 1


def test_teardown_reaps_responsive_leader_before_waiting_full_grace(
    monkeypatch,
) -> None:
    from agent_coordinator.lease_runner import _terminate_process_group

    signals = []
    elapsed = 0.0
    leader_reaped = False

    def killpg(_pid, sent_signal):
        if sent_signal == 0 and leader_reaped:
            raise ProcessLookupError
        if sent_signal != 0:
            signals.append(sent_signal)

    class ResponsiveProcess:
        pid = 1234
        returncode = None

        def poll(self):
            nonlocal leader_reaped
            leader_reaped = True
            self.returncode = -signal.SIGTERM
            return self.returncode

        def wait(self, timeout=None):
            return self.returncode or 0

    def monotonic():
        return elapsed

    def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    monkeypatch.setattr(os, "killpg", killpg)
    _terminate_process_group(ResponsiveProcess(), 10, monotonic, sleep)

    assert signals == [signal.SIGTERM]
    assert elapsed < 10


def test_teardown_failure_keeps_claim_until_lease_expiry(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")

    def fail_teardown(_process, _grace):
        raise lease_runner.ProcessTeardownError("process group 1234 survived SIGKILL")

    result = run_with_lease(
        store,
        request(tmp_path),
        process_factory=lambda *_args, **_kwargs: InterruptingProcess(),
        teardown=fail_teardown,
    )

    assert result.state.value == "teardown_failed"
    assert result.exit_code != 0
    assert result.teardown_error == "process group 1234 survived SIGKILL"
    assert result.claim is not None
    decision = TaskCoordinator(store, reclaim_dead_owners=False).status(
        result.claim.task
    )
    assert decision.claim is not None
    assert decision.claim.status == "active"


def test_post_launch_error_returns_teardown_failure_instead_of_raising(
    tmp_path,
    monkeypatch,
) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")

    def fail_heartbeat(*_args, **_kwargs):
        raise OSError("ledger unavailable")

    def fail_teardown(_process, _grace):
        raise lease_runner.ProcessTeardownError("child survived")

    monkeypatch.setattr(TaskCoordinator, "heartbeat_claim", fail_heartbeat)
    result = run_with_lease(
        store,
        request(tmp_path),
        process_factory=lambda *_args, **_kwargs: InterruptingProcess(),
        teardown=fail_teardown,
    )

    assert result.state is LeaseRunState.TEARDOWN_FAILED
    assert result.exit_code == 76
    assert result.teardown_error == "child survived"
    assert result.claim is not None
    assert result.claim.status == "active"


def test_post_launch_error_records_distinct_release_reason(
    tmp_path,
    monkeypatch,
) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")

    def fail_heartbeat(*_args, **_kwargs):
        raise OSError("ledger unavailable")

    monkeypatch.setattr(TaskCoordinator, "heartbeat_claim", fail_heartbeat)
    with pytest.raises(OSError, match="ledger unavailable"):
        run_with_lease(
            store,
            request(tmp_path),
            process_factory=lambda *_args, **_kwargs: InterruptingProcess(),
            teardown=lambda *_args: None,
        )

    decision = TaskCoordinator(store, reclaim_dead_owners=False).status(
        request(tmp_path).key.task_identity()
    )
    assert decision.claim is not None
    assert decision.claim.release_reason == "post_launch_operation_failed"


def test_repeated_interrupt_during_cleanup_keeps_claim_active(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")

    def interrupt_teardown(_process, _grace):
        raise KeyboardInterrupt

    result = run_with_lease(
        store,
        request(tmp_path),
        process_factory=lambda *_args, **_kwargs: InterruptingProcess(),
        teardown=interrupt_teardown,
    )

    assert result.state is LeaseRunState.TEARDOWN_FAILED
    assert result.teardown_error == "process teardown was interrupted"
    assert result.claim is not None
    decision = TaskCoordinator(store, reclaim_dead_owners=False).status(
        result.claim.task
    )
    assert decision.claim is not None
    assert decision.claim.status == "active"


def test_sigkill_survivor_raises_teardown_failure(monkeypatch) -> None:
    from agent_coordinator.lease_runner import _terminate_process_group

    elapsed = 0.0

    def monotonic():
        return elapsed

    def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    class UnkillableProcess:
        pid = 1234
        returncode = None

        def poll(self):
            return None

        def wait(self, timeout=None):
            raise subprocess.TimeoutExpired("child", timeout)

    monkeypatch.setattr(os, "killpg", lambda _pid, _signal: None)

    with pytest.raises(
        lease_runner.ProcessTeardownError,
        match="survived SIGKILL",
    ):
        _terminate_process_group(UnkillableProcess(), 0.1, monotonic, sleep)


def test_failed_teardown_heartbeat_retries_inside_remaining_lease_margin(
    tmp_path,
    monkeypatch,
) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    elapsed = 0.0
    attempts = []
    original_heartbeat = TaskCoordinator.heartbeat_claim

    class StubbornProcess:
        pid = 1234
        returncode = None

        def poll(self):
            return None

        def wait(self, timeout=None):
            self.returncode = -signal.SIGKILL
            return self.returncode

    def monotonic():
        return elapsed

    def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    def killpg(_pid, sig):
        if sig == 0 and elapsed >= 4:
            raise ProcessLookupError

    def heartbeat(self, *args, **kwargs):
        attempts.append(elapsed)
        if len(attempts) == 1:
            raise OSError("transient")
        return original_heartbeat(self, *args, **kwargs)

    monkeypatch.setattr(os, "killpg", killpg)
    monkeypatch.setattr(TaskCoordinator, "heartbeat_claim", heartbeat)

    with pytest.raises(OSError, match="transient"):
        run_with_lease(
            store,
            request(
                tmp_path,
                lease_seconds=3,
                heartbeat_seconds=2,
                timeout_seconds=0.5,
                terminate_grace_seconds=4,
            ),
            clock=lambda: BASE_TIME + timedelta(seconds=elapsed),
            monotonic=monotonic,
            sleep=sleep,
            process_factory=lambda *_args, **_kwargs: StubbornProcess(),
        )

    assert attempts[1] - attempts[0] < 1


def test_teardown_heartbeats_while_waiting_for_process_group(tmp_path, monkeypatch) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    elapsed = 0.0
    heartbeat_times = []
    original_heartbeat = TaskCoordinator.heartbeat_claim

    class StubbornProcess:
        pid = 1234
        returncode = None

        def poll(self):
            return None

        def wait(self, timeout=None):
            self.returncode = -signal.SIGKILL
            return self.returncode

    def monotonic():
        return elapsed

    def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    def killpg(_pid, sig):
        if sig == 0 and elapsed >= 4:
            raise ProcessLookupError

    def heartbeat(self, *args, **kwargs):
        heartbeat_times.append(elapsed)
        return original_heartbeat(self, *args, **kwargs)

    monkeypatch.setattr(os, "killpg", killpg)
    monkeypatch.setattr(TaskCoordinator, "heartbeat_claim", heartbeat)

    result = run_with_lease(
        store,
        request(
            tmp_path,
            lease_seconds=3,
            heartbeat_seconds=1,
            timeout_seconds=0.5,
            terminate_grace_seconds=4,
        ),
        clock=lambda: BASE_TIME + timedelta(seconds=elapsed),
        monotonic=monotonic,
        sleep=sleep,
        process_factory=lambda *_args, **_kwargs: StubbornProcess(),
    )

    assert result.state is LeaseRunState.TIMED_OUT
    assert len(heartbeat_times) >= 3


def test_wall_clock_timeout_includes_system_suspend(tmp_path) -> None:
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    wall_elapsed = 0.0
    torn_down = []

    class RunningProcess:
        pid = 1234
        returncode = None

        def poll(self):
            return None

    def sleep(seconds):
        nonlocal wall_elapsed
        wall_elapsed += seconds

    result = run_with_lease(
        store,
        request(
            tmp_path,
            lease_seconds=30,
            heartbeat_seconds=5,
            timeout_seconds=1,
        ),
        clock=lambda: BASE_TIME + timedelta(seconds=wall_elapsed),
        monotonic=lambda: 0,
        sleep=sleep,
        process_factory=lambda *_args, **_kwargs: RunningProcess(),
        teardown=lambda process, grace: torn_down.append((process.pid, grace)),
    )

    assert result.state is LeaseRunState.TIMED_OUT
    assert torn_down == [(1234, 0.2)]


@pytest.mark.skipif(os.name != "posix", reason="process groups require POSIX")
def test_process_guard_kills_child_when_wrapper_pipe_closes(tmp_path) -> None:
    child_pid_file = tmp_path / "child.pid"
    read_fd, write_fd = os.pipe()
    guard = subprocess.Popen(
        (
            sys.executable,
            "-m",
            "agent_coordinator.process_guard",
            "--parent-fd",
            str(read_fd),
            "--grace-seconds",
            "0.1",
            "--",
            sys.executable,
            "-c",
            (
                "import os,time,pathlib;"
                f"pathlib.Path({str(child_pid_file)!r}).write_text(str(os.getpid()));"
                "time.sleep(60)"
            ),
        ),
        pass_fds=(read_fd,),
        start_new_session=True,
    )
    os.close(read_fd)
    try:
        for _ in range(100):
            if child_pid_file.exists():
                break
            time.sleep(0.01)
        child_pid = int(child_pid_file.read_text())
        os.close(write_fd)
        guard.wait(timeout=5)
        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
    finally:
        if guard.poll() is None:
            os.killpg(guard.pid, signal.SIGKILL)
        if write_fd >= 0:
            try:
                os.close(write_fd)
            except OSError:
                pass
