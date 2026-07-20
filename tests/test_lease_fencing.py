"""BOU-2209: lease epochs must actually fence a deposed owner.

The v0.2.0 fence compared the caller's epoch against *that claim's own*
``lease_epoch``, so a supervisor whose lease expired and was superseded could
still heartbeat its own (stale) claim id and keep a second runtime alive.
These tests pin the fence to the task's current epoch and require the check to
happen inside the same store transaction as the write.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import multiprocessing
import queue

import pytest

from agent_coordinator.models import ClaimRecord, OwnerIdentity, TaskIdentity
from agent_coordinator.service import (
    ClaimConflictError,
    ClaimState,
    StaleClaimError,
    TaskCoordinator,
)
from agent_coordinator.store import JsonlClaimStore


BASE_TIME = datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc)


def coordinator(tmp_path, *, live_pids: set[int] | None = None) -> TaskCoordinator:
    live = live_pids if live_pids is not None else {101, 202}
    return TaskCoordinator(
        JsonlClaimStore(tmp_path / "claims.jsonl"),
        pid_is_live=lambda pid: pid in live,
    )


def task(fingerprint: str = "comments:a") -> TaskIdentity:
    return TaskIdentity(
        task_type="pr-maintenance",
        task_id="github:Boundless-Studios/agentic-pr-dash#8",
        fingerprint=fingerprint,
    )


def owner(session_id: str = "s1", pid: int = 101) -> OwnerIdentity:
    return OwnerIdentity(
        session_id=session_id,
        pid=pid,
        agent="codex",
        worktree_path="/tmp/worktree",
    )


def depose(tmp_path, *, lease_seconds: int = 30):
    """Build the deposed-owner scenario: A claims, A's lease expires, B claims.

    Returns ``(coord, first, second, deposed_at)`` where ``first`` is A's now
    superseded claim and ``second`` is B's live claim.
    """
    coord = coordinator(tmp_path)
    first = coord.claim_task(
        task(), owner("s1", 101), lease_seconds=lease_seconds, now=BASE_TIME
    )
    deposed_at = BASE_TIME + timedelta(seconds=lease_seconds + 1)
    second = coord.claim_task(
        task(), owner("s2", 202), lease_seconds=lease_seconds, now=deposed_at
    )
    assert second.lease_epoch > first.lease_epoch
    return coord, first, second, deposed_at


def test_deposed_owner_heartbeat_is_rejected(tmp_path):
    coord, first, second, deposed_at = depose(tmp_path)

    with pytest.raises(StaleClaimError) as excinfo:
        coord.heartbeat_claim(
            first.claim_id,
            owner_session_id="s1",
            lease_epoch=first.lease_epoch,
            lease_seconds=600,
            now=deposed_at + timedelta(seconds=1),
        )

    assert excinfo.value.received_epoch == first.lease_epoch
    assert excinfo.value.expected_epoch == second.lease_epoch


def test_deposed_owner_heartbeat_does_not_extend_its_lease(tmp_path):
    coord, first, second, deposed_at = depose(tmp_path)

    with pytest.raises(StaleClaimError):
        coord.heartbeat_claim(
            first.claim_id,
            owner_session_id="s1",
            lease_epoch=first.lease_epoch,
            lease_seconds=600,
            now=deposed_at + timedelta(seconds=1),
        )

    status = coord.status(task(), now=deposed_at + timedelta(seconds=2))
    assert status.state is ClaimState.ACTIVE
    assert status.claim is not None
    assert status.claim.claim_id == second.claim_id
    assert status.claim.lease_epoch == second.lease_epoch


def test_deposed_owner_release_is_rejected(tmp_path):
    coord, first, second, deposed_at = depose(tmp_path)

    with pytest.raises(StaleClaimError) as excinfo:
        coord.release_claim(
            first.claim_id,
            owner_session_id="s1",
            lease_epoch=first.lease_epoch,
            reason="completed",
            now=deposed_at + timedelta(seconds=1),
        )

    assert excinfo.value.expected_epoch == second.lease_epoch
    status = coord.status(task(), now=deposed_at + timedelta(seconds=2))
    assert status.state is ClaimState.ACTIVE
    assert status.claim is not None
    assert status.claim.claim_id == second.claim_id


def test_deposed_owner_learns_which_claim_deposed_it(tmp_path):
    coord, first, second, deposed_at = depose(tmp_path)

    with pytest.raises(StaleClaimError) as excinfo:
        coord.heartbeat_claim(
            first.claim_id,
            owner_session_id="s1",
            lease_epoch=first.lease_epoch,
            lease_seconds=600,
            now=deposed_at + timedelta(seconds=1),
        )

    assert excinfo.value.current_claim_id == second.claim_id


def test_superseded_claim_is_marked_inactive_at_claim_time(tmp_path):
    coord, first, second, _ = depose(tmp_path)

    superseded = coord.claim_by_id(first.claim_id)
    current = coord.claim_by_id(second.claim_id)

    assert superseded is not None
    assert superseded.status == "superseded"
    assert superseded.release_reason == "superseded"
    assert current is not None
    assert current.status == "active"


def test_rejected_stale_heartbeat_appends_no_event(tmp_path):
    coord, first, _, deposed_at = depose(tmp_path)
    before = len(coord.store.read_events())

    with pytest.raises(StaleClaimError):
        coord.heartbeat_claim(
            first.claim_id,
            owner_session_id="s1",
            lease_epoch=first.lease_epoch,
            lease_seconds=600,
            now=deposed_at + timedelta(seconds=1),
        )

    assert len(coord.store.read_events()) == before


def test_fence_is_evaluated_inside_the_write_transaction(tmp_path):
    """A check-then-act caller cannot squeeze a deposition into the gap.

    The successor claim lands between the stale owner's own status read and its
    heartbeat call -- exactly the TOCTOU window that a consumer-side pre-check
    leaves open. The heartbeat must still be rejected, because the fence is
    re-evaluated against the ledger inside ``transact_event``.
    """
    coord = coordinator(tmp_path)
    first = coord.claim_task(task(), owner("s1", 101), lease_seconds=30, now=BASE_TIME)

    # A pre-checks and still believes it owns the task.
    precheck = coord.status(task(), now=BASE_TIME + timedelta(seconds=1))
    assert precheck.state is ClaimState.ACTIVE
    assert precheck.claim is not None
    assert precheck.claim.claim_id == first.claim_id

    # A stalls past its lease; B takes over inside A's check-then-act window.
    takeover_at = BASE_TIME + timedelta(seconds=31)
    second = coord.claim_task(
        task(), owner("s2", 202), lease_seconds=30, now=takeover_at
    )

    with pytest.raises(StaleClaimError) as excinfo:
        coord.heartbeat_claim(
            first.claim_id,
            owner_session_id="s1",
            lease_epoch=first.lease_epoch,
            lease_seconds=600,
            now=takeover_at,
        )

    assert excinfo.value.expected_epoch == second.lease_epoch


def test_current_owner_heartbeat_still_succeeds_after_a_prior_deposition(tmp_path):
    coord, _, second, deposed_at = depose(tmp_path)

    updated = coord.heartbeat_claim(
        second.claim_id,
        owner_session_id="s2",
        lease_epoch=second.lease_epoch,
        lease_seconds=90,
        now=deposed_at + timedelta(seconds=5),
    )

    assert updated.claim_id == second.claim_id
    assert updated.lease_epoch == second.lease_epoch
    assert updated.lease_expires_at == deposed_at + timedelta(seconds=95)


def test_current_owner_release_still_succeeds_after_a_prior_deposition(tmp_path):
    coord, _, second, deposed_at = depose(tmp_path)

    released = coord.release_claim(
        second.claim_id,
        owner_session_id="s2",
        lease_epoch=second.lease_epoch,
        reason="completed",
        now=deposed_at + timedelta(seconds=5),
    )

    assert released.status == "completed"
    status = coord.status(task(), now=deposed_at + timedelta(seconds=6))
    assert status.state is ClaimState.RELEASED
    assert status.reclaimable is True


def test_repeated_heartbeats_by_the_live_owner_are_accepted(tmp_path):
    coord = coordinator(tmp_path)
    claim = coord.claim_task(task(), owner("s1", 101), lease_seconds=60, now=BASE_TIME)

    for offset in (10, 20, 30):
        updated = coord.heartbeat_claim(
            claim.claim_id,
            owner_session_id="s1",
            lease_epoch=claim.lease_epoch,
            lease_seconds=60,
            now=BASE_TIME + timedelta(seconds=offset),
        )
        assert updated.lease_epoch == claim.lease_epoch

    status = coord.status(task(), now=BASE_TIME + timedelta(seconds=35))
    assert status.state is ClaimState.ACTIVE


def test_expired_owner_can_still_release_when_nobody_deposed_it(tmp_path):
    coord = coordinator(tmp_path)
    claim = coord.claim_task(task(), owner("s1", 101), lease_seconds=30, now=BASE_TIME)

    released = coord.release_claim(
        claim.claim_id,
        owner_session_id="s1",
        lease_epoch=claim.lease_epoch,
        reason="completed",
        now=BASE_TIME + timedelta(seconds=90),
    )

    assert released.status == "completed"


def test_legacy_epoch_zero_claim_can_still_heartbeat(tmp_path):
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    legacy = ClaimRecord(
        claim_id="legacy-claim",
        task=task(),
        owner=owner(),
        claimed_at=BASE_TIME,
        heartbeat_at=BASE_TIME,
        lease_expires_at=BASE_TIME + timedelta(seconds=60),
    ).to_dict()
    legacy.pop("lease_epoch")
    store.append_event(
        {
            "event": "claimed",
            "timestamp": BASE_TIME.isoformat().replace("+00:00", "Z"),
            "claim": legacy,
        }
    )
    coord = TaskCoordinator(store, pid_is_live=lambda _pid: True)

    updated = coord.heartbeat_claim(
        "legacy-claim",
        owner_session_id="s1",
        lease_epoch=0,
        lease_seconds=60,
        now=BASE_TIME + timedelta(seconds=10),
    )

    assert updated.lease_epoch == 0


def test_claims_on_other_tasks_do_not_depose_this_task(tmp_path):
    coord = coordinator(tmp_path)
    mine = coord.claim_task(
        task("comments:a"), owner("s1", 101), lease_seconds=600, now=BASE_TIME
    )
    other = coord.claim_task(
        task("comments:b"),
        owner("s2", 202),
        lease_seconds=600,
        now=BASE_TIME + timedelta(seconds=1),
    )
    assert other.lease_epoch > mine.lease_epoch

    updated = coord.heartbeat_claim(
        mine.claim_id,
        owner_session_id="s1",
        lease_epoch=mine.lease_epoch,
        lease_seconds=600,
        now=BASE_TIME + timedelta(seconds=2),
    )

    assert updated.lease_epoch == mine.lease_epoch


def _race_claim_worker(store_path, index, barrier, results, now):
    """Race a claim from a separate OS process (module level so fork/spawn work)."""
    coord = TaskCoordinator(JsonlClaimStore(store_path), pid_is_live=lambda _pid: True)
    barrier.wait(timeout=30)
    try:
        record = coord.claim_task(
            task(),
            owner(f"p{index}", 1000 + index),
            lease_seconds=600,
            now=now,
        )
    except ClaimConflictError:
        results.put(("conflict", index, None))
    except Exception as exc:  # noqa: BLE001 - surfaced by the assertions below
        results.put(("error", index, repr(exc)))
    else:
        results.put(("won", index, record.lease_epoch))


def _run_claim_race(store_path, workers, now):
    context = multiprocessing.get_context("fork")
    barrier = context.Barrier(workers)
    results = context.Queue()
    processes = [
        context.Process(
            target=_race_claim_worker,
            args=(store_path, index, barrier, results, now),
        )
        for index in range(workers)
    ]
    for process in processes:
        process.start()

    outcomes = []
    for _ in range(workers):
        try:
            outcomes.append(results.get(timeout=60))
        except queue.Empty:  # pragma: no cover - only reached if a worker hangs
            break
    for process in processes:
        process.join(timeout=60)
        assert not process.is_alive()

    assert len(outcomes) == workers
    assert [item for item in outcomes if item[0] == "error"] == []
    return outcomes


def test_concurrent_processes_cannot_both_win_the_same_claim(tmp_path):
    store_path = str(tmp_path / "claims.jsonl")

    outcomes = _run_claim_race(store_path, workers=6, now=BASE_TIME)

    winners = [item for item in outcomes if item[0] == "won"]
    assert len(winners) == 1
    assert winners[0][2] == 1

    events = JsonlClaimStore(store_path).read_events()
    claimed = [event for event in events if event.get("event") == "claimed"]
    assert len(claimed) == 1

    coord = TaskCoordinator(JsonlClaimStore(store_path), pid_is_live=lambda _pid: True)
    status = coord.status(task(), now=BASE_TIME + timedelta(seconds=1))
    assert status.state is ClaimState.ACTIVE
    assert status.claim is not None
    assert status.claim.lease_epoch == 1


def test_concurrent_processes_cannot_both_reclaim_an_expired_claim(tmp_path):
    store_path = str(tmp_path / "claims.jsonl")
    seed = TaskCoordinator(JsonlClaimStore(store_path), pid_is_live=lambda _pid: True)
    seed.claim_task(task(), owner("s0", 999), lease_seconds=60, now=BASE_TIME)

    outcomes = _run_claim_race(
        store_path, workers=6, now=BASE_TIME + timedelta(seconds=61)
    )

    winners = [item for item in outcomes if item[0] == "won"]
    assert len(winners) == 1
    assert winners[0][2] == 2

    events = JsonlClaimStore(store_path).read_events()
    claimed = [event for event in events if event.get("event") == "claimed"]
    assert len(claimed) == 2
    assert sorted(event["claim"]["lease_epoch"] for event in claimed) == [1, 2]
