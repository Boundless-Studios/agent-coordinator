from __future__ import annotations

from datetime import datetime, timedelta, timezone
import threading

import pytest

from agent_coordinator.models import OwnerIdentity, TaskIdentity
from agent_coordinator import service as coordinator_service
from agent_coordinator.service import ClaimConflictError, ClaimState, TaskCoordinator
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


def test_claim_creates_active_lease(tmp_path):
    coord = coordinator(tmp_path)

    claim = coord.claim_task(task(), owner(), lease_seconds=60, now=BASE_TIME)
    status = coord.status(task(), now=BASE_TIME + timedelta(seconds=10))

    assert status.state is ClaimState.ACTIVE
    assert status.claim is not None
    assert status.claim.claim_id == claim.claim_id
    assert status.reclaimable is False


def test_foreign_active_claim_blocks_new_owner(tmp_path):
    coord = coordinator(tmp_path)
    coord.claim_task(task(), owner("s1", 101), lease_seconds=60, now=BASE_TIME)

    with pytest.raises(ClaimConflictError):
        coord.claim_task(task(), owner("s2", 202), lease_seconds=60, now=BASE_TIME + timedelta(seconds=5))


def test_same_owner_claim_refreshes_lease(tmp_path):
    coord = coordinator(tmp_path)
    first = coord.claim_task(task(), owner("s1", 101), lease_seconds=60, now=BASE_TIME)

    refreshed = coord.claim_task(
        task(),
        owner("s1", 101),
        lease_seconds=120,
        now=BASE_TIME + timedelta(seconds=30),
    )
    status = coord.status(task(), now=BASE_TIME + timedelta(seconds=100))

    assert refreshed.claim_id == first.claim_id
    assert first.lease_epoch == 1
    assert refreshed.lease_epoch == first.lease_epoch
    assert status.state is ClaimState.ACTIVE
    assert status.claim is not None
    assert status.claim.lease_expires_at == BASE_TIME + timedelta(seconds=150)


def test_heartbeat_extends_existing_claim(tmp_path):
    coord = coordinator(tmp_path)
    claim = coord.claim_task(task(), owner(), lease_seconds=60, now=BASE_TIME)

    updated = coord.heartbeat_claim(
        claim.claim_id,
        owner_session_id="s1",
        lease_epoch=claim.lease_epoch,
        lease_seconds=90,
        now=BASE_TIME + timedelta(seconds=30),
    )

    assert updated.heartbeat_at == BASE_TIME + timedelta(seconds=30)
    assert updated.lease_expires_at == BASE_TIME + timedelta(seconds=120)


def test_expired_claim_is_reclaimable(tmp_path):
    coord = coordinator(tmp_path)
    coord.claim_task(task(), owner(), lease_seconds=30, now=BASE_TIME)

    status = coord.status(task(), now=BASE_TIME + timedelta(seconds=31))

    assert status.state is ClaimState.EXPIRED
    assert status.reclaimable is True


def test_released_claim_is_reclaimable_immediately(tmp_path):
    coord = coordinator(tmp_path)
    claim = coord.claim_task(task(), owner(), lease_seconds=300, now=BASE_TIME)

    coord.release_claim(
        claim.claim_id,
        owner_session_id="s1",
        lease_epoch=claim.lease_epoch,
        reason="completed",
        now=BASE_TIME + timedelta(seconds=5),
    )
    status = coord.status(task(), now=BASE_TIME + timedelta(seconds=6))

    assert status.state is ClaimState.RELEASED
    assert status.reclaimable is True


def test_changed_fingerprint_is_not_suppressed_by_old_claim(tmp_path):
    coord = coordinator(tmp_path)
    coord.claim_task(task("comments:a"), owner(), lease_seconds=300, now=BASE_TIME)

    status = coord.status(task("comments:b"), now=BASE_TIME + timedelta(seconds=10))

    assert status.state is ClaimState.NO_CLAIM
    assert status.reclaimable is True


def test_dead_owner_claim_is_reclaimable(tmp_path):
    coord = coordinator(tmp_path, live_pids=set())
    coord.claim_task(task(), owner(pid=101), lease_seconds=300, now=BASE_TIME)

    status = coord.status(task(), now=BASE_TIME + timedelta(seconds=10))

    assert status.state is ClaimState.OWNER_DEAD
    assert status.reclaimable is True


def test_new_claims_allocate_monotonic_lease_epochs(tmp_path):
    coord = coordinator(tmp_path)

    first = coord.claim_task(task(), owner("s1", 101), lease_seconds=1, now=BASE_TIME)
    second = coord.claim_task(
        task(),
        owner("s2", 202),
        lease_seconds=60,
        now=BASE_TIME + timedelta(seconds=2),
    )

    assert first.lease_epoch == 1
    assert second.lease_epoch == 2


def test_stale_epoch_cannot_heartbeat_or_release_successor_claim(tmp_path):
    assert hasattr(coordinator_service, "StaleClaimError"), (
        "agent_coordinator.service must expose StaleClaimError"
    )
    stale_claim_error = coordinator_service.StaleClaimError
    coord = coordinator(tmp_path)
    first = coord.claim_task(task(), owner("s1", 101), lease_seconds=1, now=BASE_TIME)
    second = coord.claim_task(
        task(),
        owner("s2", 202),
        lease_seconds=60,
        now=BASE_TIME + timedelta(seconds=2),
    )

    with pytest.raises(stale_claim_error):
        coord.heartbeat_claim(
            second.claim_id,
            owner_session_id="s2",
            lease_epoch=first.lease_epoch,
            lease_seconds=60,
            now=BASE_TIME + timedelta(seconds=3),
        )
    with pytest.raises(stale_claim_error):
        coord.release_claim(
            second.claim_id,
            owner_session_id="s2",
            lease_epoch=first.lease_epoch,
            now=BASE_TIME + timedelta(seconds=3),
        )


def test_legacy_claim_event_without_epoch_decodes_as_zero(tmp_path):
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    legacy_claim = {
        "claim_id": "legacy-claim",
        "task": task().to_dict(),
        "owner": owner().to_dict(),
        "claimed_at": BASE_TIME.isoformat().replace("+00:00", "Z"),
        "heartbeat_at": BASE_TIME.isoformat().replace("+00:00", "Z"),
        "lease_expires_at": (BASE_TIME + timedelta(seconds=60)).isoformat().replace(
            "+00:00", "Z"
        ),
        "status": "active",
        "release_reason": None,
    }
    store.append_event(
        {
            "event": "claimed",
            "timestamp": BASE_TIME.isoformat().replace("+00:00", "Z"),
            "claim": legacy_claim,
        }
    )

    status = TaskCoordinator(store, pid_is_live=lambda _pid: True).status(
        task(), now=BASE_TIME
    )

    assert status.claim is not None
    assert status.claim.lease_epoch == 0


@pytest.mark.parametrize("reclaim_reason", ["expired", "owner_dead", "released"])
def test_reclaim_allocates_higher_epoch(tmp_path, reclaim_reason):
    live_pids = {101, 202}
    coord = coordinator(tmp_path, live_pids=live_pids)
    first = coord.claim_task(task(), owner("s1", 101), lease_seconds=60, now=BASE_TIME)
    reclaim_time = BASE_TIME + timedelta(seconds=1)

    if reclaim_reason == "expired":
        reclaim_time = BASE_TIME + timedelta(seconds=61)
    elif reclaim_reason == "owner_dead":
        live_pids.remove(101)
    else:
        coord.release_claim(
            first.claim_id,
            owner_session_id="s1",
            lease_epoch=first.lease_epoch,
            now=reclaim_time,
        )

    successor = coord.claim_task(
        task(),
        owner("s2", 202),
        lease_seconds=60,
        now=reclaim_time,
    )

    assert successor.lease_epoch > first.lease_epoch


def test_concurrent_claim_race_has_exactly_one_active_owner(tmp_path):
    barrier = threading.Barrier(2)

    class BarrierStore(JsonlClaimStore):
        def read_events(self):
            events = super().read_events()
            if threading.current_thread() is not threading.main_thread():
                barrier.wait(timeout=5)
            return events

    store = BarrierStore(tmp_path / "claims.jsonl")
    coordinators = [
        TaskCoordinator(store, pid_is_live=lambda _pid: True),
        TaskCoordinator(store, pid_is_live=lambda _pid: True),
    ]
    outcomes: list[object] = []

    def claim(index: int) -> None:
        try:
            outcomes.append(
                coordinators[index].claim_task(
                    task(),
                    owner(f"s{index + 1}", 101 + index),
                    lease_seconds=60,
                    now=BASE_TIME,
                )
            )
        except Exception as exc:  # The assertion below validates the exact failure.
            outcomes.append(exc)

    threads = [threading.Thread(target=claim, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)

    successes = [result for result in outcomes if not isinstance(result, Exception)]
    conflicts = [result for result in outcomes if isinstance(result, ClaimConflictError)]
    claimed_events = [
        event for event in store.read_events() if event.get("event") == "claimed"
    ]

    assert len(successes) == 1
    assert len(conflicts) == 1
    assert len(claimed_events) == 1
    assert claimed_events[0]["claim"]["lease_epoch"] == 1


def test_store_transaction_sees_prior_events_and_appends_once(tmp_path):
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    store.append_event({"event": "seed", "value": 1})
    observed: list[dict] = []

    def build_event(events):
        observed.extend(events)
        return {"event": "next", "value": len(events) + 1}

    appended = store.transact_event(build_event)

    assert observed == [{"event": "seed", "value": 1}]
    assert appended == {"event": "next", "value": 2}
    assert store.read_events() == [
        {"event": "seed", "value": 1},
        {"event": "next", "value": 2},
    ]


def test_store_transaction_builder_exception_appends_nothing(tmp_path):
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    store.append_event({"event": "seed"})

    def fail_builder(_events):
        raise RuntimeError("decision failed")

    with pytest.raises(RuntimeError, match="decision failed"):
        store.transact_event(fail_builder)

    assert store.read_events() == [{"event": "seed"}]
