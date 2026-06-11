from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from agent_coordinator.models import OwnerIdentity, TaskIdentity
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
    assert status.state is ClaimState.ACTIVE
    assert status.claim is not None
    assert status.claim.lease_expires_at == BASE_TIME + timedelta(seconds=150)


def test_heartbeat_extends_existing_claim(tmp_path):
    coord = coordinator(tmp_path)
    claim = coord.claim_task(task(), owner(), lease_seconds=60, now=BASE_TIME)

    updated = coord.heartbeat_claim(
        claim.claim_id,
        owner_session_id="s1",
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

    coord.release_claim(claim.claim_id, owner_session_id="s1", reason="completed", now=BASE_TIME + timedelta(seconds=5))
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
