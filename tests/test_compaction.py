from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from agent_coordinator.models import OwnerIdentity, TaskIdentity
from agent_coordinator.service import ClaimState, TaskCoordinator
from agent_coordinator.store import JsonlClaimStore


BASE_TIME = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


def task(name: str) -> TaskIdentity:
    return TaskIdentity(
        task_type="pr-maintenance",
        task_id=f"github:Boundless-Studios/example#{name}",
        fingerprint=f"comments:{name}",
    )


def owner(session_id: str, pid: int) -> OwnerIdentity:
    return OwnerIdentity(
        session_id=session_id,
        pid=pid,
        agent="codex",
        worktree_path=f"/tmp/{session_id}",
    )


def coordinator(tmp_path, *, threshold: int = 5) -> TaskCoordinator:
    return TaskCoordinator(
        JsonlClaimStore(tmp_path / "claims.jsonl"),
        pid_is_live=lambda _pid: True,
        compaction_event_threshold=threshold,
        claim_history_retention=timedelta(hours=1),
    )


def test_compaction_prunes_old_terminal_claims_and_keeps_recent_history(tmp_path):
    coord = coordinator(tmp_path)

    old = coord.claim_task(
        task("old"), owner("old", 101), lease_seconds=60, now=BASE_TIME
    )
    coord.release_claim(
        old.claim_id,
        owner_session_id="old",
        lease_epoch=old.lease_epoch,
        now=BASE_TIME + timedelta(seconds=1),
    )

    recent_time = BASE_TIME + timedelta(hours=2)
    recent = coord.claim_task(
        task("recent"), owner("recent", 202), lease_seconds=60, now=recent_time
    )
    coord.release_claim(
        recent.claim_id,
        owner_session_id="recent",
        lease_epoch=recent.lease_epoch,
        reason="completed",
        now=recent_time + timedelta(seconds=1),
    )
    live = coord.claim_task(
        task("live"),
        owner("live", 303),
        lease_seconds=600,
        now=recent_time + timedelta(seconds=2),
    )

    events = coord.store.read_events()
    assert len(events) == 3
    assert coord.claim_by_id(old.claim_id) is None
    assert coord.claim_by_id(recent.claim_id).status == "completed"
    assert coord.status(task("live"), now=recent_time + timedelta(seconds=3)).state is ClaimState.ACTIVE
    assert coord.claim_by_id(live.claim_id) is not None


def test_compaction_keeps_expired_claim_for_retention_then_prunes_it(tmp_path):
    coord = coordinator(tmp_path, threshold=3)
    expired = coord.claim_task(
        task("expired"), owner("expired", 101), lease_seconds=30, now=BASE_TIME
    )
    coord.claim_task(
        task("trigger-one"), owner("trigger-one", 202), lease_seconds=60,
        now=BASE_TIME + timedelta(minutes=30),
    )
    coord.claim_task(
        task("trigger-two"), owner("trigger-two", 303), lease_seconds=60,
        now=BASE_TIME + timedelta(minutes=31),
    )

    assert coord.claim_by_id(expired.claim_id) is not None

    coord.claim_task(
        task("late-trigger"), owner("late-trigger", 404), lease_seconds=60,
        now=BASE_TIME + timedelta(hours=2),
    )
    coord.claim_task(
        task("late-trigger-two"), owner("late-trigger-two", 505), lease_seconds=60,
        now=BASE_TIME + timedelta(hours=2, seconds=1),
    )
    coord.claim_task(
        task("late-trigger-three"), owner("late-trigger-three", 606), lease_seconds=60,
        now=BASE_TIME + timedelta(hours=2, seconds=2),
    )

    assert coord.claim_by_id(expired.claim_id) is None


def test_compaction_preserves_monotonic_lease_epoch_after_pruning(tmp_path):
    coord = coordinator(tmp_path, threshold=3)
    old = coord.claim_task(
        task("old"), owner("old", 101), lease_seconds=60, now=BASE_TIME
    )
    coord.release_claim(
        old.claim_id,
        owner_session_id="old",
        lease_epoch=old.lease_epoch,
        now=BASE_TIME + timedelta(seconds=1),
    )
    later = coord.claim_task(
        task("later"), owner("later", 202), lease_seconds=60,
        now=BASE_TIME + timedelta(hours=2),
    )
    newest = coord.claim_task(
        task("newest"), owner("newest", 303), lease_seconds=60,
        now=BASE_TIME + timedelta(hours=2, seconds=1),
    )

    assert later.lease_epoch > old.lease_epoch
    assert newest.lease_epoch > later.lease_epoch


def test_failed_atomic_rewrite_leaves_original_log_unchanged(tmp_path, monkeypatch):
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    store.append_event({"event": "seed", "timestamp": "2026-07-20T12:00:00Z"})
    original = store.path.read_bytes()

    def fail_replace(_source, _destination):
        raise OSError("simulated replace failure")

    monkeypatch.setattr("agent_coordinator.store.os.replace", fail_replace)
    with pytest.raises(OSError, match="simulated replace failure"):
        store.transact_event(
            lambda _events: {"event": "next"},
            compact_events=lambda events: events,
        )

    assert store.path.read_bytes() == original
    assert list(tmp_path.glob(".claims.jsonl.*.tmp")) == []


def test_legacy_store_override_keeps_working_without_compaction_keyword(tmp_path):
    class LegacyStore(JsonlClaimStore):
        def transact_event(self, build_event):
            return super().transact_event(build_event)

    coord = TaskCoordinator(
        LegacyStore(tmp_path / "claims.jsonl"),
        pid_is_live=lambda _pid: True,
        compaction_event_threshold=1,
    )

    claim = coord.claim_task(
        task("legacy-store"), owner("legacy-store", 101),
        lease_seconds=60, now=BASE_TIME,
    )

    assert coord.claim_by_id(claim.claim_id) == claim
