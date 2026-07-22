from __future__ import annotations

from datetime import datetime, timedelta, timezone
import stat

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
    assert (
        coord.status(task("live"), now=recent_time + timedelta(seconds=3)).state
        is ClaimState.ACTIVE
    )
    assert coord.claim_by_id(live.claim_id) is not None


def test_compaction_keeps_expired_claim_for_retention_then_prunes_it(tmp_path):
    coord = coordinator(tmp_path, threshold=3)
    expired = coord.claim_task(
        task("expired"), owner("expired", 101), lease_seconds=30, now=BASE_TIME
    )
    coord.claim_task(
        task("trigger-one"),
        owner("trigger-one", 202),
        lease_seconds=60,
        now=BASE_TIME + timedelta(minutes=30),
    )
    coord.claim_task(
        task("trigger-two"),
        owner("trigger-two", 303),
        lease_seconds=60,
        now=BASE_TIME + timedelta(minutes=31),
    )

    assert coord.claim_by_id(expired.claim_id) is not None

    coord.claim_task(
        task("late-trigger"),
        owner("late-trigger", 404),
        lease_seconds=60,
        now=BASE_TIME + timedelta(hours=2),
    )
    coord.claim_task(
        task("late-trigger-two"),
        owner("late-trigger-two", 505),
        lease_seconds=60,
        now=BASE_TIME + timedelta(hours=2, seconds=1),
    )
    coord.claim_task(
        task("late-trigger-three"),
        owner("late-trigger-three", 606),
        lease_seconds=60,
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
        task("later"),
        owner("later", 202),
        lease_seconds=60,
        now=BASE_TIME + timedelta(hours=2),
    )
    newest = coord.claim_task(
        task("newest"),
        owner("newest", 303),
        lease_seconds=60,
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


def test_compaction_preserves_existing_claims_log_permissions(tmp_path):
    store_path = tmp_path / "claims.jsonl"
    store = JsonlClaimStore(store_path)
    store.append_event({"event": "existing"})
    store_path.chmod(0o660)

    store.transact_event(
        lambda _events: {"event": "new"},
        compact_events=lambda events: events,
    )

    assert stat.S_IMODE(store_path.stat().st_mode) == 0o660


def test_compaction_tolerates_inability_to_preserve_claims_log_owner(
    tmp_path,
    monkeypatch,
):
    store_path = tmp_path / "claims.jsonl"
    store = JsonlClaimStore(store_path)
    store.append_event({"event": "existing"})
    store_path.chmod(0o660)

    def reject_owner_change(_descriptor, _uid, _gid):
        raise PermissionError("simulated shared-ledger owner")

    monkeypatch.setattr("agent_coordinator.store.os.fchown", reject_owner_change)

    store.transact_event(
        lambda _events: {"event": "new"},
        compact_events=lambda events: events,
    )

    assert store.read_events() == [{"event": "existing"}, {"event": "new"}]
    assert stat.S_IMODE(store_path.stat().st_mode) == 0o660


def test_compaction_falls_back_to_append_when_owner_cannot_be_preserved(
    tmp_path,
    monkeypatch,
):
    store_path = tmp_path / "claims.jsonl"
    store = JsonlClaimStore(store_path)
    store.append_event({"event": "existing"})
    original_inode = store_path.stat().st_ino

    monkeypatch.setattr(
        "agent_coordinator.store.os.fchown",
        lambda *_args: (_ for _ in ()).throw(PermissionError("shared owner")),
    )
    store.transact_event(
        lambda _events: {"event": "new"},
        compact_events=lambda _events: [{"event": "compacted"}],
    )

    assert store_path.stat().st_ino == original_inode
    assert store.read_events() == [{"event": "existing"}, {"event": "new"}]


def test_compaction_replaces_symlink_target_without_replacing_symlink(tmp_path):
    target_path = tmp_path / "shared" / "claims.jsonl"
    target_path.parent.mkdir()
    target_path.write_text('{"event": "existing"}\n', encoding="utf-8")
    store_path = tmp_path / "claims.jsonl"
    store_path.symlink_to(target_path)
    store = JsonlClaimStore(store_path)

    store.transact_event(
        lambda _events: {"event": "new"},
        compact_events=lambda events: events,
    )

    assert store_path.is_symlink()
    assert JsonlClaimStore(target_path).read_events() == [
        {"event": "existing"},
        {"event": "new"},
    ]


def test_symlink_and_direct_stores_share_the_same_lock(tmp_path):
    target_path = tmp_path / "shared" / "claims.jsonl"
    target_path.parent.mkdir()
    target_path.touch()
    symlink_path = tmp_path / "claims.jsonl"
    symlink_path.symlink_to(target_path)

    assert (
        JsonlClaimStore(symlink_path).lock_path
        == JsonlClaimStore(target_path).lock_path
    )


def test_relative_store_path_remains_anchored_after_chdir(tmp_path, monkeypatch):
    original_dir = tmp_path / "original"
    later_dir = tmp_path / "later"
    original_dir.mkdir()
    later_dir.mkdir()
    monkeypatch.chdir(original_dir)
    store = JsonlClaimStore("claims.jsonl")
    monkeypatch.chdir(later_dir)

    store.append_event({"event": "anchored"})

    assert store.path == (original_dir / "claims.jsonl").resolve()
    assert store.read_events() == [{"event": "anchored"}]
    assert not (later_dir / "claims.jsonl").exists()


def test_compaction_normalizes_naive_legacy_timestamps(tmp_path):
    coord = coordinator(tmp_path, threshold=2)
    coord.claim_task(
        task("naive"),
        owner("naive", 101),
        lease_seconds=60,
        now=datetime(2026, 7, 20, 12, 0),
    )

    coord.claim_task(
        task("aware"),
        owner("aware", 202),
        lease_seconds=60,
        now=BASE_TIME + timedelta(hours=2),
    )

    assert (
        coord.status(task("aware"), now=BASE_TIME + timedelta(hours=2)).state
        is ClaimState.ACTIVE
    )


def test_compaction_copies_extended_metadata(tmp_path, monkeypatch):
    store_path = tmp_path / "claims.jsonl"
    store = JsonlClaimStore(store_path)
    store.append_event({"event": "existing"})
    copied: list[tuple[object, object]] = []

    def record_copystat(source, destination):
        copied.append((source, destination))

    monkeypatch.setattr("agent_coordinator.store.shutil.copystat", record_copystat)

    store.transact_event(
        lambda _events: {"event": "new"},
        compact_events=lambda events: events,
    )

    assert len(copied) == 1
    assert copied[0][0] == store_path


def test_compaction_retains_unrecognized_events(tmp_path):
    coord = coordinator(tmp_path, threshold=2)
    unknown_event = {
        "event": "future_extension",
        "timestamp": "2026-07-20T12:00:00Z",
        "payload": {"audit": "retain me"},
    }
    coord.store.append_event(unknown_event)

    coord.claim_task(
        task("trigger"),
        owner("trigger", 101),
        lease_seconds=60,
        now=BASE_TIME,
    )

    assert unknown_event in coord.store.read_events()


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
        task("legacy-store"),
        owner("legacy-store", 101),
        lease_seconds=60,
        now=BASE_TIME,
    )

    assert coord.claim_by_id(claim.claim_id) == claim
