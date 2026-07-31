from datetime import datetime, timedelta, timezone

import pytest

from agent_coordinator import (
    ClaimConflictError,
    JsonlClaimStore,
    OwnerIdentity,
    TaskCoordinator,
)
from agent_coordinator.lease_runner import LeaseKey, canonical_worktree_resource


BASE_TIME = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


def owner(session_id: str, pid: int) -> OwnerIdentity:
    return OwnerIdentity(
        session_id=session_id,
        pid=pid,
        agent="test-runner",
        worktree_path="/worktree",
    )


def test_lease_key_maps_namespace_and_resource_to_stable_task_identity() -> None:
    key = LeaseKey(
        namespace="local-frontend-test",
        resource_key="/repo/worktree",
    )

    assert key.task_identity().to_dict() == {
        "task_type": "local-frontend-test",
        "task_id": "/repo/worktree",
        "fingerprint": "exclusive-resource-lease:v1",
    }


@pytest.mark.parametrize(
    ("namespace", "resource_key"),
    [
        ("", "/repo/worktree"),
        ("local-frontend-test", ""),
    ],
)
def test_lease_key_rejects_empty_components(
    namespace: str,
    resource_key: str,
) -> None:
    with pytest.raises(ValueError):
        LeaseKey(namespace=namespace, resource_key=resource_key)


def test_canonical_worktree_resource_converges_path_aliases(tmp_path) -> None:
    worktree = tmp_path / "repo" / "worktree"
    worktree.mkdir(parents=True)
    alias = tmp_path / "worktree-alias"
    alias.symlink_to(worktree, target_is_directory=True)

    assert canonical_worktree_resource(alias) == canonical_worktree_resource(
        worktree / ".." / "worktree"
    )
    assert canonical_worktree_resource(alias) == str(worktree.resolve())


def test_canonical_worktree_resource_rejects_missing_path(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        canonical_worktree_resource(tmp_path / "missing")


@pytest.mark.parametrize("pid_is_live", [lambda _pid: False, lambda _pid: True])
def test_lease_only_reclaim_never_uses_pid_evidence_before_expiry(
    tmp_path,
    pid_is_live,
) -> None:
    coordinator = TaskCoordinator(
        JsonlClaimStore(tmp_path / "claims.jsonl"),
        pid_is_live=pid_is_live,
        reclaim_dead_owners=False,
    )
    task = LeaseKey(
        namespace="local-frontend-test",
        resource_key="/repo/worktree",
    ).task_identity()
    first = coordinator.claim_task(
        task,
        owner("first", 4242),
        lease_seconds=60,
        now=BASE_TIME,
    )

    with pytest.raises(ClaimConflictError):
        coordinator.claim_task(
            task,
            owner("second", 4242),
            lease_seconds=60,
            now=BASE_TIME + timedelta(seconds=59),
        )

    successor = coordinator.claim_task(
        task,
        owner("second", 4242),
        lease_seconds=60,
        now=BASE_TIME + timedelta(seconds=60),
    )
    assert successor.claim_id != first.claim_id
    assert successor.lease_epoch > first.lease_epoch


def test_different_namespaces_and_resources_do_not_contend(tmp_path) -> None:
    coordinator = TaskCoordinator(
        JsonlClaimStore(tmp_path / "claims.jsonl"),
        reclaim_dead_owners=False,
    )
    local = LeaseKey("local-frontend-test", "/repo/one").task_identity()
    ci = LeaseKey("ci-frontend-test", "/repo/one").task_identity()
    other = LeaseKey("local-frontend-test", "/repo/two").task_identity()

    claims = [
        coordinator.claim_task(
            task,
            owner(f"owner-{index}", 100 + index),
            lease_seconds=60,
            now=BASE_TIME,
        )
        for index, task in enumerate((local, ci, other))
    ]

    assert len({claim.claim_id for claim in claims}) == 3
