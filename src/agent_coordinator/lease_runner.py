"""Exclusive resource leases for managed local commands."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .models import TaskIdentity


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
