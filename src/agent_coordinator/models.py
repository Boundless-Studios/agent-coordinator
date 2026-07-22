"""Core value objects for task ownership coordination."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def datetime_to_json(value: datetime) -> str:
    return normalize_datetime(value).isoformat().replace("+00:00", "Z")


def datetime_from_json(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return normalize_datetime(parsed)


@dataclass(frozen=True)
class TaskIdentity:
    task_type: str
    task_id: str
    fingerprint: str

    def __post_init__(self) -> None:
        if not self.task_type:
            raise ValueError("task_type is required")
        if not self.task_id:
            raise ValueError("task_id is required")
        if not self.fingerprint:
            raise ValueError("fingerprint is required")

    def to_dict(self) -> dict[str, str]:
        return {
            "task_type": self.task_type,
            "task_id": self.task_id,
            "fingerprint": self.fingerprint,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TaskIdentity":
        return cls(
            task_type=str(payload["task_type"]),
            task_id=str(payload["task_id"]),
            fingerprint=str(payload["fingerprint"]),
        )


@dataclass(frozen=True)
class OwnerIdentity:
    session_id: str
    pid: int | None = None
    agent: str = "unknown"
    worktree_path: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.session_id:
            raise ValueError("session_id is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "pid": self.pid,
            "agent": self.agent,
            "worktree_path": self.worktree_path,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "OwnerIdentity":
        raw_pid = payload.get("pid")
        return cls(
            session_id=str(payload["session_id"]),
            pid=int(raw_pid) if raw_pid is not None else None,
            agent=str(payload.get("agent") or "unknown"),
            worktree_path=payload.get("worktree_path"),
            metadata={
                str(k): str(v) for k, v in dict(payload.get("metadata") or {}).items()
            },
        )


@dataclass(frozen=True)
class ClaimRecord:
    claim_id: str
    task: TaskIdentity
    owner: OwnerIdentity
    claimed_at: datetime
    heartbeat_at: datetime
    lease_expires_at: datetime
    lease_epoch: int = 0
    status: str = "active"
    release_reason: str | None = None

    def __post_init__(self) -> None:
        if self.lease_epoch < 0:
            raise ValueError("lease_epoch must be non-negative")

    def to_dict(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "task": self.task.to_dict(),
            "owner": self.owner.to_dict(),
            "claimed_at": datetime_to_json(self.claimed_at),
            "heartbeat_at": datetime_to_json(self.heartbeat_at),
            "lease_expires_at": datetime_to_json(self.lease_expires_at),
            "lease_epoch": self.lease_epoch,
            "status": self.status,
            "release_reason": self.release_reason,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ClaimRecord":
        return cls(
            claim_id=str(payload["claim_id"]),
            task=TaskIdentity.from_dict(dict(payload["task"])),
            owner=OwnerIdentity.from_dict(dict(payload["owner"])),
            claimed_at=datetime_from_json(str(payload["claimed_at"])),
            heartbeat_at=datetime_from_json(str(payload["heartbeat_at"])),
            lease_expires_at=datetime_from_json(str(payload["lease_expires_at"])),
            lease_epoch=int(payload.get("lease_epoch", 0)),
            status=str(payload.get("status") or "active"),
            release_reason=payload.get("release_reason"),
        )
