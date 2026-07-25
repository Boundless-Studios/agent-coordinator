"""Core value objects for task ownership coordination."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
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


DECISION_CATEGORIES = frozenset({"architecture", "product", "authority", "safety"})
MIN_DECISION_OPTIONS = 2
MAX_DECISION_OPTIONS = 3


class DecisionState(str, Enum):
    """Lifecycle of a human-owned decision.

    ``waiting_human`` and ``resumable`` are the two states the rest of the
    system reacts to. Nothing but an explicit human act advances between them.
    """

    WAITING_HUMAN = "waiting_human"
    RESUMABLE = "resumable"
    RESUMED = "resumed"
    CANCELLED = "cancelled"
    SUPERSEDED = "superseded"


@dataclass(frozen=True)
class DecisionOption:
    option_id: str
    summary: str
    trade_offs: str

    def __post_init__(self) -> None:
        if not self.option_id:
            raise ValueError("option_id is required")
        if not self.summary:
            raise ValueError("summary is required")
        if not self.trade_offs:
            raise ValueError("trade_offs is required")

    def to_dict(self) -> dict[str, str]:
        return {
            "option_id": self.option_id,
            "summary": self.summary,
            "trade_offs": self.trade_offs,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DecisionOption":
        return cls(
            option_id=str(payload["option_id"]),
            summary=str(payload["summary"]),
            trade_offs=str(payload["trade_offs"]),
        )


@dataclass(frozen=True)
class DecisionRequest:
    """A question an agent owes a human before it may cross a boundary.

    ``task.fingerprint`` fingerprints the *task*. ``fingerprint`` here covers
    the *evidence and task state at request time*, and is what a resolution must
    match. The two are deliberately separate: work can move on in ways that
    invalidate the question without changing task identity.
    """

    decision_id: str
    task: TaskIdentity
    claim_id: str
    logical_key: str
    category: str
    question: str
    options: tuple[DecisionOption, ...]
    recommendation: str
    rationale: str
    affected_scope: tuple[str, ...]
    fingerprint: str
    requesting_runtime: str
    requesting_session_id: str
    created_at: datetime

    def __post_init__(self) -> None:
        for field_name in (
            "decision_id",
            "claim_id",
            "logical_key",
            "question",
            "recommendation",
            "rationale",
            "fingerprint",
            "requesting_runtime",
            "requesting_session_id",
        ):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} is required")
        if self.category not in DECISION_CATEGORIES:
            raise ValueError(
                f"category must be one of {sorted(DECISION_CATEGORIES)}: "
                f"got {self.category!r}"
            )
        object.__setattr__(self, "options", tuple(self.options))
        object.__setattr__(self, "affected_scope", tuple(self.affected_scope))
        if not MIN_DECISION_OPTIONS <= len(self.options) <= MAX_DECISION_OPTIONS:
            raise ValueError(
                f"a decision needs {MIN_DECISION_OPTIONS}-{MAX_DECISION_OPTIONS} "
                f"options: got {len(self.options)}"
            )
        option_ids = [option.option_id for option in self.options]
        if len(set(option_ids)) != len(option_ids):
            raise ValueError("option ids must be unique")
        if not self.affected_scope:
            raise ValueError("affected_scope is required")
        object.__setattr__(self, "created_at", normalize_datetime(self.created_at))

    def has_option(self, option_id: str) -> bool:
        return any(option.option_id == option_id for option in self.options)

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "task": self.task.to_dict(),
            "claim_id": self.claim_id,
            "logical_key": self.logical_key,
            "category": self.category,
            "question": self.question,
            "options": [option.to_dict() for option in self.options],
            "recommendation": self.recommendation,
            "rationale": self.rationale,
            "affected_scope": list(self.affected_scope),
            "fingerprint": self.fingerprint,
            "requesting_runtime": self.requesting_runtime,
            "requesting_session_id": self.requesting_session_id,
            "created_at": datetime_to_json(self.created_at),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DecisionRequest":
        return cls(
            decision_id=str(payload["decision_id"]),
            task=TaskIdentity.from_dict(dict(payload["task"])),
            claim_id=str(payload["claim_id"]),
            logical_key=str(payload["logical_key"]),
            category=str(payload["category"]),
            question=str(payload["question"]),
            options=tuple(
                DecisionOption.from_dict(dict(option))
                for option in payload.get("options") or ()
            ),
            recommendation=str(payload["recommendation"]),
            rationale=str(payload["rationale"]),
            affected_scope=tuple(
                str(item) for item in payload.get("affected_scope") or ()
            ),
            fingerprint=str(payload["fingerprint"]),
            requesting_runtime=str(payload["requesting_runtime"]),
            requesting_session_id=str(payload["requesting_session_id"]),
            created_at=datetime_from_json(str(payload["created_at"])),
        )


@dataclass(frozen=True)
class DecisionResolution:
    """A human's answer, bound to the exact request it answers."""

    decision_id: str
    request_fingerprint: str
    human_actor: str
    resolved_at: datetime
    selected_option_id: str | None = None
    direction: str | None = None
    rationale: str | None = None

    def __post_init__(self) -> None:
        if not self.decision_id:
            raise ValueError("decision_id is required")
        if not self.request_fingerprint:
            raise ValueError("request_fingerprint is required")
        if not self.human_actor:
            raise ValueError("human_actor is required")
        if not self.selected_option_id and not self.direction:
            raise ValueError(
                "a resolution needs either selected_option_id or direction"
            )
        object.__setattr__(self, "resolved_at", normalize_datetime(self.resolved_at))

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "request_fingerprint": self.request_fingerprint,
            "human_actor": self.human_actor,
            "resolved_at": datetime_to_json(self.resolved_at),
            "selected_option_id": self.selected_option_id,
            "direction": self.direction,
            "rationale": self.rationale,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DecisionResolution":
        return cls(
            decision_id=str(payload["decision_id"]),
            request_fingerprint=str(payload["request_fingerprint"]),
            human_actor=str(payload["human_actor"]),
            resolved_at=datetime_from_json(str(payload["resolved_at"])),
            selected_option_id=payload.get("selected_option_id"),
            direction=payload.get("direction"),
            rationale=payload.get("rationale"),
        )


@dataclass(frozen=True)
class DecisionRecord:
    """A request plus everything that has happened to it since."""

    request: DecisionRequest
    state: DecisionState
    resolution: DecisionResolution | None = None
    superseded_by: str | None = None
    resumed_by_claim_id: str | None = None
    cancelled_by: str | None = None
    cancel_reason: str | None = None

    @property
    def decision_id(self) -> str:
        return self.request.decision_id

    @property
    def is_blocking(self) -> bool:
        """Whether this decision still owes the agent an answer."""
        return self.state is DecisionState.WAITING_HUMAN

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.request.decision_id,
            "state": self.state.value,
            "request": self.request.to_dict(),
            "resolution": self.resolution.to_dict() if self.resolution else None,
            "superseded_by": self.superseded_by,
            "resumed_by_claim_id": self.resumed_by_claim_id,
            "cancelled_by": self.cancelled_by,
            "cancel_reason": self.cancel_reason,
        }
