"""Task ownership claim, heartbeat, release, and reclaim semantics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import os
import uuid

from .models import ClaimRecord, OwnerIdentity, TaskIdentity, datetime_from_json, datetime_to_json
from .store import JsonlClaimStore


class ClaimState(str, Enum):
    ACTIVE = "active"
    NO_CLAIM = "no_claim"
    EXPIRED = "expired"
    RELEASED = "released"
    OWNER_DEAD = "owner_dead"


@dataclass(frozen=True)
class ClaimDecision:
    state: ClaimState
    claim: ClaimRecord | None
    reclaimable: bool
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "reclaimable": self.reclaimable,
            "reason": self.reason,
            "claim": self.claim.to_dict() if self.claim else None,
        }


class ClaimConflictError(RuntimeError):
    def __init__(self, decision: ClaimDecision):
        self.decision = decision
        super().__init__(decision.reason)


def default_pid_is_live(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


class TaskCoordinator:
    def __init__(
        self,
        store: JsonlClaimStore,
        *,
        pid_is_live=default_pid_is_live,
    ):
        self.store = store
        self.pid_is_live = pid_is_live

    def claim_task(
        self,
        task: TaskIdentity,
        owner: OwnerIdentity,
        *,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> ClaimRecord:
        timestamp = now or self._now()
        current = self.status(task, now=timestamp)
        if current.state is ClaimState.ACTIVE and current.claim is not None:
            if current.claim.owner.session_id != owner.session_id:
                raise ClaimConflictError(current)
            return self.heartbeat_claim(
                current.claim.claim_id,
                owner_session_id=owner.session_id,
                lease_seconds=lease_seconds,
                now=timestamp,
            )

        claim = ClaimRecord(
            claim_id=uuid.uuid4().hex,
            task=task,
            owner=owner,
            claimed_at=timestamp,
            heartbeat_at=timestamp,
            lease_expires_at=timestamp + timedelta(seconds=lease_seconds),
        )
        self.store.append_event(
            {
                "event": "claimed",
                "timestamp": datetime_to_json(timestamp),
                "claim": claim.to_dict(),
            }
        )
        return claim

    def heartbeat_claim(
        self,
        claim_id: str,
        *,
        owner_session_id: str,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> ClaimRecord:
        timestamp = now or self._now()
        claim = self._claim_by_id(claim_id)
        if claim is None:
            raise KeyError(f"unknown claim_id: {claim_id}")
        if claim.owner.session_id != owner_session_id:
            raise PermissionError("owner_session_id does not own claim")
        if claim.status != "active":
            raise ValueError("cannot heartbeat a released claim")

        updated = ClaimRecord(
            claim_id=claim.claim_id,
            task=claim.task,
            owner=claim.owner,
            claimed_at=claim.claimed_at,
            heartbeat_at=timestamp,
            lease_expires_at=timestamp + timedelta(seconds=lease_seconds),
            status="active",
        )
        self.store.append_event(
            {
                "event": "heartbeat",
                "timestamp": datetime_to_json(timestamp),
                "claim_id": claim_id,
                "owner_session_id": owner_session_id,
                "heartbeat_at": datetime_to_json(updated.heartbeat_at),
                "lease_expires_at": datetime_to_json(updated.lease_expires_at),
            }
        )
        return updated

    def release_claim(
        self,
        claim_id: str,
        *,
        owner_session_id: str,
        reason: str = "released",
        now: datetime | None = None,
    ) -> ClaimRecord:
        timestamp = now or self._now()
        claim = self._claim_by_id(claim_id)
        if claim is None:
            raise KeyError(f"unknown claim_id: {claim_id}")
        if claim.owner.session_id != owner_session_id:
            raise PermissionError("owner_session_id does not own claim")

        released = ClaimRecord(
            claim_id=claim.claim_id,
            task=claim.task,
            owner=claim.owner,
            claimed_at=claim.claimed_at,
            heartbeat_at=claim.heartbeat_at,
            lease_expires_at=claim.lease_expires_at,
            status=reason or "released",
            release_reason=reason or "released",
        )
        self.store.append_event(
            {
                "event": "released",
                "timestamp": datetime_to_json(timestamp),
                "claim_id": claim_id,
                "owner_session_id": owner_session_id,
                "status": released.status,
                "release_reason": released.release_reason,
            }
        )
        return released

    def status(self, task: TaskIdentity, *, now: datetime | None = None) -> ClaimDecision:
        timestamp = now or self._now()
        claim = self._latest_claim_for_task(task)
        if claim is None:
            return ClaimDecision(ClaimState.NO_CLAIM, None, True, "no claim for task fingerprint")
        if claim.status != "active":
            return ClaimDecision(ClaimState.RELEASED, claim, True, f"claim is {claim.status}")
        if timestamp >= claim.lease_expires_at:
            return ClaimDecision(ClaimState.EXPIRED, claim, True, "claim lease expired")
        if not self.pid_is_live(claim.owner.pid):
            return ClaimDecision(ClaimState.OWNER_DEAD, claim, True, "claim owner process is not live")
        return ClaimDecision(ClaimState.ACTIVE, claim, False, "claim is active")

    def reclaimable(self, task: TaskIdentity, *, now: datetime | None = None) -> bool:
        return self.status(task, now=now).reclaimable

    def _latest_claim_for_task(self, task: TaskIdentity) -> ClaimRecord | None:
        claims = [
            claim
            for claim in self._claims_by_id().values()
            if claim.task == task
        ]
        if not claims:
            return None
        return max(claims, key=lambda item: (item.claimed_at, item.heartbeat_at, item.claim_id))

    def _claim_by_id(self, claim_id: str) -> ClaimRecord | None:
        return self._claims_by_id().get(claim_id)

    def _claims_by_id(self) -> dict[str, ClaimRecord]:
        claims: dict[str, ClaimRecord] = {}
        for event in self.store.read_events():
            event_type = event.get("event")
            if event_type == "claimed":
                claim = ClaimRecord.from_dict(dict(event["claim"]))
                claims[claim.claim_id] = claim
            elif event_type == "heartbeat":
                claim_id = str(event.get("claim_id") or "")
                claim = claims.get(claim_id)
                if claim is None:
                    continue
                claims[claim_id] = ClaimRecord(
                    claim_id=claim.claim_id,
                    task=claim.task,
                    owner=claim.owner,
                    claimed_at=claim.claimed_at,
                    heartbeat_at=datetime_from_json(str(event["heartbeat_at"])),
                    lease_expires_at=datetime_from_json(str(event["lease_expires_at"])),
                    status="active",
                )
            elif event_type == "released":
                claim_id = str(event.get("claim_id") or "")
                claim = claims.get(claim_id)
                if claim is None:
                    continue
                reason = str(event.get("release_reason") or event.get("status") or "released")
                claims[claim_id] = ClaimRecord(
                    claim_id=claim.claim_id,
                    task=claim.task,
                    owner=claim.owner,
                    claimed_at=claim.claimed_at,
                    heartbeat_at=claim.heartbeat_at,
                    lease_expires_at=claim.lease_expires_at,
                    status=reason,
                    release_reason=reason,
                )
        return claims

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)
