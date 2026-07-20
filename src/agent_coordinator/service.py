"""Task ownership claim, heartbeat, release, and reclaim semantics."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
import os
from typing import Any
import uuid

from .models import ClaimRecord, OwnerIdentity, TaskIdentity, datetime_from_json, datetime_to_json
from .store import JsonlClaimStore


SUPERSEDED_STATUS = "superseded"


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


class StaleClaimError(RuntimeError):
    """Raised when a caller's lease epoch is not the task's current epoch.

    ``current_claim_id`` names the claim that currently owns the task, so a
    deposed owner can learn *who* deposed it instead of only that it lost.
    """

    def __init__(
        self,
        *,
        expected_epoch: int,
        received_epoch: int,
        current_claim_id: str | None = None,
    ):
        self.expected_epoch = expected_epoch
        self.received_epoch = received_epoch
        self.current_claim_id = current_claim_id
        super().__init__(
            f"stale lease epoch: expected {expected_epoch}, received {received_epoch}"
        )


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
        result: ClaimRecord | None = None

        def build_event(events: list[dict[str, Any]]) -> dict[str, Any]:
            nonlocal result
            claims = self._claims_by_id(events)
            current = self._decision_for_task(task, claims, timestamp)
            if current.state is ClaimState.ACTIVE and current.claim is not None:
                if current.claim.owner.session_id != owner.session_id:
                    raise ClaimConflictError(current)
                result = ClaimRecord(
                    claim_id=current.claim.claim_id,
                    task=current.claim.task,
                    owner=current.claim.owner,
                    claimed_at=current.claim.claimed_at,
                    heartbeat_at=timestamp,
                    lease_expires_at=timestamp + timedelta(seconds=lease_seconds),
                    lease_epoch=current.claim.lease_epoch,
                    status="active",
                )
                return {
                    "event": "heartbeat",
                    "timestamp": datetime_to_json(timestamp),
                    "claim_id": result.claim_id,
                    "owner_session_id": owner.session_id,
                    "lease_epoch": result.lease_epoch,
                    "heartbeat_at": datetime_to_json(result.heartbeat_at),
                    "lease_expires_at": datetime_to_json(result.lease_expires_at),
                }

            result = ClaimRecord(
                claim_id=uuid.uuid4().hex,
                task=task,
                owner=owner,
                claimed_at=timestamp,
                heartbeat_at=timestamp,
                lease_expires_at=timestamp + timedelta(seconds=lease_seconds),
                lease_epoch=max(
                    (claim.lease_epoch for claim in claims.values()), default=0
                )
                + 1,
            )
            event: dict[str, Any] = {
                "event": "claimed",
                "timestamp": datetime_to_json(timestamp),
                "claim": result.to_dict(),
            }
            # Retire every still-active predecessor for this task in the same
            # transaction that mints the successor, so a stale claim id cannot
            # be resurrected by a resumed owner.
            superseded = sorted(
                existing.claim_id
                for existing in claims.values()
                if existing.task == task and existing.status == "active"
            )
            if superseded:
                event["superseded_claim_ids"] = superseded
            return event

        self.store.transact_event(build_event)
        if result is None:
            raise RuntimeError("claim transaction did not produce a claim")
        return result

    def heartbeat_claim(
        self,
        claim_id: str,
        *,
        owner_session_id: str,
        lease_epoch: int,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> ClaimRecord:
        timestamp = now or self._now()
        updated: ClaimRecord | None = None

        def build_event(events: list[dict[str, Any]]) -> dict[str, Any]:
            nonlocal updated
            claims = self._claims_by_id(events)
            claim = claims.get(claim_id)
            if claim is None:
                raise KeyError(f"unknown claim_id: {claim_id}")
            if claim.owner.session_id != owner_session_id:
                raise PermissionError("owner_session_id does not own claim")
            self._assert_epoch_is_current(claim, claims, lease_epoch)
            if claim.status != "active":
                raise ValueError("cannot heartbeat a released claim")

            updated = ClaimRecord(
                claim_id=claim.claim_id,
                task=claim.task,
                owner=claim.owner,
                claimed_at=claim.claimed_at,
                heartbeat_at=timestamp,
                lease_expires_at=timestamp + timedelta(seconds=lease_seconds),
                lease_epoch=claim.lease_epoch,
                status="active",
            )
            return {
                "event": "heartbeat",
                "timestamp": datetime_to_json(timestamp),
                "claim_id": claim_id,
                "owner_session_id": owner_session_id,
                "lease_epoch": updated.lease_epoch,
                "heartbeat_at": datetime_to_json(updated.heartbeat_at),
                "lease_expires_at": datetime_to_json(updated.lease_expires_at),
            }

        self.store.transact_event(build_event)
        if updated is None:
            raise RuntimeError("heartbeat transaction did not produce a claim")
        return updated

    def release_claim(
        self,
        claim_id: str,
        *,
        owner_session_id: str,
        lease_epoch: int,
        reason: str = "released",
        now: datetime | None = None,
    ) -> ClaimRecord:
        timestamp = now or self._now()
        released: ClaimRecord | None = None

        def build_event(events: list[dict[str, Any]]) -> dict[str, Any]:
            nonlocal released
            claims = self._claims_by_id(events)
            claim = claims.get(claim_id)
            if claim is None:
                raise KeyError(f"unknown claim_id: {claim_id}")
            if claim.owner.session_id != owner_session_id:
                raise PermissionError("owner_session_id does not own claim")
            self._assert_epoch_is_current(claim, claims, lease_epoch)

            release_reason = reason or "released"
            released = ClaimRecord(
                claim_id=claim.claim_id,
                task=claim.task,
                owner=claim.owner,
                claimed_at=claim.claimed_at,
                heartbeat_at=claim.heartbeat_at,
                lease_expires_at=claim.lease_expires_at,
                lease_epoch=claim.lease_epoch,
                status=release_reason,
                release_reason=release_reason,
            )
            return {
                "event": "released",
                "timestamp": datetime_to_json(timestamp),
                "claim_id": claim_id,
                "owner_session_id": owner_session_id,
                "lease_epoch": released.lease_epoch,
                "status": released.status,
                "release_reason": released.release_reason,
            }

        self.store.transact_event(build_event)
        if released is None:
            raise RuntimeError("release transaction did not produce a claim")
        return released

    def status(self, task: TaskIdentity, *, now: datetime | None = None) -> ClaimDecision:
        timestamp = now or self._now()
        return self._decision_for_task(task, self._claims_by_id(), timestamp)

    def claim_by_id(self, claim_id: str) -> ClaimRecord | None:
        """Return the materialized record for ``claim_id``, if the ledger has one.

        A deposed owner can use this to learn that its own claim was superseded.
        """
        return self._claims_by_id().get(claim_id)

    def _assert_epoch_is_current(
        self,
        claim: ClaimRecord,
        claims: dict[str, ClaimRecord],
        lease_epoch: int,
    ) -> None:
        """Fence ``lease_epoch`` against the task's current epoch.

        Comparing against the claim's *own* epoch is not a fence: a deposed
        owner still knows its own epoch. The only safe comparison is against
        the highest epoch recorded for the task, and it must happen inside the
        store transaction that performs the write.
        """
        current = self._latest_claim_for_task(claim.task, claims)
        if (
            current is not None
            and current.claim_id == claim.claim_id
            and current.lease_epoch == lease_epoch
            and claim.lease_epoch == lease_epoch
        ):
            return
        expected = current.lease_epoch if current is not None else claim.lease_epoch
        raise StaleClaimError(
            expected_epoch=expected,
            received_epoch=lease_epoch,
            current_claim_id=current.claim_id if current is not None else None,
        )

    def _decision_for_task(
        self,
        task: TaskIdentity,
        claims: dict[str, ClaimRecord],
        timestamp: datetime,
    ) -> ClaimDecision:
        claim = self._latest_claim_for_task(task, claims)
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

    def _latest_claim_for_task(
        self,
        task: TaskIdentity,
        claims_by_id: dict[str, ClaimRecord] | None = None,
    ) -> ClaimRecord | None:
        materialized = claims_by_id if claims_by_id is not None else self._claims_by_id()
        claims = [
            claim
            for claim in materialized.values()
            if claim.task == task
        ]
        if not claims:
            return None
        return max(
            claims,
            key=lambda item: (
                item.lease_epoch,
                item.claimed_at,
                item.heartbeat_at,
                item.claim_id,
            ),
        )

    def _claims_by_id(
        self,
        events: list[dict[str, Any]] | None = None,
    ) -> dict[str, ClaimRecord]:
        claims: dict[str, ClaimRecord] = {}
        source_events = self.store.read_events() if events is None else events
        for event in source_events:
            event_type = event.get("event")
            if event_type == "claimed":
                claim = ClaimRecord.from_dict(dict(event["claim"]))
                for superseded_id in event.get("superseded_claim_ids") or []:
                    prior = claims.get(str(superseded_id))
                    if prior is None or prior.status != "active":
                        continue
                    claims[prior.claim_id] = replace(
                        prior,
                        status=SUPERSEDED_STATUS,
                        release_reason=SUPERSEDED_STATUS,
                    )
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
                    lease_epoch=claim.lease_epoch,
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
                    lease_epoch=claim.lease_epoch,
                    status=reason,
                    release_reason=reason,
                )
        return claims

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)
