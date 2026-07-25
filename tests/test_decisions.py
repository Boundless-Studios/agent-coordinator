"""BOU-2039: durable human-decision requests, resolutions, and resumable claims.

The coordinator already owns *who is working on a task*. This suite specifies the
second half of the contract: *what the agent is blocked on, and who unblocked it*.

The properties that matter, and why:

- A decision is derived from the append-only ledger, exactly like a claim, so an
  agent that dies mid-question leaves a complete, replayable record.
- An unresolved decision is genuinely blocking: the task cannot be released as
  completed while a human still owes an answer.
- Nothing about the passage of time ever answers a decision. Downstream
  (agentic-pr-dash BOU-2040) relies on this to avoid a retry/death loop, so it is
  asserted directly rather than assumed.
- A resolution makes the task *resumable*, not *resumed*: it never silently
  transfers ownership or executes anything.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json

import pytest

from agent_coordinator.models import (
    DecisionOption,
    DecisionState,
    OwnerIdentity,
    TaskIdentity,
)
from agent_coordinator.service import (
    DecisionPendingError,
    DecisionNotResumableError,
    StaleDecisionError,
    TaskCoordinator,
)
from agent_coordinator.store import JsonlClaimStore


BASE_TIME = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)


def store_path(tmp_path):
    return tmp_path / "claims.jsonl"


def coordinator(tmp_path, *, live_pids: set[int] | None = None) -> TaskCoordinator:
    live = live_pids if live_pids is not None else {101, 202}
    return TaskCoordinator(
        JsonlClaimStore(store_path(tmp_path)),
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


def options() -> list[DecisionOption]:
    return [
        DecisionOption(
            option_id="split-service",
            summary="Move turn orchestration into its own service",
            trade_offs="Clean boundary, but a new deploy unit and cross-process latency",
        ),
        DecisionOption(
            option_id="keep-module",
            summary="Keep orchestration in the existing module",
            trade_offs="No new infrastructure, but the boundary stays implicit",
        ),
    ]


def request_kwargs(**overrides):
    payload = dict(
        logical_key="turn-orchestration-boundary",
        category="architecture",
        question="Should turn orchestration own its own service boundary?",
        options=options(),
        recommendation="keep-module",
        rationale="Reversible today; the cross-process hop is not yet justified",
        affected_scope=["backend/src/gaia/orchestrator", "backend/src/gaia/turns"],
        fingerprint="evidence:v1",
        requesting_runtime="codex",
        requesting_session_id="s1",
    )
    payload.update(overrides)
    return payload


def claim_and_request(coord, *, now=BASE_TIME, session_id="s1", pid=101, **overrides):
    """Claim the task, then ask the human a question against that claim."""
    claim = coord.claim_task(
        task(), owner(session_id, pid), lease_seconds=900, now=now
    )
    record = coord.request_decision(
        task(),
        claim_id=claim.claim_id,
        now=now,
        **request_kwargs(requesting_session_id=session_id, **overrides),
    )
    return claim, record


def ledger_events(tmp_path) -> list[dict]:
    return [
        json.loads(line)
        for line in store_path(tmp_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def count_events(tmp_path, event_type: str) -> int:
    return sum(1 for event in ledger_events(tmp_path) if event.get("event") == event_type)


# --------------------------------------------------------------------------
# Requesting a decision
# --------------------------------------------------------------------------


def test_request_decision_puts_the_task_in_waiting_human(tmp_path):
    coord = coordinator(tmp_path)
    claim, record = claim_and_request(coord)

    assert record.state is DecisionState.WAITING_HUMAN
    assert record.request.claim_id == claim.claim_id
    assert record.request.task == task()
    assert record.request.category == "architecture"
    assert record.request.fingerprint == "evidence:v1"
    assert [option.option_id for option in record.request.options] == [
        "split-service",
        "keep-module",
    ]
    assert record.resolution is None
    assert count_events(tmp_path, "decision_requested") == 1


def test_decision_status_finds_the_open_decision_by_task(tmp_path):
    coord = coordinator(tmp_path)
    _, record = claim_and_request(coord)

    found = coord.decision_status(task())
    assert found is not None
    assert found.request.decision_id == record.request.decision_id
    assert found.state is DecisionState.WAITING_HUMAN


def test_decision_state_survives_a_fresh_coordinator_over_the_same_ledger(tmp_path):
    """Durability: the record is on disk, not in the requesting process."""
    _, record = claim_and_request(coordinator(tmp_path))

    reopened = coordinator(tmp_path)
    found = reopened.decision_status(task())
    assert found is not None
    assert found.request.decision_id == record.request.decision_id
    assert found.request.question == record.request.question
    assert found.state is DecisionState.WAITING_HUMAN


@pytest.mark.parametrize("category", ["refactor", "", "Architecture", "misc"])
def test_unknown_decision_category_is_rejected(tmp_path, category):
    coord = coordinator(tmp_path)
    claim = coord.claim_task(task(), owner(), lease_seconds=900, now=BASE_TIME)

    with pytest.raises(ValueError):
        coord.request_decision(
            task(),
            claim_id=claim.claim_id,
            now=BASE_TIME,
            **request_kwargs(category=category),
        )


@pytest.mark.parametrize("count", [0, 1, 4])
def test_option_count_must_be_two_or_three(tmp_path, count):
    coord = coordinator(tmp_path)
    claim = coord.claim_task(task(), owner(), lease_seconds=900, now=BASE_TIME)
    too_few_or_many = [
        DecisionOption(option_id=f"o{index}", summary="s", trade_offs="t")
        for index in range(count)
    ]

    with pytest.raises(ValueError):
        coord.request_decision(
            task(),
            claim_id=claim.claim_id,
            now=BASE_TIME,
            **request_kwargs(options=too_few_or_many),
        )


def test_three_options_are_allowed(tmp_path):
    coord = coordinator(tmp_path)
    claim = coord.claim_task(task(), owner(), lease_seconds=900, now=BASE_TIME)
    three = options() + [
        DecisionOption(option_id="facade", summary="Add a facade", trade_offs="Indirect")
    ]

    record = coord.request_decision(
        task(),
        claim_id=claim.claim_id,
        now=BASE_TIME,
        **request_kwargs(options=three),
    )
    assert len(record.request.options) == 3


# --------------------------------------------------------------------------
# Idempotency and supersession
# --------------------------------------------------------------------------


def test_duplicate_request_is_idempotent(tmp_path):
    """Same logical key, same evidence -> the same question, asked once."""
    coord = coordinator(tmp_path)
    claim, first = claim_and_request(coord)

    second = coord.request_decision(
        task(),
        claim_id=claim.claim_id,
        now=BASE_TIME + timedelta(minutes=5),
        **request_kwargs(),
    )

    assert second.request.decision_id == first.request.decision_id
    assert second.state is DecisionState.WAITING_HUMAN
    assert count_events(tmp_path, "decision_requested") == 1


def test_changed_fingerprint_supersedes_the_prior_request(tmp_path):
    """The task moved on: the old question no longer describes reality."""
    coord = coordinator(tmp_path)
    claim, first = claim_and_request(coord)

    second = coord.request_decision(
        task(),
        claim_id=claim.claim_id,
        now=BASE_TIME + timedelta(minutes=5),
        **request_kwargs(fingerprint="evidence:v2"),
    )

    assert second.request.decision_id != first.request.decision_id
    assert second.state is DecisionState.WAITING_HUMAN

    superseded = coord.decision_by_id(first.request.decision_id)
    assert superseded is not None
    assert superseded.state is DecisionState.SUPERSEDED
    assert superseded.superseded_by == second.request.decision_id
    assert count_events(tmp_path, "decision_requested") == 2


def test_supersession_is_atomic_with_the_replacement_request(tmp_path):
    """Retiring the old question and opening the new one is ONE append.

    Two appends would leave a window where the old decision is superseded and
    the replacement does not exist yet. In that window the task has no blocking
    decision and could be released as completed with the question unanswered.
    So supersession rides on the replacement's own event, mirroring how
    claim_task carries superseded_claim_ids.
    """
    coord = coordinator(tmp_path)
    claim, first = claim_and_request(coord)
    second = coord.request_decision(
        task(),
        claim_id=claim.claim_id,
        now=BASE_TIME + timedelta(minutes=5),
        **request_kwargs(fingerprint="evidence:v2"),
    )

    requested = [
        event
        for event in ledger_events(tmp_path)
        if event.get("event") == "decision_requested"
    ]
    assert requested[0].get("supersedes_decision_id") is None
    assert requested[1]["supersedes_decision_id"] == first.request.decision_id
    assert requested[1]["request"]["decision_id"] == second.request.decision_id

    # Replaying the ledger up to and including that single event never yields a
    # task with zero blocking decisions.
    replayed = TaskCoordinator(JsonlClaimStore(store_path(tmp_path)))
    assert len(replayed.pending_decisions(task())) == 1


def test_a_different_logical_key_is_a_separate_decision(tmp_path):
    coord = coordinator(tmp_path)
    claim, first = claim_and_request(coord)

    second = coord.request_decision(
        task(),
        claim_id=claim.claim_id,
        now=BASE_TIME + timedelta(minutes=5),
        **request_kwargs(logical_key="persistence-model", fingerprint="evidence:p1"),
    )

    assert second.request.decision_id != first.request.decision_id
    assert coord.decision_by_id(first.request.decision_id).state is (
        DecisionState.WAITING_HUMAN
    )
    open_decisions = coord.list_decisions(
        task=task(), state=DecisionState.WAITING_HUMAN
    )
    assert len(open_decisions) == 2


def test_concurrent_duplicate_requests_collapse_to_one_event(tmp_path):
    """Two writers, one ledger, one question.

    Both coordinators open the same store path, so the idempotency check has to
    happen inside the locked transaction to hold. Deciding it before the lock
    would let both writers append.
    """
    setup = coordinator(tmp_path)
    claim = setup.claim_task(task(), owner(), lease_seconds=900, now=BASE_TIME)

    def submit(index: int):
        return coordinator(tmp_path).request_decision(
            task(),
            claim_id=claim.claim_id,
            now=BASE_TIME + timedelta(seconds=index),
            **request_kwargs(),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        records = list(pool.map(submit, range(2)))

    assert records[0].request.decision_id == records[1].request.decision_id
    assert count_events(tmp_path, "decision_requested") == 1


# --------------------------------------------------------------------------
# Resolving
# --------------------------------------------------------------------------


def test_resolution_makes_the_decision_resumable(tmp_path):
    coord = coordinator(tmp_path)
    _, record = claim_and_request(coord)

    resolved = coord.resolve_decision(
        record.request.decision_id,
        request_fingerprint="evidence:v1",
        human_actor="ilya",
        selected_option_id="split-service",
        rationale="The boundary is about to be load-bearing",
        now=BASE_TIME + timedelta(hours=3),
    )

    assert resolved.state is DecisionState.RESUMABLE
    assert resolved.resolution is not None
    assert resolved.resolution.selected_option_id == "split-service"
    assert resolved.resolution.human_actor == "ilya"
    assert resolved.resolution.rationale == "The boundary is about to be load-bearing"


def test_resolution_accepts_free_form_direction_instead_of_an_option(tmp_path):
    coord = coordinator(tmp_path)
    _, record = claim_and_request(coord)

    resolved = coord.resolve_decision(
        record.request.decision_id,
        request_fingerprint="evidence:v1",
        human_actor="ilya",
        direction="Neither. Delete the orchestrator and inline it.",
        now=BASE_TIME + timedelta(hours=1),
    )

    assert resolved.state is DecisionState.RESUMABLE
    assert resolved.resolution.selected_option_id is None
    assert resolved.resolution.direction.startswith("Neither.")


def test_resolution_requires_an_option_or_a_direction(tmp_path):
    coord = coordinator(tmp_path)
    _, record = claim_and_request(coord)

    with pytest.raises(ValueError):
        coord.resolve_decision(
            record.request.decision_id,
            request_fingerprint="evidence:v1",
            human_actor="ilya",
            now=BASE_TIME + timedelta(hours=1),
        )


def test_resolution_rejects_an_unknown_option_id(tmp_path):
    coord = coordinator(tmp_path)
    _, record = claim_and_request(coord)

    with pytest.raises(ValueError):
        coord.resolve_decision(
            record.request.decision_id,
            request_fingerprint="evidence:v1",
            human_actor="ilya",
            selected_option_id="not-an-option",
            now=BASE_TIME + timedelta(hours=1),
        )


def test_stale_resolution_against_a_superseded_request_is_rejected(tmp_path):
    """A human answering the question we no longer have must not win."""
    coord = coordinator(tmp_path)
    claim, first = claim_and_request(coord)
    coord.request_decision(
        task(),
        claim_id=claim.claim_id,
        now=BASE_TIME + timedelta(minutes=5),
        **request_kwargs(fingerprint="evidence:v2"),
    )

    with pytest.raises(StaleDecisionError) as excinfo:
        coord.resolve_decision(
            first.request.decision_id,
            request_fingerprint="evidence:v1",
            human_actor="ilya",
            selected_option_id="split-service",
            now=BASE_TIME + timedelta(hours=1),
        )

    assert excinfo.value.received_fingerprint == "evidence:v1"
    assert coord.decision_by_id(first.request.decision_id).state is (
        DecisionState.SUPERSEDED
    )
    assert count_events(tmp_path, "decision_resolved") == 0


def test_resolution_with_a_mismatched_fingerprint_is_rejected(tmp_path):
    coord = coordinator(tmp_path)
    _, record = claim_and_request(coord)

    with pytest.raises(StaleDecisionError):
        coord.resolve_decision(
            record.request.decision_id,
            request_fingerprint="evidence:whatever",
            human_actor="ilya",
            selected_option_id="split-service",
            now=BASE_TIME + timedelta(hours=1),
        )

    assert coord.decision_by_id(record.request.decision_id).state is (
        DecisionState.WAITING_HUMAN
    )


# --------------------------------------------------------------------------
# Time never answers a question
# --------------------------------------------------------------------------


def test_no_elapsed_time_moves_waiting_human_to_resumable(tmp_path):
    """The single property BOU-2040 depends on. Assert it, do not assume it."""
    coord = coordinator(tmp_path)
    _, record = claim_and_request(coord)

    far_future = BASE_TIME + timedelta(days=3650)
    assert coord.decision_status(task(), now=far_future).state is (
        DecisionState.WAITING_HUMAN
    )
    assert coord.decision_by_id(record.request.decision_id).state is (
        DecisionState.WAITING_HUMAN
    )
    assert count_events(tmp_path, "decision_resolved") == 0


def test_lease_expiry_does_not_resolve_a_decision(tmp_path):
    coord = coordinator(tmp_path)
    claim = coord.claim_task(task(), owner(), lease_seconds=30, now=BASE_TIME)
    coord.request_decision(
        task(), claim_id=claim.claim_id, now=BASE_TIME, **request_kwargs()
    )

    after_expiry = BASE_TIME + timedelta(seconds=31)
    assert coord.status(task(), now=after_expiry).reclaimable is True
    assert coord.decision_status(task(), now=after_expiry).state is (
        DecisionState.WAITING_HUMAN
    )


# --------------------------------------------------------------------------
# Completion gate
# --------------------------------------------------------------------------


def test_pending_decision_blocks_terminal_completion(tmp_path):
    coord = coordinator(tmp_path)
    claim, _ = claim_and_request(coord)

    with pytest.raises(DecisionPendingError):
        coord.release_claim(
            claim.claim_id,
            owner_session_id="s1",
            lease_epoch=claim.lease_epoch,
            reason="completed",
            now=BASE_TIME + timedelta(minutes=1),
        )

    assert count_events(tmp_path, "released") == 0
    assert coord.status(task(), now=BASE_TIME + timedelta(minutes=1)).claim.status == (
        "active"
    )


def test_pending_decision_still_allows_a_non_terminal_release(tmp_path):
    """A headless executor must be able to yield without losing the question."""
    coord = coordinator(tmp_path)
    claim, record = claim_and_request(coord)

    released = coord.release_claim(
        claim.claim_id,
        owner_session_id="s1",
        lease_epoch=claim.lease_epoch,
        reason="yielded",
        now=BASE_TIME + timedelta(minutes=1),
    )

    assert released.status == "yielded"
    assert coord.decision_by_id(record.request.decision_id).state is (
        DecisionState.WAITING_HUMAN
    )


def test_resolved_decision_unblocks_terminal_completion(tmp_path):
    coord = coordinator(tmp_path)
    claim, record = claim_and_request(coord)
    coord.resolve_decision(
        record.request.decision_id,
        request_fingerprint="evidence:v1",
        human_actor="ilya",
        selected_option_id="keep-module",
        now=BASE_TIME + timedelta(hours=1),
    )

    released = coord.release_claim(
        claim.claim_id,
        owner_session_id="s1",
        lease_epoch=claim.lease_epoch,
        reason="completed",
        now=BASE_TIME + timedelta(hours=2),
    )
    assert released.status == "completed"


def test_cancelled_decision_unblocks_terminal_completion(tmp_path):
    coord = coordinator(tmp_path)
    claim, record = claim_and_request(coord)

    cancelled = coord.cancel_decision(
        record.request.decision_id,
        actor="ilya",
        reason="question no longer applies",
        now=BASE_TIME + timedelta(minutes=30),
    )
    assert cancelled.state is DecisionState.CANCELLED

    released = coord.release_claim(
        claim.claim_id,
        owner_session_id="s1",
        lease_epoch=claim.lease_epoch,
        reason="completed",
        now=BASE_TIME + timedelta(hours=1),
    )
    assert released.status == "completed"
    assert coord.decision_status(task()) is None


def test_a_decision_on_a_different_task_does_not_block_completion(tmp_path):
    coord = coordinator(tmp_path)
    other = TaskIdentity(
        task_type="pr-maintenance",
        task_id="github:Boundless-Studios/agentic-pr-dash#9",
        fingerprint="comments:z",
    )
    other_claim = coord.claim_task(
        other, owner("s2", 202), lease_seconds=900, now=BASE_TIME
    )
    coord.request_decision(
        other,
        claim_id=other_claim.claim_id,
        now=BASE_TIME,
        **request_kwargs(requesting_session_id="s2"),
    )

    claim = coord.claim_task(task(), owner("s1", 101), lease_seconds=900, now=BASE_TIME)
    released = coord.release_claim(
        claim.claim_id,
        owner_session_id="s1",
        lease_epoch=claim.lease_epoch,
        reason="completed",
        now=BASE_TIME + timedelta(minutes=1),
    )
    assert released.status == "completed"


# --------------------------------------------------------------------------
# Owner death, replacement owner, resume
# --------------------------------------------------------------------------


def test_unresolved_decision_survives_owner_death(tmp_path):
    coord = coordinator(tmp_path, live_pids={101})
    claim, record = claim_and_request(coord)

    dead = coordinator(tmp_path, live_pids=set())
    assert dead.status(task(), now=BASE_TIME + timedelta(minutes=1)).reclaimable is True

    found = dead.decision_status(task(), now=BASE_TIME + timedelta(minutes=1))
    assert found is not None
    assert found.request.decision_id == record.request.decision_id
    assert found.state is DecisionState.WAITING_HUMAN
    assert found.request.claim_id == claim.claim_id


def test_resolution_does_not_transfer_ownership_or_execute(tmp_path):
    """Resolution unblocks; it does not hand the work to anyone."""
    coord = coordinator(tmp_path, live_pids=set())
    claim, record = claim_and_request(coord)

    coord.resolve_decision(
        record.request.decision_id,
        request_fingerprint="evidence:v1",
        human_actor="ilya",
        selected_option_id="split-service",
        now=BASE_TIME + timedelta(hours=1),
    )

    decision = coord.status(task(), now=BASE_TIME + timedelta(hours=1))
    assert decision.claim.claim_id == claim.claim_id
    assert count_events(tmp_path, "claimed") == 1
    assert count_events(tmp_path, "task_resumed") == 0
    assert coord.decision_by_id(record.request.decision_id).state is (
        DecisionState.RESUMABLE
    )


def test_replacement_owner_resumes_after_resolution(tmp_path):
    coord = coordinator(tmp_path, live_pids=set())
    first_claim, record = claim_and_request(coord)
    coord.resolve_decision(
        record.request.decision_id,
        request_fingerprint="evidence:v1",
        human_actor="ilya",
        selected_option_id="split-service",
        now=BASE_TIME + timedelta(hours=1),
    )

    resumed_at = BASE_TIME + timedelta(hours=2)
    replacement = coord.claim_task(
        task(), owner("s2", 202), lease_seconds=900, now=resumed_at
    )
    assert replacement.lease_epoch > first_claim.lease_epoch
    assert replacement.owner.session_id == "s2"

    resumed = coord.resume_task(
        record.request.decision_id,
        claim_id=replacement.claim_id,
        owner_session_id="s2",
        lease_epoch=replacement.lease_epoch,
        now=resumed_at,
    )

    assert resumed.state is DecisionState.RESUMED
    assert resumed.resumed_by_claim_id == replacement.claim_id
    # The resumed work can cite the direction it was given.
    assert resumed.resolution.selected_option_id == "split-service"
    assert resumed.resolution.human_actor == "ilya"
    assert count_events(tmp_path, "task_resumed") == 1


def test_resume_is_idempotent_for_the_same_claim(tmp_path):
    coord = coordinator(tmp_path)
    claim, record = claim_and_request(coord)
    coord.resolve_decision(
        record.request.decision_id,
        request_fingerprint="evidence:v1",
        human_actor="ilya",
        selected_option_id="keep-module",
        now=BASE_TIME + timedelta(hours=1),
    )

    kwargs = dict(
        claim_id=claim.claim_id,
        owner_session_id="s1",
        lease_epoch=claim.lease_epoch,
        now=BASE_TIME + timedelta(hours=2),
    )
    first = coord.resume_task(record.request.decision_id, **kwargs)
    second = coord.resume_task(record.request.decision_id, **kwargs)

    assert first.state is second.state is DecisionState.RESUMED
    assert count_events(tmp_path, "task_resumed") == 1


def test_resume_before_resolution_is_rejected(tmp_path):
    coord = coordinator(tmp_path)
    claim, record = claim_and_request(coord)

    with pytest.raises(DecisionNotResumableError):
        coord.resume_task(
            record.request.decision_id,
            claim_id=claim.claim_id,
            owner_session_id="s1",
            lease_epoch=claim.lease_epoch,
            now=BASE_TIME + timedelta(hours=1),
        )

    assert count_events(tmp_path, "task_resumed") == 0
    assert coord.decision_by_id(record.request.decision_id).state is (
        DecisionState.WAITING_HUMAN
    )


def test_a_deposed_owner_cannot_record_a_resume(tmp_path):
    """The existing lease-epoch fence has to cover resume too."""
    from agent_coordinator.service import StaleClaimError

    coord = coordinator(tmp_path, live_pids=set())
    first_claim, record = claim_and_request(coord)
    coord.resolve_decision(
        record.request.decision_id,
        request_fingerprint="evidence:v1",
        human_actor="ilya",
        selected_option_id="split-service",
        now=BASE_TIME + timedelta(hours=1),
    )
    coord.claim_task(
        task(), owner("s2", 202), lease_seconds=900, now=BASE_TIME + timedelta(hours=2)
    )

    with pytest.raises(StaleClaimError):
        coord.resume_task(
            record.request.decision_id,
            claim_id=first_claim.claim_id,
            owner_session_id="s1",
            lease_epoch=first_claim.lease_epoch,
            now=BASE_TIME + timedelta(hours=2),
        )

    assert count_events(tmp_path, "task_resumed") == 0


def test_a_replacement_claim_does_not_cancel_a_waiting_decision(tmp_path):
    """claim_task supersedes predecessor *claims*. Decisions outlive claims."""
    coord = coordinator(tmp_path, live_pids=set())
    _, record = claim_and_request(coord)

    coord.claim_task(
        task(), owner("s2", 202), lease_seconds=900, now=BASE_TIME + timedelta(hours=1)
    )

    assert coord.decision_by_id(record.request.decision_id).state is (
        DecisionState.WAITING_HUMAN
    )


# --------------------------------------------------------------------------
# Listing and ledger hygiene
# --------------------------------------------------------------------------


def test_list_decisions_filters_by_task_and_state(tmp_path):
    coord = coordinator(tmp_path)
    claim, first = claim_and_request(coord)
    second = coord.request_decision(
        task(),
        claim_id=claim.claim_id,
        now=BASE_TIME + timedelta(minutes=1),
        **request_kwargs(logical_key="schema-compat", fingerprint="evidence:s1"),
    )
    coord.resolve_decision(
        second.request.decision_id,
        request_fingerprint="evidence:s1",
        human_actor="ilya",
        selected_option_id="keep-module",
        now=BASE_TIME + timedelta(hours=1),
    )

    waiting = coord.list_decisions(task=task(), state=DecisionState.WAITING_HUMAN)
    resumable = coord.list_decisions(task=task(), state=DecisionState.RESUMABLE)

    assert [item.request.decision_id for item in waiting] == [first.request.decision_id]
    assert [item.request.decision_id for item in resumable] == [
        second.request.decision_id
    ]
    assert len(coord.list_decisions()) == 2


def test_decision_events_do_not_corrupt_claim_state(tmp_path):
    coord = coordinator(tmp_path)
    claim, record = claim_and_request(coord)
    coord.resolve_decision(
        record.request.decision_id,
        request_fingerprint="evidence:v1",
        human_actor="ilya",
        selected_option_id="keep-module",
        now=BASE_TIME + timedelta(hours=1),
    )

    heartbeat_at = BASE_TIME + timedelta(hours=1, minutes=1)
    beat = coord.heartbeat_claim(
        claim.claim_id,
        owner_session_id="s1",
        lease_epoch=claim.lease_epoch,
        lease_seconds=900,
        now=heartbeat_at,
    )
    assert beat.lease_epoch == claim.lease_epoch
    assert coord.status(task(), now=heartbeat_at).state.value == "active"


def test_claim_events_do_not_corrupt_decision_state(tmp_path):
    coord = coordinator(tmp_path)
    claim, record = claim_and_request(coord)

    coord.heartbeat_claim(
        claim.claim_id,
        owner_session_id="s1",
        lease_epoch=claim.lease_epoch,
        lease_seconds=900,
        now=BASE_TIME + timedelta(minutes=5),
    )

    found = coord.decision_status(task())
    assert found.request.decision_id == record.request.decision_id
    assert found.state is DecisionState.WAITING_HUMAN


def test_decision_events_survive_ledger_compaction(tmp_path):
    """Compaction rewrites the ledger. A pending question must not vanish."""
    coord = TaskCoordinator(
        JsonlClaimStore(store_path(tmp_path)),
        pid_is_live=lambda pid: True,
        compaction_event_threshold=5,
    )
    claim = coord.claim_task(task(), owner(), lease_seconds=900, now=BASE_TIME)
    record = coord.request_decision(
        task(), claim_id=claim.claim_id, now=BASE_TIME, **request_kwargs()
    )

    for index in range(12):
        coord.heartbeat_claim(
            claim.claim_id,
            owner_session_id="s1",
            lease_epoch=claim.lease_epoch,
            lease_seconds=900,
            now=BASE_TIME + timedelta(seconds=10 * (index + 1)),
        )

    assert any(event.get("event") == "compaction" for event in ledger_events(tmp_path))
    reopened = TaskCoordinator(JsonlClaimStore(store_path(tmp_path)))
    found = reopened.decision_by_id(record.request.decision_id)
    assert found is not None
    assert found.state is DecisionState.WAITING_HUMAN
    assert found.request.question == record.request.question


def test_decision_records_round_trip_through_the_ledger_verbatim(tmp_path):
    coord = coordinator(tmp_path)
    _, record = claim_and_request(coord)

    raw = [
        event
        for event in ledger_events(tmp_path)
        if event.get("event") == "decision_requested"
    ][0]
    assert raw["request"]["decision_id"] == record.request.decision_id
    assert raw["request"]["logical_key"] == "turn-orchestration-boundary"
    assert raw["request"]["requesting_runtime"] == "codex"
    assert raw["request"]["affected_scope"] == [
        "backend/src/gaia/orchestrator",
        "backend/src/gaia/turns",
    ]
    assert raw["request"]["created_at"].endswith("Z")
