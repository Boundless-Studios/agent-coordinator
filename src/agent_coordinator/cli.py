"""Command-line interface for agent-coordinator."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .models import DecisionOption, DecisionState, OwnerIdentity, TaskIdentity
from .service import (
    ClaimConflictError,
    DecisionNotResumableError,
    DecisionPendingError,
    StaleClaimError,
    StaleDecisionError,
    TaskCoordinator,
)
from .store import JsonlClaimStore


# Exit codes. Distinct per failure class so a caller can branch without parsing
# prose; no failure mode exits 0.
EXIT_OK = 0
EXIT_CLAIM_CONFLICT = 3
EXIT_STALE_LEASE_EPOCH = 4
EXIT_STALE_DECISION = 5
EXIT_DECISION_PENDING = 6
EXIT_DECISION_NOT_RESUMABLE = 7
EXIT_BAD_REQUEST = 8
EXIT_NOT_FOUND = 9


def _task_from_args(args: argparse.Namespace) -> TaskIdentity:
    return TaskIdentity(
        task_type=args.task_type,
        task_id=args.task_id,
        fingerprint=args.fingerprint,
    )


def _coordinator(args: argparse.Namespace) -> TaskCoordinator:
    return TaskCoordinator(JsonlClaimStore(args.store))


def _print(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True))


def _cmd_claim(args: argparse.Namespace) -> int:
    owner = OwnerIdentity(
        session_id=args.session_id,
        pid=args.pid,
        agent=args.agent,
        worktree_path=args.worktree_path,
    )
    coord = _coordinator(args)
    try:
        claim = coord.claim_task(
            _task_from_args(args),
            owner,
            lease_seconds=args.lease_seconds,
        )
    except ClaimConflictError as exc:
        _print(exc.decision.to_dict())
        return EXIT_CLAIM_CONFLICT
    _print({"state": "active", "claim": claim.to_dict()})
    return EXIT_OK


def _stale_payload(exc: StaleClaimError) -> dict[str, object]:
    return {
        "error": "stale_lease_epoch",
        "expected_epoch": exc.expected_epoch,
        "received_epoch": exc.received_epoch,
        "current_claim_id": exc.current_claim_id,
    }


def _cmd_heartbeat(args: argparse.Namespace) -> int:
    try:
        claim = _coordinator(args).heartbeat_claim(
            args.claim_id,
            owner_session_id=args.session_id,
            lease_epoch=args.lease_epoch,
            lease_seconds=args.lease_seconds,
        )
    except StaleClaimError as exc:
        _print(_stale_payload(exc))
        return 4
    _print({"state": "active", "claim": claim.to_dict()})
    return 0


def _cmd_release(args: argparse.Namespace) -> int:
    try:
        claim = _coordinator(args).release_claim(
            args.claim_id,
            owner_session_id=args.session_id,
            lease_epoch=args.lease_epoch,
            reason=args.reason,
        )
    except StaleClaimError as exc:
        _print(_stale_payload(exc))
        return EXIT_STALE_LEASE_EPOCH
    except DecisionPendingError as exc:
        _print(
            {
                "error": "decision_pending",
                "decision_ids": exc.decision_ids,
                "reason": args.reason,
            }
        )
        return EXIT_DECISION_PENDING
    _print({"state": "released", "claim": claim.to_dict()})
    return EXIT_OK


def _cmd_status(args: argparse.Namespace) -> int:
    decision = _coordinator(args).status(_task_from_args(args))
    _print(decision.to_dict())
    return 0


def _cmd_reclaimable(args: argparse.Namespace) -> int:
    decision = _coordinator(args).status(_task_from_args(args))
    _print(decision.to_dict())
    return EXIT_OK if decision.reclaimable else 1


def _parse_option(raw: str) -> DecisionOption:
    """Parse ``id=<id>,summary=<text>,tradeoffs=<text>``.

    Split on the *first* ``=`` per field so summaries and trade-offs may contain
    ``=``. Fields are comma-separated, so a value containing a comma must be
    passed as a separate option string — acceptable for a 2-3 option contract.
    """
    fields: dict[str, str] = {}
    for chunk in raw.split(","):
        if not chunk.strip():
            continue
        key, separator, value = chunk.partition("=")
        if not separator:
            raise argparse.ArgumentTypeError(
                f"option field {chunk!r} is not key=value"
            )
        fields[key.strip()] = value.strip()
    missing = {"id", "summary", "tradeoffs"} - fields.keys()
    if missing:
        raise argparse.ArgumentTypeError(
            f"option is missing {sorted(missing)}: {raw!r}"
        )
    return DecisionOption(
        option_id=fields["id"],
        summary=fields["summary"],
        trade_offs=fields["tradeoffs"],
    )


def _cmd_decision_request(args: argparse.Namespace) -> int:
    try:
        record = _coordinator(args).request_decision(
            _task_from_args(args),
            claim_id=args.claim_id,
            logical_key=args.logical_key,
            category=args.category,
            question=args.question,
            options=args.option,
            recommendation=args.recommendation,
            rationale=args.rationale,
            affected_scope=args.affected_scope,
            fingerprint=args.evidence_fingerprint,
            requesting_runtime=args.runtime,
            requesting_session_id=args.session_id,
        )
    except ValueError as exc:
        _print({"error": "invalid_decision_request", "detail": str(exc)})
        return EXIT_BAD_REQUEST
    _print(record.to_dict())
    return EXIT_OK


def _stale_decision_payload(exc: StaleDecisionError) -> dict[str, object]:
    return {
        "error": "stale_decision_fingerprint",
        "decision_id": exc.decision_id,
        "expected_fingerprint": exc.expected_fingerprint,
        "received_fingerprint": exc.received_fingerprint,
        "superseding_decision_id": exc.superseding_decision_id,
    }


def _cmd_decision_resolve(args: argparse.Namespace) -> int:
    try:
        record = _coordinator(args).resolve_decision(
            args.decision_id,
            request_fingerprint=args.request_fingerprint,
            human_actor=args.human_actor,
            selected_option_id=args.option_id,
            direction=args.direction,
            rationale=args.rationale,
        )
    except StaleDecisionError as exc:
        _print(_stale_decision_payload(exc))
        return EXIT_STALE_DECISION
    except KeyError:
        _print({"error": "unknown_decision", "decision_id": args.decision_id})
        return EXIT_NOT_FOUND
    except ValueError as exc:
        _print({"error": "invalid_resolution", "detail": str(exc)})
        return EXIT_BAD_REQUEST
    _print(record.to_dict())
    return EXIT_OK


def _cmd_decision_cancel(args: argparse.Namespace) -> int:
    try:
        record = _coordinator(args).cancel_decision(
            args.decision_id, actor=args.human_actor, reason=args.reason
        )
    except KeyError:
        _print({"error": "unknown_decision", "decision_id": args.decision_id})
        return EXIT_NOT_FOUND
    _print(record.to_dict())
    return EXIT_OK


def _cmd_decision_status(args: argparse.Namespace) -> int:
    record = _coordinator(args).decision_status(_task_from_args(args))
    _print({"decision": record.to_dict() if record else None})
    return EXIT_OK


def _cmd_decision_list(args: argparse.Namespace) -> int:
    task = None
    if args.task_type or args.task_id:
        if not (args.task_type and args.task_id and args.fingerprint):
            _print(
                {
                    "error": "invalid_filter",
                    "detail": "--type, --id, and --fingerprint must be given together",
                }
            )
            return EXIT_BAD_REQUEST
        task = _task_from_args(args)
    state = DecisionState(args.state) if args.state else None
    records = _coordinator(args).list_decisions(task=task, state=state)
    _print({"decisions": [record.to_dict() for record in records]})
    return EXIT_OK


def _cmd_task_resume(args: argparse.Namespace) -> int:
    try:
        record = _coordinator(args).resume_task(
            args.decision_id,
            claim_id=args.claim_id,
            owner_session_id=args.session_id,
            lease_epoch=args.lease_epoch,
        )
    except StaleClaimError as exc:
        _print(_stale_payload(exc))
        return EXIT_STALE_LEASE_EPOCH
    except DecisionNotResumableError as exc:
        _print(
            {
                "error": "decision_not_resumable",
                "decision_id": exc.decision_id,
                "state": exc.state.value,
            }
        )
        return EXIT_DECISION_NOT_RESUMABLE
    except KeyError as exc:
        _print({"error": "unknown_decision_or_claim", "detail": str(exc)})
        return EXIT_NOT_FOUND
    _print(record.to_dict())
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agent-coordinator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_store_arg(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--store",
            default=str(Path.home() / ".agent-coordinator" / "claims.jsonl"),
            help="Path to the JSONL claim event store.",
        )

    def add_task_args(subparser: argparse.ArgumentParser) -> None:
        add_store_arg(subparser)
        subparser.add_argument("--type", dest="task_type", required=True)
        subparser.add_argument("--id", dest="task_id", required=True)
        subparser.add_argument("--fingerprint", required=True)

    claim = subparsers.add_parser("claim")
    add_task_args(claim)
    claim.add_argument("--session-id", required=True)
    claim.add_argument("--pid", type=int)
    claim.add_argument("--agent", default="unknown")
    claim.add_argument("--worktree-path")
    claim.add_argument("--lease-seconds", type=int, default=900)
    claim.set_defaults(func=_cmd_claim)

    heartbeat = subparsers.add_parser("heartbeat")
    add_store_arg(heartbeat)
    heartbeat.add_argument("--claim-id", required=True)
    heartbeat.add_argument("--session-id", required=True)
    heartbeat.add_argument("--lease-epoch", type=int, required=True)
    heartbeat.add_argument("--lease-seconds", type=int, default=900)
    heartbeat.set_defaults(func=_cmd_heartbeat)

    release = subparsers.add_parser("release")
    add_store_arg(release)
    release.add_argument("--claim-id", required=True)
    release.add_argument("--session-id", required=True)
    release.add_argument("--lease-epoch", type=int, required=True)
    release.add_argument("--reason", default="released")
    release.set_defaults(func=_cmd_release)

    status = subparsers.add_parser("status")
    add_task_args(status)
    status.set_defaults(func=_cmd_status)

    reclaimable = subparsers.add_parser("reclaimable")
    add_task_args(reclaimable)
    reclaimable.set_defaults(func=_cmd_reclaimable)

    decision_request = subparsers.add_parser(
        "decision-request",
        help="Record that the agent owes a human an answer before proceeding.",
    )
    add_task_args(decision_request)
    decision_request.add_argument("--claim-id", required=True)
    decision_request.add_argument("--session-id", required=True)
    decision_request.add_argument("--runtime", required=True)
    decision_request.add_argument(
        "--logical-key",
        required=True,
        help="Stable identity of the question; repeating it is idempotent.",
    )
    decision_request.add_argument(
        "--category",
        required=True,
        choices=sorted(("architecture", "product", "authority", "safety")),
    )
    decision_request.add_argument("--question", required=True)
    decision_request.add_argument(
        "--option",
        action="append",
        required=True,
        type=_parse_option,
        metavar="id=<id>,summary=<text>,tradeoffs=<text>",
        help="Repeat 2-3 times.",
    )
    decision_request.add_argument("--recommendation", required=True)
    decision_request.add_argument("--rationale", required=True)
    decision_request.add_argument(
        "--affected-scope", action="append", required=True, dest="affected_scope"
    )
    decision_request.add_argument(
        "--evidence-fingerprint",
        required=True,
        help="Fingerprint of the evidence/task state this question is about. "
        "A changed value supersedes the prior request.",
    )
    decision_request.set_defaults(func=_cmd_decision_request)

    decision_resolve = subparsers.add_parser(
        "decision-resolve", help="Record a human's answer."
    )
    add_store_arg(decision_resolve)
    decision_resolve.add_argument("--decision-id", required=True)
    decision_resolve.add_argument("--request-fingerprint", required=True)
    decision_resolve.add_argument("--human-actor", required=True)
    decision_resolve.add_argument("--option-id")
    decision_resolve.add_argument("--direction")
    decision_resolve.add_argument("--rationale")
    decision_resolve.set_defaults(func=_cmd_decision_resolve)

    decision_cancel = subparsers.add_parser(
        "decision-cancel", help="Withdraw a question (explicit human/admin act)."
    )
    add_store_arg(decision_cancel)
    decision_cancel.add_argument("--decision-id", required=True)
    decision_cancel.add_argument("--human-actor", required=True)
    decision_cancel.add_argument("--reason")
    decision_cancel.set_defaults(func=_cmd_decision_cancel)

    decision_status = subparsers.add_parser(
        "decision-status", help="The decision currently gating a task, if any."
    )
    add_task_args(decision_status)
    decision_status.set_defaults(func=_cmd_decision_status)

    decision_list = subparsers.add_parser("decision-list")
    add_store_arg(decision_list)
    decision_list.add_argument("--type", dest="task_type")
    decision_list.add_argument("--id", dest="task_id")
    decision_list.add_argument("--fingerprint")
    decision_list.add_argument(
        "--state", choices=[state.value for state in DecisionState]
    )
    decision_list.set_defaults(func=_cmd_decision_list)

    task_resume = subparsers.add_parser(
        "task-resume", help="Take up answered work under a live claim."
    )
    add_store_arg(task_resume)
    task_resume.add_argument("--decision-id", required=True)
    task_resume.add_argument("--claim-id", required=True)
    task_resume.add_argument("--session-id", required=True)
    task_resume.add_argument("--lease-epoch", type=int, required=True)
    task_resume.set_defaults(func=_cmd_task_resume)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
