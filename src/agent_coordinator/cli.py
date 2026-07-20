"""Command-line interface for agent-coordinator."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .models import OwnerIdentity, TaskIdentity
from .service import ClaimConflictError, StaleClaimError, TaskCoordinator
from .store import JsonlClaimStore


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
        return 3
    _print({"state": "active", "claim": claim.to_dict()})
    return 0


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
        return 4
    _print({"state": "released", "claim": claim.to_dict()})
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    decision = _coordinator(args).status(_task_from_args(args))
    _print(decision.to_dict())
    return 0


def _cmd_reclaimable(args: argparse.Namespace) -> int:
    decision = _coordinator(args).status(_task_from_args(args))
    _print(decision.to_dict())
    return 0 if decision.reclaimable else 1


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

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
