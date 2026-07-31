from __future__ import annotations

import json
import sys

import pytest

from agent_coordinator import JsonlClaimStore, OwnerIdentity, TaskCoordinator
from agent_coordinator.cli import main
from agent_coordinator.lease_runner import (
    LeaseKey,
    LeaseRunResult,
    LeaseRunState,
    canonical_worktree_resource,
)


def run_cli(args, capsys) -> tuple[int, dict]:
    code = main(args)
    output = capsys.readouterr().out
    return code, json.loads(output)


def lease_run_args(tmp_path, store, *command: str) -> list[str]:
    return [
        "run-with-lease",
        "--store",
        str(store),
        "--namespace",
        "local-frontend-test",
        "--worktree-path",
        str(tmp_path),
        "--session-id",
        "cli-runner",
        "--agent",
        "pytest",
        "--lease-seconds",
        "30",
        "--heartbeat-seconds",
        "5",
        "--timeout-seconds",
        "10",
        "--terminate-grace-seconds",
        "0.2",
        "--",
        *command,
    ]


def test_cli_run_with_lease_preserves_child_exit_and_releases(tmp_path, capsys):
    store = tmp_path / "claims.jsonl"

    code, payload = run_cli(
        lease_run_args(
            tmp_path,
            store,
            sys.executable,
            "-c",
            "raise SystemExit(7)",
        ),
        capsys,
    )

    assert code == 7
    assert payload["state"] == "exited"
    assert payload["exit_code"] == 7
    assert payload["claim"]["task"]["task_type"] == "local-frontend-test"
    assert payload["claim"]["task"]["task_id"] == canonical_worktree_resource(tmp_path)
    decision = TaskCoordinator(JsonlClaimStore(store)).status(
        LeaseKey(
            "local-frontend-test",
            canonical_worktree_resource(tmp_path),
        ).task_identity()
    )
    assert decision.reclaimable is True


def test_cli_run_with_lease_keeps_child_stdout_out_of_json(tmp_path, capfd):
    store = tmp_path / "claims.jsonl"

    code = main(
        lease_run_args(
            tmp_path,
            store,
            sys.executable,
            "-c",
            "print('child-output')",
        )
    )
    captured = capfd.readouterr()

    assert code == 0
    assert json.loads(captured.out)["state"] == "exited"
    assert "child-output" not in captured.out
    assert "child-output" in captured.err


@pytest.mark.parametrize("child_exit", [126, 130, 255])
def test_cli_run_with_lease_preserves_representable_exit_codes(
    tmp_path,
    capsys,
    child_exit,
):
    code, payload = run_cli(
        lease_run_args(
            tmp_path,
            tmp_path / "claims.jsonl",
            sys.executable,
            "-c",
            f"raise SystemExit({child_exit})",
        ),
        capsys,
    )

    assert payload["exit_code"] == child_exit
    assert code == child_exit


def test_cli_run_with_lease_returns_nonzero_when_release_fails(
    tmp_path,
    capsys,
    monkeypatch,
):
    monkeypatch.setattr(
        "agent_coordinator.cli.run_with_lease",
        lambda *_args, **_kwargs: LeaseRunResult(
            state=LeaseRunState.EXITED,
            exit_code=0,
            release_error="disk full",
        ),
    )

    code, payload = run_cli(
        lease_run_args(
            tmp_path,
            tmp_path / "claims.jsonl",
            sys.executable,
            "-c",
            "raise SystemExit(0)",
        ),
        capsys,
    )

    assert payload["exit_code"] == 0
    assert payload["release_error"] == "disk full"
    assert code != 0


def test_cli_run_with_lease_reports_acquisition_io_error_as_json(
    tmp_path,
    capsys,
    monkeypatch,
):
    def fail_acquisition(*_args, **_kwargs):
        raise OSError("store is unwritable")

    monkeypatch.setattr("agent_coordinator.cli.run_with_lease", fail_acquisition)

    code, payload = run_cli(
        lease_run_args(
            tmp_path,
            tmp_path / "claims.jsonl",
            sys.executable,
            "-c",
            "raise SystemExit(0)",
        ),
        capsys,
    )

    assert code != 0
    assert payload == {
        "error": "lease_operation_failed",
        "detail": "store is unwritable",
    }


def test_cli_run_with_lease_reports_contending_holder(tmp_path, capsys):
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    task = LeaseKey(
        "local-frontend-test",
        canonical_worktree_resource(tmp_path),
    ).task_identity()
    TaskCoordinator(store, reclaim_dead_owners=False).claim_task(
        task,
        OwnerIdentity(session_id="holder", pid=4242, agent="vitest"),
        lease_seconds=60,
    )

    code, payload = run_cli(
        lease_run_args(
            tmp_path,
            store.path,
            sys.executable,
            "-c",
            "raise SystemExit(0)",
        ),
        capsys,
    )

    assert code == 3
    assert payload["state"] == "contended"
    assert payload["holder"]["owner"]["session_id"] == "holder"
    assert payload["holder_age_seconds"] >= 0
    assert "lease expiry" in payload["remediation"]


def test_cli_namespaces_do_not_contend(tmp_path, capsys):
    store = JsonlClaimStore(tmp_path / "claims.jsonl")
    TaskCoordinator(store, reclaim_dead_owners=False).claim_task(
        LeaseKey(
            "ci-frontend-test",
            canonical_worktree_resource(tmp_path),
        ).task_identity(),
        OwnerIdentity(session_id="ci", pid=4242, agent="ci"),
        lease_seconds=60,
    )

    code, payload = run_cli(
        lease_run_args(
            tmp_path,
            store.path,
            sys.executable,
            "-c",
            "raise SystemExit(0)",
        ),
        capsys,
    )

    assert code == 0
    assert payload["state"] == "exited"


def test_cli_run_with_lease_rejects_missing_worktree(tmp_path, capsys):
    args = lease_run_args(
        tmp_path / "missing",
        tmp_path / "claims.jsonl",
        sys.executable,
        "-c",
        "raise SystemExit(0)",
    )

    code, payload = run_cli(args, capsys)

    assert code == 8
    assert payload["error"] == "invalid_lease_run"


def test_cli_claim_status_reclaimable_release_flow(tmp_path, capsys):
    store = tmp_path / "claims.jsonl"
    common = [
        "--store",
        str(store),
        "--type",
        "pr-maintenance",
        "--id",
        "github:Boundless-Studios/agentic-pr-dash#8",
        "--fingerprint",
        "comments:a",
    ]

    code, claimed = run_cli(
        [
            "claim",
            *common,
            "--session-id",
            "s1",
            "--pid",
            "0",
            "--agent",
            "codex",
            "--worktree-path",
            "/tmp/worktree",
            "--lease-seconds",
            "60",
        ],
        capsys,
    )
    assert code == 0
    assert claimed["state"] == "active"
    claim_id = claimed["claim"]["claim_id"]
    lease_epoch = claimed["claim"]["lease_epoch"]

    code, status = run_cli(["status", *common], capsys)
    assert code == 0
    assert status["state"] == "active"
    assert status["reclaimable"] is False

    code, reclaimable = run_cli(["reclaimable", *common], capsys)
    assert code == 1
    assert reclaimable["reclaimable"] is False

    code, heartbeat = run_cli(
        [
            "heartbeat",
            "--store",
            str(store),
            "--claim-id",
            claim_id,
            "--session-id",
            "s1",
            "--lease-epoch",
            str(lease_epoch),
            "--lease-seconds",
            "60",
        ],
        capsys,
    )
    assert code == 0
    assert heartbeat["claim"]["lease_epoch"] == lease_epoch

    code, released = run_cli(
        [
            "release",
            "--store",
            str(store),
            "--claim-id",
            claim_id,
            "--session-id",
            "s1",
            "--lease-epoch",
            str(lease_epoch),
            "--reason",
            "completed",
        ],
        capsys,
    )
    assert code == 0
    assert released["claim"]["status"] == "completed"

    code, reclaimable = run_cli(["reclaimable", *common], capsys)
    assert code == 0
    assert reclaimable["reclaimable"] is True


def test_cli_rejects_stale_lease_epoch_for_mutations(tmp_path, capsys):
    store = tmp_path / "claims.jsonl"
    common = [
        "--store",
        str(store),
        "--type",
        "pr-maintenance",
        "--id",
        "github:Boundless-Studios/agentic-pr-dash#8",
        "--fingerprint",
        "comments:a",
    ]

    code, first = run_cli(
        [
            "claim",
            *common,
            "--session-id",
            "s1",
            "--pid",
            "0",
            "--agent",
            "codex",
            "--worktree-path",
            "/tmp/worktree",
            "--lease-seconds",
            "60",
        ],
        capsys,
    )
    assert code == 0
    first_claim = first["claim"]

    code, released = run_cli(
        [
            "release",
            "--store",
            str(store),
            "--claim-id",
            first_claim["claim_id"],
            "--session-id",
            "s1",
            "--lease-epoch",
            str(first_claim["lease_epoch"]),
        ],
        capsys,
    )
    assert code == 0
    assert released["state"] == "released"

    code, second = run_cli(
        [
            "claim",
            *common,
            "--session-id",
            "s2",
            "--pid",
            "0",
            "--agent",
            "codex",
            "--worktree-path",
            "/tmp/worktree",
            "--lease-seconds",
            "60",
        ],
        capsys,
    )
    assert code == 0
    second_claim = second["claim"]
    assert second_claim["lease_epoch"] > first_claim["lease_epoch"]

    for command in ("heartbeat", "release"):
        args = [
            command,
            "--store",
            str(store),
            "--claim-id",
            second_claim["claim_id"],
            "--session-id",
            "s2",
            "--lease-epoch",
            str(first_claim["lease_epoch"]),
        ]
        if command == "heartbeat":
            args.extend(["--lease-seconds", "60"])
        code, stale = run_cli(args, capsys)
        assert code == 4
        assert stale == {
            "error": "stale_lease_epoch",
            "expected_epoch": second_claim["lease_epoch"],
            "received_epoch": first_claim["lease_epoch"],
            "current_claim_id": second_claim["claim_id"],
        }


def test_cli_rejects_deposed_owner_mutations_on_its_own_claim(tmp_path, capsys):
    """BOU-2209: an owner deposed after a lease expiry must not re-arm itself."""
    store = tmp_path / "claims.jsonl"
    common = [
        "--store",
        str(store),
        "--type",
        "pr-maintenance",
        "--id",
        "github:Boundless-Studios/agentic-pr-dash#8",
        "--fingerprint",
        "comments:a",
    ]

    def claim_as(session_id: str, lease_seconds: int) -> dict:
        code, payload = run_cli(
            [
                "claim",
                *common,
                "--session-id",
                session_id,
                "--pid",
                "0",
                "--agent",
                "codex",
                "--worktree-path",
                "/tmp/worktree",
                "--lease-seconds",
                str(lease_seconds),
            ],
            capsys,
        )
        assert code == 0
        return payload["claim"]

    # A takes a lease so short it is already expired by the time B claims.
    first_claim = claim_as("s1", 0)
    second_claim = claim_as("s2", 600)
    assert second_claim["lease_epoch"] > first_claim["lease_epoch"]
    assert second_claim["claim_id"] != first_claim["claim_id"]

    # A resumes and tries to mutate *its own* claim id at *its own* epoch.
    for command in ("heartbeat", "release"):
        args = [
            command,
            "--store",
            str(store),
            "--claim-id",
            first_claim["claim_id"],
            "--session-id",
            "s1",
            "--lease-epoch",
            str(first_claim["lease_epoch"]),
        ]
        if command == "heartbeat":
            args.extend(["--lease-seconds", "600"])
        code, stale = run_cli(args, capsys)
        assert code == 4
        assert stale == {
            "error": "stale_lease_epoch",
            "expected_epoch": second_claim["lease_epoch"],
            "received_epoch": first_claim["lease_epoch"],
            "current_claim_id": second_claim["claim_id"],
        }

    # B is still the sole owner.
    code, status = run_cli(["status", *common], capsys)
    assert code == 0
    assert status["state"] == "active"
    assert status["claim"]["claim_id"] == second_claim["claim_id"]


# ---------------------------------------------------------------------------
# Decision protocol (BOU-2039)
#
# Driven through main() rather than a shell so the argparse wiring, exit codes,
# and JSON payloads are all covered. Codex and Claude share exactly this
# surface, so a break here is a break for both runtimes.
# ---------------------------------------------------------------------------


def decision_common(store):
    return [
        "--store",
        str(store),
        "--type",
        "pr-maintenance",
        "--id",
        "github:Boundless-Studios/agentic-pr-dash#8",
        "--fingerprint",
        "comments:a",
    ]


def request_args(store, claim_id, *, evidence="evidence:v1", extra=None):
    args = [
        "decision-request",
        *decision_common(store),
        "--claim-id",
        claim_id,
        "--session-id",
        "s1",
        "--runtime",
        "codex",
        "--logical-key",
        "turn-orchestration-boundary",
        "--category",
        "architecture",
        "--question",
        "Should turn orchestration own its own service boundary?",
        "--option",
        "id=split-service,summary=Own service,tradeoffs=Clean boundary; new deploy unit",
        "--option",
        "id=keep-module,summary=Keep in module,tradeoffs=No new infra; implicit boundary",
        "--recommendation",
        "keep-module",
        "--rationale",
        "Reversible today",
        "--affected-scope",
        "backend/src/gaia/orchestrator",
        "--evidence-fingerprint",
        evidence,
    ]
    args.extend(extra or [])
    return args


def claim_for_decision(store, capsys) -> dict:
    code, claimed = run_cli(
        [
            "claim",
            *decision_common(store),
            "--session-id",
            "s1",
            "--pid",
            "0",
            "--agent",
            "codex",
            "--lease-seconds",
            "600",
        ],
        capsys,
    )
    assert code == 0
    return claimed["claim"]


def test_cli_decision_request_resolve_resume_flow(tmp_path, capsys):
    store = tmp_path / "claims.jsonl"
    claim = claim_for_decision(store, capsys)

    code, requested = run_cli(request_args(store, claim["claim_id"]), capsys)
    assert code == 0
    assert requested["state"] == "waiting_human"
    decision_id = requested["decision_id"]
    assert len(requested["request"]["options"]) == 2

    code, status = run_cli(["decision-status", *decision_common(store)], capsys)
    assert code == 0
    assert status["decision"]["decision_id"] == decision_id

    # Completion is blocked while the human still owes an answer.
    release = [
        "release",
        "--store",
        str(store),
        "--claim-id",
        claim["claim_id"],
        "--session-id",
        "s1",
        "--lease-epoch",
        str(claim["lease_epoch"]),
    ]
    code, blocked = run_cli([*release, "--reason", "completed"], capsys)
    assert code == 6
    assert blocked["error"] == "decision_pending"
    assert blocked["decision_ids"] == [decision_id]

    code, resolved = run_cli(
        [
            "decision-resolve",
            "--store",
            str(store),
            "--decision-id",
            decision_id,
            "--request-fingerprint",
            "evidence:v1",
            "--human-actor",
            "ilya",
            "--option-id",
            "split-service",
        ],
        capsys,
    )
    assert code == 0
    assert resolved["state"] == "resumable"

    code, resumed = run_cli(
        [
            "task-resume",
            "--store",
            str(store),
            "--decision-id",
            decision_id,
            "--claim-id",
            claim["claim_id"],
            "--session-id",
            "s1",
            "--lease-epoch",
            str(claim["lease_epoch"]),
        ],
        capsys,
    )
    assert code == 0
    assert resumed["state"] == "resumed"
    assert resumed["resolution"]["selected_option_id"] == "split-service"

    code, released = run_cli([*release, "--reason", "completed"], capsys)
    assert code == 0
    assert released["claim"]["status"] == "completed"


def test_cli_duplicate_request_is_idempotent(tmp_path, capsys):
    store = tmp_path / "claims.jsonl"
    claim = claim_for_decision(store, capsys)

    code, first = run_cli(request_args(store, claim["claim_id"]), capsys)
    assert code == 0
    code, second = run_cli(request_args(store, claim["claim_id"]), capsys)
    assert code == 0
    assert second["decision_id"] == first["decision_id"]

    events = [
        json.loads(line)
        for line in store.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert sum(1 for e in events if e.get("event") == "decision_requested") == 1


def test_cli_stale_resolution_exits_distinctly(tmp_path, capsys):
    store = tmp_path / "claims.jsonl"
    claim = claim_for_decision(store, capsys)
    code, first = run_cli(request_args(store, claim["claim_id"]), capsys)
    assert code == 0
    code, second = run_cli(
        request_args(store, claim["claim_id"], evidence="evidence:v2"), capsys
    )
    assert code == 0

    code, stale = run_cli(
        [
            "decision-resolve",
            "--store",
            str(store),
            "--decision-id",
            first["decision_id"],
            "--request-fingerprint",
            "evidence:v1",
            "--human-actor",
            "ilya",
            "--option-id",
            "split-service",
        ],
        capsys,
    )
    assert code == 5
    assert stale["error"] == "stale_decision_fingerprint"
    assert stale["superseding_decision_id"] == second["decision_id"]


def test_cli_non_terminal_release_is_allowed_while_waiting(tmp_path, capsys):
    store = tmp_path / "claims.jsonl"
    claim = claim_for_decision(store, capsys)
    code, requested = run_cli(request_args(store, claim["claim_id"]), capsys)
    assert code == 0

    code, released = run_cli(
        [
            "release",
            "--store",
            str(store),
            "--claim-id",
            claim["claim_id"],
            "--session-id",
            "s1",
            "--lease-epoch",
            str(claim["lease_epoch"]),
            "--reason",
            "yielded",
        ],
        capsys,
    )
    assert code == 0
    assert released["claim"]["status"] == "yielded"

    code, status = run_cli(["decision-status", *decision_common(store)], capsys)
    assert status["decision"]["state"] == "waiting_human"


def test_cli_resume_before_resolution_exits_distinctly(tmp_path, capsys):
    store = tmp_path / "claims.jsonl"
    claim = claim_for_decision(store, capsys)
    code, requested = run_cli(request_args(store, claim["claim_id"]), capsys)
    assert code == 0

    code, payload = run_cli(
        [
            "task-resume",
            "--store",
            str(store),
            "--decision-id",
            requested["decision_id"],
            "--claim-id",
            claim["claim_id"],
            "--session-id",
            "s1",
            "--lease-epoch",
            str(claim["lease_epoch"]),
        ],
        capsys,
    )
    assert code == 7
    assert payload["error"] == "decision_not_resumable"
    assert payload["state"] == "waiting_human"


def test_cli_rejects_a_single_option(tmp_path, capsys):
    store = tmp_path / "claims.jsonl"
    claim = claim_for_decision(store, capsys)
    args = request_args(store, claim["claim_id"])
    # Drop the second --option pair.
    index = args.index("--option", args.index("--option") + 1)
    del args[index : index + 2]

    code, payload = run_cli(args, capsys)
    assert code == 8
    assert payload["error"] == "invalid_decision_request"


def test_cli_decision_list_filters_by_state(tmp_path, capsys):
    store = tmp_path / "claims.jsonl"
    claim = claim_for_decision(store, capsys)
    code, first = run_cli(request_args(store, claim["claim_id"]), capsys)
    assert code == 0
    code, second = run_cli(
        request_args(
            store,
            claim["claim_id"],
            extra=["--logical-key", "schema-compat"],
            evidence="evidence:s1",
        ),
        capsys,
    )
    assert code == 0
    code, _ = run_cli(
        [
            "decision-resolve",
            "--store",
            str(store),
            "--decision-id",
            second["decision_id"],
            "--request-fingerprint",
            "evidence:s1",
            "--human-actor",
            "ilya",
            "--direction",
            "Neither; inline it.",
        ],
        capsys,
    )
    assert code == 0

    code, waiting = run_cli(
        ["decision-list", "--store", str(store), "--state", "waiting_human"], capsys
    )
    assert code == 0
    assert [d["decision_id"] for d in waiting["decisions"]] == [first["decision_id"]]

    code, resumable = run_cli(
        ["decision-list", "--store", str(store), "--state", "resumable"], capsys
    )
    assert [d["decision_id"] for d in resumable["decisions"]] == [
        second["decision_id"]
    ]
