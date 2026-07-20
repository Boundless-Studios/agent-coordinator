from __future__ import annotations

import json

from agent_coordinator.cli import main


def run_cli(args, capsys) -> tuple[int, dict]:
    code = main(args)
    output = capsys.readouterr().out
    return code, json.loads(output)


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
