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

    code, status = run_cli(["status", *common], capsys)
    assert code == 0
    assert status["state"] == "active"
    assert status["reclaimable"] is False

    code, reclaimable = run_cli(["reclaimable", *common], capsys)
    assert code == 1
    assert reclaimable["reclaimable"] is False

    code, released = run_cli(
        [
            "release",
            "--store",
            str(store),
            "--claim-id",
            claim_id,
            "--session-id",
            "s1",
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
