# agent-coordinator Design

## Goal

Build a standalone Python library and CLI that lets local agent loops coordinate ownership of work using durable claims, leases, heartbeats, release events, and reclaim decisions.

## Boundary

The coordinator is generic. It does not know what a PR, bead, CI check, branch, or worktree means. Integrations provide:

- `task_type`: a stable category such as `pr-maintenance`
- `task_id`: an integration-specific id such as `github:Boundless-Studios/agentic-pr-dash#8`
- `fingerprint`: an integration-specific blocker fingerprint
- owner metadata such as session id, pid, agent name, and worktree path

The framework coordinates ownership of the exact task fingerprint. If the PR review comments or failing checks change, the integration computes a new fingerprint and the previous claim no longer suppresses new work.

## Storage

V1 uses an append-only JSONL event log with a file lock. This keeps hooks and shell loops dependency-free at runtime and allows audit/debugging after a session exits.

## Core Decisions

- Active lease: a claim is active only while its lease has not expired and the owner process is still live.
- Reclaimable: no claim, an expired claim, a released claim, or a dead owner claim is reclaimable.
- Release: terminal events release a claim immediately without waiting for expiry.
- Conflict: a new owner cannot claim the same task fingerprint while a different active owner still holds it.
- Same owner refresh: the same owner can claim again to refresh ownership for the same task fingerprint.

## CLI

The CLI is a thin wrapper over the library:

- `claim`
- `heartbeat`
- `release`
- `status`
- `reclaimable`

All commands support `--store <path>` so integrations can choose repo-local, worktree-local, or user-global coordination state.

## First Integration Target

PR dashboard maintenance will compute `pr-maintenance` task identities from GitHub PR state and use the coordinator to suppress duplicate work only while a valid claim exists.
