# BOU-2710 Local Command Lease Implementation Plan

**Design:** `docs/superpowers/specs/2026-07-30-local-command-lease-design.md`

## Task 1: Lease-key and conservative reclaim contract

**Files**

- Create `src/agent_coordinator/lease_runner.py`
- Create `tests/test_lease_runner_contract.py`
- Modify `src/agent_coordinator/service.py`
- Modify `src/agent_coordinator/__init__.py`

### RED

Add tests specifying:

- `LeaseKey(namespace, resource_key)` validates non-empty values and maps to a
  stable `TaskIdentity`;
- canonical worktree resources converge for relative and symlink aliases;
- missing paths are rejected;
- a coordinator configured for lease-only reclaim does not reclaim a dead or
  PID-reused owner before expiry;
- expiry permits a successor with a higher lease epoch;
- different namespaces and resource keys do not contend.

Run:

```bash
python3 -m pytest -q tests/test_lease_runner_contract.py
```

Expected RED: the lease runner types and reclaim policy do not exist.

### GREEN

Implement:

- `LeaseKey`;
- `canonical_worktree_resource`;
- `TaskCoordinator(..., reclaim_dead_owners: bool = True)`;
- lease-only decision behavior when false;
- public exports.

Preserve the existing default owner-death behavior for all current consumers.

Run the new tests plus existing service/fencing tests.

Commit:

```text
feat: define conservative local lease resources
```

## Task 2: Managed child lifecycle

**Files**

- Modify `src/agent_coordinator/lease_runner.py`
- Create `tests/test_lease_runner.py`

### RED

Specify `LeaseRunRequest`, `LeaseRunResult`, and `run_with_lease`:

- same-resource contention launches nothing and reports holder, age, and
  remediation;
- successful and failing commands preserve exit code and release;
- spawn failure releases;
- heartbeat keeps a long command leased;
- timeout sends TERM, escalates to KILL after a bounded grace period, and
  releases;
- interrupt tears down and releases;
- simulated wrapper crash leaves a claim recoverable only after expiry;
- a deposed owner cannot heartbeat or release a successor.

Use injected launcher, clock, sleeper, and heartbeat driver boundaries for
deterministic unit tests. Add one real subprocess smoke test.

Run:

```bash
python3 -m pytest -q tests/test_lease_runner.py
```

Expected RED: request/result/runner do not exist.

### GREEN

Implement a small runner with:

- validated positive timings and non-empty command;
- wrapper PID ownership;
- lease-only `TaskCoordinator`;
- child process group isolation;
- heartbeat loop;
- bounded TERM/KILL teardown;
- fenced release in `finally`;
- no RSS inspection.

Run contract, runner, service, and fencing tests.

Commit:

```text
feat: run local commands under fenced leases
```

## Task 3: JSON CLI

**Files**

- Modify `src/agent_coordinator/cli.py`
- Modify `tests/test_cli.py`
- Modify `README.md`

### RED

Specify `run-with-lease`:

- canonicalizes `--worktree-path`;
- accepts namespace/timing/store/session/agent arguments plus a command after
  `--`;
- contention returns the dedicated nonzero code and structured holder fields;
- child exit codes are preserved where representable;
- timeout and interrupt use stable nonzero codes;
- malformed requests return structured bad-request JSON;
- different CI/local namespaces do not contend.

### GREEN

Wire the CLI directly to `run_with_lease`; do not duplicate acquisition,
heartbeat, teardown, or release logic. Document the generic command and state
that runtime memory/test ceilings are adapter-owned.

Run:

```bash
python3 -m pytest -q tests/test_cli.py tests/test_lease_runner_contract.py tests/test_lease_runner.py
```

Commit:

```text
feat: expose fenced local command runner
```

## Task 4: Upstream verification and publication

Run:

```bash
python3 -m pytest -q
python3 -m build
git diff --check
```

Review the diff against every BOU-2710 upstream acceptance criterion. Push and
open one `agent-coordinator` PR. Keep the final bead open through CI/review.

## Task 5: Thin Gaia adoption

After the upstream PR merges, create a separate Gaia branch and PR.

The Gaia adapter must:

- derive the canonical worktree through the upstream CLI;
- use namespace `local-frontend-test`;
- route local frontend test entrypoints through `run-with-lease`;
- leave CI in a distinct namespace or outside the local lease path;
- apply conservative `NODE_OPTIONS=--max-old-space-size=...`;
- apply bounded Vitest execution and teardown timeouts;
- surface structured contention remediation;
- contain no copied claim, heartbeat, fencing, or process-reaping logic.

Run focused wrapper tests, frontend tests, and repository-required gates before
publication.
