# Local Command Lease Design

**Issue:** BOU-2710  
**Destination:** `agent-coordinator`, followed by a separate thin Gaia adapter

## Goal

Provide one generic, fenced way to run a local command under an exclusive
per-resource lease. Gaia will use it to permit one local frontend test run per
canonical worktree without constraining CI or tests in other worktrees.

## Boundaries

The coordinator owns:

- canonical lease-key construction;
- atomic acquisition, heartbeat, fencing, and release;
- child command lifetime and bounded teardown;
- structured contention and exit results.

The Gaia adapter owns:

- choosing namespace `local-frontend-test`;
- invoking Vitest;
- Node heap and Vitest timeout defaults;
- selecting the coordinator store path.

The coordinator does not know about Node, Vitest, Gaia, RSS, CI providers, or
the BOU-2709 process-token representation.

## Public contracts

### Lease key

```python
@dataclass(frozen=True)
class LeaseKey:
    namespace: str
    resource_key: str

    def task_identity(self) -> TaskIdentity:
        ...
```

`namespace` separates unrelated ownership domains. `resource_key` is the
canonical identity inside that domain. The task fingerprint is a fixed contract
version, not volatile command contents, so two invocations for the same
resource contend.

For worktrees:

```python
canonical_worktree_resource(path: str | PathLike[str]) -> str
```

The canonical resource is `Path(path).resolve(strict=True)`. Symlink and
relative aliases therefore converge. The function rejects missing paths.

CI callers use a different namespace and never contend with
`local-frontend-test`.

### Managed command

```python
@dataclass(frozen=True)
class LeaseRunRequest:
    key: LeaseKey
    command: tuple[str, ...]
    cwd: Path
    lease_seconds: int
    heartbeat_seconds: int
    timeout_seconds: float
    terminate_grace_seconds: float
    session_id: str
    agent: str


@dataclass(frozen=True)
class LeaseRunResult:
    state: Literal["exited", "timed_out", "interrupted", "contended"]
    exit_code: int
    claim: ClaimRecord | None
    holder: ClaimRecord | None
    holder_age_seconds: float | None
    remediation: str | None
```

`run_with_lease(store, request)` acquires, launches, heartbeats, waits, tears
down if needed, and releases in one upstream implementation.

The CLI command is:

```text
agent-coordinator run-with-lease \
  --namespace local-frontend-test \
  --worktree-path /canonical/or/alias/path \
  --lease-seconds 120 \
  --heartbeat-seconds 20 \
  --timeout-seconds 900 \
  --terminate-grace-seconds 10 \
  -- command args...
```

It emits one JSON result. Contention is a fast nonzero exit containing the
current holder, holder age, and the remediation: wait for the holder, stop it
normally, or wait for lease expiry after a crash.

## Ownership and reclaim semantics

Existing generic claims may reclaim an owner immediately when an injected PID
probe proves it dead. A managed local command uses a stricter lease-only
coordinator mode:

- a valid lease remains authoritative even if the holder PID appears absent;
- PID reuse cannot create an early takeover;
- heartbeats extend the current epoch;
- only normal fenced release or lease expiry permits a successor;
- after expiry, acquisition atomically supersedes the predecessor and receives
  a higher epoch;
- a stalled predecessor cannot heartbeat or release after supersession.

This is conservative by construction and avoids duplicating the typed native
process identity implementation published by BOU-2709. Downstream guardians
may attach BOU-2709 observations as evidence, but PID evidence never bypasses
the lease boundary.

## Command lifecycle

1. Canonicalize the requested worktree.
2. Acquire the lease with the wrapper process as owner.
3. On conflict, return immediately without launching a child.
4. Launch the command in its own process group.
5. Heartbeat before the lease can expire.
6. Wait until exit, timeout, or interrupt.
7. On timeout or interrupt, send `TERM` to the child process group, wait the
   bounded grace period, then send `KILL` if necessary.
8. Release the claim in `finally` with a non-terminal reason describing the
   outcome.
9. If fencing rejects release, preserve the child outcome and report the stale
   epoch; never mutate the successor.

The runner does not inspect RSS and never kills a valid owner for memory use.
Resource ceilings are supplied to the launched runtime by the adapter.

## Failure behavior

- Invalid keys, paths, timings, or empty commands fail before acquisition.
- Acquisition conflict launches nothing.
- Spawn failure still releases the claim.
- Test failure returns the child exit code and releases.
- Timeout uses a dedicated nonzero CLI exit.
- Interrupt returns the conventional interrupt exit code after teardown and
  release.
- Wrapper crash cannot run cleanup; heartbeat stops and expiry/fencing provide
  recovery.

## Verification

Deterministic tests cover:

- same canonical worktree contention and diagnostic fields;
- different worktrees and different namespaces running independently;
- relative/symlink path aliases;
- dead or reused PID not permitting takeover before expiry;
- expired takeover with a higher epoch;
- stale predecessor heartbeat/release fencing;
- release after success, child failure, spawn failure, timeout, and interrupt;
- bounded `TERM`/`KILL` teardown;
- child environment and exit-code preservation;
- CLI JSON and exit-code contracts.

The Gaia PR separately verifies heap flags, Vitest timeouts, local namespace,
CI namespace isolation, and that it delegates rather than reimplements leases.
