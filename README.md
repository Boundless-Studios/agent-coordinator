# agent-coordinator

`agent-coordinator` is a small Python library and CLI for coordinating ownership of agent tasks.

It provides durable task claims, lease-based heartbeats, release events, and reclaim decisions. The core package is intentionally generic: callers define task types, task ids, and fingerprints. A PR dashboard, for example, can use `task_type=pr-maintenance`, `task_id=github:owner/repo#123`, and a fingerprint derived from unresolved review threads and failing checks.

## Fenced CLI flow

Every new ownership claim receives a monotonically increasing `lease_epoch`.
Callers must return both the claim ID and epoch for heartbeat and release
mutations:

```bash
agent-coordinator claim \
  --type pr-maintenance \
  --id github:org/repo#8 \
  --fingerprint abc123 \
  --session-id s1 \
  --pid "$$"
# {"state":"active","claim":{"claim_id":"...","lease_epoch":1,...}}

agent-coordinator heartbeat \
  --claim-id <claim-id> \
  --session-id s1 \
  --lease-epoch 1

agent-coordinator status --type pr-maintenance --id github:org/repo#8 --fingerprint abc123
agent-coordinator reclaimable --type pr-maintenance --id github:org/repo#8 --fingerprint abc123

agent-coordinator release \
  --claim-id <claim-id> \
  --session-id s1 \
  --lease-epoch 1 \
  --reason completed
```

Claiming again from the same owner refreshes its lease and preserves the
epoch. Once that ownership is reclaimable, a successor receives a strictly
higher epoch. A stale heartbeat or release returns exit code `4` with a
`stale_lease_epoch` JSON error, so a predecessor cannot mutate the successor
claim.

Epoch `0` exists only to read events written by versions before `0.2.0`; the
coordinator never allocates it to a new claim.

## Fencing a deposed owner

The fence is evaluated against **the task's current epoch**, not against the
caller's own claim record, and it runs *inside* the same store transaction as
the write. That closes the window where a stalled owner (swap, `SIGSTOP`, a
long adapter call) resumes after its lease expired, heartbeats its own claim
id at its own epoch, and quietly extends a lease that a successor already took
over — two live owners on one task.

Concretely: when a successor claims a task, the same transaction that mints
epoch *N+1* also marks every still-active predecessor claim for that task
`superseded`, so a stale claim id cannot be resurrected at all. A deposed
owner's `heartbeat` and `release` both fail with `StaleClaimError`, which
carries `expected_epoch`, `received_epoch`, and `current_claim_id` — enough for
the loser to learn it was deposed and by whom. The CLI reports the same three
fields alongside `"error": "stale_lease_epoch"`.

Consumers should treat `StaleClaimError` as authoritative rather than
pre-checking with `status()` and then mutating: a separate read transaction
followed by a write transaction is a TOCTOU gap, and any pause between the two
lets a stale owner re-arm. The in-transaction fence is what makes the mutation
safe.

## Run one local command per resource

*Requires `0.5.0` or newer.*

`run-with-lease` owns acquisition, heartbeat, process-group teardown, and
fenced release for a local command:

```bash
agent-coordinator run-with-lease \
  --namespace local-frontend-test \
  --worktree-path "$PWD" \
  --session-id "$$" \
  --agent developer-shell \
  --lease-seconds 120 \
  --heartbeat-seconds 20 \
  --timeout-seconds 900 \
  --terminate-grace-seconds 10 \
  -- npm test -- --run
```

The worktree path is resolved strictly, so relative and symlink aliases contend
on one canonical resource. Different worktrees and namespaces remain
independent; CI should use a distinct namespace and therefore never wait for a
local run.

Contention exits quickly with JSON containing the current holder, holder age,
and remediation. Managed commands use conservative lease-only reclaim: a
dead-looking or PID-reused holder cannot be displaced before expiry. Wrapper
crashes stop heartbeats; expiry permits a successor with a higher epoch, and
fencing prevents the predecessor from mutating that successor.

The JSON claim is the post-release snapshot when release succeeds. Store or
release failures return a nonzero coordinator status even when the child
succeeded. If TERM/KILL cannot confirm that the complete process group stopped,
the result is `teardown_failed` and the claim is deliberately left active until
lease expiry rather than admitting an overlapping successor.

The coordinator does not inspect RSS and does not apply language-specific
limits. Adapters are responsible for Node heap ceilings, test-runner timeouts,
and other runtime policy; they should delegate lease ownership and cleanup
unchanged.

## Human decisions

*Requires `0.4.0` or newer.*

Ownership answers *who is working on a task*. A decision answers *what the agent
is blocked on, and who unblocked it*. An agent that reaches a boundary it does
not own — a service split, a schema change, a trust boundary — records a
decision request and stops, instead of guessing.

```text
active ──request──▶ waiting_human ──human answer──▶ resumable ──resume──▶ active ──▶ released
```

`cancelled` and `superseded` are the explicit terminal paths. **Time alone never
moves `waiting_human` to `resumable`, and never selects an option.** There is no
timeout, no default answer, and no auto-proceed. Downstream loops depend on this:
an unresolved decision is a *waiting* state, not an executor failure, so it must
not feed a retry/death loop.

```bash
agent-coordinator decision-request \
  --type pr-maintenance --id github:org/repo#8 --fingerprint comments:a \
  --claim-id <claim-id> --session-id s1 --runtime codex \
  --logical-key turn-orchestration-boundary \
  --category architecture \
  --question "Should turn orchestration own its own service boundary?" \
  --option "id=split-service,summary=Own service,tradeoffs=Clean boundary; new deploy unit" \
  --option "id=keep-module,summary=Keep in module,tradeoffs=No new infra; implicit boundary" \
  --recommendation keep-module \
  --rationale "Reversible today; the cross-process hop is not yet justified" \
  --affected-scope backend/src/gaia/orchestrator \
  --evidence-fingerprint evidence:v1
# {"decision_id":"...","state":"waiting_human",...}

agent-coordinator decision-status --type pr-maintenance --id github:org/repo#8 --fingerprint comments:a

agent-coordinator decision-resolve \
  --decision-id <decision-id> \
  --request-fingerprint evidence:v1 \
  --human-actor ilya \
  --option-id split-service
# {"state":"resumable",...}

agent-coordinator task-resume \
  --decision-id <decision-id> --claim-id <claim-id> \
  --session-id s2 --lease-epoch 2
# {"state":"resumed",...}
```

`decision-cancel` withdraws a question; `decision-list` filters by `--state` and
optionally by task.

### Two fingerprints, deliberately

`--fingerprint` identifies the **task**. `--evidence-fingerprint` covers the
**evidence and task state the question is about**. They are separate because work
can move on in ways that invalidate the question without changing task identity.

Re-requesting with the same `--logical-key` and the same `--evidence-fingerprint`
is idempotent: same question, same evidence, one ledger entry. Re-requesting with
a *changed* evidence fingerprint supersedes the prior request and opens a new one,
and a resolution carrying the superseded fingerprint is rejected with
`stale_decision_fingerprint` — a human cannot accidentally answer a question that
no longer exists.

Supersession is carried on the replacement's own `decision_requested` event
rather than as a separate event. Two appends would leave a window in which the
old decision is retired and the replacement does not exist yet; in that window
the task has no blocking decision and could be released as `completed` with the
question still unanswered.

### Completion is gated, yielding is not

Releasing with a reason in `TERMINAL_RELEASE_REASONS` (`completed`,
`terminal_clean`) fails with `decision_pending` while any decision on that task
is `waiting_human`. Every other reason still succeeds, so a headless executor
can yield and exit without losing the question. The check runs *inside* the
release transaction for the same TOCTOU reason as the lease fence.

### Surviving the process that asked

Decision state is derived from the ledger and keyed off task identity — never off
claim status. An agent that dies mid-question leaves a complete, replayable
record. After a human answers, a **replacement** owner may claim the task and
call `task-resume`; the resumed work can cite the direction it was given.

Resolution unblocks; it does not transfer ownership or execute anything. Nothing
runs until some owner explicitly claims the task.

`task-resume` is fenced by the same lease epoch as `heartbeat` and `release`, so
a deposed owner cannot record a resume and start a second runtime on one task.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success |
| `3` | Claim conflict |
| `4` | `stale_lease_epoch` |
| `5` | `stale_decision_fingerprint` |
| `6` | `decision_pending` — terminal completion blocked |
| `7` | `decision_not_resumable` — resume before a human answered |
| `8` | Malformed request or resolution |
| `9` | Unknown decision or claim |

## Bounded claim history

The coordinator compacts `claims.jsonl` after 1,000 new events by default.
Compaction preserves every live claim and seven days of released, superseded,
or expired claim history. It also preserves the global lease-epoch watermark,
so pruning an old claim cannot let a future epoch move backward.

The compacted log is written to a same-directory temporary file, flushed, and
atomically replaced while the existing store lock is held. Callers do not need
to coordinate compaction or change how they construct `JsonlClaimStore` and
`TaskCoordinator`.

Tests and specialized deployments can tune the bounds through
`TaskCoordinator(compaction_event_threshold=..., claim_history_retention=...)`.
