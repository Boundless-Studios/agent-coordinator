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
