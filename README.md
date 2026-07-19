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
