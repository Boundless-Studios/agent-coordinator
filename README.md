# agent-coordinator

`agent-coordinator` is a small Python library and CLI for coordinating ownership of agent tasks.

It provides durable task claims, lease-based heartbeats, release events, and reclaim decisions. The core package is intentionally generic: callers define task types, task ids, and fingerprints. A PR dashboard, for example, can use `task_type=pr-maintenance`, `task_id=github:owner/repo#123`, and a fingerprint derived from unresolved review threads and failing checks.

## CLI Sketch

```bash
agent-coordinator claim --type pr-maintenance --id github:org/repo#8 --fingerprint abc123 --session-id s1 --pid "$$"
agent-coordinator heartbeat --claim-id <claim-id> --session-id s1
agent-coordinator status --type pr-maintenance --id github:org/repo#8 --fingerprint abc123
agent-coordinator reclaimable --type pr-maintenance --id github:org/repo#8 --fingerprint abc123
agent-coordinator release --claim-id <claim-id> --session-id s1 --reason completed
```
