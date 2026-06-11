# agent-coordinator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Python library and CLI for durable agent task ownership claims, heartbeats, releases, and reclaim decisions.

**Architecture:** Use small stdlib-only modules: `models.py` for immutable value objects, `store.py` for append-only JSONL persistence with locking, `service.py` for claim/reclaim semantics, and `cli.py` for shell access. Tests drive the public library and CLI behavior.

**Tech Stack:** Python 3.11+, dataclasses, argparse, json, fcntl file locking on POSIX, pytest.

---

### Task 1: Core Ownership Semantics

**Files:**
- Create: `src/agent_coordinator/models.py`
- Create: `src/agent_coordinator/store.py`
- Create: `src/agent_coordinator/service.py`
- Test: `tests/test_service.py`

- [x] Write failing tests for claim, heartbeat, release, expiration, fingerprint changes, and dead-owner reclaim.
- [x] Run `python3 -m pytest tests/test_service.py -q` and confirm imports fail before implementation.
- [x] Implement the minimal models, store, and coordinator service.
- [x] Run `python3 -m pytest tests/test_service.py -q` and confirm the tests pass.

### Task 2: CLI

**Files:**
- Create: `src/agent_coordinator/cli.py`
- Test: `tests/test_cli.py`

- [x] Write failing tests for `claim`, `status`, `reclaimable`, and `release`.
- [x] Implement argparse commands as thin wrappers over `TaskCoordinator`.
- [x] Run `python3 -m pytest tests/test_cli.py -q` and confirm the tests pass.

### Task 3: Package Validation

**Files:**
- Modify: `README.md`
- Modify: `pyproject.toml`

- [x] Run the full test suite with `python3 -m pytest -q`.
- [x] Run a CLI smoke flow using `python3 -m agent_coordinator.cli`.
- [x] Commit the standalone repo.
