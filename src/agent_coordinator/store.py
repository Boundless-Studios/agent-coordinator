"""Append-only JSONL storage for task ownership events."""

from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Callable, Iterator, Optional


EventCompactor = Callable[[list[dict[str, Any]]], Optional[list[dict[str, Any]]]]


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            import fcntl
        except ImportError as exc:
            raise RuntimeError("exclusive file locking is unavailable") from exc
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            try:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except (ImportError, OSError):
                pass


class JsonlClaimStore:
    """Durable event store.

    Events are append-only JSON objects. Current claim state is derived by the
    service so the log remains auditable after an agent process exits.
    """

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")

    def append_event(self, event: dict[str, Any]) -> None:
        with _exclusive_lock(self.lock_path):
            self._append_event_unlocked(event)

    def read_events(self) -> list[dict[str, Any]]:
        with _exclusive_lock(self.lock_path):
            return self._read_events_unlocked()

    def transact_event(
        self,
        build_event: Callable[[list[dict[str, Any]]], dict[str, Any]],
        *,
        compact_events: EventCompactor | None = None,
    ) -> dict[str, Any]:
        with _exclusive_lock(self.lock_path):
            events = self._read_events_unlocked()
            event = build_event(events)
            if compact_events is None:
                self._append_event_unlocked(event)
                return event

            compacted = compact_events([*events, event])
            if compacted is None:
                self._append_event_unlocked(event)
            else:
                self._replace_events_unlocked(compacted)
            return event

    def _append_event_unlocked(self, event: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def _replace_events_unlocked(self, events: list[dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_path = tempfile.mkstemp(
            prefix=f".{self.path.name}.",
            suffix=".tmp",
            dir=self.path.parent,
        )
        temporary = Path(temporary_path)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                for event in events:
                    handle.write(json.dumps(event, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            directory_descriptor = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        finally:
            temporary.unlink(missing_ok=True)

    def _read_events_unlocked(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        events: list[dict[str, Any]] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                events.append(payload)
        return events
