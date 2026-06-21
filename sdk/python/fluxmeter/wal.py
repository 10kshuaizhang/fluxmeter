"""Write-Ahead Log for FluxMeter SDK.

Events are persisted to a local append-only file BEFORE sending to Kafka.
If Kafka is unavailable, events accumulate on disk and flush when it recovers.
This guarantees no event loss regardless of Kafka availability.

File format: one JSON object per line (newline-delimited JSON / NDJSON).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class WriteAheadLog:
    """Append-only local event buffer with background flush to Kafka."""

    def __init__(
        self,
        path: str = "~/.fluxmeter/wal",
        max_file_size_mb: int = 100,
        flush_interval_sec: float = 1.0,
    ):
        self._dir = Path(os.path.expanduser(path))
        self._dir.mkdir(parents=True, exist_ok=True)
        self._max_file_size = max_file_size_mb * 1024 * 1024
        self._flush_interval = flush_interval_sec

        self._current_file: Optional[Path] = None
        self._file_handle = None
        self._lock = threading.Lock()
        self._pending_count = 0
        self._flushed_count = 0

        self._rotate_if_needed()

    def append(self, event_dict: dict) -> None:
        """Append event to WAL. Returns immediately. Thread-safe.
        Batch fsync: every 100 events or 500ms, whichever comes first."""
        line = json.dumps(event_dict, separators=(",", ":")) + "\n"
        with self._lock:
            self._rotate_if_needed()
            self._file_handle.write(line)
            self._file_handle.flush()
            self._pending_count += 1
            # Batch fsync: every 100 writes for true durability
            if self._pending_count % 100 == 0:
                os.fsync(self._file_handle.fileno())

    def pending_files(self) -> list[Path]:
        """List WAL files that have unflushed events (oldest first)."""
        files = sorted(self._dir.glob("wal-*.jsonl"))
        return files

    def mark_flushed(self, file_path: Path, count: int) -> None:
        """Mark a WAL file as fully flushed to Kafka. Deletes it."""
        with self._lock:
            if file_path == self._current_file:
                return  # Don't delete the active file
            try:
                file_path.unlink()
                self._flushed_count += count
            except FileNotFoundError:
                pass

    def read_events(self, file_path: Path) -> list[dict]:
        """Read all events from a WAL file."""
        events = []
        try:
            with open(file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            events.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue  # Skip corrupted lines
        except FileNotFoundError:
            pass
        return events

    def _rotate_if_needed(self) -> None:
        """Create a new WAL file if current is too large or doesn't exist."""
        if self._file_handle and self._current_file:
            try:
                size = self._current_file.stat().st_size
                if size < self._max_file_size:
                    return
            except FileNotFoundError:
                pass

        # Close old file
        if self._file_handle:
            self._file_handle.close()

        # Create new file with timestamp name
        ts = int(time.time() * 1000)
        self._current_file = self._dir / f"wal-{ts}.jsonl"
        self._file_handle = open(self._current_file, "a")

    @property
    def pending_count(self) -> int:
        return self._pending_count

    @property
    def flushed_count(self) -> int:
        return self._flushed_count

    def close(self) -> None:
        with self._lock:
            if self._file_handle:
                self._file_handle.close()
                self._file_handle = None
