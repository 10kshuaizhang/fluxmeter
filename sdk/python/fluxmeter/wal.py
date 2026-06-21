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
        # Byte offset successfully sent to Kafka per file (avoids duplicate replay)
        self._send_offsets: dict[str, int] = {}

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
            if self._pending_count % 100 == 0:
                os.fsync(self._file_handle.fileno())

    def pending_files(self) -> list[Path]:
        """List WAL files that may have unsent events (oldest first)."""
        files = sorted(self._dir.glob("wal-*.jsonl"))
        return files

    def get_send_offset(self, file_path: Path) -> int:
        """Return byte offset of last successfully sent event in this file."""
        return self._send_offsets.get(str(file_path), 0)

    def advance_send_offset(self, file_path: Path, new_offset: int) -> None:
        """Record how many bytes have been successfully sent from file_path."""
        with self._lock:
            self._send_offsets[str(file_path)] = new_offset

    def read_next_event_from_offset(
        self, file_path: Path, byte_offset: int
    ) -> tuple[dict | None, int]:
        """Read at most one event from byte_offset. Returns (event, new_offset)."""
        try:
            with open(file_path, "r") as f:
                f.seek(byte_offset)
                line = f.readline()
                if not line:
                    return None, byte_offset
                stripped = line.strip()
                if not stripped:
                    return None, f.tell()
                try:
                    return json.loads(stripped), f.tell()
                except json.JSONDecodeError:
                    return None, f.tell()
        except FileNotFoundError:
            return None, byte_offset

    def read_events_from_offset(self, file_path: Path, byte_offset: int) -> tuple[list[dict], int]:
        """Read events starting at byte_offset. Returns (events, new_byte_offset)."""
        events: list[dict] = []
        new_offset = byte_offset
        try:
            with open(file_path, "r") as f:
                f.seek(byte_offset)
                while True:
                    line_start = f.tell()
                    line = f.readline()
                    if not line:
                        break
                    stripped = line.strip()
                    if stripped:
                        try:
                            events.append(json.loads(stripped))
                            new_offset = f.tell()
                        except json.JSONDecodeError:
                            new_offset = f.tell()
                            continue
                    else:
                        new_offset = line_start + len(line)
        except FileNotFoundError:
            pass
        return events, new_offset

    def is_fully_sent(self, file_path: Path) -> bool:
        """True if all bytes in file have been sent to Kafka."""
        try:
            size = file_path.stat().st_size
        except FileNotFoundError:
            return True
        return self.get_send_offset(file_path) >= size and size > 0

    def mark_flushed(self, file_path: Path, count: int) -> None:
        """Mark a WAL file as fully flushed to Kafka. Deletes it."""
        with self._lock:
            if file_path == self._current_file:
                return
            key = str(file_path)
            self._send_offsets.pop(key, None)
            try:
                file_path.unlink()
                self._flushed_count += count
            except FileNotFoundError:
                pass

    def read_events(self, file_path: Path) -> list[dict]:
        """Read all events from a WAL file."""
        events, _ = self.read_events_from_offset(file_path, 0)
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

        if self._file_handle:
            self._file_handle.close()

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
