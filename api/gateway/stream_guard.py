"""SSE stream guard — mid-flight budget kill aligned with sdk wrap._KillableStream."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional

from gateway.pricing_estimate import estimate_stream_cost


@dataclass
class StreamUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    chunks: int = 0
    killed: bool = False

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens


@dataclass
class StreamGuard:
    """Parse upstream SSE, estimate spend, kill when over reserved hold."""

    model: str
    reserved_usd: float
    usage: StreamUsage = field(default_factory=StreamUsage)
    _buffer: str = ""

    async def transform(self, upstream: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
        """Pass through SSE chunks; stop and emit error when hold exceeded."""
        async for raw in upstream:
            text = raw.decode("utf-8", errors="replace")
            self._buffer += text
            while "\n\n" in self._buffer:
                block, self._buffer = self._buffer.split("\n\n", 1)
                out = self._process_block(block)
                if out is None:
                    yield self._kill_sse()
                    return
                if out:
                    yield out.encode("utf-8")

        if self._buffer.strip():
            out = self._process_block(self._buffer)
            if out is None:
                yield self._kill_sse()
                return
            if out:
                yield out.encode("utf-8")

    def _process_block(self, block: str) -> Optional[str]:
        lines = [ln for ln in block.split("\n") if ln.startswith("data:")]
        if not lines:
            return block + "\n\n" if block.strip() else ""

        rebuilt: list[str] = []
        for line in lines:
            payload = line[5:].strip()
            if payload == "[DONE]":
                rebuilt.append(line)
                continue
            try:
                chunk = json.loads(payload)
            except json.JSONDecodeError:
                rebuilt.append(line)
                continue

            self.usage.chunks += 1
            self._note_chunk(chunk)
            est = estimate_stream_cost(
                self.model, self.usage.input_tokens, self.usage.output_tokens
            )
            # ponytail: char/4 heuristic when usage chunk absent; upgrade path = provider usage field
            if self.reserved_usd > 0 and est > self.reserved_usd:
                self.usage.killed = True
                return None
            rebuilt.append(f"data: {json.dumps(chunk)}")

        return "\n".join(rebuilt) + "\n\n"

    def _note_chunk(self, chunk: dict[str, Any]) -> None:
        choices = chunk.get("choices") or []
        if choices:
            delta = (choices[0] or {}).get("delta") or {}
            content = delta.get("content")
            if content:
                self.usage.output_tokens += max(1, len(content) // 4)

        usage = chunk.get("usage")
        if usage:
            if usage.get("completion_tokens") is not None:
                self.usage.output_tokens = int(usage["completion_tokens"])
            if usage.get("prompt_tokens") is not None:
                self.usage.input_tokens = int(usage["prompt_tokens"])

    @staticmethod
    def _kill_sse() -> bytes:
        err = {
            "error": {
                "message": "stream killed: estimated cost exceeds reserved hold",
                "type": "fluxmeter_budget",
                "code": "stream_killed",
            }
        }
        return (
            f"data: {json.dumps(err)}\n\n"
            "data: [DONE]\n\n"
        ).encode("utf-8")
