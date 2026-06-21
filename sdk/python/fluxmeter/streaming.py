"""Streaming response wrapper for FluxMeter.

Wraps OpenAI/Anthropic streaming responses to emit partial usage events
during the stream (heartbeat every N chunks or every interval_sec).
Provides near-real-time visibility into long-running LLM calls.

Usage:
    from fluxmeter import FluxMeter

    meter = FluxMeter(kafka_brokers="localhost:9094")

    # OpenAI streaming
    stream = client.chat.completions.create(model="gpt-4o", messages=[...], stream=True)
    for chunk in meter.wrap_stream(stream, customer_id="cust_1", model_id="gpt-4o"):
        process(chunk)
    # Final usage event emitted automatically on stream end
"""

from __future__ import annotations

import time
from typing import Iterator, Optional, Any

from fluxmeter.event import TokenEvent


class StreamingWrapper:
    """Wraps a streaming LLM response iterator with usage tracking.

    Counts output tokens (approximated from chunks) and emits partial
    usage events at regular intervals. Emits a final event on stream end.
    """

    def __init__(
        self,
        stream: Iterator[Any],
        meter,  # FluxMeter instance
        customer_id: str,
        model_id: str,
        provider: str = "openai",
        input_tokens: int = 0,
        heartbeat_interval_sec: float = 2.0,
        parent_span_id: Optional[str] = None,
        session_id: Optional[str] = None,
        environment: Optional[str] = None,
    ):
        self._stream = stream
        self._meter = meter
        self._customer_id = customer_id
        self._model_id = model_id
        self._provider = provider
        self._input_tokens = input_tokens
        self._heartbeat_interval = heartbeat_interval_sec
        self._parent_span_id = parent_span_id
        self._session_id = session_id
        self._environment = environment

        self._output_chunks = 0
        self._estimated_output_tokens = 0
        self._last_emitted_output_tokens = 0
        self._last_heartbeat = time.time()
        self._start_time = time.time()
        self._finished = False
        self._request_id: Optional[str] = None

    def __iter__(self):
        return self

    def __next__(self):
        try:
            chunk = next(self._stream)
            self._process_chunk(chunk)

            # Emit heartbeat if interval elapsed
            now = time.time()
            if now - self._last_heartbeat >= self._heartbeat_interval:
                self._emit_heartbeat()
                self._last_heartbeat = now

            return chunk
        except StopIteration:
            self._emit_final()
            raise

    def _process_chunk(self, chunk) -> None:
        """Extract token info from a streaming chunk."""
        self._output_chunks += 1

        # OpenAI: chunk.choices[0].delta.content
        if hasattr(chunk, "choices") and chunk.choices:
            delta = getattr(chunk.choices[0], "delta", None)
            if delta and getattr(delta, "content", None):
                # Approximate: ~0.75 tokens per character for English
                self._estimated_output_tokens += max(1, len(delta.content) // 4)
            if not self._request_id and hasattr(chunk, "id"):
                self._request_id = chunk.id

        # Anthropic: chunk.type == "content_block_delta", chunk.delta.text
        elif hasattr(chunk, "type") and chunk.type == "content_block_delta":
            text = getattr(getattr(chunk, "delta", None), "text", "")
            if text:
                self._estimated_output_tokens += max(1, len(text) // 4)

        # OpenAI final chunk with usage
        if hasattr(chunk, "usage") and chunk.usage:
            usage = chunk.usage
            if hasattr(usage, "completion_tokens") and usage.completion_tokens:
                self._estimated_output_tokens = usage.completion_tokens
            if hasattr(usage, "prompt_tokens") and usage.prompt_tokens:
                self._input_tokens = usage.prompt_tokens

    def _emit_heartbeat(self) -> None:
        """Emit a partial usage event (heartbeat) during streaming."""
        delta = self._estimated_output_tokens - self._last_emitted_output_tokens
        if delta <= 0:
            return
        self._last_emitted_output_tokens = self._estimated_output_tokens
        self._meter.track(
            customer_id=self._customer_id,
            model_id=self._model_id,
            provider=self._provider,
            input_tokens=0,  # Only count input once in final event
            output_tokens=delta,
            parent_span_id=self._parent_span_id,
            session_id=self._session_id,
            environment=self._environment,
            metadata={"_heartbeat": "true", "_chunks": str(self._output_chunks)},
        )

    def _emit_final(self) -> None:
        """Emit the final usage event with accurate totals on stream end."""
        if self._finished:
            return
        self._finished = True

        latency_ms = int((time.time() - self._start_time) * 1000)
        self._meter.track(
            customer_id=self._customer_id,
            model_id=self._model_id,
            provider=self._provider,
            input_tokens=self._input_tokens,
            output_tokens=self._estimated_output_tokens,
            request_id=self._request_id,
            parent_span_id=self._parent_span_id,
            session_id=self._session_id,
            latency_ms=latency_ms,
            environment=self._environment,
            metadata={"_stream_chunks": str(self._output_chunks)},
        )

    @property
    def estimated_output_tokens(self) -> int:
        return self._estimated_output_tokens

    @property
    def elapsed_ms(self) -> int:
        return int((time.time() - self._start_time) * 1000)
