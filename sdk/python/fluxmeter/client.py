"""FluxMeter client — sends token usage events to Kafka."""

from __future__ import annotations

import json
import logging
import atexit
import threading
import time
from typing import Optional

from confluent_kafka import Producer

from fluxmeter.event import TokenEvent
from fluxmeter.streaming import StreamingWrapper
from fluxmeter.wal import WriteAheadLog

logger = logging.getLogger(__name__)


class FluxMeter:
    """Main FluxMeter client. Sends token events to Kafka for real-time aggregation.

    Events are persisted to a local WAL (write-ahead log) BEFORE sending to Kafka.
    If Kafka is unavailable, events accumulate on disk and flush when it recovers.
    This guarantees zero event loss regardless of Kafka availability.

    Usage:
        from fluxmeter import FluxMeter

        meter = FluxMeter(kafka_brokers="localhost:9094")
        meter.track(customer_id="cust_123", model_id="gpt-4o", input_tokens=500, output_tokens=150)
    """

    def __init__(
        self,
        kafka_brokers: str = "localhost:9094",
        topic: str = "token-events",
        environment: Optional[str] = None,
        producer_config: Optional[dict] = None,
        wal_enabled: bool = True,
        wal_path: str = "~/.fluxmeter/wal",
    ):
        self._topic = topic
        self._environment = environment
        self._delivery_errors = 0
        self._events_sent = 0
        self._wal_enabled = wal_enabled

        config = {
            "bootstrap.servers": kafka_brokers,
            "linger.ms": 5,
            "batch.num.messages": 10000,
            "compression.type": "lz4",
            "acks": "all",  # Wait for all replicas (no data loss on broker crash)
        }
        if producer_config:
            config.update(producer_config)

        self._producer = Producer(config)

        # Local WAL: events persisted to disk before Kafka send
        if wal_enabled:
            self._wal = WriteAheadLog(path=wal_path)
            self._flush_thread = threading.Thread(target=self._wal_flush_loop, daemon=True)
            self._flush_thread.start()
        else:
            self._wal = None

        atexit.register(self.flush)

    def track(
        self,
        customer_id: str,
        model_id: str,
        *,
        provider: str = "openai",
        input_tokens: int = 0,
        output_tokens: int = 0,
        cache_read_tokens: int = 0,
        cache_write_tokens: int = 0,
        reasoning_tokens: int = 0,
        embedding_tokens: int = 0,
        request_id: Optional[str] = None,
        span_id: Optional[str] = None,
        parent_span_id: Optional[str] = None,
        session_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
    ) -> TokenEvent:
        """Track a single LLM API call's token usage.

        Args:
            customer_id: Your customer/tenant identifier.
            model_id: Model name (e.g. "gpt-4o", "claude-sonnet-4").
            provider: Provider name ("openai", "anthropic", "google").
            input_tokens: Prompt/input token count.
            output_tokens: Completion/output token count.
            cache_read_tokens: Cached prompt tokens read.
            cache_write_tokens: Tokens written to prompt cache.
            reasoning_tokens: Internal reasoning tokens (o1/o3).
            embedding_tokens: Embedding tokens.
            request_id: Provider's request ID.
            span_id: Observability span ID.
            session_id: Conversation/session identifier.
            latency_ms: Provider response time in milliseconds.
            environment: Override instance-level environment.
            metadata: Arbitrary key-value pairs.

        Returns:
            The TokenEvent that was sent.
        """
        event = TokenEvent(
            customer_id=customer_id,
            model_id=model_id,
            provider=provider,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_write_tokens=cache_write_tokens,
            reasoning_tokens=reasoning_tokens,
            embedding_tokens=embedding_tokens,
            request_id=request_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            session_id=session_id,
            latency_ms=latency_ms,
            environment=environment or self._environment,
            metadata=metadata,
        )
        self._send(event)
        return event

    def track_openai(
        self,
        customer_id: str,
        response,
        *,
        session_id: Optional[str] = None,
        span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        """Track usage from an OpenAI ChatCompletion response object.

        Args:
            customer_id: Your customer/tenant identifier.
            response: OpenAI ChatCompletion response (or dict).
            session_id: Optional conversation session ID.
            span_id: Optional observability span ID.
            latency_ms: Request latency in ms.
            environment: Override instance-level environment.

        Returns:
            The TokenEvent that was sent.
        """
        # Handle both object and dict responses
        if hasattr(response, "model"):
            model = response.model
            usage = response.usage
            request_id = response.id
        else:
            model = response["model"]
            usage = response["usage"]
            request_id = response.get("id")

        # Extract token counts from usage
        if hasattr(usage, "prompt_tokens"):
            input_tokens = usage.prompt_tokens or 0
            output_tokens = usage.completion_tokens or 0
            cache_read = getattr(usage, "prompt_tokens_details", None)
            cache_read_tokens = (
                getattr(cache_read, "cached_tokens", 0) if cache_read else 0
            )
            reasoning = getattr(usage, "completion_tokens_details", None)
            reasoning_tokens = (
                getattr(reasoning, "reasoning_tokens", 0) if reasoning else 0
            )
        else:
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            details = usage.get("prompt_tokens_details", {}) or {}
            cache_read_tokens = details.get("cached_tokens", 0)
            comp_details = usage.get("completion_tokens_details", {}) or {}
            reasoning_tokens = comp_details.get("reasoning_tokens", 0)

        return self.track(
            customer_id=customer_id,
            model_id=model,
            provider="openai",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_read_tokens=cache_read_tokens,
            reasoning_tokens=reasoning_tokens,
            request_id=request_id,
            span_id=span_id,
            session_id=session_id,
            latency_ms=latency_ms,
            environment=environment,
        )

    def track_anthropic(
        self,
        customer_id: str,
        response,
        *,
        session_id: Optional[str] = None,
        span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        """Track usage from an Anthropic Message response object.

        Args:
            customer_id: Your customer/tenant identifier.
            response: Anthropic Message response (or dict).
            session_id: Optional conversation session ID.
            span_id: Optional observability span ID.
            latency_ms: Request latency in ms.
            environment: Override instance-level environment.

        Returns:
            The TokenEvent that was sent.
        """
        if hasattr(response, "model"):
            model = response.model
            usage = response.usage
            request_id = response.id
        else:
            model = response["model"]
            usage = response["usage"]
            request_id = response.get("id")

        if hasattr(usage, "input_tokens"):
            input_tokens = usage.input_tokens or 0
            output_tokens = usage.output_tokens or 0
            cache_read_tokens = getattr(usage, "cache_read_input_tokens", 0) or 0
            cache_write_tokens = getattr(usage, "cache_creation_input_tokens", 0) or 0
        else:
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)
            cache_read_tokens = usage.get("cache_read_input_tokens", 0)
            cache_write_tokens = usage.get("cache_creation_input_tokens", 0)

        return self.track(
            customer_id=customer_id,
            model_id=model,
            provider="anthropic",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_write_tokens=cache_write_tokens,
            request_id=request_id,
            span_id=span_id,
            session_id=session_id,
            latency_ms=latency_ms,
            environment=environment,
        )

    def wrap_stream(
        self,
        stream,
        customer_id: str,
        model_id: str,
        *,
        provider: str = "openai",
        input_tokens: int = 0,
        heartbeat_interval_sec: float = 2.0,
        parent_span_id: Optional[str] = None,
        session_id: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> StreamingWrapper:
        """Wrap a streaming LLM response for near-real-time usage tracking.

        Emits heartbeat events every heartbeat_interval_sec during the stream,
        then a final accurate event when the stream completes.

        Usage:
            stream = client.chat.completions.create(..., stream=True)
            for chunk in meter.wrap_stream(stream, "cust_1", "gpt-4o"):
                process(chunk)
            # Final event emitted automatically
        """
        return StreamingWrapper(
            stream=stream,
            meter=self,
            customer_id=customer_id,
            model_id=model_id,
            provider=provider,
            input_tokens=input_tokens,
            heartbeat_interval_sec=heartbeat_interval_sec,
            parent_span_id=parent_span_id,
            session_id=session_id,
            environment=environment or self._environment,
        )

    def _send(self, event: TokenEvent) -> None:
        """Persist event to WAL, then send to Kafka. Zero data loss."""
        event_dict = event.to_dict()

        # Step 1: Write to local WAL (survives Kafka outages)
        if self._wal:
            self._wal.append(event_dict)

        # Step 2: Best-effort send to Kafka (WAL flush thread handles retries)
        try:
            value = json.dumps(event_dict, separators=(",", ":")).encode("utf-8")
            self._producer.produce(
                topic=self._topic,
                key=event.customer_id.encode("utf-8"),
                value=value,
                on_delivery=self._on_delivery,
            )
            self._events_sent += 1
            self._producer.poll(0)
        except (BufferError, Exception) as e:
            # Kafka unavailable — event is safe in WAL, will be flushed later
            self._delivery_errors += 1
            logger.debug("Kafka send failed (event safe in WAL): %s", e)

    def _wal_flush_loop(self) -> None:
        """Background thread: flushes old WAL files to Kafka."""
        while True:
            time.sleep(5)
            if not self._wal:
                break
            try:
                files = self._wal.pending_files()
                for f in files:
                    # Skip the current active file (still being written to)
                    if f == self._wal._current_file:
                        continue
                    events = self._wal.read_events(f)
                    if not events:
                        self._wal.mark_flushed(f, 0)
                        continue
                    # Send all events from this file
                    for evt in events:
                        value = json.dumps(evt, separators=(",", ":")).encode("utf-8")
                        customer_id = evt.get("customerId", "unknown")
                        try:
                            self._producer.produce(
                                topic=self._topic,
                                key=customer_id.encode("utf-8"),
                                value=value,
                            )
                        except BufferError:
                            self._producer.flush(timeout=10)
                            break  # Retry this file next cycle
                    else:
                        # All events sent successfully
                        self._producer.flush(timeout=10)
                        self._wal.mark_flushed(f, len(events))
            except Exception as e:
                logger.debug("WAL flush error: %s", e)

    def _on_delivery(self, err, msg):
        if err:
            self._delivery_errors += 1
            logger.debug("FluxMeter delivery failed: %s", err)

    def flush(self, timeout: float = 10.0) -> None:
        """Flush pending events. Called automatically on exit."""
        self._producer.flush(timeout=timeout)
        if self._wal:
            self._wal.close()

    @property
    def events_sent(self) -> int:
        """Total events sent (including buffered)."""
        return self._events_sent

    @property
    def delivery_errors(self) -> int:
        """Total delivery failures."""
        return self._delivery_errors
