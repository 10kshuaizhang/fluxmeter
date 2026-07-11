"""FluxMeter client — sends token usage events via Kafka or HTTP API."""

from __future__ import annotations

import json
import logging
import atexit
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

from fluxmeter.event import TokenEvent
from fluxmeter.streaming import StreamingWrapper

logger = logging.getLogger(__name__)


def _parse_openai_usage(response) -> dict:
    """Extract token fields from an OpenAI-compatible ChatCompletion response."""
    if hasattr(response, "model"):
        model = response.model
        usage = response.usage
        request_id = response.id
    else:
        model = response["model"]
        usage = response["usage"]
        request_id = response.get("id")

    if hasattr(usage, "prompt_tokens"):
        input_tokens = usage.prompt_tokens or 0
        output_tokens = usage.completion_tokens or 0
        cache_read = getattr(usage, "prompt_tokens_details", None)
        cache_read_tokens = getattr(cache_read, "cached_tokens", 0) if cache_read else 0
        reasoning = getattr(usage, "completion_tokens_details", None)
        reasoning_tokens = getattr(reasoning, "reasoning_tokens", 0) if reasoning else 0
    else:
        input_tokens = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)
        details = usage.get("prompt_tokens_details", {}) or {}
        cache_read_tokens = details.get("cached_tokens", 0)
        comp_details = usage.get("completion_tokens_details", {}) or {}
        reasoning_tokens = comp_details.get("reasoning_tokens", 0)

    return {
        "model_id": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_read_tokens": cache_read_tokens,
        "reasoning_tokens": reasoning_tokens,
        "request_id": request_id,
    }


class FluxMeter:
    """Main FluxMeter client.

    Modes:
      - HTTP (Lite/default): ``FluxMeter(api_url="http://localhost:8000")``
      - Kafka (Full): ``FluxMeter(kafka_brokers="localhost:9094")`` with local WAL

    Usage:
        from fluxmeter import FluxMeter

        meter = FluxMeter(api_url="http://localhost:8000")
        meter.track(customer_id="cust_123", model_id="gpt-4o", input_tokens=500, output_tokens=150)
    """

    def __init__(
        self,
        kafka_brokers: Optional[str] = None,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
        topic: str = "token-events",
        environment: Optional[str] = None,
        producer_config: Optional[dict] = None,
        wal_enabled: bool = True,
        wal_path: str = "~/.fluxmeter/wal",
    ):
        self._api_url = api_url.rstrip("/") if api_url else None
        self._api_key = api_key
        self._topic = topic
        self._environment = environment
        self._delivery_errors = 0
        self._events_sent = 0
        self._wal_enabled = wal_enabled
        self._producer = None
        self._wal = None

        if self._api_url:
            atexit.register(self.flush)
            return

        # Kafka path (Full mode)
        from confluent_kafka import Producer
        from fluxmeter.wal import WriteAheadLog

        brokers = kafka_brokers or "localhost:9094"
        config = {
            "bootstrap.servers": brokers,
            "linger.ms": 5,
            "batch.num.messages": 10000,
            "compression.type": "lz4",
            "acks": "all",
        }
        if producer_config:
            config.update(producer_config)

        self._producer = Producer(config)

        if wal_enabled:
            self._wal = WriteAheadLog(path=wal_path)
            self._flush_thread = threading.Thread(target=self._wal_flush_loop, daemon=True)
            self._flush_thread.start()

        atexit.register(self.flush)

    def _http_json(self, method: str, path: str, body: Optional[dict] = None, query: Optional[dict] = None) -> dict:
        url = f"{self._api_url}{path}"
        if query:
            url = f"{url}?{urllib.parse.urlencode({k: v for k, v in query.items() if v is not None})}"
        data = None
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["X-API-Key"] = self._api_key
        if body is not None:
            data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8")
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                raise RuntimeError(f"HTTP {e.code}: {raw}") from e

    def check(
        self,
        customer_id: str,
        estimated_cost_usd: float = 0.0,
        *,
        parent_span_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> dict:
        """Pre-request budget gate. Requires ``api_url``."""
        if not self._api_url:
            raise RuntimeError("check() requires api_url (HTTP mode)")
        return self._http_json(
            "GET",
            f"/budget/{urllib.parse.quote(customer_id, safe='')}/check",
            query={
                "estimated_cost_usd": estimated_cost_usd,
                "parent_span_id": parent_span_id,
                "session_id": session_id,
            },
        )

    def reserve(
        self,
        customer_id: str,
        estimated_cost_usd: float,
        *,
        parent_span_id: Optional[str] = None,
    ) -> dict:
        """Hold estimated cost for streaming. Requires admin-capable ``api_key`` in HTTP mode."""
        if not self._api_url:
            raise RuntimeError("reserve() requires api_url (HTTP mode)")
        query: dict[str, object] = {"estimated_cost_usd": estimated_cost_usd}
        if parent_span_id:
            query["parent_span_id"] = parent_span_id
        return self._http_json(
            "POST",
            f"/budget/{urllib.parse.quote(customer_id, safe='')}/reserve",
            query=query,
        )

    def reconcile(
        self,
        customer_id: str,
        reserved_usd: float,
        actual_usd: float = 0.0,
        *,
        parent_span_id: Optional[str] = None,
    ) -> dict:
        if not self._api_url:
            raise RuntimeError("reconcile() requires api_url (HTTP mode)")
        query: dict[str, object] = {"reserved_usd": reserved_usd, "actual_usd": actual_usd}
        if parent_span_id:
            query["parent_span_id"] = parent_span_id
        return self._http_json(
            "POST",
            f"/budget/{urllib.parse.quote(customer_id, safe='')}/reconcile",
            query=query,
        )

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
        parent_span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        """Track usage from an OpenAI ChatCompletion response object.

        Args:
            customer_id: Your customer/tenant identifier.
            response: OpenAI ChatCompletion response (or dict).
            session_id: Optional conversation session ID.
            span_id: Optional observability span ID.
            parent_span_id: Optional parent agent-run span for cost rollup.
            latency_ms: Request latency in ms.
            environment: Override instance-level environment.

        Returns:
            The TokenEvent that was sent.
        """
        parsed = _parse_openai_usage(response)
        return self.track(
            customer_id=customer_id,
            provider="openai",
            span_id=span_id,
            parent_span_id=parent_span_id,
            session_id=session_id,
            latency_ms=latency_ms,
            environment=environment,
            **parsed,
        )

    def _track_openai_compatible(
        self,
        customer_id: str,
        response,
        *,
        provider: str,
        session_id: Optional[str] = None,
        span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        parsed = _parse_openai_usage(response)
        return self.track(
            customer_id=customer_id,
            provider=provider,
            span_id=span_id,
            session_id=session_id,
            latency_ms=latency_ms,
            environment=environment,
            **parsed,
        )

    def track_deepseek(
        self,
        customer_id: str,
        response,
        *,
        session_id: Optional[str] = None,
        span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        """Track usage from a DeepSeek ChatCompletion response (OpenAI-compatible)."""
        return self._track_openai_compatible(
            customer_id, response, provider="deepseek",
            session_id=session_id, span_id=span_id,
            latency_ms=latency_ms, environment=environment,
        )

    def track_qwen(
        self,
        customer_id: str,
        response,
        *,
        session_id: Optional[str] = None,
        span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        """Track usage from a Qwen/DashScope compatible-mode response."""
        return self._track_openai_compatible(
            customer_id, response, provider="qwen",
            session_id=session_id, span_id=span_id,
            latency_ms=latency_ms, environment=environment,
        )

    def track_glm(
        self,
        customer_id: str,
        response,
        *,
        session_id: Optional[str] = None,
        span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        """Track usage from a Zhipu GLM OpenAI-compatible response."""
        return self._track_openai_compatible(
            customer_id, response, provider="zhipu",
            session_id=session_id, span_id=span_id,
            latency_ms=latency_ms, environment=environment,
        )

    def track_moonshot(
        self,
        customer_id: str,
        response,
        *,
        session_id: Optional[str] = None,
        span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        """Track usage from a Moonshot/Kimi ChatCompletion response."""
        return self._track_openai_compatible(
            customer_id, response, provider="moonshot",
            session_id=session_id, span_id=span_id,
            latency_ms=latency_ms, environment=environment,
        )

    def track_doubao(
        self,
        customer_id: str,
        response,
        *,
        session_id: Optional[str] = None,
        span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        """Track usage from a Volcengine Doubao/Ark response."""
        return self._track_openai_compatible(
            customer_id, response, provider="doubao",
            session_id=session_id, span_id=span_id,
            latency_ms=latency_ms, environment=environment,
        )

    def track_baichuan(
        self,
        customer_id: str,
        response,
        *,
        session_id: Optional[str] = None,
        span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        """Track usage from a Baichuan OpenAI-compatible response."""
        return self._track_openai_compatible(
            customer_id, response, provider="baichuan",
            session_id=session_id, span_id=span_id,
            latency_ms=latency_ms, environment=environment,
        )

    def track_minimax(
        self,
        customer_id: str,
        response,
        *,
        session_id: Optional[str] = None,
        span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        """Track usage from a MiniMax ChatCompletion response."""
        return self._track_openai_compatible(
            customer_id, response, provider="minimax",
            session_id=session_id, span_id=span_id,
            latency_ms=latency_ms, environment=environment,
        )

    def track_hunyuan(
        self,
        customer_id: str,
        response,
        *,
        session_id: Optional[str] = None,
        span_id: Optional[str] = None,
        latency_ms: int = 0,
        environment: Optional[str] = None,
    ) -> TokenEvent:
        """Track usage from a Tencent Hunyuan OpenAI-compatible response."""
        return self._track_openai_compatible(
            customer_id, response, provider="hunyuan",
            session_id=session_id, span_id=span_id,
            latency_ms=latency_ms, environment=environment,
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
        """Send event via HTTP ingest or Kafka WAL."""
        event_dict = event.to_dict()

        if self._api_url:
            try:
                self._http_json("POST", "/ingest", body=event_dict)
                self._events_sent += 1
            except Exception as e:
                self._delivery_errors += 1
                logger.debug("HTTP ingest failed: %s", e)
            return

        if self._wal:
            self._wal.append(event_dict)
            return  # WAL flush thread is the sole Kafka sender (no duplicate replay)

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
            self._delivery_errors += 1
            logger.debug("Kafka send failed: %s", e)

    def _produce_event(self, evt: dict) -> bool:
        """Send one event to Kafka and wait for broker ack. Returns False on failure."""
        value = json.dumps(evt, separators=(",", ":")).encode("utf-8")
        customer_id = evt.get("customerId", "unknown")
        for _ in range(2):
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=customer_id.encode("utf-8"),
                    value=value,
                    on_delivery=self._on_delivery,
                )
                self._producer.flush(timeout=10)
                self._events_sent += 1
                return True
            except BufferError:
                self._producer.flush(timeout=10)
            except Exception as e:
                self._delivery_errors += 1
                logger.debug("Kafka send failed: %s", e)
                return False
        self._delivery_errors += 1
        return False

    def _flush_wal_once(self) -> bool:
        """Send at most one pending WAL event across all files. Returns True if one was sent."""
        if not self._wal:
            return False
        for f in self._wal.pending_files():
            offset = self._wal.get_send_offset(f)
            evt, new_offset = self._wal.read_next_event_from_offset(f, offset)
            if evt is None:
                if f != self._wal._current_file and self._wal.is_fully_sent(f):
                    self._wal.mark_flushed(f, 0)
                continue
            if not self._produce_event(evt):
                return False
            self._wal.advance_send_offset(f, new_offset)
            if f != self._wal._current_file and self._wal.is_fully_sent(f):
                self._wal.mark_flushed(f, 1)
            return True
        return False

    def _wal_flush_loop(self) -> None:
        """Background thread: sends pending WAL events to Kafka one at a time."""
        while True:
            time.sleep(1)
            if not self._wal:
                break
            try:
                while self._flush_wal_once():
                    pass
            except Exception as e:
                logger.debug("WAL flush error: %s", e)

    def _on_delivery(self, err, msg):
        if err:
            self._delivery_errors += 1
            logger.debug("FluxMeter delivery failed: %s", err)

    def flush(self, timeout: float = 10.0) -> None:
        """Flush pending events. Drains WAL before closing (Kafka mode)."""
        if self._api_url:
            return
        if self._wal:
            deadline = time.time() + timeout
            while time.time() < deadline and self._flush_wal_once():
                pass
        if self._producer:
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
