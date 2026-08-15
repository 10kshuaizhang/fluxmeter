"""FluxMeter HTTP client for AI usage metering."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

from fluxmeter.event import TokenEvent
from fluxmeter.streaming import StreamingWrapper


class DeliveryError(RuntimeError):
    """The SDK could not transfer custody of a usage event to FluxMeter."""

    def __init__(self, event_id: str, message: str):
        super().__init__(message)
        self.event_id = event_id


class _HTTPStatusError(RuntimeError):
    def __init__(self, status: int, detail: object):
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status


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
    """Main HTTP-only FluxMeter client.

    Usage:
        from fluxmeter import FluxMeter

        meter = FluxMeter(api_url="http://localhost:8000")
        meter.track(customer_id="cust_123", model_id="gpt-4o", input_tokens=500, output_tokens=150)
    """

    def __init__(
        self,
        api_url: str = "http://localhost:8000",
        api_key: Optional[str] = None,
        environment: Optional[str] = None,
        max_retries: int = 2,
        retry_base_seconds: float = 0.1,
    ):
        self._api_url = api_url.rstrip("/")
        self._api_key = api_key
        self._environment = environment
        self._max_retries = max(0, max_retries)
        self._retry_base_seconds = max(0.0, retry_base_seconds)
        self._delivery_errors = 0
        self._events_sent = 0

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
                detail = json.loads(raw)
            except json.JSONDecodeError:
                detail = raw
            raise _HTTPStatusError(e.code, detail) from e

    def check(
        self,
        customer_id: str,
        estimated_cost_usd: float = 0.0,
        *,
        parent_span_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> dict:
        """Pre-request budget gate."""
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
        """Hold estimated cost for streaming. Requires an admin-capable API key."""
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
        """Send an event over HTTP with bounded, identity-safe retries."""
        event_dict = event.to_dict()
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                self._http_json("POST", "/ingest", body=event_dict)
                self._events_sent += 1
                return
            except Exception as exc:
                last_error = exc
                if isinstance(exc, _HTTPStatusError) and not (
                    exc.status == 429 or exc.status == 503 or exc.status >= 500
                ):
                    break
                if attempt < self._max_retries:
                    time.sleep(self._retry_base_seconds * (2**attempt))

        self._delivery_errors += 1
        raise DeliveryError(event.event_id, f"FluxMeter delivery failed: {last_error}") from last_error

    def flush(self, timeout: float = 10.0) -> None:
        """Compatibility no-op: HTTP requests transfer custody synchronously."""
        return

    @property
    def events_sent(self) -> int:
        """Total events sent (including buffered)."""
        return self._events_sent

    @property
    def delivery_errors(self) -> int:
        """Total delivery failures."""
        return self._delivery_errors
