"""Token usage event model."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class TokenEvent:
    """Represents one LLM API call's token usage.

    Supports OpenAI, Anthropic, Google, and custom providers.
    All token fields are optional — set what's available from your provider response.
    """

    customer_id: str
    model_id: str
    provider: str = "openai"

    # Token counts
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    reasoning_tokens: int = 0
    embedding_tokens: int = 0

    # Identity & tracing
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    request_id: Optional[str] = None
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None  # Links child LLM calls to parent agent run
    session_id: Optional[str] = None

    # Timing
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    latency_ms: int = 0

    # Context
    environment: Optional[str] = None
    metadata: Optional[dict[str, str]] = None

    def to_dict(self) -> dict:
        """Serialize to dict with camelCase keys (matches Java consumer)."""
        d = {
            "eventId": self.event_id,
            "customerId": self.customer_id,
            "provider": self.provider,
            "modelId": self.model_id,
            "inputTokens": self.input_tokens,
            "outputTokens": self.output_tokens,
            "cacheReadTokens": self.cache_read_tokens,
            "cacheWriteTokens": self.cache_write_tokens,
            "reasoningTokens": self.reasoning_tokens,
            "embeddingTokens": self.embedding_tokens,
            "timestamp": self.timestamp,
            "latencyMs": self.latency_ms,
        }
        if self.request_id:
            d["requestId"] = self.request_id
        if self.span_id:
            d["spanId"] = self.span_id
        if self.parent_span_id:
            d["parentSpanId"] = self.parent_span_id
        if self.session_id:
            d["sessionId"] = self.session_id
        if self.environment:
            d["environment"] = self.environment
        if self.metadata:
            d["metadata"] = self.metadata
        return d

    @property
    def total_tokens(self) -> int:
        return (
            self.input_tokens
            + self.output_tokens
            + self.cache_read_tokens
            + self.cache_write_tokens
            + self.reasoning_tokens
            + self.embedding_tokens
        )
