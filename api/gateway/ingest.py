"""Proxy-only usage ingest (Lite or Full)."""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Optional

import redis

from gateway.deps import KAFKA_TOPIC, LITE_MODE, get_kafka_producer, get_lite_aggregator, get_redis
from webhook_deliver import deliver_lite_alerts
from billing_dims import increment_dims, validate_metadata


def ingest_usage(
    r: redis.Redis,
    *,
    customer_id: str,
    model_id: str,
    input_tokens: int,
    output_tokens: int,
    parent_span_id: Optional[str] = None,
    session_id: Optional[str] = None,
    metadata: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Record token usage from Gateway (same path as POST /ingest)."""
    now_ms = int(time.time() * 1000)
    event_dict: dict[str, Any] = {
        "customerId": customer_id,
        "modelId": model_id,
        "inputTokens": max(0, input_tokens),
        "outputTokens": max(0, output_tokens),
        "timestamp": now_ms,
    }
    if parent_span_id:
        event_dict["parentSpanId"] = parent_span_id
    if session_id:
        event_dict["sessionId"] = session_id
    if metadata:
        try:
            event_dict["metadata"] = validate_metadata(metadata)
        except ValueError:
            event_dict["metadata"] = None

    if LITE_MODE:
        agg = get_lite_aggregator()
        result = agg.aggregate(event_dict)
        if result.get("status") == "ok":
            cost = float(result.get("cost_usd") or 0)
            meta = event_dict.get("metadata")
            if meta and cost > 0:
                increment_dims(r, meta, cost_usd=cost, event_ts_ms=now_ms)
        deliver_lite_alerts(r, customer_id, result, model_id)
        return result

    if "eventId" not in event_dict:
        event_dict["eventId"] = str(uuid.uuid4())
    producer = get_kafka_producer()
    value = json.dumps(event_dict).encode("utf-8")
    producer.produce(KAFKA_TOPIC, key=customer_id.encode("utf-8"), value=value)
    producer.poll(0)
    return {"status": "accepted", "eventId": event_dict["eventId"]}
