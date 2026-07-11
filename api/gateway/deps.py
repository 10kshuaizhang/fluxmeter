"""Shared dependencies for Gateway app."""

from __future__ import annotations

import os

import redis

from lite_aggregate_lua import LiteAggregator
from pricing_loader import reload_catalog

LITE_MODE = os.getenv("FLUXMETER_LITE_MODE", "false").lower() == "true"
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "token-events")
UPSTREAM_BASE = os.getenv("GATEWAY_UPSTREAM_BASE", "https://api.openai.com/v1").rstrip("/")
UPSTREAM_API_KEY = os.getenv("GATEWAY_UPSTREAM_API_KEY") or os.getenv("OPENAI_API_KEY") or ""

pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
)

_lite_aggregator: LiteAggregator | None = None
_kafka_producer = None


def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=pool)


def get_lite_aggregator() -> LiteAggregator:
    global _lite_aggregator
    if _lite_aggregator is None:
        r = get_redis()
        reload_catalog(redis_client=r)
        _lite_aggregator = LiteAggregator(r)
    return _lite_aggregator


def get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None:
        from confluent_kafka import Producer

        _kafka_producer = Producer({
            "bootstrap.servers": KAFKA_BROKERS,
            "linger.ms": 5,
            "compression.type": "lz4",
            "acks": "all",
        })
    return _kafka_producer


def init_gateway() -> None:
    """Startup hook: load pricing catalog."""
    reload_catalog(redis_client=get_redis())
