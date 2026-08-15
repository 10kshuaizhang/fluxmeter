"""Shared dependencies for Gateway app."""

from __future__ import annotations

import os

import redis

from gateway.outbox import start_worker
from pricing_loader import reload_catalog

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "token-events")
KAFKA_ACK_TIMEOUT_SECONDS = float(os.getenv("KAFKA_ACK_TIMEOUT_SECONDS", "5"))
UPSTREAM_BASE = os.getenv("GATEWAY_UPSTREAM_BASE", "https://api.openai.com/v1").rstrip("/")
UPSTREAM_API_KEY = os.getenv("GATEWAY_UPSTREAM_API_KEY") or os.getenv("OPENAI_API_KEY") or ""

pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
)

_kafka_producer = None


def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=pool)


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
    if "FLUXMETER_LITE_MODE" in os.environ:
        raise RuntimeError("FLUXMETER_LITE_MODE was removed in FluxMeter 4.0")
    reload_catalog(redis_client=get_redis())
    if os.getenv("GATEWAY_OUTBOX_WORKER", "true").lower() == "true":
        start_worker(
            get_redis,
            get_kafka_producer,
            KAFKA_TOPIC,
            KAFKA_ACK_TIMEOUT_SECONDS,
        )
