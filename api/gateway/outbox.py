"""Durable Redis outbox for Gateway usage envelopes."""

from __future__ import annotations

import json
import logging
import threading
import time

from ingestion import KafkaUnavailableError, publish_with_ack
from budget_ops import reap_expired_reservations

logger = logging.getLogger(__name__)

PENDING_KEY = "gateway:outbox:pending"


def store(r, event_id: str, envelope: dict) -> None:
    pipe = r.pipeline()
    pipe.set(f"gateway:outbox:event:{event_id}", json.dumps(envelope))
    pipe.zadd(PENDING_KEY, {event_id: time.time()})
    pipe.execute()


def remove(r, event_id: str) -> None:
    pipe = r.pipeline()
    pipe.delete(f"gateway:outbox:event:{event_id}")
    pipe.zrem(PENDING_KEY, event_id)
    pipe.execute()


def publish_envelope(r, producer, topic: str, envelope: dict, timeout_seconds: float) -> bool:
    event_id = envelope["payload"]["eventId"]
    customer_id = envelope["auth"]["customerId"]
    store(r, event_id, envelope)
    try:
        publish_with_ack(
            producer,
            topic=topic,
            key=customer_id.encode(),
            value=json.dumps(envelope).encode(),
            timeout_seconds=timeout_seconds,
        )
    except KafkaUnavailableError:
        return False
    remove(r, event_id)
    return True


def flush_once(r, producer, topic: str, timeout_seconds: float) -> bool:
    pending = r.zrange(PENDING_KEY, 0, 0)
    if not pending:
        return False
    event_id = pending[0]
    raw = r.get(f"gateway:outbox:event:{event_id}")
    if not raw:
        r.zrem(PENDING_KEY, event_id)
        return True
    try:
        envelope = json.loads(raw)
        publish_with_ack(
            producer,
            topic=topic,
            key=envelope["auth"]["customerId"].encode(),
            value=raw.encode(),
            timeout_seconds=timeout_seconds,
        )
    except KafkaUnavailableError as exc:
        logger.debug("Gateway outbox retry failed: %s", exc)
        return False
    except (ValueError, KeyError, TypeError) as exc:
        r.rpush(
            "gateway:outbox:dead",
            json.dumps({"eventId": event_id, "payload": raw, "error": str(exc)}),
        )
        remove(r, event_id)
        return True
    remove(r, event_id)
    return True


def start_worker(get_redis, get_producer, topic: str, timeout_seconds: float) -> None:
    def run() -> None:
        while True:
            try:
                r = get_redis()
                while flush_once(r, get_producer(), topic, timeout_seconds):
                    pass
                reap_expired_reservations(r)
            except Exception as exc:
                logger.debug("Gateway outbox worker error: %s", exc)
            time.sleep(1)

    threading.Thread(target=run, name="fluxmeter-gateway-outbox", daemon=True).start()
