"""Consume budget-alerts from Kafka and deliver HTTPS webhooks."""

from __future__ import annotations

import hashlib
import json
import logging
import os

import redis
from confluent_kafka import Consumer, KafkaError, Producer

from webhook_deliver import fire_budget_webhook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webhook_worker")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "budget-alerts")
GROUP_ID = os.getenv("WEBHOOK_CONSUMER_GROUP", "fluxmeter-webhook-worker")


def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )


def process_alert(r: redis.Redis, alert: dict) -> bool:
    customer_id = alert.get("customerId")
    if not customer_id:
        return False
    identity = "|".join(
        str(alert.get(field, ""))
        for field in ("customerId", "type", "windowStart", "warnPct")
    )
    transition_key = "webhook:transition:" + hashlib.sha256(identity.encode()).hexdigest()
    if not r.set(transition_key, "1", nx=True, ex=30 * 24 * 60 * 60):
        return False
    delivered = fire_budget_webhook(
        r,
        customer_id,
        alert.get("type", ""),
        balance_usd=alert.get("remainingBalanceUsd"),
        window_cost_usd=alert.get("windowCostUsd"),
        model_id=alert.get("modelId"),
        warn_pct=alert.get("warnPct"),
        spent_pct=alert.get("spentPct"),
        initial_balance_usd=alert.get("initialBalanceUsd"),
    )
    if not delivered:
        r.delete(transition_key)
        raise RuntimeError("budget webhook delivery failed")
    return True


def replay_pending_alerts(r: redis.Redis, producer: Producer, limit: int = 10) -> None:
    """Move Redis-backed alert outbox entries into Kafka with broker acknowledgement."""
    for _ in range(limit):
        raw = r.lindex("budget-alerts:pending", 0)
        if raw is None:
            return
        entry = json.loads(raw)
        producer.produce(entry["topic"], key=entry["key"], value=entry["payload"])
        if producer.flush(5) != 0:
            return
        r.lpop("budget-alerts:pending")


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BROKERS, "acks": "all"})
    consumer.subscribe([ALERT_TOPIC])
    r = get_redis()
    logger.info("Webhook worker started on %s", ALERT_TOPIC)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            replay_pending_alerts(r, producer)
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                logger.error("Kafka error: %s", msg.error())
            continue
        try:
            alert = json.loads(msg.value().decode())
            process_alert(r, alert)
            consumer.commit(message=msg, asynchronous=False)
        except Exception as e:
            logger.exception("Failed to process alert: %s", e)


if __name__ == "__main__":
    main()
