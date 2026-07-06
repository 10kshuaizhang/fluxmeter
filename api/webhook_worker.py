"""Consume budget-alerts from Kafka and deliver HTTPS webhooks."""

from __future__ import annotations

import json
import logging
import os

import redis
from confluent_kafka import Consumer, KafkaError

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


def process_alert(r: redis.Redis, alert: dict) -> None:
    customer_id = alert.get("customerId")
    if not customer_id:
        return
    fire_budget_webhook(
        r,
        customer_id,
        alert.get("type", ""),
        balance_usd=alert.get("remainingBalanceUsd"),
        window_cost_usd=alert.get("windowCostUsd"),
        model_id=alert.get("modelId"),
    )


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([ALERT_TOPIC])
    r = get_redis()
    logger.info("Webhook worker started on %s", ALERT_TOPIC)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                logger.error("Kafka error: %s", msg.error())
            continue
        try:
            alert = json.loads(msg.value().decode())
            process_alert(r, alert)
        except Exception as e:
            logger.exception("Failed to process alert: %s", e)


if __name__ == "__main__":
    main()
