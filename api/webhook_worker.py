"""Consume budget-alerts from Kafka and deliver HTTPS webhooks."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time

import httpx
import redis
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webhook_worker")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "budget-alerts")
GROUP_ID = os.getenv("WEBHOOK_CONSUMER_GROUP", "fluxmeter-webhook-worker")
MAX_RETRIES = int(os.getenv("WEBHOOK_MAX_RETRIES", "3"))


def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )


def sign_payload(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def deliver_webhook(url: str, secret: str, payload: dict) -> bool:
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if secret:
        headers["X-FluxMeter-Signature"] = sign_payload(secret, body)
    for attempt in range(MAX_RETRIES):
        try:
            resp = httpx.post(url, content=body, headers=headers, timeout=10.0)
            if resp.status_code < 500:
                return resp.status_code < 400
        except httpx.HTTPError as e:
            logger.warning("Webhook attempt %d failed: %s", attempt + 1, e)
        time.sleep(2 ** attempt)
    return False


def process_alert(r: redis.Redis, alert: dict) -> None:
    customer_id = alert.get("customerId")
    if not customer_id:
        return
    alert_type = alert.get("type", "")
    if alert_type not in ("BUDGET_LOW", "BUDGET_EXHAUSTED"):
        return

    url = r.get(f"budget:{customer_id}:webhook_url")
    if not url:
        return
    secret = r.get(f"budget:{customer_id}:webhook_secret") or ""

    payload = {
        "type": alert_type,
        "customer_id": customer_id,
        "balance_usd": alert.get("remainingBalanceUsd"),
        "window_cost_usd": alert.get("windowCostUsd"),
        "model_id": alert.get("modelId"),
        "timestamp": alert.get("timestamp"),
    }
    ok = deliver_webhook(url, secret, payload)
    if not ok:
        logger.error("Webhook delivery failed for %s %s", customer_id, alert_type)
        r.incr("metrics:webhook_delivery_failed")


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
