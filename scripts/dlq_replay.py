#!/usr/bin/env python3
"""Replay token-events-dlq messages back to token-events for reprocessing."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time

from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dlq_replay")


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay DLQ events to main topic")
    parser.add_argument("--brokers", default="localhost:9094")
    parser.add_argument("--source", default="token-events-dlq")
    parser.add_argument("--target", default="token-events")
    parser.add_argument("--group", default="fluxmeter-dlq-replay")
    parser.add_argument("--max", type=int, default=0, help="Max messages (0=all)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rate", type=int, default=1000, help="Max msgs/sec")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.brokers,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({
        "bootstrap.servers": args.brokers,
        "acks": "all",
        "linger.ms": 5,
    })
    consumer.subscribe([args.source])
    count = 0
    interval = 1.0 / max(args.rate, 1)
    logger.info("Replaying %s -> %s (dry_run=%s)", args.source, args.target, args.dry_run)

    while True:
        if args.max and count >= args.max:
            break
        msg = consumer.poll(1.0)
        if msg is None:
            if count > 0:
                break
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            logger.error("Kafka error: %s", msg.error())
            continue

        key = msg.key()
        value = msg.value()
        if args.dry_run:
            logger.info("DRY-RUN replay key=%s len=%d", key, len(value))
        else:
            producer.produce(args.target, key=key, value=value)
            producer.poll(0)
        count += 1
        consumer.commit(msg)
        time.sleep(interval)

    if not args.dry_run:
        producer.flush(10)
    consumer.close()
    logger.info("Replayed %d messages", count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
