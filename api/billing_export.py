"""Billing export — periodic usage delta to Stripe / Metronome / Orb.

Env:
  BILLING_EXPORT_TARGETS=stripe,metronome,orb   (default: stripe)
  STRIPE_EXPORT_MODE=events|cost                (default: events)
  BILLING_EXPORT_PERIOD=hourly|monthly          (default: hourly)
  STRIPE_METER_NAME / STRIPE_COST_METER_NAME
  METRONOME_API_TOKEN / METRONOME_BILLABLE_METRIC (default: token_usage)
  ORB_API_KEY / ORB_EVENT_NAME (default: token_usage)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
import redis

from pricing_loader import billing_period_month

logger = logging.getLogger(__name__)

STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")
METRONOME_API_TOKEN = os.getenv("METRONOME_API_TOKEN")
ORB_API_KEY = os.getenv("ORB_API_KEY")
EXPORT_INTERVAL_SEC = int(os.getenv("BILLING_EXPORT_INTERVAL", "3600"))
STRIPE_EXPORT_MODE = os.getenv("STRIPE_EXPORT_MODE", "events").lower()
BILLING_EXPORT_PERIOD = os.getenv("BILLING_EXPORT_PERIOD", "hourly").lower()
METER_EVENT_NAME = os.getenv("STRIPE_METER_NAME", "token_events_processed")
COST_METER_EVENT_NAME = os.getenv("STRIPE_COST_METER_NAME", "token_cost_usd_cents")
METRONOME_BILLABLE_METRIC = os.getenv("METRONOME_BILLABLE_METRIC", "token_usage")
ORB_EVENT_NAME = os.getenv("ORB_EVENT_NAME", "token_usage")
METRONOME_API = os.getenv("METRONOME_API", "https://api.metronome.com/v1")
ORB_API = os.getenv("ORB_API", "https://api.withorb.com/v1")

_PLATFORM_KEYS = {
    "stripe": "stripe_customer_id",
    "metronome": "metronome_customer_id",
    "orb": "orb_customer_id",
}

stripe = None
if STRIPE_API_KEY:
    try:
        import stripe as _stripe

        _stripe.api_key = STRIPE_API_KEY
        stripe = _stripe
    except ImportError:
        logger.warning("stripe package not installed; Stripe export disabled")


def export_targets() -> list[str]:
    raw = os.getenv("BILLING_EXPORT_TARGETS", "stripe")
    return [t.strip().lower() for t in raw.split(",") if t.strip()]


def _export_period_key() -> str:
    return f"billing:export:last_period:{BILLING_EXPORT_PERIOD}"


def should_run_export_cycle(r: redis.Redis) -> bool:
    """For monthly mode, export at most once per UTC calendar month."""
    if BILLING_EXPORT_PERIOD != "monthly":
        return True
    current = billing_period_month(int(time.time() * 1000))
    last = r.get(_export_period_key())
    return last != current


def mark_export_cycle(r: redis.Redis) -> None:
    if BILLING_EXPORT_PERIOD == "monthly":
        r.set(_export_period_key(), billing_period_month(int(time.time() * 1000)))


def link_customer_platform(
    r: redis.Redis, customer_id: str, platform: str, external_customer_id: str
) -> None:
    """Link FluxMeter customer to an external billing platform ID."""
    platform = platform.lower()
    if platform not in _PLATFORM_KEYS:
        raise ValueError(f"unsupported platform: {platform}")
    r.set(f"billing:{customer_id}:{_PLATFORM_KEYS[platform]}", external_customer_id)


def link_customer_stripe(r: redis.Redis, customer_id: str, stripe_customer_id: str):
    """Link a FluxMeter customer to a Stripe customer for billing export."""
    link_customer_platform(r, customer_id, "stripe", stripe_customer_id)


def discover_billable_customers(r: redis.Redis) -> list[str]:
    """Find customers linked to at least one billing platform."""
    customers: set[str] = set()
    for platform in _PLATFORM_KEYS:
        suffix = _PLATFORM_KEYS[platform]
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match=f"billing:*:{suffix}", count=200)
            for key in keys:
                parts = key.split(":")
                if len(parts) >= 3:
                    customers.add(parts[1])
            if cursor == 0:
                break
    return sorted(customers)


def collect_customer_usage(r: redis.Redis, customer_id: str) -> Optional[dict]:
    """Collect usage delta for one customer since last report."""
    stripe_cid = r.get(f"billing:{customer_id}:stripe_customer_id")
    metronome_cid = r.get(f"billing:{customer_id}:metronome_customer_id")
    orb_cid = r.get(f"billing:{customer_id}:orb_customer_id")
    if not any([stripe_cid, metronome_cid, orb_cid]):
        return None

    total_events = int(r.get(f"customer:{customer_id}:event_count") or 0)
    last_reported = int(r.get(f"billing:{customer_id}:last_reported_events") or 0)
    input_tokens = int(r.get(f"customer:{customer_id}:input_tokens") or 0)
    output_tokens = int(r.get(f"customer:{customer_id}:output_tokens") or 0)
    last_input = int(r.get(f"billing:{customer_id}:last_reported_input_tokens") or 0)
    last_output = int(r.get(f"billing:{customer_id}:last_reported_output_tokens") or 0)
    cost_usd = float(r.get(f"customer:{customer_id}:cost_usd") or 0)
    last_cost = float(r.get(f"billing:{customer_id}:last_reported_cost_usd") or 0)

    return {
        "customer_id": customer_id,
        "stripe_customer_id": stripe_cid,
        "metronome_customer_id": metronome_cid,
        "orb_customer_id": orb_cid,
        "new_events": max(0, total_events - last_reported),
        "total_events": total_events,
        "new_input_tokens": max(0, input_tokens - last_input),
        "new_output_tokens": max(0, output_tokens - last_output),
        "total_input_tokens": input_tokens,
        "total_output_tokens": output_tokens,
        "total_cost_usd": cost_usd,
        "new_cost_usd": max(0.0, cost_usd - last_cost),
    }


def export_value(usage: dict) -> int:
    """Meter value for Stripe (events count or cost in USD cents)."""
    if STRIPE_EXPORT_MODE == "cost":
        return int(round(usage["new_cost_usd"] * 100))
    return usage["new_events"]


def meter_event_name() -> str:
    return COST_METER_EVENT_NAME if STRIPE_EXPORT_MODE == "cost" else METER_EVENT_NAME


def idempotency_key(customer_id: str, now: int) -> str:
    """Stable dedup key per export cycle."""
    if BILLING_EXPORT_PERIOD == "monthly":
        period = billing_period_month(now * 1000)
    else:
        period = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y%m%d%H")
    return f"fluxmeter-{customer_id}-{period}-{STRIPE_EXPORT_MODE}"


def _stripe_retry(func, max_attempts: int = 3):
    delay = 1.0
    last_err = None
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as e:
            last_err = e
            status = getattr(e, "http_status", None)
            if status is not None and status not in (429, 500, 502, 503, 504):
                raise
            if attempt + 1 >= max_attempts:
                break
            time.sleep(delay)
            delay *= 2
    raise last_err  # type: ignore[misc]


def report_to_stripe(stripe_customer_id: str, event_name: str, value: int, timestamp: int):
    """Report a single meter event to Stripe. Skips if value is 0."""
    if value <= 0:
        return
    if not stripe:
        logger.debug("Stripe not configured; skipping report")
        return

    def _create():
        stripe.billing.MeterEvent.create(
            event_name=event_name,
            payload={
                "stripe_customer_id": stripe_customer_id,
                "value": str(value),
            },
            timestamp=timestamp,
        )

    _stripe_retry(_create)


def report_to_metronome(
    metronome_customer_id: str,
    usage: dict,
    *,
    timestamp_iso: str,
    transaction_id: str,
) -> None:
    if not METRONOME_API_TOKEN:
        logger.debug("METRONOME_API_TOKEN not set; skipping")
        return
    if STRIPE_EXPORT_MODE == "cost":
        if usage["new_cost_usd"] <= 0:
            return
        properties = {"total_cost_usd": usage["new_cost_usd"]}
    else:
        if usage["new_events"] <= 0 and usage["new_input_tokens"] <= 0:
            return
        properties = {
            "input_tokens": usage["new_input_tokens"],
            "output_tokens": usage["new_output_tokens"],
            "event_count": usage["new_events"],
        }
    payload = [
        {
            "customer_id": metronome_customer_id,
            "event_type": METRONOME_BILLABLE_METRIC,
            "timestamp": timestamp_iso,
            "transaction_id": transaction_id,
            "properties": properties,
        }
    ]
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            f"{METRONOME_API}/ingest",
            headers={"Authorization": f"Bearer {METRONOME_API_TOKEN}"},
            json=payload,
        )
        resp.raise_for_status()


def report_to_orb(
    orb_customer_id: str,
    usage: dict,
    *,
    timestamp_iso: str,
    idempotency_key: str,
) -> None:
    if not ORB_API_KEY:
        logger.debug("ORB_API_KEY not set; skipping")
        return
    events = []
    if STRIPE_EXPORT_MODE == "cost":
        cents = int(round(usage["new_cost_usd"] * 100))
        if cents <= 0:
            return
        events.append(
            {
                "idempotency_key": f"{idempotency_key}-cost",
                "external_customer_id": orb_customer_id,
                "event_name": ORB_EVENT_NAME,
                "timestamp": timestamp_iso,
                "properties": {"cost_usd_cents": cents},
            }
        )
    else:
        if usage["new_input_tokens"] > 0:
            events.append(
                {
                    "idempotency_key": f"{idempotency_key}-input",
                    "external_customer_id": orb_customer_id,
                    "event_name": ORB_EVENT_NAME,
                    "timestamp": timestamp_iso,
                    "properties": {
                        "token_type": "input",
                        "tokens": usage["new_input_tokens"],
                    },
                }
            )
        if usage["new_output_tokens"] > 0:
            events.append(
                {
                    "idempotency_key": f"{idempotency_key}-output",
                    "external_customer_id": orb_customer_id,
                    "event_name": ORB_EVENT_NAME,
                    "timestamp": timestamp_iso,
                    "properties": {
                        "token_type": "output",
                        "tokens": usage["new_output_tokens"],
                    },
                }
            )
        if not events:
            return
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            f"{ORB_API}/ingest",
            headers={"Authorization": f"Bearer {ORB_API_KEY}"},
            json={"events": events},
        )
        resp.raise_for_status()


def _has_exportable_delta(usage: dict) -> bool:
    if STRIPE_EXPORT_MODE == "cost":
        return usage["new_cost_usd"] > 0
    return (
        usage["new_events"] > 0
        or usage["new_input_tokens"] > 0
        or usage["new_output_tokens"] > 0
    )


def mark_customer_reported(r: redis.Redis, usage: dict) -> None:
    cid = usage["customer_id"]
    r.set(f"billing:{cid}:last_reported_events", str(usage["total_events"]))
    r.set(f"billing:{cid}:last_reported_cost_usd", str(usage["total_cost_usd"]))
    r.set(f"billing:{cid}:last_reported_input_tokens", str(usage["total_input_tokens"]))
    r.set(f"billing:{cid}:last_reported_output_tokens", str(usage["total_output_tokens"]))


def export_customer_to_targets(
    r: redis.Redis,
    usage: dict,
    targets: list[str],
    *,
    now: int,
) -> int:
    """Export one customer to enabled targets. Returns count of successful platform reports."""
    if not _has_exportable_delta(usage):
        return 0

    ts_iso = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
    txn_id = idempotency_key(usage["customer_id"], now)
    reported = 0
    errors: list[str] = []

    if "stripe" in targets and usage.get("stripe_customer_id"):
        try:
            value = export_value(usage)
            if value > 0:
                report_to_stripe(
                    stripe_customer_id=usage["stripe_customer_id"],
                    event_name=meter_event_name(),
                    value=value,
                    timestamp=now,
                )
                reported += 1
        except Exception as e:
            errors.append(f"stripe:{e}")
            logger.error("Stripe export failed for %s: %s", usage["customer_id"], e)

    if "metronome" in targets and usage.get("metronome_customer_id"):
        try:
            report_to_metronome(
                usage["metronome_customer_id"],
                usage,
                timestamp_iso=ts_iso,
                transaction_id=txn_id,
            )
            reported += 1
        except Exception as e:
            errors.append(f"metronome:{e}")
            logger.error("Metronome export failed for %s: %s", usage["customer_id"], e)

    if "orb" in targets and usage.get("orb_customer_id"):
        try:
            report_to_orb(
                usage["orb_customer_id"],
                usage,
                timestamp_iso=ts_iso,
                idempotency_key=txn_id,
            )
            reported += 1
        except Exception as e:
            errors.append(f"orb:{e}")
            logger.error("Orb export failed for %s: %s", usage["customer_id"], e)

    if reported > 0:
        mark_customer_reported(r, usage)
    elif errors:
        logger.warning("All export targets failed for %s: %s", usage["customer_id"], "; ".join(errors))
    return reported


def _export_enabled() -> bool:
    targets = export_targets()
    if "stripe" in targets and STRIPE_API_KEY:
        return True
    if "metronome" in targets and METRONOME_API_TOKEN:
        return True
    if "orb" in targets and ORB_API_KEY:
        return True
    return False


async def billing_export_loop(r: redis.Redis):
    """Background loop: report usage deltas to configured billing platforms."""
    targets = export_targets()
    if not _export_enabled():
        logger.info(
            "Billing export disabled (targets=%s; set API keys for enabled targets)",
            targets,
        )
        return

    logger.info(
        "Billing export started (interval=%ds, mode=%s, period=%s, targets=%s)",
        EXPORT_INTERVAL_SEC,
        STRIPE_EXPORT_MODE,
        BILLING_EXPORT_PERIOD,
        targets,
    )

    while True:
        try:
            await asyncio.sleep(EXPORT_INTERVAL_SEC)
            if not should_run_export_cycle(r):
                continue

            now = int(time.time())
            customers = discover_billable_customers(r)
            reported_customers = 0

            for cid in customers:
                usage = collect_customer_usage(r, cid)
                if not usage:
                    continue
                n = export_customer_to_targets(r, usage, targets, now=now)
                if n > 0:
                    reported_customers += 1

            if reported_customers > 0:
                mark_export_cycle(r)
                logger.info(
                    "Reported usage for %d customers (targets=%s)",
                    reported_customers,
                    targets,
                )

        except Exception as e:
            logger.error("Billing export error: %s", e)
            await asyncio.sleep(30)
