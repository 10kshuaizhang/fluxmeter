from __future__ import annotations

import time

import redis

from intelligence.revenue_store import set_revenue


def import_openmeter_events(r: redis.Redis, payload: dict, *, period: str) -> dict[str, int]:
    revenue_rows = 0
    ignored_rows = 0

    for event in payload.get("events", []):
        subject = str(event.get("subject", "")).lower()
        if subject == "revenue":
            set_revenue(
                r,
                event["customerId"],
                period,
                revenue_usd=float(event["value"]),
                source="openmeter",
            )
            revenue_rows += 1
        else:
            ignored_rows += 1

    r.set(f"intel:overlay:openmeter:{period}:imported_at", str(int(time.time())))

    return {"revenue_rows": revenue_rows, "ignored_rows": ignored_rows}
