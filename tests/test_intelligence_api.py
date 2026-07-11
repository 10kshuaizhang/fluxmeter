import os

os.environ["FLUXMETER_AUTH_OPTIONAL"] = "true"

import sys

sys.path.insert(0, "api")

import fakeredis
from fastapi.testclient import TestClient
from usage_buckets import model_period_key, rollup_month_key


def _seed_root_cause_data(r: fakeredis.FakeRedis) -> None:
    for period, cost in [("2026-06", "100"), ("2026-07", "140")]:
        r.hset(
            rollup_month_key("c1", period),
            mapping={
                "cost_usd": cost,
                "event_count": "1",
                "total_tokens": "10",
                "input_tokens": "5",
                "output_tokens": "5",
            },
        )
    r.hset(
        model_period_key("c1", "gpt-4o", "2026-06"),
        mapping={
            "cost_usd": "60",
            "event_count": "1",
            "total_tokens": "10",
            "input_tokens": "5",
            "output_tokens": "5",
        },
    )
    r.hset(
        model_period_key("c1", "gpt-4o", "2026-07"),
        mapping={
            "cost_usd": "100",
            "event_count": "1",
            "total_tokens": "10",
            "input_tokens": "5",
            "output_tokens": "5",
        },
    )


def test_root_cause_endpoint(monkeypatch):
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr("main.get_redis", lambda: fake)
    _seed_root_cause_data(fake)

    from main import app

    client = TestClient(app)
    resp = client.get(
        "/intelligence/root-cause",
        params={"period": "2026-07", "baseline_period": "2026-06"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "summary" in body
    assert "contributors" in body
    assert body["delta_usd"] == 40.0
    assert len(body["contributors"]) > 0


def test_simulate_model_switch_endpoint(monkeypatch):
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr("main.get_redis", lambda: fake)

    from main import app

    client = TestClient(app)
    resp = client.post(
        "/intelligence/simulate",
        json={
            "scenario": "model_switch",
            "input_tokens": 1_000_000,
            "output_tokens": 500_000,
            "from_model": "gpt-4o",
            "to_model": "claude-sonnet-4",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["scenario"] == "model_switch"
    assert body["annual_savings_usd"] is not None
    assert body["notes"]
