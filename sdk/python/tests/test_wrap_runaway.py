"""CI-runnable runaway-agent regression for wrap() (mock upstream, no provider key).

A $0.30 budget and $0.03 estimate must deny after 10 admits when wrap() holds
through settlement instead of only checking or reconciling immediately after ingest.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from fluxmeter import BudgetExceededError, wrap


IN, OUT = 4000, 2000  # gpt-4o: 4000*2.5e-6 + 2000*1e-5 = $0.03 per call
PER_CALL = 0.03
BUDGET = 0.30


class HoldMeter:
    """In-process meter that keeps held_usd until Flink-style settlement."""

    def __init__(self, budget: float = BUDGET):
        self._api_url = "http://mock"
        self.budget = budget
        self.held = 0.0
        self.reserve_calls = 0
        self.reconcile_calls = 0
        self.track_calls = 0
        self._next_id = 0

    def check(self, customer_id, estimated_cost_usd, **kwargs):
        effective = self.budget - self.held
        if effective < estimated_cost_usd:
            return {
                "allowed": False,
                "reason": "insufficient_balance",
                "held_usd": self.held,
                "effective_balance_usd": effective,
            }
        return {"allowed": True, "reason": "ok", "held_usd": self.held}

    def reserve(self, customer_id, estimated_cost_usd, **kwargs):
        self.reserve_calls += 1
        effective = self.budget - self.held
        if effective < estimated_cost_usd:
            return {
                "allowed": False,
                "reason": "insufficient_balance",
                "held_usd": self.held,
                "effective_balance_usd": effective,
            }
        self.held += estimated_cost_usd
        self._next_id += 1
        return {
            "allowed": True,
            "reserved_usd": estimated_cost_usd,
            "reservation_id": f"res-{self._next_id}",
            "held_usd": self.held,
        }

    def reconcile(self, customer_id, reserved_usd, actual_usd=0.0, **kwargs):
        self.reconcile_calls += 1
        self.held = max(0.0, self.held - reserved_usd)
        return {"released_usd": reserved_usd, "held_usd": self.held}

    def track_openai(self, customer_id, response, **kwargs):
        self.track_calls += 1
        return SimpleNamespace(event_id="evt")

    def track(self, *args, **kwargs):
        self.track_calls += 1
        return SimpleNamespace(event_id="evt")


def _completion():
    return SimpleNamespace(
        id="cmpl-mock",
        model="gpt-4o",
        usage=SimpleNamespace(
            prompt_tokens=IN,
            completion_tokens=OUT,
            prompt_tokens_details=None,
            completion_tokens_details=None,
        ),
    )


def _run_loop(meter, *, interval_s: float = 0.0, max_calls: int = 100):
    calls = 0
    original = lambda **kwargs: _completion()
    client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=original))
    )
    wrap(
        client,
        meter,
        "runaway",
        fail_open=False,
        estimated_cost_usd=PER_CALL,
    )
    blocked = None
    for _ in range(max_calls):
        try:
            client.chat.completions.create(
                model="gpt-4o", messages=[{"role": "user", "content": "loop"}]
            )
            calls += 1
        except BudgetExceededError as exc:
            blocked = exc
            break
        if interval_s:
            # Interval is modeled as "time between calls"; no real sleep in CI.
            pass
    return calls, blocked


def test_runaway_tight_loop_stops_at_budget():
    meter = HoldMeter()
    calls, blocked = _run_loop(meter, interval_s=0.0)
    assert blocked is not None
    assert blocked.gate["reason"] == "insufficient_balance"
    # $0.03 * 9 = $0.27; binary float may deny the 10th $0.03 against a $0.30 cap.
    assert 9 <= calls <= 10
    assert calls * PER_CALL <= BUDGET + 1e-9
    assert meter.reserve_calls == calls
    assert meter.reconcile_calls == 0
    assert meter.track_calls == calls
    assert meter.held <= BUDGET + 1e-9


def test_runaway_one_call_per_tick_stops_at_budget():
    meter = HoldMeter()
    calls, blocked = _run_loop(meter, interval_s=1.0)
    assert blocked is not None
    assert 9 <= calls <= 10
    assert calls * PER_CALL <= BUDGET + 1e-9
    assert meter.reconcile_calls == 0


def test_immediate_reconcile_would_overspend_this_fixture():
    """Guard the leak the audit found: releasing the hold after ingest admits unbounded calls."""

    class LeakyMeter(HoldMeter):
        def track_openai(self, customer_id, response, **kwargs):
            super().track_openai(customer_id, response, **kwargs)
            reserved = kwargs.get("reserved_usd") or PER_CALL
            self.reconcile(customer_id, reserved)

    meter = LeakyMeter()
    calls, blocked = _run_loop(meter, max_calls=50)
    assert blocked is None
    assert calls == 50
