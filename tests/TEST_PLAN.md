# FluxMeter Integration Test Plan

Tests that verify correctness in complex scenarios a billing system must handle.
Each test targets a specific failure mode that would cause financial loss.

## Test Categories

### 1. Budget Accuracy Under Concurrent Load
**Scenario:** Multiple windows fire simultaneously for the same customer across different models. Final balance must equal `initial - sum(all window costs)` exactly.

**Test:** Set budget=$100. Generate 10K events across 9 models for 1 customer. Wait for all windows. Assert: `balance = 100 - cost_usd` (within float tolerance).

### 2. Idempotency Under Replay
**Scenario:** Same window fires twice (simulating Flink restart mid-checkpoint). Counters must NOT double-count.

**Test:** Send events, wait for window to fire, record counters. Manually re-send the same events with same timestamps. Wait for another window cycle. Assert: counters unchanged (SET NX blocked the replay).

### 3. Rate Limit Boundary Precision
**Scenario:** Exactly `max_rpm` requests allowed, `max_rpm + 1` denied. Window rolls over after 60s.

**Test:** Set max_rpm=10. Send exactly 10 check requests (all allowed). Send 11th (denied). Wait 60s. Send 1 more (allowed again).

### 4. Budget Reserve/Reconcile Accuracy
**Scenario:** Reserve $5, actual cost is $2. Balance must be `original - 2` (not `original - 5`).

**Test:** Set budget=$50. Reserve $5 (balance→$45). Reconcile with actual=$2 (credit back $3, balance→$48). Assert final balance = $48.

### 5. Multi-Model Cost Correctness
**Scenario:** Same token count, different models, must produce different costs matching the pricing table exactly.

**Test:** Send 1M input tokens + 1M output tokens for each of 9 models. Assert cost matches expected:
- gpt-4o: $2.50 + $10.00 = $12.50
- gpt-4o-mini: $0.15 + $0.60 = $0.75
- o1: $15.00 + $60.00 = $75.00
- claude-opus-4: $15.00 + $75.00 = $90.00
- etc.

### 6. Re-Rating Correctness
**Scenario:** After aggregation, change GPT-4o output price from $10→$5. All customers with GPT-4o output usage must get exactly the right credit.

**Test:** Generate events for 3 customers on gpt-4o. Record costs. Apply re-rate (old=10, new=5). Assert: each customer's cost decreased by exactly `output_tokens / 1M * 5`.

### 7. Span Attribution Completeness
**Scenario:** An agent makes 5 LLM calls across 3 different models, all linked to one parentSpanId. Span aggregate must sum all 5 calls' costs.

**Test:** Send 5 events with same parentSpanId, different models. Wait for session window (60s gap). Assert: span cost = sum of individual costs, call_count = 5.

### 8. HTTP Ingest → Full Pipeline Consistency
**Scenario:** Events sent via HTTP ingest must produce identical results to events sent via direct Kafka producer.

**Test:** Send identical event payloads via both paths. Wait for windows. Assert: both customers have identical counters.

### 9. Budget Alert Ordering
**Scenario:** BUDGET_LOW must fire before BUDGET_EXHAUSTED. No EXHAUSTED without prior LOW (unless single window exceeds both threshold and balance).

**Test:** Set budget=$10, threshold=$3. Generate events slowly. Capture Kafka alerts in order. Assert: first alert is BUDGET_LOW, subsequent is BUDGET_EXHAUSTED.

### 10. Zero-Token Event Handling
**Scenario:** Events with all token fields = 0 must not crash, must still count as events, must not affect cost.

**Test:** Send 100 events with all tokens=0. Assert: event_count incremented, cost_usd unchanged (remains 0), no errors.
