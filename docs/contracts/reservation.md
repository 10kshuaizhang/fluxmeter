# Reservation lifecycle contract

State machine shared by SDK `POST /budget/.../reserve|reconcile` and Gateway async holds.
Python and Java are thin adapters over the same Redis keys; this document is the source of truth.

## States

| State | Meaning |
|-------|---------|
| `open` | Hold applied (`held_usd` increased); `reservation:{id}` hash exists; scored in `gateway:reservations:pending` |
| `attached` | `reservationId` is in `window:reservations:{windowId}` (EventProjectionSink) |
| `settled` | Window Billing sink released hold and deleted reservation (or SDK `reconcile` / `settle`) |
| `expired` | `expire` / reaper released hold after deadline |

## Transitions

1. **open** — `reserve_hold` then `register_gateway_reservation` (Gateway) or SDK reserve alone.
2. **attach_to_window** — projection sink `SADD window:reservations:{windowId}`.
3. **settle_by_window** — BudgetEnforcerSink Lua releases `held_usd` for each id in the set, `DEL reservation:{id}`, `ZREM` pending.
4. **expire** — only via `budget_ops.expire_reservations` (workers must not invent a second path).
5. **settle (no usage)** — `settle_gateway_reservation` when upstream fails before ingest.

## Keys

- `reservation:{reservationId}` — hash: `customer_id`, `tenant_id`, `reserved_usd`, `parent_span_id`
- `gateway:reservations:pending` — ZSET score = expiry epoch seconds
- `window:reservations:{windowId}` — SET of reservation ids
- Hold counter: `{budget_prefix}:held_usd` where `budget_prefix` follows `TenantKeys.budgetPrefix` / `tenant_keys.budget_prefix`

## Invariants

- Duplicate settle / expire is a no-op (0 released).
- SDK reconcile and Gateway window settle both reduce `held_usd` and must not double-release beyond the reserved amount.
- `eventId` for Gateway custody is `res:{reservationId}` when a reservation exists.
