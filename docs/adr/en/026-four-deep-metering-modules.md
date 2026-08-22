# ADR-026: Four deep metering modules

**Status:** Accepted  
**Date:** 2026-08-22  
**Version:** 4.6.0

## Context

Custody, pricing, reservations, and budgets existed, but callers still assembled their state machines from shallow helpers. HTTP and Gateway code knew topic/TTL details, Gateway estimated pricing through a pass-through module, Budget cache/rate-limit identities omitted tenant, and Gateway created a hold before separately registering its Reservation. That last ordering left an orphan-hold crash window.

## Decision

The metering runtime has four explicit deep modules:

1. `TokenEventCustody` owns identity, envelope, Kafka delivery, quarantine, backpressure, and reconciliation behind `accept` / `accept_many`.
2. `PricingCatalog` owns catalog validation, normalization, exact quotes, tier traversal, and advisory completion estimates.
3. `Reservation` owns every hold transition. Gateway `open` reserves and registers atomically in one Redis Lua operation; settlement and expiry are idempotent.
4. `Budget` owns configuration, top-up, snapshots, authorization, cache fallback, rate limiting, hierarchy caps, and API-key caps. Cache and RPM identity are tenant-scoped.

HTTP routes and Gateway orchestration are adapters. Fakeredis and fake Kafka producers exercise the same interfaces used by production callers. The removed `budget_gate`, `budget_ops`, and Gateway pricing-estimate modules must not be recreated as compatibility layers.

## Consequences

- State-machine changes have one locality and one interface-level test surface.
- Same customer/span/session IDs cannot share Budget cache, RPM, or new scope keys across tenants.
- A process crash cannot occur between Gateway hold creation and Reservation registration.
- Pricing validation and runtime quotes cannot drift within the Python runtime.
- Redis Lua remains a single-node implementation constraint; a future distributed adapter must preserve these interfaces and atomic transitions.

