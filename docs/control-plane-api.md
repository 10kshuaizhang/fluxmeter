# Control Plane API Reference

Base URL: `http://localhost:8001` (development, `make start-saas`)

Interactive docs: `GET /docs`

The control plane manages **tenants** for multi-tenant SaaS deployments. It shares Redis with the main FluxMeter API (`:8000`) for plan limits and usage counters.

---

## Authentication

All endpoints except `/health` require the `X-Admin-Key` header.

| Key | Env var | Default (dev) |
|-----|---------|---------------|
| Control plane admin | `CP_ADMIN_KEY` | `cp_admin_test_key` |

**Errors:** `403` invalid admin key

---

## Health

### `GET /health`

**Response:** `200 OK`
```json
{"status": "ok", "service": "control-plane"}
```

---

## Tenants

### `POST /tenants`

Create a tenant with plan limits and API key.

**Request body:**
```json
{
  "name": "Acme Corp",
  "email": "billing@acme.example",
  "plan": "free",
  "stripe_customer_id": "cus_optional"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Display name (1–200 chars) |
| `email` | string | Yes | Contact email |
| `plan` | enum | No | `free`, `growth`, `scale`, `enterprise` (default: `free`) |
| `stripe_customer_id` | string | No | Optional Stripe Customer ID for future billing wiring |

**Response:** `201 Created`
```json
{
  "tenant_id": "tenant_a1b2c3d4",
  "name": "Acme Corp",
  "email": "billing@acme.example",
  "plan": "free",
  "api_key": "fm_tenant_xxxxxxxx",
  "limits": {
    "max_events_per_month": 100000,
    "max_eps": 100,
    "max_customers": 10
  },
  "created_at": 1718534400.0
}
```

Store `api_key` securely — shown once only.

**Side effects:**
- Redis `cp:tenant:{tenant_id}` — tenant metadata
- Redis `tenant:{tenant_id}:max_eps`, `tenant:{tenant_id}:max_events_month` — rate limits for main API
- Redis `cp:tenants` set — tenant index

---

### `GET /tenants`

List all tenants (metadata only, no API keys).

**Response:** `200 OK`
```json
[
  {
    "tenant_id": "tenant_a1b2c3d4",
    "name": "Acme Corp",
    "email": "billing@acme.example",
    "plan": "free",
    "created_at": 1718534400.0
  }
]
```

---

### `GET /tenants/{tenant_id}/usage`

Tenant-scoped usage vs plan limits.

**Response:** `200 OK`
```json
{
  "tenant_id": "tenant_a1b2c3d4",
  "total_events": 42000,
  "total_tokens": 12500000,
  "total_cost_usd": 38.50,
  "events_this_month": 42000,
  "plan": "free",
  "limits": {
    "max_events_per_month": 100000,
    "max_eps": 100,
    "max_customers": 10
  }
}
```

**Error:** `404` if tenant not found.

Usage counters (`tenant:{tenant_id}:total_events`, etc.) are populated when events include `tenantId` and flow through Flink full mode with tenant-scoped Redis keys.

---

### `DELETE /tenants/{tenant_id}`

Delete tenant metadata and rate-limit keys.

**Response:** `200 OK`
```json
{"deleted": true, "tenant_id": "tenant_a1b2c3d4"}
```

**Error:** `404` if tenant not found.

Does not delete aggregated usage data under `tenant:{id}:customer:*` keys.

---

## Plan tiers

| Plan | max_events_per_month | max_eps | max_customers |
|------|---------------------|---------|---------------|
| `free` | 100,000 | 100 | 10 |
| `growth` | 10,000,000 | 10,000 | 1,000 |
| `scale` | 100,000,000 | 100,000 | 10,000 |
| `enterprise` | unlimited (-1) | unlimited (-1) | unlimited (-1) |

---

## Stack

Start with:

```bash
make start-saas
```

Services: main API (`:8000`), control plane (`:8001`), Redis (password-protected), Grafana (`:3000`).

Compose file: `docker-compose.saas.yml`
