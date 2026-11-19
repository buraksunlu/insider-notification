# Event-Driven Notification System — Design

Status: draft for approval · 2026-11-19

## 1. Intent

Insider One assessment: a Go service that accepts notification requests (SMS / email / push),
processes them asynchronously through queue workers, delivers them to an external provider
(webhook.site), retries intelligently and exposes status, metrics and logs.
The reviewer will judge correctness under failure (retry, idempotency, cancel, crash recovery),
code clarity and commit history more than raw feature count. Budget ≈ 12 h, deadline 2026-11-21.

### Goals
- All functional requirements (API, processing engine, delivery/retry, observability).
- All six deliverables: clean history, README, `docker compose up`, OpenAPI, versioned migrations, single-command tests.
- Bonuses in order: failure handling (falls out of the design), GitHub Actions CI, scheduled notifications, templates (if time allows).

### Non-goals
Auth / multi-tenancy, real SMS/email vendors, WebSocket updates, distributed tracing,
horizontally scaled rate limiting (documented as next step, not built).

### Anti-goals (would be a failure even if it "works")
- A notification sent twice because of a redelivery or a duplicate API call.
- A cancelled notification still being sent.
- A notification silently lost when a process crashes.
- Messy commit history / AI attribution in commits.

## 2. Approaches considered

| | Approach | Trade-off |
|---|---|---|
| A | PostgreSQL only (`FOR UPDATE SKIP LOCKED` as queue) | Fewest moving parts, strongest consistency; weak "event-driven / burst traffic" story, DB carries queue load. |
| B | PostgreSQL + Redis (sorted sets, delayed ZSET) | Redis also does rate limit + idempotency; no ack semantics → needs custom recovery. |
| **C** | **PostgreSQL + RabbitMQ** | **Chosen.** Native message priority, acks, dead-letter exchange; clearest event-driven story. Costs more code and one more container. |

Kafka was excluded: no native priority, heavy for the scope.

## 3. Architecture

```
            ┌───────────┐  INSERT (status=pending)   ┌────────────┐
 client ──▶ │  api      │ ─────────────────────────▶ │ PostgreSQL │ ◀── source of truth
            └───────────┘  NOTIFY new_notification   └────────────┘
                                                        ▲   │ claim due rows (SKIP LOCKED)
                                                        │   ▼
                         ┌──────────────────── worker process ─────────────────────┐
                         │ relay ──publish(priority)──▶ RabbitMQ  ──▶ consumers    │
                         │ (outbox + scheduler + retry scheduler)     (per channel) │
                         │ sweeper (stuck "processing" → pending)        │          │
                         └───────────────────────────────────────────────│──────────┘
                                                                         ▼
                                                     rate limiter (100/s/channel)
                                                                         ▼
                                                               provider (webhook.site)
```

Two binaries from one module: `cmd/api`, `cmd/worker`.

### Key decision: the DB is the outbox, RabbitMQ is the delivery pipe
The API never publishes to RabbitMQ directly. It only inserts rows (`status=pending`) in one
transaction and fires `NOTIFY`. The worker's **relay** loop claims due rows
(`status='pending' AND next_attempt_at <= now()`, `FOR UPDATE SKIP LOCKED`, batch of 500),
publishes them with publisher confirms, then marks them `queued`.
Woken by `LISTEN/NOTIFY`, with a 1 s polling fallback.

One path therefore covers: crash-safe enqueue (transactional outbox), **scheduled notifications**
(`next_attempt_at = scheduled_at`), and **retry with backoff** (`next_attempt_at = now + backoff`).

> Change from yesterday's sketch: retries are scheduled through the DB (`next_attempt_at`)
> instead of TTL delay queues in RabbitMQ. Reason: exact per-attempt backoff with jitter,
> no queue-per-delay zoo, and retries/scheduling share the same code path.

## 4. Data model (migrations via golang-migrate, `migrations/`)

`notifications`
| column | notes |
|---|---|
| id uuid PK | also sent to provider as idempotency key |
| batch_id uuid NULL FK | |
| idempotency_key text NULL, UNIQUE | client-supplied |
| channel enum(sms, email, push) | |
| recipient text, content text | |
| priority enum(high, normal, low) | |
| status enum(pending, queued, processing, sent, failed, cancelled) | |
| attempts int, max_attempts int (default 8) | |
| next_attempt_at timestamptz | due time (scheduling + retry) |
| scheduled_at timestamptz NULL | as requested by client |
| provider_message_id text NULL, last_error text NULL | |
| correlation_id text | |
| created_at, updated_at, sent_at | |

Indexes: `(status, next_attempt_at)` for the relay; `(created_at, id)` for keyset listing;
`(batch_id)`; `(channel, status, created_at)` for filters.

`batches` (id, total, created_at) · `delivery_attempts` (notification_id, attempt, http_status,
error, latency_ms, created_at) — attempt history for debugging.
`templates` (bonus, own migration): id, name UNIQUE, channel, body with `{{var}}` placeholders.

### State machine
```
pending ──relay──▶ queued ──consumer──▶ processing ──2xx──▶ sent
   ▲                                       │
   └──── retryable error, attempts < max ──┤
                                           └── permanent error / attempts exhausted ──▶ failed (+ DLQ)
pending | queued ──cancel──▶ cancelled
```

## 5. RabbitMQ topology
- Direct exchange `notifications`; queues `notifications.sms|email|push`, durable,
  `x-max-priority=3` (high=3, normal=2, low=1), dead-letter exchange `notifications.dlx`.
- `notifications.dlq` bound to the DLX: receives permanently failed notifications
  (published explicitly) and poison messages (`nack requeue=false`).
- Consumers: per-channel, prefetch small (≈ 10) so priority actually reorders a backlog.

## 6. Processing (consumer)
1. **Claim guard**: `UPDATE notifications SET status='processing', attempts=attempts+1
   WHERE id=$1 AND status='queued' RETURNING …`. No row → already cancelled / duplicate delivery → ack and skip.
   This is the worker-side idempotency + cancel guarantee.
2. **Rate limit**: per-channel token bucket (`golang.org/x/time/rate`, 100/s, burst 100), `Wait(ctx)`.
3. **Deliver**: POST `{to, channel, content}` to `PROVIDER_URL`, timeout 5 s,
   headers `Idempotency-Key: <id>`, `X-Correlation-ID`.
4. **Classify**:
   - 2xx → `sent`, store `messageId`.
   - 408, 429, 5xx, timeout, network error, cancellation on shutdown → retryable.
   - other 4xx and leftover 3xx (redirects are already followed by the client) → permanent.
5. Retryable and `attempts < max_attempts` → `pending`, `next_attempt_at = now + full-jitter
   backoff` (base 2 s, ×2 per attempt, cap 5 min; honour `Retry-After` on 429, capped at 5 min).
   With 8 attempts the worst-case retry window is ≈ 4 min (2+4+…+128 s), long enough to ride
   out a short provider outage; 5 attempts would give up after ≈ 30 s.
   Otherwise → `failed` + publish to DLQ.
6. Write a `delivery_attempts` row, then **ack**.

Crash recovery: an unacked message is redelivered, but its row is `processing`, so the guard
would skip it. The **sweeper** resets rows stuck in `processing` for > 2 min back to `pending`.
Delivery is therefore at-least-once toward the provider in the crash window, mitigated by the
`Idempotency-Key` header; documented in README.

Graceful shutdown: stop consuming, finish in-flight, close channels (context + `signal.NotifyContext`).

## 7. API (chi, `/api/v1`, JSON, OpenAPI 3 in `api/openapi.yaml`)
| Method & path | Behaviour |
|---|---|
| `POST /notifications` | Create one. Optional `idempotency_key`, `scheduled_at`, `template_id`+`variables`. 201; replay of the same key → 200 with the existing row. |
| `POST /notifications/batch` | 1–1000 items. Validate all first; any invalid → 422 with per-index errors, nothing stored. Otherwise single transaction → 201 `{batch_id, ids}`. |
| `GET /notifications/{id}` | Status + attempts. |
| `GET /batches/{id}` | Counts per status + paginated items. |
| `POST /notifications/{id}/cancel` | Only `pending`/`queued` → 200; otherwise 409. |
| `GET /notifications` | Filters `status, channel, from, to`; keyset pagination `limit` (≤100) + opaque `cursor`. |
| `POST /templates`, `GET /templates/{id}` | Bonus. |
| `GET /healthz` | Liveness. |
| `GET /readyz` | DB + RabbitMQ reachable. |
| `GET /metrics` | Prometheus. |
| `GET /docs` | Swagger UI over the spec. |

Errors: one JSON shape `{error: {code, message, details}}`.

### Validation (my calls)
- sms: recipient E.164 (`^\+[1-9]\d{7,14}$`), content ≤ 160 chars (runes).
- email: recipient parsed by `net/mail`, content ≤ 10 000 chars.
- push: recipient = device token, 1–512 chars, content ≤ 256 chars.
- content non-empty after trim; priority defaults to `normal`; `scheduled_at` must be in the future (≤ 30 days).

## 8. Observability
- **Metrics** (Prometheus client):
  - `notifications_created_total{channel}`
  - `deliveries_total{channel,result=sent|retry|failed}`
  - `delivery_attempt_duration_seconds{channel}` (provider latency)
  - `notification_end_to_end_seconds{channel}` (created → sent)
  - `queue_depth{queue}` (RabbitMQ passive declare, scraped every 5 s)
  - `notifications_by_status{status}` (DB gauge)
- **Logging**: `log/slog` JSON. Middleware reads or generates `X-Correlation-ID`, echoes it,
  stores it on the row and in the AMQP header; the worker logs with the same ID.

## 9. Project layout
```
cmd/api, cmd/worker
internal/
  config      env-based config
  domain      Notification, enums, validation, state transitions
  store       Postgres repository (pgx), relay/sweeper queries
  queue       RabbitMQ topology, publisher (confirms), consumer
  worker      relay, consumer handler, sweeper, backoff, classifier
  provider    webhook client
  api         chi router, handlers, middleware, error mapping
  observability  metrics, logger
migrations/   api/openapi.yaml   deploy/ (Dockerfile)   docker-compose.yml   Makefile
```

## 10. Running and testing
- `docker compose up`: postgres, rabbitmq (management UI), `migrate` one-shot, api, worker, swagger-ui.
- `PROVIDER_URL` env. Defaults to the candidate's webhook.site URL. A tiny `mockprovider`
  (configurable 202/500/latency) ships for load and failure demos, because webhook.site
  free tier limits request volume.
- `make test`: unit tests (validation, backoff, classifier, state guard, handlers with fakes)
  plus integration tests with testcontainers-go (Postgres + RabbitMQ: create → sent,
  retry → DLQ, cancel, idempotent replay). Requires Docker only.
- CI (GitHub Actions): `golangci-lint` + `make test`.

## 11. Left to the builder
Exact library versions, Go version (latest installed via brew), backoff constants tuning,
README diagram style, commit granularity (small, conventional-commit style, no AI attribution).
