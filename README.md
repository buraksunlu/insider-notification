# Notification System

[![CI](https://github.com/buraksunlu/insider-notification/actions/workflows/ci.yml/badge.svg)](https://github.com/buraksunlu/insider-notification/actions/workflows/ci.yml)

An event-driven notification service in Go. It accepts SMS, email and push
notifications over a REST API, queues them per channel in RabbitMQ with
priorities, delivers them to an external provider at most 100 messages per
second per channel, retries transient failures with backoff, and exposes
status, metrics and structured logs.

- [Quick start](#quick-start)
- [Architecture](#architecture)
- [Delivery guarantees](#delivery-guarantees)
- [API](#api)
- [Observability](#observability)
- [Configuration](#configuration)
- [Testing](#testing)
- [Trade-offs and next steps](#trade-offs-and-next-steps)

## Quick start

Requirements: Docker with Compose v2. Nothing else; Go is only needed to run the tests.

```sh
docker compose up --build
```

| What | Where |
|---|---|
| API | http://localhost:8080/api/v1 |
| Swagger UI (OpenAPI 3) | http://localhost:8080/docs · spec at [`api/openapi.yaml`](api/openapi.yaml) |
| API metrics | http://localhost:8080/metrics |
| Worker metrics | http://localhost:9091/metrics |
| RabbitMQ management | http://localhost:15672 (guest / guest) |
| Mock provider | http://localhost:8090 |

Send a notification and watch it being delivered:

```sh
curl -s -X POST localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"Hello from the notification service"}'
# → 202 {"id":"…","status":"pending",…}

curl -s localhost:8080/api/v1/notifications/<id>
# → {"status":"sent","attempts":1,"provider_message_id":"…",…}
```

### Delivering to webhook.site

By default the worker delivers to the bundled **mock provider**, because the
webhook.site free tier limits request volume and its URLs expire. To deliver to
webhook.site instead:

1. Open https://webhook.site and copy your unique URL.
2. Under **Edit**, set the default status code to `202`, content type to
   `application/json` and the body to
   `{"messageId":"<any uuid>","status":"accepted","timestamp":"2026-11-21T10:00:00Z"}`.
3. Start the stack with that URL:

```sh
PROVIDER_URL=https://webhook.site/<your-uuid> docker compose up --build
```

Each delivery appears on the webhook.site page with the `Idempotency-Key`
(the notification id) and `X-Correlation-ID` headers. The free tier returns
the same static body every time, so every notification stores the same
`provider_message_id`; the mock provider generates a fresh one per request.

## Architecture

```
            ┌───────────┐  INSERT (status=pending)   ┌────────────┐
 client ──▶ │    api    │ ─────────────────────────▶ │ PostgreSQL │ ◀── source of truth
            └───────────┘  NOTIFY new_notification   └────────────┘
                                                        ▲   │ claim due rows (SKIP LOCKED)
                                                        │   ▼
                 ┌───────────────────────── worker ─────────────────────────────┐
                 │ relay ──publish(priority)──▶ RabbitMQ ──▶ consumer per channel│
                 │ sweeper: rows stuck in processing / queued → pending          │
                 └──────────────────────────────────────────────┬───────────────┘
                                                                ▼
                                                rate limiter (100 msg/s per channel)
                                                                ▼
                                                 provider (webhook.site or mock)
```

Two binaries from one module, `cmd/api` and `cmd/worker`, plus `cmd/mockprovider`
for local runs. The full design, including the state machine and every
decision below, is in [`docs/DESIGN.md`](docs/DESIGN.md).

### Why PostgreSQL + RabbitMQ

| Option | Verdict |
|---|---|
| PostgreSQL only (`SKIP LOCKED` as the queue) | Fewest moving parts, but the database carries the queue load and there is no real event-driven pipe. |
| PostgreSQL + Redis | Redis has no acknowledgements, so crash recovery would be custom code. |
| **PostgreSQL + RabbitMQ** | **Chosen.** Native message priority, acks, dead-letter exchange. |
| Kafka | No message priority; heavy for this scope. |

### The database is the outbox

The API never publishes to RabbitMQ. It inserts rows with `status=pending` in
one transaction and fires `NOTIFY`. The worker's **relay** wakes on that
signal (with a 1 s polling fallback), claims due rows with
`FOR UPDATE SKIP LOCKED` in priority order, marks them `queued`, **commits**,
and only then publishes with publisher confirms. That means:

- An API request never succeeds with a message lost between the database and the broker.
- Scheduled notifications and retries are the same mechanism: a row becomes due
  when `next_attempt_at <= now()`.
- Retries are scheduled in the database, not with TTL queues, so the delay can
  follow the backoff and `Retry-After` exactly, and a pending retry can be cancelled.
- A backlog stays in PostgreSQL. The relay keeps at most 10 000 messages per
  channel in RabbitMQ (about 100 s of work at 100 msg/s) and tops a channel up
  as consumers drain it, so a burst of a million notifications neither floods
  the broker nor waits long enough in a queue to look lost to the sweeper.

### Processing a message

1. **Claim.** The consumer moves the row `queued → processing` in one conditional
   `UPDATE`. If no row matches (cancelled, already handled, duplicate delivery),
   the message is acked and skipped. This guard is what keeps a cancelled
   notification from being sent.
2. **Rate limit.** One token bucket per channel, 100/s with a burst of 100, so a
   flood on one channel never slows the others.
3. **Deliver.** `POST {to, channel, content}` with a 5 s timeout and the headers
   `Idempotency-Key: <notification id>` and `X-Correlation-ID`. Redirects are not
   followed.
4. **Classify.**
   - `2xx` → `sent`. The provider's `messageId` is stored.
   - `408`, `429`, `5xx`, timeouts and network errors → retry.
   - Any other `4xx` or a `3xx` → permanent failure, not retried.
5. **Retry or fail.** Retries use exponential backoff with full jitter: base 2 s,
   doubling, capped at 5 min, and `Retry-After` is honoured. After 8 attempts
   (worst case about 4 minutes) the notification is marked `failed` and copied
   to the dead-letter queue `notifications.dlq` with the reason.

Priorities: `high`, `normal` (default) and `low` map to RabbitMQ message
priorities 3, 2 and 1 on each channel queue. The relay also claims due rows
highest priority first.

## Delivery guarantees

| Scenario | What happens |
|---|---|
| Duplicate API request with the same `Idempotency-Key` | The stored notification is returned (200). A different payload gets 409. This also works after `scheduled_at` has passed. |
| Cancel after the message is already in the queue | The claim guard skips it. It is never sent. |
| Worker crashes mid-delivery | The message is redelivered but skipped (the row is `processing`). The sweeper resets rows stuck in `processing` for more than 2 minutes back to `pending`, or to `failed` if that was the last attempt. |
| Message lost between relay and consumer | The sweeper resets rows `queued` for more than 10 minutes. |
| Database blip after the provider answered | The outcome write is retried for up to 30 s rather than requeued. Requeueing would resend a delivered message. |
| RabbitMQ restarts | The worker exits and Compose restarts it. Nothing is lost: every row is in PostgreSQL. |
| `docker compose stop` during deliveries | In-flight deliveries finish and record their outcome; the worker's stop grace period (45 s) covers the provider timeout plus the outcome write. |

Delivery is **at least once** toward the provider. The only duplicate window
is a worker crash between the provider's answer and the state write. Every
request carries the notification id as `Idempotency-Key` so a provider can
deduplicate it.

## API

Base path `/api/v1`. The full contract with examples is at
[`/docs`](http://localhost:8080/docs). Every error has the same body:

```json
{"error": {"code": "validation_failed", "message": "invalid notification", "details": {"recipient": "must be an E.164 phone number, e.g. +905551234567"}}}
```

| Endpoint | Purpose |
|---|---|
| `POST /notifications` | Create one (202). Optional `Idempotency-Key` header and `scheduled_at`. |
| `POST /notifications/batch` | Create 1–1000 in one transaction; all-or-nothing validation. |
| `GET /notifications/{id}` | Status, attempts, last error, provider message id. |
| `POST /notifications/{id}/cancel` | Cancel a `pending` or `queued` notification. |
| `GET /notifications` | List with `status`, `channel`, `from`, `to` filters and cursor pagination. |
| `GET /batches/{id}` | Counts per status plus the batch's notifications. |
| `POST /batches/{id}/cancel` | Cancel every pending or queued item of a batch. |
| `POST /templates` | Create a reusable body with `{{variable}}` placeholders. |
| `GET /templates`, `GET /templates/{id}` | List or read templates, with their placeholder names. |

Validation rules:

| Channel | Recipient | Content |
|---|---|---|
| sms | E.164 (`+905551234567`) | ≤ 160 characters |
| email | an email address | ≤ 10 000 characters |
| push | device token, 1–512 characters | ≤ 256 characters |

Content length is counted in characters, so Turkish letters and emoji count as
one each. `scheduled_at` must be in the future and within 30 days. Unknown JSON
fields are rejected, so a typo never passes silently.

### Examples

```sh
API=localhost:8080/api/v1

# Idempotent create: repeat it and you get 200 with the same notification.
curl -s -X POST $API/notifications -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1001-shipped' \
  -d '{"channel":"push","recipient":"device-token-abc","content":"Your order has shipped","priority":"high"}'

# Scheduled email.
curl -s -X POST $API/notifications -H 'Content-Type: application/json' \
  -d '{"channel":"email","recipient":"ada@example.com","content":"Weekly digest","scheduled_at":"2026-12-01T09:00:00Z"}'

# Batch.
curl -s -X POST $API/notifications/batch -H 'Content-Type: application/json' -d '{
  "notifications": [
    {"channel":"sms","recipient":"+905551234567","content":"Flash sale starts now","idempotency_key":"campaign-42-user-1"},
    {"channel":"email","recipient":"ada@example.com","content":"Flash sale starts now","priority":"low"}
  ]}'
# → 202 {"batch_id":"…","ids":["…","…"]}

curl -s $API/batches/<batch_id>                       # counts per status + items
curl -s -X POST $API/batches/<batch_id>/cancel        # → {"batch_id":"…","cancelled":<n>}; sent items stay sent

# Cancel one.
curl -s -X POST $API/notifications/<id>/cancel

# List failed SMS from today, 50 per page; pass next_cursor back as cursor.
curl -s "$API/notifications?channel=sms&status=failed&from=2026-11-21T00:00:00Z&limit=50"
curl -s "$API/notifications?channel=sms&status=failed&limit=50&cursor=<next_cursor>"
```

### Templates

A template is a reusable body with `{{variable}}` placeholders. A
notification gives either `content` or `template_id` with `variables`:

```sh
curl -s -X POST $API/templates -H 'Content-Type: application/json' \
  -d '{"name":"order-shipped","channel":"sms","body":"Merhaba {{name}}, siparişin {{order}} kargoda"}'
# → 201 {"id":"<template_id>","variables":["name","order"],…}

curl -s -X POST $API/notifications -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","template_id":"<template_id>","variables":{"name":"Şule","order":"A-1001"}}'
# → 202 {"content":"Merhaba Şule, siparişin A-1001 kargoda","template_id":"<template_id>",…}
```

The rendered text is stored on the notification and checked against the
channel's limits like any other content. Variable values may be strings,
numbers or booleans; numbers keep their exact spelling (`{"code": 4821}`).
A template whose fixed text alone exceeds its channel's limit is rejected
when it is created.

- A missing variable is a 422 that names it.
- Extra variables are ignored, so a template can drop a placeholder without
  breaking clients.
- The template's channel must match the notification's channel.
- Templates are immutable: `template_id` on a notification always names the
  body it was rendered from.
- Batch items can use templates too. A batch of 1000 items that share one
  template reads it once.

### Trying failures with the mock provider

| Mock setting | Effect |
|---|---|
| content starting with `mock:500` (or `mock:429`, `mock:503`) | always answered with that status: retried, then `failed` and in the DLQ |
| content starting with `mock:400` | permanent failure after one attempt |
| `MOCK_FAIL_RATE=0.3 docker compose up` | 30% of requests answered with 500 |
| `MOCK_LATENCY_MS=300 docker compose up` | adds latency to every answer |

```sh
curl -s -X POST localhost:8080/api/v1/notifications -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"mock:500 will exhaust its retries"}'
```

With the default 2 s retry base this takes about 4 minutes. Start the stack
with `RETRY_BASE=50ms` to watch it fail within seconds.

## Observability

**Metrics** (Prometheus):

| Metric | Source | Meaning |
|---|---|---|
| `notifications_created_total{channel}` | api | New notifications; idempotent replays excluded. |
| `http_request_duration_seconds{method,route,code}` | api | Request latency per route pattern. |
| `deliveries_total{channel,result}` | worker | `sent`, `retry`, `failed`, `skipped` (cancelled or duplicate). |
| `delivery_attempt_duration_seconds{channel}` | worker | Provider call latency, timeouts included. |
| `notification_end_to_end_seconds{channel}` | worker | From due time to sent, retries included. |
| `queue_depth{queue}` | worker | Ready messages per channel queue and the DLQ. |
| `notifications_by_status{status}` | worker | Rows per status. |

Example queries:

```promql
# delivery success rate per channel over 5 minutes
sum by (channel) (rate(deliveries_total{result="sent"}[5m]))
  / sum by (channel) (rate(deliveries_total{result=~"sent|failed|retry"}[5m]))

# p95 end-to-end latency
histogram_quantile(0.95, sum by (le, channel) (rate(notification_end_to_end_seconds_bucket[5m])))
```

**Logs** are JSON (`log/slog`). Every request gets an `X-Correlation-ID` (read
from the request or generated) that is echoed in the response, stored on the
notification, carried in the AMQP message and sent to the provider. A
notification can be followed from the API log line to the delivery:

```sh
docker compose logs api worker | grep <correlation-id>
```

**Health:** `GET /healthz` (liveness) and `GET /readyz` (database reachable) on
the API. The worker serves `/healthz` and `/metrics` on `:9091`.

## Configuration

All configuration comes from environment variables.

| Variable | Default | Used by |
|---|---|---|
| `DATABASE_URL` | required | api, worker |
| `AMQP_URL` | required | worker |
| `PROVIDER_URL` | required (compose: mock provider) | worker |
| `HTTP_ADDR` | `:8080` | api |
| `METRICS_ADDR` | `:9091` | worker |
| `LOG_LEVEL` | `info` | api, worker |
| `RETRY_BASE` | `2s` | worker: base of the retry backoff |
| `WORKER_PREFETCH` | `50` | worker: concurrent deliveries per channel |
| `MOCK_FAIL_RATE`, `MOCK_LATENCY_MS` | `0`, `50` | compose: mock provider behaviour |

Schema changes are versioned migrations in [`migrations/`](migrations), applied
by the `migrate` service before the API and worker start.

## Testing

```sh
make test   # unit + integration tests (needs Docker: testcontainers starts Postgres and RabbitMQ)
make lint   # golangci-lint
make e2e    # builds and starts the compose stack with RETRY_BASE=50ms, runs end-to-end scenarios
make down   # stops the stack and removes its volumes
```

- **Unit tests:** validation (rune limits with Turkish characters and emoji),
  backoff and classification, the message handler's state machine, the relay,
  the sweeper and the API with fakes.
- **Integration tests** against real PostgreSQL and RabbitMQ: repository
  queries, queue topology and priority ordering, the DLQ, and a relay +
  consumer run that proves every notification is sent exactly once.
- **End-to-end tests** (`e2e/`) drive the running stack over HTTP:
  - delivery
  - retries exhausted, ending in `failed` and the DLQ
  - a `4xx` is not retried
  - a cancelled scheduled notification is never sent
  - batch summary
  - idempotent replay
  - a notification rendered from a template

CI runs lint, the tests and the end-to-end suite on every push and pull request.

> **Colima users:** testcontainers needs
> `export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`.

## Trade-offs and next steps

- **Rate limit per worker process.** The 100 msg/s limit is a token bucket
  inside the worker. With several worker replicas the limit would need to be
  shared, for example a Redis token bucket or one consumer per channel.
- **No authentication or multi-tenancy.** These are out of scope for the case.
  An API key per tenant and per-tenant limits would come first.
- **At-least-once delivery.** There is one small duplicate window when a worker
  crashes after the provider answered. It is covered by the `Idempotency-Key`
  header, not removed.
- **Connection loss restarts the worker** instead of reconnecting in-process.
  The code stays simple and nothing is lost, at the cost of a few seconds of
  downtime.
- **Status gauges count the table.** `notifications_by_status` runs a
  `GROUP BY status` every 5 s. That is fine for this scope, but with tens of
  millions of rows it should come from a counter table updated in the same
  transactions, or count only the active statuses through their partial indexes.
- **Retention.** Delivered rows and attempts are kept forever. A production
  setup would partition `notifications` by time and archive old partitions.
- **Not built:** WebSocket status updates, distributed tracing, and template
  versioning (templates are immutable; a new version is a new template).
  The correlation id already gives a trace key to build on.
