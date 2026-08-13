# marketmanager

A Go service that ingests EVE Online market data into PostgreSQL.

It does two things:

1. **Current market orders** for 25 regions, from ESI, refreshed on each region's `Expires` tick
   (about every 5 minutes).
2. **Daily market history** for those regions, imported in bulk from the EVE Ref dataset.

The service owns the `market` schema and is its only writer. Readers query that schema directly and
never call this service. It exposes no public API beyond a health check.

## Requirements

- Go 1.26 or later
- PostgreSQL 17 or later, with the `sde` schema readable (the region set is derived from it)

## Running

```sh
go build ./cmd/marketmanager
./marketmanager
```

The service creates its own tables on start, guarded by a Postgres advisory lock so two instances
cannot race during a rolling deploy. The `market` schema itself must already exist and be owned by
the connecting role; see `PROJECT.md` for the one-time bootstrap.

## Configuration

All configuration comes from the environment. There is no config file. `.env.example` holds the
same list with the reasoning inline.

Two values are required and the service refuses to start without them:

| Variable | Meaning |
|---|---|
| `DATABASE_URL` | PostgreSQL DSN for the unprivileged role that owns the `market` schema |
| `USER_AGENT` | Sent on every ESI request. Must carry an app name and a contact email. |

### ESI client

| Variable | Default | Meaning |
|---|---|---|
| `ESI_BASE_URL` | `https://esi.evetech.net` | ESI root. |
| `X_COMPATIBILITY_DATE` | `2026-08-04` | Pins the response schema. Bump it deliberately. |
| `ESI_CONNECT_TIMEOUT` | `5s` | Bounds the TCP dial and the TLS handshake only. |
| `ESI_REQUEST_TIMEOUT` | `30s` | Bounds the whole request including the body read. |
| `FETCH_RPS` | `70` | Global request rate cap. The politeness contract. |
| `FETCH_CONCURRENCY` | `16` | Global ceiling on requests in flight. |

`ESI_REQUEST_TIMEOUT` must be at least `ESI_CONNECT_TIMEOUT`. The two are separate because they
fail for different reasons: a connection that will not open is dead, while a slow response is
merely slow. See `PROJECT.md` for why the request timeout is deliberately generous.

`FETCH_RPS` binds where network latency is low and `FETCH_CONCURRENCY` binds where it is high.
Whichever binds first, binds. ESI also grants each source IP a throughput allowance and queues
everything offered above it, so raising either knob past the allowance only inflates per-request
latency. The default of 16 in flight saturates the allowance in every measured regime; see
`PROJECT.md` for the measurements.

### Budget governor

| Variable | Default | Meaning |
|---|---|---|
| `BUDGET_RESERVE` | `600` | ESI tokens left unspent when deciding whether a region's page set fits. |
| `BUDGET_FLOOR` | `300` | Below this the fetcher stops entirely and probes for recovery. |

`BUDGET_RESERVE` must be at least `BUDGET_FLOOR`; below the floor the fetcher stops outright, so a
smaller reserve could never trigger a deferral.

### Retries

| Variable | Default | Meaning |
|---|---|---|
| `PAGE_ATTEMPTS` | `3` | Attempts per page, including the first. Applies to every region. |
| `PAGE_BACKOFF_UNIT` | `250ms` | Backoff step between page attempts. |
| `ERROR_LIMIT_FLOOR` | `30` | Retries stop while the legacy per-IP error budget is below this. |

Region-level retries are separate and are limited to priorities 1-4. Page-level retries are not,
because the economics invert; `PROJECT.md` explains why.

### Database

| Variable | Default | Meaning |
|---|---|---|
| `DELTA_WORK_MEM` | `64MB` | `work_mem` for the delta transaction, set per session, never globally. |

### History import

| Variable | Default | Meaning |
|---|---|---|
| `EVEREF_BASE_URL` | `https://data.everef.net/market-history` | EVE Ref dataset root. |
| `HISTORY_POLL_INTERVAL` | `15m` | How often to check the dataset index for new or grown day files. |
| `HISTORY_BACKFILL_DAYS` | `730` | How deep to backfill on first run. |
| `HISTORY_RECENT_DAYS` | `10` | How far back to keep re-checking. Older days are treated as final. |
| `HISTORY_SPACING` | `2s` | Pause between day files, to stay polite to a best-effort service. |

A 730-day backfill is roughly 25M rows after the tracked-region filter, and it runs on **every**
start where the data is absent. Pin a smaller value when testing.

### Service

| Variable | Default | Meaning |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Listen address for the health endpoint. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |
| `INGEST_LOG_RETENTION_DAYS` | `30` | How long to keep `market.ingest_log` rows. |
| `PRUNE_INTERVAL` | `24h` | How often to trim the ingest log. |
| `ANALYZE_INTERVAL` | `1h` | How often to `ANALYZE` the partitioned parents. Also runs once at start. |

`debug` adds a line per ESI request and per cycle phase. It is far too chatty for a live service.
Response bodies are never logged at any level.

`ANALYZE_INTERVAL` is not optional maintenance: autovacuum never analyses a partitioned parent, so
without it a reader's cross-region query plans against empty statistics.

## Health

`GET /healthz` returns `ok` when the database answers, and 503 otherwise.

The binary is its own health check client, because the container image has no shell:

```sh
./marketmanager -healthcheck   # exit 0 healthy, 1 unhealthy
```

## Observability

There is no metrics endpoint. Two tables carry the operational record and are queryable in SQL:

- `market.region_status` — one row per region: `refreshed_at`, `expires`, `pages`, `order_count`,
  `consecutive_errors`, `last_error`.
- `market.ingest_log` — one row per cycle: outcome, phase timings, tokens spent, rows changed.

The same numbers appear in the JSON logs, so a long run can be analysed either way.

## Tests

```sh
go test -race ./...
golangci-lint run
```

Integration tests are behind a build tag and **skip silently** when their
environment is missing, so a run that prints `ok` without these set has not
exercised them:

```sh
export MM_TEST_DSN="postgres://marketmanager:...@127.0.0.1:5432/eve?sslmode=disable"
export MM_TEST_USER_AGENT="marketmanager-test/1.0 (you@example.com)"
go test -tags integration ./...
```

`MM_TEST_DSN` needs a database with the `market` schema present. `MM_TEST_USER_AGENT`
is required by the tests that call ESI and EVE Ref; they are deliberately cheap
(the global PLEX market is a single page), but they do use the real services.
