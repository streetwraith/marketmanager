# marketmanager — architecture and design decisions

This file records the *why*. `README.md` covers what it is and how to run it.

## Module layout

```
cmd/marketmanager/     the only entrypoint
internal/config/       environment loading and validation
internal/store/        every database access; all SQL lives here
internal/region/       which regions to ingest, and in what order
internal/esi/          the ESI client, rate limiting, and the budget governor
internal/everef/       the EVE Ref daily history dataset client and parser
internal/ingest/       the sweep, the delta cycle, the scheduler, history, maintenance
migrations/            embedded SQL, applied at start under an advisory lock
```

## Schema ownership

The service owns the `market` schema and is its only writer. Consumers get `SELECT` and read it
through their own unmanaged models. This mirrors how the `sde` schema is owned by a separate
importer and read by everything else.

The schema must be created out of band by a superuser, because the app role deliberately has no
`CREATE` on the database. Migrations only create objects inside the schema the role owns.

One-time bootstrap, run as a superuser:

```sql
CREATE ROLE marketmanager LOGIN PASSWORD '...';
CREATE SCHEMA market AUTHORIZATION marketmanager;
GRANT USAGE ON SCHEMA market TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA market TO PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE marketmanager IN SCHEMA market
    GRANT SELECT ON TABLES TO PUBLIC;
GRANT USAGE ON SCHEMA sde TO marketmanager;
```

`market` must live in the same database as `sde`, because the region set is derived from `sde` and
consumers join market data to it.

## The region set

The 25 regions are derived at start-up rather than hardcoded, because new empire regions do appear:

```sql
SELECT r._key AS region_id, r.name_en
FROM sde.map_regions r
JOIN sde.map_solar_systems s ON s.region_id = r._key
GROUP BY r._key, r.name_en
HAVING max(s.security_status) >= 0.05
ORDER BY r._key;
```

The resolved set is logged at start-up, because one member of it arrives through that security
filter surprisingly. See "The global PLEX market" below.

Page counts drift — the total moved from 1,515 to 1,516 within 40 minutes of a census — so
`X-Pages` is read every cycle and a page count is never cached. The census below is a snapshot for
sizing, not a source of truth.

| Priority | Region | `region_id` | Pages | Tokens/cycle |
|---|---|---|---|---|
| 1 | Domain | 10000043 | 182 | 364 |
| 2 | The Forge | 10000002 | 413 | 826 |
| 3 | Heimatar | 10000030 | 74 | 148 |
| 4 | Metropolis | 10000042 | 119 | 238 |
| 5 | Sinq Laison | 10000032 | 124 | 248 |
| 6 | Lonetrek, The Citadel, Essence, Tash-Murkon, Placid, Verge Vendor, Genesis, Everyshore, Kador, Molden Heath, Kor-Azor, Aridia, Solitude, Derelik, Devoid, Exordium, Black Rise, The Bleak Lands, Khanid | | 553 | 1,106 |
| 6 | GPMR-01 | 19000001 | 1 | 2 |

That is 1,515 pages and 3,030 tokens per full pass, so **9,090 tokens per 15-minute window, 75.8%
of the bucket**. A 48-hour run measured 3,034 tokens per pass and a median `remaining` of 2,118,
with a floor of 653 at the worst alignment.

Data volume is 36 MB gzipped per full pass, about 24 KB per page. Bandwidth is not a constraint.

### The global PLEX market

`region_id` 19000001, named **GPMR-01** in the SDE, is a universe-wide market rather than a
geographic region. It qualifies through the security filter above because its single system has
security 1.0, which is why the region count is 25 and not 24.

- It holds only type 44992 (PLEX), about 940-970 orders, always one page.
- Its orders sit in stations across the whole universe, including nullsec and one wormhole region,
  so any reader filter that assumes a region's orders are geographically local is wrong for it.
- **It is disjoint from the normal region feeds.** A full 28-page sweep of a normal region returned
  zero type 44992 rows and zero overlapping `order_id` values, so there is no deduplication problem.

Being a single cheap page also makes it the budget governor's canary: probing it costs 2 tokens and
returns data that is wanted anyway.

## The order snapshot is applied as a delta, not replaced

This is the central design decision.

ESI returns a complete snapshot of a region's order book, so the obvious approach is to replace the
stored copy wholesale. Measured against The Forge, two consecutive snapshots 300 seconds apart
differ by only **0.52%** of rows: 0.05% added, 0.05% removed, 0.42% changed in place.

Replacing the whole table therefore rewrites ~406,000 rows to change ~2,200. Measured cost of the
three candidate strategies, for one hub region:

| Strategy | Time to visible | Reader blocking | WAL per cycle |
|---|---|---|---|
| `TRUNCATE` + `COPY` in one transaction | 17.5 s | 17.5 s, `ACCESS EXCLUSIVE` on the partition | 241 MB |
| Staging table, then partition swap | 5.3 s | ~50 ms, `ACCESS EXCLUSIVE` on the **parent** | 95 MB |
| **Delta apply** | **3.3 s** | **none** | **1-3 MB** |

The delta wins on every axis. Its apply transaction takes only `ROW EXCLUSIVE`, which does not
conflict with the `ACCESS SHARE` readers take, so readers are never blocked at all. It is one
transaction, so a reader sees either the whole old snapshot or the whole new one.

Confirmed in a live run across all 25 regions. One steady-state pass, every region applying a
delta rather than a first load:

```
26 delta cycles: +913 inserted  ~3,225 updated  -805 deleted  0 duplicates
                 4,943 rows touched out of 1,504,536  =  0.329% of the table
dead tuples across all partitions afterwards: 0.215%
```

Per-region cost at steady state: Domain 182 pages, fetch 3.6s, apply 0.83s; The Forge 413 pages,
fetch 8.0s, apply 2.5s. Storage for 1.5M orders is 552 MB (169 MB heap, 382 MB indexes), plus
172 MB of staging tables.

The partition swap was rejected because `ALTER TABLE ... DETACH PARTITION` takes `ACCESS EXCLUSIVE`
on the *parent*, briefly blocking readers of every region, and queueing behind a slow reader turns
a 50 ms operation into a multi-second stall for everyone. `DETACH CONCURRENTLY` takes a lighter
lock but cannot run inside a transaction, so it cannot give an atomic swap.

### How a cycle works

1. Fetch every page of the region into an **UNLOGGED, unindexed** staging table via `COPY`.
2. Verify `Last-Modified` is identical across all pages and `X-Pages` did not change. On a
   mismatch, discard the staging table. **A partial page set is never published**: a missing page
   would look to a consumer like orders vanished.
3. Compute the whole delta in one `FULL OUTER JOIN` pass into an UNLOGGED table.
4. Apply `DELETE`, `UPDATE`, `INSERT` in a single transaction.
5. Verify with a checksum, update `region_status`, write an `ingest_log` row.

`TRUNCATE` + `COPY` remains as the **resync path** for a single region, used on first load and
whenever the checksum disagrees.

### Duplicate order ids must be removed

ESI can return the same order on two pages when the page set shifts mid-fetch. Left in place, a
duplicated new order produces two inserts and violates the primary key, aborting the entire cycle;
a duplicated existing order makes `UPDATE ... FROM` apply an arbitrary one of them. The delta query
deduplicates with `DISTINCT ON (order_id)`, and the duplicate count is recorded in `ingest_log`
because a non-zero count means the consistency check missed something.

## Rate limiting

The ESI `market-order` bucket is **12,000 tokens per 15 minutes, keyed by source IP**, and it is a
true **sliding window**: each spent token returns exactly 900 seconds later. Verified against 5,516
logged responses, `remaining = 12000 - (tokens spent in the trailing 900s)` held with a median
error of 0.

There is no reset header, so the service gates on the `X-Ratelimit-Remaining` value carried on
every response. That is deliberately simpler than a local ledger and strictly better in one way: it
reflects *every* consumer sharing the IP, not just this service.

Two limits are configured, and whichever binds first, binds:

- `FETCH_RPS` is the politeness contract and binds where network latency is low.
- `FETCH_CONCURRENCY` binds where latency is high, since throughput is roughly
  `concurrency / latency`.

Fixing only concurrency would make a low-latency host an order of magnitude more aggressive than a
high-latency one for the same setting.

Concurrency was chosen by measurement, sweeping one 182-page region from a high-latency host:

| Concurrency | Pages/s | p50 | p90 | Errors |
|---|---|---|---|---|
| 8 | 9.6 | 805 ms | 979 ms | none |
| 16 | 19.1 | 758 ms | 933 ms | none |
| 32 | 34.0 | 864 ms | 959 ms | none |
| **64** | **67.9** | 779 ms | 897 ms | none |
| 96 | 80.0 | 859 ms | 969 ms | none |

Latency stays flat from 8 to 96, so ESI itself does not degrade under this load; no 429 and no 420
appeared across about 11,000 requests. Throughput scales almost linearly to 64 and then knees, with
96 buying only 18% more. 64 is the knee, which is why it is the default.

**That sweep used a fresh HTTP/1.1 connection per request, so its numbers do not carry over to the
service**, which negotiates HTTP/2 and multiplexes onto one connection. The 64 default is therefore
a ceiling that the service never reaches — see "One TCP connection carries the whole sweep" below
for what actually binds. Re-run this sweep against the real client before treating 64 as tuned.

### The legacy error limit is a second, separate limit

Independent of the token bucket, ESI allows **100 non-2xx/3xx responses per minute per IP**, then
returns HTTP 420 on *every* route until the window resets. Headers `X-ESI-Error-Limit-Remain` and
`X-ESI-Error-Limit-Reset` track it.

The two limits interact awkwardly. A 5xx costs **zero** tokens but still strikes the error limit,
so server errors are cheap to retry against the budget and expensive against the error limit. That
is why retries stop at `ERROR_LIMIT_FLOOR`: retrying into a bad spell is exactly how a 420 happens,
and a 420 blocks every other application sharing the address, not just this one.

### Sharing the address with other applications

Unauthenticated ESI routes key on source IP, and ESI publishes no AAAA records, so one host means
one budget however many applications run on it. Gating on the `X-Ratelimit-Remaining` header rather
than a local ledger is what makes this safe: the governor sees every consumer's spend, not just its
own, and simply defers when the number is low.

Deferring is safe but not free — the governor defers whatever region is due at the trough, and the
expensive high-priority regions are the ones most likely to not fit. A deployment that must share
the bucket should expect hub regions to be the ones that slip.

### Timeouts

Connecting and responding get separate budgets, because they fail for different reasons.

`ESI_CONNECT_TIMEOUT` (5s) bounds the TCP dial and the TLS handshake. A connection that will not
open in a few seconds is dead, and failing fast on it costs nothing.

`ESI_REQUEST_TIMEOUT` (30s) bounds the whole request including the body read, and is deliberately
far more generous. Measured per-request latency swings from **~0.8s off-peak to ~15s during EVE
prime time** (18:00-21:00 CEST) with identical payloads — a network and upstream-load effect, not
more data. A timeout tuned for the quiet case would fire on more than half of every sweep at peak.
Most of that swing is queueing against ESI's per-IP throughput allowance and scales with offered
in-flight, so `FETCH_CONCURRENCY` sets how close a sweep runs to this timeout (see below).

That asymmetry matters more than it looks: **a client-side timeout does not cancel the server's
work.** ESI has already processed the request and charged the token, so an early timeout wastes it
and the retry spends another. Waiting is cheaper than retrying whenever the response is merely slow
rather than absent.

Note that setting `Transport.DialContext` disables automatic HTTP/2 negotiation unless
`ForceAttemptHTTP2` is also set. Without h2 the sweep would fall back to one request per
connection. `TestLiveStillNegotiatesHTTP2` guards this.

### ESI grants each source IP a throughput allowance, and client knobs cannot lift it

First, the Go behaviour that started this investigation, because it is real and worth knowing:
Go's HTTP/2 transport multiplexes every request onto a **single** TCP connection per host, and
`MaxConnsPerHost` governs only the HTTP/1.1 path. Confirmed by sampling `ss -tnp` during an active
sweep: exactly one established connection to ESI. As of Go 1.26 there is no built-in way to spread
load across h2 connections below the server's stream limit; `Transport.NewClientConn` exists so
that a pool can be built by hand.

That single connection was the suspected throughput ceiling. Measurement disproved it. A
round-robin pool of cloned transports — one TCP connection each, mechanics verified against a
local h2 server — was benchmarked live against real order pages, 64 in flight, 150 pages per arm:

| Connections | req/s | p50 |
|---|---|---|
| 1 | 34.8 | 1381ms |
| 2 | 37.2 | 770ms |
| 4 | 24.9 | 1884ms |
| 8 | 41.8 | 1160ms |

Flat. A concurrency ladder at fixed connections (80 pages per arm) shows where the ceiling
actually lives:

| In flight | req/s | p50 |
|---|---|---|
| 8 | 27.8 | 187ms |
| 16 | 34.8 | 204ms |
| 32 | 27.9 | 847ms |
| 64 | 35.6 | 1875ms |

Throughput stays flat while latency inflates tenfold, and at 64 in flight p50 equals p95. That is
a server-side fair queue: **ESI grants each source IP a throughput allowance and parks everything
offered above it**. The allowance moves with EVE server load — measured across 48 hours of
sweeps, ~45-65 pages/s at quiet hours and **3-10 pages/s during EVE prime time** (roughly
16:00-22:00 CEST). The other suspects are cleared directly: ESI advertises
`MAX_CONCURRENT_STREAMS=128`, Go's h2 receive windows are 4 MB per stream and 1 GB per
connection, and `x/time/rate` at burst=1 delivers 98% of its configured rate under contention.

Consequences, in order of importance:

- **`FETCH_CONCURRENCY` is the knob that matters, and lower is better.** Offered in-flight sets
  the server-side queue time: `latency ≈ in_flight / allowance`. At the prime-time allowance of
  ~4 pages/s, 64 in flight meant ~15s per request — brushing the 30s request timeout, and one
  4-hour prime window logged 29 timeouts, 10 failed sweeps and 5 inconsistent page sets. The
  default is therefore **16**: it saturates the allowance in every measured regime (16/0.3s ≈ 53
  off-peak, 16/4s = 4 at prime) while keeping per-request latency far from the timeout.
- **`FETCH_RPS=70` almost never binds** — the allowance sat below it at every measured hour. It
  stays as the politeness backstop, not as a throughput target; raising it buys nothing.
- **A connection pool is rejected on evidence, not deferred.** The sweep stays on the single h2
  connection.
- An earlier reading of this data — a fixed ~8-10 in-flight pipeline limit on the single
  connection, capping throughput near 90 req/s — was wrong. In-flight ≈ throughput × latency was
  a symptom of the allowance, not a transport property.

The allowance was measured from one host and one IP. A deployment on another address should
re-derive it from the achieved rate (`pages / fetch_ms`) rather than trust these numbers.

One wart stands in the client: `get` takes the concurrency slot *before* waiting on the rate
limiter, so slots are held by goroutines doing no I/O. That makes the two knobs interact instead
of bounding different things.

### Cold start

A naive start fits **four** full cycles into the first 15-minute window instead of three, because
the first sweep catches data already partway through its TTL and the second therefore falls ~3
minutes later rather than 5. That alone exhausts the bucket, and it would repeat on every restart.

So on start the service fetches only page 1 of each region to learn its `Expires`, then waits for
the next tick before the first full sweep.

## Region priority

Every region turns fresh inside the same ~60 second band, because the upstream regenerates all
region snapshots in one batch. Start offsets therefore cannot be staggered; the schedule is
dictated upstream.

Priority instead decides the fetch *order* within that band, so the most important regions are
never queued behind the least important. It also decides retry eligibility: a retry costs tokens,
so only priorities 1 to 4 may retry a failure. Everything below abandons the cycle and waits for
the next tick, which costs nothing.

Regions are refreshed **one at a time, in priority order**, so a hub never waits behind a region
nobody reads, and the pipeline has no per-region concurrency to tune.

Sequential processing leaves ample headroom. All 25 regions' work sums to about **80s per 300s
cycle, a 27% duty cycle**, measured across a full day on a high-latency host; the worst hour
observed reached 50%. Regions therefore do not queue behind each other, and the serial worker is
idle roughly three quarters of the time.

Do not measure this by grouping refresh completions into "passes" separated by an idle gap. Regions
run on independent 300s cadences, so completions are near-continuous and no such gap exists; that
method reports a meaningless figure. Sum `total_ms` over a window and divide by the window instead.

The freshness a reader actually sees, measured as `refreshed_at - last_modified` off-peak:

| Region | Pages | Publish lag | of which fetch | apply + verify |
|---|---|---|---|---|
| The Forge | 413 | 22.9s | 12.2s | 4.6s |
| Domain | 182 | 15.6s | 5.9s | 1.9s |
| Heimatar | 74 | 13.4s | 2.4s | 0.8s |
| The Citadel | 52 | 5.5s | 2.0s | 0.4s |

About 3s of every lag is scheduling padding: `Expires` plus 1s, plus 0-3s of jitter, plus up to 1s
of ticker granularity.

Database-side retries are exempt from the priority rule, because the fetched snapshot is already in
the staging table and retrying the apply costs no tokens.

### Page retries are separate from region retries

A single page is retried in place, up to `PAGE_ATTEMPTS`, rather than failing the sweep. This is
deliberately **not** gated on region priority, unlike the region-level rule, because the economics
invert: a page retry costs 2 tokens, while discarding the sweep throws away every token already
spent on the other pages. For The Forge that is the difference between 2 tokens and re-fetching 413
pages, and a priority 1-4 region would then do that up to three times — 2,478 tokens, spent during
exactly the peak hours when ESI is least able to serve them.

Retryable: timeouts, connection errors, 5xx, and 429 (honouring `Retry-After`). Not retryable: any
other 4xx, which will fail identically, and 420, which blocks the whole IP until the error window
resets. Retries also stop while the legacy error budget is below `ERROR_LIMIT_FLOOR`, because
retrying into a bad spell is how it becomes a 420.

Backoff is short by design. A sweep must finish inside the 300s snapshot window or the
`Last-Modified` check rejects it, so a page retry competes with the deadline that makes the sweep
worth doing at all.

## Daily history comes from EVE Ref, not from ESI

Full history coverage through ESI would need one request per (region, type) pair,
which is 150k-400k calls a day. The EVE Ref bulk dataset publishes the same data
as one file per day.

Two properties of that dataset shape the importer:

1. **A day file is not cumulative, and it is not complete when it appears.** Each
   file holds exactly one day, and it keeps growing for about four days as EVE Ref
   works through (region, type) pairs. There is no instant at which a day is
   final, so the importer has no completeness threshold: it re-imports a day
   whenever the index reports more records than it last stored. A fixed threshold
   would be worse than useless, because real files sit far below any "complete"
   count for days at a time.
2. **Re-importing naively rewrites everything.** Because a day fills in waves,
   importing it four times would write roughly 2.6 rows for every row stored. Each
   row carries an `http_last_modified` stamp, so the importer keeps a per-day
   watermark and only upserts rows scraped at or after the last pass.

Only tracked regions are stored. EVE Ref publishes ~77 regions, of which the 25
this service follows are about 68-70% of every file. **That share is the health
check for an import**: a day materially below it is missing rows, not quiet.

### The watermark bound is inclusive, and must stay that way

EVE Ref stamps every record of one scrape batch with an **identical**
`http_last_modified`, then keeps appending to the file for hours. A first import
therefore stores a watermark *equal to* the timestamp of the rows that arrive
next, not below it.

An exclusive bound consequently drops the entire tail of the batch it last read,
and because a file can carry only that one distinct timestamp, no later wave ever
rescues those rows. Observed on 2026-08-10: all 45,127 records of the file shared
one timestamp, of which 31,134 were for tracked regions, and an exclusive bound
held the day at **5,117 rows permanently** while three re-import passes kept zero.
Two neighbouring days were stuck at 55-56% of their file for the same reason.

The inclusive bound cannot reintroduce the write amplification it was built to
prevent, because `selectDays` only re-fetches a day whose record count actually
grew. `TestParseKeepsLaterRowsOfTheSameScrapeBatch` holds this.

Beware the check that hid this for a day: `rows_upserted == stored` proves only
that nothing was **rewritten**. It says nothing about whether the rows ever
arrived, and it reads as a clean pass while a day sits at 11% of its file. Compare
against the 68-70% share instead.

Both the compressed response and its expansion are capped. EVE Ref is a
third-party, best-effort service and bzip2 reaches roughly 1000:1 on crafted
input, so an unbounded stream could exhaust memory and take down order
ingestion, which is this service's actual job. A complete day is ~550 KB
compressed and ~7 MB decompressed, so the caps leave wide headroom; do not
remove them when raising a limit. The ISK columns are validated as they are
parsed for the same reason: a bad value should fail the record that is wrong,
not the whole day at the COPY.

`market.history` is RANGE-partitioned by year, so changing the retention depth is
cheap in both directions: deeper is importing older files, shallower is
`DROP PARTITION` rather than a mass `DELETE` that would bloat the table.

Gaps in an illiquid type's history are real and cannot be filled: the upstream
reports a day only if the type actually traded. Consumers must gap-fill.

## Logging

Structured JSON to stdout, one line per event, at `LOG_LEVEL` (default `info`).

**Response bodies are never logged.** An order page is ~240 KB and a cycle fetches
1,516 of them, so even a fragment would bury the signal and balloon storage. The
ESI error type deliberately does not retain the body, so it cannot escape through
`%+v` or a panic dump either; `TestNoResponseBodyInLogs` and
`TestHTTPErrorDoesNotCarryBody` hold that line.

- **INFO** is the steady-state record, built for later analysis. Every region
  refresh carries its phase timings — `fetch_ms`, `copy_ms`, `apply_ms`,
  `verify_ms`, `total_ms` — plus pages, row counts, tokens spent and budget
  remaining. History imports and maintenance carry their own durations. The same
  numbers land in `market.ingest_log`, so a long run can be analysed in SQL rather
  than by parsing logs.
- **ERROR** is for failures and for hitting a limit: a rate limit reached, the
  legacy error limit tripped, a cycle that exhausted its retries, a failed
  history import. A 429 during a retry is logged at ERROR rather than WARN,
  because the token bucket is a shared resource.
- **WARN** is for recoverable oddities that need no action: a checksum drift that
  was repaired, duplicate order ids in a page set, a bookkeeping write that failed
  while the data itself landed.
- **DEBUG** is development only: one line per ESI request (path, status, ms,
  bytes, remaining — never the body), per-phase staging and apply detail, and the
  scheduler's due-time decisions.

`fetch_ms` and `copy_ms` overlap almost entirely and must not be summed: pages
stream into Postgres while later pages are still in flight.

## Maintenance

Two jobs on separate tickers, because their natural frequencies differ by roughly 24x.

`PRUNE_INTERVAL` (24h) trims `market.ingest_log` to `INGEST_LOG_RETENTION_DAYS`. At ~7,200 rows a
day there is nothing to gain from running it more often.

`ANALYZE_INTERVAL` (1h), plus once at start-up, refreshes statistics on the partitioned parents.
**Autovacuum never analyses a parent** — it processes the leaf partitions and skips the parent —
so nothing else keeps those statistics alive, and a consumer query that aggregates across regions
would plan against empty ones. Measured cost is ~4.5s for `market.orders` and ~0.8s for
`market.history`, and it takes only `SHARE UPDATE EXCLUSIVE`, so it never blocks a reader. It does
conflict with the `TRUNCATE` a region resync uses; a collision just retries on the next tick.

The start-up run matters more than the interval. Row *distribution* moves slowly here — only
0.13-0.33% of rows change per cycle, and what statistics describe (rows per region, the spread of
`type_id` and `price`) barely shifts hour to hour. The failure mode worth avoiding is not stale
statistics but *absent* ones, which is what an interval-only schedule produces after every restart.

## PostgreSQL notes

The service is designed to run against a stock `postgres:17` on near-defaults, sharing a cluster
with other applications. Nothing here requires a cluster-wide setting change.

**`work_mem` is set per transaction, never globally.** The delta's hash join over ~406k rows:

| `work_mem` | Batches | Time |
|---|---|---|
| 4 MB (the default) | 4, spills to disk | 1,267 ms |
| **64 MB** | **1** | **831 ms** |
| 256 MB | 1 | 827 ms |

64 MB is the knee. `SET LOCAL` keeps it off every other session on the cluster.

**Extensions are effectively unavailable**, and the design does not need them. The stock image ships
only `pg_prewarm`, `pg_stat_statements` and `pgstattuple`; `pg_repack` and `pg_squeeze` are absent,
and adding them means a custom image for a shared cluster. The delta produces about 1,500 dead
tuples per region per cycle instead of 406,000, so ordinary autovacuum is sufficient. Setting a low
per-table `autovacuum_vacuum_scale_factor` (about 0.02) on the order partitions keeps those vacuums
frequent and small rather than rare and large.

**Watch index bloat, not just table bloat.** The delta's `UPDATE`s cannot use HOT, because the
columns that change most (`price`, `volume_remain`) are indexed, so every update writes a new entry
into each index. Lowering `fillfactor` does not help, precisely because HOT is unavailable.
`pgstattuple` is available for measuring it.

Three further hazards were checked against the documentation. None changes the design, but missing
any one of them causes a real defect. A fourth — autovacuum never analysing a partitioned parent —
is covered under "Maintenance".

1. **`TRUNCATE` is not MVCC-safe.** A concurrent transaction holding a snapshot from before the
   truncation sees the table as *empty*, not as its old contents. This affects the resync path
   only, and under `READ COMMITTED` — the PostgreSQL default — each statement takes a fresh
   snapshot, so the hazard is unreachable. A reader that raises its isolation level is exposed.
   The resync transaction also sets `lock_timeout`, so a `TRUNCATE` waiting for `ACCESS EXCLUSIVE`
   cannot make readers queue behind it.
2. **A long-running reader stops dead tuples being reclaimed.** Vacuum can only remove tuples older
   than the global xmin horizon. One long reader transaction, or a stalled replication slot, holds
   that horizon back and lets a bloat-free design bloat anyway. Watch `pg_stat_activity` for long
   transactions, not only `n_dead_tup`.
3. **UNLOGGED relations cannot be read during recovery at all.** A read on a hot standby raises
   `cannot access temporary or unlogged relations during recovery` rather than returning empty.
   That is safe here because only the staging and delta tables are UNLOGGED, and it is another
   reason never to make a live partition UNLOGGED. PostgreSQL 18 disallows UNLOGGED *partitioned*
   tables, meaning the parent only; individual UNLOGGED partitions stay legal, so this design
   survives that upgrade.

## Deliberately absent

- **No derived columns.** The service writes ESI fields plus `region_id`. Anything a consumer
  computes from its own reference data stays with that consumer, so a consumer's product decision
  never forces a redeploy and a re-ingest here.
- **No public API.** Consumers read the schema. The only endpoint is `/healthz`.
- **No metrics endpoint.** `region_status` and `ingest_log` are queryable in SQL, which needs no
  additional infrastructure.
- **No ESI history fallback.** `GET /markets/{region_id}/history` needs one request per
  (region, type) pair, which is 150k-400k calls a day. EVE Ref publishes the same data as one file
  per day. Gaps in the EVE Ref data are real rather than missing, so a fallback would not fill them.

## References

- <https://developers.eveonline.com/docs/> — root of the EVE developer documentation.
- <https://developers.eveonline.com/api-explorer> — interactive reference for all ESI routes.
- <https://developers.eveonline.com/docs/services/esi/best-practices/> — user agent rules, cache
  headers, error limits.
- <https://developers.eveonline.com/docs/services/esi/rate-limiting/> — token buckets, the
  `X-Ratelimit-*` headers, token costs, the legacy error limit and HTTP 420.
- <https://developers.eveonline.com/docs/services/esi/pagination/x-pages/> — offset pagination and
  its cache caveats between pages.
- <https://esi.evetech.net/meta/openapi.json> — machine-readable spec, with per-route
  `x-rate-limit` and `x-server-cache-ttl` extensions.
- <https://github.com/esi/esi-docs> — docs source, useful when a docs URL returns 404.
- <https://data.everef.net/market-history/> — the EVE Ref market history dataset.
- <https://everef.net/datasets> — EVE Ref dataset documentation.
