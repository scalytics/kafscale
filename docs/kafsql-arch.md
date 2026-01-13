<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# KAFSQL Architecture + Tasklist (Draft)

KAFSQL is a standalone SQL query service for Kafscale data stored as KFS
segments in S3. It exposes the Postgres wire protocol for wide client
compatibility, while keeping Kafka semantics (topics, partitions, offsets,
timestamps) first-class.

This doc captures architecture intent and a phased tasklist. It reflects the
current KFS spec, which only defines a sparse offset index (.index) and does not
include key/timestamp indexes or segment-level min/max timestamp metadata.

## Goals

- S3-native reads of KFS segments (no broker dependency).
- Low-ceremony SQL access for ad-hoc query, BI tooling, and debugging.
- Kafka semantics in SQL (offsets/partitions as implicit columns).
- Cost transparency and query governance.

## Philosophy (product focus)

KAFSQL is for the 80% of Kafka data questions that are simple and immediate:

- What's in this topic right now?
- What happened at a specific time?
- Where is message X?
- How much data flowed in the last hour?

ksqlDB and similar tools focus on continuous processing (streaming ETL,
materialized views, and long-running joins). KAFSQL intentionally targets the
underserved pull-query use case: ad-hoc, time-bounded inspection of durable data
stored in S3. The product should stay focused on fast, stateless answers rather
than continuous stateful pipelines.

## Non-Goals (v0.6)

- General-purpose SQL joins (only Kafka-native key joins with time bounds).
- Subqueries or updates.
- Continuous queries / streaming push.
- Full SQL feature parity with Postgres.

## Data model alignment (KFS spec)

KFS layout (from `kafscale-spec.md`):

- Segment: `s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.kfs`
- Index:   `s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.index`

Only the sparse offset index is defined in the spec. Any key or timestamp index
is out of scope unless we extend the segment and index format.

## Architecture (v0.6)

```
Clients (psql/JDBC/BI)
        |
   Postgres Wire
        |
   +------------+        +--------------+        +----------------+
   |  KAFSQL    | -----> |     S3       | -----> | KFS Segments   |
   |  (Go)      |        |  (Segments)  |        | + .index files |
   +------------+        +--------------+        +----------------+
        |
        +-----> etcd (optional: topic/partition metadata)
        |
        +-----> Schema registry (optional: JSON/Avro/Proto decoding)
```

Two deployment modes:

- Internal-only: single service with S3 credentials, cluster access only.
- Proxy + internal: external proxy handles TLS/auth/ACL and forwards to internal
  KAFSQL executors (no S3 creds exposed).

## Query model

### Implicit columns

- `_topic` (TEXT)
- `_partition` (INT)
- `_offset` (BIGINT)
- `_ts` (TIMESTAMP)
- `_key` (BYTEA)
- `_value` (BYTEA)
- `_headers` (JSON)
- `_segment` (TEXT)

### Metadata commands

- `SHOW TOPICS;` (returns only topics visible to the caller)
- `SHOW PARTITIONS FROM <topic>;`
- `DESCRIBE <topic>;`
- `DESCRIBE <topic> EXTENDED;` (segment counts, size, time range if known)

### Schema handling (v0.6)

Schema is optional and can be defined alongside topic creation (YAML). When
present, KAFSQL exposes typed columns for clean SQL and joins. When absent,
users rely on `json_value()` and casts for schema-on-read.

Example (topic YAML):

```
schema:
  columns:
    - name: order_id
      type: string
      path: $.order_id
    - name: amount
      type: double
      path: $.amount
    - name: created_at
      type: timestamp
      path: $.created_at
```

`json_value()` behavior:

- Invalid JSON in `_value` returns NULL.
- Missing paths return NULL.
- Users cast explicitly: `json_value(_value, '$.amount')::DOUBLE`.

### Minimal SQL subset

- SELECT, WHERE, LIMIT, ORDER BY (timestamp only)
- Aggregates: COUNT, MIN, MAX, SUM, AVG
- GROUP BY (limited to explicit columns)

### Join support (v0.6)

KAFSQL supports Kafka-native joins that are bounded by time and keyed equality.
This is intentionally limited to keep the system stateless and S3-friendly.

Constraints:

- Join types: `INNER` and `LEFT` only.
- Two topics per query (no multi-join chains).
- Join predicate: key equality only (`_key` or explicit JSON field).
- Required time window: `WITHIN <duration>`.
- Required driver bound: `LAST <duration>` or `TAIL <n>` on the left side.

Semantics:

- Event-time join using `_ts` from each record.
- At-least-once data can produce duplicate join rows.
- The left side is the driver stream for scan scope and windowing.
- The right side is scanned only within the join window of left-side rows.

Examples:

```
SELECT o._ts, o._key, o._value, p._value
FROM orders o
JOIN payments p
  ON o._key = p._key
WITHIN 10m
LAST 1h;

SELECT o._key, o._value, s._value
FROM orders o
LEFT JOIN shipments s
  ON json_value(o._value, '$.order_id') = json_value(s._value, '$.order_id')
WITHIN 2h
LAST 24h;
```

### Kafka-native extensions

- `TAIL <n>`
- `LAST <duration>`
- `SCAN FULL` (explicit unbounded opt-in)

Also support pure SQL equivalents for client compatibility:

- `WHERE _ts >= now() - INTERVAL '15 minutes'`
- `ORDER BY _ts DESC LIMIT 100`

### Query governance

- Default requires `LAST`, `TAIL`, or explicit `SCAN FULL`.
- Server flags to allow bounded unqualified queries up to a record limit.

### KFS-native "wow" queries

- `SELECT * FROM orders TAIL 100;` (SQL tail)
- `SELECT * FROM orders LAST 15m;` (time-bounded scan)
- `SELECT _partition, count(*), max(_ts) AS latest FROM orders LAST 5m GROUP BY _partition;`
- `SELECT count(*) FROM orders WINDOW 1m LAST 1h;`
- `SELECT o._key, o._value, p._value FROM orders o JOIN payments p ON o._key = p._key WITHIN 10m LAST 1h;`

## Planning and execution (v0.6)

### Discovery

- Preferred: etcd for topic/partition metadata.
- Fallback: S3 prefix listing for discovery (cache results to avoid cost).
- Hybrid proposal: brokers load topic metadata as a bounded snapshot at startup
  and refresh with TTL or explicit reload, so S3 is never used as a continuous
  metadata source.

### Segment selection

- Time bounds are inferred by reading segment header/footer and batch timestamps.
- Offset bounds use the sparse offset index (.index) to seek to approximate
  positions inside a segment.

### Filters

- Partition filters are applied early.
- Offset ranges use index-based seeks.
- Key lookups are scan-only until a key index exists.
- Timestamp filters are scan-only until segment-level min/max ts is stored.
- Joins are evaluated as bounded hash joins on the left-side time window.

### Cost estimation

- Use S3 object sizes and number of candidate segments for estimates.
- Expose scan cost in `EXPLAIN` and warn on large scans.

## Protocol and client compatibility

- Postgres wire protocol for broad JDBC/BI support.
- Simple query protocol is required for v0.x; extended protocol support is a
  later phase to maximize tool compatibility.
- Ignore or minimally honor common `SET` statements (`client_encoding`,
  `application_name`) to keep drivers happy.

## Metrics (v0.6)

KAFSQL should emit metrics that explain both query load and S3 cost impact.
These are Prometheus-friendly counters/histograms.

Query load:

- `kafsql_queries_total{type,status}`
- `kafsql_query_duration_ms{type,status}`
- `kafsql_query_rows_total{type}`
- `kafsql_query_bytes_scanned_total{type}`
- `kafsql_query_segments_scanned_total{type}`
- `kafsql_query_unbounded_rejected_total`

S3 cost/throughput:

- `kafsql_s3_requests_total{operation}`
- `kafsql_s3_bytes_downloaded_total`
- `kafsql_s3_request_duration_ms{operation}`
- `kafsql_s3_errors_total{operation}`

Concurrency:

- `kafsql_connections_active`
- `kafsql_connections_total`
- `kafsql_active_queries`

Data correctness signals:

- `kafsql_decode_errors_total`
- `kafsql_invalid_json_total`
- `kafsql_schema_miss_total{topic}`

## Testing targets (v0.6)

- Unit coverage target: >= 80% for `internal/sql`, `internal/metadata`,
  `internal/config`, `internal/decoder`, and `internal/discovery`.
- Integration tests: embedded etcd snapshot lookup; optional MinIO for S3 scans.
- Regression tests for parser, schema extraction, and query governance.

### Simple query flow (v0.6)

Server accepts the minimal Postgres startup + query flow:

1. Client sends `StartupMessage`
2. Server replies `AuthenticationOk`
3. Server replies `ParameterStatus` for:
   - `client_encoding=UTF8`
   - `server_version=15.0` (static placeholder)
4. Server replies `ReadyForQuery`
5. Client sends `Query`
6. Server replies with `RowDescription`, zero or more `DataRow`,
   then `CommandComplete`, then `ReadyForQuery`
7. On errors: `ErrorResponse` + `ReadyForQuery`

## Decoder requirement

KAFSQL must include a KFS decoder to read segment batches and extract `_key`,
`_value`, headers, timestamps, and offsets. This can reuse the segment decode
logic from the Iceberg processor, but with a SQL row projection sink.

## Auth and ACL model (deployment)

- Internal-only mode trusts cluster networking and service accounts.
- External access is routed through a proxy for TLS termination, OIDC auth, and
  topic-level ACL checks before forwarding queries.
- Proxy should redact or reject queries before they reach S3 if access is denied.

## Gaps vs future format improvements

If we later extend KFS format, KAFSQL can take advantage of:

- Segment footer: min_ts, max_ts, min_offset, max_offset
- Optional key index and timestamp index files
- Lightweight manifest per topic/partition to avoid S3 list storms

These are not required for v0.x but enable faster planning and pruning.

## Tasklist (phased)

### Phase 0: Spec + alignment

- [x] Align KAFSQL doc with KFS spec (paths, index names, metadata constraints).
- [x] Decide on SQL grammar surface (extensions + SQL equivalents).
- [x] Confirm minimal protocol support (simple query vs extended).
- [x] Lock join constraints (two topics, key equality, WITHIN window, LAST/TAIL).
- [x] Define topic schema config shape in YAML and how it maps to SQL columns.
- [x] Define metadata snapshot contract (broker cache TTL, refresh triggers).

### Phase 1: MVP (read-only, bounded)

- [x] Choose Postgres wire library (use `github.com/jackc/pgproto3/v2`).
- [x] Define minimal protocol surface (Startup, AuthOk, Query, RowDescription, DataRow, CommandComplete, ErrorResponse, ReadyForQuery).
- [x] Implement Postgres wire server (simple query protocol).
- [x] Document simple query flow and required ParameterStatus responses.
- [x] Add env overrides for config values (defaults remain in config).
- [x] Parse SQL subset + Kafka extensions.
- [x] Resolve topics/partitions (etcd + S3 fallback + metadata snapshot cache).
- [x] Implement static metadata resolver + snapshot cache (config-based).
- [x] Implement etcd metadata resolver (snapshot key).
- [x] Discover segments and decode KFS batches (shared decoder path).
- [x] Apply filters and projections; stream results.
- [x] Implement join execution (bounded hash join over time window).
- [x] Enforce query governance (LAST/TAIL/SCAN FULL).
- [x] Implement schema-on-read + config-defined columns.
- [x] Add basic metrics and query logging.
- [x] Provide `pg_catalog` + `information_schema` support for client compatibility.
- [x] Define test strategy + coverage targets for KAFSQL v0.6.
- [x] Add unit tests for parser, planner, and decoder paths.
- [x] Add integration tests for S3 segment scans and joins (local MinIO).

### Phase 2: Performance + UX

- [ ] Add segment metadata caching.
- [ ] Improve cost estimation in EXPLAIN.
- [ ] Support JSON helpers on `_value` (safe null on non-JSON).
- [ ] Add client compatibility for common Postgres SET commands.
- [ ] Add proxy mode (TLS/OIDC/ACL) for external access.
- [ ] Add audit logging at proxy and query logs in executors.

### Phase 3: Protocol + scale

- [ ] Add minimal extended protocol support (prepare/bind/execute).
- [ ] Add result caching for repeated queries.
- [ ] Add executor pool and backpressure for large scans.

### Phase 4: Format extensions (optional) - we should not change the Kafka protocol

- [ ] Extend KFS segment footer to include min/max ts and offsets.
- [ ] Add optional key index format and reader.
- [ ] Add optional timestamp index format and reader.
- [ ] Add manifest writer for partitions (for faster discovery).

## Open questions

- Do we want schema registry integration in v0.7+ (JSON/Avro/Proto)?
- What is the minimum acceptable metadata snapshot TTL for large clusters?
- Should multi-topic joins remain limited to two topics in v0.7+?
