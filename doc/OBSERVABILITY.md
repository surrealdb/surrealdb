# Observability

SurrealDB exposes a single, unified observability surface backed by
OpenTelemetry. Every metric, audit record, and slow-query record is
recorded once and routed to multiple exporters:

- **Prometheus text exposition** at `GET /metrics` (pull, on-demand). The recommended path for
  scraping deployments running an existing Prometheus stack.
- **OpenTelemetry Protocol (OTLP) push** when `SURREAL_TELEMETRY_PROVIDER=otlp` is configured.
  Subscribers receive metrics, logs (audit + slow-query), and traces over a single connection.

For the OTLP push pipeline configuration see
[`TELEMETRY.md`](TELEMETRY.md).

## One labelled family per signal

Every metric is recorded once, in labelled form, by the unified
[`MetricsObserver`](../surrealdb/server/src/observe/metrics.rs). A
single `surrealdb.statement` counter carries `statement_type`, `outcome`,
`namespace`, `database`, and `user` labels regardless of edition;
operators run the same dashboard query in community and enterprise
deployments. When a tenant ctx field is unresolved (anonymous request,
pre-auth event, no namespace selected) the corresponding label collapses
to a `"-"` sentinel so the metric still has a stable shape.

```text
HTTP / WS / Executor → ExecutionObserver → FanOutObserver
                                            ├── MetricsObserver         (labelled `surrealdb.*`)
                                            ├── RollupsObserver         (`surrealdb.tenant.*`)
                                            ├── DsMetrics               (`surrealdb.ds.*`)
                                            ├── AuditObserver           (audit log + OTel logs)
                                            └── SlowQueryObserver       (slow-query log + OTel logs)
                                                       │
                                                       ▼
                                            SdkMeterProvider + SdkLoggerProvider
                                                       │
                                       ┌───────────────┼────────────────┐
                                       ▼               ▼                ▼
                          Prometheus text       OTLP metrics push   OTLP logs push
                          exporter (/metrics)
```

The fan-out short-circuits via
[`ExecutionObserver::is_noop`](../surrealdb/core/src/observe/observer.rs)
so a process with no exporters configured pays nothing on the hot path.
Enterprise composers contribute only the per-tenant rollups, SurrealDS
cluster internals, and the audit / slow-query pipelines; the labelled
primary surface is unchanged across editions.

---

## Edition is a resource attribute

Build flavour (community vs enterprise) is conveyed once on the OTel
`Resource` via `service.edition`, set at process start through
[`crate::telemetry::set_service_edition`](../surrealdb/server/src/telemetry/mod.rs).
It surfaces on every Prometheus series via
`target_info{service_edition="enterprise"}` and on every OTLP record via
the resource bundle.

Operators filter or group dashboards by `service_edition` instead of by
meter scope. Renaming meter scopes therefore does not affect what an
edition-aware dashboard sees.

---

## Naming convention

Instruments use OpenTelemetry semantic-convention-style dotted names
(`surrealdb.statement.duration`) registered under signal-domain meter
scopes (`surrealdb.statement`, `surrealdb.query`, `surrealdb.transaction`,
`surrealdb.rpc`, `surrealdb.session`, `surrealdb.auth`, `surrealdb.network`,
`surrealdb.http`, `surrealdb.live_query`, `surrealdb.process`,
`surrealdb.tenant`, `surrealdb.ds`, `surrealdb.audit`,
`surrealdb.slow_query`, `surrealdb.storage`). The Prometheus text
exporter converts each name deterministically to the underscore-separated,
suffixed form Prometheus operators expect.

Examples:

| OTel name | Unit | Prom name |
|---|---|---|
| `surrealdb.statement.duration` | `s` | `surrealdb_statement_duration_seconds` |
| `surrealdb.statement` | (none) | `surrealdb_statement_total` |
| `surrealdb.network.received` | `By` | `surrealdb_network_received_bytes_total` |
| `surrealdb.process.memory` | `By` | `surrealdb_process_memory_bytes` |
| `surrealdb.http.active_requests` | (none) | `surrealdb_http_active_requests` |

Counters get a `_total` suffix. Histograms / counters with unit `s` get
a `_seconds` suffix; with unit `By` get a `_bytes` suffix. Dotted names
are converted to underscores. The exporter follows OTel semantic-
convention rules. Attribute keys with dots in the name (e.g.
`http.request.method`, `http.route`, `rpc.method`) are sanitised by the
Prometheus exporter (`http_request_method`, `http_route`, `rpc_method`)
so generic OTel SemConv-aware dashboards work without bespoke relabel
rules.

### Migration from previous releases

This release collapses the prior split between aggregate and dimensional
emission, drops the edition-tier meter scopes, and removes the parallel
SemConv-only HTTP / RPC families. Operators with existing dashboards
should update their queries.

| Old Prom name | New Prom name |
|---|---|
| `surrealdb_statements_completed_total` | `surrealdb_statement_total{outcome="success"}` |
| `surrealdb_statement_errors_total` | `surrealdb_statement_total{outcome="error"}` |
| `surrealdb_queries_completed_total` | `surrealdb_query_total{outcome="success"}` |
| `surrealdb_query_errors_total` | `surrealdb_query_total{outcome="error"}` |
| `surrealdb_query_duration_seconds` | `surrealdb_query_duration_seconds{outcome,namespace,database,user}` |
| `surrealdb_query_dim_duration_seconds` | `surrealdb_query_duration_seconds` |
| `surrealdb_transactions_completed_total` | `surrealdb_transaction_total{outcome="success"}` |
| `surrealdb_transaction_writes_total` | `surrealdb_transaction_total{write="true",outcome="success"}` |
| `surrealdb_transaction_errors_total` | `surrealdb_transaction_total{outcome="error"}` |
| `surrealdb_rpcs_completed_total` | `surrealdb_rpc_total{outcome="success"}` |
| `surrealdb_rpc_errors_total` | `surrealdb_rpc_total{outcome="error"}` |
| `surrealdb_auth_attempts_total` | `surrealdb_auth_total` |
| `surrealdb_auth_failures_total` | `surrealdb_auth_total{outcome!="success"}` |
| `surrealdb_http_requests_completed_total` | `surrealdb_http_request_total{outcome="success"}` |
| `surrealdb_http_request_errors_total` | `surrealdb_http_request_total{outcome="error"}` |
| `surrealdb_http_request_duration_seconds` | `surrealdb_http_request_duration_seconds{...}` (now labelled) |
| `surrealdb_http_request_dim_duration_seconds` | `surrealdb_http_request_duration_seconds` |
| `surrealdb_http_dim_active_requests` | `surrealdb_http_active_requests` |
| `surrealdb_network_dim_received_bytes_total` | `surrealdb_network_received_bytes_total` |
| `surrealdb_network_dim_sent_bytes_total` | `surrealdb_network_sent_bytes_total` |
| `http_server_request_duration_milliseconds` | `surrealdb_http_request_duration_seconds{...}` |
| `http_server_request_count_total` | `surrealdb_http_request_total{...}` |
| `http_server_active_requests` | `surrealdb_http_active_requests{...}` |
| `rpc_server_request_duration_milliseconds` | `surrealdb_rpc_duration_seconds{...}` |
| `rpc_server_active_connections` | `surrealdb_session_active{protocol="websocket"}` |

The `otel_scope_name` label changes shape too: scopes are now
signal-domain (`surrealdb.statement`, `surrealdb.query`, …) rather than
edition-tier (`surrealdb.community`, `surrealdb.enterprise`). Dashboards
that filtered by scope should switch to the metric name (which carries
the same information now that scopes mirror the family prefix).

---

## On / off model

Metrics are either on or off for the entire process.

| Switch | Default | Effect |
| --- | --- | --- |
| `SURREAL_METRICS_ENABLED` | `true` | When `false`, the `/metrics` route is not mounted |
| `SURREAL_TELEMETRY_PROVIDER` | unset | Set to `otlp` to enable the OTLP push pipeline (metrics, logs, traces) |
| `SURREAL_TELEMETRY_DISABLE_METRICS` | `false` | When `true`, suppresses the OTLP metrics reader (logs / traces unaffected) |

When metrics are disabled the `/metrics` route returns `404`. Recording
sites may still emit events into the fan-out; observers without a target
exporter are no-ops.

When metrics are enabled the behaviour you see at `/metrics` depends on
the authenticity of the request. Anonymous scrapers see only a small
allowlist of non-attributable process gauges; root-authenticated
scrapers see the full surface contributed by the running build.

Authentication uses the standard root credentials (`--user` / `--pass`
or `SURREAL_USER` / `SURREAL_PASS`). Namespace and database users are
explicitly treated as unauthenticated for this endpoint.

---

## Security boundary: instrument-name allowlist

The Prometheus surface is guarded by a single render-time allowlist:
[`PUBLIC_METRICS`](../surrealdb/server/src/observe/public.rs). When an
unauthenticated consumer scrapes `/metrics`, only metric families whose
**name** is on the list are included; everything else requires
operator authentication (root level) to view. The list keys off the
rendered Prometheus family name (so renaming OTel scopes does not
change the boundary).

The labelled families registered by the unified `MetricsObserver`
(`surrealdb.statement`, `surrealdb.query`, …) are NOT in the public
list. They carry resolved tenant labels (`namespace`, `database`,
`user`) by design — the labels are useful in operator dashboards and
in OTLP exports — but they never reach anonymous consumers because the
family name itself is filtered out at render time.

Tenant ctx values are bounded at the source. Anonymous sessions leave
`namespace` / `database` / `user` unset (rendered as the `"-"`
sentinel); record-access principals collapse to a fixed `<record>`
sentinel rather than emitting raw record ids. See
[`TenantIdentity::from_session`](../surrealdb/core/src/observe/events.rs).
Bounded enums like `Outcome`, `StatementType`, `SessionProtocol`,
`HttpMethod`, and `HttpVersion` are the only thing the executor ever
puts on safe-half attributes, so values stay closed at compile time.

Adding a new instrument to `PUBLIC_METRICS` requires a security review.
The CODEOWNERS for `public.rs` direct review to `@surrealdb/security`.

---

## Metric catalogue

### Publicly-scrapable (the `PUBLIC_METRICS` allowlist)

Visible to anonymous scrapers, by name. Aggregate process signals only —
no per-call, per-tenant, or per-protocol detail.

| Prom name | Type | Labels | Notes |
| --- | --- | --- | --- |
| `surrealdb_build_info` | gauge | `build_version` | Always `1`. Label is the static compile-time version. |
| `surrealdb_process_uptime_seconds` | gauge | none | Seconds since process start. |
| `surrealdb_process_memory_bytes` | gauge | none | Resident set size across the whole process. |
| `surrealdb_process_cpu_percent` | gauge | none | Aggregate process CPU %; may exceed 100 on multi-core. |
| `target_info` | gauge | resource attrs | Emitted by the OTel SDK from the process Resource. Carries `service_edition`. |
| `otel_scope_info` | gauge | scope attrs | Emitted by the OTel SDK per instrumentation scope. |

### Authenticated-only — primary signal families

Recorded by the unified
[`MetricsObserver`](../surrealdb/server/src/observe/metrics.rs) under
signal-domain meter scopes. All families carry `outcome`, plus the
resolved tenant ctx (`namespace`, `database`, `user`) where
applicable. Sentinel `"-"` values appear when ctx is unresolved.

| Family (Prom name) | Scope | Type | Notable labels |
| --- | --- | --- | --- |
| `surrealdb_statement_total` | `surrealdb.statement` | counter | `statement_type, outcome, namespace, database, user` |
| `surrealdb_statement_duration_seconds` | `surrealdb.statement` | histogram | same as above |
| `surrealdb_statement_rows_total` | `surrealdb.statement` | counter | same as above (DML only) |
| `surrealdb_query_total` | `surrealdb.query` | counter | `outcome, namespace, database, user` |
| `surrealdb_query_duration_seconds` | `surrealdb.query` | histogram | same as above |
| `surrealdb_transaction_total` | `surrealdb.transaction` | counter | `write, outcome, namespace, database, user` |
| `surrealdb_transaction_duration_seconds` | `surrealdb.transaction` | histogram | same as above |
| `surrealdb_transaction_kv_ops_total` | `surrealdb.transaction` | counter | `op` |
| `surrealdb_transaction_keys_read_total` | `surrealdb.transaction` | counter | `outcome` |
| `surrealdb_transaction_keys_written_total` | `surrealdb.transaction` | counter | `outcome` |
| `surrealdb_transaction_bytes_read_total` | `surrealdb.transaction` | counter | `outcome` |
| `surrealdb_transaction_bytes_written_total` | `surrealdb.transaction` | counter | `outcome` |
| `surrealdb_rpc_total` | `surrealdb.rpc` | counter | `rpc_method, outcome, namespace, database, user` |
| `surrealdb_rpc_duration_seconds` | `surrealdb.rpc` | histogram | same as above |
| `surrealdb_auth_total` | `surrealdb.auth` | counter | `auth_action, auth_scope, outcome, namespace, database, user` |
| `surrealdb_session_total` | `surrealdb.session` | counter | `session_action, protocol, service` |
| `surrealdb_session_active` | `surrealdb.session` | gauge | `protocol, service` |
| `surrealdb_session_duration_seconds` | `surrealdb.session` | histogram | `protocol, service` |
| `surrealdb_network_received_bytes_total` | `surrealdb.network` | counter | `protocol, namespace, database, user` |
| `surrealdb_network_sent_bytes_total` | `surrealdb.network` | counter | same as above |
| `surrealdb_http_request_total` | `surrealdb.http` | counter | `http_request_method, http_route, http_response_status_code, outcome, namespace, database, user` |
| `surrealdb_http_request_duration_seconds` | `surrealdb.http` | histogram | same as above |
| `surrealdb_http_request_size_bytes_total` | `surrealdb.http` | counter | same as above |
| `surrealdb_http_response_size_bytes_total` | `surrealdb.http` | counter | same as above |
| `surrealdb_http_active_requests` | `surrealdb.http` | gauge | `http_request_method, http_route` (attribute-stripped to keep the gauge balanced) |
| `surrealdb_live_query_active` | `surrealdb.live_query` | gauge | none |
| `surrealdb_live_query_notifications_total` | `surrealdb.live_query` | counter | none |

### Authenticated-only — pipeline self-metrics

Registered when the audit log and / or slow-query log pipeline is
configured. Read live from the queue / sink counters via OTel observable
gauges.

| Family | Scope | Notes |
| --- | --- | --- |
| `surrealdb_audit_records` | `surrealdb.audit` | Cumulative count of audit records successfully enqueued. |
| `surrealdb_audit_dropped` | `surrealdb.audit` | Cumulative count of audit records dropped on queue overflow. |
| `surrealdb_audit_queue_depth` | `surrealdb.audit` | Records currently buffered in the queue. |
| `surrealdb_audit_appended` | `surrealdb.audit` | Records the worker successfully wrote to the file sink. |
| `surrealdb_audit_append_errors` | `surrealdb.audit` | Records the worker failed to write — alert on any non-zero value. |
| `surrealdb_slow_query_*` | `surrealdb.slow_query` | Same family for slow-query records. |

### Authenticated-only — per-tenant rollups (`surrealdb.tenant` scope)

Pre-aggregated per-tenant counters keyed on `(namespace, database)`
only. A bounded `__overflow__` series catches tenants beyond the
configured cap so cardinality stays predictable. These exist alongside
the labelled primary families above so billing pipelines can pull a
guaranteed-low-cardinality stream without aggregating across the
higher-cardinality dimensional surface.

Examples:

- `surrealdb_tenant_statements_total{namespace, database}`
- `surrealdb_tenant_statement_rows_total{namespace, database}`
- `surrealdb_tenant_transactions_total{namespace, database}`
- `surrealdb_tenant_network_received_bytes_total{namespace, database}`

### Authenticated-only — SurrealDS cluster (`surrealdb.ds` scope)

Cluster-internal observability for TAPIR replicas:
`surrealdb_ds_network_bytes_sent_bytes_total`,
`surrealdb_ds_messages_sent_total{message_type, peer}`,
`surrealdb_ds_view_changes_total{outcome}`, `surrealdb_ds_gc_*`, etc.

### Authenticated-only — storage backend (`surrealdb.storage` scope)

Storage backends publish a manifest of `u64` metrics through
[`TransactionBuilder::register_metrics`](../surrealdb/core/src/kvs/ds.rs).
The server bridge registers each entry as an OTel observable gauge under
the `surrealdb.storage.<backend>.<metric>` namespace; the Prometheus
exporter renders them as `surrealdb_storage_<backend>_<metric>`.

---

## Audit and slow-query records

Audit and slow-query records flow through TWO parallel paths:

1. **Durable file sink** — bounded queue, background worker, hash-chained NDJSON file with
   rotation and optional fsync. Compliance / SIEM operators consume this for tamper-evident
   long-term retention. See `SURREAL_AUDIT_*` and `SURREAL_SLOW_QUERY_*` env knobs.

2. **OpenTelemetry logs** — same record emitted as an
   [`opentelemetry::logs::LogRecord`](https://docs.rs/opentelemetry/0.31.0/opentelemetry/logs/trait.LogRecord.html)
   on the `SdkLoggerProvider` built at server start. When OTLP is configured, the records ride
   the same connection as metrics + traces so an OTLP subscriber receives the full audit /
   slow-query stream alongside metrics.

Severity mapping for the OTel logs path:

- `Statement` / `Query` / `Transaction` / `Session` / `Http` (success): `INFO`
- `Auth` failure: `WARN`
- HTTP 5xx, append errors, queue overflow: `ERROR`
- Slow-query records: `WARN` (slow-query is by definition a degraded path)

Redaction runs **synchronously on the executor thread before either
path**, so the file sink and the OTel log emitter both see the same
already-scrubbed record. See
[`audit_log/redact.rs`](../crates/enterprise/src/observe/audit_log/redact.rs)
for the redaction passes (`literal → tokens → regex`).

### Durability settings

When the audit log or slow-query log is configured with a file sink,
two settings govern how durable the records are after the process emits
them:

- `SURREAL_AUDIT_FSYNC_EVERY` / `SURREAL_SLOW_QUERY_FSYNC_EVERY` control mid-stream `sync_data`
  cadence. `0` (default) skips mid-stream fsync; `1` fsyncs after every record (strict);
  larger values batch syncs every N records. **Rotation and clean shutdown always flush + fsync
  regardless of this setting.**
- `SURREAL_AUDIT_OVERFLOW` / `SURREAL_SLOW_QUERY_OVERFLOW` control what happens when the
  bounded observer→worker queue fills: `drop` (default) silently drops records and increments
  the `*_dropped` self-metric; `block` applies back-pressure all the way to the executor
  (recommended for compliance deployments).

Compliance operators should monitor both `*_dropped` (queue overflow)
and `*_append_errors` (sink write failures) — both are *lost* records.
The `*_records - *_appended` gap is queue depth plus failures.

The file sinks open files with mode `0600` (Unix) and refuse to
auto-create the parent directory; configuring a path under a
nonexistent parent fails loud at startup.

### PII redaction

Both the audit log and slow-query log can capture full SQL text
(`SURREAL_AUDIT_INCLUDE_SQL`; the slow-query log always captures it
because that is the entire point of the log). User-supplied SurrealQL
can embed PII inside string literals (`'alice@example.com'`,
`'4111111111111111'`, …). Redaction runs **synchronously on the
executor thread before the queue**, so once a record reaches the worker
the SQL has already been scrubbed.

Three layered redaction passes run in order:

1. **Literal pass** — when `SURREAL_AUDIT_REDACT_LITERALS=true`, every single- or double-quoted
   span in the SQL is replaced with `'***'` / `"***"`. **Off by default.**
2. **Identifier-token pass** — `SURREAL_AUDIT_REDACT_TABLES=secrets,pii_addresses` (CSV) replaces
   every case-insensitive occurrence of those identifier tokens with `***`.
3. **Regex pass** — `SURREAL_AUDIT_REDACT_REGEX="<pat1>;<pat2>"` (semicolon-separated; each
   pattern is compiled at startup, invalid patterns fail the server early) is applied to
   whatever the previous passes produced.

Equivalent slow-query keys are `SURREAL_SLOW_QUERY_REDACT_TABLES` and
`SURREAL_SLOW_QUERY_REDACT_REGEX`.

---

## Multi-tenant guidance

A single SurrealDB process that runs multiple tenants MUST treat the
`/metrics` endpoint as an operator-only surface:

- Do not expose the main server port directly to tenants. Aggregate process signals (CPU,
  memory, uptime) in the public allowlist are not tenant-specific, but they still reveal
  host-level load that can be used to infer activity.
- Keep `SURREAL_METRICS_ENABLED` set to `false` unless you can guarantee that only operators
  reach the process. Prefer disabling the endpoint entirely over relying on the allowlist
  alone.
- If metrics must be reachable, bind the `/metrics` endpoint to a non-tenant-visible interface
  via the reverse proxy or service mesh.
- Root credentials are the authentication boundary for the richer view. Ensure tenants never
  receive root credentials.

---

## Local development

Starting the server with metrics enabled and a root user:

```bash
cargo run --no-default-features --features storage-mem,http,scripting -- \
    start --log trace --user root --pass root memory
curl http://127.0.0.1:8000/metrics                          # public view
curl -u root:root http://127.0.0.1:8000/metrics             # operator view
```

Disabling metrics entirely:

```bash
SURREAL_METRICS_ENABLED=false cargo run -- start ...        # /metrics -> 404
```

Pushing to an OTLP collector (metrics + logs + traces):

```bash
SURREAL_TELEMETRY_PROVIDER=otlp \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
cargo run -- start ...
```
