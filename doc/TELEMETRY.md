# Telemetry

SurrealDB leverages OpenTelemetry to instrument the code. After the
unification pass, **OTel is the source of truth** for metrics and audit /
slow-query logs: a single `SdkMeterProvider` and a single
`SdkLoggerProvider` route every measurement to multiple exporters,
including the Prometheus text exposition rendered at
[`/metrics`](OBSERVABILITY.md). An OTLP subscriber receives the entire
metric and log surface that a Prometheus operator sees, plus tracing.

For the pull-based Prometheus surface, allowlists, and multi-tenant
guidance see [`OBSERVABILITY.md`](OBSERVABILITY.md).

## What flows over OTLP

| Signal | Source | Carries |
| --- | --- | --- |
| Metrics | `SdkMeterProvider` (community + enterprise + rollups + DS + storage + pipeline scopes) | Every instrument also surfaced on `/metrics`, plus the legacy `http.server.*` / `rpc.server.*` instruments contributed by [`OtelObserver`](../surrealdb/server/src/observe/otel.rs). |
| Logs | `SdkLoggerProvider` ([`audit_logs`](../surrealdb/server/src/telemetry/audit_logs.rs)) | Audit records (scope `surrealdb.audit`), slow-query records (scope `surrealdb.slow_query`). Severity mapping in [`OBSERVABILITY.md`](OBSERVABILITY.md). |
| Traces | `tracing-opentelemetry` bridge | All `#[instrument]` spans, enriched with tenant attributes when [`OtelEnrichmentObserver`](../crates/enterprise/src/observe/otel.rs) is enabled. |

The OTLP push pipeline is configured by setting:

```
SURREAL_TELEMETRY_PROVIDER=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317   # gRPC by default
```

When `SURREAL_TELEMETRY_PROVIDER` is unset (or any value other than
`otlp`) the OTLP exporters are not built; metrics and logs continue to
flow through their other paths (Prometheus `/metrics` and the durable
file sinks for audit / slow-query).

When `SURREAL_TELEMETRY_DISABLE_METRICS=true` the OTLP metrics reader
is skipped while logs and traces continue to push.

## Push interval

Metric push frequency follows the OpenTelemetry specification and is
controlled by the standard `OTEL_METRIC_EXPORT_INTERVAL` environment
variable (milliseconds; default `60000`). The Rust OpenTelemetry SDK
reads this value on startup and applies it to the `PeriodicReader` that
backs the OTLP metrics pipeline. Sub-minute intervals (10 – 15 s) are
common in production deployments that want responsive dashboards.

## Process metric freshness

`surrealdb.process.memory` and `surrealdb.process.cpu_percent` are
observable gauges populated from a process-wide cache. A background
task spawned at startup calls
[`surrealdb_core::observe::refresh_process_snapshot`](../surrealdb/core/src/observe/process.rs)
on a fixed cadence so the cache stays fresh independently of who is
reading.

| Variable | Default | Notes |
| --- | --- | --- |
| `SURREAL_PROCESS_METRICS_REFRESH_INTERVAL` | `5` | Refresh cadence in seconds. Tighter intervals give less staleness at the cost of noisier `cpu_percent` readings (sysinfo computes CPU% as a delta since the last refresh). Floored at 1 second. |

The task only runs when at least one metrics reader is configured
(Prometheus and / or OTLP). OTLP-only deployments (no `/metrics`
scrape) get the same freshness guarantee as Prometheus scrapers.

## Histogram bucket views

The `SdkMeterProvider` is built with three views in
[`telemetry/metrics`](../surrealdb/server/src/telemetry/metrics/mod.rs):

- Instruments named `*.duration` with unit `s` use a quasi-exponential second-scale bucket
  family (5 ms – 30 s).
- Instruments named `*.duration` with unit `ms` use a parallel millisecond-scale family for the
  legacy HTTP / RPC pipeline.
- Instruments named `*.size` with unit `By` (bytes) use a 1 KiB – 100 MiB byte family.

Operators can override these by setting custom views before calling the
provider builder; refer to the OpenTelemetry SDK docs.

## Backward-compatibility instrument set (`OtelObserver`)

[`OtelObserver`](../surrealdb/server/src/observe/otel.rs) records the
legacy OTel HTTP / RPC instrument set under separate meter scopes
(`surrealdb.http`, `surrealdb.rpc`). These instruments coexist with the
new `surrealdb.*` instruments to avoid breaking existing OTLP dashboards
that pivot on the OpenTelemetry semantic-convention names.

| Instrument | Kind | Unit | Labels |
| --- | --- | --- | --- |
| `http.server.active_requests` | UpDownCounter\<i64> | - | `http.request.method`, `http.route`, `network.protocol.{name,version}` |
| `http.server.request.count` | Counter\<u64> | - | as above + `http.response.status_code` |
| `http.server.request.duration` | Histogram\<u64> | `ms` | as `http.server.request.count` |
| `http.server.request.size` | Histogram\<u64> | `By` | as `http.server.request.count` |
| `http.server.response.size` | Histogram\<u64> | `By` | as `http.server.request.count` |
| `rpc.server.active_connections` | UpDownCounter\<i64> | - | `rpc.service` |
| `rpc.server.connection.count` | Counter\<u64> | - | `rpc.service` (incremented on connect only) |
| `rpc.server.request.duration` | Histogram\<u64> | `ms` | `rpc.service`, `rpc.method`, `rpc.error` |
| `rpc.server.request.size` | Histogram\<u64> | `By` | `rpc.service` (per WebSocket frame) |
| `rpc.server.response.size` | Histogram\<u64> | `By` | `rpc.service` (per WebSocket frame) |

## Removed environment variables

The following telemetry environment variables are still parsed for
backwards compatibility but are no longer applied. The server emits a
deprecation warning at startup if either is set:

- `SURREAL_TELEMETRY_NAMESPACE` — the `namespace` attribute was removed from telemetry metrics
  because it is tenant-identifying in multi-tenant deployments.
- `SURREAL_TELEMETRY_RPC_LIVE_ID` — per-notification OTLP attribution by `rpc.live_id` was
  removed when WebSocket telemetry was unified into the `ExecutionObserver` pipeline.

## Local development

For local development, start the observability stack defined in
`dev/docker`. It spins up an OpenTelemetry collector, Grafana,
Prometheus, Tempo, and Loki:

```
$ docker-compose -f dev/docker/compose.yaml up -d
$ SURREAL_TELEMETRY_PROVIDER=otlp OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317" surreal start
```

Open http://localhost:3000 to see the telemetry data; default Grafana
credentials are `admin` / `admin`.

To adjust the verbosity of OpenTelemetry traces separately from standard
logs, use the `--log-otel-level` command-line option (or
`SURREAL_LOG_OTEL_LEVEL` environment variable). File logging can be
tuned with `--log-file-level` / `SURREAL_LOG_FILE_LEVEL`. Logs and
traces may also be streamed to a TCP socket using the `--log-socket` /
`SURREAL_LOG_SOCKET` flags.
