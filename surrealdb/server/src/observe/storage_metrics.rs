//! Backend-storage metric bridge.
//!
//! Walks the storage engine's [`surrealdb_core::kvs::Datastore::register_metrics`]
//! manifest and registers each entry as an OpenTelemetry observable gauge.
//! The callbacks fire on every collection (Prometheus scrape or OTLP push),
//! pulling fresh values from the datastore.
//!
//! # Security / exposure
//!
//! All storage-backend metrics are prefixed with `surrealdb.storage.` and
//! resolved by the Prometheus text exporter to `surrealdb_storage_*`. None
//! of these names are in [`super::public::PUBLIC_METRICS`], so the
//! `/metrics` handler filters them out for unauthenticated consumers. They
//! only reach scrapers that present root-level credentials.
//!
//! # Name sanitisation
//!
//! Storage engines report hierarchical names with dots (e.g.
//! `rocksdb.block_cache_usage`). We prepend `surrealdb.storage.` and pass
//! the rest through unchanged; the Prometheus text exporter handles the
//! dot-to-underscore conversion at render time.

use std::sync::Arc;

use surrealdb_core::kvs::Datastore;

use super::runtime::ObservabilityRuntime;

const STORAGE_METER_SCOPE: &str = "surrealdb.storage";

/// Register every metric exposed by `ds.register_metrics()` as an OTel
/// observable gauge under the `surrealdb.storage` meter scope of the
/// supplied runtime.
///
/// Returns the number of metrics registered. Zero indicates the active
/// storage flavour does not expose backend metrics.
pub fn register_storage_metrics(
	ds: &Arc<Datastore>,
	runtime: &ObservabilityRuntime,
) -> anyhow::Result<usize> {
	let Some(metrics) = ds.register_metrics() else {
		return Ok(0);
	};
	let meter = runtime.meter(STORAGE_METER_SCOPE);
	let mut registered = 0usize;
	for metric in metrics.u64_metrics {
		let ds = Arc::clone(ds);
		let name = metric.name;
		// Build the OTel name. The exporter sanitises `.` to `_` so
		// `surrealdb.storage.rocksdb.block_cache_usage` becomes
		// `surrealdb_storage_rocksdb_block_cache_usage`.
		let otel_name = format!("surrealdb.storage.{name}");
		let _gauge = meter
			.u64_observable_gauge(otel_name)
			.with_description(metric.description)
			.with_callback(move |obs| {
				if let Some(val) = ds.collect_u64_metric(name) {
					obs.observe(val, &[]);
				}
			})
			.build();
		registered += 1;
	}
	Ok(registered)
}
