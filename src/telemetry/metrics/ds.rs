use opentelemetry::global;
use std::sync::Arc;
use surrealdb_core::kvs::Datastore;

pub fn register_datastore_metrics(ds: Arc<Datastore>) {
	if let Some(metrics) = ds.register_metrics() {
		let meter = global::meter(metrics.name);
		for u64_metric in metrics.u64_metrics {
			let ds = ds.clone();
			let _ = meter
				.u64_observable_gauge(u64_metric.name)
				.with_description(u64_metric.description)
				.with_callback(move |observer| {
					if let Some(val) = ds.collect_u64_metric(u64_metric.name) {
						observer.observe(val, &[]);
					}
				})
				.init();
		}
	}
}
