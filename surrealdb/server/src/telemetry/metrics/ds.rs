use std::sync::Arc;

use opentelemetry::global;
use surrealdb_core::kvs::Datastore;

/// Registers datastore-specific metrics with the global OpenTelemetry meter.
///
/// This function checks if the datastore provides any metrics and, if so,
/// registers them as observable gauges. Observable gauges are useful for
/// metrics that are updated at a regular interval or on demand, such as
/// memory usage or cache statistics.
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
				.build();
		}
	}
}

#[cfg(all(test, feature = "storage-rocksdb"))]
mod tests {
	use std::sync::Arc;

	use opentelemetry::global;
	use opentelemetry_sdk::error::OTelSdkError;
	use opentelemetry_sdk::metrics::data::ResourceMetrics;
	use opentelemetry_sdk::metrics::reader::MetricReader;
	use opentelemetry_sdk::metrics::{InstrumentKind, ManualReader, SdkMeterProvider, Temporality};
	use surrealdb_core::kvs::Datastore;

	use super::register_datastore_metrics;

	#[derive(Clone, Debug)]
	struct TestReader {
		inner: Arc<ManualReader>,
	}

	impl MetricReader for TestReader {
		fn register_pipeline(
			&self,
			pipeline: std::sync::Weak<opentelemetry_sdk::metrics::Pipeline>,
		) {
			self.inner.register_pipeline(pipeline);
		}

		fn collect(
			&self,
			rm: &mut opentelemetry_sdk::metrics::data::ResourceMetrics,
		) -> Result<(), OTelSdkError> {
			self.inner.collect(rm)
		}

		fn force_flush(&self) -> Result<(), OTelSdkError> {
			self.inner.force_flush()
		}

		fn shutdown(&self) -> Result<(), OTelSdkError> {
			self.inner.shutdown()
		}

		fn shutdown_with_timeout(&self, timeout: std::time::Duration) -> Result<(), OTelSdkError> {
			self.inner.shutdown_with_timeout(timeout)
		}

		fn temporality(&self, kind: InstrumentKind) -> Temporality {
			self.inner.temporality(kind)
		}
	}

	#[tokio::test]
	#[serial_test::serial]
	async fn registers_rocksdb_metrics_with_opentelemetry() {
		let temp_dir = tempfile::TempDir::new().unwrap();
		let path = format!("rocksdb:{}", temp_dir.path().to_string_lossy());
		let ds = Arc::new(Datastore::new(&path).await.unwrap());

		let reader = TestReader {
			inner: Arc::new(ManualReader::builder().build()),
		};
		let provider = SdkMeterProvider::builder().with_reader(reader.clone()).build();
		global::set_meter_provider(provider.clone());

		register_datastore_metrics(ds);

		let mut resource_metrics = ResourceMetrics::default();
		reader.collect(&mut resource_metrics).unwrap();

		let expected_metrics = [
			"rocksdb.block_cache_usage",
			"rocksdb.block_cache_pinned_usage",
			"rocksdb.estimate_table_readers_mem",
			"rocksdb.cur_size_all_mem_tables",
		];

		for metric_name in expected_metrics {
			let _metric = resource_metrics
				.scope_metrics()
				.flat_map(|scope| scope.metrics())
				.find(|metric| metric.name() == metric_name)
				.unwrap_or_else(|| panic!("missing expected metric {metric_name}"));
			// Successfully found the metric - that's the main assertion we need
		}

		provider.shutdown().unwrap();
	}
}
