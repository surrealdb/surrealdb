use uuid::Uuid;

use super::CreateDs;

pub async fn registers_rocksdb_metrics(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("b7afc077-3234-476f-bee0-43d7504f1e0a").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	let metrics = ds.register_metrics().expect("expected RocksDB metrics");
	assert_eq!(metrics.name, "surrealdb.rocksdb");

	let expected_metrics = [
		"rocksdb.block_cache_usage",
		"rocksdb.block_cache_pinned_usage",
		"rocksdb.estimate_table_readers_mem",
		"rocksdb.cur_size_all_mem_tables",
	];

	for metric_name in expected_metrics {
		assert!(
			metrics.u64_metrics.iter().any(|metric| metric.name == metric_name),
			"missing expected metric {metric_name}"
		);
		assert!(
			ds.collect_u64_metric(metric_name).is_some(),
			"failed to collect metric {metric_name}"
		);
	}
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn registers_rocksdb_metrics() {
			super::metrics::registers_rocksdb_metrics($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
