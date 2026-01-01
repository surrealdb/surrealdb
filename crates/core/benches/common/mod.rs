//! Common utilities for benchmarks
//!
//! This module provides shared infrastructure for all benchmark files including:

use surrealdb_core::dbs::{Capabilities, Session};
use surrealdb_core::kvs::Datastore;
use tokio::runtime::Runtime;

/// Create a new multithreaded Tokio runtime for benchmarks
pub fn create_runtime() -> Runtime {
	tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap()
}

/// Helper to run async code synchronously (for setup only)
pub fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
	create_runtime().block_on(future)
}

/// Macro for executing a benchmark query
///
/// Usage:
/// ```ignore
/// execute!(&dbs, &ses, "SELECT * FROM {table};");
/// ```
#[macro_export]
macro_rules! query {
	($dbs: expr, $ses: expr, $($fmt:tt)*) => {
		criterion::black_box($dbs.execute(&format!($($fmt)*), $ses, None).await).unwrap()
	};
}

/// Macro for executing a benchmark query
///
/// Usage:
/// ```ignore
/// execute!(&dbs, &ses, "SELECT * FROM {table};");
/// ```
#[macro_export]
macro_rules! execute {
	($dbs: expr, $ses: expr, $($fmt:tt)*) => {
		$crate::common::block_on(async {
			criterion::black_box($dbs.execute(&format!($($fmt)*), $ses, None).await).unwrap()
		})
	};
}

/// Macro for configuring and executing an async benchmark query
///
/// Usage:
/// ```ignore
/// bench!(group, benchmark_name, &dbs, &ses, "SELECT * FROM {table};");
/// bench!(group, benchmark_name, &dbs, &ses, throughput: 10, "SELECT * FROM table WHERE {condition};");
/// bench!(group, benchmark_name, &dbs, &ses, expected: |result| result.is_ok(), "SELECT * FROM {table};");
/// bench!(group, benchmark_name, &dbs, &ses, throughput: 10, expected: |result| result.len() == 1, "SELECT * FROM {table};");
/// ```
#[macro_export]
macro_rules! bench {
	// Variant with both `throughput` and `expected` parameters
	($group: expr, $name: ident, $dbs: expr, $ses: expr, throughput: $throughput:expr, expected: $expected:expr, $($fmt:tt)*) => {
		// Format the query
		let query = format!($($fmt)*);
		// Configure throughput for the benchmark using provided value
		$group.throughput(criterion::Throughput::Elements($throughput));
		// Benchmark the query with the given name
		$group.bench_function(stringify!($name), |b| {
			// Create a multithreaded runtime for async benchmarking
			let runtime = $crate::common::create_runtime();
			// Run an initial query for validation
			let result: surrealdb_types::Value = execute!($dbs, $ses, $($fmt)*).remove(0).result.unwrap();
			// Validate the result against the expected expression
			let check: fn(&surrealdb_types::Value) -> bool = $expected;
			assert!(check(&result), "Result did not match expected value: {result:?}");
			// Iterate over the benchmark
			b.to_async(&runtime).iter(|| async {
				criterion::black_box($dbs.execute(&query, $ses, None).await).unwrap()
			})
		});
	};
	// Variant with only `throughput` parameter
	($group: expr, $name: ident, $dbs: expr, $ses: expr, throughput: $throughput:expr, $($fmt:tt)*) => {
		// Format the query
		let query = format!($($fmt)*);
		// Configure throughput for the benchmark using provided value
		$group.throughput(criterion::Throughput::Elements($throughput));
		// Benchmark the query with the given name
		$group.bench_function(stringify!($name), |b| {
			// Create a multithreaded runtime for async benchmarking
			let runtime = $crate::common::create_runtime();
			// Iterate over the benchmark
			b.to_async(&runtime).iter(|| async {
				criterion::black_box($dbs.execute(&query, $ses, None).await).unwrap()
			})
		});
	};
	// Variant with only `expected` parameter
	($group: expr, $name: ident, $dbs: expr, $ses: expr, expected: $expected:expr, $($fmt:tt)*) => {
		$crate::bench!($group, $name, $dbs, $ses, throughput: 1, expected: $expected, $($fmt)*);
	};
	// Default variant: original behaviour
	($group: expr, $name: ident, $dbs: expr, $ses: expr, $($fmt:tt)*) => {
		$crate::bench!($group, $name, $dbs, $ses, throughput: 1, $($fmt)*);
	};
}

/// Helper function to setup a datastore
///
/// Usage:
/// ```ignore
/// let (dbs, ses) = setup_datastore().await;
/// ```
#[allow(dead_code)]
pub async fn setup_datastore() -> (Datastore, Session) {
	// Setup the in-memory datastore
	let dbs = Datastore::new("memory").await.unwrap();
	// Enable all datastore capabilities
	let dbs = dbs.with_capabilities(Capabilities::all());
	// Setup a root-level datastore session
	let ses = Session::owner().with_ns("test").with_db("test");
	// Specify the test namespace and database
	dbs.execute("USE NAMESPACE test DATABASE test", &ses, None).await.unwrap();
	// Return the datastore and session
	(dbs, ses)
}

/// Helper function to setup a datastore with a query
///
/// Usage:
/// ```ignore
/// let (dbs, ses) = setup_datastore_with_query("CREATE person:tobie;").await;
/// ```
#[allow(dead_code)]
pub async fn setup_datastore_with_query(query: &str) -> (Datastore, Session) {
	// Setup the in-memory datastore
	let dbs = Datastore::new("memory").await.unwrap();
	// Enable all datastore capabilities
	let dbs = dbs.with_capabilities(Capabilities::all());
	// Setup a root-level datastore session
	let ses = Session::owner().with_ns("test").with_db("test");
	// Specify the test namespace and database
	dbs.execute("USE NAMESPACE test DATABASE test", &ses, None).await.unwrap();
	// Load data using executor (setup phase, not benchmarked)
	dbs.execute(&query, &ses, None).await.unwrap();
	// Return the datastore and session
	(dbs, ses)
}

/// Helper function to setup a datastore with fake records
///
/// Usage:
/// ```ignore
/// let (dbs, ses) = setup_datastore_with_records(100_000).await;
/// ```
#[allow(dead_code)]
pub async fn setup_datastore_with_records(count: u64) -> (Datastore, Session) {
	// Setup the in-memory datastore
	let dbs = Datastore::new("memory").await.unwrap();
	// Enable all datastore capabilities
	let dbs = dbs.with_capabilities(Capabilities::all());
	// Setup a root-level datastore session
	let ses = Session::owner().with_ns("test").with_db("test");
	// Specify the test namespace and database
	dbs.execute("USE NAMESPACE test DATABASE test", &ses, None).await.unwrap();
	// Load data using executor (setup phase, not benchmarked)
	if count > 0 {
		let mut setup = String::new();
		for i in 0..count {
			setup.push_str(&format!(
				r#"CREATE item:{i} SET
					name = 'Item {i}',
					level = {},
					active = {},
					stats = {{
						score: {},
						rank: {},
						details: {{
							created: true,
							verified: {}
						}}
					}};
				"#,
				1 + (i % 100),
				i % 2 == 0,
				i % 100,
				i % 10,
				i % 3 == 0
			));
		}
		dbs.execute(&setup, &ses, None).await.unwrap();
	}
	// Return the datastore and session
	(dbs, ses)
}
