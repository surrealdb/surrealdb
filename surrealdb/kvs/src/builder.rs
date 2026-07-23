//! The datastore-level abstraction implemented by every storage backend:
//! creating transactions, shutting down, and reporting metrics.

use std::any::{Any, TypeId};
use std::sync::Arc;

use common::future::BoxFut;

use crate::TransactionType;
use crate::api::Transactable;
use crate::err::Result;

/// Represents a collection of metrics for a specific datastore flavor.
///
/// This structure is used to expose datastore-specific metrics to the telemetry system.
pub struct Metrics {
	/// The name of the metrics group (e.g., "surrealdb.rocksdb").
	pub name: &'static str,
	/// A list of u64-based metrics.
	pub u64_metrics: Vec<Metric>,
}

/// Represents a single metric with a name and description.
pub struct Metric {
	/// The name of the metric.
	pub name: &'static str,
	/// A human-readable description of the metric.
	pub description: &'static str,
}

/// Abstraction over storage backends for creating and managing transactions.
///
/// This trait allows decoupling the datastore from concrete KV engines (memory,
/// RocksDB, TiKV, SurrealKV, SurrealDS, etc.). Implementors translate the
/// generic transaction parameters into a backend-specific transaction and
/// report whether the transaction is considered "local" (used internally to
/// enable some optimizations).
///
/// This was introduced to make the server more composable/embeddable. External
/// crates can implement `TransactionBuilder` to plug in custom backends while
/// reusing the rest of SurrealDB.
pub trait TransactionBuilder: Send + Sync + 'static {
	fn name(&self) -> &'static str;

	/// Create a new backend transaction.
	///
	/// - `write`: whether the transaction is writable (Write vs Read)
	///
	/// Returns the backend transaction object and a flag indicating if the
	/// transaction is local to the process (true) or requires external resources
	/// (false).
	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>>;

	/// Perform any backend-specific shutdown/cleanup.
	fn shutdown(&self) -> BoxFut<'_, Result<()>>;

	/// Registers metrics for the current datastore flavor if supported.
	///
	/// This will return a list of available metrics and their descriptions.
	fn register_metrics(&self) -> Option<Metrics>;

	/// Collects a specific u64 metric by name if supported by the datastore flavor.
	///
	/// - `metric`: The name of the metric to collect.
	fn collect_u64_metric(&self, metric: &str) -> Option<u64>;

	/// Returns an immutable backend-specific extension handle.
	///
	/// Backends expose only stable, shareable handles through this hook. The
	/// default implementation keeps community datastores free of extension
	/// state.
	///
	/// This is the extension point for backend-specific operations that
	/// don't fit the generic transaction interface: e.g. the TiKV backend
	/// returns its `TikvOpsHandle` (matched on `TypeId`) so the engine can
	/// offer MVCC-GC / lock-cleanup / `unsafe_destroy_range` to operators
	/// without polluting this trait with TiKV-only signatures every other
	/// backend would have to no-op.
	fn extension(&self, _: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
		None
	}
}
