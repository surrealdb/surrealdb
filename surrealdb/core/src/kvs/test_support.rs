//! Observation helpers shared by the tests that assert how large a background
//! job's write transactions are allowed to get.
//!
//! Bounding a queue drain or a data reclaim is only a claim until something
//! counts the keys each of its transactions writes, so the helpers live here
//! rather than beside any one of those tests.

use std::sync::Mutex;

use crate::observe::{ExecutionObserver, TransactionEvent};

/// Records the keys written by every write transaction, so a test can assert
/// per-transaction write cardinality.
///
/// `keys_written` counts individually-accounted writes, which is exactly the
/// cardinality a bound is about: a range or prefix delete reports zero however
/// many keys it expands to below this layer, so a job that stays bounded by
/// deleting per key is also the only shape this can observe.
#[derive(Default)]
pub(crate) struct WriteTxSizeObserver(Mutex<Vec<u64>>);

impl ExecutionObserver for WriteTxSizeObserver {
	fn on_transaction_complete(&self, event: &TransactionEvent) {
		if event.safe.write {
			self.0.lock().unwrap().push(event.safe.metrics.keys_written);
		}
	}
}

impl WriteTxSizeObserver {
	/// The observed write-transaction sizes, in completion order.
	pub(crate) fn sizes(&self) -> Vec<u64> {
		self.0.lock().unwrap().clone()
	}

	/// Discard everything observed so far, so a test can scope its assertions
	/// to the transactions of one job rather than to its own setup.
	pub(crate) fn clear(&self) {
		self.0.lock().unwrap().clear();
	}
}

/// Strips task-lease maintenance writes — always single-key transactions —
/// from an observed write-transaction size sequence, leaving the sizes of the
/// job's own batches.
pub(crate) fn cleanup_sizes(sizes: &[u64]) -> Vec<u64> {
	sizes.iter().copied().filter(|&n| n > 1).collect()
}
