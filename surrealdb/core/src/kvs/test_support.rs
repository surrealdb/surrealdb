//! Helpers shared by the tests of the background jobs: observers of how large
//! a job's write transactions are allowed to get, and catalog entries a job
//! must carry on past.
//!
//! Bounding a queue drain or a data reclaim is only a claim until something
//! counts the keys each of its transactions writes, and every paged pass over
//! the catalog must skip an entry a newer release wrote, so the helpers live
//! here rather than beside any one of those tests.

use std::sync::Mutex;

use surrealdb_catalog::stored::CATALOG_MARKER;

use crate::kvs::Release;
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

/// An entry in the stored grammar whose floor is one release above this
/// build, with `body` after the header and floor: every reader of the entry
/// refuses it with the release it needs before looking at the body.
pub(crate) fn with_future_floor(body: &[u8]) -> Vec<u8> {
	let future = Release::new(Release::CURRENT.major(), Release::CURRENT.minor() + 1);
	let mut out = vec![CATALOG_MARKER, 0];
	let mut value = u64::from(future.to_u32());
	loop {
		let byte = (value & 0x7f) as u8;
		value >>= 7;
		if value == 0 {
			out.push(byte);
			break;
		}
		out.push(byte | 0x80);
	}
	out.extend_from_slice(body);
	out
}
