use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::kvs::Transaction;

/// Wraps a KV transaction with additional state for client-side transaction
/// management. Tracks whether the transaction has been "poisoned" by a
/// previous query error, so that subsequent operations and commit attempts
/// produce clear error messages rather than silently succeeding.
pub struct ClientTransaction {
	pub tx: Arc<Transaction>,
	poisoned: AtomicBool,
}

impl ClientTransaction {
	pub fn new(tx: Arc<Transaction>) -> Self {
		Self {
			tx,
			poisoned: AtomicBool::new(false),
		}
	}

	/// Mark this transaction as poisoned due to a query error.
	pub fn poison(&self) {
		self.poisoned.store(true, Ordering::SeqCst);
	}

	/// Returns true if the transaction was poisoned by a prior error.
	pub fn is_poisoned(&self) -> bool {
		self.poisoned.load(Ordering::SeqCst)
	}
}
