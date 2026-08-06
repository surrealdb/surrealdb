//! What opens a transaction.
//!
//! Holds the backend handle, the observer and the per-transaction limits, and
//! hands each new transaction its own copy. The datastore above owns one of
//! these; so does anything else that needs a transaction without owning a
//! datastore, which is how the maintenance paths open their own.

use std::any::{Any, TypeId};
use std::sync::Arc;

use anyhow::Result;
use surrealdb_kvs::{Metrics, TransactionBuilder, TransactionType};
use surrealdb_observe::{ExecutionObserver, NoopObserver};

use crate::config::TransactionConfig;
use crate::sequences::Sequences;
use crate::tr::Transactor;
use crate::triggers::CommitTriggers;
use crate::tx::Transaction;

#[derive(Clone)]
pub struct TransactionFactory {
	// The inner datastore type
	builder: Arc<Box<dyn TransactionBuilder>>,
	/// Post-commit wake-ups handed to every transaction this factory opens.
	triggers: Arc<CommitTriggers>,
	/// Observer invoked on transaction lifecycle events. Defaults to
	/// [`NoopObserver`]; replaced by the datastore's observer when one is
	/// configured.
	observer: Arc<dyn ExecutionObserver>,
	/// Limits handed to every transaction this factory opens.
	config: Arc<TransactionConfig>,
}

impl TransactionFactory {
	pub fn new(
		triggers: Arc<CommitTriggers>,
		builder: Box<dyn TransactionBuilder>,
		config: Arc<TransactionConfig>,
	) -> Self {
		Self {
			builder: Arc::new(builder),
			triggers,
			observer: Arc::new(NoopObserver),
			config,
		}
	}

	/// Replace the observer. Used by the datastore builder to propagate the
	/// chosen observer to all transactions created after the swap.
	pub fn with_observer(mut self, observer: Arc<dyn ExecutionObserver>) -> Self {
		self.observer = observer;
		self
	}

	/// Access the observer. Transaction instrumentation fires events through
	/// this handle.
	#[allow(dead_code)]
	pub fn observer(&self) -> &Arc<dyn ExecutionObserver> {
		&self.observer
	}

	#[allow(
		unreachable_code,
		unreachable_patterns,
		unused_variables,
		reason = "Some variables are unused when no backends are enabled."
	)]
	pub async fn transaction(
		&self,
		write: TransactionType,
		sequences: Sequences,
	) -> Result<Transaction> {
		// Create a new transaction on the datastore
		let (inner, local) = self.builder.new_transaction(write).await?;
		Ok(Transaction::new(
			local,
			sequences,
			Arc::clone(&self.triggers),
			Arc::clone(&self.observer),
			Transactor {
				inner,
			},
			&self.config,
		))
	}

	/// Names the storage backend transactions are opened against.
	pub fn backend_name(&self) -> &'static str {
		self.builder.name()
	}

	/// The backend's operational handle for `id`, if it offers one. Which types
	/// are on offer is the backend's business; a caller that asks for the wrong
	/// one gets `None`.
	pub fn extension(&self, id: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
		self.builder.extension(id)
	}

	/// The write-key ceiling every transaction from this factory enforces.
	/// Zero disables the limit.
	pub fn max_write_keys(&self) -> u64 {
		self.config.transaction_max_write_keys
	}

	/// Closes the storage backend. No transaction may be opened afterwards.
	pub async fn shutdown(&self) -> Result<()> {
		Ok(self.builder.shutdown().await?)
	}

	/// Registers metrics for the current datastore flavor if supported.
	pub fn register_metrics(&self) -> Option<Metrics> {
		self.builder.register_metrics()
	}

	/// Collects a specific u64 metric by name if supported by the datastore flavor.
	pub fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.builder.collect_u64_metric(metric)
	}
}
