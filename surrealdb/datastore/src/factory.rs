//! What opens a transaction.
//!
//! Holds the backend handle, the observer and the per-transaction limits, and
//! hands each new transaction its own copy. The datastore above owns one of
//! these; so does anything else that needs a transaction without owning a
//! datastore, which is how the maintenance paths open their own.

use std::any::{Any, TypeId};
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use surrealdb_kvs::{Metrics, TransactionBuilder, TransactionType};
use surrealdb_observe::{ExecutionObserver, NoopObserver};

use crate::config::TransactionConfig;
use crate::error::DatastoreError;
use crate::feature::FeatureLevelCell;
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
	/// The feature level the store was last read at, shared by every transaction this factory
	/// opens and by every clone of the factory. See [`FeatureLevelCell`].
	feature: Arc<FeatureLevelCell>,
	/// The ledger ids and recorded names the store holds that this build does not maintain, set
	/// once: the store is read-only exactly when the cell is set, so that [`Self::transaction`]
	/// refuses to open a write transaction. See [`Self::bookkeeping`].
	read_only: Arc<OnceLock<String>>,
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
			feature: Arc::new(FeatureLevelCell::unsettled()),
			read_only: Arc::new(OnceLock::new()),
		}
	}

	/// Marks the store read-only for every transaction opened by this factory or any of its
	/// clones. `features` names the ledger entries the store records that this build does not
	/// maintain; it is the first value set that is kept, and it is reported by every refusal.
	/// The mark is never cleared.
	pub fn set_read_only(&self, features: String) {
		// The first value wins; a later call changes nothing and the mark stays set.
		let _ = self.read_only.set(features);
	}

	/// Whether the store is open read-only. See [`Self::set_read_only`].
	pub fn is_read_only(&self) -> bool {
		self.read_only.get().is_some()
	}

	/// Replace the feature level cell, so that every transaction opened afterwards, and every
	/// clone of the factory made afterwards, shares `cell`.
	pub fn with_feature_level(mut self, cell: Arc<FeatureLevelCell>) -> Self {
		self.feature = cell;
		self
	}

	/// The feature level cell shared by this factory's transactions.
	pub fn feature_level(&self) -> &Arc<FeatureLevelCell> {
		&self.feature
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

	/// Opens a transaction of the given type.
	///
	/// A write transaction is refused with [`DatastoreError::StoreReadOnly`] while the store is
	/// read-only; a read transaction is never refused.
	pub async fn transaction(
		&self,
		write: TransactionType,
		sequences: Sequences,
	) -> Result<Transaction> {
		// A write transaction on a read-only store is refused before the backend opens one.
		if matches!(write, TransactionType::Write)
			&& let Some(features) = self.read_only.get()
		{
			return Err(DatastoreError::StoreReadOnly {
				features: features.clone(),
			}
			.into());
		}
		self.open(write, sequences).await
	}

	/// Opens a write transaction that ignores the read-only mark.
	///
	/// Reserved for a node's own membership rows and for deleting a torn-down session's durable
	/// copy, which must still be written on a read-only store. Nothing else may call it: a
	/// statement, a maintenance job or a feature-state write goes through [`Self::transaction`].
	pub async fn bookkeeping(&self, sequences: Sequences) -> Result<Transaction> {
		self.open(TransactionType::Write, sequences).await
	}

	#[allow(
		unreachable_code,
		unreachable_patterns,
		unused_variables,
		reason = "Some variables are unused when no backends are enabled."
	)]
	async fn open(&self, write: TransactionType, sequences: Sequences) -> Result<Transaction> {
		// The catalog commits finished before the transaction opens, read
		// before it opens so that its snapshot holds at least those.
		let settled = self.triggers.catalog_epoch.settled();
		// Create a new transaction on the datastore
		let (inner, local) = self.builder.new_transaction(write).await?;
		let sole_writer = inner.sole_writer();
		let catalog_epoch = settled.filter(|_| sole_writer).unwrap_or(crate::tx::NO_CATALOG_EPOCH);
		Ok(Transaction::new(
			local,
			sequences,
			Arc::clone(&self.triggers),
			Arc::clone(&self.observer),
			Transactor::new(inner),
			&self.config,
			catalog_epoch,
			sole_writer,
			Arc::clone(&self.feature),
		))
	}

	/// Release the DiskANN writer leases this datastore holds on indexes it has
	/// committed nothing under for `idle`, and return how many it released.
	/// See [`crate::tx::release_idle_diskann_writer_leases`].
	pub async fn release_idle_diskann_writer_leases(
		&self,
		sequences: &Sequences,
		idle: std::time::Duration,
	) -> Result<usize> {
		crate::tx::release_idle_diskann_writer_leases(self, &self.triggers, sequences, idle).await
	}

	/// The commit triggers of the datastore this factory belongs to, for the
	/// tests that drive them.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn triggers(&self) -> &Arc<CommitTriggers> {
		&self.triggers
	}

	/// A factory over the same store with its own commit triggers, as a second
	/// datastore over the store would open its transactions. For the tests that
	/// run two datastores over one in-memory store.
	#[cfg(any(test, feature = "test-hooks"))]
	pub fn with_triggers(&self, triggers: Arc<CommitTriggers>) -> Self {
		Self {
			builder: Arc::clone(&self.builder),
			triggers,
			observer: Arc::clone(&self.observer),
			config: Arc::clone(&self.config),
			feature: Arc::clone(&self.feature),
			read_only: Arc::clone(&self.read_only),
		}
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

	/// Waits until the backend is able to serve transactions.
	///
	/// See [`TransactionBuilder::wait_until_serve_ready`]: the wait is unbounded,
	/// and `Err` means the backend will never serve.
	pub async fn wait_until_serve_ready(&self) -> Result<()> {
		Ok(self.builder.wait_until_serve_ready().await?)
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
