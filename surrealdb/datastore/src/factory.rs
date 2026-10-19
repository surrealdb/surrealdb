//! What opens a transaction.
//!
//! Holds the backend handle, the observer and the per-transaction limits, and
//! hands each new transaction its own copy. The datastore above owns one of
//! these; so does anything else that needs a transaction without owning a
//! datastore, which is how the maintenance paths open their own.

use std::any::{Any, TypeId};
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Result;
use surrealdb_kvs::{
	Isolation, IsolationLevelsHandle, Metrics, ReadOutcome, ReadSnapshotRequest, SnapshotInfo,
	StaleReads, StaleReadsHandle, TransactionBuilder, TransactionType,
};
use surrealdb_observe::{ExecutionObserver, NoopObserver};
use web_time::Instant;

use crate::config::{FallbackPolicy, TransactionConfig};
use crate::error::DatastoreError;
use crate::feature::FeatureLevelCell;
use crate::out_of_line::OutOfLine;
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
	/// The window the rate-limited fall-back policy counts in.
	fallbacks: Arc<std::sync::Mutex<(Instant, u32)>>,
}

/// A request to open a read-only transaction over a snapshot no older than
/// `max_staleness`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadRequest {
	pub max_staleness: Duration,
	/// Refuse a current read when no snapshot meets the bound.
	pub strict: bool,
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
			fallbacks: Arc::new(std::sync::Mutex::new((Instant::now(), 0))),
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
	#[inline(never)]
	pub fn transaction(
		&self,
		write: TransactionType,
		sequences: Sequences,
	) -> OutOfLine<impl Future<Output = Result<Transaction>>> {
		OutOfLine::new(self.transaction_body(write, sequences))
	}

	async fn transaction_body(
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

	/// Opens a transaction at `isolation`.
	///
	/// [`Isolation::Snapshot`] is what every backend's ordinary transaction
	/// provides, so a backend that publishes no [`IsolationLevelsHandle`] is
	/// opened as [`Self::transaction`] opens it. Any other level needs a handle
	/// that supports it, and is refused with
	/// [`DatastoreError::IsolationNotSupported`] otherwise, naming the levels the
	/// datastore does offer; a level is never silently downgraded.
	pub async fn transaction_at(
		&self,
		write: TransactionType,
		isolation: Isolation,
		sequences: Sequences,
	) -> Result<Transaction> {
		let handle = self
			.builder
			.extension(TypeId::of::<IsolationLevelsHandle>())
			.and_then(|ext| ext.downcast::<IsolationLevelsHandle>().ok());
		let Some(handle) = handle else {
			if isolation == Isolation::Snapshot {
				return self.transaction(write, sequences).await;
			}
			return Err(DatastoreError::IsolationNotSupported {
				requested: isolation.to_string(),
				supported: Isolation::Snapshot.to_string(),
			}
			.into());
		};
		let levels = handle.0.supported();
		if !levels.contains(&isolation) {
			return Err(DatastoreError::IsolationNotSupported {
				requested: isolation.to_string(),
				supported: levels.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "),
			}
			.into());
		}
		if matches!(write, TransactionType::Write)
			&& let Some(features) = self.read_only.get()
		{
			return Err(DatastoreError::StoreReadOnly {
				features: features.clone(),
			}
			.into());
		}
		let settled = self.triggers.catalog_epoch.settled();
		let live_bucket = self.triggers.live.open();
		let (inner, local) = match handle.0.open(write, isolation).await {
			Ok(opened) => opened,
			Err(e) => {
				self.triggers.live.close(live_bucket);
				return Err(e.into());
			}
		};
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
		)
		.with_live_bucket(live_bucket))
	}

	/// Opens a read-only transaction over a snapshot no older than the
	/// request's bound.
	///
	/// A backend that publishes no [`StaleReadsHandle`] is read at its current
	/// state, which meets every bound; the fall-back policy is not consulted.
	/// Otherwise the backend's newest snapshot within the bound is used, and
	/// when there is none the configured [`FallbackPolicy`] decides between a
	/// current read and [`DatastoreError::ReadSnapshotTooStale`]. A `STRICT`
	/// request never gets a current read after the stale tier has declined.
	///
	/// A bound above the configured maximum fails with
	/// [`DatastoreError::StalenessAboveMaximum`] before anything is opened.
	pub async fn read_transaction(
		&self,
		request: ReadRequest,
		sequences: Sequences,
	) -> Result<(Transaction, SnapshotInfo)> {
		let maximum = self.config.read_snapshot_max_staleness;
		if request.max_staleness > maximum {
			return Err(DatastoreError::StalenessAboveMaximum {
				bound: request.max_staleness,
				maximum,
			}
			.into());
		}
		if request.max_staleness.is_zero() {
			let tx = self.transaction(TransactionType::Read, sequences).await?;
			return Ok((tx, SnapshotInfo::current()));
		}
		let tier = self
			.builder
			.extension(TypeId::of::<StaleReadsHandle>())
			.and_then(|ext| ext.downcast::<StaleReadsHandle>().ok());
		let Some(tier) = tier else {
			let tx = self.transaction(TransactionType::Read, sequences).await?;
			return Ok((tx, SnapshotInfo::current()));
		};
		let tier: &Arc<dyn StaleReads> = &tier.0;
		let newest_age = match self
			.open_stale(
				tier,
				ReadSnapshotRequest {
					max_staleness: request.max_staleness,
				},
				sequences.clone(),
			)
			.await?
		{
			Ok(opened) => return Ok(opened),
			Err(newest_age) => newest_age,
		};
		let allowed = !request.strict
			&& match self.config.read_snapshot_fallback {
				FallbackPolicy::Allow => true,
				FallbackPolicy::Deny => false,
				FallbackPolicy::RateLimited(per_second) => self.admit_fallback(per_second),
			};
		if !allowed {
			return Err(DatastoreError::ReadSnapshotTooStale {
				bound: request.max_staleness,
				newest_age,
			}
			.into());
		}
		let tx = self.transaction(TransactionType::Read, sequences).await?;
		Ok((tx, SnapshotInfo::current()))
	}

	/// Whether one more fall-back fits in the current one-second window.
	fn admit_fallback(&self, per_second: u32) -> bool {
		let mut window = self.fallbacks.lock().unwrap_or_else(|e| e.into_inner());
		if window.0.elapsed() >= Duration::from_secs(1) {
			*window = (Instant::now(), 0);
		}
		if window.1 < per_second {
			window.1 += 1;
			true
		} else {
			false
		}
	}

	/// Opens a transaction over a published snapshot, or reports the age of
	/// the newest one when none is within the bound.
	async fn open_stale(
		&self,
		tier: &Arc<dyn StaleReads>,
		request: ReadSnapshotRequest,
		sequences: Sequences,
	) -> Result<std::result::Result<(Transaction, SnapshotInfo), Duration>> {
		let settled = self.triggers.catalog_epoch.settled();
		let live_bucket = self.triggers.live.open();
		let outcome = match tier.open(request).await {
			Ok(outcome) => outcome,
			Err(e) => {
				self.triggers.live.close(live_bucket);
				return Err(e.into());
			}
		};
		match outcome {
			ReadOutcome::BoundUnmet {
				newest_age,
			} => {
				self.triggers.live.close(live_bucket);
				Ok(Err(newest_age))
			}
			ReadOutcome::Opened {
				transaction,
				local,
				info,
			} => {
				// A stale snapshot may predate catalog changes the settled epoch
				// counts, so it is never identified as reading the current catalog
				// and never trusted as the sole writer's view.
				let sole_writer = transaction.sole_writer() && !info.stale;
				let catalog_epoch =
					settled.filter(|_| sole_writer).unwrap_or(crate::tx::NO_CATALOG_EPOCH);
				let tx = Transaction::new(
					local,
					sequences,
					Arc::clone(&self.triggers),
					Arc::clone(&self.observer),
					Transactor::new(transaction),
					&self.config,
					catalog_epoch,
					sole_writer,
					Arc::clone(&self.feature),
				)
				.with_live_bucket(live_bucket);
				Ok(Ok((tx, info)))
			}
		}
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
		// Counted live before its snapshot is taken, so a wait for the
		// transactions older than a moment never misses this one.
		let live_bucket = self.triggers.live.open();
		// Create a new transaction on the datastore
		let (inner, local) = match self.builder.new_transaction(write).await {
			Ok(opened) => opened,
			Err(e) => {
				self.triggers.live.close(live_bucket);
				return Err(e.into());
			}
		};
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
		)
		.with_live_bucket(live_bucket))
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
			fallbacks: Arc::clone(&self.fallbacks),
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
