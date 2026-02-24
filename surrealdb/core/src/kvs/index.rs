use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Result, ensure};
use futures::channel::oneshot::{Receiver, Sender, channel};
use reblessive::TreeStack;
use revision::revisioned;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;
use web_time::Instant;

use crate::catalog::providers::TableProvider;
use crate::catalog::{
	DatabaseDefinition, DatabaseId, IndexDefinition, IndexId, NamespaceId, Record, TableId,
};
use crate::cnf::INDEXING_BATCH_SIZE;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::index::IndexOperation;
use crate::key::index::ig::IndexAppending;
use crate::key::record;
use crate::kvs::LockType::Optimistic;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::{KVValue, Key, Transaction, TransactionType, Val, impl_kv_value_revisioned};
use crate::mem::ALLOC;
use crate::val::{Object, RecordId, RecordIdKey, TableName, Value};

#[derive(Debug, Clone)]
pub(crate) enum BuildingStatus {
	Started,
	Cleaning,
	Indexing {
		initial: Option<usize>,
		updated: Option<usize>,
		pending: Option<usize>,
	},
	Ready {
		initial: Option<usize>,
		updated: Option<usize>,
		pending: Option<usize>,
	},
	Aborted,
	Error(String),
}

impl Default for BuildingStatus {
	fn default() -> Self {
		Self::Ready {
			initial: None,
			updated: None,
			pending: None,
		}
	}
}
pub(crate) enum ConsumeResult {
	/// The document has been enqueued to be indexed
	Enqueued,
	/// The index has been built, the document can be indexed normally
	Ignored(Option<Vec<Value>>, Option<Vec<Value>>),
}

impl BuildingStatus {
	fn is_error(&self) -> bool {
		matches!(self, Self::Error(_))
	}

	fn is_ready(&self) -> bool {
		matches!(self, Self::Ready { .. })
	}
}

impl From<BuildingStatus> for Value {
	fn from(st: BuildingStatus) -> Self {
		let mut o = Object::default();
		let s = match st {
			BuildingStatus::Started => "started",
			BuildingStatus::Cleaning => "cleaning",
			BuildingStatus::Indexing {
				initial,
				pending,
				updated,
			} => {
				if let Some(c) = initial {
					o.insert("initial".to_string(), c.into());
				}
				if let Some(c) = pending {
					o.insert("pending".to_string(), c.into());
				}
				if let Some(c) = updated {
					o.insert("updated".to_string(), c.into());
				}
				"indexing"
			}
			BuildingStatus::Ready {
				initial,
				pending,
				updated,
			} => {
				if let Some(c) = initial {
					o.insert("initial".to_string(), c.into());
				}
				if let Some(c) = pending {
					o.insert("pending".to_string(), c.into());
				}
				if let Some(c) = updated {
					o.insert("updated".to_string(), c.into());
				}
				"ready"
			}
			BuildingStatus::Aborted => "aborted",
			BuildingStatus::Error(error) => {
				o.insert("error".to_string(), error.into());
				"error"
			}
		};
		o.insert("status".to_string(), s.into());
		o.into()
	}
}

type IndexBuilding = Arc<Building>;

pub(super) type SharedIndexKey = Arc<IndexKey>;

#[derive(Hash, PartialEq, Eq)]
pub(super) struct IndexKey {
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	ix: IndexId,
}

impl IndexKey {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &TableName, ix: IndexId) -> Self {
		Self {
			ns,
			db,
			tb: tb.to_owned(),
			ix,
		}
	}
}

#[derive(Clone)]
pub(crate) struct IndexBuilder {
	tf: TransactionFactory,
	indexes: Arc<RwLock<HashMap<SharedIndexKey, IndexBuilding>>>,
}

impl IndexBuilder {
	pub(super) fn new(tf: TransactionFactory) -> Self {
		Self {
			tf,
			indexes: Default::default(),
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn start_building(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		ix_key: SharedIndexKey,
		sdr: Option<Sender<Result<()>>>,
	) -> Result<IndexBuilding> {
		let building = Arc::new(Building::new(ctx, self.tf.clone(), opt, tb, ix, ix_key)?);
		let b = building.clone();
		spawn(async move {
			let guard = BuildingFinishGuard(b.clone());
			let r = b.run().await;
			if let Err(err) = &r {
				b.set_status(BuildingStatus::Error(err.to_string())).await;
			}
			drop(guard);
			if let Some(s) = sdr
				&& s.send(r).is_err()
			{
				warn!("Failed to send index building result to the consumer");
			}
		});
		Ok(building)
	}

	pub(crate) async fn build(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		blocking: bool,
	) -> Result<Option<Receiver<Result<()>>>> {
		ix.expect_not_prepare_remove()?;
		let (ns, db) = ctx.expect_ns_db_ids(&opt).await?;
		let key = Arc::new(IndexKey::new(ns, db, &ix.table_name, ix.index_id));
		let (rcv, sdr) = if blocking {
			let (s, r) = channel();
			(Some(r), Some(s))
		} else {
			(None, None)
		};
		match self.indexes.write().await.entry(key.clone()) {
			Entry::Occupied(mut e) => {
				// If the building is currently running, we return an error
				ensure!(
					e.get().is_finished(),
					Error::IndexAlreadyBuilding {
						name: ix.name.clone(),
					}
				);
				let ib = self.start_building(ctx, opt, tb, ix, key, sdr)?;
				e.insert(ib);
			}
			Entry::Vacant(e) => {
				// No index is currently building, we can start building it
				let ib = self.start_building(ctx, opt, tb, ix, key, sdr)?;
				e.insert(ib);
			}
		};
		Ok(rcv)
	}

	pub(crate) async fn consume(
		&self,
		db: &DatabaseDefinition,
		ctx: &FrozenContext,
		ix: &IndexDefinition,
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
		rid: &RecordId,
	) -> Result<ConsumeResult> {
		let key = IndexKey::new(db.namespace_id, db.database_id, &ix.table_name, ix.index_id);
		if let Some(b) = self.indexes.read().await.get(&key) {
			return b.maybe_consume(ctx, old_values, new_values, rid).await;
		}
		Ok(ConsumeResult::Ignored(old_values, new_values))
	}

	pub(crate) async fn get_status(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
	) -> BuildingStatus {
		let key = IndexKey::new(ns, db, &ix.table_name, ix.index_id);
		if let Some(b) = self.indexes.read().await.get(&key) {
			b.status.read().await.clone()
		} else {
			BuildingStatus::default()
		}
	}

	pub(crate) async fn remove_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> Result<()> {
		let key = IndexKey::new(ns, db, tb, ix);
		if let Some(b) = self.indexes.write().await.remove(&key) {
			b.abort();
		}
		Ok(())
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, PartialEq)]
pub(crate) struct Appending {
	old_values: Option<Vec<Value>>,
	new_values: Option<Vec<Value>>,
	id: RecordIdKey,
}

impl_kv_value_revisioned!(Appending);

impl Appending {
	#[cfg(test)]
	pub(crate) fn new(
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
		id: RecordIdKey,
	) -> Self {
		Self {
			old_values,
			new_values,
			id,
		}
	}
}

#[revisioned(revision = 2)]
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub(crate) struct PrimaryAppending(
	/// Appending id within the concurrent indexing queue.
	AppendingId,
	/// Batch id associated with this append.
	#[revision(start = 2)]
	BatchId,
);

pub(crate) type BatchId = u32;
pub(crate) type AppendingId = u32;

impl_kv_value_revisioned!(PrimaryAppending);

impl PrimaryAppending {
	#[cfg(test)]
	pub(crate) fn new(appending_id: AppendingId, batch_id: BatchId) -> Self {
		Self(appending_id, batch_id)
	}
}

/// Tracks sequence numbers and batches for the background indexing queue.
struct QueueSequences {
	/// Number of queued appends awaiting indexing.
	pending: u32,
	/// Next appending id to assign.
	next_appending: AppendingId,
	/// Next batch id to assign.
	next_batch: BatchId,
	/// Appends tracked per batch for cleanup after rollback (cancel or failed commit).
	batches: HashMap<BatchId, RoaringBitmap>,
}

/// Batch IDs queued for deferred cleanup after rollback (cancel or failed commit).
pub(super) type BatchIdsCleanQueue = Arc<Mutex<Vec<BatchId>>>;

impl Default for QueueSequences {
	fn default() -> Self {
		Self {
			pending: 0,
			next_appending: 0,
			// Batch IDs are 1-based; 0 is reserved for legacy/missing batch IDs.
			next_batch: Self::DEFAULT_NEXT_BATCH_ID,
			batches: Default::default(),
		}
	}
}
impl QueueSequences {
	const DEFAULT_NEXT_BATCH_ID: u32 = 1;
	const LEGACY_BATCH_ID: u32 = 0;

	fn is_empty(&self) -> bool {
		self.pending == 0
	}

	fn new_batch(&mut self) -> u32 {
		let batch_id = self.next_batch;
		self.batches.insert(batch_id, RoaringBitmap::new());
		self.next_batch += 1;
		batch_id
	}

	fn add_update(&mut self, batch_id: BatchId) -> Result<AppendingId, Error> {
		if let Some(batch) = self.batches.get_mut(&batch_id) {
			let appending_id = self.next_appending;
			batch.insert(appending_id);
			self.next_appending += 1;
			self.pending += 1;
			Ok(appending_id)
		} else {
			Err(Error::Internal(format!("The batch does not exist: {}", batch_id)))
		}
	}

	/// Returns the number of updates pending in the queue.
	fn pending(&self) -> u32 {
		self.pending
	}

	fn clean(&mut self, to_clean: HashMap<BatchId, Vec<AppendingId>>) {
		for (batch_id, appending_ids) in to_clean {
			if let Entry::Occupied(mut e) = self.batches.entry(batch_id) {
				let batch = e.get_mut();
				for appending_id in appending_ids {
					if batch.remove(appending_id) {
						self.pending -= 1;
					}
				}
				if batch.is_empty() {
					e.remove();
				}
			}
		}
	}

	pub(super) fn clean_batch_ids(&mut self, batch_ids: Vec<BatchId>) {
		for batch_id in batch_ids {
			if let Some(v) = self.batches.remove(&batch_id) {
				self.pending -= v.len() as u32;
			}
		}
	}
}

struct Building {
	/// Context used during the build.
	ctx: FrozenContext,
	/// Options used during the build.
	opt: Options,
	/// Table id being indexed.
	tb: TableId,
	ikb: IndexKeyBase,
	/// Transaction factory for new transactions.
	tf: TransactionFactory,
	/// Index definition being built.
	ix: Arc<IndexDefinition>,
	/// Index key (namespace/db/table/index ids).
	ix_key: SharedIndexKey,
	/// Current build status.
	status: Arc<RwLock<BuildingStatus>>,
	/// Queue of records awaiting indexing.
	queue: Arc<RwLock<QueueSequences>>,
	/// Batch IDs scheduled for deferred cleanup after rollback (cancel or failed commit).
	clean_queue: BatchIdsCleanQueue,
	/// Abort flag for the build process.
	aborted: AtomicBool,
	finished: AtomicBool,
}

impl Building {
	fn new(
		ctx: &FrozenContext,
		tf: TransactionFactory,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		ix_key: SharedIndexKey,
	) -> Result<Self> {
		let ikb = IndexKeyBase::new(ix_key.ns, ix_key.db, ix.table_name.clone(), ix.index_id);
		Ok(Self {
			ctx: Context::new_concurrent(ctx).freeze(),
			opt,
			tb,
			ikb,
			tf,
			ix,
			ix_key,
			status: Arc::new(RwLock::new(BuildingStatus::Started)),
			queue: Default::default(),
			clean_queue: Default::default(),
			aborted: AtomicBool::new(false),
			finished: AtomicBool::new(false),
		})
	}

	async fn set_status(&self, status: BuildingStatus) {
		let mut s = self.status.write().await;
		// We want to keep only the first error
		if !s.is_error() {
			*s = status;
		}
	}

	async fn maybe_consume(
		&self,
		ctx: &FrozenContext,
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
		rid: &RecordId,
	) -> Result<ConsumeResult> {
		let mut queue = self.queue.write().await;
		// With the queue locked, we can safely decide whether the async build is done.
		if queue.is_empty() && self.status.read().await.is_ready() {
			// If the queue is empty and the index is built...
			// ... return the values so the document can be updated normally.
			return Ok(ConsumeResult::Ignored(old_values, new_values));
		}

		let tx = ctx.tx();
		let appending = Appending {
			old_values,
			new_values,
			id: rid.key.clone(),
		};

		// Get the per-transaction batch id.
		let mut pending_index_batches = tx.lock_pending_index_batches().await;
		let batch_id = if let Some((batch_id, _)) = pending_index_batches.get(&self.ix_key) {
			*batch_id
		} else {
			let batch_id = queue.new_batch();
			pending_index_batches.insert(self.ix_key.clone(), (batch_id, self.clean_queue.clone()));
			batch_id
		};

		// Allocate the appending id for this update.
		let appending_id = queue.add_update(batch_id)?;
		// Store the appending
		let ig = self.ikb.new_ig_key(appending_id, batch_id);
		tx.set(&ig, &appending, None).await?;
		// Do we already have a primary appending?
		let ip = self.ikb.new_ip_key(rid.key.clone());
		let pa = tx.get(&ip, None).await?;
		// Do we have a primary indexing?
		// We ignore legacy primary indexing.
		// The initial batch is responsible for removing them, if any,
		// and reporting their presence in the logs.
		let is_pa = if let Some(pa) = pa {
			pa.1 != QueueSequences::LEGACY_BATCH_ID
		} else {
			false
		};
		if !is_pa {
			// If not, set it.
			tx.set(&ip, &PrimaryAppending(appending_id, batch_id), None).await?;
		}
		drop(queue);
		Ok(ConsumeResult::Enqueued)
	}

	async fn new_read_tx(&self) -> Result<Transaction> {
		self.tf
			.transaction(TransactionType::Read, Optimistic, self.ctx.try_get_sequences()?.clone())
			.await
	}

	async fn new_write_tx_ctx(&self) -> Result<FrozenContext> {
		let tx = self
			.tf
			.transaction(TransactionType::Write, Optimistic, self.ctx.try_get_sequences()?.clone())
			.await?
			.into();
		let mut ctx = Context::new(&self.ctx);
		ctx.set_transaction(tx);
		Ok(ctx.freeze())
	}

	async fn check_prepare_remove_with_tx(
		&self,
		last_prepare_remove_check: &mut Instant,
		tx: &Transaction,
	) -> Result<()> {
		if last_prepare_remove_check.elapsed() < Duration::from_secs(5) {
			return Ok(());
		};
		// Check the index still exists and has not been marked for removal.
		// We use get_tb_index (returns Option) instead of expect_tb_index because
		// this check runs on a separate read transaction. During a blocking DEFINE
		// INDEX, the index definition is only committed after indexing completes,
		// so this read transaction may not yet see it.
		// If the index is not found, we continue — the prepare_remove flag can only
		// be set by REMOVE INDEX, which runs in a separate transaction.
		if let Some(ix) = tx
			.get_tb_index(self.ix_key.ns, self.ix_key.db, &self.ix.table_name, &self.ix.name)
			.await?
		{
			ix.expect_not_prepare_remove()?;
		}
		*last_prepare_remove_check = Instant::now();
		Ok(())
	}

	async fn check_prepare_remove(&self, last_prepare_remove_check: &mut Instant) -> Result<()> {
		let tx = self.new_read_tx().await?;
		catch!(tx, self.check_prepare_remove_with_tx(last_prepare_remove_check, &tx).await);
		tx.cancel().await?;
		Ok(())
	}

	async fn run(&self) -> Result<()> {
		let mut last_prepare_remove_check = Instant::now();

		// Remove existing index data.
		{
			self.set_status(BuildingStatus::Cleaning).await;
			let ctx = self.new_write_tx_ctx().await?;
			let key = crate::key::index::all::new(
				self.ix_key.ns,
				self.ix_key.db,
				&self.ix_key.tb,
				self.ix_key.ix,
			);
			let tx = ctx.tx();
			catch!(tx, tx.delp(&key).await);
			catch!(tx, tx.commit().await);
		}

		// First pass: index every record.
		let beg = record::prefix(self.ix_key.ns, self.ix_key.db, self.ikb.table())?;
		let end = record::suffix(self.ix_key.ns, self.ix_key.db, self.ikb.table())?;
		let mut next = Some(beg..end);
		let mut initial_count = 0;
		let mut v1_appending_sentinel = false;
		// Set the initial status.
		self.set_status(BuildingStatus::Indexing {
			initial: Some(initial_count),
			pending: Some(self.queue.read().await.pending() as usize),
			updated: None,
		})
		.await;

		while let Some(rng) = next {
			if self.is_aborted().await {
				return Ok(());
			}
			self.is_beyond_threshold(None)?;
			let batch = {
				let tx = self.new_read_tx().await?;
				// Check if the index has been marked for removal
				catch!(
					tx,
					self.check_prepare_remove_with_tx(&mut last_prepare_remove_check, &tx).await
				);
				// Get the next batch of records.
				let res = catch!(tx, tx.batch_keys_vals(rng, *INDEXING_BATCH_SIZE, None).await);
				tx.cancel().await?;
				res
			};
			// Set the next scan range
			next = batch.next;
			// Check whether any records remain.
			if batch.result.is_empty() {
				// If not, initial indexing is complete.
				break;
			}
			// Create a new context with a write transaction.
			{
				let ctx = self.new_write_tx_ctx().await?;
				let tx = ctx.tx();
				// Index the batch.
				catch!(
					tx,
					self.index_initial_batch(
						&ctx,
						&tx,
						batch.result,
						&mut initial_count,
						&mut v1_appending_sentinel
					)
					.await
				);
				catch!(tx, tx.commit().await);
			}
		}
		// Second pass: index/remove records that changed during the initial pass.
		self.set_status(BuildingStatus::Indexing {
			initial: Some(initial_count),
			pending: Some(self.queue.read().await.pending() as usize),
			updated: Some(0),
		})
		.await;
		let mut updates_count = 0;
		let rng = self.ikb.new_ig_range()?;
		loop {
			if self.is_aborted().await {
				return Ok(());
			}
			self.is_beyond_threshold(None)?;
			// Check the index still exists and has not been marked for removal
			self.check_prepare_remove(&mut last_prepare_remove_check).await?;

			let keys = {
				let mut queue = self.queue.write().await;
				let clean_queue = {
					let mut batch_ids_to_clean = self.clean_queue.lock().await;
					std::mem::take(&mut *batch_ids_to_clean)
				};
				// Clean batch IDs from canceled or failed transactions before checking pending
				// state.
				queue.clean_batch_ids(clean_queue);
				let keys = {
					let tx = self.new_read_tx().await?;
					let keys =
						catch!(tx, tx.keys(rng.clone(), *INDEXING_BATCH_SIZE, 0, None).await);
					tx.cancel().await?;
					keys
				};
				let pending = queue.pending() as usize;
				if keys.is_empty() && pending == 0 {
					// If no keys remain and no updates are pending, we're done.
					// With the queue lock held, no other task can add items, so indexing is
					// complete.
					self.set_status(BuildingStatus::Ready {
						initial: Some(initial_count),
						pending: Some(pending),
						updated: Some(updates_count),
					})
					.await;
					break;
				}
				// Pending appends exist but none are committed yet; wait and retry.
				self.set_status(BuildingStatus::Indexing {
					initial: Some(initial_count),
					pending: Some(pending),
					updated: Some(updates_count),
				})
				.await;
				drop(queue);
				keys
			};
			if !keys.is_empty() {
				// We have committed appendings to index.
				// Create a new context with a write transaction.
				let ctx = self.new_write_tx_ctx().await?;
				let tx = ctx.tx();
				let indexed = catch!(
					tx,
					self.index_appending_range(&ctx, &tx, keys, initial_count, &mut updates_count)
						.await
				);
				catch!(tx, tx.commit().await);
				// Clean up completed appendings and drop any stale rollback batch IDs.
				if !indexed.is_empty() {
					{
						let mut clean_queue = self.clean_queue.lock().await;
						for batch_id in indexed.keys() {
							if let Some(idx) = clean_queue.iter().position(|&id| id == *batch_id) {
								clean_queue.remove(idx);
							}
						}
					}
					self.queue.write().await.clean(indexed);
				}
			} else {
				// No committed appends yet, but updates are in-flight; wait.
				sleep(Duration::from_millis(100)).await;
			}
		}
		Ok(())
	}

	async fn index_initial_batch(
		&self,
		ctx: &FrozenContext,
		tx: &Transaction,
		values: Vec<(Key, Val)>,
		count: &mut usize,
		v1_appending_sentinel: &mut bool,
	) -> Result<()> {
		let mut rc = false;
		let mut stack = TreeStack::new();
		// Index the records.
		for (k, v) in values {
			if self.is_aborted().await {
				return Ok(());
			}
			self.is_beyond_threshold(Some(*count))?;
			let key = record::RecordKey::decode_key(&k)?;
			// Parse the value.
			let val = Record::kv_decode_value(v)?;
			let rid: Arc<RecordId> = RecordId {
				table: key.tb.into_owned(),
				key: key.id,
			}
			.into();

			// Is there already a queued update for this record?
			let opt_values = if let Some(a) =
				self.check_existing_primary_appending(tx, &rid.key, v1_appending_sentinel).await?
			{
				a.old_values
			} else {
				// Otherwise, proceed with normal indexing.
				let doc = CursorDoc::new(Some(rid.clone()), None, val);
				stack
					.enter(|stk| Document::build_opt_values(stk, ctx, &self.opt, &self.ix, &doc))
					.finish()
					.await?
			};
			// Index the record.
			let mut io = IndexOperation::new(
				ctx,
				&self.opt,
				self.ix_key.ns,
				self.ix_key.db,
				self.tb,
				&self.ix,
				None,
				opt_values.clone(),
				&rid,
			);
			stack.enter(|stk| io.compute(stk, &mut rc)).finish().await?;

			// Increment the count and update the status.
			*count += 1;
			self.set_status(BuildingStatus::Indexing {
				initial: Some(*count),
				pending: Some(self.queue.read().await.pending() as usize),
				updated: None,
			})
			.await;
		}
		// Trigger compaction if needed.
		self.check_index_compaction(tx, &mut rc).await?;
		// We're done.
		Ok(())
	}

	async fn check_existing_primary_appending(
		&self,
		tx: &Transaction,
		id_key: &RecordIdKey,
		v1_appending_sentinel: &mut bool,
	) -> Result<Option<Appending>> {
		let ip = self.ikb.new_ip_key(id_key.clone());
		let Some(pa) = tx.get(&ip, None).await? else {
			return Ok(None);
		};
		// Use the old values from the queued update as the initial indexing input.
		if pa.1 == QueueSequences::LEGACY_BATCH_ID {
			// Legacy v1 primary appending entry (no batch id; queue stored under !ia).
			// We can't resolve it to a v2 !ig record, so drop the marker and ignore the legacy
			// queue.
			tx.del(&ip).await?;
			if !*v1_appending_sentinel {
				*v1_appending_sentinel = true;
				warn!(
					"Found legacy v1 primary appending entry from an older version; legacy queued updates will be ignored. Consider rebuilding index {} on table {}.",
					self.ix.name, self.ix.table_name
				);
			}
			return Ok(None);
		}
		let ib = self.ikb.new_ig_key(pa.0, pa.1);
		let appending = tx
			.get(&ib, None)
			.await?
			.ok_or_else(|| Error::CorruptedIndex("Appending record is missing"))?;
		Ok(Some(appending))
	}

	async fn index_appending_range(
		&self,
		ctx: &FrozenContext,
		tx: &Transaction,
		keys: Vec<Key>,
		initial: usize,
		count: &mut usize,
	) -> Result<HashMap<BatchId, Vec<AppendingId>>> {
		let mut rc = false;
		let mut stack = TreeStack::new();
		let mut indexed = HashMap::new();
		for k in keys {
			if self.is_aborted().await {
				return Ok(indexed);
			}
			self.is_beyond_threshold(Some(*count))?;
			let ig = IndexAppending::decode_key(&k)?;
			if let Some(appending) = tx.get(&ig, None).await? {
				let rid = RecordId {
					table: self.ikb.table().clone(),
					key: appending.id,
				};
				let mut io = IndexOperation::new(
					ctx,
					&self.opt,
					self.ix_key.ns,
					self.ix_key.db,
					self.tb,
					&self.ix,
					appending.old_values,
					appending.new_values,
					&rid,
				);
				stack.enter(|stk| io.compute(stk, &mut rc)).finish().await?;
				tx.del(&ig).await?;

				// We can delete the ip record if any
				let ip = self.ikb.new_ip_key(rid.key);
				tx.del(&ip).await?;
			}

			*count += 1;
			self.set_status(BuildingStatus::Indexing {
				initial: Some(initial),
				pending: Some(self.queue.read().await.pending() as usize),
				updated: Some(*count),
			})
			.await;
			indexed.entry(ig.batch_id).or_insert(vec![]).push(ig.appending_id);
		}
		// Trigger compaction if needed.
		self.check_index_compaction(tx, &mut rc).await?;
		// We're done.
		Ok(indexed)
	}

	async fn check_index_compaction(&self, tx: &Transaction, rc: &mut bool) -> Result<()> {
		if !*rc {
			return Ok(());
		}
		IndexOperation::compaction_trigger(&self.ikb, tx, self.opt.id()).await?;
		*rc = false;
		Ok(())
	}
	/// Abort the current indexing process.
	fn abort(&self) {
		// We use `Ordering::Relaxed` as the caller does not require synchronization.
		// We just want the current builder to eventually stop.
		self.aborted.store(true, Ordering::Relaxed);
	}

	/// Check if the indexing process is aborting.
	async fn is_aborted(&self) -> bool {
		// We use `Ordering::Relaxed` as there are no shared data accesses requiring
		// synchronization. This method is only called by the single thread building
		// the index.
		if self.aborted.load(Ordering::Relaxed) {
			self.set_status(BuildingStatus::Aborted).await;
			true
		} else {
			false
		}
	}

	fn is_beyond_threshold(&self, count: Option<usize>) -> Result<()> {
		if let Some(count) = count
			&& count % 100 != 0
		{
			return Ok(());
		}
		if ALLOC.is_beyond_threshold() {
			Err(anyhow::Error::new(Error::QueryBeyondMemoryThreshold))
		} else {
			Ok(())
		}
	}

	fn is_finished(&self) -> bool {
		self.finished.load(Ordering::Relaxed)
	}
}

struct BuildingFinishGuard(IndexBuilding);

impl Drop for BuildingFinishGuard {
	fn drop(&mut self) {
		self.0.finished.store(true, Ordering::Relaxed);
	}
}
