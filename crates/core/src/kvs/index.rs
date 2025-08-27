use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, ensure};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures::channel::oneshot::{Receiver, Sender, channel};
use reblessive::TreeStack;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::task;
use tokio::task::JoinHandle;

use crate::catalog::{DatabaseDefinition, DatabaseId, IndexDefinition, NamespaceId};
use crate::cnf::{INDEXING_BATCH_SIZE, NORMAL_FETCH_SIZE};
use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::index::IndexOperation;
use crate::key::thing;
use crate::kvs::LockType::Optimistic;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::{KVValue, Key, Transaction, TransactionType, Val, impl_kv_value_revisioned};
use crate::mem::ALLOC;
use crate::val::record::Record;
use crate::val::{Object, RecordId, RecordIdKey, Value};

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
				o.insert("error".to_string(), error.to_string().into());
				"error"
			}
		};
		o.insert("status".to_string(), s.into());
		o.into()
	}
}

type IndexBuilding = (Arc<Building>, JoinHandle<()>);

#[derive(Hash, PartialEq, Eq)]
struct IndexKey {
	ns: NamespaceId,
	db: DatabaseId,
	tb: String,
	ix: String,
}

impl IndexKey {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &str, ix: &str) -> Self {
		Self {
			ns,
			db,
			tb: tb.to_owned(),
			ix: ix.to_owned(),
		}
	}
}

#[derive(Clone)]
pub(crate) struct IndexBuilder {
	tf: TransactionFactory,
	indexes: Arc<DashMap<IndexKey, IndexBuilding>>,
}

impl IndexBuilder {
	pub(super) fn new(tf: TransactionFactory) -> Self {
		Self {
			tf,
			indexes: Default::default(),
		}
	}

	fn start_building(
		&self,
		ctx: &Context,
		opt: Options,
		ns: NamespaceId,
		db: DatabaseId,
		ix: Arc<IndexDefinition>,
		sdr: Option<Sender<Result<()>>>,
	) -> Result<IndexBuilding> {
		let building = Arc::new(Building::new(ctx, self.tf.clone(), opt, ns, db, ix)?);
		let b = building.clone();
		let jh = task::spawn(async move {
			let r = b.run().await;
			if let Err(err) = &r {
				b.set_status(BuildingStatus::Error(err.to_string())).await;
			}
			if let Some(s) = sdr {
				if s.send(r).is_err() {
					warn!("Failed to send index building result to the consumer");
				}
			}
		});
		Ok((building, jh))
	}

	pub(crate) fn build(
		&self,
		ctx: &Context,
		opt: Options,
		ns: NamespaceId,
		db: DatabaseId,
		ix: Arc<IndexDefinition>,
		blocking: bool,
	) -> Result<Option<Receiver<Result<()>>>> {
		let key = IndexKey::new(ns, db, &ix.what, &ix.name);
		let (rcv, sdr) = if blocking {
			let (s, r) = channel();
			(Some(r), Some(s))
		} else {
			(None, None)
		};
		match self.indexes.entry(key) {
			Entry::Occupied(e) => {
				// If the building is currently running, we return error
				ensure!(
					e.get().1.is_finished(),
					Error::IndexAlreadyBuilding {
						name: e.key().ix.clone(),
					}
				);
				let ib = self.start_building(ctx, opt, ns, db, ix, sdr)?;
				e.replace_entry(ib);
			}
			Entry::Vacant(e) => {
				// No index is currently building, we can start building it
				let ib = self.start_building(ctx, opt, ns, db, ix, sdr)?;
				e.insert(ib);
			}
		};
		Ok(rcv)
	}

	pub(crate) async fn consume(
		&self,
		db: &DatabaseDefinition,
		ctx: &Context,
		ix: &IndexDefinition,
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
		rid: &RecordId,
	) -> Result<ConsumeResult> {
		let key = IndexKey::new(db.namespace_id, db.database_id, &ix.what, &ix.name);
		if let Some(r) = self.indexes.get(&key) {
			let (b, _) = r.value();
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
		let key = IndexKey::new(ns, db, &ix.what, &ix.name);
		if let Some(a) = self.indexes.get(&key) {
			a.value().0.status.read().await.clone()
		} else {
			BuildingStatus::default()
		}
	}

	pub(crate) fn remove_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
	) -> Result<()> {
		let key = IndexKey::new(ns, db, tb, ix);
		if let Some((_, b)) = self.indexes.remove(&key) {
			b.0.abort();
		}
		Ok(())
	}
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Appending {
	old_values: Option<Vec<Value>>,
	new_values: Option<Vec<Value>>,
	id: RecordIdKey,
}

impl_kv_value_revisioned!(Appending);

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct PrimaryAppending(u32);

impl_kv_value_revisioned!(PrimaryAppending);

#[derive(Default)]
struct QueueSequences {
	/// The index of the next appending to be indexed
	to_index: u32,
	/// The index of the next appending to be added
	next: u32,
}

impl QueueSequences {
	fn is_empty(&self) -> bool {
		self.to_index == self.next
	}

	fn add_update(&mut self) -> u32 {
		let i = self.next;
		self.next += 1;
		i
	}

	fn clear(&mut self) {
		self.to_index = 0;
		self.next = 0;
	}

	fn pending(&self) -> u32 {
		self.next - self.to_index
	}

	fn set_to_index(&mut self, i: u32) {
		self.to_index = i;
	}

	fn next_indexing_batch(&self, page: u32) -> Range<u32> {
		let s = self.to_index;
		let e = (s + page).min(self.next);
		s..e
	}
}

struct Building {
	ctx: Context,
	opt: Options,
	ns: NamespaceId,
	db: DatabaseId,
	ikb: IndexKeyBase,
	tf: TransactionFactory,
	ix: Arc<IndexDefinition>,
	status: Arc<RwLock<BuildingStatus>>,
	queue: Arc<RwLock<QueueSequences>>,
	aborted: AtomicBool,
}

impl Building {
	fn new(
		ctx: &Context,
		tf: TransactionFactory,
		opt: Options,
		ns: NamespaceId,
		db: DatabaseId,
		ix: Arc<IndexDefinition>,
	) -> Result<Self> {
		let ikb = IndexKeyBase::new(ns, db, &ix.what, &ix.name);
		Ok(Self {
			ctx: MutableContext::new_concurrent(ctx).freeze(),
			opt,
			ns,
			db,
			ikb,
			tf,
			ix,
			status: Arc::new(RwLock::new(BuildingStatus::Started)),
			queue: Default::default(),
			aborted: AtomicBool::new(false),
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
		ctx: &Context,
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
		rid: &RecordId,
	) -> Result<ConsumeResult> {
		let mut queue = self.queue.write().await;
		// Now that the queue is locked, we have the possibility to assess if the
		// asynchronous build is done.
		if queue.is_empty() {
			// If the appending queue is empty and the index is built...
			if self.status.read().await.is_ready() {
				// ... we return the values back, so the document can be updated the usual way
				return Ok(ConsumeResult::Ignored(old_values, new_values));
			}
		}

		let tx = ctx.tx();
		let a = Appending {
			old_values,
			new_values,
			id: rid.key.clone(),
		};
		// Get the idx of this appended record from the sequence
		let idx = queue.add_update();
		// Store the appending
		let ia = self.ikb.new_ia_key(idx);
		tx.set(&ia, &a, None).await?;
		// Do we already have a primary appending?
		let ip = self.ikb.new_ip_key(rid.key.clone());
		if tx.get(&ip, None).await?.is_none() {
			// If not, we set it
			tx.set(&ip, &PrimaryAppending(idx), None).await?;
		}
		drop(queue);
		Ok(ConsumeResult::Enqueued)
	}

	async fn new_read_tx(&self) -> Result<Transaction> {
		self.tf.transaction(TransactionType::Read, Optimistic).await
	}

	async fn new_write_tx_ctx(&self) -> Result<Context> {
		let tx = self.tf.transaction(TransactionType::Write, Optimistic).await?.into();
		let mut ctx = MutableContext::new(&self.ctx);
		ctx.set_transaction(tx);
		Ok(ctx.freeze())
	}

	async fn run(&self) -> Result<()> {
		// Remove the index data
		{
			self.set_status(BuildingStatus::Cleaning).await;
			let ctx = self.new_write_tx_ctx().await?;
			let key =
				crate::key::index::all::new(self.ns, self.db, self.ikb.table(), self.ikb.index());
			let tx = ctx.tx();
			tx.delp(&key).await?;
			tx.commit().await?;
		}

		// First iteration, we index every key
		let beg = thing::prefix(self.ns, self.db, self.ikb.table())?;
		let end = thing::suffix(self.ns, self.db, self.ikb.table())?;
		let mut next = Some(beg..end);
		let mut initial_count = 0;
		// Set the initial status
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
			// Get the next batch of records
			let batch = {
				let tx = self.new_read_tx().await?;
				catch!(tx, tx.batch_keys_vals(rng, *INDEXING_BATCH_SIZE, None).await)
			};
			// Set the next scan range
			next = batch.next;
			// Check there are records
			if batch.result.is_empty() {
				// If not, we are with the initial indexing
				break;
			}
			// Create a new context with a "write" transaction
			{
				let ctx = self.new_write_tx_ctx().await?;
				let tx = ctx.tx();
				// Index the batch
				catch!(
					tx,
					self.index_initial_batch(&ctx, &tx, batch.result, &mut initial_count).await
				);
				tx.commit().await?;
			}
		}
		// Second iteration, we index/remove any records that has been added or removed
		// since the initial indexing
		self.set_status(BuildingStatus::Indexing {
			initial: Some(initial_count),
			pending: Some(self.queue.read().await.pending() as usize),
			updated: Some(0),
		})
		.await;
		let mut updates_count = 0;
		let mut next_to_index = None;
		loop {
			if self.is_aborted().await {
				return Ok(());
			}
			self.is_beyond_threshold(None)?;
			let range = {
				let mut queue = self.queue.write().await;
				if let Some(ni) = next_to_index {
					queue.set_to_index(ni);
				}
				if queue.is_empty() {
					// If the batch is empty, we are done.
					// Due to the lock on self.queue, we know that no external process can add an
					// item to the queue.
					self.set_status(BuildingStatus::Ready {
						initial: Some(initial_count),
						pending: Some(queue.pending() as usize),
						updated: Some(updates_count),
					})
					.await;
					// This is here to be sure the lock on back is not released early
					queue.clear();
					break;
				}
				queue.next_indexing_batch(*NORMAL_FETCH_SIZE)
			};
			if range.is_empty() {
				continue;
			}
			next_to_index = Some(range.end);
			// Create a new context with a write transaction
			{
				let ctx = self.new_write_tx_ctx().await?;
				let tx = ctx.tx();
				catch!(
					tx,
					self.index_appending_range(&ctx, &tx, range, initial_count, &mut updates_count)
						.await
				);
				tx.commit().await?;
			}
		}
		Ok(())
	}

	async fn index_initial_batch(
		&self,
		ctx: &Context,
		tx: &Transaction,
		values: Vec<(Key, Val)>,
		count: &mut usize,
	) -> Result<()> {
		let rc = AtomicBool::new(false);
		let mut stack = TreeStack::new();
		// Index the records
		for (k, v) in values.into_iter() {
			if self.is_aborted().await {
				return Ok(());
			}
			self.is_beyond_threshold(Some(*count))?;
			let key = thing::ThingKey::decode_key(&k)?;
			// Parse the value
			let val = Record::kv_decode_value(v)?;
			let rid: Arc<RecordId> = RecordId {
				table: key.tb.to_owned(),
				key: key.id,
			}
			.into();

			let opt_values;

			// Do we already have an appended value?
			let ip = self.ikb.new_ip_key(rid.key.clone());
			if let Some(pa) = tx.get(&ip, None).await? {
				// Then we take the old value of the appending value as the initial indexing
				// value
				let ia = self.ikb.new_ia_key(pa.0);
				let a = tx
					.get(&ia, None)
					.await?
					.ok_or_else(|| Error::CorruptedIndex("Appending record is missing"))?;
				opt_values = a.old_values;
			} else {
				// Otherwise, we normally proceed to the indexing
				let doc = CursorDoc::new(Some(rid.clone()), None, val);
				opt_values = stack
					.enter(|stk| Document::build_opt_values(stk, ctx, &self.opt, &self.ix, &doc))
					.finish()
					.await?;
			}

			// Index the record
			let mut io = IndexOperation::new(
				ctx,
				&self.opt,
				self.ns,
				self.db,
				&self.ix,
				None,
				opt_values.clone(),
				&rid,
			);
			stack.enter(|stk| io.compute(stk, &rc)).finish().await?;

			// Increment the count and update the status
			*count += 1;
			self.set_status(BuildingStatus::Indexing {
				initial: Some(*count),
				pending: Some(self.queue.read().await.pending() as usize),
				updated: None,
			})
			.await;
		}
		// Check if we trigger the compaction
		self.check_index_compaction(tx, &rc).await?;
		// We're done
		Ok(())
	}

	async fn index_appending_range(
		&self,
		ctx: &Context,
		tx: &Transaction,
		range: Range<u32>,
		initial: usize,
		count: &mut usize,
	) -> Result<()> {
		let rc = AtomicBool::new(false);
		let mut stack = TreeStack::new();
		for i in range {
			if self.is_aborted().await {
				return Ok(());
			}
			self.is_beyond_threshold(Some(*count))?;
			let ia = self.ikb.new_ia_key(i);
			if let Some(a) = tx.get(&ia, None).await? {
				tx.del(&ia).await?;
				let rid = RecordId {
					table: self.ikb.table().to_string(),
					key: a.id,
				};
				let mut io = IndexOperation::new(
					ctx,
					&self.opt,
					self.ns,
					self.db,
					&self.ix,
					a.old_values,
					a.new_values,
					&rid,
				);
				stack.enter(|stk| io.compute(stk, &rc)).finish().await?;

				// We can delete the ip record if any
				let ip = self.ikb.new_ip_key(rid.key);
				tx.del(&ip).await?;

				*count += 1;
				self.set_status(BuildingStatus::Indexing {
					initial: Some(initial),
					pending: Some(self.queue.read().await.pending() as usize),
					updated: Some(*count),
				})
				.await;
			}
		}
		// Check if we trigger the compaction
		self.check_index_compaction(tx, &rc).await?;
		// We're done
		Ok(())
	}

	async fn check_index_compaction(&self, tx: &Transaction, rc: &AtomicBool) -> Result<()> {
		if !rc.load(Ordering::Relaxed) {
			return Ok(());
		}
		FullTextIndex::trigger_compaction(&self.ikb, tx, self.opt.id()?).await?;
		rc.store(false, Ordering::Relaxed);
		Ok(())
	}
	/// Abort the current indexing process.
	fn abort(&self) {
		// We use `Ordering::Relaxed` as the called does not require to be synchronized.
		// We just want the current builder to eventually stop.
		self.aborted.store(true, Ordering::Relaxed);
	}

	/// Check if the indexing process is aborting.
	async fn is_aborted(&self) -> bool {
		// We use `Ordering::Relaxed` as there are no shared data that would require any
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
		if let Some(count) = count {
			if count % 100 != 0 {
				return Ok(());
			}
		}
		if ALLOC.is_beyond_threshold() {
			Err(anyhow::Error::new(Error::QueryBeyondMemoryThreshold))
		} else {
			Ok(())
		}
	}
}
