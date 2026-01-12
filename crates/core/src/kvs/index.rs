use super::KeyDecode;
use crate::cnf::{INDEXING_BATCH_SIZE, NORMAL_FETCH_SIZE};
use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::idx::index::IndexOperation;
use crate::key::index::df::Df;
use crate::key::index::ia::Ia;
use crate::key::index::ip::Ip;
use crate::key::thing;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::LockType::Optimistic;
use crate::kvs::{Key, Transaction, TransactionType, Val};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Id, Object, Thing, Value};
use ahash::HashMap;
use futures::channel::oneshot::{channel, Receiver, Sender};
use reblessive::TreeStack;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use tracing::warn;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;

#[derive(Debug, Clone)]
pub(crate) enum BuildingStatus {
	/// The indexing process has started
	Started,
	/// The index is being cleaned
	Cleaning,
	/// The index is currently being built
	Indexing {
		/// The number of initial records indexed
		initial: Option<usize>,
		/// The number of records updated since the initial indexing
		updated: Option<usize>,
		/// The number of records pending in the appending queue
		pending: Option<usize>,
	},
	/// The index is ready and fully up-to-date
	Ready {
		/// The number of initial records indexed
		initial: Option<usize>,
		/// The number of records updated since the initial indexing
		updated: Option<usize>,
		/// The number of records pending in the appending queue
		pending: Option<usize>,
	},
	/// The indexing process was aborted
	Aborted,
	/// An error occurred during the indexing process
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

type IndexBuilding = Arc<Building>;

/// A unique key for an index building process
#[derive(Hash, PartialEq, Eq, Clone)]
struct IndexKey {
	ns: String,
	db: String,
	tb: String,
	ix: String,
}

impl IndexKey {
	fn new(ns: &str, db: &str, tb: &str, ix: &str) -> Self {
		Self {
			ns: ns.to_owned(),
			db: db.to_owned(),
			tb: tb.to_owned(),
			ix: ix.to_owned(),
		}
	}
}

/// The builder for background index creation
#[derive(Clone)]
pub(crate) struct IndexBuilder {
	tf: TransactionFactory,
	indexes: Arc<RwLock<HashMap<IndexKey, IndexBuilding>>>,
}

impl IndexBuilder {
	/// Create a new IndexBuilder
	pub(super) fn new(tf: TransactionFactory) -> Self {
		Self {
			tf,
			indexes: Default::default(),
		}
	}

	/// Restart deferred indexes after database startup
	pub(super) async fn restart_deferred_index(
		&self,
		ctx: &Context,
		opt: &Options,
		tb: &str,
		ix: &DefineIndexStatement,
	) -> Result<(), Error> {
		if !ix.defer {
			return Ok(());
		}
		let (ns, db) = opt.ns_db()?;
		let tx = ctx.tx();
		let df = Df::new(ns, db, tb, &ix.name);
		let key = IndexKey::new(ns, db, df.tb, df.ix);
		let initial_build_done = match tx.get(df, None).await? {
			None => false,
			Some(v) => revision::from_slice::<bool>(&v)?,
		};
		let ix = Arc::new(ix.clone());
		if let Entry::Vacant(e) = self.indexes.write().await.entry(key) {
			let building = if initial_build_done {
				self.start_deferred_index(ctx, opt.clone(), ix).await?
			} else {
				self.start_building(ctx, opt.clone(), ix, None).await?
			};
			e.insert(building);
		}
		Ok(())
	}

	/// Start building an index in the background
	async fn start_building(
		&self,
		ctx: &Context,
		opt: Options,
		ix: Arc<DefineIndexStatement>,
		sdr: Option<Sender<Result<(), Error>>>,
	) -> Result<IndexBuilding, Error> {
		let building = Arc::new(Building::new(ctx, self.tf.clone(), opt, ix)?);
		building.recover_queue().await?;
		let b = building.clone();
		spawn(async move {
			// Ensure that in case of an unexpected exit the initial build is marked as complete
			let initial_guard = InitialBuildGuard(b.clone());
			let r = b.run().await;
			if let Err(err) = &r {
				b.set_status(BuildingStatus::Error(err.to_string())).await;
			}
			let is_err = r.is_err();
			if let Some(s) = sdr {
				if s.send(r).is_err() {
					warn!("Failed to send index building result to the consumer");
				}
			}
			if is_err {
				return;
			}
			if b.ix.defer {
				// If it is a deferred indexing, start the daemon and return
				Self::spawn_deferred_daemon(b.clone());
			}
			drop(initial_guard);
		});
		Ok(building)
	}

	async fn start_deferred_index(
		&self,
		ctx: &Context,
		opt: Options,
		ix: Arc<DefineIndexStatement>,
	) -> Result<IndexBuilding, Error> {
		let building = Arc::new(Building::new(ctx, self.tf.clone(), opt, ix)?);
		building.recover_queue().await?;
		building.initial_build_complete.store(true, Ordering::Relaxed);
		Self::spawn_deferred_daemon(building.clone());
		Ok(building)
	}

	fn spawn_deferred_daemon(building: IndexBuilding) {
		spawn(async move {
			// Ensure that the daemon running flag is properly managed
			let daemon_guard = DeferredDaemonGuard(building.clone());
			building.deferred_daemon_running.store(true, Ordering::Relaxed);
			loop {
				if building.is_aborted().await {
					building.set_status(BuildingStatus::Aborted).await;
					break;
				}
				if let Err(e) = building.index_appending_loop(None).await {
					error!("Index appending loop error: {}", e);
					building.set_status(BuildingStatus::Error(e.to_string())).await;
					break;
				}
				sleep(Duration::from_millis(100)).await;
			}
			drop(daemon_guard);
		});
	}

	/// Build an index
	pub(crate) async fn build(
		&self,
		ctx: &Context,
		opt: Options,
		ix: Arc<DefineIndexStatement>,
		blocking: bool,
	) -> Result<Option<Receiver<Result<(), Error>>>, Error> {
		let (ns, db) = opt.ns_db()?;
		let key = IndexKey::new(ns, db, &ix.what, &ix.name);
		let (rcv, sdr) = if blocking {
			let (s, r) = channel();
			(Some(r), Some(s))
		} else {
			(None, None)
		};
		match self.indexes.write().await.entry(key.clone()) {
			Entry::Occupied(mut e) => {
				// If the building is currently running, we need to wait for it to finish
				let old_builder = e.get();
				// Abort the old builder to signal it should stop
				old_builder.abort();
				// Wait for the old builder to fully stop
				old_builder.wait_for_completion().await;
				// Start building the index
				let ib = self.start_building(ctx, opt, ix, sdr).await?;
				e.insert(ib);
			}
			Entry::Vacant(e) => {
				// No index is currently building, we can start building it
				let ib = self.start_building(ctx, opt, ix, sdr).await?;
				e.insert(ib);
			}
		}
		Ok(rcv)
	}

	/// Consume a document update for indexing
	pub(crate) async fn consume(
		&self,
		ctx: &Context,
		opt: &Options,
		ix: &DefineIndexStatement,
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
		rid: &Thing,
	) -> Result<ConsumeResult, Error> {
		let (ns, db) = opt.ns_db()?;
		let key = IndexKey::new(ns, db, &ix.what, &ix.name);
		// This is the normal path
		if let Some(building) = self.indexes.read().await.get(&key) {
			// And index is currently building, or is a deferred index
			return building.maybe_consume(ctx, old_values, new_values, rid).await;
		}
		// There is no index building process (defer or initial building)
		Ok(ConsumeResult::Ignored(old_values, new_values))
	}

	/// Get the status of an index building process
	pub(crate) async fn get_status(
		&self,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
	) -> BuildingStatus {
		let key = IndexKey::new(ns, db, &ix.what, &ix.name);
		if let Some(b) = self.indexes.read().await.get(&key) {
			b.status.read().await.clone()
		} else {
			BuildingStatus::default()
		}
	}

	/// Remove an index building process
	pub(crate) async fn remove_index(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		ix: &str,
	) -> Result<(), Error> {
		let key = IndexKey::new(ns, db, tb, ix);
		if let Some(b) = self.indexes.write().await.remove(&key) {
			b.abort();
		}
		Ok(())
	}
}

/// A document update enqueued for background indexing
#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize, Debug)]
#[non_exhaustive]
struct Appending {
	/// The old values of the document
	old_values: Option<Vec<Value>>,
	/// The new values of the document
	new_values: Option<Vec<Value>>,
	/// The ID of the document
	id: Id,
}

/// A sequence number for the primary key of the appending queue
#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize, Debug)]
#[non_exhaustive]
struct PrimaryAppending(u32);

/// Manages the sequence numbers for the background indexing queue
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

	/// Returns the number of updates pending in the queue
	fn pending(&self) -> u32 {
		self.next - self.to_index
	}

	/// Sets the index of the next update to be indexed
	fn set_to_index(&mut self, i: u32) {
		self.to_index = i;
	}

	/// Returns the range of the next batch of updates to be indexed
	fn next_indexing_batch(&self, page: u32) -> Range<u32> {
		let s = self.to_index;
		let e = (s + page).min(self.next);
		s..e
	}

	/// Restores the queue range from the given minimum and maximum indices
	fn restore_range(&mut self, min: u32, max: u32) {
		self.to_index = min;
		self.next = max.saturating_add(1);
	}
}

/// A building process for a specific index
struct Building {
	/// The context used for the building process
	ctx: Context,
	/// The options used for the building process
	opt: Options,
	/// The transaction factory used to create transactions
	tf: TransactionFactory,
	/// The statement that defines the index
	ix: Arc<DefineIndexStatement>,
	/// The table name
	tb: String,
	/// The current status of the building process
	status: RwLock<BuildingStatus>,
	/// The queue of records that need to be indexed
	queue: RwLock<QueueSequences>,
	/// Whether the building process has been aborted
	aborted: AtomicBool,
	/// Whether the initial building process (run()) has completed
	initial_build_complete: AtomicBool,
	/// Whether the deferred daemon is currently running (for deferred indexes only)
	deferred_daemon_running: AtomicBool,
	/// Notifier for signaling completion (initial build or daemon shutdown)
	completion_notify: Notify,
}

impl Building {
	fn new(
		ctx: &Context,
		tf: TransactionFactory,
		opt: Options,
		ix: Arc<DefineIndexStatement>,
	) -> Result<Self, Error> {
		Ok(Self {
			ctx: MutableContext::new_concurrent(ctx).freeze(),
			opt,
			tf,
			tb: ix.what.to_raw(),
			ix,
			status: RwLock::new(BuildingStatus::Started),
			queue: Default::default(),
			aborted: AtomicBool::new(false),
			initial_build_complete: AtomicBool::new(false),
			deferred_daemon_running: AtomicBool::new(false),
			completion_notify: Notify::new(),
		})
	}

	/// Set the status of the building process
	async fn set_status(&self, status: BuildingStatus) {
		let mut s = self.status.write().await;
		// We want to keep only the first error
		if !s.is_error() {
			*s = status;
		}
	}

	/// Recovers the index queue from the storage.
	/// This is used when the server is restarted during an indexing process.
	async fn recover_queue(&self) -> Result<(), Error> {
		let (ns, db) = self.opt.ns_db()?;
		let beg = crate::key::index::ia::prefix_beg(ns, db, &self.ix.what, &self.ix.name)?;
		let end = crate::key::index::ia::prefix_end(ns, db, &self.ix.what, &self.ix.name)?;
		let mut next = Some(beg..end);
		let mut min_idx: Option<u32> = None;
		let mut max_idx: Option<u32> = None;
		while let Some(rng) = next {
			let tx = self.new_read_tx().await?;
			let batch = catch!(tx, tx.batch_keys(rng, *NORMAL_FETCH_SIZE, None).await);
			next = batch.next;
			for key in batch.result {
				let ia = Ia::decode(&key)?;
				min_idx = Some(min_idx.map_or(ia.i, |current| current.min(ia.i)));
				max_idx = Some(max_idx.map_or(ia.i, |current| current.max(ia.i)));
			}
		}
		if let Some(max) = max_idx {
			let min = min_idx.unwrap_or(max);
			let mut queue = self.queue.write().await;
			queue.restore_range(min, max);
		}
		Ok(())
	}

	/// Try to consume a document update.
	/// If the index is currently building, the update is enqueued to be indexed asynchronously.
	/// If the index is already built and not deferred, the update is ignored and the document can be updated normally.
	async fn maybe_consume(
		&self,
		ctx: &Context,
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
		rid: &Thing,
	) -> Result<ConsumeResult, Error> {
		let mut queue = self.queue.write().await;
		// Now that the queue is locked, we have the possibility to assess if the asynchronous build is done.
		if queue.is_empty() {
			// If the appending queue is empty and the index is built, and it is not a deferred index:
			if !self.ix.defer && self.status.read().await.is_ready() {
				// ... we return the values back, so the document can be updated the usual way
				return Ok(ConsumeResult::Ignored(old_values, new_values));
			}
		}

		let tx = ctx.tx();
		let a = Appending {
			old_values,
			new_values,
			id: rid.id.clone(),
		};
		// Get the idx of this appended record from the sequence
		let idx = queue.add_update();
		// Store the appending
		let ia = self.new_ia_key(idx)?;
		tx.set(ia, revision::to_vec(&a)?, None).await?;
		// Do we already have a primary appending?
		let ip = self.new_ip_key(rid.id.clone())?;
		if tx.get(ip.clone(), None).await?.is_none() {
			// If not, we set it
			tx.set(ip, revision::to_vec(&PrimaryAppending(idx))?, None).await?;
		}
		// Free the queue
		drop(queue);
		Ok(ConsumeResult::Enqueued)
	}

	fn new_ia_key(&self, i: u32) -> Result<Ia<'_>, Error> {
		let (ns, db) = self.opt.ns_db()?;
		Ok(Ia::new(ns, db, &self.ix.what, &self.ix.name, i))
	}

	fn new_ip_key(&self, id: Id) -> Result<Ip<'_>, Error> {
		let (ns, db) = self.opt.ns_db()?;
		Ok(Ip::new(ns, db, &self.ix.what, &self.ix.name, id))
	}

	async fn new_read_tx(&self) -> Result<Transaction, Error> {
		self.tf.transaction(TransactionType::Read, Optimistic).await
	}

	async fn new_write_tx_ctx(&self) -> Result<Context, Error> {
		let tx = self.tf.transaction(TransactionType::Write, Optimistic).await?.into();
		let mut ctx = MutableContext::new(&self.ctx);
		ctx.set_transaction(tx);
		Ok(ctx.freeze())
	}

	/// Run the initial building process
	async fn run(&self) -> Result<(), Error> {
		let (ns, db) = self.opt.ns_db()?;
		// Remove the index data
		{
			self.set_status(BuildingStatus::Cleaning).await;
			let ctx = self.new_write_tx_ctx().await?;
			let key = crate::key::index::all::new(ns, db, &self.tb, &self.ix.name);
			let tx = ctx.tx();
			catch!(tx, tx.delp(&key).await);
			if self.ix.defer {
				// We need to know we have started building a deferred index
				// and that the initial build is not done
				catch!(tx, tx.set(key, revision::to_vec(&false)?, None).await);
			}
			tx.commit().await?;
		}

		// First iteration, we index every keys
		let beg = thing::prefix(ns, db, &self.tb)?;
		let end = thing::suffix(ns, db, &self.tb)?;
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
			// Get the next batch of records
			let batch = {
				let tx = self.new_read_tx().await?;
				catch!(tx, tx.batch_keys_vals(rng, *INDEXING_BATCH_SIZE, None).await)
			};
			// Set the next scan range
			next = batch.next;
			// Check there are records
			if batch.result.is_empty() {
				// If not, we are done with the initial indexing
				break;
			}
			// Create a new context with a write transaction
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
		self.set_status(BuildingStatus::Indexing {
			initial: Some(initial_count),
			pending: Some(self.queue.read().await.pending() as usize),
			updated: None,
		})
		.await;
		if !self.ix.defer {
			// Second iteration, we index/remove any records that have been added or removed since the initial indexing
			self.index_appending_loop(Some(initial_count)).await?;
		} else {
			self.set_df_status(true).await?;
		}
		Ok(())
	}

	async fn set_df_status(&self, initial_build_done: bool) -> Result<(), Error> {
		let (ns, db) = self.opt.ns_db()?;
		// confirm that the initial build has been successful
		let key = Df::new(ns, db, &self.ix.what, &self.ix.name);
		let ctx = self.new_write_tx_ctx().await?;
		let tx = ctx.tx();
		catch!(tx, tx.set(key, revision::to_vec(&initial_build_done)?, None).await);
		tx.commit().await?;
		Ok(())
	}

	/// Loop through the appending queue and index the records
	async fn index_appending_loop(&self, initial_count: Option<usize>) -> Result<(), Error> {
		let mut updates_count = initial_count.map(|_| 0);
		let mut next_to_index = None;
		loop {
			if self.is_aborted().await {
				break;
			}
			let range = {
				let mut queue = self.queue.write().await;
				if let Some(ni) = next_to_index {
					queue.set_to_index(ni);
				}
				if queue.is_empty() {
					// If the batch is empty, we are done.
					// Due to the lock on self.queue, we know that no external process can add an item to the queue.
					self.set_status(BuildingStatus::Ready {
						initial: initial_count,
						pending: Some(queue.pending() as usize),
						updated: updates_count,
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

	/// Index a batch of records from the table
	async fn index_initial_batch(
		&self,
		ctx: &Context,
		tx: &Transaction,
		values: Vec<(Key, Val)>,
		count: &mut usize,
	) -> Result<(), Error> {
		let mut rc = false;
		let mut stack = TreeStack::new();
		// Index the records
		for (k, v) in values.into_iter() {
			if self.is_aborted().await {
				return Ok(());
			}
			let key = thing::Thing::decode(&k)?;
			// Parse the value
			let val: Value = revision::from_slice(&v)?;
			let rid: Arc<Thing> = Thing::from((key.tb, key.id)).into();

			let opt_values;

			// Do we already have an appended value?
			let ip = self.new_ip_key(rid.id.clone())?;
			if let Some(v) = tx.get(ip, None).await? {
				// Then we take the old value of the appending value as the initial indexing value
				let pa: PrimaryAppending = revision::from_slice(&v)?;
				let ia = self.new_ia_key(pa.0)?;
				let v = tx
					.get(ia, None)
					.await?
					.ok_or_else(|| Error::CorruptedIndex("Appending record is missing"))?;
				let a: Appending = revision::from_slice(&v)?;
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
			let mut io =
				IndexOperation::new(ctx, &self.opt, &self.ix, None, opt_values.clone(), &rid);
			stack.enter(|stk| io.compute(stk, &mut rc)).finish().await?;

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
		self.check_index_compaction(tx, &mut rc).await?;
		// We're done
		Ok(())
	}

	/// Index a range of records from the appending queue
	async fn index_appending_range(
		&self,
		ctx: &Context,
		tx: &Transaction,
		range: Range<u32>,
		initial: Option<usize>,
		count: &mut Option<usize>,
	) -> Result<(), Error> {
		let mut rc = false;
		let mut stack = TreeStack::new();
		for i in range {
			if self.is_aborted().await {
				return Ok(());
			}
			let ia = self.new_ia_key(i)?;
			if let Some(v) = tx.get(ia.clone(), None).await? {
				tx.del(ia).await?;
				let a: Appending = revision::from_slice(&v)?;
				let rid = Thing::from((self.tb.clone(), a.id));
				let mut io =
					IndexOperation::new(ctx, &self.opt, &self.ix, a.old_values, a.new_values, &rid);
				stack.enter(|stk| io.compute(stk, &mut rc)).finish().await?;

				// We can delete the ip record if any
				let ip = self.new_ip_key(rid.id)?;
				tx.del(ip).await?;

				if let Some(c) = count {
					*c += 1;
				}
				self.set_status(BuildingStatus::Indexing {
					initial,
					pending: Some(self.queue.read().await.pending() as usize),
					updated: *count,
				})
				.await;
			}
		}
		// Check if we trigger the compaction
		self.check_index_compaction(tx, &mut rc).await?;
		// We're done
		Ok(())
	}

	/// Check if the index needs compaction and trigger it if necessary
	async fn check_index_compaction(&self, tx: &Transaction, rc: &mut bool) -> Result<(), Error> {
		if !*rc {
			return Ok(());
		}
		let (ns, db) = self.opt.ns_db()?;
		IndexOperation::put_trigger_compaction(
			ns,
			db,
			&self.ix.what,
			&self.ix.name,
			tx,
			self.opt.id()?,
		)
		.await?;
		*rc = false;
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
		// We use `Ordering::Relaxed` as there are no shared data that would require any synchronization.
		// This method is only called by the single thread building the index.
		if self.aborted.load(Ordering::Relaxed) {
			self.set_status(BuildingStatus::Aborted).await;
			true
		} else {
			false
		}
	}

	fn is_finished(&self) -> bool {
		// Check if initial build is complete
		// Use Acquire ordering to synchronize with Release in guards
		if !self.initial_build_complete.load(Ordering::Acquire) {
			return false;
		}
		// For deferred indexes, we're not finished while the daemon is running
		if self.deferred_daemon_running.load(Ordering::Acquire) {
			return false;
		}
		true
	}

	/// Wait for the builder to finish (both initial build and deferred daemon if applicable)
	async fn wait_for_completion(&self) {
		// Fast path: already finished
		if self.is_finished() {
			return;
		}
		// Wait for notification, but re-check condition after each wake
		// (handles spurious wakeups and race conditions)
		loop {
			// Use notified() future - this registers interest before checking condition
			let notified = self.completion_notify.notified();
			// Re-check after registering (avoids race condition)
			if self.is_finished() {
				return;
			}
			// Wait for notification
			notified.await;
			// Check again after being notified
			if self.is_finished() {
				return;
			}
		}
	}
}

/// Guard to mark the initial build as complete when dropped
struct InitialBuildGuard(IndexBuilding);

impl Drop for InitialBuildGuard {
	fn drop(&mut self) {
		self.0.initial_build_complete.store(true, Ordering::Release);
		self.0.completion_notify.notify_waiters();
	}
}

/// Guard to track the deferred daemon lifecycle
struct DeferredDaemonGuard(IndexBuilding);

impl Drop for DeferredDaemonGuard {
	fn drop(&mut self) {
		self.0.deferred_daemon_running.store(false, Ordering::Release);
		self.0.completion_notify.notify_waiters();
	}
}
