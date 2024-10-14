use crate::cnf::{INDEXING_BATCH_SIZE, NORMAL_FETCH_SIZE};
use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::idx::index::IndexOperation;
use crate::key::index::ia::Ia;
use crate::key::index::ip::Ip;
use crate::key::thing;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::LockType::Optimistic;
use crate::kvs::{Key, Transaction, TransactionType, Val};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Id, Object, Thing, Value};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use derive::Store;
use reblessive::TreeStack;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub(crate) enum BuildingStatus {
	Started,
	InitialIndexing(usize),
	UpdatesIndexing(usize),
	Error(Arc<Error>),
	Built,
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

	fn is_built(&self) -> bool {
		matches!(self, Self::Built)
	}
}

impl From<BuildingStatus> for Value {
	fn from(st: BuildingStatus) -> Self {
		let mut o = Object::default();
		let s = match st {
			BuildingStatus::Started => "started",
			BuildingStatus::InitialIndexing(count) => {
				o.insert("count".to_string(), count.into());
				"initial"
			}
			BuildingStatus::UpdatesIndexing(count) => {
				o.insert("count".to_string(), count.into());
				"updates"
			}
			BuildingStatus::Error(error) => {
				o.insert("error".to_string(), error.to_string().into());
				"error"
			}
			BuildingStatus::Built => "built",
		};
		o.insert("status".to_string(), s.into());
		o.into()
	}
}

type IndexBuilding = (Arc<Building>, JoinHandle<()>);

#[derive(Clone)]
pub(crate) struct IndexBuilder {
	tf: TransactionFactory,
	indexes: Arc<DashMap<Arc<DefineIndexStatement>, IndexBuilding>>,
}

impl IndexBuilder {
	pub(super) fn new(tf: TransactionFactory) -> Self {
		Self {
			tf,
			indexes: Default::default(),
		}
	}

	pub(crate) fn build(
		&self,
		ctx: &Context,
		opt: Options,
		ix: Arc<DefineIndexStatement>,
	) -> Result<(), Error> {
		match self.indexes.entry(ix) {
			Entry::Occupied(e) => {
				// If the building is currently running we return error
				if !e.get().1.is_finished() {
					return Err(Error::IndexAlreadyBuilding {
						index: e.key().name.to_string(),
					});
				}
			}
			Entry::Vacant(e) => {
				// No index is currently building, we can start building it
				let building = Arc::new(Building::new(ctx, self.tf.clone(), opt, e.key().clone())?);
				let b = building.clone();
				let jh = task::spawn(async move {
					if let Err(err) = b.compute().await {
						b.set_status(BuildingStatus::Error(err.into())).await;
					}
				});
				e.insert((building, jh));
			}
		}
		Ok(())
	}

	pub(crate) async fn consume(
		&self,
		ctx: &Context,
		ix: &DefineIndexStatement,
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
		rid: &Thing,
	) -> Result<ConsumeResult, Error> {
		if let Some(r) = self.indexes.get(ix) {
			let (b, _) = r.value();
			return b.maybe_consume(ctx, old_values, new_values, rid).await;
		}
		Ok(ConsumeResult::Ignored(old_values, new_values))
	}

	pub(crate) async fn get_status(&self, ix: &DefineIndexStatement) -> Option<BuildingStatus> {
		if let Some(a) = self.indexes.get(ix) {
			Some(a.value().0.status.lock().await.clone())
		} else {
			None
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize, Store, Debug)]
#[non_exhaustive]
struct Appending {
	old_values: Option<Vec<Value>>,
	new_values: Option<Vec<Value>>,
	id: Id,
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize, Store, Debug)]
#[non_exhaustive]
struct PrimaryAppending(u32);

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
	tf: TransactionFactory,
	ix: Arc<DefineIndexStatement>,
	tb: String,
	status: Arc<Mutex<BuildingStatus>>,
	// Should be stored on a temporary table
	queue: Arc<Mutex<QueueSequences>>,
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
			tb: ix.what.to_string(),
			ix,
			status: Arc::new(Mutex::new(BuildingStatus::Started)),
			queue: Default::default(),
		})
	}

	async fn set_status(&self, status: BuildingStatus) {
		let mut s = self.status.lock().await;
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
		rid: &Thing,
	) -> Result<ConsumeResult, Error> {
		let mut queue = self.queue.lock().await;
		// Now that the queue is locked, we have the possibility to assess if the asynchronous build is done.
		if queue.is_empty() {
			// If the appending queue is empty and the index is built...
			if self.status.lock().await.is_built() {
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
		tx.set(ia, a, None).await?;
		// Do we already have a primary appending?
		let ip = self.new_ip_key(rid.id.clone())?;
		if tx.get(ip.clone(), None).await?.is_none() {
			// If not we set it
			tx.set(ip, PrimaryAppending(idx), None).await?;
		}
		Ok(ConsumeResult::Enqueued)
	}

	fn new_ia_key(&self, i: u32) -> Result<Ia, Error> {
		let ns = self.opt.ns()?;
		let db = self.opt.db()?;
		Ok(Ia::new(ns, db, &self.ix.what, &self.ix.name, i))
	}

	fn new_ip_key(&self, id: Id) -> Result<Ip, Error> {
		let ns = self.opt.ns()?;
		let db = self.opt.db()?;
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

	async fn compute(&self) -> Result<(), Error> {
		// Set the initial status
		self.set_status(BuildingStatus::InitialIndexing(0)).await;
		// First iteration, we index every keys
		let ns = self.opt.ns()?;
		let db = self.opt.db()?;
		let beg = thing::prefix(ns, db, &self.tb);
		let end = thing::suffix(ns, db, &self.tb);
		let mut next = Some(beg..end);
		let mut count = 0;
		while let Some(rng) = next {
			// Get the next batch of records
			let tx = self.new_read_tx().await?;
			let batch = catch!(tx, tx.batch(rng, *INDEXING_BATCH_SIZE, true, None, false).await);
			// We can release the read transaction
			drop(tx);
			// Set the next scan range
			next = batch.next;
			// Check there are records
			if batch.values.is_empty() {
				// If not, we are with the initial indexing
				break;
			}
			// Create a new context with a write transaction
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			// Index the batch
			catch!(tx, self.index_initial_batch(&ctx, &tx, batch.values, &mut count).await);
			tx.commit().await?;
		}
		// Second iteration, we index/remove any records that has been added or removed since the initial indexing
		self.set_status(BuildingStatus::UpdatesIndexing(0)).await;
		loop {
			let mut queue = self.queue.lock().await;
			if queue.is_empty() {
				// If the batch is empty, we are done.
				// Due to the lock on self.appended, we know that no external process can add an item to the queue.
				self.set_status(BuildingStatus::Built).await;
				// This is here to be sure the lock on back is not released early
				queue.clear();
				break;
			}
			let range = queue.next_indexing_batch(*NORMAL_FETCH_SIZE);
			if range.is_empty() {
				continue;
			}
			let next_to_index = range.end;

			// Create a new context with a write transaction
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			catch!(tx, self.index_appending_range(&ctx, &tx, range, &mut count).await);
			tx.commit().await?;
			queue.set_to_index(next_to_index);
		}
		Ok(())
	}

	async fn index_initial_batch(
		&self,
		ctx: &Context,
		tx: &Transaction,
		values: Vec<(Key, Val)>,
		count: &mut usize,
	) -> Result<(), Error> {
		let mut stack = TreeStack::new();
		// Index the records
		for (k, v) in values.into_iter() {
			let key: thing::Thing = (&k).into();
			// Parse the value
			let val: Value = (&v).into();
			let rid: Arc<Thing> = Thing::from((key.tb, key.id)).into();

			let opt_values;

			// Do we already have an appended value?
			let ip = self.new_ip_key(rid.id.clone())?;
			if let Some(v) = tx.get(ip, None).await? {
				// Then we take the old value of the appending value as the initial indexing value
				let pa: PrimaryAppending = v.into();
				let ia = self.new_ia_key(pa.0)?;
				let v = tx
					.get(ia, None)
					.await?
					.ok_or_else(|| Error::CorruptedIndex("Appending record is missing"))?;
				let a: Appending = v.into();
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
			stack.enter(|stk| io.compute(stk)).finish().await?;

			// Increment the count and update the status
			*count += 1;
			self.set_status(BuildingStatus::InitialIndexing(*count)).await;
		}
		Ok(())
	}

	async fn index_appending_range(
		&self,
		ctx: &Context,
		tx: &Transaction,
		range: Range<u32>,
		count: &mut usize,
	) -> Result<(), Error> {
		let mut stack = TreeStack::new();
		for i in range {
			let ia = self.new_ia_key(i)?;
			if let Some(v) = tx.get(ia.clone(), None).await? {
				tx.del(ia).await?;
				let a: Appending = v.into();
				let rid = Thing::from((self.tb.clone(), a.id));
				let mut io =
					IndexOperation::new(ctx, &self.opt, &self.ix, a.old_values, a.new_values, &rid);
				stack.enter(|stk| io.compute(stk)).finish().await?;

				// We can delete the ip record if any
				let ip = self.new_ip_key(rid.id)?;
				tx.del(ip).await?;

				*count += 1;
				self.set_status(BuildingStatus::UpdatesIndexing(*count)).await;
			}
		}
		Ok(())
	}
}
