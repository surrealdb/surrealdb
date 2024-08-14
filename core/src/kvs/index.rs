use crate::cnf::{EXPORT_BATCH_SIZE, NORMAL_FETCH_SIZE};
use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::{build_opt_values, CursorDoc};
use crate::err::Error;
use crate::idx::index::IndexOperation;
use crate::key::thing;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::LockType::Optimistic;
use crate::kvs::{Transaction, TransactionType};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Id, Thing, Value};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use reblessive::TreeStack;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task;
use tokio::task::JoinHandle;

enum BuildingStatus {
	Initiated,
	Building(usize),
	Error(Error),
	Done,
}

#[derive(Clone)]
pub(crate) struct IndexBuilder {
	tf: TransactionFactory,
	indexes: Arc<DashMap<Arc<DefineIndexStatement>, (Arc<Building>, JoinHandle<()>)>>,
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
					panic!("Is running") // TODO replace by error
				}
			}
			Entry::Vacant(e) => {
				// No index is currently building, we can start building it
				let building = Arc::new(Building::new(ctx, self.tf.clone(), opt, e.key().clone())?);
				let b = building.clone();
				let jh = task::spawn(async move {
					if let Err(err) = b.compute().await {
						panic!("{err}") // TODO replace by error
					}
				});
				e.insert((building, jh));
			}
		}
		Ok(())
	}

	pub(crate) async fn append(
		&self,
		ix: &DefineIndexStatement,
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
		rid: Thing,
	) -> Result<(), Error> {
		if let Some(e) = self.indexes.get(ix) {
			e.value()
				.0
				.append(Appending {
					old_values,
					new_values,
					id: rid.id,
				})
				.await;
			Ok(())
		} else {
			panic!("Not running") // TODO replace by error
		}
	}
}

struct Appending {
	old_values: Option<Vec<Value>>,
	new_values: Option<Vec<Value>>,
	id: Id,
}

struct Building {
	ctx: Context,
	opt: Options,
	tf: TransactionFactory,
	ix: Arc<DefineIndexStatement>,
	tb: String,
	status: Arc<Mutex<BuildingStatus>>,
	// Should be stored on a temporary table
	appended: Arc<Mutex<VecDeque<Appending>>>,
	// Index barrier
	index_barrier: Arc<Mutex<()>>,
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
			status: Arc::new(Mutex::new(BuildingStatus::Initiated)),
			appended: Default::default(),
			index_barrier: Default::default(),
		})
	}

	async fn set_status(&self, status: BuildingStatus) {
		*self.status.lock().await = status;
	}

	async fn append(&self, a: Appending) {
		self.appended.lock().await.push_back(a);
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
		let mut stack = TreeStack::new();

		self.set_status(BuildingStatus::Building(0)).await;
		// First iteration, we index every keys
		let ns = self.opt.ns()?;
		let db = self.opt.db()?;
		let beg = crate::key::thing::prefix(ns, db, &self.tb);
		let end = crate::key::thing::suffix(ns, db, &self.tb);
		let mut next = Some(beg..end);
		let mut count = 0;
		while let Some(rng) = next {
			// Get the next batch of records
			let batch = self.new_read_tx().await?.batch(rng, *EXPORT_BATCH_SIZE, true).await?;
			// Set the next scan range
			next = batch.next;
			// Check there are records
			if batch.values.is_empty() {
				break;
			}
			// Create a new context with a write transaction
			let ctx = self.new_write_tx_ctx().await?;
			// Index the records
			for (k, v) in batch.values.into_iter() {
				let key: thing::Thing = (&k).into();
				// Parse the value
				let val: Value = (&v).into();
				let rid: Arc<Thing> = Thing::from((key.tb, key.id)).into();
				let doc = CursorDoc::new(Some(rid.clone()), None, val);
				let opt_values = stack
					.enter(|stk| build_opt_values(stk, &ctx, &self.opt, &self.ix, &doc))
					.finish()
					.await?;
				// Index the record
				let mut io = IndexOperation::new(&ctx, &self.opt, &self.ix, None, opt_values, &rid);
				stack.enter(|stk| io.compute(stk)).finish().await?;
				count += 1;
				self.set_status(BuildingStatus::Building(count)).await;
			}
			ctx.tx().commit().await?;
		}
		// Second iteration, we index/remove any records that has been added or removed since the initial indexing
		loop {
			let mut batch = self.appended.lock().await;
			let drain = batch.drain(0..*NORMAL_FETCH_SIZE as usize);
			if drain.len() == 0 {
				// ATOMIC SWAP
				// LOCK INDEXING
				// if self.appended is still empty, we are done, otherwise we continue
				// UNLOCK INDEXING
				break;
			}
			// Create a new context with a write transaction
			let ctx = self.new_write_tx_ctx().await?;

			for a in drain {
				let rid = Thing::from((self.tb.clone(), a.id));
				let mut io = IndexOperation::new(
					&ctx,
					&self.opt,
					&self.ix,
					a.old_values,
					a.new_values,
					&rid,
				);
				stack.enter(|stk| io.compute(stk)).finish().await?;
				count += 1;
				self.set_status(BuildingStatus::Building(count)).await;
			}
			ctx.tx().commit().await?;
		}
		self.set_status(BuildingStatus::Done).await;
		Ok(())
	}
}
