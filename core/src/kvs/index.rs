use crate::cnf::{EXPORT_BATCH_SIZE, NORMAL_FETCH_SIZE};
use crate::dbs::Options;
use crate::err::Error;
use crate::kvs::ds::Inner;
use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::Id;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

enum BuildingStatus {
	INITIATED,
	BUILDING(usize),
	ERROR(Error),
	BUILT,
}

pub(crate) struct IndexBuilder {
	inner: Arc<Inner>,
	indexes: DashMap<DefineIndexStatement, (Building, JoinHandle<()>)>,
}

impl IndexBuilder {
	pub(crate) fn new(inner: Arc<Inner>) -> Self {
		Self {
			inner,
			indexes: Default::default(),
		}
	}

	pub(crate) fn build(&mut self, opt: &Options, ix: DefineIndexStatement) -> Result<(), Error> {
		match self.indexes.entry(ix) {
			Entry::Occupied(e) => {
				// If the building is currently running we return error
				if !e.get().is_finished() {
					panic!("Is running") // TODO replace by error
				}
			}
			Entry::Vacant(e) => {
				// No index is currently building, we can start building it
				let building = Building::new(self.inner.clone(), opt, e.key())?;
				let jh = tokio::spawn(|| building.compute());
				e.insert((building, jh));
			}
		}
		Ok(())
	}

	pub(crate) async fn updated(&self, ix: &DefineIndexStatement, id: Id) -> Result<(), Error> {
		if let Some(mut e) = self.indexes.get(ix) {
			e.value().0.append(Appending::Updated(id)).await;
			Ok(())
		} else {
			panic!("Not running") // TODO replace by error
		}
	}

	pub(crate) async fn removed(&self, ix: &DefineIndexStatement, id: Id) -> Result<(), Error> {
		if let Some(mut e) = self.indexes.get(ix) {
			e.value().0.append(Appending::Removed(id)).await;
			Ok(())
		} else {
			panic!("Not running") // TODO replace by error
		}
	}
}

enum Appending {
	Updated(Id),
	Removed(Id),
}

struct Building {
	inner: Arc<Inner>,
	ns: String,
	db: String,
	tb: String,
	status: Arc<Mutex<BuildingStatus>>,
	// Should be stored on a temporary table
	appended: Arc<Mutex<VecDeque<Appending>>>,
	// Index barrier
	index_barrier: Arc<Mutex<()>>,
}

impl Building {
	fn new(inner: Arc<Inner>, opt: &Options, ix: &DefineIndexStatement) -> Result<Self, Error> {
		Ok(Self {
			inner,
			ns: opt.ns()?.to_string(),
			db: opt.db()?.to_string(),
			tb: ix.what.to_string(),
			status: Arc::new(Mutex::new(BuildingStatus::INITIATED)),
			appended: Default::default(),
			index_barrier: Default::default(),
		})
	}

	async fn append(&self, a: Appending) {
		self.appended.lock().await.push_back(a);
	}

	async fn compute(&self) -> Result<(), Error> {
		*self.status.lock().await = BuildingStatus::BUILDING(0);
		// First iteration, we index every keys
		let beg = crate::key::thing::prefix(&self.ns, &self.db, &self.tb);
		let end = crate::key::thing::suffix(&self.ns, &self.db, &self.tb);
		let mut next = Some(beg..end);
		let mut count = 0;
		while let Some(rng) = next {
			let tx = self.inner.tra
			// Get the next batch of records
			let batch = tx.batch(rng, *EXPORT_BATCH_SIZE, true).await?;
			// Set the next scan range
			next = batch.next;
			// Check there are records
			if batch.values.is_empty() {
				break;
			}
			// Index the records
			for (_, v) in batch.values.into_iter() {
				count += 1;
				*self.status.lock().await = BuildingStatus::BUILDING(count);
			}
			tx.commit()?;
		}
		// Second iteration, we index/remove any keys that has been added or removed since the initial indexing
		loop {
			let batch = self.appended.lock().await.drain(0..*NORMAL_FETCH_SIZE as usize);
			if batch.len() == 0 {
				// LOCK INDEXING
				// if self.appended is still empty, we are done
				// UNLOCK INDEXING
				break;
			}
			let tx = self.ds.transaction(TransactionType::Write, Optimistic).await?;
			for id in batch {
				match id {
					Appending::Updated(id) => {
						let key = crate::key::thing::new(&self.ns, &self.db, &self.tb, &id);
						let val = tx.get(key).await?;
						todo!("Add to index");
					}
					Appending::Removed(id) => {
						todo!("Remove from index")
					}
				}
				count += 1;
				*self.status.lock().await = BuildingStatus::BUILDING(count);
			}
			tx.commit()?;
		}
		Ok(())
	}
}
