use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::vector::store::{PointKeyProvider, PointsStore, StoredPoint};
use crate::idx::vector::Vector;
use crate::idx::{IndexKeyBase, StoreType};
use crate::kvs::Transaction;
use crate::sql::index::VectorType;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(super) struct Points {
	store: Arc<Mutex<PointsStore>>,
}

impl Points {
	pub(super) fn new(ikb: IndexKeyBase, vt: VectorType, st: StoreType) -> Self {
		// TODO replace 40 by a configuration parameter
		let store = PointsStore::new(PointKeyProvider::Point(ikb), st, 40, vt);
		Self {
			store,
		}
	}

	pub(super) async fn _get(
		&mut self,
		tx: &mut Transaction,
		id: DocId,
	) -> Result<Option<StoredPoint>, Error> {
		self.store.lock().await._get(tx, id).await
	}

	pub(super) async fn set(&mut self, id: DocId, point: Vector) -> Result<(), Error> {
		self.store.lock().await.put(id, point)?;
		Ok(())
	}

	pub(super) async fn remove(&mut self, id: DocId) -> Result<(), Error> {
		self.store.lock().await.remove(id, None)?;
		Ok(())
	}

	pub(crate) async fn finish(&mut self, tx: &mut Transaction) -> Result<(), Error> {
		self.store.lock().await.finish(tx).await?;
		Ok(())
	}
}
