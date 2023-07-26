use crate::err::Error;
use crate::idx::btree::store::BTreeStoreType;
use crate::idx::docids::DocIds;
use crate::idx::IndexKeyBase;
use crate::kvs::Transaction;
use crate::sql::{Array, Thing};
use std::sync::Arc;
use tokio::sync::RwLock;

type _Vector = Vec<f64>;
type _NodeId = u64;
type _PointId = u64;
const _THRESHOLD: usize = 30;

struct _Node {
	centroid: _Vector,
	radius: f64,
	indices: Vec<_PointId>,
	left: Option<_NodeId>,
	right: Option<_NodeId>,
}

pub(crate) struct BallTreeIndex {
	_index_key_base: IndexKeyBase,
	doc_ids: Arc<RwLock<DocIds>>,
}

impl BallTreeIndex {
	pub(crate) async fn new(
		tx: &mut Transaction,
		ikb: IndexKeyBase,
		_dimension: u16,
		_bucket_size: u16,
		docids_order: u32,
		store_type: BTreeStoreType,
	) -> Result<Self, Error> {
		let doc_ids =
			Arc::new(RwLock::new(DocIds::new(tx, ikb.clone(), docids_order, store_type).await?));
		Ok(Self {
			_index_key_base: ikb,
			doc_ids,
		})
	}

	pub(crate) async fn index_document(
		&mut self,
		_tx: &mut Transaction,
		_rid: &Thing,
		_content: &Array,
	) -> Result<(), Error> {
		todo!()
	}

	pub(crate) async fn remove_document(
		&mut self,
		_tx: &mut Transaction,
		_rid: &Thing,
	) -> Result<(), Error> {
		todo!()
	}

	pub(crate) async fn finish(self, tx: &mut Transaction) -> Result<(), Error> {
		self.doc_ids.write().await.finish(tx).await?;
		Ok(())
	}
}
