use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::idx::trees::btree::BStatistics;
use crate::idx::trees::store::TreeStoreType;
use crate::idx::vector::Vector;
use crate::idx::{IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction};
use crate::sql::index::{MTreeParams, VectorType};
use crate::sql::{Object, Thing, Value};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) struct MTreeIndex {
	vt: VectorType,
	dim: usize,
	doc_ids: Arc<RwLock<DocIds>>,
	mtree: Arc<RwLock<MTree>>,
}

impl MTreeIndex {
	pub(crate) async fn new(
		tx: &mut Transaction,
		ikb: IndexKeyBase,
		p: &MTreeParams,
		st: TreeStoreType,
	) -> Result<Self, Error> {
		let doc_ids =
			Arc::new(RwLock::new(DocIds::new(tx, ikb.clone(), p.doc_ids_order, st).await?));
		let state_key = ikb.new_vm_key(None);
		let state: MState = if let Some(val) = tx.get(state_key).await? {
			MState::try_from_val(val)?
		} else {
			MState::new(p.capacity)
		};

		let mtree = Arc::new(RwLock::new(MTree::new(state)));
		Ok(Self {
			vt: p.vector_type.clone(),
			dim: p.dimension as usize,
			doc_ids,
			mtree,
		})
	}

	pub(crate) async fn index_document(
		&mut self,
		tx: &mut Transaction,
		rid: &Thing,
		content: &[Value],
	) -> Result<(), Error> {
		// Resolve the doc_id
		let resolved = self.doc_ids.write().await.resolve_doc_id(tx, rid.into()).await?;
		let doc_id = *resolved.doc_id();

		for v in content {
			// Extract the point
			let point = Vector::new(v, &self.vt, self.dim)?;
			self.mtree.write().await.insert(point, doc_id)?;
		}
		Ok(())
	}

	pub(crate) async fn remove_document(
		&mut self,
		tx: &mut Transaction,
		rid: &Thing,
	) -> Result<(), Error> {
		if let Some(_doc_id) = self.doc_ids.write().await.remove_doc(tx, rid.into()).await? {
			todo!()
		}
		Ok(())
	}

	pub(in crate::idx) fn doc_ids(&self) -> Arc<RwLock<DocIds>> {
		self.doc_ids.clone()
	}

	pub(crate) async fn statistics(&self, tx: &mut Transaction) -> Result<MtStatistics, Error> {
		Ok(MtStatistics {
			doc_ids: self.doc_ids.read().await.statistics(tx).await?,
		})
	}

	pub(crate) async fn finish(self, tx: &mut Transaction) -> Result<(), Error> {
		self.doc_ids.write().await.finish(tx).await
	}
}

// https://en.wikipedia.org/wiki/M-tree
struct MTree {
	_state: MState,
	_updated: bool,
}

impl MTree {
	fn new(state: MState) -> Self {
		Self {
			_state: state,
			_updated: false,
		}
	}

	fn insert(&mut self, _vec: Vector, _doc_id: DocId) -> Result<(), Error> {
		todo!()
	}
}

pub(crate) type NodeId = u64;

#[derive(Serialize, Deserialize)]
pub(in crate::idx) enum MTreeNode {
	Internal(Vec<MtRouting>),
	Leaf(Vec<MtObject>),
}

impl SerdeState for MTreeNode {}

impl MTreeNode {
	async fn _read(tx: &mut Transaction, key: Key) -> Result<(Self, u32), Error> {
		if let Some(val) = tx.get(key).await? {
			let size = val.len() as u32;
			Ok((Self::try_from_val(val)?, size))
		} else {
			Err(Error::CorruptedIndex)
		}
	}

	pub(crate) async fn _write(&self, tx: &mut Transaction, key: Key) -> Result<u32, Error> {
		let val = self.try_to_val()?;
		let size = val.len();
		tx.set(key, val).await?;
		Ok(size as u32)
	}
}

pub(crate) struct MtStatistics {
	doc_ids: BStatistics,
}

impl From<MtStatistics> for Value {
	fn from(stats: MtStatistics) -> Self {
		let mut res = Object::default();
		res.insert("doc_ids".to_owned(), Value::from(stats.doc_ids));
		Value::from(res)
	}
}

#[derive(Clone, Serialize, Deserialize)]
struct MState {
	capacity: u16,
	root: Option<NodeId>,
	next_node_id: NodeId,
}

impl MState {
	pub fn new(capacity: u16) -> Self {
		assert!(capacity >= 2, "Capacity should be >= 2");
		Self {
			capacity,
			root: None,
			next_node_id: 0,
		}
	}
}

#[derive(Serialize, Deserialize)]
pub(in crate::idx) struct MtRouting {
	// Feature value
	value: Vector,
	// Covering radius
	radius: f64,
	// Distance to its parent object
	dist: f64,
}

#[derive(Serialize, Deserialize)]
pub(in crate::idx) struct MtObject {
	// Feature value
	value: Vector,
	// Distance to its parent object
	dist: f64,
	// The documents with this vector
	docs: RoaringTreemap,
}

impl SerdeState for MState {}
