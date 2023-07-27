use crate::err::Error;
use crate::idx::docids::DocIds;
use crate::idx::vector::points::Points;
use crate::idx::vector::Vector;
use crate::idx::{IndexKeyBase, StoreType};
use crate::kvs::Transaction;
use crate::sql::index::VectorType;
use crate::sql::{Thing, Value};
use roaring::RoaringTreemap;
use std::sync::Arc;
use tokio::sync::RwLock;

type _NodeId = u64;
const _THRESHOLD: usize = 30;

struct _Node {
	centroid: Vector,
	radius: f64,
	indices: RoaringTreemap,
	left: Option<_NodeId>,
	right: Option<_NodeId>,
}

pub(crate) struct BallTreeIndex {
	vt: VectorType,
	dim: usize,
	doc_ids: Arc<RwLock<DocIds>>,
	points: Arc<RwLock<Points>>,
}

impl BallTreeIndex {
	pub(crate) async fn new(
		tx: &mut Transaction,
		ikb: IndexKeyBase,
		vt: VectorType,
		dim: u16,
		_bucket_size: u16,
		doc_ids_order: u32,
		st: StoreType,
	) -> Result<Self, Error> {
		let doc_ids = Arc::new(RwLock::new(DocIds::new(tx, ikb.clone(), doc_ids_order, st).await?));
		let points = Arc::new(RwLock::new(Points::new(ikb, vt.clone(), st)));
		Ok(Self {
			vt,
			dim: dim as usize,
			doc_ids,
			points,
		})
	}

	pub(crate) async fn index_document(
		&mut self,
		tx: &mut Transaction,
		rid: &Thing,
		content: &[Value],
	) -> Result<(), Error> {
		// Extract the point
		let point = Vector::new(content, &self.vt, self.dim)?;

		// Resolve the doc_id
		let resolved = self.doc_ids.write().await.resolve_doc_id(tx, rid.into()).await?;
		let doc_id = *resolved.doc_id();

		// Write the point
		self.points.write().await.set(doc_id, point).await?;
		Ok(())
	}

	pub(crate) async fn remove_document(
		&mut self,
		tx: &mut Transaction,
		rid: &Thing,
	) -> Result<(), Error> {
		if let Some(doc_id) = self.doc_ids.write().await.remove_doc(tx, rid.into()).await? {
			self.points.write().await.remove(doc_id).await?;
		}
		Ok(())
	}

	pub(crate) async fn finish(self, tx: &mut Transaction) -> Result<(), Error> {
		self.doc_ids.write().await.finish(tx).await?;
		self.points.write().await.finish(tx).await?;
		Ok(())
	}
}
