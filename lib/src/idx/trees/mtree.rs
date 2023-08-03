use crate::err::Error;
use crate::fnc::util::math::vector::{
	CosineSimilarity, EuclideanDistance, HammingDistance, ManhattanDistance, MinkowskiDistance,
};
use crate::idx::docids::{DocId, DocIds};
use crate::idx::trees::btree::BStatistics;
use crate::idx::trees::store::{TreeNode, TreeNodeProvider, TreeNodeStore, TreeStoreType};
use crate::idx::{IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction, Val};
use crate::sql::index::{Distance, MTreeParams};
use crate::sql::{Number, Object, Thing, Value};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

type MTreeNodeStore = TreeNodeStore<MTreeNode>;
type Vector = Vec<Number>;

pub(crate) struct MTreeIndex {
	state_key: Key,
	dim: usize,
	doc_ids: Arc<RwLock<DocIds>>,
	mtree: Arc<RwLock<MTree>>,
	store: Arc<Mutex<MTreeNodeStore>>,
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
		let state: MState = if let Some(val) = tx.get(state_key.clone()).await? {
			MState::try_from_val(val)?
		} else {
			MState::new(p.capacity)
		};

		let store = TreeNodeStore::new(TreeNodeProvider::Vector(ikb), st, 20);
		let mtree = Arc::new(RwLock::new(MTree::new(state, p.distance.clone())));
		Ok(Self {
			state_key,
			dim: p.dimension as usize,
			doc_ids,
			mtree,
			store,
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
		// Index the values
		let mut store = self.store.lock().await;
		for v in content {
			// Extract the vector
			let vector = self.check_vector(v)?;
			self.mtree.write().await.insert(tx, &mut store, vector, doc_id).await?;
		}
		Ok(())
	}

	fn check_vector(&self, v: &Value) -> Result<Vector, Error> {
		if let Value::Array(a) = v {
			if a.0.len() != self.dim {
				return Err(Error::InvalidVectorDimension {
					current: a.0.len(),
					expected: self.dim,
				});
			}
			let mut vec = Vec::with_capacity(a.len());
			for v in &a.0 {
				if let Value::Number(n) = v {
					vec.push(n.clone());
				} else {
					return Err(Error::InvalidVectorType {
						current: v.clone().to_string(),
						expected: "Number",
					});
				}
			}
			Ok(vec)
		} else {
			Err(Error::InvalidVectorValue {
				current: v.clone().to_raw_string(),
			})
		}
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
		self.doc_ids.write().await.finish(tx).await?;
		self.store.lock().await.finish(tx).await?;
		self.mtree.write().await.finish(tx, self.state_key).await?;
		Ok(())
	}
}

// https://en.wikipedia.org/wiki/M-tree
struct MTree {
	state: MState,
	distance: Distance,
}

impl MTree {
	fn new(state: MState, distance: Distance) -> Self {
		Self {
			state,
			distance,
		}
	}

	fn new_node_id(&mut self) -> NodeId {
		let new_node_id = self.state.next_node_id;
		self.state.next_node_id += 1;
		new_node_id
	}

	async fn insert(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		v: Vec<Number>,
		id: DocId,
	) -> Result<(), Error> {
		if let Some(root_id) = self.state.root {
			self.insert_node(tx, store, root_id, None, v, id).await?;
		} else {
			let new_root_id = self.new_node_id();
			let new_leaf_root = MTreeNode::new_leaf_root(v, id);
			let new_root_node = store.new_node(new_root_id, new_leaf_root)?;
			store.set_node(new_root_node, true)?;
			self.state.root = Some(new_root_id);
		}
		Ok(())
	}

	async fn insert_node(
		&self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		center: Option<&Vector>,
		v: Vector,
		id: DocId,
	) -> Result<(), Error> {
		let mut next_node = Some((node_id, center));
		while let Some((node_id, parent_center)) = next_node.take() {
			let mut node = store.get_node(tx, node_id).await?;
			match &mut node.n {
				MTreeNode::Routing(_routings) => {
					todo!()
				}
				MTreeNode::Leaf(objects) => {
					if self.insert_node_leaf(objects, v, parent_center, id) {
						return self.split_node(store, node_id, objects);
					}
					store.set_node(node, true)?;
					return Ok(());
				}
			};
		}
		Ok(())
	}

	fn insert_node_leaf(
		&self,
		objects: &mut HashMap<Vector, MObjectProperties>,
		v: Vector,
		parent_center: Option<&Vector>,
		id: DocId,
	) -> bool {
		match objects.entry(v) {
			Entry::Occupied(mut e) => {
				e.get_mut().docs.insert(id);
				false
			}
			Entry::Vacant(e) => {
				let d = self.distance(parent_center, e.key());
				e.insert(MObjectProperties::new(d, id));
				objects.len() > self.state.capacity as usize
			}
		}
	}

	fn split_node(
		&self,
		_store: &mut MTreeNodeStore,
		_node_id: NodeId,
		objects: &mut HashMap<Vector, MObjectProperties>,
	) -> Result<(), Error> {
		let (_p1, _p2) = self.select_promotion_objects(objects)?;
		todo!()
	}

	fn select_promotion_objects<'a>(
		&self,
		objects: &'a mut HashMap<Vector, MObjectProperties>,
	) -> Result<(&'a Vector, &'a Vector), Error> {
		let mut max_distance = 0.0;
		let mut promo = None;
		// Compare each pair of objects
		for (i1, vec1) in objects.keys().enumerate() {
			for (i2, vec2) in objects.keys().enumerate() {
				if i1 != i2 {
					let distance = self.distance(Some(vec1), vec2);
					// If this pair is further apart than the current maximum, update the promotion objects
					if distance > max_distance {
						promo = Some((vec1, vec2));
						max_distance = distance;
					}
				}
			}
		}
		promo.ok_or(Error::CorruptedIndex)
	}

	fn distance(&self, parent_center: Option<&Vector>, v: &Vector) -> f64 {
		if let Some(c) = parent_center {
			match &self.distance {
				Distance::Euclidean => c.euclidean_distance(v).unwrap().as_float(),
				Distance::Manhattan => c.manhattan_distance(v).unwrap().as_float(),
				Distance::Cosine => c.cosine_similarity(v).unwrap().as_float(),
				Distance::Hamming => c.hamming_distance(v).unwrap().as_float(),
				Distance::Mahalanobis => c.manhattan_distance(v).unwrap().as_float(),
				Distance::Minkowski(order) => c.minkowski_distance(v, order).unwrap().as_float(),
			}
		} else {
			0.0
		}
	}

	async fn finish(&self, tx: &mut Transaction, key: Key) -> Result<(), Error> {
		if self.state.updated {
			tx.set(key, self.state.try_to_val()?).await?;
		}
		Ok(())
	}
}

pub(crate) type NodeId = u64;

pub(in crate::idx) enum MTreeNode {
	Routing(Vec<MRoutingProperties>),
	Leaf(HashMap<Vector, MObjectProperties>),
}

impl MTreeNode {
	fn new_leaf_root(v: Vector, id: DocId) -> Self {
		let p = MObjectProperties::new_root(id);
		let mut o = HashMap::with_capacity(1);
		o.insert(v, p);
		Self::Leaf(o)
	}
}
impl TreeNode for MTreeNode {
	fn try_from_val(val: Val) -> Result<Self, Error> {
		let mut c: Cursor<Vec<u8>> = Cursor::new(val);
		let node_type: u8 = bincode::deserialize_from(&mut c)?;
		match node_type {
			1u8 => {
				let objects: HashMap<Vector, MObjectProperties> = bincode::deserialize_from(c)?;
				Ok(MTreeNode::Leaf(objects))
			}
			2u8 => {
				let routings: Vec<MRoutingProperties> = bincode::deserialize_from(c)?;
				Ok(MTreeNode::Routing(routings))
			}
			_ => Err(Error::CorruptedIndex),
		}
	}

	fn try_into_val(&mut self) -> Result<Val, Error> {
		let mut c: Cursor<Vec<u8>> = Cursor::new(Vec::new());
		match self {
			MTreeNode::Leaf(objects) => {
				bincode::serialize_into(&mut c, &1u8)?;
				bincode::serialize_into(&mut c, objects)?;
			}
			MTreeNode::Routing(routings) => {
				bincode::serialize_into(&mut c, &2u8)?;
				bincode::serialize_into(&mut c, routings)?;
			}
		};
		Ok(c.into_inner())
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
	#[serde(skip)]
	updated: bool,
}

impl MState {
	pub fn new(capacity: u16) -> Self {
		assert!(capacity >= 2, "Capacity should be >= 2");
		Self {
			capacity,
			root: None,
			next_node_id: 0,
			updated: false,
		}
	}
}

#[derive(Serialize, Deserialize)]
pub(in crate::idx) struct MRoutingProperties {
	center: Vector,
	// Covering radius
	radius: f64,
	// Distance to its parent object
	parent_dist: f64,
}

#[derive(Serialize, Deserialize)]
pub(in crate::idx) struct MObjectProperties {
	// Distance to its parent object
	parent_dist: f64,
	// The documents pointing to this vector
	docs: RoaringTreemap,
}

impl MObjectProperties {
	fn new(parent_dist: f64, id: DocId) -> Self {
		let mut docs = RoaringTreemap::new();
		docs.insert(id);
		Self {
			parent_dist,
			docs,
		}
	}

	fn new_root(id: DocId) -> Self {
		Self::new(0.0, id)
	}
}

impl SerdeState for MState {}

#[cfg(test)]
mod tests {
	use crate::err::Error;
	use crate::idx::docids::DocId;
	use crate::idx::trees::mtree::{MState, MTree, Vector};
	use crate::idx::trees::store::{TreeNodeProvider, TreeNodeStore, TreeStoreType};
	use crate::kvs::Datastore;
	use crate::sql::index::Distance;
	use test_log::test;

	#[test(tokio::test)]
	async fn test_mtree_insertions() -> Result<(), Error> {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
		let mut s = s.lock().await;
		let mut t = MTree::new(MState::new(3), Distance::Euclidean);
		let ds = Datastore::new("memory").await?;
		let mut tx = ds.transaction(true, false).await?;

		let samples: Vec<(DocId, Vector)> = vec![
			(1, vec![1.into(), 2.into(), 3.into(), 4.into()]),
			(1, vec![5.into(), 6.into(), 7.into(), 8.into()]),
			(1, vec![9.into(), 10.into(), 11.into(), 12.into()]),
			(1, vec![(-1).into(), (-2).into(), (-3).into(), (-4).into()]),
			(1, vec![0.1.into(), 0.2.into(), 0.3.into(), 0.4.into()]),
		];
		for (id, vec) in samples {
			t.insert(&mut tx, &mut s, vec, id).await?;
		}
		Ok(())
	}
}
