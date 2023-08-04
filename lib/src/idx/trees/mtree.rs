use crate::err::Error;
use crate::fnc::util::math::vector::{
	CosineSimilarity, EuclideanDistance, HammingDistance, ManhattanDistance, MinkowskiDistance,
};
use crate::idx::docids::{DocId, DocIds};
use crate::idx::trees::btree::BStatistics;
use crate::idx::trees::store::{
	NodeId, StoredNode, TreeNode, TreeNodeProvider, TreeNodeStore, TreeStoreType,
};
use crate::idx::{IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction, Val};
use crate::sql::index::{Distance, MTreeParams};
use crate::sql::{Number, Object, Thing, Value};
use indexmap::map::Entry;
use indexmap::IndexMap;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
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
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		center: Option<Vector>,
		v: Vector,
		id: DocId,
	) -> Result<(), Error> {
		let mut next_node = Some((node_id, center));
		while let Some((node_id, parent_center)) = next_node.take() {
			let mut node = store.get_node(tx, node_id).await?;
			match &mut node.n {
				MTreeNode::Routing(routings) => {
					let idx = self.find_closest(routings, &v)?;
					let r = &routings[idx];
					next_node = Some((r.node, Some(r.center.clone())));
					// Bring the node back to the cache
					store.set_node(node, false)?;
				}
				MTreeNode::Leaf(objects) => {
					if self.insert_node_leaf(objects, v, parent_center.as_ref(), id) {
						// The node need to be split
						self.split_node(store, node_id, node.key, objects)?;
						return Ok(());
					}
					store.set_node(node, true)?;
					return Ok(());
				}
			};
		}
		Ok(())
	}

	fn find_closest(
		&self,
		routings: &Vec<MRoutingProperties>,
		vec: &Vector,
	) -> Result<usize, Error> {
		let res = routings.iter().enumerate().min_by(|&(_, a), &(_, b)| {
			let distance_a = self.calculate_distance(&a.center, &vec);
			let distance_b = self.calculate_distance(&b.center, &vec);
			distance_a.partial_cmp(&distance_b).unwrap()
		});
		let (idx, _) = res.ok_or(Error::Unreachable)?;
		Ok(idx)
	}

	fn insert_node_leaf(
		&self,
		objects: &mut IndexMap<Vector, MObjectProperties>,
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
				let d = parent_center.map_or(0f64, |v| self.calculate_distance(v, e.key()));
				e.insert(MObjectProperties::new(d, id));
				objects.len() > self.state.capacity as usize
			}
		}
	}

	fn split_node(
		&mut self,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		objects: &mut IndexMap<Vector, MObjectProperties>,
	) -> Result<(), Error> {
		let distances = self.compute_distance_matrix(objects)?;
		let (p1, p2) = Self::select_promotion_objects(&distances);

		// Extract the promoted vectors
		let (promo1, _) = objects.get_index(p1).ok_or(Error::Unreachable)?;
		let (promo2, _) = objects.get_index(p2).ok_or(Error::Unreachable)?;
		let promo1 = promo1.clone();
		let promo2 = promo2.clone();

		let mut leaf1 = IndexMap::new();
		let mut leaf2 = IndexMap::new();

		let (mut r1, mut r2) = (0f64, 0f64);

		// Distribute entries and calculate radius
		for (i, (v, p)) in objects.drain(..).enumerate() {
			if distances[i][p1] <= distances[i][p2] {
				leaf1.insert(v, p);
				let d = distances[i][p1];
				if d > r1 {
					r1 = d;
				}
			} else {
				leaf2.insert(v, p);
				let d = distances[i][p2];
				if d > r2 {
					r2 = d;
				}
			}
		}

		// Store the new leaf nodes
		let n1 = self.new_node_id();
		let n2 = self.new_node_id();

		// Update the store/cache
		let n = store.new_node(n1, MTreeNode::Leaf(leaf1))?;
		store.set_node(n, true)?;
		let n = store.new_node(n2, MTreeNode::Leaf(leaf2))?;
		store.set_node(n, true)?;

		// Update the splitted node
		let r1 = MRoutingProperties {
			node: n1,
			center: promo1.clone(),
			radius: r1,
		};
		let r2 = MRoutingProperties {
			node: n2,
			center: promo2.clone(),
			radius: r2,
		};
		let node = StoredNode {
			n: MTreeNode::Routing(vec![r1, r2]),
			id: node_id,
			key: node_key,
			size: 0,
		};
		// Update the store/cache
		store.set_node(node, true)?;
		Ok(())
	}

	fn select_promotion_objects(distances: &Vec<Vec<f64>>) -> (usize, usize) {
		let mut promo = (0, 1);
		let mut max_distance = distances[0][1];
		// Compare each pair of objects
		let n = distances.len();
		for i in 0..n {
			for j in i + 1..n {
				let distance = distances[i][j];
				// If this pair is further apart than the current maximum, update the promotion objects
				if distance > max_distance {
					promo = (i, j);
					max_distance = distance;
				}
			}
		}
		promo
	}

	fn compute_distance_matrix(
		&self,
		objects: &IndexMap<Vector, MObjectProperties>,
	) -> Result<Vec<Vec<f64>>, Error> {
		let n = objects.len();
		let mut distances = vec![vec![0.0; n]; n];
		for i in 0..n {
			let (v1, _) = objects.get_index(i).ok_or(Error::Unreachable)?;
			for j in i + 1..n {
				let (v2, _) = objects.get_index(j).ok_or(Error::Unreachable)?;
				let distance = self.calculate_distance(v1, v2);
				distances[i][j] = distance;
				distances[j][i] = distance; // Because the distance function is symmetric
			}
		}
		Ok(distances)
	}

	fn calculate_distance(&self, v1: &Vector, v2: &Vector) -> f64 {
		match &self.distance {
			Distance::Euclidean => v1.euclidean_distance(v2).unwrap().as_float(),
			Distance::Manhattan => v1.manhattan_distance(v2).unwrap().as_float(),
			Distance::Cosine => v1.cosine_similarity(v2).unwrap().as_float(),
			Distance::Hamming => v1.hamming_distance(v2).unwrap().as_float(),
			Distance::Mahalanobis => v1.manhattan_distance(v2).unwrap().as_float(),
			Distance::Minkowski(order) => v1.minkowski_distance(v2, order).unwrap().as_float(),
		}
	}

	async fn finish(&self, tx: &mut Transaction, key: Key) -> Result<(), Error> {
		if self.state.updated {
			tx.set(key, self.state.try_to_val()?).await?;
		}
		Ok(())
	}
}

pub(in crate::idx) enum MTreeNode {
	Routing(Vec<MRoutingProperties>),
	Leaf(IndexMap<Vector, MObjectProperties>),
}

impl MTreeNode {
	fn new_leaf_root(v: Vector, id: DocId) -> Self {
		let p = MObjectProperties::new_root(id);
		let mut o = IndexMap::with_capacity(1);
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
				let objects: IndexMap<Vector, MObjectProperties> = bincode::deserialize_from(c)?;
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
	// Reference to the node
	node: NodeId,
	// Center of the node
	center: Vector,
	// Covering radius
	radius: f64,
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
	use crate::idx::docids::DocId;
	use crate::idx::trees::mtree::{
		MObjectProperties, MRoutingProperties, MState, MTree, MTreeNode, MTreeNodeStore, Vector,
	};
	use crate::idx::trees::store::{NodeId, TreeNodeProvider, TreeNodeStore, TreeStoreType};
	use crate::kvs::Datastore;
	use crate::kvs::Transaction;
	use crate::sql::index::Distance;
	use indexmap::IndexMap;
	use test_log::test;

	#[test(tokio::test)]
	async fn test_mtree_insertions() {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
		let mut s = s.lock().await;
		let mut t = MTree::new(MState::new(3), Distance::Euclidean);
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();

		// Insert single element
		let vec1 = vec![1.into()];
		{
			t.insert(&mut tx, &mut s, vec1.clone(), 1).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 1);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
			})
			.await;
		}

		// insert second element
		let vec2 = vec![2.into()];
		{
			t.insert(&mut tx, &mut s, vec2.clone(), 2).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 0.0, &[2]);
			})
			.await;
		}

		// insert new doc to existing vector
		{
			t.insert(&mut tx, &mut s, vec2.clone(), 3).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 0.0, &[2, 3]);
			})
			.await;
		}

		// insert third vector
		let vec3 = vec![3.into()];
		{
			t.insert(&mut tx, &mut s, vec3.clone(), 3).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 0.0, &[2, 3]);
				check_leaf_vec(m, 2, &vec3, 0.0, &[3]);
			})
			.await;
		}

		// Check split node
		let vec4 = vec![4.into()];
		{
			t.insert(&mut tx, &mut s, vec4.clone(), 4).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_routing(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, 0, &vec1, 1, 1.0);
				check_routing_vec(m, 1, &vec4, 2, 1.0);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 1.0, &[2, 3]); // Right???
			})
			.await;
			check_leaf(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec3, 1.0, &[3]);
				check_leaf_vec(m, 1, &vec4, 0.0, &[4]);
			})
			.await;
		}
	}

	fn check_leaf_vec(
		m: &IndexMap<Vector, MObjectProperties>,
		idx: usize,
		vec: &Vector,
		parent_dist: f64,
		docs: &[DocId],
	) {
		let (v, p) = m.get_index(idx).unwrap();
		assert_eq!(v, vec);
		assert_eq!(p.docs.len(), docs.len() as u64);
		for doc in docs {
			assert!(p.docs.contains(*doc));
		}
		assert_eq!(p.parent_dist, parent_dist);
	}

	fn check_routing_vec(
		m: &Vec<MRoutingProperties>,
		idx: usize,
		center: &Vector,
		node_id: NodeId,
		radius: f64,
	) {
		let p = &m[idx];
		assert_eq!(center, &p.center);
		assert_eq!(node_id, p.node);
		assert_eq!(radius, p.radius);
	}

	async fn check_node<F>(
		tx: &mut Transaction,
		s: &mut MTreeNodeStore,
		node_id: NodeId,
		check_func: F,
	) where
		F: FnOnce(&MTreeNode),
	{
		let n = s.get_node(tx, node_id).await.unwrap();
		check_func(&n.n);
		s.set_node(n, false).unwrap();
	}

	async fn check_leaf<F>(
		tx: &mut Transaction,
		s: &mut MTreeNodeStore,
		node_id: NodeId,
		check_func: F,
	) where
		F: FnOnce(&IndexMap<Vector, MObjectProperties>),
	{
		check_node(tx, s, node_id, |n| {
			if let MTreeNode::Leaf(m) = n {
				check_func(m);
			} else {
				panic!("The node is not a leaf node: {node_id}")
			}
		})
		.await
	}

	async fn check_routing<F>(
		tx: &mut Transaction,
		s: &mut MTreeNodeStore,
		node_id: NodeId,
		check_func: F,
	) where
		F: FnOnce(&Vec<MRoutingProperties>),
	{
		check_node(tx, s, node_id, |n| {
			if let MTreeNode::Routing(m) = n {
				check_func(m);
			} else {
				panic!("The node is not a routing node: {node_id}")
			}
		})
		.await
	}
}
