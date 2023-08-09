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
use std::cmp::Ordering;
use std::collections::BinaryHeap;
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

	async fn knn_search(
		&self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		v: &Vector,
		k: usize,
	) -> Result<(Vec<(Arc<Vector>, f64)>, RoaringTreemap), Error> {
		let mut queue = BinaryHeap::new();
		let mut results = Vec::with_capacity(k);
		if let Some(root_id) = self.state.root {
			queue.push(PriorityNode(root_id, 0.0));
		}
		let mut max_dist = f64::INFINITY;
		while let Some(current) = queue.pop() {
			let node = store.get_node(tx, current.0).await?;
			match node.n {
				MTreeNode::Leaf(ref indexmap) => {
					for (o, p) in indexmap {
						let d = self.calculate_distance(o.as_ref(), v);
						if max_dist == f64::INFINITY || d > max_dist {
							max_dist = d;
						}
						results.push((o.clone(), d, p.docs.clone()));
						results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
						if results.len() > k {
							results.pop();
						}
					}
				}
				MTreeNode::Routing(ref entries) => {
					for entry in entries {
						let d = self.calculate_distance(entry.center.as_ref(), v);
						let min_dist = (d - entry.radius).abs();
						if results.len() < k || min_dist < max_dist {
							queue.push(PriorityNode(entry.node, min_dist));
						}
					}
				}
			}
			store.set_node(node, false)?;
		}
		let mut global_docs = RoaringTreemap::new();
		let results = results
			.into_iter()
			.map(|(v, d, docs)| {
				global_docs |= docs;
				(v, d)
			})
			.collect();
		Ok((results, global_docs))
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
			self.insert_from_root(tx, store, root_id, Arc::new(v), id).await?;
		} else {
			self.create_new_root(store, v, id)?;
		}
		Ok(())
	}

	fn create_new_root(
		&mut self,
		store: &mut MTreeNodeStore,
		v: Vec<Number>,
		id: DocId,
	) -> Result<(), Error> {
		let new_root_id = self.new_node_id();
		let new_leaf_root = MTreeNode::new_leaf_root(v, id);
		let new_root_node = store.new_node(new_root_id, new_leaf_root)?;
		store.set_node(new_root_node, true)?;
		self.state.root = Some(new_root_id);
		Ok(())
	}

	async fn insert_from_root(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		root_id: NodeId,
		v: Arc<Vector>,
		id: DocId,
	) -> Result<(), Error> {
		let mut recompute_radius = vec![];
		let mut next_node = Some((root_id, None, v, id));
		while let Some((node_id, parent_center, v, id)) = next_node.take() {
			let mut node = store.get_node(tx, node_id).await?;
			match &mut node.n {
				MTreeNode::Routing(entries) => {
					let idx = self.find_closest(entries, &v)?;
					let r = &entries[idx];
					next_node = Some((r.node, Some(r.center.clone()), v, id));
					// The radius of the routing node will be recalculated
					recompute_radius.push((node, idx));
				}
				MTreeNode::Leaf(objects) => {
					if self.insert_node_leaf(objects, v, parent_center, id) {
						// The node need to be split
						self.split_node(store, node_id, node.key, objects)?;
					} else {
						store.set_node(node, true)?;
					}
				}
			};
		}
		// Recompute radiuses
		while let Some((mut node, idx)) = recompute_radius.pop() {
			if let MTreeNode::Routing(entries) = &mut node.n {
				let r = &mut entries[idx];
				let child = store.get_node(tx, r.node).await?;
				let mut update = false;
				let new_radius = match &child.n {
					MTreeNode::Routing(entries) => {
						self.compute_routing_radius(r.center.as_ref(), entries)
					}
					MTreeNode::Leaf(indexmap) => {
						self.compute_leaf_radius(r.center.as_ref(), indexmap)
					}
				};
				store.set_node(child, false)?;
				if new_radius != r.radius {
					r.radius = new_radius;
					update = true;
				}
				store.set_node(node, update)?;
			}
		}

		Ok(())
	}

	fn compute_routing_radius(&self, center: &Vector, entries: &Vec<MRoutingProperties>) -> f64 {
		let mut max_radius = f64::MIN;
		for e in entries {
			let radius = self.calculate_distance(e.center.as_ref(), center.as_ref()) + e.radius;
			if radius > max_radius {
				max_radius = radius;
			}
		}
		max_radius
	}

	fn compute_leaf_radius(
		&self,
		center: &Vector,
		indexmap: &IndexMap<Arc<Vector>, MObjectProperties>,
	) -> f64 {
		let mut max_radius = f64::MIN;
		for v in indexmap.keys() {
			let radius = self.calculate_distance(v, center);
			if radius > max_radius {
				max_radius = radius;
			}
		}
		max_radius
	}

	fn find_closest(
		&self,
		entries: &Vec<MRoutingProperties>,
		vec: &Vector,
	) -> Result<usize, Error> {
		let res = entries.iter().enumerate().min_by(|&(_, a), &(_, b)| {
			let distance_a = self.calculate_distance(&a.center, &vec);
			let distance_b = self.calculate_distance(&b.center, &vec);
			distance_a.partial_cmp(&distance_b).unwrap()
		});
		let (idx, _) = res.ok_or(Error::Unreachable)?;
		Ok(idx)
	}

	fn insert_node_leaf(
		&self,
		objects: &mut IndexMap<Arc<Vector>, MObjectProperties>,
		v: Arc<Vector>,
		parent_center: Option<Arc<Vector>>,
		id: DocId,
	) -> bool {
		match objects.entry(v) {
			Entry::Occupied(mut e) => {
				e.get_mut().docs.insert(id);
				false
			}
			Entry::Vacant(e) => {
				let d =
					parent_center.map_or(0f64, |v| self.calculate_distance(v.as_ref(), e.key()));
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
		objects: &mut IndexMap<Arc<Vector>, MObjectProperties>,
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

		// Distribute entries, update parent_dist and calculate radius
		for (i, (v, mut p)) in objects.drain(..).enumerate() {
			let dist_p1 = distances[i][p1];
			let dist_p2 = distances[i][p2];
			if dist_p1 <= dist_p2 {
				p.parent_dist = dist_p1;
				leaf1.insert(v, p);
				if dist_p1 > r1 {
					r1 = dist_p1;
				}
			} else {
				p.parent_dist = dist_p2;
				leaf2.insert(v, p);
				if dist_p2 > r2 {
					r2 = dist_p2;
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
		objects: &IndexMap<Arc<Vector>, MObjectProperties>,
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

#[derive(PartialEq)]
struct PriorityNode(NodeId, f64);

impl Ord for PriorityNode {
	fn cmp(&self, other: &Self) -> Ordering {
		other.1.partial_cmp(&self.1).unwrap()
	}
}

impl PartialOrd for PriorityNode {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Eq for PriorityNode {}

pub(in crate::idx) enum MTreeNode {
	Routing(Vec<MRoutingProperties>),
	Leaf(IndexMap<Arc<Vector>, MObjectProperties>),
}

impl MTreeNode {
	fn new_leaf_root(v: Vector, id: DocId) -> Self {
		let p = MObjectProperties::new_root(id);
		let mut o = IndexMap::with_capacity(1);
		o.insert(Arc::new(v), p);
		Self::Leaf(o)
	}
}
impl TreeNode for MTreeNode {
	fn try_from_val(val: Val) -> Result<Self, Error> {
		let mut c: Cursor<Vec<u8>> = Cursor::new(val);
		let node_type: u8 = bincode::deserialize_from(&mut c)?;
		match node_type {
			1u8 => {
				let objects: IndexMap<Arc<Vector>, MObjectProperties> =
					bincode::deserialize_from(c)?;
				Ok(MTreeNode::Leaf(objects))
			}
			2u8 => {
				let entries: Vec<MRoutingProperties> = bincode::deserialize_from(c)?;
				Ok(MTreeNode::Routing(entries))
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
			MTreeNode::Routing(entries) => {
				bincode::serialize_into(&mut c, &2u8)?;
				bincode::serialize_into(&mut c, entries)?;
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
	center: Arc<Vector>,
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
	use roaring::RoaringTreemap;
	use std::sync::Arc;
	use test_log::test;

	#[test(tokio::test)]
	async fn test_mtree_insertions() {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
		let mut s = s.lock().await;
		let mut t = MTree::new(MState::new(3), Distance::Euclidean);
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();

		let vec1 = vec![1.into()];
		// First the index is empty
		{
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res, vec![]);
			check_docs(&docs, vec![]);
		}
		// Insert single element
		{
			t.insert(&mut tx, &mut s, vec1.clone(), 1).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 1);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
			})
			.await;
		}
		// Check KNN
		{
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res, vec![(&vec1, 0.0)]);
			check_docs(&docs, vec![1]);
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
		// vec1 knn
		{
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res, vec![(&vec1, 0.0), (&vec2, 1.0)]);
			check_docs(&docs, vec![1, 2]);
		}
		// vec2 knn
		{
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec2, 10).await.unwrap();
			check_knn(&res, vec![(&vec2, 0.0), (&vec1, 1.0)]);
			check_docs(&docs, vec![1, 2]);
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
		// vec2 knn
		{
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec2, 10).await.unwrap();
			check_knn(&res, vec![(&vec2, 0.0), (&vec1, 1.0)]);
			check_docs(&docs, vec![1, 2, 3]);
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
		// vec3 knn
		{
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec3, 10).await.unwrap();
			check_knn(&res, vec![(&vec3, 0.0), (&vec2, 1.0), (&vec1, 2.0)]);
			check_docs(&docs, vec![1, 2, 3]);
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
				check_leaf_vec(m, 1, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec3, 1.0, &[3]);
				check_leaf_vec(m, 1, &vec4, 0.0, &[4]);
			})
			.await;
		}
		// vec4 knn
		{
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec4, 10).await.unwrap();
			check_knn(&res, vec![(&vec4, 0.0), (&vec3, 1.0), (&vec2, 2.0), (&vec1, 3.0)]);
			check_docs(&docs, vec![1, 2, 3, 4]);
		}

		// Insert vec extending the radius of the last node, calling compute_leaf_radius
		let vec6 = vec![6.into()];
		{
			t.insert(&mut tx, &mut s, vec6.clone(), 6).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_routing(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, 0, &vec1, 1, 1.0);
				check_routing_vec(m, 1, &vec4, 2, 2.0);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, 0, &vec3, 1.0, &[3]);
				check_leaf_vec(m, 1, &vec4, 0.0, &[4]);
				check_leaf_vec(m, 2, &vec6, 2.0, &[6]);
			})
			.await;
		}
		// vec6 knn
		{
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec6, 10).await.unwrap();
			check_knn(
				&res,
				vec![(&vec6, 0.0), (&vec4, 2.0), (&vec3, 3.0), (&vec2, 4.0), (&vec1, 5.0)],
			);
			check_docs(&docs, vec![1, 2, 3, 4, 6]);
		}

		// Insert vec extending the radius of the last node, calling compute_routing_radius
		let vec8 = vec![8.into()];
		{
			t.insert(&mut tx, &mut s, vec8.clone(), 8).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_routing(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, 0, &vec1, 1, 1.0);
				check_routing_vec(m, 1, &vec4, 2, 6.0);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_routing(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, 0, &vec3, 3, 1.0);
				check_routing_vec(m, 1, &vec8, 4, 2.0);
			})
			.await;
			check_leaf(&mut tx, &mut s, 3, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec3, 0.0, &[3]);
				check_leaf_vec(m, 1, &vec4, 1.0, &[4]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 4, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec6, 2.0, &[6]);
				check_leaf_vec(m, 1, &vec8, 0.0, &[8]);
			})
			.await;
		}
		// vec8 knn
		{
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec8, 10).await.unwrap();
			check_knn(
				&res,
				vec![
					(&vec8, 0.0),
					(&vec6, 2.0),
					(&vec4, 4.0),
					(&vec3, 5.0),
					(&vec2, 6.0),
					(&vec1, 7.0),
				],
			);
			check_docs(&docs, vec![1, 2, 3, 4, 6, 8]);
		}
		// vec4 knn(2)
		{
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec4, 2).await.unwrap();
			check_knn(&res, vec![(&vec4, 0.0), (&vec3, 1.0)]);
			check_docs(&docs, vec![3, 4]);
		}
	}

	fn check_leaf_vec(
		m: &IndexMap<Arc<Vector>, MObjectProperties>,
		idx: usize,
		vec: &Vector,
		parent_dist: f64,
		docs: &[DocId],
	) {
		let (v, p) = m.get_index(idx).unwrap();
		assert_eq!(v.as_ref(), vec);
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
		assert_eq!(center, p.center.as_ref());
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
		F: FnOnce(&IndexMap<Arc<Vector>, MObjectProperties>),
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

	fn check_knn(res: &Vec<(Arc<Vector>, f64)>, expected: Vec<(&Vector, f64)>) {
		assert_eq!(res.len(), expected.len());
		for (i, (a, b)) in res.iter().zip(expected.iter()).enumerate() {
			assert_eq!(a.0.as_ref(), b.0, "{}", i);
			assert_eq!(a.1, b.1, "{}", i);
		}
	}

	fn check_docs(docs: &RoaringTreemap, expected: Vec<DocId>) {
		assert_eq!(docs.len() as usize, expected.len());
		for id in expected {
			assert!(docs.contains(id), "{}", id);
		}
	}
}
