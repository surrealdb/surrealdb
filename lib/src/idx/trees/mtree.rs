use crate::err::Error;
use crate::fnc::util::math::vector::{
	CosineSimilarity, EuclideanDistance, HammingDistance, ManhattanDistance, MinkowskiDistance,
};
use crate::idx::docids::{DocId, DocIds};
use crate::idx::trees::btree::BStatistics;
use crate::idx::trees::store::{
	NodeId, StoredNode, TreeNode, TreeNodeProvider, TreeNodeStore, TreeStoreType,
};
use crate::idx::{IndexKeyBase, VersionedSerdeState};
use crate::kvs::{Key, Transaction, Val};
use crate::sql::index::{Distance, MTreeParams};
use crate::sql::{Array, Number, Object, Thing, Value};
use async_recursion::async_recursion;
use indexmap::map::Entry;
use indexmap::IndexMap;
use revision::revisioned;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub(crate) type Vector = Vec<Number>;

type MTreeNodeStore = TreeNodeStore<MTreeNode>;

type LeafIndexMap = IndexMap<Arc<Vector>, ObjectProperties>;

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
		content: Vec<Value>,
	) -> Result<(), Error> {
		// Resolve the doc_id
		let resolved = self.doc_ids.write().await.resolve_doc_id(tx, rid.into()).await?;
		let doc_id = *resolved.doc_id();
		// Index the values
		let mut store = self.store.lock().await;
		let mut mtree = self.mtree.write().await;
		for v in content {
			// Extract the vector
			let vector = self.check_vector_value(v)?;
			mtree.insert(tx, &mut store, vector, doc_id).await?;
		}
		Ok(())
	}

	pub(crate) async fn knn_search(
		&self,
		tx: &mut Transaction,
		a: Array,
		k: usize,
	) -> Result<VecDeque<RoaringTreemap>, Error> {
		// Extract the vector
		let vector = self.check_vector_array(a)?;
		// Lock the store
		let mut store = self.store.lock().await;
		let res = self.mtree.read().await.knn_search(tx, &mut store, &vector, k).await?;
		Ok(res.objects)
	}

	fn check_vector_array(&self, a: Array) -> Result<Vector, Error> {
		if a.0.len() != self.dim {
			return Err(Error::InvalidVectorDimension {
				current: a.0.len(),
				expected: self.dim,
			});
		}
		let mut vec = Vec::with_capacity(a.len());
		for v in a.0 {
			if let Value::Number(n) = v {
				vec.push(n);
			} else {
				return Err(Error::InvalidVectorType {
					current: v.clone().to_string(),
					expected: "Number",
				});
			}
		}
		Ok(vec)
	}

	fn check_vector_value(&self, v: Value) -> Result<Vector, Error> {
		if let Value::Array(a) = v {
			self.check_vector_array(a)
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
		content: Vec<Value>,
	) -> Result<(), Error> {
		if let Some(doc_id) = self.doc_ids.write().await.remove_doc(tx, rid.into()).await? {
			// Index the values
			let mut store = self.store.lock().await;
			let mut mtree = self.mtree.write().await;
			for v in content {
				// Extract the vector
				let vector = self.check_vector_value(v)?;
				mtree.delete(tx, &mut store, vector, doc_id).await?;
			}
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

struct KnnResult {
	objects: VecDeque<RoaringTreemap>,
	#[cfg(debug_assertions)]
	#[allow(dead_code)]
	visited_nodes: usize,
}

// https://en.wikipedia.org/wiki/M-tree
// https://arxiv.org/pdf/1004.4216.pdf
struct MTree {
	state: MState,
	distance: Distance,
	minimum: usize,
	updated: bool,
}

impl MTree {
	fn new(state: MState, distance: Distance) -> Self {
		let minimum = state.capacity as usize / 2;
		Self {
			state,
			distance,
			minimum,
			updated: false,
		}
	}

	async fn knn_search(
		&self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		v: &Vector,
		k: usize,
	) -> Result<KnnResult, Error> {
		let mut queue = BinaryHeap::new();
		let mut res = BTreeMap::new();
		if let Some(root_id) = self.state.root {
			queue.push(PriorityNode(0.0, root_id));
		}
		#[cfg(debug_assertions)]
		let mut visited_nodes = 0;
		while let Some(current) = queue.pop() {
			#[cfg(debug_assertions)]
			{
				visited_nodes += 1;
			}
			let node = store.get_node(tx, current.1).await?;
			match node.n {
				MTreeNode::Leaf(ref n) => {
					for (o, p) in n {
						let d = self.calculate_distance(o.as_ref(), v);
						if Self::check_add(k, d, &res) {
							res.insert(PriorityResult(d, o.clone()), p.docs.clone());
							if res.len() > k {
								res.pop_last();
							}
						}
					}
				}
				MTreeNode::Internal(ref n) => {
					for entry in n {
						let d = self.calculate_distance(entry.center.as_ref(), v);
						let min_dist = (d - entry.radius).max(0.0);
						if Self::check_add(k, min_dist, &res) {
							queue.push(PriorityNode(min_dist, entry.node));
						}
					}
				}
			}
			store.set_node(node, false)?;
		}
		let mut objects = VecDeque::with_capacity(res.len());
		for (_, d) in res {
			objects.push_back(d);
		}
		Ok(KnnResult {
			objects,
			#[cfg(debug_assertions)]
			visited_nodes,
		})
	}

	fn check_add(k: usize, dist: f64, res: &BTreeMap<PriorityResult, RoaringTreemap>) -> bool {
		if res.len() < k {
			true
		} else if let Some(l) = res.keys().last() {
			dist < l.0
		} else {
			true
		}
	}
}

enum InsertionResult {
	DocAdded,
	CoveringRadius(f64),
	PromotedEntries(RoutingEntry, RoutingEntry),
}

enum DeletionResult {
	NotFound,
	DocRemoved,
	CoveringRadius(f64),
	Underflown(NodeId, Key, MTreeNode),
}

// Insertion
impl MTree {
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
			let node = store.get_node(tx, root_id).await?;
			if let InsertionResult::PromotedEntries(r1, r2) =
				self.insert_at_node(tx, store, node, &None, Arc::new(v), id).await?
			{
				self.create_new_internal_root(store, r1, r2)?;
			}
		} else {
			self.create_new_leaf_root(store, v, id)?;
		}
		Ok(())
	}

	fn create_new_leaf_root(
		&mut self,
		store: &mut MTreeNodeStore,
		v: Vec<Number>,
		id: DocId,
	) -> Result<(), Error> {
		let new_root_id = self.new_node_id();
		let p = ObjectProperties::new_root(id);
		let mut objects = LeafIndexMap::with_capacity(1);
		objects.insert(Arc::new(v), p);
		let new_root_node = store.new_node(new_root_id, MTreeNode::Leaf(objects))?;
		store.set_node(new_root_node, true)?;
		self.set_root(Some(new_root_id));
		Ok(())
	}

	fn create_new_internal_root(
		&mut self,
		store: &mut MTreeNodeStore,
		r1: RoutingEntry,
		r2: RoutingEntry,
	) -> Result<(), Error> {
		let new_root_id = self.new_node_id();
		let new_root_node = store.new_node(new_root_id, MTreeNode::Internal(vec![r1, r2]))?;
		store.set_node(new_root_node, true)?;
		self.set_root(Some(new_root_id));
		Ok(())
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	async fn insert_at_node(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		node: StoredNode<MTreeNode>,
		parent_center: &Option<Arc<Vector>>,
		object: Arc<Vector>,
		id: DocId,
	) -> Result<InsertionResult, Error> {
		match node.n {
			// If (N is a leaf)
			MTreeNode::Leaf(n) => {
				self.insert_node_leaf(store, node.id, node.key, n, parent_center, object, id)
			}
			// Else
			MTreeNode::Internal(n) => {
				self.insert_node_internal(
					tx,
					store,
					node.id,
					node.key,
					n,
					parent_center,
					object,
					id,
				)
				.await
			}
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn insert_node_internal(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		mut node: InternalNode,
		parent_center: &Option<Arc<Vector>>,
		object: Arc<Vector>,
		id: DocId,
	) -> Result<InsertionResult, Error> {
		// Choose `best` substree entry ObestSubstree from N;
		let best_entry_idx = self.find_closest(&node, &object)?;
		let best_entry = &mut node[best_entry_idx];
		let best_node = store.get_node(tx, best_entry.node).await?;
		// Insert(Oi, child(ObestSubstree), ObestSubtree);
		match self
			.insert_at_node(tx, store, best_node, &Some(best_entry.center.clone()), object, id)
			.await?
		{
			// If (entry returned)
			InsertionResult::PromotedEntries(p1, p2) => {
				// Remove ObestSubstree from N;
				node.remove(best_entry_idx);
				// Let P be the set of returned entries
				node.push(p1);
				node.push(p2);
				// if (N U P will fit into N)
				if node.len() <= self.state.capacity as usize {
					let max_dist = self.compute_internal_max_distance(&node, parent_center);
					store.set_node(
						StoredNode::new(node.into_mtree_node(), node_id, node_key, 0),
						true,
					)?;
					return Ok(InsertionResult::CoveringRadius(max_dist));
				}
				self.split_node(store, node_id, node_key, node)
			}
			InsertionResult::DocAdded => {
				store.set_node(
					StoredNode::new(node.into_mtree_node(), node_id, node_key, 0),
					false,
				)?;
				Ok(InsertionResult::DocAdded)
			}
			InsertionResult::CoveringRadius(covering_radius) => {
				let mut updated = false;
				if covering_radius > best_entry.radius {
					best_entry.radius = covering_radius;
					updated = true;
				}
				let max_dist = self.compute_internal_max_distance(&node, parent_center);
				store.set_node(
					StoredNode::new(node.into_mtree_node(), node_id, node_key, 0),
					updated,
				)?;
				Ok(InsertionResult::CoveringRadius(max_dist))
			}
		}
	}

	fn find_closest(&self, node: &InternalNode, object: &Vector) -> Result<usize, Error> {
		let mut idx = 0;
		let dist = f64::MAX;
		for (i, e) in node.iter().enumerate() {
			let d = self.calculate_distance(e.center.as_ref(), object);
			if d < dist {
				idx = i;
			}
		}
		Ok(idx)
	}

	#[allow(clippy::too_many_arguments)]
	fn insert_node_leaf(
		&mut self,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		mut node: LeafNode,
		parent_center: &Option<Arc<Vector>>,
		object: Arc<Vector>,
		id: DocId,
	) -> Result<InsertionResult, Error> {
		match node.entry(object) {
			Entry::Occupied(mut e) => {
				e.get_mut().docs.insert(id);
				store.set_node(
					StoredNode::new(node.into_mtree_node(), node_id, node_key, 0),
					true,
				)?;
				return Ok(InsertionResult::DocAdded);
			}
			// Add Oi to N
			Entry::Vacant(e) => {
				// Let parentDistance(Oi) = d(Oi, parent(N))
				let parent_dist = parent_center
					.as_ref()
					.map_or(0f64, |v| self.calculate_distance(v.as_ref(), e.key()));
				e.insert(ObjectProperties::new(parent_dist, id));
			}
		};
		// If (N will fit into N)
		if node.len() <= self.state.capacity as usize {
			let max_dist = self.compute_leaf_max_distance(&node, parent_center);
			store.set_node(StoredNode::new(node.into_mtree_node(), node_id, node_key, 0), true)?;
			Ok(InsertionResult::CoveringRadius(max_dist))
		} else {
			// Else
			// Split (N)
			self.split_node(store, node_id, node_key, node)
		}
	}

	fn set_root(&mut self, new_root: Option<NodeId>) {
		#[cfg(debug_assertions)]
		debug!("SET_ROOT: {:?}", new_root);
		self.state.root = new_root;
		self.updated = true;
	}

	fn split_node<N>(
		&mut self,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		node: N,
	) -> Result<InsertionResult, Error>
	where
		N: NodeVectors,
	{
		let distances = self.compute_distance_matrix(&node)?;
		let (p1_idx, p2_idx) = Self::select_promotion_objects(&distances);
		let p1_obj = node.get_vector(p1_idx)?;
		let p2_obj = node.get_vector(p2_idx)?;

		// Distribute entries, update parent_dist and calculate radius
		let (node1, r1, node2, r2) = node.distribute_entries(&distances, p1_idx, p2_idx)?;

		// Create a new node
		let new_node = self.new_node_id();

		// Update the store/cache
		let n = StoredNode::new(node1.into_mtree_node(), node_id, node_key, 0);
		store.set_node(n, true)?;
		let n = store.new_node(new_node, node2.into_mtree_node())?;
		store.set_node(n, true)?;

		// Update the split node
		let r1 = RoutingEntry {
			node: node_id,
			center: p1_obj,
			radius: r1,
		};
		let r2 = RoutingEntry {
			node: new_node,
			center: p2_obj,
			radius: r2,
		};
		Ok(InsertionResult::PromotedEntries(r1, r2))
	}

	fn select_promotion_objects(distances: &[Vec<f64>]) -> (usize, usize) {
		let mut promo = (0, 1);
		let mut max_distance = distances[0][1];
		// Compare each pair of objects
		let n = distances.len();
		#[allow(clippy::needless_range_loop)]
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

	fn compute_internal_max_distance(
		&self,
		node: &InternalNode,
		parent: &Option<Arc<Vector>>,
	) -> f64 {
		parent.as_ref().map_or(0.0, |p| {
			let mut max_dist = 0f64;
			for e in node {
				max_dist = max_dist.max(self.calculate_distance(p.as_ref(), e.center.as_ref()));
			}
			max_dist
		})
	}

	fn compute_leaf_max_distance(&self, node: &LeafNode, parent: &Option<Arc<Vector>>) -> f64 {
		parent.as_ref().map_or(0.0, |p| {
			let mut max_dist = 0f64;
			for o in node.keys() {
				max_dist = max_dist.max(self.calculate_distance(p.as_ref(), o.as_ref()));
			}
			max_dist
		})
	}

	fn compute_distance_matrix<N>(&self, vectors: &N) -> Result<Vec<Vec<f64>>, Error>
	where
		N: NodeVectors,
	{
		let n = vectors.len();
		let mut distances = vec![vec![0.0; n]; n];
		for i in 0..n {
			let v1 = vectors.get_vector(i)?;
			for j in i + 1..n {
				let v2 = vectors.get_vector(j)?;
				let distance = self.calculate_distance(v1.as_ref(), v2.as_ref());
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

	async fn delete(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		object: Vector,
		doc_id: DocId,
	) -> Result<bool, Error> {
		if let Some(root_id) = self.state.root {
			let root_node = store.get_node(tx, root_id).await?;
			match self.delete_at_node(tx, store, root_node, &None, Arc::new(object), doc_id).await?
			{
				DeletionResult::DocRemoved => return Ok(true),
				DeletionResult::CoveringRadius(_) | DeletionResult::NotFound => return Ok(false),
				DeletionResult::Underflown(id, key, n) => {
					match &n {
						MTreeNode::Internal(n) => match n.len() {
							0 => {
								store.remove_node(id, key)?;
								self.set_root(None);
								return Ok(true);
							}
							1 => {
								store.remove_node(id, key)?;
								self.set_root(Some(n[0].node));
								return Ok(true);
							}
							_ => {}
						},
						MTreeNode::Leaf(n) => {
							if n.is_empty() {
								store.remove_node(id, key)?;
								self.set_root(None);
								return Ok(true);
							}
						}
					}
					let sn = StoredNode::new(n, id, key, 0);
					store.set_node(sn, true)?;
					return Ok(true);
				}
			}
		}
		Ok(false)
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	async fn delete_at_node(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		node: StoredNode<MTreeNode>,
		parent_center: &Option<Arc<Vector>>,
		object: Arc<Vector>,
		id: DocId,
	) -> Result<DeletionResult, Error> {
		#[cfg(debug_assertions)]
		debug!("delete_at_node: {} {:?}", node.id, object);
		// Delete ( Od:LeafEntry, N:Node)
		match node.n {
			// If (N is a leaf)
			MTreeNode::Leaf(n) => {
				self.delete_node_leaf(store, node.id, node.key, n, parent_center, object, id).await
			}
			// Else
			MTreeNode::Internal(n) => {
				self.delete_node_internal(
					tx,
					store,
					node.id,
					node.key,
					n,
					parent_center,
					object,
					id,
				)
				.await
			}
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn delete_node_internal(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		mut n_node: InternalNode,
		parent_center: &Option<Arc<Vector>>,
		od: Arc<Vector>,
		id: DocId,
	) -> Result<DeletionResult, Error> {
		#[cfg(debug_assertions)]
		debug!("delete_node_internal: {} {:?}", node_id, od);
		let mut on_idx = None;
		// For each On E N
		for (i, on_entry) in n_node.iter().enumerate() {
			let on_od_dist = self.calculate_distance(on_entry.center.as_ref(), od.as_ref());
			#[cfg(debug_assertions)]
			debug!(
				"on_od_dist: {:?} / {} / {}",
				on_entry.center.as_ref(),
				on_od_dist,
				on_entry.radius
			);
			// If (d(Od, On) <= r(On))
			if on_od_dist <= on_entry.radius {
				on_idx = Some(i);
				break;
			}
		}
		#[cfg(debug_assertions)]
		debug!("on_idx: {:?}", on_idx);
		if let Some(on_idx) = on_idx {
			// Delete (Od, child(On))
			let (on_center, on_node) = {
				let on_entry = &n_node[on_idx];
				let on_node = store.get_node(tx, on_entry.node).await?;
				(on_entry.center.clone(), on_node)
			};
			match self.delete_at_node(tx, store, on_node, &Some(on_center.clone()), od, id).await? {
				DeletionResult::NotFound => {}
				DeletionResult::DocRemoved => {
					Self::set_internal_node(store, node_id, node_key, n_node, false)?;
					return Ok(DeletionResult::DocRemoved);
				}
				// Let r = returned covering radius
				DeletionResult::CoveringRadius(r) => {
					let on_entry = &mut n_node[on_idx];
					let mut n_updated = false;
					// If (r > r(On))
					if r > on_entry.radius {
						// Let r(On) = r;
						on_entry.radius = r;
						n_updated = true;
					}
					return self.delete_node_internal_check_underflown(
						store,
						node_id,
						node_key,
						n_node,
						on_idx,
						parent_center,
						n_updated,
					);
				}
				DeletionResult::Underflown(p_node_id, p_node_key, p_node) => {
					let n_updated = self
						.deletion_underflown(
							tx,
							store,
							on_idx,
							&mut n_node,
							on_center.as_ref(),
							p_node_id,
							p_node_key,
							p_node,
						)
						.await?;
					return self.delete_node_internal_check_underflown(
						store,
						node_id,
						node_key,
						n_node,
						on_idx,
						parent_center,
						n_updated,
					);
				}
			}
		}
		Self::set_internal_node(store, node_id, node_key, n_node, false)?;
		Ok(DeletionResult::NotFound)
	}

	#[allow(clippy::too_many_arguments)]
	fn delete_node_internal_check_underflown(
		&mut self,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		n_node: InternalNode,
		on_idx: usize,
		parent_center: &Option<Arc<Vector>>,
		n_updated: bool,
	) -> Result<DeletionResult, Error> {
		// If (N is underflown)
		if n_node.len() < self.minimum {
			// Return N
			return Ok(DeletionResult::Underflown(node_id, node_key, MTreeNode::Internal(n_node)));
		}
		// Return max(On E N) { parentDistance(On) + r(On)}
		let max_dist =
			self.compute_internal_max_distance(&n_node, parent_center) + n_node[on_idx].radius;
		Self::set_internal_node(store, node_id, node_key, n_node, n_updated)?;
		Ok(DeletionResult::CoveringRadius(max_dist))
	}

	fn set_internal_node(
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		internal_node: InternalNode,
		updated: bool,
	) -> Result<(), Error> {
		let sn = StoredNode::new(MTreeNode::Internal(internal_node), node_id, node_key, 0);
		store.set_node(sn, updated)
	}

	#[allow(unused_variables, unused_assignments, clippy::too_many_arguments)]
	async fn deletion_underflown(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		on_idx: usize,
		n_node: &mut InternalNode,
		other_center: &Vector,
		p_id: NodeId,
		p_key: Key,
		p_node: MTreeNode,
	) -> Result<bool, Error> {
		let min = f64::NAN;
		let mut onn_idx = None;
		// Find node entry Onn € N, e <> 0, for which d(On, Onn) is a minimum
		for (i, e) in n_node.iter().enumerate() {
			if e.node != p_id {
				let d = self.calculate_distance(other_center, e.center.as_ref());
				if min.is_nan() || d < min {
					onn_idx = Some(i);
				}
			}
		}
		let mut n_updated = false;
		if let Some(onn_idx) = onn_idx {
			let onn_entry = &mut n_node[onn_idx];
			let onn_center = onn_entry.center.clone();
			// Let S be the set of entries in child(Onn()
			let mut onn_node = store.get_node(tx, onn_entry.node).await?;
			// If (S U P) will fit into child(Onn)
			if onn_node.n.len() + p_node.len() <= self.state.capacity as usize {
				// Remove On from N;
				n_node.remove(on_idx);
				n_updated = true;
				match &mut onn_node.n {
					MTreeNode::Internal(s) => {
						let p_node = p_node.internal()?;
						// for each Op E P
						for op in p_node {
							// Let parentDistance(Op) = d(Op, Onn);
							let parent_dist =
								self.calculate_distance(op.center.as_ref(), onn_center.as_ref());
							// Add Op to S;
							s.push(op);
						}
						//TODO
						return Err(Error::FeatureNotYetImplemented {
							feature: "MTREE deletions (underflow internal)".to_string(),
						});
					}
					MTreeNode::Leaf(s) => {
						let p_node = p_node.leaf()?;
						// for each Op E P
						for (op, mut p) in p_node {
							// Let parentDistance(Op) = d(Op, Onn);
							p.parent_dist =
								self.calculate_distance(op.as_ref(), onn_center.as_ref());
							// Add Op to S;
							s.insert(op, p);
						}
						// Let r(Onn) = max (Os E S) {[arentDistance Os)}
						let mut max_parent_distance = 0.0;
						for (_, p) in s {
							if p.parent_dist > max_parent_distance {
								max_parent_distance = p.parent_dist;
							}
						}
						let onn_entry = &mut n_node[onn_idx];
						if onn_entry.radius != max_parent_distance {
							onn_entry.radius = max_parent_distance;
							n_updated = true;
						}
					}
				}
			} else {
				return Err(Error::FeatureNotYetImplemented {
					feature: "MTREE deletions ()".to_string(),
				});
			}
			store.remove_node(p_id, p_key)?;
			store.set_node(onn_node, true)?;
		}
		Ok(n_updated)
	}

	#[allow(clippy::too_many_arguments)]
	async fn delete_node_leaf(
		&mut self,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		mut leaf_node: LeafNode,
		parent_center: &Option<Arc<Vector>>,
		od: Arc<Vector>,
		id: DocId,
	) -> Result<DeletionResult, Error> {
		#[cfg(debug_assertions)]
		debug!("delete_node_leaf: {} {:?}", node_id, od);
		let mut doc_removed = false;
		let mut entry_removed = false;
		// If (Od E N)
		if let Entry::Occupied(mut e) = leaf_node.entry(od) {
			let p = e.get_mut();
			// Remove Od from N
			if p.docs.remove(id) {
				doc_removed = true;
				if p.docs.is_empty() {
					e.remove();
					entry_removed = true;
				}
			}
		}
		// If (N is underflown)
		if entry_removed && leaf_node.len() < self.minimum {
			return Ok(DeletionResult::Underflown(node_id, node_key, MTreeNode::Leaf(leaf_node)));
		}
		if doc_removed {
			let sn = StoredNode::new(MTreeNode::Leaf(leaf_node), node_id, node_key, 0);
			store.set_node(sn, true)?;
			return Ok(DeletionResult::DocRemoved);
		}
		// Return max(Ol E N) { parentDistance(Ol)};
		let max_dist = self.compute_leaf_max_distance(&leaf_node, parent_center);
		let sn = StoredNode::new(MTreeNode::Leaf(leaf_node), node_id, node_key, 0);
		store.set_node(sn, false)?;
		Ok(DeletionResult::CoveringRadius(max_dist))
	}

	async fn finish(&self, tx: &mut Transaction, key: Key) -> Result<(), Error> {
		if self.updated {
			tx.set(key, self.state.try_to_val()?).await?;
		}
		Ok(())
	}
}

#[derive(PartialEq)]
struct PriorityNode(f64, NodeId);

impl Eq for PriorityNode {}

fn partial_cmp_f64(a: f64, b: f64) -> Option<Ordering> {
	let a = if a.is_nan() {
		f64::NEG_INFINITY
	} else {
		a
	};
	let b = if b.is_nan() {
		f64::NEG_INFINITY
	} else {
		b
	};
	a.partial_cmp(&b)
}

impl PartialOrd for PriorityNode {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for PriorityNode {
	fn cmp(&self, other: &Self) -> Ordering {
		match partial_cmp_f64(self.0, other.0).unwrap_or(Ordering::Equal) {
			Ordering::Equal => self.1.cmp(&other.1),
			other => other,
		}
	}
}

#[derive(PartialEq)]
struct PriorityResult(f64, Arc<Vector>);

impl Eq for PriorityResult {}

impl PartialOrd for PriorityResult {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for PriorityResult {
	fn cmp(&self, other: &Self) -> Ordering {
		match partial_cmp_f64(self.0, other.0).unwrap_or(Ordering::Equal) {
			Ordering::Equal => self.1.cmp(&other.1),
			other => other,
		}
	}
}

#[derive(Debug)]
enum MTreeNode {
	Internal(InternalNode),
	Leaf(LeafNode),
}

impl MTreeNode {
	fn len(&self) -> usize {
		match self {
			MTreeNode::Internal(e) => e.len(),
			MTreeNode::Leaf(m) => m.len(),
		}
	}

	fn internal(self) -> Result<InternalNode, Error> {
		match self {
			MTreeNode::Internal(n) => Ok(n),
			MTreeNode::Leaf(_) => Err(Error::Unreachable),
		}
	}

	fn leaf(self) -> Result<LeafNode, Error> {
		match self {
			MTreeNode::Internal(_) => Err(Error::Unreachable),
			MTreeNode::Leaf(n) => Ok(n),
		}
	}
}
trait NodeVectors: Sized {
	fn len(&self) -> usize;
	fn get_vector(&self, i: usize) -> Result<Arc<Vector>, Error>;

	fn distribute_entries(
		self,
		distances: &[Vec<f64>],
		p1: usize,
		p2: usize,
	) -> Result<(Self, f64, Self, f64), Error>;

	fn into_mtree_node(self) -> MTreeNode;
}

impl NodeVectors for LeafNode {
	fn len(&self) -> usize {
		self.len()
	}

	fn get_vector(&self, i: usize) -> Result<Arc<Vector>, Error> {
		self.get_index(i).ok_or(Error::Unreachable).map(|(v, _)| v.clone())
	}

	fn distribute_entries(
		mut self,
		distances: &[Vec<f64>],
		p1: usize,
		p2: usize,
	) -> Result<(Self, f64, Self, f64), Error> {
		let mut leaf1 = LeafNode::new();
		let mut leaf2 = LeafNode::new();
		let (mut r1, mut r2) = (0f64, 0f64);
		for (i, (v, mut p)) in self.drain(..).enumerate() {
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
		Ok((leaf1, r1, leaf2, r2))
	}

	fn into_mtree_node(self) -> MTreeNode {
		MTreeNode::Leaf(self)
	}
}

impl NodeVectors for InternalNode {
	fn len(&self) -> usize {
		self.len()
	}

	fn get_vector(&self, i: usize) -> Result<Arc<Vector>, Error> {
		self.get(i).ok_or(Error::Unreachable).map(|e| e.center.clone())
	}

	fn distribute_entries(
		self,
		distances: &[Vec<f64>],
		p1: usize,
		p2: usize,
	) -> Result<(Self, f64, Self, f64), Error> {
		let mut internal1 = InternalNode::new();
		let mut internal2 = InternalNode::new();
		let (mut r1, mut r2) = (0f64, 0f64);
		for (i, r) in self.into_iter().enumerate() {
			let dist_p1 = distances[i][p1];
			let dist_p2 = distances[i][p2];
			if dist_p1 <= dist_p2 {
				internal1.push(r);
				if dist_p1 > r1 {
					r1 = dist_p1;
				}
			} else {
				internal2.push(r);
				if dist_p2 > r2 {
					r2 = dist_p2;
				}
			}
		}
		Ok((internal1, r1, internal2, r2))
	}

	fn into_mtree_node(self) -> MTreeNode {
		MTreeNode::Internal(self)
	}
}

type InternalNode = Vec<RoutingEntry>;
type LeafNode = LeafIndexMap;

impl TreeNode for MTreeNode {
	fn try_from_val(val: Val) -> Result<Self, Error> {
		let mut c: Cursor<Vec<u8>> = Cursor::new(val);
		let node_type: u8 = bincode::deserialize_from(&mut c)?;
		match node_type {
			1u8 => {
				let objects: IndexMap<Arc<Vector>, ObjectProperties> =
					bincode::deserialize_from(c)?;
				Ok(MTreeNode::Leaf(objects))
			}
			2u8 => {
				let entries: Vec<RoutingEntry> = bincode::deserialize_from(c)?;
				Ok(MTreeNode::Internal(entries))
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
			MTreeNode::Internal(entries) => {
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
#[revisioned(revision = 1)]
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

#[derive(Serialize, Deserialize, Debug)]
pub(in crate::idx) struct RoutingEntry {
	// Reference to the node
	node: NodeId,
	// Center of the node
	center: Arc<Vector>,
	// Covering radius
	radius: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub(in crate::idx) struct ObjectProperties {
	// Distance to its parent object
	parent_dist: f64,
	// The documents pointing to this vector
	docs: RoaringTreemap,
}

impl ObjectProperties {
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

impl VersionedSerdeState for MState {}

#[cfg(test)]
mod tests {
	use crate::idx::docids::DocId;
	use crate::idx::trees::mtree::{
		MState, MTree, MTreeNode, MTreeNodeStore, ObjectProperties, RoutingEntry, Vector,
	};
	use crate::idx::trees::store::{NodeId, TreeNodeProvider, TreeNodeStore, TreeStoreType};
	use crate::kvs::Datastore;
	use crate::kvs::Transaction;
	use crate::sql::index::Distance;
	use indexmap::IndexMap;
	use roaring::RoaringTreemap;
	use std::collections::VecDeque;
	use std::sync::Arc;
	use test_log::test;
	use tokio::sync::{Mutex, MutexGuard};

	async fn new_operation(
		ds: &Datastore,
		t: TreeStoreType,
	) -> (Arc<Mutex<TreeNodeStore<MTreeNode>>>, Transaction) {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, t, 20);
		let tx = ds.transaction(t == TreeStoreType::Write, false).await.unwrap();
		(s, tx)
	}

	async fn finish_operation(
		mut tx: Transaction,
		mut s: MutexGuard<'_, TreeNodeStore<MTreeNode>>,
		commit: bool,
	) {
		s.finish(&mut tx).await.unwrap();
		if commit {
			tx.commit().await.unwrap();
		} else {
			tx.cancel().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_mtree_insertions() {
		let mut t = MTree::new(MState::new(3), Distance::Euclidean);
		let ds = Datastore::new("memory").await.unwrap();

		let vec1 = vec![1.into()];
		// First the index is empty
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.objects, vec![]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 0);
		}
		// Insert single element
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec1.clone(), 1).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 1);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
			})
			.await;
			finish_operation(tx, s, true).await;
		}
		// Check KNN
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![1]]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 1);
			check_tree_properties(&mut tx, &mut s, &t, 1, 1, Some(1), Some(1), 1, 1).await;
		}

		// insert second element
		let vec2 = vec![2.into()];
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec2.clone(), 2).await.unwrap();
			finish_operation(tx, s, true).await;
		}
		// vec1 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![1], vec![2]]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 1);
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 0.0, &[2]);
			})
			.await;
			check_tree_properties(&mut tx, &mut s, &t, 1, 1, Some(2), Some(2), 2, 2).await;
		}
		// vec2 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec2, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![2], vec![1]]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 1);
		}

		// insert new doc to existing vector
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec2.clone(), 3).await.unwrap();
			finish_operation(tx, s, true).await;
		}
		// vec2 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec2, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![2, 3], vec![1]]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 1);
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 0.0, &[2, 3]);
			})
			.await;
			check_tree_properties(&mut tx, &mut s, &t, 1, 1, Some(2), Some(2), 2, 3).await;
		}

		// insert third vector
		let vec3 = vec![3.into()];
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec3.clone(), 3).await.unwrap();
			finish_operation(tx, s, true).await;
		}
		// vec3 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec3, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![3], vec![2, 3], vec![1]]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 1);
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 0.0, &[2, 3]);
				check_leaf_vec(m, 2, &vec3, 0.0, &[3]);
			})
			.await;
			check_tree_properties(&mut tx, &mut s, &t, 1, 1, Some(3), Some(3), 3, 4).await;
		}

		// Check split leaf node
		let vec4 = vec![4.into()];
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec4.clone(), 4).await.unwrap();
			finish_operation(tx, s, true).await;
		}
		// vec4 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec4, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![4], vec![3], vec![2, 3], vec![1]]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 3);
			assert_eq!(t.state.root, Some(2));
			check_internal(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, 0, &vec1, 0, 1.0);
				check_routing_vec(m, 1, &vec4, 1, 1.0);
			})
			.await;
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec3, 1.0, &[3]);
				check_leaf_vec(m, 1, &vec4, 0.0, &[4]);
			})
			.await;
			check_tree_properties(&mut tx, &mut s, &t, 3, 2, Some(2), Some(2), 4, 5).await;
		}

		// Insert vec extending the radius of the last node, calling compute_leaf_radius
		let vec6 = vec![6.into()];
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec6.clone(), 6).await.unwrap();
			finish_operation(tx, s, true).await;
		}
		// vec6 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec6, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![6], vec![4], vec![3], vec![2, 3], vec![1]]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 3);
			assert_eq!(t.state.root, Some(2));
			check_internal(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, 0, &vec1, 0, 1.0);
				check_routing_vec(m, 1, &vec4, 1, 2.0);
			})
			.await;
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, 0, &vec3, 1.0, &[3]);
				check_leaf_vec(m, 1, &vec4, 0.0, &[4]);
				check_leaf_vec(m, 2, &vec6, 2.0, &[6]);
			})
			.await;
			check_tree_properties(&mut tx, &mut s, &t, 3, 2, Some(2), Some(3), 5, 6).await;
		}

		// Insert check split internal node
		let vec8 = vec![8.into()];
		let vec9 = vec![9.into()];
		let vec10 = vec![10.into()];
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec8.clone(), 8).await.unwrap();
			t.insert(&mut tx, &mut s, vec9.clone(), 9).await.unwrap();
			t.insert(&mut tx, &mut s, vec10.clone(), 10).await.unwrap();
			finish_operation(tx, s, true).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			check_tree_properties(&mut tx, &mut s, &t, 7, 3, Some(2), Some(2), 8, 9).await;
			assert_eq!(t.state.root, Some(6));
			// Check Root node (level 1)
			check_internal(&mut tx, &mut s, 6, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, 0, &vec1, 2, 2.0);
				check_routing_vec(m, 1, &vec10, 5, 4.0);
			})
			.await;
			// Check level 2
			check_internal(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, 0, &vec1, 0, 1.0);
				check_routing_vec(m, 1, &vec3, 1, 1.0);
			})
			.await;
			check_internal(&mut tx, &mut s, 5, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, 0, &vec6, 3, 2.0);
				check_routing_vec(m, 1, &vec10, 4, 1.0);
			})
			.await;
			// Check level 3
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec3, 0.0, &[3]);
				check_leaf_vec(m, 1, &vec4, 1.0, &[4]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 3, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec6, 0.0, &[6]);
				check_leaf_vec(m, 1, &vec8, 2.0, &[8]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 4, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec9, 1.0, &[9]);
				check_leaf_vec(m, 1, &vec10, 0.0, &[10]);
			})
			.await;
		}
		// vec8 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec8, 20).await.unwrap();
			check_knn(
				&res.objects,
				vec![vec![8], vec![9], vec![6], vec![10], vec![4], vec![3], vec![2, 3], vec![1]],
			);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 7);
		}
		// vec4 knn(2)
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec4, 2).await.unwrap();
			check_knn(&res.objects, vec![vec![4], vec![3]]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 7);
		}

		// vec10 knn(2)
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec10, 2).await.unwrap();
			check_knn(&res.objects, vec![vec![10], vec![9]]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 7);
		}
	}

	#[test(tokio::test)]
	async fn test_mtree_deletion_doc_removed_and_none() {
		let ds = Datastore::new("memory").await.unwrap();

		let mut t = MTree::new(MState::new(4), Distance::Euclidean);

		let vec1 = vec![1.into()];
		let vec2 = vec![2.into()];

		// Create the tree with vec1 and vec2 having two documents
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec1.clone(), 10).await.unwrap();
			t.insert(&mut tx, &mut s, vec2.clone(), 20).await.unwrap();
			t.insert(&mut tx, &mut s, vec2.clone(), 21).await.unwrap();
			finish_operation(tx, s, true).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![10], vec![20, 21]]);
			check_tree_properties(&mut tx, &mut s, &t, 1, 1, Some(2), Some(2), 2, 3).await;
		}

		// Remove the doc 21
		{
			debug!("Remove vec 2/21");
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(t.delete(&mut tx, &mut s, vec2.clone(), 21).await.unwrap());
			finish_operation(tx, s, true).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![10], vec![20]]);
			check_tree_properties(&mut tx, &mut s, &t, 1, 1, Some(2), Some(2), 2, 2).await;
		}

		// Remove again vec2 / 21 => Deletion::None
		{
			debug!("Remove vec 2/21");
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(!t.delete(&mut tx, &mut s, vec2.clone(), 21).await.unwrap());
			assert!(!t.delete(&mut tx, &mut s, vec2.clone(), 21).await.unwrap());
			finish_operation(tx, s, true).await;
		}

		let vec3 = vec![3.into()];
		let vec4 = vec![4.into()];
		let vec5 = vec![5.into()];

		// Add vec3, vec4 and vec5 having two documents
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec3.clone(), 30).await.unwrap();
			t.insert(&mut tx, &mut s, vec4.clone(), 40).await.unwrap();
			t.insert(&mut tx, &mut s, vec5.clone(), 50).await.unwrap();
			t.insert(&mut tx, &mut s, vec5.clone(), 51).await.unwrap();
			finish_operation(tx, s, true).await;
		}
		{
			debug!("Remove vec 3/4/5");
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![10], vec![20], vec![30], vec![40], vec![51]]);
			check_tree_properties(&mut tx, &mut s, &t, 3, 2, Some(2), Some(3), 5, 6).await;
		}

		// Remove the doc 51
		{
			debug!("Remove vec 5/51");
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(t.delete(&mut tx, &mut s, vec5.clone(), 51).await.unwrap());
			finish_operation(tx, s, true).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![10], vec![20], vec![30], vec![40], vec![50]]);
			check_tree_properties(&mut tx, &mut s, &t, 3, 2, Some(2), Some(3), 5, 5).await;
		}

		// Remove again vec5 / 51 => Deletion::None
		{
			debug!("Remove vec5/51 (again)");
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(!t.delete(&mut tx, &mut s, vec5.clone(), 51).await.unwrap());
			assert!(!t.delete(&mut tx, &mut s, vec5.clone(), 51).await.unwrap());
			finish_operation(tx, s, true).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![10], vec![20], vec![30], vec![40], vec![50]]);
			check_tree_properties(&mut tx, &mut s, &t, 3, 2, Some(2), Some(3), 5, 5).await;
		}

		// Remove vec5 / 50 => DeleteResult::UnderflownLeafIndexMap
		{
			debug!("Remove vec5/50");
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(t.delete(&mut tx, &mut s, vec5.clone(), 50).await.unwrap());
			finish_operation(tx, s, true).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.objects, vec![vec![10], vec![20], vec![30], vec![40]]);
			check_tree_properties(&mut tx, &mut s, &t, 1, 1, Some(4), Some(4), 4, 4).await;
		}

		// Remove vec 1/2/3/4 => Root = None
		{
			debug!("Remove vec 1/2/3/4");
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(t.delete(&mut tx, &mut s, vec1.clone(), 10).await.unwrap());
			assert!(t.delete(&mut tx, &mut s, vec2.clone(), 20).await.unwrap());
			assert!(t.delete(&mut tx, &mut s, vec3.clone(), 30).await.unwrap());
			assert!(t.delete(&mut tx, &mut s, vec4.clone(), 40).await.unwrap());
			finish_operation(tx, s, true).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.objects, vec![]);
			check_tree_properties(&mut tx, &mut s, &t, 0, 0, None, None, 0, 0).await;
		}
	}

	#[test(tokio::test)]
	async fn test_mtree_deletions_underflown_internal_node() {
		let ds = Datastore::new("memory").await.unwrap();

		let mut t = MTree::new(MState::new(4), Distance::Euclidean);

		let v0 = vec![0.into()];
		let v1 = vec![1.into()];
		let v2 = vec![2.into()];
		let v3 = vec![3.into()];
		let v4 = vec![4.into()];
		let v5 = vec![5.into()];
		let v6 = vec![6.into()];
		let v7 = vec![7.into()];
		let v8 = vec![8.into()];
		let v9 = vec![9.into()];
		let v10 = vec![10.into()];
		let v11 = vec![11.into()];
		let v12 = vec![12.into()];
		let v13 = vec![13.into()];
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, v9.clone(), 90).await.unwrap();
			t.insert(&mut tx, &mut s, v10.clone(), 100).await.unwrap();
			t.insert(&mut tx, &mut s, v11.clone(), 110).await.unwrap();
			t.insert(&mut tx, &mut s, v12.clone(), 120).await.unwrap();
			t.insert(&mut tx, &mut s, v13.clone(), 130).await.unwrap();
			t.insert(&mut tx, &mut s, v1.clone(), 10).await.unwrap();
			t.insert(&mut tx, &mut s, v2.clone(), 20).await.unwrap();
			t.insert(&mut tx, &mut s, v3.clone(), 30).await.unwrap();
			t.insert(&mut tx, &mut s, v4.clone(), 40).await.unwrap();
			t.insert(&mut tx, &mut s, v5.clone(), 50).await.unwrap();
			t.insert(&mut tx, &mut s, v6.clone(), 60).await.unwrap();
			t.insert(&mut tx, &mut s, v7.clone(), 70).await.unwrap();
			t.insert(&mut tx, &mut s, v8.clone(), 80).await.unwrap();
			finish_operation(tx, s, true).await;
		}

		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &v0, 20).await.unwrap();
			check_knn(
				&res.objects,
				vec![
					vec![10],
					vec![20],
					vec![30],
					vec![40],
					vec![50],
					vec![60],
					vec![70],
					vec![80],
					vec![90],
					vec![100],
					vec![110],
					vec![120],
					vec![130],
				],
			);
			check_tree_properties(&mut tx, &mut s, &t, 8, 3, Some(2), Some(3), 13, 13).await;
		}

		// Remove v4 ->
		{
			debug!("Remove v4");
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(t.delete(&mut tx, &mut s, v4.clone(), 40).await.unwrap());
			finish_operation(tx, s, true).await;
		}

		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			check_tree_properties(&mut tx, &mut s, &t, 8, 3, Some(2), Some(3), 12, 12).await;
		}

		// Remove v4 ->
		{
			debug!("Remove v5");
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(t.delete(&mut tx, &mut s, v5.clone(), 50).await.unwrap());
			finish_operation(tx, s, true).await;
		}

		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			check_tree_properties(&mut tx, &mut s, &t, 8, 3, Some(2), Some(3), 11, 11).await;
		}
	}

	fn check_leaf_vec(
		m: &IndexMap<Arc<Vector>, ObjectProperties>,
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
		m: &Vec<RoutingEntry>,
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
		F: FnOnce(&IndexMap<Arc<Vector>, ObjectProperties>),
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

	async fn check_internal<F>(
		tx: &mut Transaction,
		s: &mut MTreeNodeStore,
		node_id: NodeId,
		check_func: F,
	) where
		F: FnOnce(&Vec<RoutingEntry>),
	{
		check_node(tx, s, node_id, |n| {
			if let MTreeNode::Internal(m) = n {
				check_func(m);
			} else {
				panic!("The node is not a routing node: {node_id}")
			}
		})
		.await
	}

	fn check_knn(res: &VecDeque<RoaringTreemap>, expected: Vec<Vec<DocId>>) {
		assert_eq!(res.len(), expected.len(), "{:?}", res);
		for (i, (a, b)) in res.iter().zip(expected.iter()).enumerate() {
			for id in b {
				assert!(a.contains(*id), "{}: {}", i, id);
			}
		}
	}

	async fn check_tree_properties(
		tx: &mut Transaction,
		s: &mut MTreeNodeStore,
		t: &MTree,
		expected_node_count: usize,
		expected_depth: usize,
		expected_min_objects: Option<usize>,
		expected_max_objects: Option<usize>,
		expected_object_count: usize,
		expected_doc_count: usize,
	) {
		debug!("CheckTreeProperties");
		let mut node_count = 0;
		let mut max_depth = 0;
		let mut min_leaf_depth = None;
		let mut max_leaf_depth = None;
		let mut min_objects = None;
		let mut max_objects = None;
		let mut object_count = 0;
		let mut doc_count = 0;
		let mut nodes = VecDeque::new();
		if let Some(root_id) = t.state.root {
			nodes.push_back((root_id, 1));
		}
		while let Some((node_id, depth)) = nodes.pop_front() {
			node_count += 1;
			if depth > max_depth {
				max_depth = depth;
			}
			let node = s.get_node(tx, node_id).await.unwrap();
			debug!(
				"Node id: {} - depth: {} - len: {} - {:?}",
				node.id,
				depth,
				node.n.len(),
				node.n
			);
			match node.n {
				MTreeNode::Internal(entries) => {
					let next_depth = depth + 1;
					entries.iter().for_each(|p| nodes.push_back((p.node, next_depth)));
				}
				MTreeNode::Leaf(m) => {
					object_count += m.len();
					update_min(&mut min_objects, m.len());
					update_max(&mut max_objects, m.len());
					update_min(&mut min_leaf_depth, depth);
					update_max(&mut max_leaf_depth, depth);
					for (_, p) in m {
						doc_count += p.docs.len();
					}
				}
			}
		}
		assert_eq!(node_count, expected_node_count, "Node count");
		assert_eq!(max_depth, expected_depth, "Max depth");
		let expected_leaf_depth = if expected_depth == 0 {
			None
		} else {
			Some(expected_depth)
		};
		assert_eq!(min_leaf_depth, expected_leaf_depth, "Min leaf depth");
		assert_eq!(max_leaf_depth, expected_leaf_depth, "Max leaf depth");
		assert_eq!(min_objects, expected_min_objects, "Min objects");
		assert_eq!(max_objects, expected_max_objects, "Max objects");
		assert_eq!(object_count, expected_object_count, "Object count");
		assert_eq!(doc_count as usize, expected_doc_count, "Doc count");
	}

	fn update_min(min: &mut Option<usize>, val: usize) {
		if let Some(m) = *min {
			if val < m {
				*min = Some(val);
			}
		} else {
			*min = Some(val);
		}
	}

	fn update_max(max: &mut Option<usize>, val: usize) {
		if let Some(m) = *max {
			if val > m {
				*max = Some(val);
			}
		} else {
			*max = Some(val);
		}
	}
}
