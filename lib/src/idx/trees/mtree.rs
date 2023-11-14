use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

use async_recursion::async_recursion;
use revision::revisioned;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::err::Error;

use crate::idx::docids::{DocId, DocIds};
use crate::idx::trees::btree::BStatistics;
use crate::idx::trees::store::{
	NodeId, StoredNode, TreeNode, TreeNodeProvider, TreeNodeStore, TreeStoreType,
};
use crate::idx::trees::vector::{SharedVector, Vector};
use crate::idx::{IndexKeyBase, VersionedSerdeState};
use crate::kvs::{Key, Transaction, Val};
use crate::sql::index::{Distance, MTreeParams, VectorType};
use crate::sql::{Array, Object, Thing, Value};

pub(crate) struct MTreeIndex {
	state_key: Key,
	dim: usize,
	vector_type: VectorType,
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
			vector_type: p.vector_type,
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
			let vector = self.extract_vector(v)?;
			mtree.insert(tx, &mut store, vector, doc_id).await?;
		}
		Ok(())
	}

	pub(crate) async fn knn_search(
		&self,
		tx: &mut Transaction,
		a: Array,
		k: usize,
	) -> Result<VecDeque<DocId>, Error> {
		// Extract the vector
		let vector = self.check_vector_array(a)?;
		// Lock the store
		let mut store = self.store.lock().await;
		let res = self.mtree.read().await.knn_search(tx, &mut store, &vector, k).await?;
		Ok(res.docs)
	}

	fn check_vector_array(&self, a: Array) -> Result<Vector, Error> {
		if a.0.len() != self.dim {
			return Err(Error::InvalidVectorDimension {
				current: a.0.len(),
				expected: self.dim,
			});
		}
		let mut vec = Vector::new(self.vector_type, a.len());
		for v in a.0 {
			if let Value::Number(n) = v {
				vec.add(n);
			} else {
				return Err(Error::InvalidVectorType {
					current: v.clone().to_string(),
					expected: "Number",
				});
			}
		}
		Ok(vec)
	}

	fn extract_vector(&self, v: Value) -> Result<Vector, Error> {
		let mut vec = Vector::new(self.vector_type, self.dim);
		Self::check_vector_value(v, &mut vec)?;
		if vec.len() != self.dim {
			return Err(Error::InvalidVectorDimension {
				current: vec.len(),
				expected: self.dim,
			});
		}
		Ok(vec)
	}

	fn check_vector_value(value: Value, vec: &mut Vector) -> Result<(), Error> {
		match value {
			Value::Array(a) => {
				for v in a {
					Self::check_vector_value(v, vec)?;
				}
				Ok(())
			}
			Value::Number(n) => {
				vec.add(n);
				Ok(())
			}
			_ => Err(Error::InvalidVectorValue {
				current: value.clone().to_raw_string(),
			}),
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
				let vector = self.extract_vector(v)?;
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

struct KnnResultBuilder {
	knn: u64,
	docs: RoaringTreemap,
	priority_list: BTreeMap<PriorityResult, RoaringTreemap>,
}

impl KnnResultBuilder {
	fn new(knn: usize) -> Self {
		Self {
			knn: knn as u64,
			docs: RoaringTreemap::default(),
			priority_list: BTreeMap::default(),
		}
	}
	fn check_add(&self, dist: f64) -> bool {
		if self.docs.len() < self.knn {
			true
		} else if let Some(pr) = self.priority_list.keys().last() {
			dist <= pr.0
		} else {
			true
		}
	}

	fn add(&mut self, dist: f64, docs: &RoaringTreemap) {
		let pr = PriorityResult(dist);
		match self.priority_list.entry(pr) {
			Entry::Vacant(e) => {
				e.insert(docs.clone());
			}
			Entry::Occupied(mut e) => {
				let d = e.get_mut();
				for doc in docs {
					d.insert(doc);
				}
			}
		}
		// Do possible eviction
		let docs_len = self.docs.len();
		if docs_len > self.knn {
			if let Some((_, d)) = self.priority_list.last_key_value() {
				if docs_len - d.len() >= self.knn {
					self.priority_list.pop_last();
				}
			}
		}
	}

	fn build(self, #[cfg(debug_assertions)] visited_nodes: usize) -> KnnResult {
		let mut objects = VecDeque::with_capacity(self.knn as usize);
		let mut left = self.knn;
		for (_, docs) in self.priority_list {
			let dl = docs.len();
			if dl > left {
				for doc_id in docs.iter().take(left as usize) {
					objects.push_back(doc_id);
				}
				break;
			}
			for doc_id in docs {
				objects.push_back(doc_id);
			}
			left -= dl;
			// We don't expect anymore result, we can leave
			if left == 0 {
				break;
			}
		}
		KnnResult {
			docs: objects,
			#[cfg(debug_assertions)]
			visited_nodes,
		}
	}
}

pub struct KnnResult {
	docs: VecDeque<DocId>,
	#[cfg(debug_assertions)]
	#[allow(dead_code)]
	visited_nodes: usize,
}

// https://en.wikipedia.org/wiki/M-tree
// https://arxiv.org/pdf/1004.4216.pdf
pub struct MTree {
	state: MState,
	distance: Distance,
	minimum: usize,
	updated: bool,
}

impl MTree {
	pub fn new(state: MState, distance: Distance) -> Self {
		let minimum = (state.capacity + 1) as usize / 2;
		Self {
			state,
			distance,
			minimum,
			updated: false,
		}
	}

	pub async fn knn_search(
		&self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		v: &Vector,
		k: usize,
	) -> Result<KnnResult, Error> {
		#[cfg(debug_assertions)]
		debug!("knn_search - v: {:?} - k: {}", v, k);
		let mut queue = BinaryHeap::new();
		let mut res = KnnResultBuilder::new(k);
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
						let d = self.calculate_distance(o.as_ref(), v)?;
						#[cfg(debug_assertions)]
						debug!("Leaf found: {} - Obj: {:?} - Docs: {:?}", node.id, o, p.docs);
						if res.check_add(d) {
							res.add(d, &p.docs);
						}
					}
				}
				MTreeNode::Internal(ref n) => {
					for (o, p) in n {
						let d = self.calculate_distance(o.as_ref(), v)?;
						let min_dist = (d - p.radius).max(0.0);
						if res.check_add(min_dist) {
							queue.push(PriorityNode(min_dist, p.node));
						}
					}
				}
			}
			store.set_node(node, false)?;
		}
		Ok(res.build(
			#[cfg(debug_assertions)]
			visited_nodes,
		))
	}
}

enum InsertionResult {
	DocAdded,
	CoveringRadius(f64),
	PromotedEntries(SharedVector, RoutingProperties, SharedVector, RoutingProperties),
}

enum DeletionResult {
	NotFound,
	DocRemoved,
	CoveringRadius(f64),
	Underflown(StoredNode<MTreeNode>, bool),
}

// Insertion
impl MTree {
	fn new_node_id(&mut self) -> NodeId {
		let new_node_id = self.state.next_node_id;
		self.state.next_node_id += 1;
		new_node_id
	}

	pub async fn insert(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		obj: Vector,
		id: DocId,
	) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		debug!("Insert - obj: {:?} - doc: {}", obj, id);
		let obj = Arc::new(obj);
		// First we check if we already have the object. In this case we just append the doc.
		if self.append(tx, store, obj.clone(), id).await? {
			return Ok(());
		}
		if let Some(root_id) = self.state.root {
			let node = store.get_node(tx, root_id).await?;
			// Otherwise, we insert the object with possibly mutating the tree
			if let InsertionResult::PromotedEntries(o1, p1, o2, p2) =
				self.insert_at_node(tx, store, node, &None, obj, id).await?
			{
				self.create_new_internal_root(store, o1, p1, o2, p2)?;
			}
		} else {
			self.create_new_leaf_root(store, obj, id)?;
		}
		Ok(())
	}

	fn create_new_leaf_root(
		&mut self,
		store: &mut MTreeNodeStore,
		obj: SharedVector,
		id: DocId,
	) -> Result<(), Error> {
		let new_root_id = self.new_node_id();
		let p = ObjectProperties::new_root(id);
		let mut objects = LeafMap::new();
		objects.insert(obj, p);
		let new_root_node = store.new_node(new_root_id, MTreeNode::Leaf(objects))?;
		store.set_node(new_root_node, true)?;
		self.set_root(Some(new_root_id));
		Ok(())
	}

	fn create_new_internal_root(
		&mut self,
		store: &mut MTreeNodeStore,
		o1: SharedVector,
		p1: RoutingProperties,
		o2: SharedVector,
		p2: RoutingProperties,
	) -> Result<(), Error> {
		let new_root_id = self.new_node_id();
		#[cfg(debug_assertions)]
		debug!(
			"New internal root - node: {} - r1: {:?}/{} - r2: {:?}/{}",
			new_root_id,
			o1.as_ref(),
			p1.radius,
			o2.as_ref(),
			p2.radius
		);
		let mut entries = InternalMap::new();
		entries.insert(o1, p1);
		entries.insert(o2, p2);
		let new_root_node = store.new_node(new_root_id, MTreeNode::Internal(entries))?;
		store.set_node(new_root_node, true)?;
		self.set_root(Some(new_root_id));
		Ok(())
	}

	async fn append(
		&self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		object: SharedVector,
		id: DocId,
	) -> Result<bool, Error> {
		let mut queue = BinaryHeap::new();
		if let Some(root_id) = self.state.root {
			queue.push(root_id);
		}
		while let Some(current) = queue.pop() {
			let mut node = store.get_node(tx, current).await?;
			match node.n {
				MTreeNode::Leaf(ref mut n) => {
					if let Some(p) = n.get_mut(&object) {
						p.docs.insert(id);
						store.set_node(node, true)?;
						return Ok(true);
					}
				}
				MTreeNode::Internal(ref n) => {
					for (o, p) in n {
						let d = self.calculate_distance(o.as_ref(), object.as_ref())?;
						if d <= p.radius {
							queue.push(p.node);
						}
					}
				}
			}
			store.set_node(node, false)?;
		}
		Ok(false)
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	async fn insert_at_node(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		node: StoredNode<MTreeNode>,
		parent_center: &Option<Arc<Vector>>,
		object: SharedVector,
		id: DocId,
	) -> Result<InsertionResult, Error> {
		#[cfg(debug_assertions)]
		debug!("insert_at_node - node: {} - obj: {:?}", node.id, object);
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
		object: SharedVector,
		id: DocId,
	) -> Result<InsertionResult, Error> {
		// Choose `best` substree entry ObestSubstree from N;
		let (best_entry_obj, mut best_entry) = self.find_closest(&node, &object)?;
		let best_node = store.get_node(tx, best_entry.node).await?;
		// Insert(Oi, child(ObestSubstree), ObestSubtree);
		match self
			.insert_at_node(tx, store, best_node, &Some(best_entry_obj.clone()), object, id)
			.await?
		{
			// If (entry returned)
			InsertionResult::PromotedEntries(o1, mut p1, o2, mut p2) => {
				#[cfg(debug_assertions)]
				debug!(
					"Promote to Node: {} - e1: {} {:?} {} - e2: {} {:?} {} ",
					node_id, p1.node, o1, p1.radius, p2.node, o2, p2.radius
				);
				// Remove ObestSubtree from N;
				node.remove(&best_entry_obj);
				// if (N U P will fit into N)
				let mut nup: HashSet<Arc<Vector>> = HashSet::from_iter(node.keys().cloned());
				nup.insert(o1.clone());
				nup.insert(o2.clone());
				if nup.len() <= self.state.capacity as usize {
					// Let parentDistance(Op) = d(Op, parent(N));
					p1.parent_dist = parent_center.as_ref().map_or(0.0, |pd| {
						self.calculate_distance(o1.as_ref(), pd.as_ref()).unwrap_or(0.0)
					});
					p2.parent_dist = parent_center.as_ref().map_or(0.0, |pd| {
						self.calculate_distance(o2.as_ref(), pd.as_ref()).unwrap_or(0.0)
					});
					node.insert(o1, p1);
					node.insert(o2, p2);
					let max_dist = self.compute_internal_max_distance(&node);
					#[cfg(debug_assertions)]
					debug!("NODE: {} - MAX_DIST: {:?}", node_id, max_dist);
					Self::set_stored_node(store, node_id, node_key, node.into_mtree_node(), true)?;
					Ok(InsertionResult::CoveringRadius(max_dist))
				} else {
					node.insert(o1, p1);
					node.insert(o2, p2);
					// Split(N U P)
					let (o1, p1, o2, p2) = self.split_node(store, node_id, node_key, node)?;
					Ok(InsertionResult::PromotedEntries(o1, p1, o2, p2))
				}
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
					#[cfg(debug_assertions)]
					debug!(
						"NODE: {} - BE_OBJ: {:?} - BE_RADIUS: {} -> {}",
						node_id,
						best_entry_obj.as_ref(),
						best_entry.radius,
						covering_radius
					);
					best_entry.radius = covering_radius;
					node.insert(best_entry_obj, best_entry);
					updated = true;
				}
				let max_dist = self.compute_internal_max_distance(&node);
				#[cfg(debug_assertions)]
				debug!("NODE INTERNAL: {} - MAX_DIST: {:?}", node_id, max_dist);
				store.set_node(
					StoredNode::new(node.into_mtree_node(), node_id, node_key, 0),
					updated,
				)?;
				Ok(InsertionResult::CoveringRadius(max_dist))
			}
		}
	}

	fn find_closest(
		&self,
		node: &InternalNode,
		object: &Vector,
	) -> Result<(SharedVector, RoutingProperties), Error> {
		let mut closest = None;
		let mut dist = f64::MAX;
		for (o, p) in node {
			let d = self.calculate_distance(o.as_ref(), object)?;
			if d < dist {
				closest = Some((o.clone(), p.clone()));
				dist = d;
			}
		}
		#[cfg(debug_assertions)]
		debug!("Find closest {:?} - Res: {:?}", object, closest);
		if let Some((o, p)) = closest {
			Ok((o, p))
		} else {
			Err(Error::Unreachable)
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn insert_node_leaf(
		&mut self,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		mut node: LeafNode,
		parent_center: &Option<Arc<Vector>>,
		object: SharedVector,
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
					.map_or(0.0, |v| self.calculate_distance(v.as_ref(), e.key()).unwrap_or(0.0));
				e.insert(ObjectProperties::new(parent_dist, id));
			}
		};
		// If (N will fit into N)
		if node.len() <= self.state.capacity as usize {
			let max_dist = self.compute_leaf_max_distance(&node, parent_center);
			#[cfg(debug_assertions)]
			debug!("NODE LEAF: {} - MAX_DIST: {:?}", node_id, max_dist);
			store.set_node(StoredNode::new(node.into_mtree_node(), node_id, node_key, 0), true)?;
			Ok(InsertionResult::CoveringRadius(max_dist))
		} else {
			// Else
			// Split (N)
			let (o1, p1, o2, p2) = self.split_node(store, node_id, node_key, node)?;
			Ok(InsertionResult::PromotedEntries(o1, p1, o2, p2))
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
		mut node: N,
	) -> Result<(SharedVector, RoutingProperties, SharedVector, RoutingProperties), Error>
	where
		N: NodeVectors + Debug,
	{
		#[cfg(debug_assertions)]
		debug!("Split node: {:?}", node);
		let mut a2 = node.get_objects();
		let (distances, o1, o2) = self.compute_distances_and_promoted_objects(&a2)?;

		// Distributed objects
		a2.sort_by(|o1, o2| {
			let d1 = *distances.0.get(&(o1.clone(), o1.clone())).unwrap_or(&0.0);
			let d2 = *distances.0.get(&(o2.clone(), o2.clone())).unwrap_or(&0.0);
			d1.total_cmp(&d2)
		});
		let a1_size = a2.len() / 2;
		let a1: Vec<Arc<Vector>> = a2.drain(0..a1_size).collect();

		let (node1, r1, o1) = node.extract_node(&distances, o1, a1)?;
		let (node2, r2, o2) = node.extract_node(&distances, o2, a2)?;

		// Create a new node
		let new_node_id = self.new_node_id();

		// Update the store/cache
		let n = StoredNode::new(node1.into_mtree_node(), node_id, node_key, 0);
		store.set_node(n, true)?;
		let n = store.new_node(new_node_id, node2.into_mtree_node())?;
		store.set_node(n, true)?;

		// Update the split node
		let p1 = RoutingProperties {
			node: node_id,
			radius: r1,
			parent_dist: 0.0,
		};
		let p2 = RoutingProperties {
			node: new_node_id,
			radius: r2,
			parent_dist: 0.0,
		};
		Ok((o1, p1, o2, p2))
	}

	// Compute the distance cache, and return the most distant objects
	fn compute_distances_and_promoted_objects(
		&self,
		objects: &[SharedVector],
	) -> Result<(DistanceCache, SharedVector, SharedVector), Error> {
		let mut promo = None;
		let mut max_dist = 0f64;
		let n = objects.len();
		let mut dist_cache = HashMap::with_capacity(n * 2);
		for (i, o1) in objects.iter().enumerate() {
			for o2 in objects.iter().take(n).skip(i + 1) {
				let distance = self.calculate_distance(o1, o2.as_ref())?;
				dist_cache.insert((o1.clone(), o2.clone()), distance);
				dist_cache.insert((o2.clone(), o1.clone()), distance); // Because the distance function is symmetric
				#[cfg(debug_assertions)]
				debug!("dist_cache {} ({:?} - {:?})", dist_cache.len(), o1, o2);
				if distance > max_dist {
					promo = Some((o1.clone(), o2.clone()));
					max_dist = distance;
				}
			}
		}
		#[cfg(debug_assertions)]
		assert_eq!(dist_cache.len(), n * n - n);
		match promo {
			None => Err(Error::Unreachable),
			Some((p1, p2)) => Ok((DistanceCache(dist_cache), p1, p2)),
		}
	}

	fn compute_internal_max_distance(&self, node: &InternalNode) -> f64 {
		let mut max_dist = 0f64;
		for p in node.values() {
			max_dist = max_dist.max(p.parent_dist + p.radius);
		}
		max_dist
	}

	fn compute_leaf_max_distance(&self, node: &LeafNode, parent: &Option<Arc<Vector>>) -> f64 {
		parent.as_ref().map_or(0.0, |p| {
			let mut max_dist = 0f64;
			for o in node.keys() {
				max_dist =
					max_dist.max(self.calculate_distance(p.as_ref(), o.as_ref()).unwrap_or(0.0));
			}
			max_dist
		})
	}

	fn calculate_distance(&self, v1: &Vector, v2: &Vector) -> Result<f64, Error> {
		match &self.distance {
			Distance::Euclidean => v1.euclidean_distance(v2),
			Distance::Manhattan => v1.manhattan_distance(v2),
			Distance::Cosine => v1.cosine_similarity(v2),
			Distance::Hamming => v1.hamming_distance(v2),
			Distance::Minkowski(order) => v1.minkowski_distance(v2, order),
		}
	}

	async fn delete(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		object: Vector,
		doc_id: DocId,
	) -> Result<bool, Error> {
		let mut deleted = false;
		if let Some(root_id) = self.state.root {
			let root_node = store.get_node(tx, root_id).await?;
			if let DeletionResult::Underflown(sn, n_updated) = self
				.delete_at_node(tx, store, root_node, &None, Arc::new(object), doc_id, &mut deleted)
				.await?
			{
				match &sn.n {
					MTreeNode::Internal(n) => match n.len() {
						0 => {
							store.remove_node(sn.id, sn.key)?;
							self.set_root(None);
							return Ok(deleted);
						}
						1 => {
							store.remove_node(sn.id, sn.key)?;
							let e = n.values().next().ok_or(Error::Unreachable)?;
							self.set_root(Some(e.node));
							return Ok(deleted);
						}
						_ => {}
					},
					MTreeNode::Leaf(n) => {
						if n.is_empty() {
							store.remove_node(sn.id, sn.key)?;
							self.set_root(None);
							return Ok(deleted);
						}
					}
				}
				store.set_node(sn, n_updated)?;
			}
		}
		Ok(deleted)
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	#[allow(clippy::too_many_arguments)]
	async fn delete_at_node(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		node: StoredNode<MTreeNode>,
		parent_center: &Option<Arc<Vector>>,
		object: SharedVector,
		id: DocId,
		deleted: &mut bool,
	) -> Result<DeletionResult, Error> {
		#[cfg(debug_assertions)]
		debug!("delete_at_node: {} {:?}", node.id, object);
		// Delete ( Od:LeafEntry, N:Node)
		match node.n {
			// If (N is a leaf)
			MTreeNode::Leaf(n) => {
				self.delete_node_leaf(
					store,
					node.id,
					node.key,
					n,
					parent_center,
					object,
					id,
					deleted,
				)
				.await
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
					deleted,
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
		od: SharedVector,
		id: DocId,
		deleted: &mut bool,
	) -> Result<DeletionResult, Error> {
		#[cfg(debug_assertions)]
		debug!("delete_node_internal: {} {:?}", node_id, od);
		let mut on_objs = Vec::new();
		let mut n_updated = false;
		// For each On E N
		for (on_obj, on_entry) in &n_node {
			let on_od_dist = self.calculate_distance(on_obj.as_ref(), od.as_ref())?;
			#[cfg(debug_assertions)]
			debug!("on_od_dist: {:?} / {} / {}", on_obj.as_ref(), on_od_dist, on_entry.radius);
			// If (d(Od, On) <= r(On))
			if on_od_dist <= on_entry.radius {
				on_objs.push((on_obj.clone(), on_entry.clone()));
			}
		}
		#[cfg(debug_assertions)]
		debug!("on_objs: {:?}", on_objs);
		for (on_obj, mut on_entry) in on_objs {
			#[cfg(debug_assertions)]
			debug!("on_obj: {:?}", on_obj.as_ref());
			// Delete (Od, child(On))
			let on_node = store.get_node(tx, on_entry.node).await?;
			#[cfg(debug_assertions)]
			let d_id = on_node.id;
			match self
				.delete_at_node(tx, store, on_node, &Some(on_obj.clone()), od.clone(), id, deleted)
				.await?
			{
				DeletionResult::NotFound => {
					#[cfg(debug_assertions)]
					debug!("delete_at_node {} => NotFound", d_id);
				}
				DeletionResult::DocRemoved => {
					#[cfg(debug_assertions)]
					debug!("delete_at_node {} => DocRemoved", d_id);
				}
				// Let r = returned covering radius
				DeletionResult::CoveringRadius(r) => {
					#[cfg(debug_assertions)]
					debug!("delete_at_node {} => CoveringRadius", d_id);
					// If (r > r(On))
					if r > on_entry.radius {
						// Let r(On) = r;
						on_entry.radius = r;
						n_node.insert(on_obj, on_entry);
						n_updated = true;
					}
				}
				DeletionResult::Underflown(sn, sn_updated) => {
					#[cfg(debug_assertions)]
					debug!("delete_at_node {} => Underflown", d_id);
					if self
						.deletion_underflown(
							tx,
							store,
							parent_center,
							&mut n_node,
							on_obj,
							sn,
							sn_updated,
						)
						.await?
					{
						n_updated = true;
						break;
					}
				}
			}
		}
		self.delete_node_internal_check_underflown(store, node_id, node_key, n_node, n_updated)
	}

	fn delete_node_internal_check_underflown(
		&mut self,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		n_node: InternalNode,
		n_updated: bool,
	) -> Result<DeletionResult, Error> {
		// If (N is underflown)
		if n_node.len() < self.minimum {
			// Return N
			return Ok(DeletionResult::Underflown(
				StoredNode::new(MTreeNode::Internal(n_node), node_id, node_key, 0),
				n_updated,
			));
		}
		// Return max(On E N) { parentDistance(On) + r(On)}
		let max_dist = self.compute_internal_max_distance(&n_node);
		Self::set_stored_node(store, node_id, node_key, n_node.into_mtree_node(), n_updated)?;
		Ok(DeletionResult::CoveringRadius(max_dist))
	}

	fn set_stored_node(
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		node: MTreeNode,
		updated: bool,
	) -> Result<(), Error> {
		store.set_node(StoredNode::new(node, node_id, node_key, 0), updated)
	}

	#[allow(clippy::too_many_arguments)]
	async fn deletion_underflown(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		parent_center: &Option<Arc<Vector>>,
		n_node: &mut InternalNode,
		on_obj: SharedVector,
		p: StoredNode<MTreeNode>,
		p_updated: bool,
	) -> Result<bool, Error> {
		#[cfg(debug_assertions)]
		debug!("deletion_underflown: {}", p.id);
		let min = f64::NAN;
		let mut onn = None;
		// Find node entry Onn € N, e <> 0, for which d(On, Onn) is a minimum
		for (onn_obj, onn_entry) in n_node.iter() {
			if onn_entry.node != p.id {
				let d = self.calculate_distance(on_obj.as_ref(), onn_obj.as_ref())?;
				if min.is_nan() || d < min {
					onn = Some((onn_obj.clone(), onn_entry.clone()));
				}
			}
		}
		#[cfg(debug_assertions)]
		debug!("deletion_underflown - p_id: {} - onn: {:?} - n_len: {}", p.id, onn, n_node.len());
		if let Some((onn_obj, onn_entry)) = onn {
			#[cfg(debug_assertions)]
			debug!("deletion_underflown: onn_entry {}", onn_entry.node);
			// Let S be the set of entries in child(Onn()
			let onn_child = store.get_node(tx, onn_entry.node).await?;
			// If (S U P) will fit into child(Onn)
			if onn_child.n.len() + p.n.len() <= self.state.capacity as usize {
				self.delete_underflown_fit_into_child(
					store, n_node, on_obj, p, onn_obj, onn_entry, onn_child,
				)
				.await?;
			} else {
				self.delete_underflown_redistribute(
					store,
					parent_center,
					n_node,
					on_obj,
					onn_obj,
					p,
					onn_child,
				)?;
			}
			return Ok(true);
		}
		store.set_node(p, p_updated)?;
		Ok(false)
	}

	#[allow(clippy::too_many_arguments)]
	async fn delete_underflown_fit_into_child(
		&mut self,
		store: &mut MTreeNodeStore,
		n_node: &mut InternalNode,
		on_obj: SharedVector,
		p: StoredNode<MTreeNode>,
		onn_obj: SharedVector,
		mut onn_entry: RoutingProperties,
		mut onn_child: StoredNode<MTreeNode>,
	) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		debug!("deletion_underflown - fit into: {}", onn_child.id);
		// Remove On from N;
		n_node.remove(&on_obj);
		match &mut onn_child.n {
			MTreeNode::Internal(s) => {
				let p_node = p.n.internal()?;
				// for each Op E P
				for (p_obj, mut p_entry) in p_node {
					// Let parentDistance(Op) = d(Op, Onn);
					p_entry.parent_dist =
						self.calculate_distance(p_obj.as_ref(), onn_obj.as_ref())?;
					// Add Op to S;
					s.insert(p_obj, p_entry);
				}
				// Let r(Onn) = max (Os E S) {parentDistance(Os) + r(Os)}
				let mut radius = 0.0;
				for s_entry in s.values() {
					let d = s_entry.parent_dist + s_entry.radius;
					if d > radius {
						radius = d;
					}
				}
				if onn_entry.radius != radius {
					onn_entry.radius = radius;
				}
				n_node.insert(onn_obj, onn_entry);
			}
			MTreeNode::Leaf(s) => {
				let p_node = p.n.leaf()?;
				// for each Op E P
				for (p_obj, mut p_entry) in p_node {
					// Let parentDistance(Op) = d(Op, Onn);
					p_entry.parent_dist =
						self.calculate_distance(p_obj.as_ref(), onn_obj.as_ref())?;
					// Add Op to S;
					s.insert(p_obj, p_entry);
				}
				// Let r(Onn) = max (Os E S) {parentDistance(Os)}
				let mut radius = 0.0;
				for s_entry in s.values() {
					if s_entry.parent_dist > radius {
						radius = s_entry.parent_dist;
					}
				}
				if onn_entry.radius != radius {
					onn_entry.radius = radius;
				}
				n_node.insert(onn_obj, onn_entry);
			}
		}
		store.remove_node(p.id, p.key)?;
		store.set_node(onn_child, true)?;
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	fn delete_underflown_redistribute(
		&mut self,
		store: &mut MTreeNodeStore,
		parent_center: &Option<Arc<Vector>>,
		n_node: &mut InternalNode,
		on_obj: SharedVector,
		onn_obj: SharedVector,
		mut p: StoredNode<MTreeNode>,
		onn_child: StoredNode<MTreeNode>,
	) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		debug!("deletion_underflown - delete_underflown_redistribute: {}", p.id);
		// Remove On and Onn from N;
		n_node.remove(&on_obj);
		n_node.remove(&onn_obj);
		// (S U P)
		p.n.merge(onn_child.n)?;
		// Split(S U P)
		let (o1, mut e1, o2, mut e2) = match p.n {
			MTreeNode::Internal(n) => self.split_node(store, p.id, p.key, n)?,
			MTreeNode::Leaf(n) => self.split_node(store, p.id, p.key, n)?,
		};
		e1.parent_dist = parent_center
			.as_ref()
			.map_or(0.0, |pd| self.calculate_distance(o1.as_ref(), pd.as_ref()).unwrap_or(0.0));
		e2.parent_dist = parent_center
			.as_ref()
			.map_or(0.0, |pd| self.calculate_distance(o2.as_ref(), pd.as_ref()).unwrap_or(0.0));
		// Add new child pointer entries to N;
		n_node.insert(o1, e1);
		n_node.insert(o2, e2);
		store.remove_node(onn_child.id, onn_child.key)?;
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn delete_node_leaf(
		&mut self,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		mut leaf_node: LeafNode,
		parent_center: &Option<Arc<Vector>>,
		od: SharedVector,
		id: DocId,
		deleted: &mut bool,
	) -> Result<DeletionResult, Error> {
		#[cfg(debug_assertions)]
		debug!("delete_node_leaf - n_id: {} - obj: {:?} - doc: {}", node_id, od, id);
		let mut entry_removed = false;
		// If (Od E N)
		if let Entry::Occupied(mut e) = leaf_node.entry(od) {
			let p = e.get_mut();
			// Remove Od from N
			if p.docs.remove(id) {
				*deleted = true;
				#[cfg(debug_assertions)]
				debug!("deleted - n_id: {} - doc: {}", node_id, id);
				if p.docs.is_empty() {
					e.remove();
					entry_removed = true;
				}
			}
		} else {
			let sn = StoredNode::new(MTreeNode::Leaf(leaf_node), node_id, node_key, 0);
			store.set_node(sn, false)?;
			return Ok(DeletionResult::NotFound);
		}
		if entry_removed {
			// If (N is underflown)
			if leaf_node.len() < self.minimum {
				return Ok(DeletionResult::Underflown(
					StoredNode::new(MTreeNode::Leaf(leaf_node), node_id, node_key, 0),
					true,
				));
			}
			// Return max(Ol E N) { parentDistance(Ol)};
			let max_dist = self.compute_leaf_max_distance(&leaf_node, parent_center);
			let sn = StoredNode::new(MTreeNode::Leaf(leaf_node), node_id, node_key, 0);
			store.set_node(sn, true)?;
			Ok(DeletionResult::CoveringRadius(max_dist))
		} else {
			let sn = StoredNode::new(MTreeNode::Leaf(leaf_node), node_id, node_key, 0);
			store.set_node(sn, true)?;
			Ok(DeletionResult::DocRemoved)
		}
	}

	async fn finish(&self, tx: &mut Transaction, key: Key) -> Result<(), Error> {
		if self.updated {
			tx.set(key, self.state.try_to_val()?).await?;
		}
		Ok(())
	}
}

struct DistanceCache(HashMap<(SharedVector, SharedVector), f64>);

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
struct PriorityResult(f64);

impl Eq for PriorityResult {}

impl PartialOrd for PriorityResult {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for PriorityResult {
	fn cmp(&self, other: &Self) -> Ordering {
		partial_cmp_f64(self.0, other.0).unwrap_or(Ordering::Equal)
	}
}

type MTreeNodeStore = TreeNodeStore<MTreeNode>;

type InternalMap = BTreeMap<SharedVector, RoutingProperties>;

type LeafMap = BTreeMap<SharedVector, ObjectProperties>;

#[derive(Debug)]
/// A node in this tree structure holds entries.
/// Each entry is a tuple consisting of an object and its associated properties.
/// It's essential to note that the properties vary between a LeafNode and an InternalNode.
/// Both LeafNodes and InternalNodes are implemented as a map.
/// In this map, the key is an object, and the values correspond to its properties.
/// In essence, an entry can be visualized as a tuple of the form (object, properties).
pub enum MTreeNode {
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

	fn merge(&mut self, other: MTreeNode) -> Result<(), Error> {
		match (self, other) {
			(MTreeNode::Internal(s), MTreeNode::Internal(o)) => {
				Self::merge_internal(s, o);
				Ok(())
			}
			(MTreeNode::Leaf(s), MTreeNode::Leaf(o)) => {
				Self::merge_leaf(s, o);
				Ok(())
			}
			(_, _) => Err(Error::Unreachable),
		}
	}

	fn merge_internal(n: &mut InternalNode, other: InternalNode) {
		for (o, p) in other {
			n.insert(o, p);
		}
	}

	fn merge_leaf(s: &mut LeafNode, o: LeafNode) {
		for (o, p) in o {
			match s.entry(o) {
				Entry::Occupied(mut e) => {
					e.get_mut().docs |= p.docs;
				}
				Entry::Vacant(e) => {
					e.insert(p);
				}
			}
		}
	}
}
trait NodeVectors: Sized {
	fn len(&self) -> usize;

	fn get_objects(&self) -> Vec<Arc<Vector>>;

	fn extract_node(
		&mut self,
		distances: &DistanceCache,
		p: SharedVector,
		a: Vec<Arc<Vector>>,
	) -> Result<(Self, f64, SharedVector), Error>;

	fn into_mtree_node(self) -> MTreeNode;
}

impl NodeVectors for LeafNode {
	fn len(&self) -> usize {
		self.len()
	}

	fn get_objects(&self) -> Vec<Arc<Vector>> {
		self.keys().cloned().collect()
	}

	fn extract_node(
		&mut self,
		distances: &DistanceCache,
		p: SharedVector,
		a: Vec<Arc<Vector>>,
	) -> Result<(Self, f64, SharedVector), Error> {
		let mut n = LeafNode::new();
		let mut r = 0f64;
		for o in a {
			let mut props = self.remove(&o).ok_or(Error::Unreachable)?;
			let dist = *distances.0.get(&(o.clone(), p.clone())).unwrap_or(&0f64);
			if dist > r {
				r = dist;
			}
			props.parent_dist = dist;
			n.insert(o, props);
		}
		Ok((n, r, p))
	}

	fn into_mtree_node(self) -> MTreeNode {
		MTreeNode::Leaf(self)
	}
}

impl NodeVectors for InternalNode {
	fn len(&self) -> usize {
		self.len()
	}

	fn get_objects(&self) -> Vec<Arc<Vector>> {
		self.keys().cloned().collect()
	}

	fn extract_node(
		&mut self,
		distances: &DistanceCache,
		p: SharedVector,
		a: Vec<Arc<Vector>>,
	) -> Result<(Self, f64, SharedVector), Error> {
		let mut n = InternalNode::new();
		let mut max_r = 0f64;
		for o in a {
			let mut props = self.remove(&o).ok_or(Error::Unreachable)?;
			let dist = *distances.0.get(&(o.clone(), p.clone())).unwrap_or(&0f64);
			let r = dist + props.radius;
			if r > max_r {
				max_r = r;
			}
			props.parent_dist = dist;
			n.insert(o, props);
		}
		Ok((n, max_r, p))
	}

	fn into_mtree_node(self) -> MTreeNode {
		MTreeNode::Internal(self)
	}
}

pub type InternalNode = InternalMap;
pub type LeafNode = LeafMap;

impl TreeNode for MTreeNode {
	fn try_from_val(val: Val) -> Result<Self, Error> {
		let mut c: Cursor<Vec<u8>> = Cursor::new(val);
		let node_type: u8 = bincode::deserialize_from(&mut c)?;
		match node_type {
			1u8 => {
				let objects: BTreeMap<SharedVector, ObjectProperties> =
					bincode::deserialize_from(c)?;
				Ok(MTreeNode::Leaf(objects))
			}
			2u8 => {
				let entries: BTreeMap<SharedVector, RoutingProperties> =
					bincode::deserialize_from(c)?;
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
pub struct MState {
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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct RoutingProperties {
	// Reference to the node
	node: NodeId,
	// Distance to its parent object
	parent_dist: f64,
	// Covering radius
	radius: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ObjectProperties {
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
	use rand::prelude::{SliceRandom, StdRng};
	use rand::SeedableRng;
	use std::collections::{BTreeMap, HashSet, VecDeque};
	use std::sync::Arc;

	use test_log::test;
	use tokio::sync::{Mutex, MutexGuard};

	use crate::idx::docids::DocId;
	use crate::idx::trees::mtree::{
		InternalMap, MState, MTree, MTreeNode, MTreeNodeStore, ObjectProperties,
	};
	use crate::idx::trees::store::{NodeId, TreeNodeProvider, TreeNodeStore, TreeStoreType};
	use crate::idx::trees::vector::{SharedVector, Vector};
	use crate::kvs::Datastore;
	use crate::kvs::LockType::*;
	use crate::kvs::Transaction;
	use crate::sql::index::Distance;

	async fn new_operation(
		ds: &Datastore,
		t: TreeStoreType,
	) -> (Arc<Mutex<TreeNodeStore<MTreeNode>>>, Transaction) {
		let s = TreeNodeStore::new(TreeNodeProvider::Debug, t, 20);
		let tx = ds.transaction(t.into(), Optimistic).await.unwrap();
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

	fn new_vec(n: i64) -> Vector {
		Vector::I64(vec![n])
	}

	#[test(tokio::test)]
	async fn test_mtree_insertions() {
		let mut t = MTree::new(MState::new(3), Distance::Euclidean);
		let ds = Datastore::new("memory").await.unwrap();

		let vec1 = new_vec(1);
		// First the index is empty
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.docs, vec![]);
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
				check_leaf_vec(m, &vec1, 0.0, &[1]);
			})
			.await;
			finish_operation(tx, s, true).await;
		}
		// Check KNN
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res.docs, vec![1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 1);
			check_tree_properties(&mut tx, &mut s, &t).await.check(1, 1, Some(1), Some(1), 1, 1);
		}

		// insert second element
		let vec2 = new_vec(2);
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
			check_knn(&res.docs, vec![1, 2]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 1);
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 0.0, &[2]);
			})
			.await;
			check_tree_properties(&mut tx, &mut s, &t).await.check(1, 1, Some(2), Some(2), 2, 2);
		}
		// vec2 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec2, 10).await.unwrap();
			check_knn(&res.docs, vec![2, 1]);
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
			check_knn(&res.docs, vec![2, 3, 1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 1);
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 0.0, &[2, 3]);
			})
			.await;
			check_tree_properties(&mut tx, &mut s, &t).await.check(1, 1, Some(2), Some(2), 2, 3);
		}

		// insert third vector
		let vec3 = new_vec(3);
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
			check_knn(&res.docs, vec![3, 2, 3, 1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 1);
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 0.0, &[2, 3]);
				check_leaf_vec(m, &vec3, 0.0, &[3]);
			})
			.await;
			check_tree_properties(&mut tx, &mut s, &t).await.check(1, 1, Some(3), Some(3), 3, 4);
		}

		// Check split leaf node
		let vec4 = new_vec(4);
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
			check_knn(&res.docs, vec![4, 3, 2, 3, 1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 3);
			assert_eq!(t.state.root, Some(2));
			check_internal(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, &vec1, 0.0, 0, 1.0);
				check_routing_vec(m, &vec4, 0.0, 1, 1.0);
			})
			.await;
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec3, 1.0, &[3]);
				check_leaf_vec(m, &vec4, 0.0, &[4]);
			})
			.await;
			check_tree_properties(&mut tx, &mut s, &t).await.check(3, 2, Some(2), Some(2), 4, 5);
		}

		// Insert vec extending the radius of the last node, calling compute_leaf_radius
		let vec6 = new_vec(6);
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
			check_knn(&res.docs, vec![6, 4, 3, 2, 3, 1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 3);
			assert_eq!(t.state.root, Some(2));
			check_internal(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, &vec1, 0.0, 0, 1.0);
				check_routing_vec(m, &vec4, 0.0, 1, 2.0);
			})
			.await;
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, &vec3, 1.0, &[3]);
				check_leaf_vec(m, &vec4, 0.0, &[4]);
				check_leaf_vec(m, &vec6, 2.0, &[6]);
			})
			.await;
			check_tree_properties(&mut tx, &mut s, &t).await.check(3, 2, Some(2), Some(3), 5, 6);
		}

		// Insert check split internal node

		// Insert vec8
		let vec8 = new_vec(8);
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec8.clone(), 8).await.unwrap();
			finish_operation(tx, s, true).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			check_tree_properties(&mut tx, &mut s, &t).await.check(4, 2, Some(2), Some(2), 6, 7);
			assert_eq!(t.state.root, Some(2));
			// Check Root node (level 1)
			check_internal(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 3);
				check_routing_vec(m, &vec1, 0.0, 0, 1.0);
				check_routing_vec(m, &vec3, 0.0, 1, 1.0);
				check_routing_vec(m, &vec8, 0.0, 3, 2.0);
			})
			.await;
			// Check level 2
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec3, 0.0, &[3]);
				check_leaf_vec(m, &vec4, 1.0, &[4]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 3, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec6, 2.0, &[6]);
				check_leaf_vec(m, &vec8, 0.0, &[8]);
			})
			.await;
		}

		let vec9 = new_vec(9);
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec9.clone(), 9).await.unwrap();
			finish_operation(tx, s, true).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			check_tree_properties(&mut tx, &mut s, &t).await.check(4, 2, Some(2), Some(3), 7, 8);
			assert_eq!(t.state.root, Some(2));
			// Check Root node (level 1)
			check_internal(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 3);
				check_routing_vec(m, &vec1, 0.0, 0, 1.0);
				check_routing_vec(m, &vec3, 0.0, 1, 1.0);
				check_routing_vec(m, &vec8, 0.0, 3, 2.0);
			})
			.await;
			// Check level 2
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec3, 0.0, &[3]);
				check_leaf_vec(m, &vec4, 1.0, &[4]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 3, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, &vec6, 2.0, &[6]);
				check_leaf_vec(m, &vec8, 0.0, &[8]);
				check_leaf_vec(m, &vec9, 1.0, &[9]);
			})
			.await;
		}

		let vec10 = new_vec(10);
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec10.clone(), 10).await.unwrap();
			finish_operation(tx, s, true).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			check_tree_properties(&mut tx, &mut s, &t).await.check(7, 3, Some(2), Some(2), 8, 9);
			assert_eq!(t.state.root, Some(6));
			// Check Root node (level 1)
			check_internal(&mut tx, &mut s, 6, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, &vec1, 0.0, 2, 3.0);
				check_routing_vec(m, &vec10, 0.0, 5, 6.0);
			})
			.await;
			// Check level 2
			check_internal(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, &vec1, 0.0, 0, 1.0);
				check_routing_vec(m, &vec3, 2.0, 1, 1.0);
			})
			.await;
			check_internal(&mut tx, &mut s, 5, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, &vec6, 4.0, 3, 2.0);
				check_routing_vec(m, &vec10, 0.0, 4, 1.0);
			})
			.await;
			// Check level 3
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec3, 0.0, &[3]);
				check_leaf_vec(m, &vec4, 1.0, &[4]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 3, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec6, 0.0, &[6]);
				check_leaf_vec(m, &vec8, 2.0, &[8]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 4, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec9, 1.0, &[9]);
				check_leaf_vec(m, &vec10, 0.0, &[10]);
			})
			.await;
		}

		// vec8 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec8, 20).await.unwrap();
			check_knn(&res.docs, vec![8, 9, 6, 10, 4, 3, 2, 3, 1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 7);
		}
		// vec4 knn(2)
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec4, 2).await.unwrap();
			check_knn(&res.docs, vec![4, 3]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 7);
		}

		// vec10 knn(2)
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let res = t.knn_search(&mut tx, &mut s, &vec10, 2).await.unwrap();
			check_knn(&res.docs, vec![10, 9]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes, 7);
		}
	}

	async fn test_mtree_collection(collection: Vec<(DocId, Vector)>, is_unique: bool) {
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = MTree::new(MState::new(3), Distance::Euclidean);

		// Insert
		for (doc_id, obj) in &collection {
			{
				let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
				let mut s = s.lock().await;
				t.insert(&mut tx, &mut s, obj.clone(), *doc_id).await.unwrap();
				finish_operation(tx, s, true).await;
			}
			{
				let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
				let mut s = s.lock().await;
				check_tree_properties(&mut tx, &mut s, &t).await;
			}
		}

		// Find
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let max_knn = 20.max(collection.len());
			for (doc_id, obj) in &collection {
				for knn in 1..max_knn {
					let res = t.knn_search(&mut tx, &mut s, obj, knn).await.unwrap();
					if is_unique {
						assert!(
							res.docs.contains(doc_id),
							"Search: {:?} - Knn: {} - Expected: {} - Got: {:?}",
							obj,
							knn,
							doc_id,
							res.docs
						);
					}
					let expected_len = collection.len().min(knn);
					assert_eq!(
						expected_len,
						res.docs.len(),
						"Wrong knn count - Expected: {} - Got: {} - Collection: {}",
						expected_len,
						res.docs.len(),
						collection.len(),
					)
				}
			}
		}

		// Deletion
		for (doc_id, obj) in &collection {
			{
				debug!("### Remove {} {:?}", doc_id, obj);
				let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
				let mut s = s.lock().await;
				assert!(
					t.delete(&mut tx, &mut s, obj.clone(), *doc_id).await.unwrap(),
					"Delete failed: {} {:?}",
					doc_id,
					obj
				);
				finish_operation(tx, s, true).await;
			}
			{
				let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
				let mut s = s.lock().await;
				let res = t.knn_search(&mut tx, &mut s, obj, 1).await.unwrap();
				assert!(!res.docs.contains(doc_id), "Found: {} {:?}", doc_id, obj);
			}
			{
				let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
				let mut s = s.lock().await;
				check_tree_properties(&mut tx, &mut s, &t).await;
			}
		}

		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			check_tree_properties(&mut tx, &mut s, &t).await.check(0, 0, None, None, 0, 0);
		}
	}

	async fn test_mtree_unique_and_sorted(size: usize) {
		let mut collection = vec![];

		// Prepare data set
		for doc_id in 0..size as DocId {
			collection.push((doc_id, new_vec(doc_id as i64)));
		}

		test_mtree_collection(collection, true).await;
	}

	#[test(tokio::test)]
	async fn test_mtree_unique_and_sorted_small() {
		for i in 0..30 {
			test_mtree_unique_and_sorted(i).await;
		}
	}

	#[test(tokio::test)]
	async fn test_mtree_unique_and_sorted_large() {
		test_mtree_unique_and_sorted(200).await;
	}

	async fn test_mtree_unique_and_shuffled(size: usize) {
		let mut collection = vec![];

		// Prepare data set
		for doc_id in 0..size as DocId {
			collection.push((doc_id, new_vec(doc_id as i64)));
		}

		// Shuffle
		let mut rng = get_seed_rnd();
		collection.shuffle(&mut rng);

		test_mtree_collection(collection, true).await;
	}

	#[test(tokio::test)]
	async fn test_mtree_unique_and_shuffled_small() {
		for i in 0..30 {
			test_mtree_unique_and_shuffled(i).await;
		}
	}

	#[test(tokio::test)]
	async fn test_mtree_unique_and_shuffled_large() {
		test_mtree_unique_and_shuffled(200).await;
	}

	async fn test_mtree_non_unique_and_sorted(size: usize) {
		let mut collection = vec![];

		// Prepare data set
		for doc_id in 0..size {
			let obj = doc_id % (size / 2);
			collection.push((doc_id as DocId, new_vec(obj as i64)));
		}

		test_mtree_collection(collection, false).await;
	}

	#[test(tokio::test)]
	async fn test_mtree_non_unique_and_sorted_small() {
		for i in 2..30 {
			test_mtree_non_unique_and_sorted(i).await;
		}
	}

	#[test(tokio::test)]
	async fn test_mtree_non_unique_and_sorted_large() {
		test_mtree_non_unique_and_sorted(200).await;
	}

	async fn test_mtree_non_unique_and_shuffled(size: usize) {
		let mut collection = vec![];

		// Prepare data set
		for doc_id in 0..size {
			let obj = doc_id % (size / 2);
			collection.push((doc_id as DocId, new_vec(obj as i64)));
		}

		// Shuffle
		let mut rng = get_seed_rnd();
		collection.shuffle(&mut rng);

		test_mtree_collection(collection, false).await;
	}

	#[test(tokio::test)]
	async fn test_mtree_non_unique_and_shuffled_small() {
		for i in 2..30 {
			test_mtree_non_unique_and_shuffled(i).await;
		}
	}

	#[test(tokio::test)]
	async fn test_mtree_non_unique_and_shuffled_large() {
		test_mtree_non_unique_and_shuffled(200).await;
	}

	fn check_leaf_vec(
		m: &BTreeMap<SharedVector, ObjectProperties>,
		obj: &Vector,
		parent_dist: f64,
		docs: &[DocId],
	) {
		let p = m.get(obj).unwrap();
		assert_eq!(p.docs.len(), docs.len() as u64);
		for doc in docs {
			assert!(p.docs.contains(*doc));
		}
		assert_eq!(p.parent_dist, parent_dist);
	}

	fn check_routing_vec(
		m: &InternalMap,
		center: &Vector,
		parent_dist: f64,
		node_id: NodeId,
		radius: f64,
	) {
		let p = m.get(center).unwrap();
		assert_eq!(parent_dist, p.parent_dist);
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
		F: FnOnce(&BTreeMap<SharedVector, ObjectProperties>),
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
		F: FnOnce(&InternalMap),
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

	fn check_knn(res: &VecDeque<DocId>, expected: Vec<DocId>) {
		let expected: VecDeque<DocId> = expected.into_iter().collect();
		assert_eq!(res, &expected);
	}

	#[derive(Default, Debug)]
	struct CheckedProperties {
		node_count: usize,
		max_depth: usize,
		min_leaf_depth: Option<usize>,
		max_leaf_depth: Option<usize>,
		min_objects: Option<usize>,
		max_objects: Option<usize>,
		object_count: usize,
		doc_count: usize,
	}

	impl CheckedProperties {
		fn check(
			&self,
			expected_node_count: usize,
			expected_depth: usize,
			expected_min_objects: Option<usize>,
			expected_max_objects: Option<usize>,
			expected_object_count: usize,
			expected_doc_count: usize,
		) {
			assert_eq!(self.node_count, expected_node_count, "Node count - {:?}", self);
			assert_eq!(self.max_depth, expected_depth, "Max depth - {:?}", self);
			let expected_leaf_depth = if expected_depth == 0 {
				None
			} else {
				Some(expected_depth)
			};
			assert_eq!(self.min_leaf_depth, expected_leaf_depth, "Min leaf depth - {:?}", self);
			assert_eq!(self.max_leaf_depth, expected_leaf_depth, "Max leaf depth - {:?}", self);
			assert_eq!(self.min_objects, expected_min_objects, "Min objects - {:?}", self);
			assert_eq!(self.max_objects, expected_max_objects, "Max objects - {:?}", self);
			assert_eq!(self.object_count, expected_object_count, "Object count- {:?}", self);
			assert_eq!(self.doc_count, expected_doc_count, "Doc count - {:?}", self);
		}
	}

	async fn check_tree_properties(
		tx: &mut Transaction,
		s: &mut MTreeNodeStore,
		t: &MTree,
	) -> CheckedProperties {
		debug!("CheckTreeProperties");
		let mut checks = CheckedProperties::default();
		let mut nodes: VecDeque<(NodeId, f64, Option<Arc<Vector>>, usize)> = VecDeque::new();
		if let Some(root_id) = t.state.root {
			nodes.push_back((root_id, 0.0, None, 1));
		}
		let mut leaf_objects = HashSet::new();
		while let Some((node_id, radius, center, depth)) = nodes.pop_front() {
			checks.node_count += 1;
			if depth > checks.max_depth {
				checks.max_depth = depth;
			}
			let node = s.get_node(tx, node_id).await.unwrap();
			debug!(
				"Node id: {} - depth: {} - len: {} - {:?}",
				node.id,
				depth,
				node.n.len(),
				node.n
			);
			assert_ne!(node.n.len(), 0, "Empty node! {}", node.id);
			if Some(node_id) != t.state.root {
				assert!(
					node.n.len() >= t.minimum && node.n.len() <= t.state.capacity as usize,
					"Wrong node size - Node: {} - Size: {}",
					node_id,
					node.n.len()
				);
			}
			match node.n {
				MTreeNode::Internal(entries) => {
					let next_depth = depth + 1;
					entries.iter().for_each(|(o, p)| {
						if let Some(center) = center.as_ref() {
							let pd = t.calculate_distance(center.as_ref(), o.as_ref()).unwrap();
							assert_eq!(pd, p.parent_dist, "Incorrect parent distance");
							assert!(pd + p.radius <= radius);
						}
						nodes.push_back((p.node, p.radius, Some(o.clone()), next_depth))
					});
				}
				MTreeNode::Leaf(m) => {
					checks.object_count += m.len();
					update_min(&mut checks.min_objects, m.len());
					update_max(&mut checks.max_objects, m.len());
					update_min(&mut checks.min_leaf_depth, depth);
					update_max(&mut checks.max_leaf_depth, depth);
					for (o, p) in m {
						if !leaf_objects.insert(o.clone()) {
							panic!("Leaf object already exists: {:?}", o);
						}
						if let Some(center) = center.as_ref() {
							let pd = t.calculate_distance(center.as_ref(), o.as_ref()).unwrap();
							assert_eq!(pd, p.parent_dist);
						}
						checks.doc_count += p.docs.len() as usize;
					}
				}
			}
		}
		checks
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

	fn get_seed_rnd() -> StdRng {
		let seed: u64 = std::env::var("TEST_SEED")
			.unwrap_or_else(|_| rand::random::<u64>().to_string())
			.parse()
			.expect("Failed to parse seed");
		info!("Seed: {}", seed);
		// Create a seeded RNG
		StdRng::seed_from_u64(seed)
	}
}
