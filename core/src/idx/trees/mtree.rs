use std::cmp::{Ordering, Reverse};
use std::collections::btree_map::Entry;
#[cfg(debug_assertions)]
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::sync::Arc;

use async_recursion::async_recursion;
use revision::revisioned;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::err::Error;

use crate::idx::docids::{DocId, DocIds};
use crate::idx::trees::btree::BStatistics;
use crate::idx::trees::store::{
	IndexStores, NodeId, StoredNode, TreeNode, TreeNodeProvider, TreeStore,
};
use crate::idx::trees::vector::{SharedVector, Vector};
use crate::idx::{IndexKeyBase, VersionedSerdeState};
use crate::kvs::{Key, Transaction, TransactionType, Val};
use crate::sql::index::{Distance, MTreeParams, VectorType};
use crate::sql::{Array, Object, Thing, Value};

pub(crate) struct MTreeIndex {
	state_key: Key,
	dim: usize,
	vector_type: VectorType,
	store: MTreeStore,
	doc_ids: Arc<RwLock<DocIds>>,
	mtree: Arc<RwLock<MTree>>,
}

impl MTreeIndex {
	pub(crate) async fn new(
		ixs: &IndexStores,
		tx: &mut Transaction,
		ikb: IndexKeyBase,
		p: &MTreeParams,
		tt: TransactionType,
	) -> Result<Self, Error> {
		let doc_ids = Arc::new(RwLock::new(
			DocIds::new(ixs, tx, tt, ikb.clone(), p.doc_ids_order, p.doc_ids_cache).await?,
		));
		let state_key = ikb.new_vm_key(None);
		let state: MState = if let Some(val) = tx.get(state_key.clone()).await? {
			MState::try_from_val(val)?
		} else {
			MState::new(p.capacity)
		};
		let store = ixs
			.get_store_mtree(
				TreeNodeProvider::Vector(ikb),
				state.generation,
				tt,
				p.mtree_cache as usize,
			)
			.await;
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
		let mut mtree = self.mtree.write().await;
		for v in content {
			// Extract the vector
			let vector = self.extract_vector(v)?.into();
			mtree.insert(tx, &mut self.store, vector, doc_id).await?;
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
		let res = self.mtree.read().await.knn_search(tx, &self.store, &vector, k).await?;
		Ok(res.docs)
	}

	fn check_vector_array(&self, a: Array) -> Result<SharedVector, Error> {
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
		Ok(vec.into())
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
			_ => Err(Error::InvalidVectorValue(value.clone().to_raw_string())),
		}
	}

	pub(crate) async fn remove_document(
		&mut self,
		tx: &mut Transaction,
		rid: &Thing,
		content: Vec<Value>,
	) -> Result<(), Error> {
		if let Some(doc_id) = self.doc_ids.write().await.remove_doc(tx, rid.into()).await? {
			let mut mtree = self.mtree.write().await;
			for v in content {
				// Extract the vector
				let vector = self.extract_vector(v)?.into();
				mtree.delete(tx, &mut self.store, vector, doc_id).await?;
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

	pub(crate) async fn finish(&mut self, tx: &mut Transaction) -> Result<(), Error> {
		self.doc_ids.write().await.finish(tx).await?;
		let mut mtree = self.mtree.write().await;
		if self.store.finish(tx).await? {
			mtree.state.generation += 1;
			tx.set(self.state_key.clone(), mtree.state.try_to_val()?).await?;
		}
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
				for doc in docs {
					self.docs.insert(doc);
				}
				e.insert(docs.clone());
			}
			Entry::Occupied(mut e) => {
				let d = e.get_mut();
				for doc in docs {
					d.insert(doc);
					self.docs.insert(doc);
				}
			}
		}

		#[cfg(debug_assertions)]
		debug!("KnnResult add - dist: {} - docs: {:?} - total: {}", dist, docs, self.docs.len());
		debug!("{:?}", self.priority_list);

		// Do possible eviction
		let docs_len = self.docs.len();
		if docs_len > self.knn {
			if let Some((_, d)) = self.priority_list.last_key_value() {
				if docs_len - d.len() >= self.knn {
					if let Some((_, evicted_docs)) = self.priority_list.pop_last() {
						self.docs -= evicted_docs;
					}
				}
			}
		}
	}

	fn build(self, #[cfg(debug_assertions)] visited_nodes: HashMap<NodeId, usize>) -> KnnResult {
		let mut sorted_docs = VecDeque::with_capacity(self.knn as usize);
		#[cfg(debug_assertions)]
		debug!("self.priority_list: {:?} - self.docs: {:?}", self.priority_list, self.docs);
		let mut left = self.knn;
		for (_, docs) in self.priority_list {
			let dl = docs.len();
			if dl > left {
				for doc_id in docs.iter().take(left as usize) {
					sorted_docs.push_back(doc_id);
				}
				break;
			}
			for doc_id in docs {
				sorted_docs.push_back(doc_id);
			}
			left -= dl;
			// We don't expect anymore result, we can leave
			if left == 0 {
				break;
			}
		}
		debug!("sorted_docs: {:?}", sorted_docs);
		KnnResult {
			docs: sorted_docs,
			#[cfg(debug_assertions)]
			visited_nodes,
		}
	}
}

#[non_exhaustive]
pub struct KnnResult {
	docs: VecDeque<DocId>,
	#[cfg(debug_assertions)]
	#[allow(dead_code)]
	visited_nodes: HashMap<NodeId, usize>,
}

// https://en.wikipedia.org/wiki/M-tree
// https://arxiv.org/pdf/1004.4216.pdf
#[non_exhaustive]
pub struct MTree {
	state: MState,
	distance: Distance,
	minimum: usize,
}

impl MTree {
	pub fn new(state: MState, distance: Distance) -> Self {
		let minimum = (state.capacity + 1) as usize / 2;
		Self {
			state,
			distance,
			minimum,
		}
	}

	pub async fn knn_search(
		&self,
		tx: &mut Transaction,
		store: &MTreeStore,
		v: &SharedVector,
		k: usize,
	) -> Result<KnnResult, Error> {
		#[cfg(debug_assertions)]
		debug!("knn_search - v: {:?} - k: {}", v, k);
		let mut queue = BinaryHeap::new();
		let mut res = KnnResultBuilder::new(k);
		if let Some(root_id) = self.state.root {
			queue.push(Reverse(PriorityNode(0.0, root_id)));
		}
		#[cfg(debug_assertions)]
		let mut visited_nodes = HashMap::new();
		while let Some(current) = queue.pop() {
			let node = store.get_node(tx, current.0 .1).await?;
			#[cfg(debug_assertions)]
			{
				debug!("Visit node id: {} - dist: {}", current.0 .1, current.0 .1);
				if visited_nodes.insert(current.0 .1, node.n.len()).is_some() {
					return Err(Error::Unreachable("MTree::knn_search"));
				}
			}
			match node.n {
				MTreeNode::Leaf(ref n) => {
					#[cfg(debug_assertions)]
					debug!("Leaf found - id: {} - len: {}", node.id, n.len(),);
					for (o, p) in n {
						let d = self.calculate_distance(o, v)?;
						if res.check_add(d) {
							#[cfg(debug_assertions)]
							debug!("Add: {d} - obj: {o:?} - docs: {:?}", p.docs);
							res.add(d, &p.docs);
						}
					}
				}
				MTreeNode::Internal(ref n) => {
					#[cfg(debug_assertions)]
					debug!("Internal found - id: {} - {:?}", node.id, n);
					for (o, p) in n {
						let d = self.calculate_distance(o, v)?;
						let min_dist = (d - p.radius).max(0.0);
						if res.check_add(min_dist) {
							debug!("Queue add - dist: {} - node: {}", min_dist, p.node);
							queue.push(Reverse(PriorityNode(min_dist, p.node)));
						}
					}
				}
			}
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
	Underflown(MStoredNode, bool),
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
		store: &mut MTreeStore,
		obj: SharedVector,
		id: DocId,
	) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		debug!("Insert - obj: {:?} - doc: {}", obj, id);
		// First we check if we already have the object. In this case we just append the doc.
		if self.append(tx, store, &obj, id).await? {
			return Ok(());
		}
		if let Some(root_id) = self.state.root {
			let node = store.get_node_mut(tx, root_id).await?;
			// Otherwise, we insert the object with possibly mutating the tree
			if let InsertionResult::PromotedEntries(o1, p1, o2, p2) =
				self.insert_at_node(tx, store, node, &None, obj, id).await?
			{
				self.create_new_internal_root(store, o1, p1, o2, p2).await?;
			}
		} else {
			self.create_new_leaf_root(store, obj, id).await?;
		}
		Ok(())
	}

	async fn create_new_leaf_root(
		&mut self,
		store: &mut MTreeStore,
		obj: SharedVector,
		id: DocId,
	) -> Result<(), Error> {
		let new_root_id = self.new_node_id();
		let p = ObjectProperties::new_root(id);
		let mut objects = LeafMap::new();
		objects.insert(obj, p);
		let new_root_node = store.new_node(new_root_id, MTreeNode::Leaf(objects))?;
		store.set_node(new_root_node, true).await?;
		self.set_root(Some(new_root_id));
		Ok(())
	}

	async fn create_new_internal_root(
		&mut self,
		store: &mut MTreeStore,
		o1: SharedVector,
		p1: RoutingProperties,
		o2: SharedVector,
		p2: RoutingProperties,
	) -> Result<(), Error> {
		let new_root_id = self.new_node_id();
		#[cfg(debug_assertions)]
		debug!(
			"New internal root - node: {} - e1.node: {} - e1.obj: {:?} - e1.radius: {} - e2.node: {} - e2.obj: {:?} - e2.radius: {}",
			new_root_id,
			p1.node,
			o1,
			p1.radius,
			p2.node,
			o2,
			p2.radius
		);
		let mut entries = InternalMap::new();
		entries.insert(o1, p1);
		entries.insert(o2, p2);
		let new_root_node = store.new_node(new_root_id, MTreeNode::Internal(entries))?;
		store.set_node(new_root_node, true).await?;
		self.set_root(Some(new_root_id));
		Ok(())
	}

	async fn append(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeStore,
		object: &SharedVector,
		id: DocId,
	) -> Result<bool, Error> {
		let mut queue = BinaryHeap::new();
		if let Some(root_id) = self.state.root {
			queue.push(root_id);
		}
		while let Some(current) = queue.pop() {
			let mut node = store.get_node_mut(tx, current).await?;
			match node.n {
				MTreeNode::Leaf(ref mut n) => {
					if let Some(p) = n.get_mut(object) {
						p.docs.insert(id);
						store.set_node(node, true).await?;
						return Ok(true);
					}
				}
				MTreeNode::Internal(ref n) => {
					for (o, p) in n {
						let d = self.calculate_distance(o, object)?;
						if d <= p.radius {
							queue.push(p.node);
						}
					}
				}
			}
			store.set_node(node, false).await?;
		}
		Ok(false)
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(? Send))]
	async fn insert_at_node(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeStore,
		node: MStoredNode,
		parent_center: &Option<SharedVector>,
		object: SharedVector,
		doc: DocId,
	) -> Result<InsertionResult, Error> {
		#[cfg(debug_assertions)]
		debug!("insert_at_node - node: {} - doc: {} - obj: {:?}", node.id, doc, object);
		match node.n {
			// If (N is a leaf)
			MTreeNode::Leaf(n) => {
				self.insert_node_leaf(store, node.id, node.key, n, parent_center, object, doc).await
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
					doc,
				)
				.await
			}
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn insert_node_internal(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeStore,
		node_id: NodeId,
		node_key: Key,
		mut node: InternalNode,
		parent_center: &Option<SharedVector>,
		object: SharedVector,
		doc_id: DocId,
	) -> Result<InsertionResult, Error> {
		// Choose `best` subtree entry ObestSubstree from N;
		let (best_entry_obj, mut best_entry) = self.find_closest(&node, &object)?;
		let best_node = store.get_node_mut(tx, best_entry.node).await?;
		// Insert(Oi, child(ObestSubstree), ObestSubtree);
		match self
			.insert_at_node(tx, store, best_node, &Some(best_entry_obj.clone()), object, doc_id)
			.await?
		{
			// If (entry returned)
			InsertionResult::PromotedEntries(o1, mut p1, o2, mut p2) => {
				#[cfg(debug_assertions)]
				debug!(
					"Promote to Node ID: {} - e1.node: {} - e1.obj: {:?} - e1.radius: {} - e2.node: {} - e2.obj: {:?} - e2.radius: {} ",
					node_id, p1.node, o1, p1.radius, p2.node, o2, p2.radius
				);
				// Remove ObestSubtree from N;
				node.remove(&best_entry_obj);
				// if (N U P will fit into N)
				let mut nup: BTreeSet<SharedVector> = BTreeSet::from_iter(node.keys().cloned());
				nup.insert(o1.clone());
				nup.insert(o2.clone());
				if nup.len() <= self.state.capacity as usize {
					// Let parentDistance(Op) = d(Op, parent(N));
					if let Some(pc) = parent_center {
						p1.parent_dist = self.calculate_distance(&o1, pc)?;
						p2.parent_dist = self.calculate_distance(&o2, pc)?;
					} else {
						p1.parent_dist = 0.0;
						p2.parent_dist = 0.0;
					}
					node.insert(o1, p1);
					node.insert(o2, p2);
					let max_dist = self.compute_internal_max_distance(&node);
					Self::set_stored_node(store, node_id, node_key, node.into_mtree_node(), true)
						.await?;
					Ok(InsertionResult::CoveringRadius(max_dist))
				} else {
					node.insert(o1, p1);
					node.insert(o2, p2);
					// Split(N U P)
					let (o1, p1, o2, p2) = self.split_node(store, node_id, node_key, node).await?;
					Ok(InsertionResult::PromotedEntries(o1, p1, o2, p2))
				}
			}
			InsertionResult::DocAdded => {
				store
					.set_node(StoredNode::new(node.into_mtree_node(), node_id, node_key, 0), false)
					.await?;
				Ok(InsertionResult::DocAdded)
			}
			InsertionResult::CoveringRadius(covering_radius) => {
				let mut updated = false;
				if covering_radius > best_entry.radius {
					#[cfg(debug_assertions)]
					debug!(
						"NODE: {} - BE_OBJ: {:?} - BE_RADIUS: {} -> {}",
						node_id, best_entry_obj, best_entry.radius, covering_radius
					);
					best_entry.radius = covering_radius;
					node.insert(best_entry_obj, best_entry);
					updated = true;
				}
				let max_dist = self.compute_internal_max_distance(&node);
				#[cfg(debug_assertions)]
				debug!("NODE INTERNAL: {} - MAX_DIST: {:?}", node_id, max_dist);
				store
					.set_node(
						StoredNode::new(node.into_mtree_node(), node_id, node_key, 0),
						updated,
					)
					.await?;
				Ok(InsertionResult::CoveringRadius(max_dist))
			}
		}
	}

	fn find_closest(
		&self,
		node: &InternalNode,
		object: &SharedVector,
	) -> Result<(SharedVector, RoutingProperties), Error> {
		let mut closest = None;
		let mut dist = f64::MAX;
		for (o, p) in node {
			let d = self.calculate_distance(o, object)?;
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
			Err(Error::Unreachable("MTree::find_closest"))
		}
	}

	#[allow(clippy::too_many_arguments)]
	async fn insert_node_leaf(
		&mut self,
		store: &mut MTreeStore,
		node_id: NodeId,
		node_key: Key,
		mut node: LeafNode,
		parent_center: &Option<SharedVector>,
		object: SharedVector,
		doc_id: DocId,
	) -> Result<InsertionResult, Error> {
		match node.entry(object) {
			Entry::Occupied(mut e) => {
				e.get_mut().docs.insert(doc_id);
				store
					.set_node(StoredNode::new(node.into_mtree_node(), node_id, node_key, 0), true)
					.await?;
				return Ok(InsertionResult::DocAdded);
			}
			// Add Oi to N
			Entry::Vacant(e) => {
				// Let parentDistance(Oi) = d(Oi, parent(N))
				let parent_dist = if let Some(pc) = parent_center {
					self.calculate_distance(pc, e.key())?
				} else {
					0.0
				};
				e.insert(ObjectProperties::new(parent_dist, doc_id));
			}
		};
		// If (N will fit into N)
		if node.len() <= self.state.capacity as usize {
			let max_dist = self.compute_leaf_max_distance(&node, parent_center)?;
			#[cfg(debug_assertions)]
			debug!("NODE LEAF: {} - MAX_DIST: {:?}", node_id, max_dist);
			store
				.set_node(StoredNode::new(node.into_mtree_node(), node_id, node_key, 0), true)
				.await?;
			Ok(InsertionResult::CoveringRadius(max_dist))
		} else {
			// Else
			// Split (N)
			let (o1, p1, o2, p2) = self.split_node(store, node_id, node_key, node).await?;
			Ok(InsertionResult::PromotedEntries(o1, p1, o2, p2))
		}
	}

	fn set_root(&mut self, new_root: Option<NodeId>) {
		#[cfg(debug_assertions)]
		debug!("SET_ROOT: {:?}", new_root);
		self.state.root = new_root;
	}

	async fn split_node<N>(
		&mut self,
		store: &mut MTreeStore,
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
		let (distances, p1, p2) = self.compute_distances_and_promoted_objects(&a2)?;

		// Distributed objects
		a2.sort_by(|o1, o2| {
			let d1 = *distances.0.get(&(p1.clone(), o1.clone())).unwrap_or(&0.0);
			let d2 = *distances.0.get(&(p1.clone(), o2.clone())).unwrap_or(&0.0);
			d1.total_cmp(&d2)
		});
		let a1_size = a2.len() / 2;
		let a1: Vec<SharedVector> = a2.drain(0..a1_size).collect();

		let (node1, r1, o1) = node.extract_node(&distances, p1, a1)?;
		let (node2, r2, o2) = node.extract_node(&distances, p2, a2)?;

		// Create a new node
		let new_node_id = self.new_node_id();

		// Update the store/cache
		let n = StoredNode::new(node1.into_mtree_node(), node_id, node_key, 0);
		store.set_node(n, true).await?;
		let n = store.new_node(new_node_id, node2.into_mtree_node())?;
		store.set_node(n, true).await?;

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

		#[cfg(debug_assertions)]
		if p1.node == p2.node {
			return Err(Error::Unreachable("MTree::split_node"));
		}
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
		let mut dist_cache = BTreeMap::new();
		for (i, o1) in objects.iter().enumerate() {
			for o2 in objects.iter().take(n).skip(i + 1) {
				let distance = self.calculate_distance(o1, o2)?;
				dist_cache.insert((o1.clone(), o2.clone()), distance);
				dist_cache.insert((o2.clone(), o1.clone()), distance); // Because the distance function is symmetric
				#[cfg(debug_assertions)]
				{
					// Check that the distance is commutative
					assert_eq!(self.calculate_distance(o2, o1)?, distance);
					debug!(
						"dist_cache - len: {} - dist: {} - o1: {:?} - o2: {:?})",
						dist_cache.len(),
						distance,
						o1,
						o2
					);
				}
				if distance > max_dist {
					promo = Some((o1.clone(), o2.clone()));
					max_dist = distance;
				}
			}
		}
		#[cfg(debug_assertions)]
		{
			debug!("Promo: {:?}", promo);
			assert_eq!(dist_cache.len(), n * n - n);
		}
		match promo {
			None => Err(Error::Unreachable("MTree::compute_distances_and_promoted_objects")),
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

	fn compute_leaf_max_distance(
		&self,
		node: &LeafNode,
		parent: &Option<SharedVector>,
	) -> Result<f64, Error> {
		Ok(if let Some(p) = parent {
			let mut max_dist = 0f64;
			for o in node.keys() {
				max_dist = max_dist.max(self.calculate_distance(p, o)?);
			}
			max_dist
		} else {
			0.0
		})
	}

	fn calculate_distance(&self, v1: &SharedVector, v2: &SharedVector) -> Result<f64, Error> {
		if v1.eq(v2) {
			return Ok(0.0);
		}
		let dist = match &self.distance {
			Distance::Euclidean => v1.euclidean_distance(v2)?,
			Distance::Cosine => v1.cosine_distance(v2),
			Distance::Manhattan => v1.manhattan_distance(v2)?,
			Distance::Minkowski(order) => v1.minkowski_distance(v2, order)?,
			_ => return Err(Error::UnsupportedDistance(self.distance.clone())),
		};
		if dist.is_finite() {
			Ok(dist)
		} else {
			Err(Error::InvalidVectorDistance {
				left: v1.clone(),
				right: v2.clone(),
				dist,
			})
		}
	}

	async fn delete(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeStore,
		object: SharedVector,
		doc_id: DocId,
	) -> Result<bool, Error> {
		let mut deleted = false;
		if let Some(root_id) = self.state.root {
			let root_node = store.get_node_mut(tx, root_id).await?;
			if let DeletionResult::Underflown(sn, n_updated) = self
				.delete_at_node(tx, store, root_node, &None, object, doc_id, &mut deleted)
				.await?
			{
				match &sn.n {
					MTreeNode::Internal(n) => match n.len() {
						0 => {
							store.remove_node(sn.id, sn.key).await?;
							self.set_root(None);
							return Ok(deleted);
						}
						1 => {
							store.remove_node(sn.id, sn.key).await?;
							let e = n.values().next().ok_or(Error::Unreachable("MTree::delete"))?;
							self.set_root(Some(e.node));
							return Ok(deleted);
						}
						_ => {}
					},
					MTreeNode::Leaf(n) => {
						if n.is_empty() {
							store.remove_node(sn.id, sn.key).await?;
							self.set_root(None);
							return Ok(deleted);
						}
					}
				}
				store.set_node(sn, n_updated).await?;
			}
		}
		Ok(deleted)
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(? Send))]
	#[allow(clippy::too_many_arguments)]
	async fn delete_at_node(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeStore,
		node: MStoredNode,
		parent_center: &Option<SharedVector>,
		object: SharedVector,
		id: DocId,
		deleted: &mut bool,
	) -> Result<DeletionResult, Error> {
		#[cfg(debug_assertions)]
		debug!("delete_at_node ID: {} - obj: {:?}", node.id, object);
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
		store: &mut MTreeStore,
		node_id: NodeId,
		node_key: Key,
		mut n_node: InternalNode,
		parent_center: &Option<SharedVector>,
		od: SharedVector,
		id: DocId,
		deleted: &mut bool,
	) -> Result<DeletionResult, Error> {
		#[cfg(debug_assertions)]
		debug!("delete_node_internal ID: {} - DocID: {} - obj: {:?}", node_id, id, od);
		let mut on_objs = Vec::new();
		let mut n_updated = false;
		// For each On E N
		for (on_obj, on_entry) in &n_node {
			let on_od_dist = self.calculate_distance(on_obj, &od)?;
			#[cfg(debug_assertions)]
			debug!("on_od_dist: {:?} / {} / {}", on_obj, on_od_dist, on_entry.radius);
			// If (d(Od, On) <= r(On))
			if on_od_dist <= on_entry.radius {
				on_objs.push((on_obj.clone(), on_entry.clone()));
			}
		}
		#[cfg(debug_assertions)]
		debug!("on_objs: {:?}", on_objs);
		for (on_obj, mut on_entry) in on_objs {
			#[cfg(debug_assertions)]
			debug!("on_obj: {:?}", on_obj);
			// Delete (Od, child(On))
			let on_node = store.get_node_mut(tx, on_entry.node).await?;
			#[cfg(debug_assertions)]
			let d_id = on_node.id;
			match self
				.delete_at_node(tx, store, on_node, &Some(on_obj.clone()), od.clone(), id, deleted)
				.await?
			{
				DeletionResult::NotFound => {
					#[cfg(debug_assertions)]
					debug!("delete_at_node ID {} => NotFound", d_id);
				}
				DeletionResult::DocRemoved => {
					#[cfg(debug_assertions)]
					debug!("delete_at_node ID {} => DocRemoved", d_id);
				}
				// Let r = returned covering radius
				DeletionResult::CoveringRadius(r) => {
					#[cfg(debug_assertions)]
					debug!("delete_at_node ID {} => CoveringRadius", d_id);
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
			.await
	}

	async fn delete_node_internal_check_underflown(
		&mut self,
		store: &mut MTreeStore,
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
		Self::set_stored_node(store, node_id, node_key, n_node.into_mtree_node(), n_updated)
			.await?;
		Ok(DeletionResult::CoveringRadius(max_dist))
	}

	async fn set_stored_node(
		store: &mut MTreeStore,
		node_id: NodeId,
		node_key: Key,
		node: MTreeNode,
		updated: bool,
	) -> Result<(), Error> {
		store.set_node(StoredNode::new(node, node_id, node_key, 0), updated).await?;
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn deletion_underflown(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeStore,
		parent_center: &Option<SharedVector>,
		n_node: &mut InternalNode,
		on_obj: SharedVector,
		p: MStoredNode,
		p_updated: bool,
	) -> Result<bool, Error> {
		#[cfg(debug_assertions)]
		debug!("deletion_underflown Node ID: {}", p.id);
		let min = f64::NAN;
		let mut onn = None;
		// Find node entry Onn € N, e <> 0, for which d(On, Onn) is a minimum
		for (onn_obj, onn_entry) in n_node.iter() {
			if onn_entry.node != p.id {
				let d = self.calculate_distance(&on_obj, onn_obj)?;
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
			let onn_child = store.get_node_mut(tx, onn_entry.node).await?;
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
				)
				.await?;
			}
			return Ok(true);
		}
		store.set_node(p, p_updated).await?;
		Ok(false)
	}

	#[allow(clippy::too_many_arguments)]
	async fn delete_underflown_fit_into_child(
		&mut self,
		store: &mut MTreeStore,
		n_node: &mut InternalNode,
		on_obj: SharedVector,
		p: MStoredNode,
		onn_obj: SharedVector,
		mut onn_entry: RoutingProperties,
		mut onn_child: MStoredNode,
	) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		debug!("deletion_underflown - fit into Node ID: {}", onn_child.id);
		// Remove On from N;
		n_node.remove(&on_obj);
		match &mut onn_child.n {
			MTreeNode::Internal(s) => {
				let p_node = p.n.internal()?;
				// for each Op E P
				for (p_obj, mut p_entry) in p_node {
					// Let parentDistance(Op) = d(Op, Onn);
					p_entry.parent_dist = self.calculate_distance(&p_obj, &onn_obj)?;
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
					p_entry.parent_dist = self.calculate_distance(&p_obj, &onn_obj)?;
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
		store.remove_node(p.id, p.key).await?;
		store.set_node(onn_child, true).await?;
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn delete_underflown_redistribute(
		&mut self,
		store: &mut MTreeStore,
		parent_center: &Option<SharedVector>,
		n_node: &mut InternalNode,
		on_obj: SharedVector,
		onn_obj: SharedVector,
		mut p: MStoredNode,
		onn_child: MStoredNode,
	) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		debug!("deletion_underflown - delete_underflown_redistribute Node ID: {}", p.id);
		// Remove On and Onn from N;
		n_node.remove(&on_obj);
		n_node.remove(&onn_obj);
		// (S U P)
		p.n.merge(onn_child.n)?;
		// Split(S U P)
		let (o1, mut e1, o2, mut e2) = match p.n {
			MTreeNode::Internal(n) => self.split_node(store, p.id, p.key, n).await?,
			MTreeNode::Leaf(n) => self.split_node(store, p.id, p.key, n).await?,
		};
		if let Some(pc) = parent_center {
			e1.parent_dist = self.calculate_distance(&o1, pc)?;
			e2.parent_dist = self.calculate_distance(&o2, pc)?;
		} else {
			e1.parent_dist = 0.0;
			e2.parent_dist = 0.0;
		}
		// Add new child pointer entries to N;
		n_node.insert(o1, e1);
		n_node.insert(o2, e2);
		store.remove_node(onn_child.id, onn_child.key).await?;
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn delete_node_leaf(
		&mut self,
		store: &mut MTreeStore,
		node_id: NodeId,
		node_key: Key,
		mut leaf_node: LeafNode,
		parent_center: &Option<SharedVector>,
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
			store.set_node(sn, false).await?;
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
			let max_dist = self.compute_leaf_max_distance(&leaf_node, parent_center)?;
			let sn = StoredNode::new(MTreeNode::Leaf(leaf_node), node_id, node_key, 0);
			store.set_node(sn, true).await?;
			Ok(DeletionResult::CoveringRadius(max_dist))
		} else {
			let sn = StoredNode::new(MTreeNode::Leaf(leaf_node), node_id, node_key, 0);
			store.set_node(sn, true).await?;
			Ok(DeletionResult::DocRemoved)
		}
	}
}

struct DistanceCache(BTreeMap<(SharedVector, SharedVector), f64>);

struct PriorityNode(f64, NodeId);

impl PartialEq<Self> for PriorityNode {
	fn eq(&self, other: &Self) -> bool {
		self.0 == other.0 && self.1 == other.1
	}
}

impl Eq for PriorityNode {}

impl PartialOrd<Self> for PriorityNode {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for PriorityNode {
	fn cmp(&self, other: &Self) -> Ordering {
		let cmp = cmp_f64(&self.0, &other.0);
		if cmp != Ordering::Equal {
			return cmp;
		}
		self.1.cmp(&other.1)
	}
}

#[derive(Debug)]
struct PriorityResult(f64);

impl Eq for PriorityResult {}

impl PartialEq<Self> for PriorityResult {
	fn eq(&self, other: &Self) -> bool {
		self.0 == other.0
	}
}

impl PartialOrd<Self> for PriorityResult {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for PriorityResult {
	fn cmp(&self, other: &Self) -> Ordering {
		cmp_f64(&self.0, &other.0)
	}
}

fn cmp_f64(f1: &f64, f2: &f64) -> Ordering {
	if let Some(cmp) = f1.partial_cmp(f2) {
		return cmp;
	}
	if f1.is_nan() {
		if f2.is_nan() {
			Ordering::Equal
		} else {
			Ordering::Less
		}
	} else {
		Ordering::Greater
	}
}

pub(in crate::idx) type MTreeStore = TreeStore<MTreeNode>;
type MStoredNode = StoredNode<MTreeNode>;

type InternalMap = BTreeMap<SharedVector, RoutingProperties>;

type LeafMap = BTreeMap<SharedVector, ObjectProperties>;

#[derive(Debug, Clone)]
/// A node in this tree structure holds entries.
/// Each entry is a tuple consisting of an object and its associated properties.
/// It's essential to note that the properties vary between a LeafNode and an InternalNode.
/// Both LeafNodes and InternalNodes are implemented as a map.
/// In this map, the key is an object, and the values correspond to its properties.
/// In essence, an entry can be visualized as a tuple of the form (object, properties).
#[non_exhaustive]
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
			MTreeNode::Leaf(_) => Err(Error::Unreachable("MTreeNode::internal")),
		}
	}

	fn leaf(self) -> Result<LeafNode, Error> {
		match self {
			MTreeNode::Internal(_) => Err(Error::Unreachable("MTreeNode::lead")),
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
			(_, _) => Err(Error::Unreachable("MTreeNode::merge")),
		}
	}

	fn merge_internal(n: &mut InternalNode, other: InternalNode) {
		for (o, p) in other {
			n.insert(o, p);
		}
	}

	fn merge_leaf(s: &mut LeafNode, o: LeafNode) {
		for (v, p) in o {
			match s.entry(v) {
				Entry::Occupied(mut e) => {
					let props = e.get_mut();
					for doc in p.docs {
						props.docs.insert(doc);
					}
				}
				Entry::Vacant(e) => {
					e.insert(p);
				}
			}
		}
	}
}

impl Display for MTreeNode {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			MTreeNode::Internal(i) => write!(f, "Internal: {i:?}"),
			MTreeNode::Leaf(l) => write!(f, "Leaf: {l:?}"),
		}
	}
}

trait NodeVectors: Sized {
	#[allow(dead_code)]
	fn len(&self) -> usize;

	fn get_objects(&self) -> Vec<SharedVector>;

	fn extract_node(
		&mut self,
		distances: &DistanceCache,
		p: SharedVector,
		a: Vec<SharedVector>,
	) -> Result<(Self, f64, SharedVector), Error>;

	fn into_mtree_node(self) -> MTreeNode;
}

impl NodeVectors for LeafNode {
	fn len(&self) -> usize {
		self.len()
	}

	fn get_objects(&self) -> Vec<SharedVector> {
		self.keys().cloned().collect()
	}

	fn extract_node(
		&mut self,
		distances: &DistanceCache,
		p: SharedVector,
		a: Vec<SharedVector>,
	) -> Result<(Self, f64, SharedVector), Error> {
		let mut n = LeafNode::new();
		let mut r = 0f64;
		for o in a {
			let mut props =
				self.remove(&o).ok_or(Error::Unreachable("NodeVectors/LeafNode::extract_node)"))?;
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

	fn get_objects(&self) -> Vec<SharedVector> {
		self.keys().cloned().collect()
	}

	fn extract_node(
		&mut self,
		distances: &DistanceCache,
		p: SharedVector,
		a: Vec<SharedVector>,
	) -> Result<(Self, f64, SharedVector), Error> {
		let mut n = InternalNode::new();
		let mut max_r = 0f64;
		for o in a {
			let mut props = self
				.remove(&o)
				.ok_or(Error::Unreachable("NodeVectors/InternalNode::extract_node"))?;
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
			_ => Err(Error::CorruptedIndex("MTreeNode::try_from_val")),
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
#[revisioned(revision = 2)]
#[non_exhaustive]
pub struct MState {
	capacity: u16,
	root: Option<NodeId>,
	next_node_id: NodeId,
	#[revision(start = 2)]
	generation: u64,
}

impl MState {
	pub fn new(capacity: u16) -> Self {
		assert!(capacity >= 2, "Capacity should be >= 2");
		Self {
			capacity,
			root: None,
			next_node_id: 0,
			generation: 0,
		}
	}
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[non_exhaustive]
pub struct RoutingProperties {
	// Reference to the node
	node: NodeId,
	// Distance to its parent object
	parent_dist: f64,
	// Covering radius
	radius: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[non_exhaustive]
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
	use rand::prelude::StdRng;
	use rand::{Rng, SeedableRng};
	use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

	use crate::err::Error;
	use test_log::test;

	use crate::idx::docids::DocId;
	use crate::idx::trees::mtree::{
		InternalMap, MState, MTree, MTreeNode, MTreeStore, ObjectProperties,
	};
	use crate::idx::trees::store::{NodeId, TreeNodeProvider, TreeStore};
	use crate::idx::trees::vector::{SharedVector, Vector};
	use crate::kvs::LockType::*;
	use crate::kvs::Transaction;
	use crate::kvs::{Datastore, TransactionType};
	use crate::sql::index::{Distance, VectorType};
	use crate::sql::Number;

	async fn new_operation(
		ds: &Datastore,
		t: &MTree,
		tt: TransactionType,
		cache_size: usize,
	) -> (TreeStore<MTreeNode>, Transaction) {
		let st = ds
			.index_store()
			.get_store_mtree(TreeNodeProvider::Debug, t.state.generation, tt, cache_size)
			.await;
		let tx = ds.transaction(tt, Optimistic).await.unwrap();
		(st, tx)
	}

	async fn finish_operation(
		t: &mut MTree,
		mut tx: Transaction,
		mut st: TreeStore<MTreeNode>,
		commit: bool,
	) -> Result<(), Error> {
		if st.finish(&mut tx).await? {
			t.state.generation += 1;
		}
		if commit {
			tx.commit().await
		} else {
			tx.cancel().await
		}
	}

	fn new_vec(mut n: i64, t: VectorType, dim: usize) -> SharedVector {
		let mut vec = Vector::new(t, dim);
		vec.add(Number::Int(n));
		for _ in 1..dim {
			n += 1;
			vec.add(Number::Int(n));
		}
		vec.into()
	}

	fn new_random_vec(rng: &mut StdRng, t: VectorType, dim: usize) -> SharedVector {
		let mut vec = Vector::new(t, dim);
		for _ in 0..dim {
			vec.add(Number::Float(rng.gen_range(-5.0..5.0)));
		}
		vec.into()
	}

	#[test(tokio::test)]
	async fn test_mtree_insertions() -> Result<(), Error> {
		const CACHE_SIZE: usize = 20;

		let mut t = MTree::new(MState::new(3), Distance::Euclidean);
		let ds = Datastore::new("memory").await?;

		let vec1 = new_vec(1, VectorType::F64, 1);
		// First the index is empty
		{
			let (st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			let res = t.knn_search(&mut tx, &st, &vec1, 10).await?;
			check_knn(&res.docs, vec![]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes.len(), 0);
		}
		// Insert single element
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Write, CACHE_SIZE).await;
			t.insert(&mut tx, &mut st, vec1.clone(), 1).await?;
			assert_eq!(t.state.root, Some(0));
			check_leaf_write(&mut tx, &mut st, 0, |m| {
				assert_eq!(m.len(), 1);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
			})
			.await;
			finish_operation(&mut t, tx, st, true).await?;
		}
		// Check KNN
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			let res = t.knn_search(&mut tx, &st, &vec1, 10).await?;
			check_knn(&res.docs, vec![1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes.len(), 1);
			check_tree_properties(&mut tx, &mut st, &t).await?.check(1, 1, Some(1), Some(1), 1, 1);
		}

		// insert second element
		let vec2 = new_vec(2, VectorType::F64, 1);
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Write, CACHE_SIZE).await;
			t.insert(&mut tx, &mut st, vec2.clone(), 2).await?;
			finish_operation(&mut t, tx, st, true).await?;
		}
		// vec1 knn
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			let res = t.knn_search(&mut tx, &st, &vec1, 10).await?;
			check_knn(&res.docs, vec![1, 2]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes.len(), 1);
			assert_eq!(t.state.root, Some(0));
			check_leaf_read(&mut tx, &mut st, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 0.0, &[2]);
			})
			.await;
			check_tree_properties(&mut tx, &mut st, &t).await?.check(1, 1, Some(2), Some(2), 2, 2);
		}
		// vec2 knn
		{
			let (st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			let res = t.knn_search(&mut tx, &st, &vec2, 10).await?;
			check_knn(&res.docs, vec![2, 1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes.len(), 1);
		}

		// insert new doc to existing vector
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Write, CACHE_SIZE).await;
			t.insert(&mut tx, &mut st, vec2.clone(), 3).await?;
			finish_operation(&mut t, tx, st, true).await?;
		}
		// vec2 knn
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			let res = t.knn_search(&mut tx, &st, &vec2, 10).await?;
			check_knn(&res.docs, vec![2, 3, 1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes.len(), 1);
			assert_eq!(t.state.root, Some(0));
			check_leaf_read(&mut tx, &mut st, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 0.0, &[2, 3]);
			})
			.await;
			check_tree_properties(&mut tx, &mut st, &t).await?.check(1, 1, Some(2), Some(2), 2, 3);
		}

		// insert third vector
		let vec3 = new_vec(3, VectorType::F64, 1);
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Write, CACHE_SIZE).await;
			t.insert(&mut tx, &mut st, vec3.clone(), 3).await?;
			finish_operation(&mut t, tx, st, true).await?;
		}
		// vec3 knn
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			let res = t.knn_search(&mut tx, &st, &vec3, 10).await?;
			check_knn(&res.docs, vec![3, 2, 3, 1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes.len(), 1);
			assert_eq!(t.state.root, Some(0));
			check_leaf_read(&mut tx, &mut st, 0, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 0.0, &[2, 3]);
				check_leaf_vec(m, &vec3, 0.0, &[3]);
			})
			.await;
			check_tree_properties(&mut tx, &mut st, &t).await?.check(1, 1, Some(3), Some(3), 3, 4);
		}

		// Check split leaf node
		let vec4 = new_vec(4, VectorType::F64, 1);
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Write, CACHE_SIZE).await;
			t.insert(&mut tx, &mut st, vec4.clone(), 4).await?;
			finish_operation(&mut t, tx, st, true).await?;
		}
		// vec4 knn
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			let res = t.knn_search(&mut tx, &st, &vec4, 10).await?;
			check_knn(&res.docs, vec![4, 3, 2, 3, 1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes.len(), 3);
			assert_eq!(t.state.root, Some(2));
			check_internal(&mut tx, &mut st, 2, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, &vec1, 0.0, 0, 1.0);
				check_routing_vec(m, &vec4, 0.0, 1, 1.0);
			})
			.await;
			check_leaf_read(&mut tx, &mut st, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf_read(&mut tx, &mut st, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec3, 1.0, &[3]);
				check_leaf_vec(m, &vec4, 0.0, &[4]);
			})
			.await;
			check_tree_properties(&mut tx, &mut st, &t).await?.check(3, 2, Some(2), Some(2), 4, 5);
		}

		// Insert vec extending the radius of the last node, calling compute_leaf_radius
		let vec6 = new_vec(6, VectorType::F64, 1);
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Write, CACHE_SIZE).await;
			t.insert(&mut tx, &mut st, vec6.clone(), 6).await?;
			finish_operation(&mut t, tx, st, true).await?;
		}
		// vec6 knn
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			let res = t.knn_search(&mut tx, &st, &vec6, 10).await?;
			check_knn(&res.docs, vec![6, 4, 3, 2, 3, 1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes.len(), 3);
			assert_eq!(t.state.root, Some(2));
			check_internal(&mut tx, &mut st, 2, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, &vec1, 0.0, 0, 1.0);
				check_routing_vec(m, &vec4, 0.0, 1, 2.0);
			})
			.await;
			check_leaf_read(&mut tx, &mut st, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf_read(&mut tx, &mut st, 1, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, &vec3, 1.0, &[3]);
				check_leaf_vec(m, &vec4, 0.0, &[4]);
				check_leaf_vec(m, &vec6, 2.0, &[6]);
			})
			.await;
			check_tree_properties(&mut tx, &mut st, &t).await?.check(3, 2, Some(2), Some(3), 5, 6);
		}

		// Insert check split internal node

		// Insert vec8
		let vec8 = new_vec(8, VectorType::F64, 1);
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Write, CACHE_SIZE).await;
			t.insert(&mut tx, &mut st, vec8.clone(), 8).await?;
			finish_operation(&mut t, tx, st, true).await?;
		}
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			check_tree_properties(&mut tx, &mut st, &t).await?.check(4, 2, Some(2), Some(2), 6, 7);
			assert_eq!(t.state.root, Some(2));
			// Check Root node (level 1)
			check_internal(&mut tx, &mut st, 2, |m| {
				assert_eq!(m.len(), 3);
				check_routing_vec(m, &vec1, 0.0, 0, 1.0);
				check_routing_vec(m, &vec3, 0.0, 1, 1.0);
				check_routing_vec(m, &vec8, 0.0, 3, 2.0);
			})
			.await;
			// Check level 2
			check_leaf_read(&mut tx, &mut st, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf_read(&mut tx, &mut st, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec3, 0.0, &[3]);
				check_leaf_vec(m, &vec4, 1.0, &[4]);
			})
			.await;
			check_leaf_read(&mut tx, &mut st, 3, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec6, 2.0, &[6]);
				check_leaf_vec(m, &vec8, 0.0, &[8]);
			})
			.await;
		}

		let vec9 = new_vec(9, VectorType::F64, 1);
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Write, CACHE_SIZE).await;
			t.insert(&mut tx, &mut st, vec9.clone(), 9).await?;
			finish_operation(&mut t, tx, st, true).await?;
		}
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			check_tree_properties(&mut tx, &mut st, &t).await?.check(4, 2, Some(2), Some(3), 7, 8);
			assert_eq!(t.state.root, Some(2));
			// Check Root node (level 1)
			check_internal(&mut tx, &mut st, 2, |m| {
				assert_eq!(m.len(), 3);
				check_routing_vec(m, &vec1, 0.0, 0, 1.0);
				check_routing_vec(m, &vec3, 0.0, 1, 1.0);
				check_routing_vec(m, &vec8, 0.0, 3, 2.0);
			})
			.await;
			// Check level 2
			check_leaf_read(&mut tx, &mut st, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf_read(&mut tx, &mut st, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec3, 0.0, &[3]);
				check_leaf_vec(m, &vec4, 1.0, &[4]);
			})
			.await;
			check_leaf_read(&mut tx, &mut st, 3, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, &vec6, 2.0, &[6]);
				check_leaf_vec(m, &vec8, 0.0, &[8]);
				check_leaf_vec(m, &vec9, 1.0, &[9]);
			})
			.await;
		}

		let vec10 = new_vec(10, VectorType::F64, 1);
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Write, CACHE_SIZE).await;
			t.insert(&mut tx, &mut st, vec10.clone(), 10).await?;
			finish_operation(&mut t, tx, st, true).await?;
		}
		{
			let (mut st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			check_tree_properties(&mut tx, &mut st, &t).await?.check(7, 3, Some(2), Some(2), 8, 9);
			assert_eq!(t.state.root, Some(6));
			// Check Root node (level 1)
			check_internal(&mut tx, &mut st, 6, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, &vec1, 0.0, 2, 3.0);
				check_routing_vec(m, &vec10, 0.0, 5, 6.0);
			})
			.await;
			// Check level 2
			check_internal(&mut tx, &mut st, 2, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, &vec1, 0.0, 0, 1.0);
				check_routing_vec(m, &vec3, 2.0, 1, 1.0);
			})
			.await;
			check_internal(&mut tx, &mut st, 5, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, &vec6, 4.0, 3, 2.0);
				check_routing_vec(m, &vec10, 0.0, 4, 1.0);
			})
			.await;
			// Check level 3
			check_leaf_read(&mut tx, &mut st, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec1, 0.0, &[1]);
				check_leaf_vec(m, &vec2, 1.0, &[2, 3]);
			})
			.await;
			check_leaf_read(&mut tx, &mut st, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec3, 0.0, &[3]);
				check_leaf_vec(m, &vec4, 1.0, &[4]);
			})
			.await;
			check_leaf_read(&mut tx, &mut st, 3, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec6, 0.0, &[6]);
				check_leaf_vec(m, &vec8, 2.0, &[8]);
			})
			.await;
			check_leaf_read(&mut tx, &mut st, 4, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, &vec9, 1.0, &[9]);
				check_leaf_vec(m, &vec10, 0.0, &[10]);
			})
			.await;
		}

		// vec8 knn
		{
			let (st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			let res = t.knn_search(&mut tx, &st, &vec8, 20).await?;
			check_knn(&res.docs, vec![8, 9, 6, 10, 4, 3, 2, 3, 1]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes.len(), 7);
		}
		// vec4 knn(2)
		{
			let (st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			let res = t.knn_search(&mut tx, &st, &vec4, 2).await?;
			check_knn(&res.docs, vec![4, 3]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes.len(), 6);
		}

		// vec10 knn(2)
		{
			let (st, mut tx) = new_operation(&ds, &t, TransactionType::Read, CACHE_SIZE).await;
			let res = t.knn_search(&mut tx, &st, &vec10, 2).await?;
			check_knn(&res.docs, vec![10, 9]);
			#[cfg(debug_assertions)]
			assert_eq!(res.visited_nodes.len(), 5);
		}
		Ok(())
	}

	async fn insert_collection_one_by_one(
		ds: &Datastore,
		t: &mut MTree,
		collection: &TestCollection,
		cache_size: usize,
	) -> Result<HashMap<DocId, SharedVector>, Error> {
		let mut map = HashMap::with_capacity(collection.as_ref().len());
		let mut c = 0;
		for (doc_id, obj) in collection.as_ref() {
			{
				let (mut st, mut tx) =
					new_operation(ds, t, TransactionType::Write, cache_size).await;
				t.insert(&mut tx, &mut st, obj.clone(), *doc_id).await?;
				finish_operation(t, tx, st, true).await?;
				map.insert(*doc_id, obj.clone());
			}
			c += 1;
			{
				let (mut st, mut tx) =
					new_operation(ds, t, TransactionType::Read, cache_size).await;
				let p = check_tree_properties(&mut tx, &mut st, t).await?;
				assert_eq!(p.doc_count, c);
			}
		}
		Ok(map)
	}

	async fn insert_collection_batch(
		ds: &Datastore,
		t: &mut MTree,
		collection: &TestCollection,
		cache_size: usize,
	) -> Result<HashMap<DocId, SharedVector>, Error> {
		let mut map = HashMap::with_capacity(collection.as_ref().len());
		{
			let (mut st, mut tx) = new_operation(ds, t, TransactionType::Write, cache_size).await;
			for (doc_id, obj) in collection.as_ref() {
				t.insert(&mut tx, &mut st, obj.clone(), *doc_id).await?;
				map.insert(*doc_id, obj.clone());
			}
			finish_operation(t, tx, st, true).await?;
		}
		{
			let (mut st, mut tx) = new_operation(ds, t, TransactionType::Read, cache_size).await;
			check_tree_properties(&mut tx, &mut st, t).await?;
		}
		Ok(map)
	}

	async fn delete_collection(
		ds: &Datastore,
		t: &mut MTree,
		collection: &TestCollection,
		cache_size: usize,
	) -> Result<(), Error> {
		let mut all_deleted = true;
		for (doc_id, obj) in collection.as_ref() {
			let deleted = {
				debug!("### Remove {} {:?}", doc_id, obj);
				let (mut st, mut tx) =
					new_operation(ds, t, TransactionType::Write, cache_size).await;
				let deleted = t.delete(&mut tx, &mut st, obj.clone(), *doc_id).await?;
				finish_operation(t, tx, st, true).await?;
				deleted
			};
			all_deleted = all_deleted && deleted;
			if deleted {
				let (st, mut tx) = new_operation(ds, t, TransactionType::Read, cache_size).await;
				let res = t.knn_search(&mut tx, &st, obj, 1).await?;
				assert!(!res.docs.contains(doc_id), "Found: {} {:?}", doc_id, obj);
			} else {
				// In v1.2.x deletion is experimental. Will be fixed in 1.3
				warn!("Delete failed: {} {:?}", doc_id, obj);
			}
			{
				let (mut st, mut tx) =
					new_operation(ds, t, TransactionType::Read, cache_size).await;
				check_tree_properties(&mut tx, &mut st, t).await?;
			}
		}

		if all_deleted {
			let (mut st, mut tx) = new_operation(ds, t, TransactionType::Read, cache_size).await;
			check_tree_properties(&mut tx, &mut st, t).await?.check(0, 0, None, None, 0, 0);
		}
		Ok(())
	}

	async fn find_collection(
		ds: &Datastore,
		t: &mut MTree,
		collection: &TestCollection,
		cache_size: usize,
	) -> Result<(), Error> {
		let (mut st, mut tx) = new_operation(ds, t, TransactionType::Read, cache_size).await;
		let max_knn = 20.max(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			for knn in 1..max_knn {
				let res = t.knn_search(&mut tx, &st, obj, knn).await?;
				if collection.is_unique() {
					assert!(
						res.docs.contains(doc_id),
						"Search: {:?} - Knn: {} - Wrong Doc - Expected: {} - Got: {:?}",
						obj,
						knn,
						doc_id,
						res.docs
					);
				}
				let expected_len = collection.as_ref().len().min(knn);
				if expected_len != res.docs.len() {
					debug!("{:?}", res.visited_nodes);
					check_tree_properties(&mut tx, &mut st, t).await?;
				}
				assert_eq!(
					expected_len,
					res.docs.len(),
					"Wrong knn count - Expected: {} - Got: {} - Collection: {}",
					expected_len,
					res.docs.len(),
					collection.as_ref().len(),
				)
			}
		}
		Ok(())
	}

	async fn check_full_knn(
		ds: &Datastore,
		t: &mut MTree,
		map: &HashMap<DocId, SharedVector>,
		cache_size: usize,
	) -> Result<(), Error> {
		let (st, mut tx) = new_operation(ds, t, TransactionType::Read, cache_size).await;
		for obj in map.values() {
			let res = t.knn_search(&mut tx, &st, obj, map.len()).await?;
			assert_eq!(
				map.len(),
				res.docs.len(),
				"Wrong knn count - Expected: {} - Got: {} - Collection: {}",
				map.len(),
				res.docs.len(),
				map.len(),
			);
			// We check that the results are sorted by ascending distance
			let mut dist = 0.0;
			for doc in res.docs {
				let o = map.get(&doc).unwrap();
				let d = t.calculate_distance(obj, o)?;
				debug!("doc: {doc} - d: {d} - {obj:?} - {o:?}");
				assert!(d >= dist, "d: {d} - dist: {dist}");
				dist = d;
			}
		}
		Ok(())
	}

	async fn test_mtree_collection(
		capacities: &[u16],
		vector_type: VectorType,
		collection: TestCollection,
		check_find: bool,
		check_full: bool,
		check_delete: bool,
		cache_size: usize,
	) -> Result<(), Error> {
		for distance in [Distance::Euclidean, Distance::Cosine, Distance::Manhattan] {
			if distance == Distance::Cosine && vector_type == VectorType::F64 {
				// Tests based on Cosine distance with F64 may fail due to float rounding errors
				continue;
			}
			for capacity in capacities {
				info!(
					"test_mtree_collection - Distance: {:?} - Capacity: {} - Collection: {} - Vector type: {}",
					distance,
					capacity,
					collection.as_ref().len(),
					vector_type,
				);
				let ds = Datastore::new("memory").await?;
				let mut t = MTree::new(MState::new(*capacity), distance.clone());

				let map = if collection.as_ref().len() < 1000 {
					insert_collection_one_by_one(&ds, &mut t, &collection, cache_size).await?
				} else {
					insert_collection_batch(&ds, &mut t, &collection, cache_size).await?
				};
				if check_find {
					find_collection(&ds, &mut t, &collection, cache_size).await?;
				}
				if check_full {
					check_full_knn(&ds, &mut t, &map, cache_size).await?;
				}
				if check_delete {
					delete_collection(&ds, &mut t, &collection, cache_size).await?;
				}
			}
		}
		Ok(())
	}

	enum TestCollection {
		Unique(Vec<(DocId, SharedVector)>),
		NonUnique(Vec<(DocId, SharedVector)>),
	}

	impl AsRef<Vec<(DocId, SharedVector)>> for TestCollection {
		fn as_ref(&self) -> &Vec<(DocId, SharedVector)> {
			match self {
				TestCollection::Unique(c) | TestCollection::NonUnique(c) => c,
			}
		}
	}

	impl TestCollection {
		fn new_unique(
			collection_size: usize,
			vector_type: VectorType,
			dimension: usize,
		) -> TestCollection {
			let mut collection = vec![];
			for doc_id in 0..collection_size as DocId {
				collection.push((doc_id, new_vec((doc_id + 1) as i64, vector_type, dimension)));
			}
			TestCollection::Unique(collection)
		}

		fn new_random(
			collection_size: usize,
			vector_type: VectorType,
			dimension: usize,
		) -> TestCollection {
			let mut rng = get_seed_rnd();
			let mut collection = vec![];

			// Prepare data set
			for doc_id in 0..collection_size {
				collection
					.push((doc_id as DocId, new_random_vec(&mut rng, vector_type, dimension)));
			}
			TestCollection::NonUnique(collection)
		}

		fn is_unique(&self) -> bool {
			matches!(self, TestCollection::Unique(_))
		}
	}

	#[test(tokio::test)]
	#[ignore]
	async fn test_mtree_unique_xs() -> Result<(), Error> {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			for i in 0..30 {
				test_mtree_collection(
					&[3, 40],
					vt,
					TestCollection::new_unique(i, vt, 2),
					true,
					true,
					true,
					100,
				)
				.await?;
			}
		}
		Ok(())
	}

	#[test(tokio::test)]
	#[ignore]
	async fn test_mtree_unique_xs_full_cache() -> Result<(), Error> {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			for i in 0..30 {
				test_mtree_collection(
					&[3, 40],
					vt,
					TestCollection::new_unique(i, vt, 2),
					true,
					true,
					true,
					0,
				)
				.await?;
			}
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn test_mtree_unique_small() -> Result<(), Error> {
		for vt in [VectorType::F64, VectorType::I64] {
			test_mtree_collection(
				&[10, 20],
				vt,
				TestCollection::new_unique(150, vt, 3),
				true,
				true,
				false,
				0,
			)
			.await?;
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn test_mtree_unique_normal() -> Result<(), Error> {
		for vt in [VectorType::F32, VectorType::I32] {
			test_mtree_collection(
				&[40],
				vt,
				TestCollection::new_unique(1000, vt, 10),
				false,
				true,
				false,
				100,
			)
			.await?;
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn test_mtree_unique_normal_full_cache() -> Result<(), Error> {
		for vt in [VectorType::F32, VectorType::I32] {
			test_mtree_collection(
				&[40],
				vt,
				TestCollection::new_unique(1000, vt, 10),
				false,
				true,
				false,
				0,
			)
			.await?;
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn test_mtree_unique_normal_root_cache() -> Result<(), Error> {
		for vt in [VectorType::F32, VectorType::I32] {
			test_mtree_collection(
				&[40],
				vt,
				TestCollection::new_unique(1000, vt, 10),
				false,
				true,
				false,
				1,
			)
			.await?;
		}
		Ok(())
	}

	#[test(tokio::test)]
	#[ignore]
	async fn test_mtree_random_xs() -> Result<(), Error> {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			for i in 0..30 {
				// 10, 40
				test_mtree_collection(
					&[3, 40],
					vt,
					TestCollection::new_random(i, vt, 1),
					true,
					true,
					true,
					0,
				)
				.await?;
			}
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn test_mtree_random_small() -> Result<(), Error> {
		for vt in [VectorType::F64, VectorType::I64] {
			test_mtree_collection(
				&[10, 20],
				vt,
				TestCollection::new_random(150, vt, 3),
				true,
				true,
				false,
				0,
			)
			.await?;
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn test_mtree_random_normal() -> Result<(), Error> {
		for vt in [VectorType::F32, VectorType::I32] {
			test_mtree_collection(
				&[40],
				vt,
				TestCollection::new_random(1000, vt, 10),
				false,
				true,
				false,
				0,
			)
			.await?;
		}
		Ok(())
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

	async fn check_node_read<F>(
		tx: &mut Transaction,
		st: &mut MTreeStore,
		node_id: NodeId,
		check_func: F,
	) where
		F: FnOnce(&MTreeNode),
	{
		let n = st.get_node(tx, node_id).await.unwrap();
		check_func(&n.n);
	}

	async fn check_node_write<F>(
		tx: &mut Transaction,
		st: &mut MTreeStore,
		node_id: NodeId,
		check_func: F,
	) where
		F: FnOnce(&MTreeNode),
	{
		let n = st.get_node_mut(tx, node_id).await.unwrap();
		check_func(&n.n);
		st.set_node(n, false).await.unwrap();
	}

	async fn check_leaf_read<F>(
		tx: &mut Transaction,
		st: &mut MTreeStore,
		node_id: NodeId,
		check_func: F,
	) where
		F: FnOnce(&BTreeMap<SharedVector, ObjectProperties>),
	{
		check_node_read(tx, st, node_id, |n| {
			if let MTreeNode::Leaf(m) = n {
				check_func(m);
			} else {
				panic!("The node is not a leaf node: {node_id}")
			}
		})
		.await
	}

	async fn check_leaf_write<F>(
		tx: &mut Transaction,
		st: &mut MTreeStore,
		node_id: NodeId,
		check_func: F,
	) where
		F: FnOnce(&BTreeMap<SharedVector, ObjectProperties>),
	{
		check_node_write(tx, st, node_id, |n| {
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
		st: &mut MTreeStore,
		node_id: NodeId,
		check_func: F,
	) where
		F: FnOnce(&InternalMap),
	{
		check_node_read(tx, st, node_id, |n| {
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
		st: &mut MTreeStore,
		t: &MTree,
	) -> Result<CheckedProperties, Error> {
		debug!("CheckTreeProperties");
		let mut node_ids = HashSet::new();
		let mut checks = CheckedProperties::default();
		let mut nodes: VecDeque<(NodeId, f64, Option<SharedVector>, usize)> = VecDeque::new();
		if let Some(root_id) = t.state.root {
			nodes.push_back((root_id, 0.0, None, 1));
		}
		let mut leaf_objects = BTreeSet::new();
		while let Some((node_id, radius, center, depth)) = nodes.pop_front() {
			assert!(node_ids.insert(node_id), "Node already exist: {}", node_id);
			checks.node_count += 1;
			if depth > checks.max_depth {
				checks.max_depth = depth;
			}
			let node = st.get_node(tx, node_id).await?;
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
			match &node.n {
				MTreeNode::Internal(entries) => {
					let next_depth = depth + 1;
					for (o, p) in entries {
						if let Some(center) = center.as_ref() {
							let pd = t.calculate_distance(center, o)?;
							assert_eq!(pd, p.parent_dist, "Incorrect parent distance");
							assert!(pd + p.radius <= radius);
						}
						nodes.push_back((p.node, p.radius, Some(o.clone()), next_depth))
					}
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
							let pd = t.calculate_distance(center, o)?;
							debug!("calc_dist: {:?} {:?} = {}", center, &o, pd);
							assert_eq!(pd, p.parent_dist, "Invalid parent distance ({}): {} - Expected: {} - Node Id: {} - Obj: {:?} - Center: {:?}", p.parent_dist, t.distance, pd, node_id, o, center);
						}
						checks.doc_count += p.docs.len() as usize;
					}
				}
			}
		}
		Ok(checks)
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
		debug!("Seed: {}", seed);
		// Create a seeded RNG
		StdRng::seed_from_u64(seed)
	}
}
