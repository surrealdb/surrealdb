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
use std::collections::{BTreeSet, BinaryHeap};
use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

type Vector = Vec<Number>;

type MTreeNodeStore = TreeNodeStore<Box<dyn TreeNode>>;

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
	) -> Result<(Vec<(Arc<Vector>, f64)>, RoaringTreemap), Error> {
		let mut queue = BinaryHeap::new();
		let mut res = BTreeSet::new();
		if let Some(root_id) = self.state.root {
			queue.push(PriorityNode(0.0, root_id));
		}
		let mut max_dist = f64::INFINITY;
		while let Some(current) = queue.pop() {
			let node = store.get_node(tx, current.1).await?;
			match node.n {
				MTreeNode::Leaf(ref indexmap) => {
					for (o, p) in indexmap {
						let d = self.calculate_distance(o.as_ref(), v);
						if max_dist == f64::INFINITY || d > max_dist {
							max_dist = d;
						}
						res.insert(PriorityResult(d, o.clone(), p.docs.clone()));
						if res.len() > k {
							res.pop_last();
						}
					}
				}
				MTreeNode::Routing(ref entries) => {
					for entry in entries {
						let d = self.calculate_distance(entry.center.as_ref(), v);
						let min_dist = (d - entry.radius).abs();
						if res.len() < k || min_dist < max_dist {
							queue.push(PriorityNode(min_dist, entry.node));
						}
					}
				}
			}
			store.set_node(node, false)?;
		}
		let mut global_docs = RoaringTreemap::new();
		let mut results = Vec::with_capacity(res.len());
		for r in res {
			global_docs |= r.2;
			results.push((r.1, r.0));
		}
		Ok((results, global_docs))
	}
}

enum InsertResult {
	DocAdded,
	ObjectInserted,
	PromotedEntries(Vector, Vector),
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
		let new_root_leaf = LeafNode::new_root_leaf(v, id);
		let new_root_node = store.new_node(new_root_id, new_root_leaf)?;
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
		let mut routing_nodes = vec![];
		let mut next_node = Some((root_id, None, v, id));
		while let Some((node_id, parent_center, v, id)) = next_node.take() {
			let mut node = store.get_node(tx, node_id).await?;
			match &mut node.n {
				MTreeNode::Routing(entries) => {
					let idx = self.find_closest(entries, &v)?;
					let r = &entries[idx];
					next_node = Some((r.node, Some(r.center.clone()), v, id));
					// The radius of the routing node will be recalculated
					routing_nodes.push((node, idx));
				}
				MTreeNode::Leaf(objects) => {
					self.insert_node_leaf(objects, v, parent_center, id);
					if objects.len() > self.state.capacity as usize {
						// The node need to be split
						self.split_node(store, node_id, node.key, objects)?;
					} else {
						store.set_node(node, true)?;
					}
				}
			};
		}
		self.recompute_radius(tx, store, &mut routing_nodes).await?;
		Ok(())
	}

	async fn recompute_radius(
		&self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		routing_nodes: &mut Vec<(StoredNode<MTreeNode>, usize)>,
	) -> Result<(), Error> {
		while let Some((node, idx)) = routing_nodes.pop() {
			self.compute_radius(tx, store, node, vec![idx]).await?;
		}
		Ok(())
	}

	async fn compute_radius(
		&self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		mut node: StoredNode<MTreeNode>,
		idxs: Vec<usize>,
	) -> Result<(), Error> {
		let mut update = false;
		if let MTreeNode::Routing(entries) = &mut node.n {
			for idx in idxs {
				let r = &mut entries[idx];
				let child = store.get_node(tx, r.node).await?;
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
			}
		}
		store.set_node(node, update)?;
		Ok(())
	}

	fn compute_routing_radius(&self, center: &Vector, entries: &Vec<RoutingEntry>) -> f64 {
		let mut max_radius = f64::MIN;
		for e in entries {
			let radius = self.calculate_distance(e.center.as_ref(), center.as_ref()) + e.radius;
			if radius > max_radius {
				max_radius = radius;
			}
		}
		max_radius
	}

	fn compute_leaf_radius(&self, center: &Vector, indexmap: &LeafIndexMap) -> f64 {
		let mut max_radius = f64::MIN;
		for v in indexmap.keys() {
			let radius = self.calculate_distance(v, center);
			if radius > max_radius {
				max_radius = radius;
			}
		}
		max_radius
	}

	fn find_closest(&self, entries: &Vec<RoutingEntry>, vec: &Vector) -> Result<usize, Error> {
		let res = entries.iter().enumerate().min_by(|&(_, a), &(_, b)| {
			let distance_a = self.calculate_distance(&a.center, &vec);
			let distance_b = self.calculate_distance(&b.center, &vec);
			distance_a.partial_cmp(&distance_b).unwrap()
		});
		let (idx, _) = res.ok_or(Error::Unreachable)?;
		Ok(idx)
	}

	fn insert_node_leaf(
		&mut self,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		mut node: LeafNode,
		parent_center: Option<Arc<Vector>>,
		object: Arc<Vector>,
		id: DocId,
	) -> Result<InsertResult, Error> {
		match &mut node.n.objects.entry(object) {
			Entry::Occupied(mut e) => {
				e.get_mut().docs.insert(id);
				store.set_node(StoredNode::new(Box::new(node), node_id, node_key, 0), true)?;
				return Ok(InsertResult::DocAdded);
			}
			Entry::Vacant(e) => {
				let d =
					parent_center.map_or(0f64, |v| self.calculate_distance(v.as_ref(), e.key()));
				if d > self.max_parent_distance {
					self.max_parent_distance = d;
				}
				e.insert(ObjectProperties::new(d, id));
			}
		};
		if node.objects.len() < self.state.capacity as usize {
			store.set_node(StoredNode::new(Box::new(node), node_id, node_key, 0), true)?;
			Ok(InsertResult::ObjectInserted)
		} else {
			self.split_leaf_node(store, node_id, node_key, node.objects)
		}
	}

	fn split_leaf_node(
		&mut self,
		store: &mut MTreeNodeStore,
		node_id: NodeId,
		node_key: Key,
		objects: LeafIndexMap,
	) -> Result<InsertResult, Error> {
		let distances = self.compute_distance_matrix(&objects)?;
		let (p1, p2) = Self::select_promotion_objects(&distances);

		// Extract the promoted vectors
		let (promo1, _) = objects.get_index(p1).ok_or(Error::Unreachable)?;
		let (promo2, _) = objects.get_index(p2).ok_or(Error::Unreachable)?;
		let promo1 = promo1.clone();
		let promo2 = promo2.clone();

		let mut leaf1 = Vec::new();
		let mut leaf2 = Vec::new();

		let (mut r1, mut r2) = (0f64, 0f64);

		// Distribute entries, update parent_dist and calculate radius
		for (i, (v, mut p)) in objects.drain(..).enumerate() {
			let dist_p1 = distances[i][p1];
			let dist_p2 = distances[i][p2];
			if dist_p1 <= dist_p2 {
				p.parent_dist = dist_p1;
				leaf1.push((v, p));
				if dist_p1 > r1 {
					r1 = dist_p1;
				}
			} else {
				p.parent_dist = dist_p2;
				leaf2.push((v, p));
				if dist_p2 > r2 {
					r2 = dist_p2;
				}
			}
		}

		// Store the new leaf nodes
		let n1 = self.new_node_id();
		let n2 = self.new_node_id();

		// Sort the leaf nodes
		leaf1.sort_by(|(_, p1), (_, p2)| p1.parent_dist.total_cmp(&p2.parent_dist));
		leaf2.sort_by(|(_, p1), (_, p2)| p1.parent_dist.total_cmp(&p2.parent_dist));
		let leaf1 = IndexMap::from_iter(leaf1);
		let leaf2 = IndexMap::from_iter(leaf2);

		// Update the store/cache
		let n = store.new_node(n1, MTreeNode::Leaf(leaf1))?;
		store.set_node(n, true)?;
		let n = store.new_node(n2, MTreeNode::Leaf(leaf2))?;
		store.set_node(n, true)?;

		// Update the splitted node
		let r1 = RoutingEntry {
			node: n1,
			center: promo1.clone(),
			radius: r1,
		};
		let r2 = RoutingEntry {
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

	fn compute_distance_matrix(&self, objects: &LeafIndexMap) -> Result<Vec<Vec<f64>>, Error> {
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

	async fn remove(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		v: Vector,
		doc_id: DocId,
	) -> Result<bool, Error> {
		if let Some(root_id) = self.state.root {
			Ok(self.remove_vector(tx, store, root_id, Arc::new(v), doc_id).await?)
		} else {
			Ok(false)
		}
	}

	async fn remove_vector(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		root_id: NodeId,
		v: Arc<Vector>,
		doc_id: DocId,
	) -> Result<bool, Error> {
		let mut routing_nodes = vec![];
		let mut current = Some(root_id);
		let mut result = false;
		while let Some(node_id) = current.take() {
			let mut node = store.get_node(tx, node_id).await?;
			match &mut node.n {
				MTreeNode::Routing(entries) => {
					if let Some((node_idx, node_id)) =
						self.find_next_child_entry(entries, v.as_ref())
					{
						routing_nodes.push((node, node_idx));
						current = Some(node_id);
					} else {
						store.set_node(node, false)?;
					}
				}
				MTreeNode::Leaf(indexmap) => {
					match self.delete_entry(indexmap, v.clone(), doc_id)? {
						// A key has been removed, we need to check if any node adjustment is required
						Deletion::KeyRemoved => {
							if indexmap.len() < self.minimum {
								store.set_node(node, true)?;
								self.deletion_adjustments(tx, store, &mut routing_nodes).await?;
							} else {
								store.set_node(node, true)?;
								self.recompute_radius(tx, store, &mut routing_nodes).await?;
							}
							result = true;
							break;
						}
						// In this case, no key has been removed, the MTree structure did not change
						// There is no need for any adjustment in the node hierarchy
						Deletion::DocRemoved => {
							store.set_node(node, true)?;
							result = true;
							break;
						}
						Deletion::None => {
							store.set_node(node, false)?;
							break;
						}
					}
				}
			}
		}
		// Cleanup
		while let Some((node, _)) = routing_nodes.pop() {
			store.set_node(node, false)?;
		}
		Ok(result)
	}

	fn find_next_child_entry(
		&self,
		entries: &Vec<RoutingEntry>,
		v: &Vector,
	) -> Option<(usize, NodeId)> {
		for (idx, entry) in entries.iter().enumerate() {
			let d = self.calculate_distance(v.as_ref(), entry.center.as_ref());
			if d <= entry.radius {
				return Some((idx, entry.node));
			}
		}
		None
	}

	fn delete_entry(
		&self,
		indexmap: &mut LeafIndexMap,
		v: Arc<Vector>,
		doc_id: DocId,
	) -> Result<Deletion, Error> {
		if let Entry::Occupied(mut e) = indexmap.entry(v) {
			let p = e.get_mut();
			if p.docs.remove(doc_id) {
				if p.docs.is_empty() {
					e.remove();
					return Ok(Deletion::KeyRemoved);
				}
				return Ok(Deletion::DocRemoved);
			}
		}
		Ok(Deletion::None)
	}

	async fn deletion_adjustments(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		routing_nodes: &mut Vec<(StoredNode<MTreeNode>, usize)>,
	) -> Result<(), Error> {
		while let Some((node, idx)) = routing_nodes.pop() {
			if let MTreeNode::Routing(children) = &node.n {
				let child = store.get_node(tx, children[idx].node).await?;
				if child.n.len() < self.minimum {
					let mut predecessor = if idx > 0 {
						self.deletion_sibling_option(tx, store, &child.n, children, idx - 1).await?
					} else {
						Sibling::None
					};
					let mut successor = if idx < children.len() - 1 {
						self.deletion_sibling_option(tx, store, &child.n, children, idx + 1).await?
					} else {
						Sibling::None
					};
					if let Sibling::Borrow(sibling) = predecessor {
						predecessor = Sibling::None;
						self.borrow_from_predecessor(tx, store, node, sibling, child, idx).await?;
					} else if let Sibling::Borrow(sibling) = successor {
						successor = Sibling::None;
						self.borrow_from_successor(store, node, sibling, child)?;
					} else if let Sibling::Merge(sibling) = successor {
						successor = Sibling::None;
						self.merge_nodes(tx, store, node, sibling, child, idx).await?;
					} else if let Sibling::Merge(sibling) = predecessor {
						predecessor = Sibling::None;
						self.merge_nodes(tx, store, node, sibling, child, idx - 1).await?;
					} else {
						store.set_node(child, false)?;
						self.compute_radius(tx, store, node, vec![idx]).await?;
					}
					Self::sibling_cleanup(store, predecessor)?;
					Self::sibling_cleanup(store, successor)?;
				}
				continue;
			}
			self.compute_radius(tx, store, node, vec![idx]).await?;
		}
		Ok(())
	}

	async fn deletion_sibling_option(
		&self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		child: &MTreeNode,
		children: &Vec<RoutingEntry>,
		idx: usize,
	) -> Result<Sibling, Error> {
		let sibling = store.get_node(tx, children[idx].node).await?;
		if !child.is_same_type(&sibling.n) {
			return Ok(Sibling::None);
		}
		Ok(if sibling.n.len() > self.minimum {
			Sibling::Borrow(sibling)
		} else {
			Sibling::Merge(sibling)
		})
	}

	async fn borrow_from_predecessor(
		&self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		parent: StoredNode<MTreeNode>,
		mut predecessor: StoredNode<MTreeNode>,
		mut child: StoredNode<MTreeNode>,
		idx: usize,
	) -> Result<(), Error> {
		match (&mut predecessor.n, &mut child.n) {
			(MTreeNode::Routing(_pre), MTreeNode::Routing(_chi)) => {
				todo!()
			}
			(MTreeNode::Leaf(ref mut pre), MTreeNode::Leaf(ref mut chi)) => {
				if let Some((vec, props)) = pre.pop() {
					let mut new = IndexMap::with_capacity(pre.len() + 1);
					new.insert(vec, props);
					for (k, v) in chi.drain(..) {
						new.insert(k, v);
					}
					*chi = new;
				} else {
					return Err(Error::Unreachable);
				}
			}
			_ => return Err(Error::Unreachable),
		}
		store.set_node(predecessor, true)?;
		store.set_node(child, true)?;
		self.compute_radius(tx, store, parent, vec![idx - 1, idx]).await?;
		Ok(())
	}

	fn borrow_from_successor(
		&self,
		_store: &mut MTreeNodeStore,
		_parent: StoredNode<MTreeNode>,
		_predecessor: StoredNode<MTreeNode>,
		_child: StoredNode<MTreeNode>,
	) -> Result<(), Error> {
		todo!()
	}

	async fn merge_nodes(
		&mut self,
		tx: &mut Transaction,
		store: &mut MTreeNodeStore,
		mut parent: StoredNode<MTreeNode>,
		mut left: StoredNode<MTreeNode>,
		mut right: StoredNode<MTreeNode>,
		left_idx: usize,
	) -> Result<(), Error> {
		match (&mut left.n, &mut right.n) {
			(MTreeNode::Routing(_pre), MTreeNode::Routing(_chi)) => {
				todo!()
			}
			(MTreeNode::Leaf(ref mut l), MTreeNode::Leaf(ref mut r)) => {
				for (_, v) in l.iter_mut() {
					// We set the parent_dist to zero in the case the node becomes the new root
					v.parent_dist = 0.0;
				}
				for (k, mut v) in r.drain(..) {
					v.parent_dist = 0.0;
					l.insert(k, v);
				}
			}
			_ => return Err(Error::Unreachable),
		}
		let left_id = left.id;
		store.set_node(left, true)?;
		store.remove_node(right.id, right.key)?;
		if let MTreeNode::Routing(ref mut r) = parent.n {
			r.remove(left_idx + 1);
			if r.len() == 1 {
				if let Some(root_id) = self.state.root {
					if root_id == parent.id {
						let root = store.get_node(tx, root_id).await?;
						store.remove_node(root.id, root.key)?;
						self.state.root = Some(left_id);
						self.updated = true;
					} else {
						self.compute_radius(tx, store, parent, vec![left_idx]).await?;
					}
				} else {
					// No root node?
					return Err(Error::Unreachable);
				}
			}
		}
		Ok(())
	}

	fn sibling_cleanup(store: &mut MTreeNodeStore, sibling: Sibling) -> Result<(), Error> {
		match sibling {
			Sibling::None => Ok(()),
			Sibling::Borrow(n) | Sibling::Merge(n) => store.set_node(n, false),
		}
	}

	async fn finish(&self, tx: &mut Transaction, key: Key) -> Result<(), Error> {
		if self.updated {
			tx.set(key, self.state.try_to_val()?).await?;
		}
		Ok(())
	}
}

#[derive(PartialEq)]
enum Deletion {
	KeyRemoved,
	DocRemoved,
	None,
}

enum Sibling {
	None,
	Borrow(StoredNode<MTreeNode>),
	Merge(StoredNode<MTreeNode>),
}

#[derive(PartialEq)]
struct PriorityNode(f64, NodeId);

impl PartialOrd for PriorityNode {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		other.0.partial_cmp(&self.0)
	}
}

impl Ord for PriorityNode {
	fn cmp(&self, other: &Self) -> Ordering {
		other.0.total_cmp(&self.0)
	}
}

impl Eq for PriorityNode {}

#[derive(PartialEq, Debug)]
struct PriorityResult(f64, Arc<Vector>, RoaringTreemap);

impl PartialOrd for PriorityResult {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.0.partial_cmp(&other.0)
	}
}

impl Ord for PriorityResult {
	// We want the priority result to be sorted by ascending distance
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.total_cmp(&other.0)
	}
}

impl Eq for PriorityResult {}

trait MTreeNode: TreeNode {}

struct InternalNode(Vec<RoutingEntry>);

struct LeafNode {
	objects: LeafIndexMap,
	max_parent_distance: f64,
}

impl LeafNode {
	fn new_root_leaf(v: Vector, id: DocId) -> Self {
		let p = ObjectProperties::new_root(id);
		let mut objects = IndexMap::with_capacity(1);
		objects.insert(Arc::new(v), p);
		Self {
			objects,
			max_parent_distance: 0.0,
		}
	}
}

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

impl SerdeState for MState {}

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
		let tx = ds.transaction(true, false).await.unwrap();
		(s, tx)
	}

	async fn finish_operation(
		mut tx: Transaction,
		mut s: MutexGuard<'_, TreeNodeStore<MTreeNode>>,
	) {
		s.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
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
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res, vec![]);
			check_docs(&docs, vec![]);
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
			finish_operation(tx, s).await;
		}
		// Check KNN
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res, vec![(&vec1, 0.0)]);
			check_docs(&docs, vec![1]);
		}

		// insert second element
		let vec2 = vec![2.into()];
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec2.clone(), 2).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 0.0, &[2]);
			})
			.await;
			finish_operation(tx, s).await;
		}
		// vec1 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res, vec![(&vec1, 0.0), (&vec2, 1.0)]);
			check_docs(&docs, vec![1, 2]);
		}
		// vec2 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec2, 10).await.unwrap();
			check_knn(&res, vec![(&vec2, 0.0), (&vec1, 1.0)]);
			check_docs(&docs, vec![1, 2]);
		}

		// insert new doc to existing vector
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec2.clone(), 3).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 0.0, &[2, 3]);
			})
			.await;
			finish_operation(tx, s).await;
		}
		// vec2 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec2, 10).await.unwrap();
			check_knn(&res, vec![(&vec2, 0.0), (&vec1, 1.0)]);
			check_docs(&docs, vec![1, 2, 3]);
		}

		// insert third vector
		let vec3 = vec![3.into()];
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec3.clone(), 3).await.unwrap();
			assert_eq!(t.state.root, Some(0));
			check_leaf(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, 0, &vec1, 0.0, &[1]);
				check_leaf_vec(m, 1, &vec2, 0.0, &[2, 3]);
				check_leaf_vec(m, 2, &vec3, 0.0, &[3]);
			})
			.await;
			finish_operation(tx, s).await;
		}
		// vec3 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec3, 10).await.unwrap();
			check_knn(&res, vec![(&vec3, 0.0), (&vec2, 1.0), (&vec1, 2.0)]);
			check_docs(&docs, vec![1, 2, 3]);
		}

		// Check split node
		let vec4 = vec![4.into()];
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
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
			finish_operation(tx, s).await;
		}
		// vec4 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec4, 10).await.unwrap();
			check_knn(&res, vec![(&vec4, 0.0), (&vec3, 1.0), (&vec2, 2.0), (&vec1, 3.0)]);
			check_docs(&docs, vec![1, 2, 3, 4]);
		}

		// Insert vec extending the radius of the last node, calling compute_leaf_radius
		let vec6 = vec![6.into()];
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
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
			finish_operation(tx, s).await;
		}
		// vec6 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
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
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, vec8.clone(), 8).await.unwrap();
			finish_operation(tx, s).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
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
			check_tree_properties(&mut tx, &mut s, &t, 5, 3, Some(2), Some(3), Some(2), Some(2))
				.await;
		}
		// vec8 knn
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
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
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Read).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec4, 2).await.unwrap();
			check_knn(&res, vec![(&vec4, 0.0), (&vec3, 1.0)]);
			check_docs(&docs, vec![3, 4]);
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
			finish_operation(tx, s).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res, vec![(&vec1, 0.0), (&vec2, 1.0)]);
			check_docs(&docs, vec![10, 20, 21]);
			check_tree_properties(&mut tx, &mut s, &t, 1, 1, Some(1), Some(1), Some(2), Some(2))
				.await;
		}

		// Remove the doc 21
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(t.remove(&mut tx, &mut s, vec2.clone(), 21).await.unwrap());
			finish_operation(tx, s).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(&res, vec![(&vec1, 0.0), (&vec2, 1.0)]);
			check_docs(&docs, vec![10, 20]);
			check_tree_properties(&mut tx, &mut s, &t, 1, 1, Some(1), Some(1), Some(2), Some(2))
				.await;
		}

		// Remove again vec2 / 21 => Deletion::None
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(!t.remove(&mut tx, &mut s, vec2.clone(), 21).await.unwrap());
			assert!(!t.remove(&mut tx, &mut s, vec2.clone(), 21).await.unwrap());
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
			finish_operation(tx, s).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(
				&res,
				vec![(&vec1, 0.0), (&vec2, 1.0), (&vec3, 2.0), (&vec4, 3.0), (&vec5, 4.0)],
			);
			check_docs(&docs, vec![10, 20, 30, 40, 51, 51]);
			check_tree_properties(&mut tx, &mut s, &t, 3, 2, Some(2), Some(2), Some(2), Some(3))
				.await;
		}

		// Remove the doc 51
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(t.remove(&mut tx, &mut s, vec5.clone(), 51).await.unwrap());
			finish_operation(tx, s).await;
		}
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &vec1, 10).await.unwrap();
			check_knn(
				&res,
				vec![(&vec1, 0.0), (&vec2, 1.0), (&vec3, 2.0), (&vec4, 3.0), (&vec5, 4.0)],
			);
			check_docs(&docs, vec![10, 20, 30, 40, 50]);
			check_tree_properties(&mut tx, &mut s, &t, 3, 2, Some(2), Some(2), Some(2), Some(3))
				.await;
		}

		// Remove again vec5 / 51 => Deletion::None
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(!t.remove(&mut tx, &mut s, vec5.clone(), 51).await.unwrap());
			assert!(!t.remove(&mut tx, &mut s, vec5.clone(), 51).await.unwrap());
		}
	}

	#[test(tokio::test)]
	async fn test_mtree_deletions_leaf_operations() {
		let ds = Datastore::new("memory").await.unwrap();

		let mut t = MTree::new(MState::new(4), Distance::Euclidean);

		let v0 = vec![0.into()];
		let v1 = vec![1.into()];
		let v2 = vec![2.into()];
		let v3 = vec![3.into()];
		let v4 = vec![4.into()];
		let v5 = vec![5.into()];
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, v1.clone(), 10).await.unwrap();
			t.insert(&mut tx, &mut s, v2.clone(), 20).await.unwrap();
			t.insert(&mut tx, &mut s, v3.clone(), 30).await.unwrap();
			t.insert(&mut tx, &mut s, v4.clone(), 40).await.unwrap();
			t.insert(&mut tx, &mut s, v5.clone(), 50).await.unwrap();
			finish_operation(tx, s).await;
		}

		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &v0, 20).await.unwrap();
			check_knn(&res, vec![(&v1, 1.0), (&v2, 2.0), (&v3, 3.0), (&v4, 4.0), (&v5, 5.0)]);
			check_docs(&docs, vec![10, 20, 30, 40, 50]);
			check_tree_properties(&mut tx, &mut s, &t, 3, 2, Some(2), Some(2), Some(2), Some(3))
				.await;
			assert_eq!(t.state.root, Some(0));
			check_routing(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, 0, &v1, 1, 2.0);
				check_routing_vec(m, 1, &v5, 2, 1.0);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, 0, &v1, 0.0, &[10]);
				check_leaf_vec(m, 1, &v2, 1.0, &[20]);
				check_leaf_vec(m, 2, &v3, 2.0, &[30]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &v4, 1.0, &[40]);
				check_leaf_vec(m, 1, &v5, 0.0, &[50]);
			})
			.await;
		}

		// Remove -> Borrow from predecessor
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(t.remove(&mut tx, &mut s, v4.clone(), 40).await.unwrap());
			finish_operation(tx, s).await;
		}
		// Check -> Borrow from predecessor
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &v0, 20).await.unwrap();
			check_knn(&res, vec![(&v1, 1.0), (&v2, 2.0), (&v3, 3.0), (&v5, 5.0)]);
			check_docs(&docs, vec![10, 20, 30, 50]);
			check_tree_properties(&mut tx, &mut s, &t, 3, 2, Some(2), Some(2), Some(2), Some(2))
				.await;
			assert_eq!(t.state.root, Some(0));
			check_routing(&mut tx, &mut s, 0, |m| {
				assert_eq!(m.len(), 2);
				check_routing_vec(m, 0, &v1, 1, 1.0);
				check_routing_vec(m, 1, &v5, 2, 2.0);
			})
			.await;
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &v1, 0.0, &[10]);
				check_leaf_vec(m, 1, &v2, 1.0, &[20]);
			})
			.await;
			check_leaf(&mut tx, &mut s, 2, |m| {
				assert_eq!(m.len(), 2);
				check_leaf_vec(m, 0, &v3, 2.0, &[30]);
				check_leaf_vec(m, 1, &v5, 0.0, &[50]);
			})
			.await;
		}
		// Remove -> Merge nodes + reduce to root node
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(t.remove(&mut tx, &mut s, v3.clone(), 30).await.unwrap());
			finish_operation(tx, s).await;
		}
		// Check -> Merge nodes + reduce to root node
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &v0, 20).await.unwrap();
			check_knn(&res, vec![(&v1, 1.0), (&v2, 2.0), (&v5, 5.0)]);
			check_docs(&docs, vec![10, 20, 50]);
			check_tree_properties(&mut tx, &mut s, &t, 1, 1, Some(1), Some(1), Some(3), Some(3))
				.await;
			assert_eq!(t.state.root, Some(1));
			check_leaf(&mut tx, &mut s, 1, |m| {
				assert_eq!(m.len(), 3);
				check_leaf_vec(m, 0, &v1, 0.0, &[10]);
				check_leaf_vec(m, 1, &v2, 0.0, &[20]);
				check_leaf_vec(m, 2, &v5, 0.0, &[50]);
			})
			.await;
		}
	}

	#[test(tokio::test)]
	async fn test_mtree_deletions_merge_routing_node() {
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
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			t.insert(&mut tx, &mut s, v9.clone(), 90).await.unwrap();
			t.insert(&mut tx, &mut s, v10.clone(), 100).await.unwrap();
			t.insert(&mut tx, &mut s, v1.clone(), 10).await.unwrap();
			t.insert(&mut tx, &mut s, v2.clone(), 20).await.unwrap();
			t.insert(&mut tx, &mut s, v3.clone(), 30).await.unwrap();
			t.insert(&mut tx, &mut s, v4.clone(), 40).await.unwrap();
			t.insert(&mut tx, &mut s, v5.clone(), 50).await.unwrap();
			t.insert(&mut tx, &mut s, v6.clone(), 60).await.unwrap();
			t.insert(&mut tx, &mut s, v7.clone(), 70).await.unwrap();
			t.insert(&mut tx, &mut s, v8.clone(), 80).await.unwrap();
			finish_operation(tx, s).await;
		}

		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Traversal).await;
			let mut s = s.lock().await;
			let (res, docs) = t.knn_search(&mut tx, &mut s, &v0, 20).await.unwrap();
			check_knn(
				&res,
				vec![
					(&v1, 1.0),
					(&v2, 2.0),
					(&v3, 3.0),
					(&v4, 4.0),
					(&v5, 5.0),
					(&v6, 6.0),
					(&v7, 7.0),
					(&v8, 8.0),
					(&v9, 9.0),
					(&v10, 10.0),
				],
			);
			check_docs(&docs, vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
			check_tree_properties(&mut tx, &mut s, &t, 7, 3, Some(3), Some(3), Some(2), Some(3))
				.await;
		}

		// Remove ->
		{
			let (s, mut tx) = new_operation(&ds, TreeStoreType::Write).await;
			let mut s = s.lock().await;
			assert!(t.remove(&mut tx, &mut s, v9.clone(), 90).await.unwrap());
			finish_operation(tx, s).await;
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

	async fn check_routing<F>(
		tx: &mut Transaction,
		s: &mut MTreeNodeStore,
		node_id: NodeId,
		check_func: F,
	) where
		F: FnOnce(&Vec<RoutingEntry>),
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

	async fn check_tree_properties(
		tx: &mut Transaction,
		s: &mut MTreeNodeStore,
		t: &MTree,
		expected_node_count: usize,
		expected_max_depth: usize,
		expected_min_leaf_depth: Option<usize>,
		expected_max_leaf_depth: Option<usize>,
		expected_min_objects: Option<usize>,
		expected_max_objects: Option<usize>,
	) {
		let mut node_count = 0;
		let mut max_depth = 0;
		let mut min_leaf_depth = None;
		let mut max_leaf_depth = None;
		let mut min_objects = None;
		let mut max_objects = None;
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
			println!(
				"Node id: {} - depth: {} - len: {} - {:?}",
				node.id,
				depth,
				node.n.len(),
				node.n
			);
			match node.n {
				MTreeNode::Routing(children) => {
					let next_depth = depth + 1;
					children.iter().for_each(|p| nodes.push_back((p.node, next_depth)));
				}
				MTreeNode::Leaf(m) => {
					update_min(&mut min_objects, m.len());
					update_max(&mut max_objects, m.len());
					update_min(&mut min_leaf_depth, depth);
					update_max(&mut max_leaf_depth, depth);
				}
			}
		}
		assert_eq!(node_count, expected_node_count, "Node count");
		assert_eq!(max_depth, expected_max_depth, "Max depth");
		assert_eq!(min_leaf_depth, expected_min_leaf_depth, "Min leaf depth");
		assert_eq!(max_leaf_depth, expected_max_leaf_depth, "Max leaf depth");
		assert_eq!(min_objects, expected_min_objects, "Min objects");
		assert_eq!(max_objects, expected_max_objects, "Max objects");
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
