use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt, HashSet};
use anyhow::Result;
use reblessive::tree::Stk;
use revision::{Revisioned, revisioned};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::catalog::{DatabaseDefinition, Distance, MTreeParams, VectorType};
use crate::ctx::Context;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::docids::DocId;
use crate::idx::docids::btdocids::BTreeDocIds;
use crate::idx::planner::checker::MTreeConditionChecker;
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::trees::btree::BStatistics;
use crate::idx::trees::knn::{Ids64, KnnResult, KnnResultBuilder, PriorityNode};
use crate::idx::trees::store::{NodeId, StoredNode, TreeNode, TreeNodeProvider, TreeStore};
use crate::idx::trees::vector::{SharedVector, Vector};
use crate::kvs::{KVValue, Key, Transaction, TransactionType, Val};
use crate::val::{Number, Object, RecordId, Value};

pub struct MTreeIndex {
	ikb: IndexKeyBase,
	dim: usize,
	vector_type: VectorType,
	store: MTreeStore,
	doc_ids: Arc<RwLock<BTreeDocIds>>,
	mtree: Arc<RwLock<MTree>>,
}

struct MTreeSearchContext<'a> {
	ctx: &'a Context,
	pt: SharedVector,
	k: usize,
	store: &'a MTreeStore,
}

impl MTreeIndex {
	pub async fn new(
		txn: &Transaction,
		ikb: IndexKeyBase,
		p: &MTreeParams,
		tt: TransactionType,
	) -> Result<Self> {
		let doc_ids = Arc::new(RwLock::new(
			BTreeDocIds::new(txn, tt, ikb.clone(), p.doc_ids_order, p.doc_ids_cache).await?,
		));
		let state_key = ikb.new_vm_root_key();
		let state: MState = if let Some(val) = txn.get(&state_key, None).await? {
			val
		} else {
			MState::new(p.capacity)
		};
		let store = txn
			.index_caches()
			.get_store_mtree(
				TreeNodeProvider::Vector(ikb.clone()),
				state.generation,
				tt,
				p.mtree_cache as usize,
			)
			.await?;
		let mtree = Arc::new(RwLock::new(MTree::new(state, p.distance.clone())));
		Ok(Self {
			ikb,
			dim: p.dimension as usize,
			vector_type: p.vector_type,
			doc_ids,
			mtree,
			store,
		})
	}

	pub async fn index_document(
		&mut self,
		stk: &mut Stk,
		txn: &Transaction,
		rid: &RecordId,
		content: &[Value],
	) -> Result<()> {
		// Resolve the doc_id
		let mut doc_ids = self.doc_ids.write().await;
		let resolved = doc_ids.resolve_doc_id(txn, rid).await?;
		let doc_id = resolved.doc_id();
		drop(doc_ids);
		// Index the values
		let mut mtree = self.mtree.write().await;
		for v in content.iter().filter(|v| !v.is_nullish()) {
			// Extract the vector
			let vector = Vector::try_from_value(self.vector_type, self.dim, v)?;
			vector.check_dimension(self.dim)?;
			// Insert the vector in the index
			mtree.insert(stk, txn, &mut self.store, vector.into(), doc_id).await?;
		}
		drop(mtree);
		Ok(())
	}

	pub async fn remove_document(
		&mut self,
		stk: &mut Stk,
		txn: &Transaction,
		rid: &RecordId,
		content: &[Value],
	) -> Result<()> {
		let mut doc_ids = self.doc_ids.write().await;
		let doc_id = doc_ids.remove_doc(txn, rid).await?;
		drop(doc_ids);
		if let Some(doc_id) = doc_id {
			// Lock the index
			let mut mtree = self.mtree.write().await;
			for v in content.iter().filter(|v| !v.is_nullish()) {
				// Extract the vector
				let vector = Vector::try_from_value(self.vector_type, self.dim, v)?;
				vector.check_dimension(self.dim)?;
				// Remove the vector
				mtree.delete(stk, txn, &mut self.store, vector.into(), doc_id).await?;
			}
			drop(mtree);
		}
		Ok(())
	}

	pub async fn knn_search(
		&self,
		db: &DatabaseDefinition,
		stk: &mut Stk,
		ctx: &Context,
		v: &[Number],
		k: usize,
		mut chk: MTreeConditionChecker<'_>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		// Extract the vector
		let vector = Vector::try_from_vector(self.vector_type, v)?;
		vector.check_dimension(self.dim)?;
		// Build the search context
		let search = MTreeSearchContext {
			ctx,
			pt: vector.into(),
			k,
			store: &self.store,
		};
		// Lock the tree and the docs
		let mtree = self.mtree.read().await;
		let doc_ids = self.doc_ids.read().await;
		// Do the search
		let res = mtree.knn_search(db, &search, &doc_ids, stk, &mut chk).await?;
		drop(mtree);
		// Resolve the doc_id to Thing and the optional value
		let res = chk.convert_result(&doc_ids, res.docs).await;
		drop(doc_ids);
		res
	}

	pub(crate) async fn statistics(&self, tx: &Transaction) -> Result<MtStatistics> {
		Ok(MtStatistics {
			doc_ids: self.doc_ids.read().await.statistics(tx).await?,
		})
	}

	pub async fn finish(&mut self, tx: &Transaction) -> Result<()> {
		let mut doc_ids = self.doc_ids.write().await;
		doc_ids.finish(tx).await?;
		drop(doc_ids);
		let mut mtree = self.mtree.write().await;
		if let Some(new_cache) = self.store.finish(tx).await? {
			mtree.state.generation += 1;
			let state_key = self.ikb.new_vm_root_key();
			tx.set(&state_key, &mtree.state, None).await?;
			tx.index_caches().advance_store_mtree(new_cache);
		}
		drop(mtree);
		Ok(())
	}
}

// https://en.wikipedia.org/wiki/M-tree
// https://arxiv.org/pdf/1004.4216.pdf
struct MTree {
	state: MState,
	distance: Distance,
	minimum: usize,
}

impl MTree {
	fn new(state: MState, distance: Distance) -> Self {
		let minimum = (state.capacity + 1) as usize / 2;
		Self {
			state,
			distance,
			minimum,
		}
	}

	async fn knn_search(
		&self,
		db: &DatabaseDefinition,
		search: &MTreeSearchContext<'_>,
		doc_ids: &BTreeDocIds,
		stk: &mut Stk,
		chk: &mut MTreeConditionChecker<'_>,
	) -> Result<KnnResult> {
		#[cfg(debug_assertions)]
		debug!("knn_search - pt: {:?} - k: {}", search.pt, search.k);
		let mut queue = BinaryHeap::new();
		let mut res = KnnResultBuilder::new(search.k);
		if let Some(root_id) = self.state.root {
			queue.push(PriorityNode::new(0.0, root_id));
		}
		#[cfg(debug_assertions)]
		let mut visited_nodes = HashMap::default();
		while let Some(e) = queue.pop() {
			let id = e.id();
			let node = search.store.get_node_txn(search.ctx, id).await?;
			#[cfg(debug_assertions)]
			{
				debug!("Visit node id: {}", id);
				if visited_nodes.insert(id, node.n.len()).is_some() {
					fail!("MTree::knn_search")
				}
			}
			match node.n {
				MTreeNode::Leaf(ref n) => {
					#[cfg(debug_assertions)]
					debug!("Leaf found - id: {} - len: {}", node.id, n.len(),);
					for (o, p) in n {
						let d = self.calculate_distance(o, &search.pt)?;
						if res.check_add(d) {
							#[cfg(debug_assertions)]
							debug!("Add: {d} - obj: {o:?} - docs: {:?}", p.docs);
							let mut docs = Ids64::Empty;
							for doc in &p.docs {
								if chk.check_truthy(db, stk, doc_ids, doc).await? {
									if let Some(new_docs) = docs.insert(doc) {
										docs = new_docs;
									}
								}
							}
							if !docs.is_empty() {
								let evicted_docs = res.add(d, docs);
								chk.expires(evicted_docs);
							}
						}
					}
				}
				MTreeNode::Internal(ref n) => {
					#[cfg(debug_assertions)]
					debug!("Internal found - id: {} - {:?}", node.id, n);
					for (o, p) in n {
						let d = self.calculate_distance(o, &search.pt)?;
						let min_dist = (d - p.radius).max(0.0);
						if res.check_add(min_dist) {
							debug!("Queue add - dist: {} - node: {}", min_dist, p.node);
							queue.push(PriorityNode::new(min_dist, p.node));
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

	async fn insert(
		&mut self,
		stk: &mut Stk,
		tx: &Transaction,
		store: &mut MTreeStore,
		obj: SharedVector,
		id: DocId,
	) -> Result<()> {
		#[cfg(debug_assertions)]
		debug!("Insert - obj: {:?} - doc: {}", obj, id);
		// First we check if we already have the object. In this case we just append the
		// doc.
		if self.append(tx, store, &obj, id).await? {
			return Ok(());
		}
		if let Some(root_id) = self.state.root {
			let node = store.get_node_mut(tx, root_id).await?;
			// Otherwise, we insert the object with possibly mutating the tree
			if let InsertionResult::PromotedEntries(o1, p1, o2, p2) =
				self.insert_at_node(stk, tx, store, node, &None, obj, id).await?
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
	) -> Result<()> {
		let new_root_id = self.new_node_id();
		let p = ObjectProperties::new_root(id);
		let mut objects = LeafMap::with_capacity(1);
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
	) -> Result<()> {
		let new_root_id = self.new_node_id();
		#[cfg(debug_assertions)]
		debug!(
			"New internal root - node: {} - e1.node: {} - e1.obj: {:?} - e1.radius: {} - e2.node: {} - e2.obj: {:?} - e2.radius: {}",
			new_root_id, p1.node, o1, p1.radius, p2.node, o2, p2.radius
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
		tx: &Transaction,
		store: &mut MTreeStore,
		object: &SharedVector,
		id: DocId,
	) -> Result<bool> {
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

	/// Was marked recursive
	#[expect(clippy::too_many_arguments)]
	async fn insert_at_node(
		&mut self,
		stk: &mut Stk,
		tx: &Transaction,
		store: &mut MTreeStore,
		node: MStoredNode,
		parent_center: &Option<SharedVector>,
		object: SharedVector,
		doc: DocId,
	) -> Result<InsertionResult> {
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
					stk,
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

	#[expect(clippy::too_many_arguments)]
	async fn insert_node_internal(
		&mut self,
		stk: &mut Stk,
		tx: &Transaction,
		store: &mut MTreeStore,
		node_id: NodeId,
		node_key: Key,
		mut node: InternalNode,
		parent_center: &Option<SharedVector>,
		object: SharedVector,
		doc_id: DocId,
	) -> Result<InsertionResult> {
		// Choose `best` subtree entry ObestSubstree from N;
		let (best_entry_obj, mut best_entry) = self.find_closest(&node, &object)?;
		let best_node = store.get_node_mut(tx, best_entry.node).await?;
		// Insert(Oi, child(ObestSubstree), ObestSubtree);
		let best_entry_obj_op = Some(best_entry_obj.clone());
		let this = &mut *self;
		match stk
			.run(|stk| async {
				this.insert_at_node(stk, tx, store, best_node, &best_entry_obj_op, object, doc_id)
					.await
			})
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
				let mut nup: HashSet<SharedVector> = HashSet::from_iter(node.keys().cloned());
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
	) -> Result<(SharedVector, RoutingProperties)> {
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
			fail!("MTree::find_closest")
		}
	}

	#[expect(clippy::too_many_arguments)]
	async fn insert_node_leaf(
		&mut self,
		store: &mut MTreeStore,
		node_id: NodeId,
		node_key: Key,
		mut node: LeafNode,
		parent_center: &Option<SharedVector>,
		object: SharedVector,
		doc_id: DocId,
	) -> Result<InsertionResult> {
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
	) -> Result<(SharedVector, RoutingProperties, SharedVector, RoutingProperties)>
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
			fail!("MTree::split_node")
		}
		Ok((o1, p1, o2, p2))
	}

	// Compute the distance cache, and return the most distant objects
	fn compute_distances_and_promoted_objects(
		&self,
		objects: &[SharedVector],
	) -> Result<(DistanceCache, SharedVector, SharedVector)> {
		let mut promo = None;
		let mut max_dist = 0f64;
		let n = objects.len();
		let mut dist_cache = HashMap::new();
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
			None => fail!("MTree::compute_distances_and_promoted_objects"),
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
	) -> Result<f64> {
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

	fn calculate_distance(&self, v1: &SharedVector, v2: &SharedVector) -> Result<f64> {
		if v1.eq(v2) {
			return Ok(0.0);
		}
		let dist = self.distance.calculate(v1, v2);
		if dist.is_finite() {
			Ok(dist)
		} else {
			Err(anyhow::Error::new(Error::InvalidVectorDistance {
				left: v1.clone(),
				right: v2.clone(),
				dist,
			}))
		}
	}

	async fn delete(
		&mut self,
		stk: &mut Stk,
		tx: &Transaction,
		store: &mut MTreeStore,
		object: SharedVector,
		doc_id: DocId,
	) -> Result<bool> {
		let mut deleted = false;
		if let Some(root_id) = self.state.root {
			let root_node = store.get_node_mut(tx, root_id).await?;
			if let DeletionResult::Underflown(sn, n_updated) = self
				.delete_at_node(stk, tx, store, root_node, &None, object, doc_id, &mut deleted)
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
							let e = n
								.values()
								.next()
								.ok_or_else(|| Error::unreachable("MTree::delete"))?;
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

	/// Was marked recursive
	#[expect(clippy::too_many_arguments)]
	async fn delete_at_node(
		&mut self,
		stk: &mut Stk,
		tx: &Transaction,
		store: &mut MTreeStore,
		node: MStoredNode,
		parent_center: &Option<SharedVector>,
		object: SharedVector,
		id: DocId,
		deleted: &mut bool,
	) -> Result<DeletionResult> {
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
					stk,
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

	#[expect(clippy::too_many_arguments)]
	async fn delete_node_internal(
		&mut self,
		stk: &mut Stk,
		tx: &Transaction,
		store: &mut MTreeStore,
		node_id: NodeId,
		node_key: Key,
		mut n_node: InternalNode,
		parent_center: &Option<SharedVector>,
		od: SharedVector,
		id: DocId,
		deleted: &mut bool,
	) -> Result<DeletionResult> {
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
			let on_obj_op = Some(on_obj.clone());
			match stk
				.run(|stk| {
					self.delete_at_node(
						stk,
						tx,
						store,
						on_node,
						&on_obj_op,
						od.clone(),
						id,
						deleted,
					)
				})
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
	) -> Result<DeletionResult> {
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
	) -> Result<()> {
		store.set_node(StoredNode::new(node, node_id, node_key, 0), updated).await?;
		Ok(())
	}

	#[expect(clippy::too_many_arguments)]
	async fn deletion_underflown(
		&mut self,
		tx: &Transaction,
		store: &mut MTreeStore,
		parent_center: &Option<SharedVector>,
		n_node: &mut InternalNode,
		on_obj: SharedVector,
		p: MStoredNode,
		p_updated: bool,
	) -> Result<bool> {
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

	#[expect(clippy::too_many_arguments)]
	async fn delete_underflown_fit_into_child(
		&mut self,
		store: &mut MTreeStore,
		n_node: &mut InternalNode,
		on_obj: SharedVector,
		p: MStoredNode,
		onn_obj: SharedVector,
		mut onn_entry: RoutingProperties,
		mut onn_child: MStoredNode,
	) -> Result<()> {
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

	#[expect(clippy::too_many_arguments)]
	async fn delete_underflown_redistribute(
		&mut self,
		store: &mut MTreeStore,
		parent_center: &Option<SharedVector>,
		n_node: &mut InternalNode,
		on_obj: SharedVector,
		onn_obj: SharedVector,
		mut p: MStoredNode,
		onn_child: MStoredNode,
	) -> Result<()> {
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

	#[expect(clippy::too_many_arguments)]
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
	) -> Result<DeletionResult> {
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

struct DistanceCache(HashMap<(SharedVector, SharedVector), f64>);

pub(in crate::idx) type MTreeStore = TreeStore<MTreeNode>;
type MStoredNode = StoredNode<MTreeNode>;

type InternalMap = HashMap<SharedVector, RoutingProperties>;

type LeafMap = HashMap<SharedVector, ObjectProperties>;

#[derive(Debug, Clone)]
/// A node in this tree structure holds entries.
/// Each entry is a tuple consisting of an object and its associated properties.
/// It's essential to note that the properties vary between a LeafNode and an
/// InternalNode. Both LeafNodes and InternalNodes are implemented as a map.
/// In this map, the key is an object, and the values correspond to its
/// properties. In essence, an entry can be visualized as a tuple of the form
/// (object, properties).
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

	fn internal(self) -> Result<InternalNode> {
		match self {
			MTreeNode::Internal(n) => Ok(n),
			MTreeNode::Leaf(_) => fail!("MTreeNode::internal"),
		}
	}

	fn leaf(self) -> Result<LeafNode> {
		match self {
			MTreeNode::Internal(_) => fail!("MTreeNode::lead"),
			MTreeNode::Leaf(n) => Ok(n),
		}
	}

	fn merge(&mut self, other: MTreeNode) -> Result<()> {
		match (self, other) {
			(MTreeNode::Internal(s), MTreeNode::Internal(o)) => {
				Self::merge_internal(s, o);
				Ok(())
			}
			(MTreeNode::Leaf(s), MTreeNode::Leaf(o)) => {
				Self::merge_leaf(s, o);
				Ok(())
			}
			(_, _) => fail!("MTreeNode::merge"),
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
	#[expect(dead_code)]
	fn len(&self) -> usize;

	fn get_objects(&self) -> Vec<SharedVector>;

	fn extract_node(
		&mut self,
		distances: &DistanceCache,
		p: SharedVector,
		a: Vec<SharedVector>,
	) -> Result<(Self, f64, SharedVector)>;

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
	) -> Result<(Self, f64, SharedVector)> {
		let mut n = LeafNode::new();
		let mut r = 0f64;
		for o in a {
			let mut props = self
				.remove(&o)
				.ok_or_else(|| Error::unreachable("NodeVectors/LeafNode::extract_node)"))?;
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
	) -> Result<(Self, f64, SharedVector)> {
		let mut n = InternalNode::new();
		let mut max_r = 0f64;
		for o in a {
			let mut props = self
				.remove(&o)
				.ok_or_else(|| Error::unreachable("NodeVectors/InternalNode::extract_node"))?;
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
	fn try_from_val(val: Val) -> Result<Self> {
		let mut c: Cursor<Vec<u8>> = Cursor::new(val);
		let node_type: u8 = bincode::deserialize_from(&mut c)?;
		match node_type {
			1u8 => {
				let objects: LeafNode = bincode::deserialize_from(c)?;
				Ok(MTreeNode::Leaf(objects))
			}
			2u8 => {
				let entries: InternalNode = bincode::deserialize_from(c)?;
				Ok(MTreeNode::Internal(entries))
			}
			_ => Err(anyhow::Error::new(Error::CorruptedIndex("MTreeNode::try_from_val"))),
		}
	}

	fn try_into_val(&self) -> Result<Val> {
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

#[revisioned(revision = 2)]
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct MState {
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
pub struct RoutingProperties {
	// Reference to the node
	node: NodeId,
	// Distance to its parent object
	parent_dist: f64,
	// Covering radius
	radius: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

impl KVValue for MState {
	#[inline]
	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut val = Vec::new();
		self.serialize_revisioned(&mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(val: Vec<u8>) -> Result<Self> {
		Ok(Self::deserialize_revisioned(&mut val.as_slice())?)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::VecDeque;
	use std::sync::Arc;

	use ahash::{HashMap, HashMapExt, HashSet};
	use anyhow::Result;
	use reblessive::tree::Stk;
	use test_log::test;

	use crate::catalog::{DatabaseDefinition, DatabaseId, Distance, NamespaceId, VectorType};
	use crate::ctx::{Context, MutableContext};
	use crate::idx::IndexKeyBase;
	use crate::idx::docids::DocId;
	use crate::idx::docids::btdocids::BTreeDocIds;
	use crate::idx::planner::checker::MTreeConditionChecker;
	use crate::idx::trees::knn::tests::TestCollection;
	use crate::idx::trees::mtree::{MState, MTree, MTreeNode, MTreeSearchContext, MTreeStore};
	use crate::idx::trees::store::{NodeId, TreeNodeProvider, TreeStore};
	use crate::idx::trees::vector::SharedVector;
	use crate::kvs::LockType::*;
	use crate::kvs::{Datastore, Transaction, TransactionType};

	async fn get_db(ds: &Datastore) -> Arc<DatabaseDefinition> {
		let tx = ds.transaction(TransactionType::Write, Optimistic).await.unwrap();
		tx.ensure_ns_db("myns", "mydb", false).await.unwrap()
	}

	async fn new_operation(
		ds: &Datastore,
		t: &MTree,
		tt: TransactionType,
		cache_size: usize,
	) -> (Context, TreeStore<MTreeNode>) {
		let tx = ds.transaction(tt, Optimistic).await.unwrap().enclose();
		let st = tx
			.index_caches()
			.get_store_mtree(TreeNodeProvider::Debug, t.state.generation, tt, cache_size)
			.await
			.unwrap();
		let mut ctx = MutableContext::default();
		ctx.set_transaction(tx);
		(ctx.freeze(), st)
	}

	async fn finish_operation(
		t: &mut MTree,
		tx: &Transaction,
		mut st: TreeStore<MTreeNode>,
		commit: bool,
	) -> Result<()> {
		if let Some(new_cache) = st.finish(tx).await? {
			assert!(new_cache.len() > 0, "new_cache.len() = {}", new_cache.len());
			t.state.generation += 1;
			tx.index_caches().advance_store_mtree(new_cache);
		}
		if commit {
			tx.commit().await?;
			Ok(())
		} else {
			tx.cancel().await
		}
	}

	async fn insert_collection_one_by_one(
		stk: &mut Stk,
		ds: &Datastore,
		t: &mut MTree,
		collection: &TestCollection,
		cache_size: usize,
	) -> Result<HashMap<DocId, SharedVector>> {
		let mut map = HashMap::with_capacity(collection.len());
		let mut c = 0;
		for (doc_id, obj) in collection.to_vec_ref() {
			{
				let (ctx, mut st) = new_operation(ds, t, TransactionType::Write, cache_size).await;
				let tx = ctx.tx();
				t.insert(stk, &tx, &mut st, obj.clone(), *doc_id).await?;
				finish_operation(t, &tx, st, true).await?;
				map.insert(*doc_id, obj.clone());
			}
			c += 1;
			{
				let (ctx, mut st) = new_operation(ds, t, TransactionType::Read, cache_size).await;
				let tx = ctx.tx();
				let p = check_tree_properties(&tx, &mut st, t).await?;
				assert_eq!(p.doc_count, c);
			}
		}
		Ok(map)
	}

	async fn insert_collection_batch(
		stk: &mut Stk,
		ds: &Datastore,
		t: &mut MTree,
		collection: &TestCollection,
		cache_size: usize,
	) -> Result<HashMap<DocId, SharedVector>> {
		let mut map = HashMap::with_capacity(collection.len());
		{
			let (ctx, mut st) = new_operation(ds, t, TransactionType::Write, cache_size).await;
			let tx = ctx.tx();
			for (doc_id, obj) in collection.to_vec_ref() {
				t.insert(stk, &tx, &mut st, obj.clone(), *doc_id).await?;
				map.insert(*doc_id, obj.clone());
			}
			finish_operation(t, &tx, st, true).await?;
		}
		{
			let (ctx, mut st) = new_operation(ds, t, TransactionType::Read, cache_size).await;
			let tx = ctx.tx();
			check_tree_properties(&tx, &mut st, t).await?;
		}
		Ok(map)
	}

	async fn delete_collection(
		db: &DatabaseDefinition,
		stk: &mut Stk,
		ds: &Datastore,
		doc_ids: &BTreeDocIds,
		t: &mut MTree,
		collection: &TestCollection,
		cache_size: usize,
	) -> Result<()> {
		let mut all_deleted = true;
		for (doc_id, obj) in collection.to_vec_ref() {
			let deleted = {
				debug!("### Remove {} {:?}", doc_id, obj);
				let (ctx, mut st) = new_operation(ds, t, TransactionType::Write, cache_size).await;
				let tx = ctx.tx();
				let deleted = t.delete(stk, &tx, &mut st, obj.clone(), *doc_id).await?;
				finish_operation(t, &tx, st, true).await?;
				drop(tx);
				deleted
			};
			all_deleted = all_deleted && deleted;
			if deleted {
				let (ctx, st) = new_operation(ds, t, TransactionType::Read, cache_size).await;
				let mut chk = MTreeConditionChecker::new(&ctx);
				let search = MTreeSearchContext {
					ctx: &ctx,
					pt: obj.clone(),
					k: 1,
					store: &st,
				};
				let res = t.knn_search(db, &search, doc_ids, stk, &mut chk).await?;
				assert!(
					!res.docs.iter().any(|(id, _)| id == doc_id),
					"Found: {} {:?}",
					doc_id,
					obj
				);
			} else {
				// In v1.2.x deletion is experimental. Will be fixed in 1.3
				warn!("Delete failed: {} {:?}", doc_id, obj);
			}
			{
				let (ctx, mut st) = new_operation(ds, t, TransactionType::Read, cache_size).await;
				let tx = ctx.tx();
				check_tree_properties(&tx, &mut st, t).await?;
				drop(tx);
			}
		}

		if all_deleted {
			let (ctx, mut st) = new_operation(ds, t, TransactionType::Read, cache_size).await;
			let tx = ctx.tx();
			check_tree_properties(&tx, &mut st, t).await?.check(0, 0, None, None, 0, 0);
			drop(tx);
		}
		Ok(())
	}

	async fn find_collection(
		db: &DatabaseDefinition,
		stk: &mut Stk,
		ds: &Datastore,
		doc_ids: &BTreeDocIds,
		t: &mut MTree,
		collection: &TestCollection,
		cache_size: usize,
	) -> Result<()> {
		let (ctx, mut st) = new_operation(ds, t, TransactionType::Read, cache_size).await;
		let max_knn = 20.max(collection.len());
		for (doc_id, obj) in collection.to_vec_ref() {
			for knn in 1..max_knn {
				let mut chk = MTreeConditionChecker::new(&ctx);
				let search = MTreeSearchContext {
					ctx: &ctx,
					pt: obj.clone(),
					k: knn,
					store: &st,
				};
				let res = t.knn_search(db, &search, doc_ids, stk, &mut chk).await?;
				let docs: Vec<DocId> = res.docs.iter().map(|(d, _)| *d).collect();
				if collection.is_unique() {
					assert!(
						docs.contains(doc_id),
						"Search: {:?} - Knn: {} - Wrong Doc - Expected: {} - Got: {:?}",
						obj,
						knn,
						doc_id,
						res.docs
					);
				}
				let expected_len = collection.len().min(knn);
				if expected_len != res.docs.len() {
					#[cfg(debug_assertions)]
					debug!("{:?}", res.visited_nodes);
					let tx = ctx.tx();
					check_tree_properties(&tx, &mut st, t).await?;
				}
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
		Ok(())
	}

	async fn check_full_knn(
		db: &DatabaseDefinition,
		stk: &mut Stk,
		ds: &Datastore,
		doc_ids: &BTreeDocIds,
		t: &mut MTree,
		map: &HashMap<DocId, SharedVector>,
		cache_size: usize,
	) -> Result<()> {
		let (ctx, st) = new_operation(ds, t, TransactionType::Read, cache_size).await;
		for obj in map.values() {
			let mut chk = MTreeConditionChecker::new(&ctx);
			let search = MTreeSearchContext {
				ctx: &ctx,
				pt: obj.clone(),
				k: map.len(),
				store: &st,
			};
			let res = t.knn_search(db, &search, doc_ids, stk, &mut chk).await?;
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
			for (doc, d) in res.docs {
				let o = &map[&doc];
				debug!("doc: {doc} - d: {d} - {obj:?} - {o:?}");
				assert!(d >= dist, "d: {d} - dist: {dist}");
				dist = d;
			}
		}
		Ok(())
	}

	#[expect(clippy::too_many_arguments)]
	async fn test_mtree_collection(
		stk: &mut Stk,
		capacities: &[u16],
		vector_type: VectorType,
		collection: TestCollection,
		check_find: bool,
		check_full: bool,
		check_delete: bool,
		cache_size: usize,
	) -> Result<()> {
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
					collection.len(),
					vector_type,
				);
				let ds = Datastore::new("memory").await?;
				let db = get_db(&ds).await;

				let mut t = MTree::new(MState::new(*capacity), distance.clone());

				let (ctx, _st) = new_operation(&ds, &t, TransactionType::Read, cache_size).await;
				let tx = ctx.tx();
				let doc_ids = BTreeDocIds::new(
					&tx,
					TransactionType::Read,
					IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb", "ix"),
					7,
					100,
				)
				.await
				.unwrap();

				let map = if collection.len() < 1000 {
					insert_collection_one_by_one(stk, &ds, &mut t, &collection, cache_size).await?
				} else {
					insert_collection_batch(stk, &ds, &mut t, &collection, cache_size).await?
				};
				if check_find {
					find_collection(&db, stk, &ds, &doc_ids, &mut t, &collection, cache_size)
						.await?;
				}
				if check_full {
					check_full_knn(&db, stk, &ds, &doc_ids, &mut t, &map, cache_size).await?;
				}
				if check_delete {
					delete_collection(&db, stk, &ds, &doc_ids, &mut t, &collection, cache_size)
						.await?;
				}
			}
		}
		Ok(())
	}

	#[test(tokio::test)]
	#[ignore]
	async fn test_mtree_unique_xs() -> Result<()> {
		let mut stack = reblessive::tree::TreeStack::new();
		stack
			.enter(|stk| async {
				for vt in [
					VectorType::F64,
					VectorType::F32,
					VectorType::I64,
					VectorType::I32,
					VectorType::I16,
				] {
					for i in 0..30 {
						test_mtree_collection(
							stk,
							&[3, 40],
							vt,
							TestCollection::new(true, i, vt, 2, &Distance::Euclidean),
							true,
							true,
							true,
							100,
						)
						.await?;
					}
				}
				Ok(())
			})
			.finish()
			.await
	}

	#[test(tokio::test)]
	#[ignore]
	async fn test_mtree_unique_xs_full_cache() -> Result<()> {
		let mut stack = reblessive::tree::TreeStack::new();
		stack
			.enter(|stk| async {
				for vt in [
					VectorType::F64,
					VectorType::F32,
					VectorType::I64,
					VectorType::I32,
					VectorType::I16,
				] {
					for i in 0..30 {
						test_mtree_collection(
							stk,
							&[3, 40],
							vt,
							TestCollection::new(true, i, vt, 2, &Distance::Euclidean),
							true,
							true,
							true,
							0,
						)
						.await?;
					}
				}
				Ok(())
			})
			.finish()
			.await
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[ignore]
	async fn test_mtree_unique_small() -> Result<()> {
		let mut stack = reblessive::tree::TreeStack::new();
		stack
			.enter(|stk| async {
				for vt in [VectorType::F64, VectorType::I64] {
					test_mtree_collection(
						stk,
						&[10, 20],
						vt,
						TestCollection::new(true, 150, vt, 3, &Distance::Euclidean),
						true,
						true,
						false,
						0,
					)
					.await?;
				}
				Ok(())
			})
			.finish()
			.await
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_mtree_unique_normal() -> Result<()> {
		let mut stack = reblessive::tree::TreeStack::new();
		stack
			.enter(|stk| async {
				for vt in [VectorType::F32, VectorType::I32] {
					test_mtree_collection(
						stk,
						&[40],
						vt,
						TestCollection::new(true, 500, vt, 5, &Distance::Euclidean),
						false,
						true,
						false,
						100,
					)
					.await?;
				}
				Ok(())
			})
			.finish()
			.await
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_mtree_unique_normal_full_cache() -> Result<()> {
		let mut stack = reblessive::tree::TreeStack::new();
		stack
			.enter(|stk| async {
				for vt in [VectorType::F32, VectorType::I32] {
					test_mtree_collection(
						stk,
						&[40],
						vt,
						TestCollection::new(true, 500, vt, 5, &Distance::Euclidean),
						false,
						true,
						false,
						0,
					)
					.await?;
				}
				Ok(())
			})
			.finish()
			.await
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_mtree_unique_normal_small_cache() -> Result<()> {
		let mut stack = reblessive::tree::TreeStack::new();
		stack
			.enter(|stk| async {
				for vt in [VectorType::F32, VectorType::I32] {
					test_mtree_collection(
						stk,
						&[40],
						vt,
						TestCollection::new(true, 500, vt, 5, &Distance::Euclidean),
						false,
						true,
						false,
						10,
					)
					.await?;
				}
				Ok(())
			})
			.finish()
			.await
	}

	#[test(tokio::test)]
	#[ignore]
	async fn test_mtree_random_xs() -> Result<()> {
		let mut stack = reblessive::tree::TreeStack::new();
		stack
			.enter(|stk| async {
				for vt in [
					VectorType::F64,
					VectorType::F32,
					VectorType::I64,
					VectorType::I32,
					VectorType::I16,
				] {
					for collection_size in [0, 1, 5, 10, 15, 20, 30, 40] {
						test_mtree_collection(
							stk,
							&[3, 10, 40],
							vt,
							TestCollection::new(
								false,
								collection_size,
								vt,
								1,
								&Distance::Euclidean,
							),
							true,
							true,
							true,
							0,
						)
						.await?;
					}
				}
				Ok(())
			})
			.finish()
			.await
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	#[ignore]
	async fn test_mtree_random_small() -> Result<()> {
		let mut stack = reblessive::tree::TreeStack::new();
		stack
			.enter(|stk| async {
				for vt in [VectorType::F64, VectorType::I64] {
					test_mtree_collection(
						stk,
						&[10, 20],
						vt,
						TestCollection::new(false, 150, vt, 3, &Distance::Euclidean),
						true,
						true,
						false,
						0,
					)
					.await?;
				}
				Ok(())
			})
			.finish()
			.await
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_mtree_random_normal() -> Result<()> {
		let mut stack = reblessive::tree::TreeStack::new();
		stack
			.enter(|stk| async {
				for vt in [VectorType::F32, VectorType::I32] {
					test_mtree_collection(
						stk,
						&[40],
						vt,
						TestCollection::new(false, 500, vt, 5, &Distance::Euclidean),
						false,
						true,
						false,
						0,
					)
					.await?;
				}
				Ok(())
			})
			.finish()
			.await
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
		tx: &Transaction,
		st: &mut MTreeStore,
		t: &MTree,
	) -> Result<CheckedProperties> {
		debug!("CheckTreeProperties");
		let mut node_ids = HashSet::default();
		let mut checks = CheckedProperties::default();
		let mut nodes: VecDeque<(NodeId, f64, Option<SharedVector>, usize)> = VecDeque::new();
		if let Some(root_id) = t.state.root {
			nodes.push_back((root_id, 0.0, None, 1));
		}
		let mut leaf_objects = HashSet::default();
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
							assert_eq!(
								pd, p.parent_dist,
								"Invalid parent distance ({}): {} - Expected: {} - Node Id: {} - Obj: {:?} - Center: {:?}",
								p.parent_dist, t.distance, pd, node_id, o, center
							);
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
}
