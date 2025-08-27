use std::collections::VecDeque;
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::marker::PhantomData;

#[cfg(debug_assertions)]
use ahash::HashSet;
use anyhow::{Result, bail};
use revision::{Revisioned, revisioned};
use serde::{Deserialize, Serialize};

use crate::err::Error;
use crate::idx::trees::bkeys::BKeys;
use crate::idx::trees::store::{NodeId, StoreGeneration, StoredNode, TreeNode, TreeStore};
use crate::kvs::{KVValue, Key, Transaction, Val};
use crate::val::{Object, Value};

pub type Payload = u64;

type BStoredNode<BK> = StoredNode<BTreeNode<BK>>;
pub(in crate::idx) type BTreeStore<BK> = TreeStore<BTreeNode<BK>>;

pub struct BTree<BK>
where
	BK: BKeys,
{
	state: BState,
	full_size: u32,
	bk: PhantomData<BK>,
}

#[revisioned(revision = 2)]
#[derive(Clone, Serialize, Deserialize)]
pub struct BState {
	minimum_degree: u32,
	root: Option<NodeId>,
	next_node_id: NodeId,
	#[revision(start = 2)]
	generation: StoreGeneration,
}

impl KVValue for BState {
	#[inline]
	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut val = Vec::new();
		self.serialize_revisioned(&mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(val: Vec<u8>) -> anyhow::Result<Self> {
		match Self::deserialize_revisioned(&mut val.as_slice()) {
			Ok(r) => Ok(r),
			// If it fails here, there is the chance it was an old version of BState
			// that included the #[serde[skip]] updated parameter
			Err(e) => match BState1skip::deserialize_revisioned(&mut val.as_slice()) {
				Ok(b_old) => Ok(b_old.into()),
				Err(_) => match BState1::deserialize_revisioned(&mut val.as_slice()) {
					Ok(b_old) => Ok(b_old.into()),
					// Otherwise we return the initial error
					Err(_) => Err(anyhow::Error::new(Error::Revision(e))),
				},
			},
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Serialize, Deserialize)]
pub(in crate::idx) struct BState1 {
	minimum_degree: u32,
	root: Option<NodeId>,
	next_node_id: NodeId,
}

#[revisioned(revision = 1)]
#[derive(Clone, Serialize, Deserialize)]
pub(in crate::idx) struct BState1skip {
	minimum_degree: u32,
	root: Option<NodeId>,
	next_node_id: NodeId,
	#[serde(skip)]
	updated: bool,
}

impl From<BState1> for BState {
	fn from(o: BState1) -> Self {
		Self {
			minimum_degree: o.minimum_degree,
			root: o.root,
			next_node_id: o.next_node_id,
			generation: 0,
		}
	}
}

impl From<BState1skip> for BState {
	fn from(o: BState1skip) -> Self {
		Self {
			minimum_degree: o.minimum_degree,
			root: o.root,
			next_node_id: o.next_node_id,
			generation: 0,
		}
	}
}

impl BState {
	pub fn new(minimum_degree: u32) -> Self {
		assert!(minimum_degree >= 2, "Minimum degree should be >= 2");
		Self {
			minimum_degree,
			root: None,
			next_node_id: 0,
			generation: 0,
		}
	}

	fn set_root(&mut self, node_id: Option<NodeId>) {
		if node_id.ne(&self.root) {
			self.root = node_id;
		}
	}

	fn new_node_id(&mut self) -> NodeId {
		let new_node_id = self.next_node_id;
		self.next_node_id += 1;
		new_node_id
	}

	pub(in crate::idx) fn generation(&self) -> u64 {
		self.generation
	}
}

#[derive(Debug, Default, PartialEq)]
pub(in crate::idx) struct BStatistics {
	pub(in crate::idx) keys_count: u64,
	pub(in crate::idx) max_depth: u32,
	pub(in crate::idx) nodes_count: u32,
	pub(in crate::idx) total_size: u64,
}

impl From<BStatistics> for Value {
	fn from(stats: BStatistics) -> Self {
		let mut res = Object::default();
		res.insert("keys_count".to_owned(), Value::from(stats.keys_count));
		res.insert("max_depth".to_owned(), Value::from(stats.max_depth));
		res.insert("nodes_count".to_owned(), Value::from(stats.nodes_count));
		res.insert("total_size".to_owned(), Value::from(stats.total_size));
		Value::from(res)
	}
}

#[derive(Debug, Clone)]
pub enum BTreeNode<BK>
where
	BK: BKeys + Clone,
{
	Internal(BK, Vec<NodeId>),
	Leaf(BK),
}

impl<BK> TreeNode for BTreeNode<BK>
where
	BK: BKeys + Clone,
{
	fn prepare_save(&mut self) {
		self.keys_mut().compile();
	}

	fn try_from_val(val: Val) -> Result<Self> {
		let mut c: Cursor<Vec<u8>> = Cursor::new(val);
		let node_type: u8 = bincode::deserialize_from(&mut c)?;
		let keys = BK::read_from(&mut c)?;
		match node_type {
			1u8 => {
				let child: Vec<NodeId> = bincode::deserialize_from(c)?;
				Ok(BTreeNode::Internal(keys, child))
			}
			2u8 => Ok(BTreeNode::Leaf(keys)),
			_ => Err(anyhow::Error::new(Error::CorruptedIndex("BTreeNode::try_from_val"))),
		}
	}

	fn try_into_val(&self) -> Result<Val> {
		let mut c: Cursor<Vec<u8>> = Cursor::new(Vec::new());
		match self {
			BTreeNode::Internal(keys, child) => {
				bincode::serialize_into(&mut c, &1u8)?;
				keys.write_to(&mut c)?;
				bincode::serialize_into(&mut c, &child)?;
			}
			BTreeNode::Leaf(keys) => {
				bincode::serialize_into(&mut c, &2u8)?;
				keys.write_to(&mut c)?;
			}
		};
		Ok(c.into_inner())
	}
}

impl<BK> BTreeNode<BK>
where
	BK: BKeys + Clone,
{
	fn keys(&self) -> &BK {
		match self {
			BTreeNode::Internal(keys, _) => keys,
			BTreeNode::Leaf(keys) => keys,
		}
	}

	fn keys_mut(&mut self) -> &mut BK {
		match self {
			BTreeNode::Internal(keys, _) => keys,
			BTreeNode::Leaf(keys) => keys,
		}
	}

	fn append(
		&mut self,
		key: Key,
		payload: Payload,
		node: BTreeNode<BK>,
	) -> Result<Option<Payload>> {
		match self {
			BTreeNode::Internal(keys, children) => {
				if let BTreeNode::Internal(append_keys, mut append_children) = node {
					keys.append(append_keys);
					children.append(&mut append_children);
					Ok(keys.insert(key, payload))
				} else {
					Err(anyhow::Error::new(Error::CorruptedIndex("BTree::append(1)")))
				}
			}
			BTreeNode::Leaf(keys) => {
				if let BTreeNode::Leaf(append_keys) = node {
					keys.append(append_keys);
					Ok(keys.insert(key, payload))
				} else {
					Err(anyhow::Error::new(Error::CorruptedIndex("BTree::append(2)")))
				}
			}
		}
	}
	#[cfg(debug_assertions)]
	fn check(&self) {
		match self {
			BTreeNode::Internal(k, c) => {
				if (k.len() + 1) as usize != c.len() {
					panic!("k: {} - c: {} - {}", k.len(), c.len(), self);
				}
			}
			BTreeNode::Leaf(_) => {}
		}
	}
}

impl<BK> Display for BTreeNode<BK>
where
	BK: BKeys + Clone,
{
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			BTreeNode::Internal(k, c) => write!(f, "(I) - k: {} - c: {:?}", k, c),
			BTreeNode::Leaf(k) => write!(f, "(L) - k: {}", k),
		}
	}
}

struct SplitResult {
	left_node_id: NodeId,
	right_node_id: NodeId,
	median_key: Key,
}

impl<BK> BTree<BK>
where
	BK: BKeys + Debug + Clone,
{
	pub fn new(state: BState) -> Self {
		Self {
			full_size: state.minimum_degree * 2 - 1,
			state,
			bk: PhantomData,
		}
	}

	pub(in crate::idx) fn inc_generation(&mut self) -> &BState {
		self.state.generation += 1;
		&self.state
	}

	pub async fn search(
		&self,
		tx: &Transaction,
		store: &BTreeStore<BK>,
		searched_key: &Key,
	) -> Result<Option<Payload>> {
		let mut next_node = self.state.root;
		while let Some(node_id) = next_node.take() {
			let current = store.get_node(tx, node_id).await?;
			if let Some(payload) = current.n.keys().get(searched_key) {
				return Ok(Some(payload));
			}
			if let BTreeNode::Internal(keys, children) = &current.n {
				let child_idx = keys.get_child_idx(searched_key);
				next_node.replace(children[child_idx]);
			}
		}
		Ok(None)
	}

	pub async fn search_mut(
		&self,
		tx: &Transaction,
		store: &mut BTreeStore<BK>,
		searched_key: &Key,
	) -> Result<Option<Payload>> {
		let mut next_node = self.state.root;
		while let Some(node_id) = next_node.take() {
			let current = store.get_node_mut(tx, node_id).await?;
			if let Some(payload) = current.n.keys().get(searched_key) {
				store.set_node(current, false).await?;
				return Ok(Some(payload));
			}
			if let BTreeNode::Internal(keys, children) = &current.n {
				let child_idx = keys.get_child_idx(searched_key);
				next_node.replace(children[child_idx]);
			}
			store.set_node(current, false).await?;
		}
		Ok(None)
	}

	pub async fn insert(
		&mut self,
		tx: &Transaction,
		store: &mut BTreeStore<BK>,
		key: Key,
		payload: Payload,
	) -> Result<()> {
		if let Some(root_id) = self.state.root {
			// We already have a root node
			let root = store.get_node_mut(tx, root_id).await?;
			if root.n.keys().len() == self.full_size {
				// The root node is full, let's split it
				let new_root_id = self.state.new_node_id();
				let new_root = store
					.new_node(new_root_id, BTreeNode::Internal(BK::default(), vec![root_id]))?;
				self.state.set_root(Some(new_root.id));
				self.split_child(store, new_root, 0, root).await?;
				self.insert_non_full(tx, store, new_root_id, key, payload).await?;
			} else {
				// The root node has place, let's insert the value
				let root_id = root.id;
				store.set_node(root, false).await?;
				self.insert_non_full(tx, store, root_id, key, payload).await?;
			}
		} else {
			// We don't have a root node, let's create id
			let new_root_id = self.state.new_node_id();
			let new_root_node =
				store.new_node(new_root_id, BTreeNode::Leaf(BK::with_key_val(key, payload)?))?;
			#[cfg(debug_assertions)]
			new_root_node.n.check();
			store.set_node(new_root_node, true).await?;
			self.state.set_root(Some(new_root_id));
		}
		Ok(())
	}

	async fn insert_non_full(
		&mut self,
		tx: &Transaction,
		store: &mut BTreeStore<BK>,
		node_id: NodeId,
		key: Key,
		payload: Payload,
	) -> Result<()> {
		let mut next_node_id = Some(node_id);
		while let Some(node_id) = next_node_id.take() {
			let mut node = store.get_node_mut(tx, node_id).await?;
			let key: Key = key.clone();
			match &mut node.n {
				BTreeNode::Leaf(keys) => {
					keys.insert(key, payload);
					store.set_node(node, true).await?;
				}
				BTreeNode::Internal(keys, children) => {
					if keys.get(&key).is_some() {
						keys.insert(key, payload);
						#[cfg(debug_assertions)]
						node.n.check();
						store.set_node(node, true).await?;
						return Ok(());
					}
					let child_idx = keys.get_child_idx(&key);
					let child = store.get_node_mut(tx, children[child_idx]).await?;
					let next_id = if child.n.keys().len() == self.full_size {
						let split_result = self.split_child(store, node, child_idx, child).await?;
						if key.gt(&split_result.median_key) {
							split_result.right_node_id
						} else {
							split_result.left_node_id
						}
					} else {
						let child_id = child.id;
						store.set_node(node, false).await?;
						store.set_node(child, false).await?;
						child_id
					};
					next_node_id.replace(next_id);
				}
			}
		}
		Ok(())
	}

	async fn split_child(
		&mut self,
		store: &mut BTreeStore<BK>,
		mut parent_node: StoredNode<BTreeNode<BK>>,
		idx: usize,
		child_node: BStoredNode<BK>,
	) -> Result<SplitResult> {
		let (left_node, right_node, median_key, median_payload) = match child_node.n {
			BTreeNode::Internal(keys, children) => self.split_internal_node(keys, children)?,
			BTreeNode::Leaf(keys) => self.split_leaf_node(keys)?,
		};
		let right_node_id = self.state.new_node_id();
		match parent_node.n {
			BTreeNode::Internal(ref mut keys, ref mut children) => {
				if keys.insert(median_key.clone(), median_payload).is_some() {
					#[cfg(debug_assertions)]
					panic!("Existing key: {} - {}", String::from_utf8(median_key)?, parent_node.n)
				}
				children.insert(idx + 1, right_node_id);
			}
			BTreeNode::Leaf(ref mut keys) => {
				keys.insert(median_key.clone(), median_payload);
			}
		};
		// Save the mutated split child with half the (lower) keys
		let left_node_id = child_node.id;
		let left_node = store.new_node(left_node_id, left_node)?;
		#[cfg(debug_assertions)]
		left_node.n.check();
		store.set_node(left_node, true).await?;
		// Save the new child with half the (upper) keys
		let right_node = store.new_node(right_node_id, right_node)?;
		#[cfg(debug_assertions)]
		right_node.n.check();
		store.set_node(right_node, true).await?;
		// Save the parent node
		#[cfg(debug_assertions)]
		parent_node.n.check();
		store.set_node(parent_node, true).await?;
		Ok(SplitResult {
			left_node_id,
			right_node_id,
			median_key,
		})
	}

	fn split_internal_node(
		&mut self,
		keys: BK,
		mut left_children: Vec<NodeId>,
	) -> Result<(BTreeNode<BK>, BTreeNode<BK>, Key, Payload)> {
		let r = keys.split_keys()?;
		let right_children = left_children.split_off(r.median_idx + 1);
		let left_node = BTreeNode::Internal(r.left, left_children);
		let right_node = BTreeNode::Internal(r.right, right_children);
		Ok((left_node, right_node, r.median_key, r.median_payload))
	}

	fn split_leaf_node(
		&mut self,
		keys: BK,
	) -> Result<(BTreeNode<BK>, BTreeNode<BK>, Key, Payload)> {
		let r = keys.split_keys()?;
		let left_node = BTreeNode::Leaf(r.left);
		let right_node = BTreeNode::Leaf(r.right);
		Ok((left_node, right_node, r.median_key, r.median_payload))
	}

	pub(in crate::idx) async fn delete(
		&mut self,
		tx: &Transaction,
		store: &mut BTreeStore<BK>,
		key_to_delete: Key,
	) -> Result<Option<Payload>> {
		let mut deleted_payload = None;

		if let Some(root_id) = self.state.root {
			let mut next_node = Some((true, key_to_delete, root_id));

			while let Some((is_main_key, key_to_delete, node_id)) = next_node.take() {
				let mut node = store.get_node_mut(tx, node_id).await?;
				#[cfg(debug_assertions)]
				debug!(
					"Delete loop - key_to_delete: {} - {node}",
					String::from_utf8_lossy(&key_to_delete)
				);
				match &mut node.n {
					BTreeNode::Leaf(keys) => {
						// CLRS: 1
						#[cfg(debug_assertions)]
						debug!(
							"CLRS: 1 - node: {node_id} - key_to_delete: {} - keys: {keys}",
							String::from_utf8_lossy(&key_to_delete)
						);
						if let Some(payload) = keys.get(&key_to_delete) {
							if is_main_key {
								deleted_payload = Some(payload);
							}
							keys.remove(&key_to_delete);
							if keys.len() == 0 {
								// The node is empty, we can delete it
								store.remove_node(node.id, node.key).await?;
								// Check if this was the root node
								if Some(node_id) == self.state.root {
									self.state.set_root(None);
								}
							} else {
								#[cfg(debug_assertions)]
								node.n.check();
								store.set_node(node, true).await?;
							}
						} else {
							store.set_node(node, false).await?;
						}
					}
					BTreeNode::Internal(keys, children) => {
						if let Some(payload) = keys.get(&key_to_delete) {
							// CLRS: 2
							#[cfg(debug_assertions)]
							debug!(
								"CLRS: 2 - node: {node_id} - key_to_delete: {} - k: {keys} - c: {children:?}",
								String::from_utf8_lossy(&key_to_delete)
							);
							if is_main_key {
								deleted_payload = Some(payload);
							}
							next_node.replace(
								self.deleted_from_internal(
									tx,
									store,
									keys,
									children,
									key_to_delete,
								)
								.await?,
							);
							#[cfg(debug_assertions)]
							node.n.check();
							store.set_node(node, true).await?;
						} else {
							// CLRS: 3
							#[cfg(debug_assertions)]
							debug!(
								"CLRS: 3 - node: {node_id} - key_to_delete: {} - keys: {keys}",
								String::from_utf8_lossy(&key_to_delete)
							);
							let (node_update, is_main_key, key_to_delete, next_stored_node) = self
								.deleted_traversal(
									tx,
									store,
									keys,
									children,
									key_to_delete,
									is_main_key,
								)
								.await?;
							if keys.len() == 0 {
								if let Some(root_id) = self.state.root {
									// Delete the old root node
									if root_id != node.id {
										fail!("BTree::delete")
									}
								}
								store.remove_node(node.id, node.key).await?;
								self.state.set_root(Some(next_stored_node));
							} else {
								#[cfg(debug_assertions)]
								node.n.check();
								store.set_node(node, node_update).await?;
							}
							next_node.replace((is_main_key, key_to_delete, next_stored_node));
						}
					}
				}
			}
		}
		Ok(deleted_payload)
	}

	async fn deleted_from_internal(
		&mut self,
		tx: &Transaction,
		store: &mut BTreeStore<BK>,
		keys: &mut BK,
		children: &mut Vec<NodeId>,
		key_to_delete: Key,
	) -> Result<(bool, Key, NodeId)> {
		#[cfg(debug_assertions)]
		debug!(
			"Delete from internal - key_to_delete: {} - keys: {keys}",
			String::from_utf8_lossy(&key_to_delete)
		);
		let left_idx = keys.get_child_idx(&key_to_delete);
		let left_id = children[left_idx];
		let mut left_node = store.get_node_mut(tx, left_id).await?;
		// if the child y that precedes k in nodexx has at least t keys
		if left_node.n.keys().len() >= self.state.minimum_degree {
			// CLRS: 2a -> left_node is named `y` in the book
			#[cfg(debug_assertions)]
			debug!(
				"CLRS: 2a - key_to_delete: {} - left: {left_node} - keys: {keys}",
				String::from_utf8_lossy(&key_to_delete)
			);
			let (key_prim, payload_prim) = self.find_highest(tx, store, left_node).await?;
			if keys.remove(&key_to_delete).is_none() {
				#[cfg(debug_assertions)]
				panic!("Remove key {} {} ", String::from_utf8(key_to_delete)?, keys);
			}
			if keys.insert(key_prim.clone(), payload_prim).is_some() {
				#[cfg(debug_assertions)]
				panic!("Insert key {} {} ", String::from_utf8(key_prim)?, keys);
			}
			return Ok((false, key_prim, left_id));
		}

		let right_idx = left_idx + 1;
		let right_id = children[right_idx];
		let right_node = store.get_node_mut(tx, right_id).await?;
		if right_node.n.keys().len() >= self.state.minimum_degree {
			// Cleanup 2a evaluation
			store.set_node(left_node, false).await?;
			// CLRS: 2b -> right_node is name `z` in the book
			#[cfg(debug_assertions)]
			debug!(
				"CLRS: 2b - key_to_delete: {} - right: {right_node} - keys: {keys}",
				String::from_utf8_lossy(&key_to_delete)
			);
			let (key_prim, payload_prim) = self.find_lowest(tx, store, right_node).await?;
			if keys.remove(&key_to_delete).is_none() {
				#[cfg(debug_assertions)]
				panic!("Remove key {} {} ", String::from_utf8(key_to_delete)?, keys);
			}
			if keys.insert(key_prim.clone(), payload_prim).is_some() {
				panic!("Insert key {} {} ", String::from_utf8(key_prim)?, keys);
			}
			return Ok((false, key_prim, right_id));
		}

		// CLRS: 2c
		// Merge children
		// The payload is set to 0. The payload does not matter, as the key will be
		// deleted after anyway.
		#[cfg(debug_assertions)]
		{
			left_node.n.check();
			debug!("CLRS: 2c");
		}
		left_node.n.append(key_to_delete.clone(), 0, right_node.n)?;
		#[cfg(debug_assertions)]
		left_node.n.check();
		store.set_node(left_node, true).await?;
		store.remove_node(right_id, right_node.key).await?;
		keys.remove(&key_to_delete);
		children.remove(right_idx);
		Ok((false, key_to_delete, left_id))
	}

	async fn find_highest(
		&mut self,
		tx: &Transaction,
		store: &mut BTreeStore<BK>,
		node: StoredNode<BTreeNode<BK>>,
	) -> Result<(Key, Payload)> {
		let mut next_node = Some(node);
		while let Some(node) = next_node.take() {
			match &node.n {
				BTreeNode::Internal(_, c) => {
					let id = c[c.len() - 1];
					store.set_node(node, false).await?;
					let node = store.get_node_mut(tx, id).await?;
					next_node.replace(node);
				}
				BTreeNode::Leaf(k) => {
					let (key, payload) = k
						.get_last_key()
						.ok_or_else(|| Error::unreachable("BTree::find_highest(1)"))?;
					#[cfg(debug_assertions)]
					debug!("Find highest: {} - node: {}", String::from_utf8_lossy(&key), node);
					store.set_node(node, false).await?;
					return Ok((key, payload));
				}
			}
		}
		fail!("BTree::find_highest(2)")
	}

	async fn find_lowest(
		&mut self,
		tx: &Transaction,
		store: &mut BTreeStore<BK>,
		node: StoredNode<BTreeNode<BK>>,
	) -> Result<(Key, Payload)> {
		let mut next_node = Some(node);
		while let Some(node) = next_node.take() {
			match &node.n {
				BTreeNode::Internal(_, c) => {
					let id = c[0];
					store.set_node(node, false).await?;
					let node = store.get_node_mut(tx, id).await?;
					next_node.replace(node);
				}
				BTreeNode::Leaf(k) => {
					let (key, payload) = k
						.get_first_key()
						.ok_or_else(|| Error::unreachable("BTree::find_lowest(1)"))?;
					#[cfg(debug_assertions)]
					debug!("Find lowest: {} - node: {}", String::from_utf8_lossy(&key), node.id);
					store.set_node(node, false).await?;
					return Ok((key, payload));
				}
			}
		}
		fail!("BTree::find_lowest(2)")
	}

	async fn deleted_traversal(
		&mut self,
		tx: &Transaction,
		store: &mut BTreeStore<BK>,
		keys: &mut BK,
		children: &mut Vec<NodeId>,
		key_to_delete: Key,
		is_main_key: bool,
	) -> Result<(bool, bool, Key, NodeId)> {
		// CLRS 3 Determine the root x.ci that must contain k
		let child_idx = keys.get_child_idx(&key_to_delete);
		let child_id = match children.get(child_idx) {
			None => bail!(Error::CorruptedIndex("deleted_traversal:invalid_child_idx")),
			Some(&child_id) => child_id,
		};
		#[cfg(debug_assertions)]
		debug!(
			"CLRS: 3 - key_to_delete: {} - child_id: {child_id}",
			String::from_utf8_lossy(&key_to_delete)
		);
		let child_stored_node = store.get_node_mut(tx, child_id).await?;
		// If x.ci has only t-1 keys, execute 3a or 3b
		if child_stored_node.n.keys().len() < self.state.minimum_degree {
			if child_idx < children.len() - 1 {
				let right_child_stored_node =
					store.get_node_mut(tx, children[child_idx + 1]).await?;
				return if right_child_stored_node.n.keys().len() >= self.state.minimum_degree {
					#[cfg(debug_assertions)]
					debug!(
						"CLRS: 3a - xci_child: {child_stored_node} - right_sibling_child: {right_child_stored_node}"
					);
					Self::delete_adjust_successor(
						store,
						keys,
						child_idx,
						key_to_delete,
						is_main_key,
						child_stored_node,
						right_child_stored_node,
					)
					.await
				} else {
					// CLRS 3b successor
					#[cfg(debug_assertions)]
					debug!(
						"CLRS: 3b merge - keys: {keys} - xci_child: {child_stored_node} - right_sibling_child: {right_child_stored_node}"
					);
					Self::merge_nodes(
						store,
						keys,
						children,
						child_idx,
						key_to_delete,
						is_main_key,
						child_stored_node,
						right_child_stored_node,
					)
					.await
				};
			}

			// left child (predecessor)
			if child_idx > 0 {
				let child_idx = child_idx - 1;
				let left_child_stored_node = store.get_node_mut(tx, children[child_idx]).await?;
				return if left_child_stored_node.n.keys().len() >= self.state.minimum_degree {
					#[cfg(debug_assertions)]
					debug!(
						"CLRS: 3a - left_sibling_child: {left_child_stored_node} - xci_child: {child_stored_node}",
					);
					Self::delete_adjust_predecessor(
						store,
						keys,
						child_idx,
						key_to_delete,
						is_main_key,
						left_child_stored_node,
						child_stored_node,
					)
					.await
				} else {
					// CLRS 3b predecessor
					#[cfg(debug_assertions)]
					debug!(
						"CLRS: 3b merge - keys: {keys} - left_sibling_child: {left_child_stored_node} - xci_child: {child_stored_node}"
					);
					Self::merge_nodes(
						store,
						keys,
						children,
						child_idx,
						key_to_delete,
						is_main_key,
						left_child_stored_node,
						child_stored_node,
					)
					.await
				};
			}
		}

		store.set_node(child_stored_node, false).await?;
		Ok((false, true, key_to_delete, child_id))
	}

	async fn delete_adjust_successor(
		store: &mut BTreeStore<BK>,
		keys: &mut BK,
		child_idx: usize,
		key_to_delete: Key,
		is_main_key: bool,
		mut child_stored_node: BStoredNode<BK>,
		mut right_child_stored_node: BStoredNode<BK>,
	) -> Result<(bool, bool, Key, NodeId)> {
		let (ascending_key, ascending_payload) =
			right_child_stored_node
				.n
				.keys()
				.get_first_key()
				.ok_or(Error::CorruptedIndex("BTree::delete_adjust_successor(1)"))?;
		right_child_stored_node.n.keys_mut().remove(&ascending_key);
		let descending_key = keys
			.get_key(child_idx)
			.ok_or(Error::CorruptedIndex("BTree::delete_adjust_successor(2)"))?;
		let descending_payload = keys
			.remove(&descending_key)
			.ok_or(Error::CorruptedIndex("BTree::delete_adjust_successor(3)"))?;
		if child_stored_node.n.keys_mut().insert(descending_key, descending_payload).is_some() {
			#[cfg(debug_assertions)]
			panic!("Duplicate insert key {} ", child_stored_node.n);
		}
		if let BTreeNode::Internal(_, rc) = &mut right_child_stored_node.n {
			if let BTreeNode::Internal(_, lc) = &mut child_stored_node.n {
				lc.push(rc.remove(0))
			}
		}
		if keys.insert(ascending_key, ascending_payload).is_some() {
			#[cfg(debug_assertions)]
			panic!("Duplicate insert key {} ", keys);
		}
		let child_id = child_stored_node.id;
		#[cfg(debug_assertions)]
		{
			child_stored_node.n.check();
			right_child_stored_node.n.check();
		}
		store.set_node(child_stored_node, true).await?;
		store.set_node(right_child_stored_node, true).await?;
		Ok((true, is_main_key, key_to_delete, child_id))
	}

	async fn delete_adjust_predecessor(
		store: &mut BTreeStore<BK>,
		keys: &mut BK,
		child_idx: usize,
		key_to_delete: Key,
		is_main_key: bool,
		mut left_child_stored_node: BStoredNode<BK>,
		mut child_stored_node: BStoredNode<BK>,
	) -> Result<(bool, bool, Key, NodeId)> {
		let (ascending_key, ascending_payload) = left_child_stored_node
			.n
			.keys()
			.get_last_key()
			.ok_or(Error::CorruptedIndex("BTree::delete_adjust_predecessor(1)"))?;
		if left_child_stored_node.n.keys_mut().remove(&ascending_key).is_none() {
			#[cfg(debug_assertions)]
			panic!("Remove key {} {}", String::from_utf8(ascending_key)?, left_child_stored_node.n);
		}
		let descending_key = keys
			.get_key(child_idx)
			.ok_or(Error::CorruptedIndex("BTree::delete_adjust_predecessor(2)"))?;
		let descending_payload = keys
			.remove(&descending_key)
			.ok_or(Error::CorruptedIndex("BTree::delete_adjust_predecessor(3)"))?;
		if child_stored_node.n.keys_mut().insert(descending_key, descending_payload).is_some() {
			#[cfg(debug_assertions)]
			panic!("Insert key {}", child_stored_node.n);
		}
		if let BTreeNode::Internal(_, lc) = &mut left_child_stored_node.n {
			if let BTreeNode::Internal(_, rc) = &mut child_stored_node.n {
				rc.insert(0, lc.remove(lc.len() - 1));
			}
		}
		if keys.insert(ascending_key, ascending_payload).is_some() {
			#[cfg(debug_assertions)]
			panic!("Insert key {}", keys);
		}
		let child_id = child_stored_node.id;
		#[cfg(debug_assertions)]
		{
			child_stored_node.n.check();
			left_child_stored_node.n.check();
			debug!("{}", left_child_stored_node);
			debug!("{}", child_stored_node);
		}
		store.set_node(child_stored_node, true).await?;
		store.set_node(left_child_stored_node, true).await?;
		Ok((true, is_main_key, key_to_delete, child_id))
	}

	#[expect(clippy::too_many_arguments)]
	async fn merge_nodes(
		store: &mut BTreeStore<BK>,
		keys: &mut BK,
		children: &mut Vec<NodeId>,
		child_idx: usize,
		key_to_delete: Key,
		is_main_key: bool,
		mut left_child: BStoredNode<BK>,
		right_child: BStoredNode<BK>,
	) -> Result<(bool, bool, Key, NodeId)> {
		#[cfg(debug_assertions)]
		debug!("Keys: {keys}");
		let descending_key =
			keys.get_key(child_idx).ok_or(Error::CorruptedIndex("BTree::merge_nodes(1)"))?;
		let descending_payload =
			keys.remove(&descending_key).ok_or(Error::CorruptedIndex("BTree::merge_nodes(2)"))?;
		#[cfg(debug_assertions)]
		debug!("descending_key: {}", String::from_utf8_lossy(&descending_key));
		children.remove(child_idx + 1);
		let left_id = left_child.id;
		if left_child.n.append(descending_key, descending_payload, right_child.n)?.is_some() {
			#[cfg(debug_assertions)]
			panic!("Key already present");
		}
		#[cfg(debug_assertions)]
		left_child.n.check();
		store.set_node(left_child, true).await?;
		store.remove_node(right_child.id, right_child.key).await?;
		Ok((true, is_main_key, key_to_delete, left_id))
	}

	pub(in crate::idx) async fn statistics(
		&self,
		tx: &Transaction,
		store: &BTreeStore<BK>,
	) -> Result<BStatistics> {
		let mut stats = BStatistics::default();
		#[cfg(debug_assertions)]
		let mut keys = HashSet::default();
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = self.state.root {
			node_queue.push_front((node_id, 1));
		}
		while let Some((node_id, depth)) = node_queue.pop_front() {
			let stored = store.get_node(tx, node_id).await?;
			stats.keys_count += stored.n.keys().len() as u64;
			if depth > stats.max_depth {
				stats.max_depth = depth;
			}
			#[cfg(debug_assertions)]
			{
				let k = stored.n.keys();
				for i in 0..k.len() {
					if let Some(k) = k.get_key(i as usize) {
						if !keys.insert(k.clone()) {
							panic!("Duplicate key: {}", String::from_utf8(k)?);
						}
					}
				}
			}
			stats.nodes_count += 1;
			stats.total_size += stored.size as u64;
			if let BTreeNode::Internal(_, children) = &stored.n {
				let depth = depth + 1;
				for child_id in children.iter() {
					node_queue.push_front((*child_id, depth));
				}
			};
		}
		Ok(stats)
	}
}

#[cfg(test)]
mod tests {
	use std::cmp::Ordering;
	use std::collections::BTreeMap;
	use std::sync::Arc;

	use rand::seq::SliceRandom;
	use rand::thread_rng;
	use test_log::test;

	use super::*;
	use crate::idx::trees::bkeys::{FstKeys, TrieKeys};
	use crate::idx::trees::store::TreeNodeProvider;
	use crate::kvs::{Datastore, LockType, TransactionType};

	#[test]
	fn test_btree_state_serde() {
		let s = BState::new(3);
		let val = s.kv_encode_value().unwrap();
		let s: BState = BState::kv_decode_value(val).unwrap();
		assert_eq!(s.minimum_degree, 3);
		assert_eq!(s.root, None);
		assert_eq!(s.next_node_id, 0);
	}

	#[test]
	fn test_node_serde_internal() {
		let mut node = BTreeNode::Internal(FstKeys::default(), vec![]);
		node.prepare_save();
		let val = node.try_into_val().unwrap();
		let _: BTreeNode<FstKeys> = BTreeNode::try_from_val(val).unwrap();
	}

	#[test]
	fn test_node_serde_leaf() {
		let mut node = BTreeNode::Leaf(TrieKeys::default());
		node.prepare_save();
		let val = node.try_into_val().unwrap();
		let _: BTreeNode<TrieKeys> = BTreeNode::try_from_val(val).unwrap();
	}

	async fn insertions_test<F, BK>(
		tx: Transaction,
		mut st: BTreeStore<BK>,
		t: &mut BTree<BK>,
		samples_size: usize,
		sample_provider: F,
	) where
		F: Fn(usize) -> (Key, Payload),
		BK: BKeys + Debug + Clone,
	{
		for i in 0..samples_size {
			let (key, payload) = sample_provider(i);
			// Insert the sample
			t.insert(&tx, &mut st, key, payload).await.unwrap();
		}
		st.finish(&tx).await.unwrap();
		tx.commit().await.unwrap();
	}

	async fn check_insertions<F, BK>(
		tx: Transaction,
		st: BTreeStore<BK>,
		t: &mut BTree<BK>,
		samples_size: usize,
		sample_provider: F,
	) where
		F: Fn(usize) -> (Key, Payload),
		BK: BKeys + Debug + Clone,
	{
		for i in 0..samples_size {
			let (key, payload) = sample_provider(i);
			assert_eq!(t.search(&tx, &st, &key).await.unwrap(), Some(payload));
		}
		tx.cancel().await.unwrap();
	}

	fn get_key_value(idx: usize) -> (Key, Payload) {
		(format!("{}", idx).into(), (idx * 10) as Payload)
	}

	async fn new_operation_fst<BK>(
		ds: &Datastore,
		t: &BTree<BK>,
		tt: TransactionType,
		cache_size: usize,
	) -> (Transaction, BTreeStore<FstKeys>)
	where
		BK: BKeys + Debug + Clone,
	{
		let tx = ds.transaction(tt, LockType::Optimistic).await.unwrap();
		let st = tx
			.index_caches()
			.get_store_btree_fst(TreeNodeProvider::Debug, t.state.generation, tt, cache_size)
			.await
			.unwrap();
		(tx, st)
	}

	async fn new_operation_trie<BK>(
		ds: &Datastore,
		t: &BTree<BK>,
		tt: TransactionType,
		cache_size: usize,
	) -> (Transaction, BTreeStore<TrieKeys>)
	where
		BK: BKeys + Debug + Clone,
	{
		let tx = ds.transaction(tt, LockType::Optimistic).await.unwrap();
		let st = tx
			.index_caches()
			.get_store_btree_trie(TreeNodeProvider::Debug, t.state.generation, tt, cache_size)
			.await
			.unwrap();
		(tx, st)
	}

	#[test(tokio::test)]
	async fn test_btree_fst_small_order_sequential_insertions() {
		let ds = Datastore::new("memory").await.unwrap();

		let mut t = BTree::new(BState::new(5));

		{
			let (tx, st) = new_operation_fst(&ds, &t, TransactionType::Write, 20).await;
			insertions_test::<_, FstKeys>(tx, st, &mut t, 100, get_key_value).await;
		}

		{
			let (tx, st) = new_operation_fst(&ds, &t, TransactionType::Read, 20).await;
			check_insertions(tx, st, &mut t, 100, get_key_value).await;
		}

		{
			let (tx, st) = new_operation_fst(&ds, &t, TransactionType::Read, 20).await;
			assert_eq!(
				t.statistics(&tx, &st).await.unwrap(),
				BStatistics {
					keys_count: 100,
					max_depth: 3,
					nodes_count: 22,
					total_size: 1691,
				}
			);
			tx.cancel().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_btree_trie_small_order_sequential_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::new(BState::new(6));

		{
			let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Write, 20).await;
			insertions_test::<_, TrieKeys>(tx, st, &mut t, 100, get_key_value).await;
		}

		{
			let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Read, 20).await;
			check_insertions(tx, st, &mut t, 100, get_key_value).await;
		}

		{
			let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Read, 20).await;
			assert_eq!(
				t.statistics(&tx, &st).await.unwrap(),
				BStatistics {
					keys_count: 100,
					max_depth: 3,
					nodes_count: 18,
					total_size: 1656,
				}
			);
			tx.cancel().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_btree_fst_small_order_random_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::new(BState::new(8));

		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);

		{
			let (tx, st) = new_operation_fst(&ds, &t, TransactionType::Write, 20).await;
			insertions_test(tx, st, &mut t, 100, |i| get_key_value(samples[i])).await;
		}

		{
			let (tx, st) = new_operation_fst(&ds, &t, TransactionType::Read, 20).await;
			check_insertions(tx, st, &mut t, 100, |i| get_key_value(samples[i])).await;
		}

		{
			let (tx, st) = new_operation_fst(&ds, &t, TransactionType::Read, 20).await;
			let s = t.statistics(&tx, &st).await.unwrap();
			assert_eq!(s.keys_count, 100);
			tx.cancel().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_btree_trie_small_order_random_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::new(BState::new(75));

		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);

		{
			let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Write, 20).await;
			insertions_test(tx, st, &mut t, 100, |i| get_key_value(samples[i])).await;
		}

		{
			let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Read, 20).await;
			check_insertions(tx, st, &mut t, 100, |i| get_key_value(samples[i])).await;
		}

		{
			let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Read, 20).await;
			let s = t.statistics(&tx, &st).await.unwrap();
			assert_eq!(s.keys_count, 100);
			tx.cancel().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_large_order_sequential_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::new(BState::new(60));

		{
			let (tx, st) = new_operation_fst(&ds, &t, TransactionType::Write, 20).await;
			insertions_test(tx, st, &mut t, 10000, get_key_value).await;
		}

		{
			let (tx, st) = new_operation_fst(&ds, &t, TransactionType::Read, 20).await;
			check_insertions(tx, st, &mut t, 10000, get_key_value).await;
		}

		{
			let (tx, st) = new_operation_fst(&ds, &t, TransactionType::Read, 20).await;
			assert_eq!(
				t.statistics(&tx, &st).await.unwrap(),
				BStatistics {
					keys_count: 10000,
					max_depth: 3,
					nodes_count: 158,
					total_size: 57486,
				}
			);
			tx.cancel().await.unwrap();
		}
	}

	async fn test_btree_trie_keys_large_order_sequential_insertions(cache_size: usize) {
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::new(BState::new(60));

		{
			let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Write, cache_size).await;
			insertions_test(tx, st, &mut t, 10000, get_key_value).await;
		}

		{
			let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Read, cache_size).await;
			check_insertions(tx, st, &mut t, 10000, get_key_value).await;
		}

		{
			let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Read, cache_size).await;
			assert_eq!(
				t.statistics(&tx, &st).await.unwrap(),
				BStatistics {
					keys_count: 10000,
					max_depth: 3,
					nodes_count: 158,
					total_size: 75206,
				}
			);
			tx.cancel().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_large_order_sequential_insertions_lru_cache() {
		test_btree_trie_keys_large_order_sequential_insertions(20).await
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_large_order_sequential_insertions_full_cache() {
		test_btree_trie_keys_large_order_sequential_insertions(0).await
	}

	const REAL_WORLD_TERMS: [&str; 30] = [
		"the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "the", "fast",
		"fox", "jumped", "over", "the", "lazy", "dog", "the", "dog", "sat", "there", "and", "did",
		"nothing", "the", "other", "animals", "sat", "there", "watching",
	];

	async fn test_btree_fst_real_world_insertions(default_minimum_degree: u32) -> BStatistics {
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::new(BState::new(default_minimum_degree));

		{
			let (tx, st) = new_operation_fst(&ds, &t, TransactionType::Write, 20).await;
			insertions_test(tx, st, &mut t, REAL_WORLD_TERMS.len(), |i| {
				(REAL_WORLD_TERMS[i].as_bytes().to_vec(), i as Payload)
			})
			.await;
		}

		let (tx, st) = new_operation_fst(&ds, &t, TransactionType::Read, 20).await;
		let statistics = t.statistics(&tx, &st).await.unwrap();
		tx.cancel().await.unwrap();
		statistics
	}

	async fn test_btree_trie_real_world_insertions(default_minimum_degree: u32) -> BStatistics {
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::new(BState::new(default_minimum_degree));

		{
			let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Write, 20).await;
			insertions_test(tx, st, &mut t, REAL_WORLD_TERMS.len(), |i| {
				(REAL_WORLD_TERMS[i].as_bytes().to_vec(), i as Payload)
			})
			.await;
		}

		let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Read, 20).await;
		let statistics = t.statistics(&tx, &st).await.unwrap();
		tx.cancel().await.unwrap();

		statistics
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_real_world_insertions_small_order() {
		let expected = BStatistics {
			keys_count: 17,
			max_depth: 2,
			nodes_count: 5,
			total_size: 421,
		};
		let s = test_btree_fst_real_world_insertions(4).await;
		assert_eq!(s, expected);
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_real_world_insertions_large_order() {
		let expected = BStatistics {
			keys_count: 17,
			max_depth: 1,
			nodes_count: 1,
			total_size: 189,
		};
		let s = test_btree_fst_real_world_insertions(100).await;
		assert_eq!(s, expected);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_real_world_insertions_small() {
		let expected = BStatistics {
			keys_count: 17,
			max_depth: 2,
			nodes_count: 3,
			total_size: 339,
		};
		let s = test_btree_trie_real_world_insertions(6).await;
		assert_eq!(s, expected);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_real_world_insertions_large_order() {
		let expected = BStatistics {
			keys_count: 17,
			max_depth: 1,
			nodes_count: 1,
			total_size: 229,
		};
		let s = test_btree_trie_real_world_insertions(100).await;
		assert_eq!(s, expected);
	}

	// This is the examples from the chapter B-Trees in CLRS:
	// https://en.wikipedia.org/wiki/Introduction_to_Algorithms
	const CLRS_EXAMPLE: [(&str, Payload); 23] = [
		("a", 1),
		("c", 3),
		("g", 7),
		("j", 10),
		("k", 11),
		("m", 13),
		("n", 14),
		("o", 15),
		("p", 16),
		("t", 20),
		("u", 21),
		("x", 24),
		("y", 25),
		("z", 26),
		("v", 22),
		("d", 4),
		("e", 5),
		("r", 18),
		("s", 19), // (a) Initial tree
		("b", 2),  // (b) B inserted
		("q", 17), // (c) Q inserted
		("l", 12), // (d) L inserted
		("f", 6),  // (e) F inserted
	];

	#[test(tokio::test)]
	// This check node splitting. CLRS: Figure 18.7, page 498.
	async fn clrs_insertion_test() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::<TrieKeys>::new(BState::new(3));

		{
			let (tx, mut st) = new_operation_trie(&ds, &t, TransactionType::Write, 20).await;
			for (key, payload) in CLRS_EXAMPLE {
				t.insert(&tx, &mut st, key.into(), payload).await.unwrap();
			}
			st.finish(&tx).await.unwrap();
			tx.commit().await.unwrap();
		}

		let (tx, mut st) = new_operation_trie(&ds, &t, TransactionType::Read, 20).await;

		let s = t.statistics(&tx, &st).await.unwrap();
		assert_eq!(s.keys_count, 23);
		assert_eq!(s.max_depth, 3);
		assert_eq!(s.nodes_count, 10);
		// There should be one record per node
		assert_eq!(10, tx.scan(vec![]..vec![0xf], 100, None).await.unwrap().len());

		let nodes_count = t
			.inspect_nodes(&tx, &mut st, |count, depth, node_id, node| match count {
				0 => {
					assert_eq!(depth, 1);
					assert_eq!(node_id, 7);
					check_is_internal_node(&node.n, vec![("p", 16)], vec![1, 8]);
				}
				1 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 1);
					check_is_internal_node(
						&node.n,
						vec![("c", 3), ("g", 7), ("m", 13)],
						vec![0, 9, 2, 3],
					);
				}
				2 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 8);
					check_is_internal_node(&node.n, vec![("t", 20), ("x", 24)], vec![4, 6, 5]);
				}
				3 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 0);
					check_is_leaf_node(&node.n, vec![("a", 1), ("b", 2)]);
				}
				4 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 9);
					check_is_leaf_node(&node.n, vec![("d", 4), ("e", 5), ("f", 6)]);
				}
				5 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 2);
					check_is_leaf_node(&node.n, vec![("j", 10), ("k", 11), ("l", 12)]);
				}
				6 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 3);
					check_is_leaf_node(&node.n, vec![("n", 14), ("o", 15)]);
				}
				7 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 4);
					check_is_leaf_node(&node.n, vec![("q", 17), ("r", 18), ("s", 19)]);
				}
				8 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 6);
					check_is_leaf_node(&node.n, vec![("u", 21), ("v", 22)]);
				}
				9 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 5);
					check_is_leaf_node(&node.n, vec![("y", 25), ("z", 26)]);
				}
				_ => panic!("This node should not exist {}", count),
			})
			.await
			.unwrap();
		assert_eq!(nodes_count, 10);
		tx.cancel().await.unwrap();
	}

	async fn check_finish_commit<BK>(
		t: &mut BTree<BK>,
		mut st: BTreeStore<BK>,
		tx: Transaction,
		mut r#gen: u64,
		info: String,
	) -> Result<u64>
	where
		BK: BKeys + Clone + Debug,
	{
		if st.finish(&tx).await?.is_some() {
			t.state.generation += 1;
		}
		r#gen += 1;
		assert_eq!(t.state.generation, r#gen, "{}", info);
		tx.commit().await?;
		Ok(r#gen)
	}

	// This check the possible deletion cases. CRLS, Figure 18.8, pages 500-501
	#[test(tokio::test)]
	async fn test_btree_clrs_deletion_test() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let mut t = BTree::<TrieKeys>::new(BState::new(3));
		let mut check_generation = 0;
		{
			let (tx, mut st) = new_operation_trie(&ds, &t, TransactionType::Write, 20).await;
			for (key, payload) in CLRS_EXAMPLE {
				t.insert(&tx, &mut st, key.into(), payload).await?;
			}
			check_generation = check_finish_commit(
				&mut t,
				st,
				tx,
				check_generation,
				"Insert CLRS example".to_string(),
			)
			.await?;
		}

		{
			let mut key_count = CLRS_EXAMPLE.len() as u64;
			for (key, payload) in [("f", 6), ("m", 13), ("g", 7), ("d", 4), ("b", 2)] {
				{
					let (tx, mut st) =
						new_operation_trie(&ds, &t, TransactionType::Write, 20).await;
					debug!("Delete {}", key);
					assert_eq!(t.delete(&tx, &mut st, key.into()).await?, Some(payload));
					check_generation = check_finish_commit(
						&mut t,
						st,
						tx,
						check_generation,
						format!("Delete {key}"),
					)
					.await?;
				}
				key_count -= 1;
				{
					let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Read, 20).await;
					let s = t.statistics(&tx, &st).await?;
					assert_eq!(s.keys_count, key_count);
				}
			}
		}

		let (tx, mut st) = new_operation_trie(&ds, &t, TransactionType::Read, 20).await;

		let s = t.statistics(&tx, &st).await.unwrap();
		assert_eq!(s.keys_count, 18);
		assert_eq!(s.max_depth, 2);
		assert_eq!(s.nodes_count, 7);
		// There should be one record per node
		assert_eq!(7, tx.scan(vec![]..vec![0xf], 100, None).await.unwrap().len());

		let nodes_count = t
			.inspect_nodes(&tx, &mut st, |count, depth, node_id, node| match count {
				0 => {
					assert_eq!(depth, 1);
					assert_eq!(node_id, 1);
					check_is_internal_node(
						&node.n,
						vec![("e", 5), ("l", 12), ("p", 16), ("t", 20), ("x", 24)],
						vec![0, 9, 3, 4, 6, 5],
					);
				}
				1 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 0);
					check_is_leaf_node(&node.n, vec![("a", 1), ("c", 3)]);
				}
				2 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 9);
					check_is_leaf_node(&node.n, vec![("j", 10), ("k", 11)]);
				}
				3 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 3);
					check_is_leaf_node(&node.n, vec![("n", 14), ("o", 15)]);
				}
				4 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 4);
					check_is_leaf_node(&node.n, vec![("q", 17), ("r", 18), ("s", 19)]);
				}
				5 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 6);
					check_is_leaf_node(&node.n, vec![("u", 21), ("v", 22)]);
				}
				6 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 5);
					check_is_leaf_node(&node.n, vec![("y", 25), ("z", 26)]);
				}
				_ => panic!("This node should not exist {}", count),
			})
			.await
			.unwrap();
		assert_eq!(nodes_count, 7);
		tx.cancel().await?;
		Ok(())
	}

	// This check the possible deletion cases. CRLS, Figure 18.8, pages 500-501
	#[test(tokio::test)]
	async fn test_btree_fill_and_empty() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let mut t = BTree::<TrieKeys>::new(BState::new(3));

		let mut expected_keys = BTreeMap::new();

		let mut check_generation = 0;
		{
			let (tx, mut st) = new_operation_trie(&ds, &t, TransactionType::Write, 20).await;
			for (key, payload) in CLRS_EXAMPLE {
				expected_keys.insert(key.to_string(), payload);
				t.insert(&tx, &mut st, key.into(), payload).await?;
				let (_, tree_keys) = check_btree_properties(&t, &tx, &mut st).await?;
				assert_eq!(expected_keys, tree_keys);
			}
			check_generation = check_finish_commit(
				&mut t,
				st,
				tx,
				check_generation,
				"Insert CLRS example".to_string(),
			)
			.await?;
		}

		{
			let (tx, mut st) = new_operation_trie(&ds, &t, TransactionType::Read, 20).await;
			print_tree(&tx, &mut st, &t).await;
			tx.cancel().await?;
		}

		for (key, _) in CLRS_EXAMPLE {
			debug!("------------------------");
			debug!("Delete {}", key);
			{
				let (tx, mut st) = new_operation_trie(&ds, &t, TransactionType::Write, 20).await;
				assert!(t.delete(&tx, &mut st, key.into()).await?.is_some());
				expected_keys.remove(key);
				let (_, tree_keys) = check_btree_properties(&t, &tx, &mut st).await?;
				assert_eq!(expected_keys, tree_keys);
				check_generation = check_finish_commit(
					&mut t,
					st,
					tx,
					check_generation,
					format!("Delete CLRS example {key}"),
				)
				.await?;
			}

			// Check that every expected keys are still found in the tree
			{
				let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Read, 20).await;
				for (key, payload) in &expected_keys {
					assert_eq!(
						t.search(&tx, &st, &key.as_str().into()).await?,
						Some(*payload),
						"Can't find: {key}",
					)
				}
				tx.cancel().await?;
			}
		}

		let (tx, st) = new_operation_trie(&ds, &t, TransactionType::Read, 20).await;
		let s = t.statistics(&tx, &st).await?;
		assert_eq!(s.keys_count, 0);
		assert_eq!(s.max_depth, 0);
		assert_eq!(s.nodes_count, 0);
		// There should not be any record in the database
		assert_eq!(0, tx.scan(vec![]..vec![0xf], 100, None).await.unwrap().len());
		tx.cancel().await?;
		Ok(())
	}

	#[test(tokio::test)]
	async fn test_delete_adjust() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let mut t = BTree::<FstKeys>::new(BState::new(3));

		let terms = [
			"aliquam",
			"delete",
			"if",
			"from",
			"Docusaurus",
			"amet,",
			"don't",
			"And",
			"interactive",
			"well!",
			"supports",
			"ultricies.",
			"Fusce",
			"consequat.",
			"just",
			"use",
			"elementum",
			"term",
			"blogging",
			"to",
			"want",
			"added",
			"Lorem",
			"ipsum",
			"blog:",
			"MDX.",
			"posts.",
			"features",
			"posts",
			"features,",
			"truncate",
			"images:",
			"Long",
			"Pellentesque",
			"authors.yml.",
			"filenames,",
			"such",
			"co-locate",
			"you",
			"can",
			"the",
			"-->",
			"comment",
			"tags",
			"A",
			"React",
			"The",
			"adipiscing",
			"consectetur",
			"very",
			"this",
			"and",
			"sit",
			"directory,",
			"Regular",
			"Markdown",
			"Simply",
			"blog",
			"MDX",
			"list",
			"extracted",
			"summary",
			"amet",
			"plugin.",
			"your",
			"long",
			"First",
			"power",
			"post,",
			"convenient",
			"folders)",
			"of",
			"date",
			"powered",
			"2019-05-30-welcome.md",
			"view.",
			"are",
			"be",
			"<!--",
			"Welcome",
			"is",
			"2019-05-30-welcome/index.md",
			"by",
			"directory.",
			"folder",
			"Use",
			"search",
			"authors",
			"false",
			"as:",
			"tempor",
			"files",
			"config.",
			"dignissim",
			"as",
			"a",
			"in",
			"This",
			"authors.yml",
			"create",
			"dolor",
			"Enter",
			"support",
			"add",
			"eros",
			"post",
			"Post",
			"size",
			"(or",
			"rhoncus",
			"Blog",
			"limit",
			"elit.",
		];
		let mut keys = BTreeMap::new();
		{
			let (tx, mut st) = new_operation_fst(&ds, &t, TransactionType::Write, 100).await;
			for term in terms {
				t.insert(&tx, &mut st, term.into(), 0).await?;
				keys.insert(term.to_string(), 0);
				let (_, tree_keys) = check_btree_properties(&t, &tx, &mut st).await?;
				assert_eq!(keys, tree_keys);
			}
			st.finish(&tx).await?;
			tx.commit().await?;
		}
		{
			let (tx, mut st) = new_operation_fst(&ds, &t, TransactionType::Read, 100).await;
			print_tree(&tx, &mut st, &t).await;
		}
		{
			let (tx, mut st) = new_operation_fst(&ds, &t, TransactionType::Write, 100).await;
			for term in terms {
				debug!("Delete {term}");
				t.delete(&tx, &mut st, term.into()).await?;
				print_tree_mut(&tx, &mut st, &t).await;
				keys.remove(term);
				let (_, tree_keys) = check_btree_properties(&t, &tx, &mut st).await?;
				assert_eq!(keys, tree_keys);
			}
			st.finish(&tx).await?;
			tx.commit().await?;
		}
		{
			let (tx, mut st) = new_operation_fst(&ds, &t, TransactionType::Write, 100).await;
			assert_eq!(check_btree_properties(&t, &tx, &mut st).await?.0, 0);
			st.finish(&tx).await?;
			tx.cancel().await?;
		}
		Ok(())
	}

	async fn check_btree_properties<BK>(
		t: &BTree<BK>,
		tx: &Transaction,
		st: &mut BTreeStore<BK>,
	) -> Result<(usize, BTreeMap<String, Payload>)>
	where
		BK: BKeys + Clone + Debug,
	{
		let mut unique_keys = BTreeMap::new();
		let n = t
			.inspect_nodes_mut(tx, st, |_, _, _, sn| {
				let keys = sn.n.keys();
				for i in 0..keys.len() {
					let key = keys.get_key(i as usize).unwrap_or_else(|| panic!("No key"));
					let payload = keys.get(&key).unwrap_or_else(|| panic!("No payload"));
					if unique_keys.insert(String::from_utf8(key).unwrap(), payload).is_some() {
						panic!("Non unique");
					}
				}
			})
			.await?;
		Ok((n, unique_keys))
	}

	/////////////
	// HELPERS //
	/////////////

	fn check_is_internal_node<BK>(
		node: &BTreeNode<BK>,
		expected_keys: Vec<(&str, i32)>,
		expected_children: Vec<NodeId>,
	) where
		BK: BKeys + Clone,
	{
		if let BTreeNode::Internal(keys, children) = node {
			check_keys(keys, expected_keys);
			assert_eq!(children, &expected_children, "The children are not matching");
		} else {
			panic!("An internal node was expected, we got a leaf node");
		}
	}

	fn check_is_leaf_node<BK>(node: &BTreeNode<BK>, expected_keys: Vec<(&str, i32)>)
	where
		BK: BKeys + Clone,
	{
		if let BTreeNode::Leaf(keys) = node {
			check_keys(keys, expected_keys);
		} else {
			panic!("An internal node was expected, we got a leaf node");
		}
	}

	async fn print_tree<BK>(tx: &Transaction, st: &mut BTreeStore<BK>, t: &BTree<BK>)
	where
		BK: BKeys + Debug + Clone,
	{
		debug!("----------------------------------");
		t.inspect_nodes(tx, st, |_count, depth, node_id, node| {
			debug!("depth: {} - node: {} - {}", depth, node_id, node.n);
		})
		.await
		.unwrap();
		debug!("----------------------------------");
	}

	async fn print_tree_mut<BK>(tx: &Transaction, st: &mut BTreeStore<BK>, t: &BTree<BK>)
	where
		BK: BKeys + Debug + Clone,
	{
		debug!("----------------------------------");
		t.inspect_nodes_mut(tx, st, |_count, depth, node_id, node| {
			debug!("depth: {} - node: {} - {}", depth, node_id, node.n);
		})
		.await
		.unwrap();
		debug!("----------------------------------");
	}

	fn check_keys<BK>(keys: &BK, expected_keys: Vec<(&str, i32)>)
	where
		BK: BKeys,
	{
		assert_eq!(keys.len() as usize, expected_keys.len(), "The number of keys does not match");
		for (key, payload) in expected_keys {
			assert_eq!(
				keys.get(&key.into()),
				Some(payload as Payload),
				"The key {} does not match",
				key
			);
		}
	}

	impl<BK> BTree<BK>
	where
		BK: BKeys + Debug + Clone,
	{
		/// This is for debugging
		async fn inspect_nodes<F>(
			&self,
			tx: &Transaction,
			st: &mut BTreeStore<BK>,
			inspect_func: F,
		) -> Result<usize>
		where
			F: Fn(usize, usize, NodeId, Arc<BStoredNode<BK>>),
		{
			let mut node_queue = VecDeque::new();
			if let Some(node_id) = self.state.root {
				node_queue.push_front((node_id, 1));
			}
			let mut count = 0;
			while let Some((node_id, depth)) = node_queue.pop_front() {
				let stored_node = st.get_node(tx, node_id).await?;
				if let BTreeNode::Internal(_, children) = &stored_node.n {
					let depth = depth + 1;
					for child_id in children {
						node_queue.push_back((*child_id, depth));
					}
				}
				inspect_func(count, depth, node_id, stored_node);
				count += 1;
			}
			Ok(count)
		}

		/// This is for debugging
		async fn inspect_nodes_mut<F>(
			&self,
			tx: &Transaction,
			st: &mut BTreeStore<BK>,
			mut inspect_func: F,
		) -> Result<usize>
		where
			F: FnMut(usize, usize, NodeId, &BStoredNode<BK>),
		{
			let mut node_queue = VecDeque::new();
			if let Some(node_id) = self.state.root {
				node_queue.push_front((node_id, 1, None::<Key>, None::<Key>));
			}
			let mut count = 0;
			while let Some((node_id, depth, left_key, right_key)) = node_queue.pop_front() {
				let stored_node = st.get_node_mut(tx, node_id).await?;
				if let BTreeNode::Internal(keys, children) = &stored_node.n {
					let depth = depth + 1;
					let mut child_right_key = None;
					for (idx, child_id) in children.iter().enumerate() {
						let child_left_key = child_right_key;
						child_right_key = keys.get_key(idx);
						if let Some(crk) = &child_left_key {
							if let Some(lk) = &left_key {
								assert_eq!(
									lk.cmp(crk),
									Ordering::Less,
									"left: {} < {} - node: {} - {}",
									String::from_utf8_lossy(lk),
									String::from_utf8_lossy(crk),
									stored_node.id,
									stored_node.n
								);
							}
							if let Some(rk) = &right_key {
								assert_eq!(
									crk.cmp(rk),
									Ordering::Less,
									"right: {} < {} - node: {} - {}",
									String::from_utf8_lossy(crk),
									String::from_utf8_lossy(rk),
									stored_node.id,
									stored_node.n
								);
							}
						}
						node_queue.push_back((
							*child_id,
							depth,
							child_left_key.clone(),
							child_right_key.clone(),
						));
					}
				}
				inspect_func(count, depth, node_id, &stored_node);
				st.set_node(stored_node, false).await?;
				count += 1;
			}
			Ok(count)
		}
	}
}
