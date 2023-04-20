use crate::err::Error;
use crate::idx::bkeys::{BKeys, KeyVisitor};
use crate::idx::SerdeState;
use crate::kvs::{Key, Transaction};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) type NodeId = u64;
pub(super) type Payload = u64;

pub(super) trait KeyProvider {
	fn get_node_key(&self, node_id: NodeId) -> Key;
	fn get_state_key(&self) -> Key {
		panic!("Not supported")
	}
}

pub(super) struct BTree<K>
where
	K: KeyProvider,
{
	keys: K,
	state: State,
	full_size: usize,
	updated: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct State {
	minimum_degree: usize,
	root: Option<NodeId>,
	next_node_id: NodeId,
}

impl SerdeState for State {}

impl State {
	pub(super) fn new(minimum_degree: usize) -> Self {
		assert!(minimum_degree >= 2, "Minimum degree should be >= 2");
		Self {
			minimum_degree,
			root: None,
			next_node_id: 0,
		}
	}
}

#[derive(Debug, Default, PartialEq)]
pub(super) struct Statistics {
	pub(super) keys_count: usize,
	pub(super) max_depth: usize,
	pub(super) nodes_count: usize,
	pub(super) total_size: usize,
}

#[derive(Serialize, Deserialize)]
enum Node<BK>
where
	BK: BKeys,
{
	Internal(BK, Vec<NodeId>),
	Leaf(BK),
}

impl<BK> Node<BK>
where
	BK: BKeys,
{
	fn keys(&self) -> &BK {
		match self {
			Node::Internal(keys, _) => keys,
			Node::Leaf(keys) => keys,
		}
	}

	fn keys_mut(&mut self) -> &mut BK {
		match self {
			Node::Internal(keys, _) => keys,
			Node::Leaf(keys) => keys,
		}
	}

	fn append(&mut self, key: Key, payload: Payload, node: Node<BK>) -> Result<(), Error> {
		match self {
			Node::Internal(keys, children) => {
				if let Node::Internal(append_keys, mut append_children) = node {
					keys.insert(key, payload);
					keys.append(append_keys);
					children.append(&mut append_children);
					Ok(())
				} else {
					Err(Error::CorruptedIndex)
				}
			}
			Node::Leaf(keys) => {
				if let Node::Leaf(append_keys) = node {
					keys.insert(key, payload);
					keys.append(append_keys);
					Ok(())
				} else {
					Err(Error::CorruptedIndex)
				}
			}
		}
	}
}

impl<BK> SerdeState for Node<BK> where BK: BKeys + Serialize + DeserializeOwned {}

struct SplitResult<BK>
where
	BK: BKeys,
{
	parent_node: StoredNode<BK>,
	left_node: StoredNode<BK>,
	right_node: StoredNode<BK>,
	median_key: Key,
}

impl<K> BTree<K>
where
	K: KeyProvider,
{
	pub(super) fn new(keys: K, state: State) -> Self {
		Self {
			keys,
			full_size: state.minimum_degree * 2 - 1,
			state,
			updated: false,
		}
	}

	pub(super) async fn search<BK>(
		&self,
		tx: &mut Transaction,
		searched_key: &Key,
	) -> Result<Option<Payload>, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		Ok(self.locate_key_in_node::<BK>(tx, searched_key).await?.map(|(_, payload)| payload))
	}

	async fn locate_key_in_node<BK>(
		&self,
		tx: &mut Transaction,
		searched_key: &Key,
	) -> Result<Option<(StoredNode<BK>, Payload)>, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = self.state.root {
			node_queue.push_front(node_id);
		}
		while let Some(node_id) = node_queue.pop_front() {
			let stored_node = StoredNode::<BK>::read(tx, self.keys.get_node_key(node_id)).await?;
			if let Some(payload) = stored_node.node.keys().get(searched_key) {
				return Ok(Some((stored_node, payload)));
			}
			if let Node::Internal(keys, children) = stored_node.node {
				let child_idx = keys.get_child_idx(searched_key);
				node_queue.push_front(children[child_idx]);
			}
		}
		Ok(None)
	}

	pub(super) async fn search_by_prefix<BK, V>(
		&self,
		tx: &mut Transaction,
		prefix_key: &Key,
		visitor: &mut V,
	) -> Result<(), Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
		V: KeyVisitor + Send,
	{
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = self.state.root {
			node_queue.push_front((node_id, Arc::new(AtomicBool::new(false))));
		}
		while let Some((node_id, matches_found)) = node_queue.pop_front() {
			let node = StoredNode::<BK>::read(tx, self.keys.get_node_key(node_id)).await?.node;
			if node.keys().collect_with_prefix(tx, prefix_key, visitor).await? {
				matches_found.fetch_and(true, Ordering::Relaxed);
			} else if matches_found.load(Ordering::Relaxed) {
				// If we have found matches in previous (lower) nodes,
				// but we don't find matches anymore, there is no chance we can find new matches
				// in upper child nodes, therefore we can stop the traversal.
				break;
			}
			if let Node::Internal(keys, children) = node {
				let same_level_matches_found = Arc::new(AtomicBool::new(false));
				let child_idx = keys.get_child_idx(prefix_key);
				for i in child_idx..children.len() {
					node_queue.push_front((children[i], same_level_matches_found.clone()));
				}
			}
		}
		Ok(())
	}

	pub(super) async fn insert<BK>(
		&mut self,
		tx: &mut Transaction,
		key: Key,
		payload: Payload,
	) -> Result<(), Error>
	where
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		if let Some(root_id) = self.state.root {
			let root = StoredNode::<BK>::read(tx, self.keys.get_node_key(root_id)).await?;
			if root.node.keys().len() == self.full_size {
				let new_root_id = self.new_node_id();
				let new_root_key = self.keys.get_node_key(new_root_id);
				let new_root_node = Node::Internal(BK::default(), vec![root_id]);
				self.state.root = Some(new_root_id);
				let new_root =
					self.split_child(tx, new_root_key, new_root_node, 0, root).await?.parent_node;
				self.insert_non_full(tx, new_root, key, payload).await?;
			} else {
				self.insert_non_full(tx, root, key, payload).await?;
			}
		} else {
			let new_root_id = self.new_node_id();
			let new_root_node = Node::Leaf(BK::with_key_val(key, payload)?);
			self.state.root = Some(new_root_id);
			StoredNode::<BK>::write(tx, self.keys.get_node_key(new_root_id), new_root_node).await?;
		}
		self.updated = true;
		Ok(())
	}

	async fn insert_non_full<BK>(
		&mut self,
		tx: &mut Transaction,
		node: StoredNode<BK>,
		key: Key,
		payload: Payload,
	) -> Result<(), Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut next_node = Some(node);
		while let Some(mut node) = next_node.take() {
			let key: Key = key.clone();
			match &mut node.node {
				Node::Leaf(keys) => {
					keys.insert(key, payload);
					StoredNode::<BK>::write(tx, node.key, node.node).await?;
				}
				Node::Internal(keys, children) => {
					if keys.get(&key).is_some() {
						keys.insert(key, payload);
						StoredNode::<BK>::write(tx, node.key, node.node).await?;
						return Ok(());
					}
					let child_idx = keys.get_child_idx(&key);
					let child_key = self.keys.get_node_key(children[child_idx]);
					let child_node = StoredNode::<BK>::read(tx, child_key).await?;
					let child_node = if child_node.node.keys().len() == self.full_size {
						let split_result = self
							.split_child::<BK>(tx, node.key, node.node, child_idx, child_node)
							.await?;
						if key.gt(&split_result.median_key) {
							split_result.right_node
						} else {
							split_result.left_node
						}
					} else {
						child_node
					};
					next_node.replace(child_node);
				}
			}
		}
		Ok(())
	}

	async fn split_child<BK>(
		&mut self,
		tx: &mut Transaction,
		parent_key: Key,
		parent_node: Node<BK>,
		idx: usize,
		child_node: StoredNode<BK>,
	) -> Result<SplitResult<BK>, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let (left_node, right_node, median_key, median_payload) = match child_node.node {
			Node::Internal(keys, children) => self.split_internal_node::<BK>(keys, children),
			Node::Leaf(keys) => self.split_leaf_node(keys),
		};
		let right_node_id = self.new_node_id();
		let parent_node = match parent_node {
			Node::Internal(mut keys, mut children) => {
				keys.insert(median_key.clone(), median_payload);
				children.insert(idx + 1, right_node_id);
				Node::Internal(keys, children)
			}
			Node::Leaf(mut keys) => {
				keys.insert(median_key.clone(), median_payload);
				Node::Leaf(keys)
			}
		};
		// Save the mutated split child with half the (lower) keys
		let left_node = StoredNode::<BK>::write(tx, child_node.key, left_node).await?;
		// Save the new child with half the (upper) keys
		let right_node =
			StoredNode::<BK>::write(tx, self.keys.get_node_key(right_node_id), right_node).await?;
		// Save the parent node
		let parent_node = StoredNode::<BK>::write(tx, parent_key, parent_node).await?;
		Ok(SplitResult {
			parent_node,
			left_node,
			right_node,
			median_key,
		})
	}

	fn split_internal_node<BK>(
		&mut self,
		keys: BK,
		mut left_children: Vec<NodeId>,
	) -> (Node<BK>, Node<BK>, Key, Payload)
	where
		BK: BKeys,
	{
		let r = keys.split_keys();
		let right_children = left_children.split_off(r.median_idx + 1);
		let left_node = Node::Internal(r.left, left_children);
		let right_node = Node::Internal(r.right, right_children);
		(left_node, right_node, r.median_key, r.median_payload)
	}

	fn split_leaf_node<BK>(&mut self, keys: BK) -> (Node<BK>, Node<BK>, Key, Payload)
	where
		BK: BKeys,
	{
		let r = keys.split_keys();
		let left_node = Node::Leaf(r.left);
		let right_node = Node::Leaf(r.right);
		(left_node, right_node, r.median_key, r.median_payload)
	}

	fn new_node_id(&mut self) -> NodeId {
		let new_node_id = self.state.next_node_id;
		self.state.next_node_id += 1;
		new_node_id
	}

	pub(super) async fn delete<BK>(
		&mut self,
		tx: &mut Transaction,
		key_to_delete: Key,
	) -> Result<Option<Payload>, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		if let Some((stored_node, deleted_payload)) =
			self.locate_key_in_node(tx, &key_to_delete).await?
		{
			let mut next_node = Some((key_to_delete, deleted_payload, stored_node));
			while let Some((key_to_delete, payload_to_delete, stored_node)) = next_node.take() {
				let mut node: Node<BK> = stored_node.node;
				match &mut node {
					Node::Internal(keys, children) => {
						let left_idx = keys.get_child_idx(&key_to_delete);
						let left_id = children[left_idx];
						let mut left_node =
							StoredNode::<BK>::read(tx, self.keys.get_node_key(left_id)).await?;
						if left_node.node.keys().len() >= self.state.minimum_degree {
							// CLRS: 2a -> left_node is named `y` in the book
							if let Some((key_prim, payload_prim)) =
								left_node.node.keys().get_last_key()
							{
								keys.remove(&key_to_delete);
								keys.insert(key_prim.clone(), payload_prim);
								let stored_node =
									StoredNode::write(tx, stored_node.key, node).await?;
								next_node.replace((key_prim, payload_prim, stored_node));
							}
						} else {
							let right_idx = left_idx + 1;
							let right_id = children[right_idx];
							let right_node =
								StoredNode::<BK>::read(tx, self.keys.get_node_key(right_id))
									.await?;
							if right_node.node.keys().len() >= self.state.minimum_degree {
								// CLRS: 2b -> right_node is name `z` in the book
								if let Some((key_prim, payload_prim)) =
									left_node.node.keys().get_first_key()
								{
									keys.remove(&key_to_delete);
									keys.insert(key_prim.clone(), payload_prim);
									let stored_node =
										StoredNode::write(tx, stored_node.key, node).await?;
									next_node.replace((key_prim, payload_prim, stored_node));
									self.updated = true;
								}
							} else {
								// CLRS: 2c
								// Merge children
								left_node.node.append(
									key_to_delete.clone(),
									deleted_payload,
									right_node.node,
								)?;
								let left_node =
									StoredNode::<BK>::write(tx, left_node.key, left_node.node)
										.await?;
								keys.remove(&key_to_delete);
								children.remove(right_idx);
								StoredNode::<BK>::write(tx, stored_node.key, node).await?;
								next_node = Some((key_to_delete, payload_to_delete, left_node));
								self.updated = true;
							}
						}
					}
					Node::Leaf(keys) => {
						keys.remove(&key_to_delete);
						StoredNode::<BK>::write(tx, stored_node.key, node).await?;
						self.updated = true;
					}
				}
			}
			Ok(Some(deleted_payload))
		} else {
			// CLRS 3
			panic!("CLRS3")
		}
	}

	/// This is for debugging
	async fn inspect_nodes<BK, F>(
		&self,
		tx: &mut Transaction,
		inspect_func: F,
	) -> Result<usize, Error>
	where
		F: Fn(usize, usize, NodeId, StoredNode<BK>),
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = self.state.root {
			node_queue.push_front((node_id, 1));
		}
		let mut count = 0;
		while let Some((node_id, depth)) = node_queue.pop_front() {
			let stored_node = StoredNode::<BK>::read(tx, self.keys.get_node_key(node_id)).await?;
			if let Node::Internal(_, children) = &stored_node.node {
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

	pub(super) async fn statistics<BK>(&self, tx: &mut Transaction) -> Result<Statistics, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut stats = Statistics::default();
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = self.state.root {
			node_queue.push_front((node_id, 1));
		}
		while let Some((node_id, depth)) = node_queue.pop_front() {
			let stored = StoredNode::<BK>::read(tx, self.keys.get_node_key(node_id)).await?;
			stats.keys_count += stored.node.keys().len();
			if depth > stats.max_depth {
				stats.max_depth = depth;
			}
			stats.nodes_count += 1;
			stats.total_size += stored.size;
			if let Node::Internal(_, children) = stored.node {
				let depth = depth + 1;
				for child_id in &children {
					node_queue.push_front((*child_id, depth));
				}
			};
		}
		Ok(stats)
	}

	pub(super) fn get_state(&self) -> &State {
		&self.state
	}

	pub(super) fn is_updated(&self) -> bool {
		self.updated
	}
}

struct StoredNode<BK>
where
	BK: BKeys,
{
	key: Key,
	node: Node<BK>,
	size: usize,
}

impl<BK> StoredNode<BK>
where
	BK: BKeys + Serialize + DeserializeOwned,
{
	async fn read(tx: &mut Transaction, key: Key) -> Result<Self, Error> {
		if let Some(val) = tx.get(key.clone()).await? {
			Ok(Self {
				key,
				size: val.len(),
				node: Node::try_from_val(val)?,
			})
		} else {
			Err(Error::CorruptedIndex)
		}
	}

	async fn write(tx: &mut Transaction, key: Key, mut node: Node<BK>) -> Result<Self, Error> {
		node.keys_mut().compile();
		let val = node.try_to_val()?;
		let size = val.len();
		tx.set(key.clone(), val).await?;
		Ok(Self {
			node,
			size,
			key,
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::bkeys::{BKeys, FstKeys, TrieKeys};
	use crate::idx::btree::{BTree, KeyProvider, Node, NodeId, Payload, State, Statistics};
	use crate::idx::tests::HashVisitor;
	use crate::idx::SerdeState;
	use crate::kvs::{Datastore, Key, Transaction};
	use rand::prelude::{SliceRandom, ThreadRng};
	use rand::thread_rng;
	use serde::de::DeserializeOwned;
	use serde::Serialize;
	use test_log::test;

	struct TestKeyProvider {}

	impl KeyProvider for TestKeyProvider {
		fn get_node_key(&self, node_id: NodeId) -> Key {
			node_id.to_be_bytes().to_vec()
		}
		fn get_state_key(&self) -> Key {
			"".into()
		}
	}

	#[test]
	fn test_btree_state_serde() {
		let s = State::new(3);
		let val = s.try_to_val().unwrap();
		let s: State = State::try_from_val(val).unwrap();
		assert_eq!(s.minimum_degree, 3);
		assert_eq!(s.root, None);
		assert_eq!(s.next_node_id, 0);
	}

	#[test]
	fn test_node_serde_internal() {
		let node = Node::Internal(FstKeys::default(), vec![]);
		let val = node.try_to_val().unwrap();
		let _: Node<FstKeys> = Node::try_from_val(val).unwrap();
	}

	#[test]
	fn test_node_serde_leaf() {
		let node = Node::Leaf(TrieKeys::default());
		let val = node.try_to_val().unwrap();
		let _: Node<TrieKeys> = Node::try_from_val(val).unwrap();
	}

	async fn insertions_test<F, BK, K>(
		tx: &mut Transaction,
		t: &mut BTree<K>,
		samples_size: usize,
		sample_provider: F,
	) where
		F: Fn(usize) -> (Key, Payload),
		BK: BKeys + Serialize + DeserializeOwned + Default,
		K: KeyProvider,
	{
		for i in 0..samples_size {
			let (key, payload) = sample_provider(i);
			// Insert the sample
			t.insert::<BK>(tx, key.clone(), payload).await.unwrap();
			// Check we can find it
			assert_eq!(t.search::<BK>(tx, &key).await.unwrap(), Some(payload));
		}
	}

	fn get_key_value(idx: usize) -> (Key, Payload) {
		(format!("{}", idx).into(), (idx * 10) as Payload)
	}

	#[test(tokio::test)]
	async fn test_btree_fst_small_order_sequential_insertions() {
		let mut t = BTree::new(TestKeyProvider {}, State::new(75));
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		insertions_test::<_, FstKeys, _>(&mut tx, &mut t, 100, get_key_value).await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		assert_eq!(
			t.statistics::<FstKeys>(&mut tx).await.unwrap(),
			Statistics {
				keys_count: 100,
				max_depth: 3,
				nodes_count: 10,
				total_size: 1042,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_small_order_sequential_insertions() {
		let mut t = BTree::new(TestKeyProvider {}, State::new(75));
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		insertions_test::<_, TrieKeys, _>(&mut tx, &mut t, 100, get_key_value).await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		assert_eq!(
			t.statistics::<TrieKeys>(&mut tx).await.unwrap(),
			Statistics {
				keys_count: 100,
				max_depth: 3,
				nodes_count: 16,
				total_size: 1615,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_fst_small_order_random_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(75));
		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);
		insertions_test::<_, FstKeys, _>(&mut tx, &mut t, 100, |i| get_key_value(samples[i])).await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		let s = t.statistics::<FstKeys>(&mut tx).await.unwrap();
		assert_eq!(s.keys_count, 100);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_small_order_random_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(75));
		let mut samples: Vec<usize> = (0..100).collect();
		let mut rng = thread_rng();
		samples.shuffle(&mut rng);
		insertions_test::<_, TrieKeys, _>(&mut tx, &mut t, 100, |i| get_key_value(samples[i]))
			.await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		let s = t.statistics::<TrieKeys>(&mut tx).await.unwrap();
		assert_eq!(s.keys_count, 100);
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_large_order_sequential_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(500));
		insertions_test::<_, FstKeys, _>(&mut tx, &mut t, 10000, get_key_value).await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		assert_eq!(
			t.statistics::<FstKeys>(&mut tx).await.unwrap(),
			Statistics {
				keys_count: 10000,
				max_depth: 3,
				nodes_count: 100,
				total_size: 54548,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_large_order_sequential_insertions() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(500));
		insertions_test::<_, TrieKeys, _>(&mut tx, &mut t, 10000, get_key_value).await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		assert_eq!(
			t.statistics::<TrieKeys>(&mut tx).await.unwrap(),
			Statistics {
				keys_count: 10000,
				max_depth: 3,
				nodes_count: 135,
				total_size: 74107,
			}
		);
	}

	const REAL_WORLD_TERMS: [&str; 30] = [
		"the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "the", "fast",
		"fox", "jumped", "over", "the", "lazy", "dog", "the", "dog", "sat", "there", "and", "did",
		"nothing", "the", "other", "animals", "sat", "there", "watching",
	];

	async fn test_btree_read_world_insertions<BK>(default_btree_order: usize) -> Statistics
	where
		BK: BKeys + Serialize + DeserializeOwned + Default,
	{
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(default_btree_order));
		insertions_test::<_, BK, _>(&mut tx, &mut t, REAL_WORLD_TERMS.len(), |i| {
			(REAL_WORLD_TERMS[i].as_bytes().to_vec(), i as Payload)
		})
		.await;
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		t.statistics::<BK>(&mut tx).await.unwrap()
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_read_world_insertions_small_order() {
		let s = test_btree_read_world_insertions::<FstKeys>(3).await;
		assert_eq!(
			s,
			Statistics {
				keys_count: 17,
				max_depth: 2,
				nodes_count: 3,
				total_size: 317,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_fst_keys_read_world_insertions_large_order() {
		let s = test_btree_read_world_insertions::<FstKeys>(100).await;
		assert_eq!(
			s,
			Statistics {
				keys_count: 17,
				max_depth: 1,
				nodes_count: 1,
				total_size: 192,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_read_world_insertions_small_order() {
		let s = test_btree_read_world_insertions::<TrieKeys>(3).await;
		assert_eq!(
			s,
			Statistics {
				keys_count: 17,
				max_depth: 2,
				nodes_count: 3,
				total_size: 346,
			}
		);
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_read_world_insertions_large_order() {
		let s = test_btree_read_world_insertions::<TrieKeys>(100).await;
		assert_eq!(
			s,
			Statistics {
				keys_count: 17,
				max_depth: 1,
				nodes_count: 1,
				total_size: 232,
			}
		);
	}

	async fn test_btree_search_by_prefix(
		ds: &Datastore,
		order: usize,
		shuffle: bool,
		mut samples: Vec<(&str, Payload)>,
	) -> (BTree<TestKeyProvider>, Statistics) {
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(order));
		let samples_len = samples.len();
		if shuffle {
			samples.shuffle(&mut ThreadRng::default());
		}
		for (key, payload) in samples {
			t.insert::<TrieKeys>(&mut tx, key.into(), payload).await.unwrap();
		}
		tx.commit().await.unwrap();
		let mut tx = ds.transaction(false, false).await.unwrap();
		let s = t.statistics::<TrieKeys>(&mut tx).await.unwrap();
		assert_eq!(s.keys_count, samples_len);
		(t, s)
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_search_by_prefix() {
		for _ in 0..50 {
			let samples = vec![
				("aaaa", 0),
				("bb1", 21),
				("bb2", 22),
				("bb3", 23),
				("bb4", 24),
				("dddd", 0),
				("eeee", 0),
				("ffff", 0),
				("gggg", 0),
				("hhhh", 0),
			];
			let ds = Datastore::new("memory").await.unwrap();
			let (t, s) = test_btree_search_by_prefix(&ds, 3, true, samples).await;

			// For this test to be relevant, we expect the BTree to match the following properties:
			assert_eq!(s.max_depth, 2, "Tree depth should be 2");
			assert!(
				s.nodes_count > 2 && s.nodes_count < 5,
				"The number of node should be between 3 and 4 inclusive"
			);

			let mut tx = ds.transaction(false, false).await.unwrap();

			// We should find all the keys prefixed with "bb"
			let mut visitor = HashVisitor::default();
			t.search_by_prefix::<TrieKeys, _>(&mut tx, &"bb".into(), &mut visitor).await.unwrap();
			visitor.check(
				vec![
					("bb1".into(), 21),
					("bb2".into(), 22),
					("bb3".into(), 23),
					("bb4".into(), 24),
				],
				"bb",
			);
		}
	}

	#[test(tokio::test)]
	async fn test_btree_trie_keys_real_world_search_by_prefix() {
		// We do multiples tests to run the test over many different possible forms of Tree.
		// The samples are shuffled, therefore the insertion order is different on each test,
		// ending up in slightly different variants of the BTrees.
		for _ in 0..50 {
			// This samples simulate postings. Pair of terms and document ids.
			let samples = vec![
				("the-1", 0),
				("quick-1", 0),
				("brown-1", 0),
				("fox-1", 0),
				("jumped-1", 0),
				("over-1", 0),
				("lazy-1", 0),
				("dog-1", 0),
				("the-2", 0),
				("fast-2", 0),
				("fox-2", 0),
				("jumped-2", 0),
				("over-2", 0),
				("lazy-2", 0),
				("dog-2", 0),
				("the-3", 0),
				("dog-3", 0),
				("sat-3", 0),
				("there-3", 0),
				("and-3", 0),
				("did-3", 0),
				("nothing-3", 0),
				("the-4", 0),
				("other-4", 0),
				("animals-4", 0),
				("sat-4", 0),
				("there-4", 0),
				("watching-4", 0),
			];

			let ds = Datastore::new("memory").await.unwrap();
			let (t, s) = test_btree_search_by_prefix(&ds, 3, true, samples).await;

			// For this test to be relevant, we expect the BTree to match the following properties:
			assert_eq!(s.max_depth, 2, "Tree depth should be 2");
			assert!(
				s.nodes_count > 2 && s.nodes_count < 5,
				"The number of node should be > 2 and < 5"
			);

			let mut tx = ds.transaction(false, false).await.unwrap();

			for (prefix, count) in vec![
				("the", 6),
				("there", 2),
				("dog", 3),
				("jumped", 2),
				("lazy", 2),
				("fox", 2),
				("over", 2),
				("sat", 2),
				("other", 1),
				("nothing", 1),
				("animals", 1),
				("watching", 1),
			] {
				let mut visitor = HashVisitor::default();
				t.search_by_prefix::<TrieKeys, _>(&mut tx, &prefix.into(), &mut visitor)
					.await
					.unwrap();
				visitor.check_len(count, prefix);
			}
		}
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
		let mut t = BTree::new(TestKeyProvider {}, State::new(3));
		let mut tx = ds.transaction(true, false).await.unwrap();
		for (key, payload) in CLRS_EXAMPLE {
			t.insert::<TrieKeys>(&mut tx, key.into(), payload).await.unwrap();
		}
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(false, false).await.unwrap();
		let s = t.statistics::<TrieKeys>(&mut tx).await.unwrap();
		assert_eq!(s.keys_count, 23);
		assert_eq!(s.max_depth, 3);
		assert_eq!(s.nodes_count, 10);

		let nodes_count = t
			.inspect_nodes::<TrieKeys, _>(&mut tx, |count, depth, node_id, node| match count {
				0 => {
					assert_eq!(depth, 1);
					assert_eq!(node_id, 7);
					check_is_internal_node(node.node, vec![("p", 16)], vec![1, 8]);
				}
				1 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 1);
					check_is_internal_node(
						node.node,
						vec![("c", 3), ("g", 7), ("m", 13)],
						vec![0, 9, 2, 3],
					);
				}
				2 => {
					assert_eq!(depth, 2);
					assert_eq!(node_id, 8);
					check_is_internal_node(node.node, vec![("t", 20), ("x", 24)], vec![4, 6, 5]);
				}
				3 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 0);
					check_is_leaf_node(node.node, vec![("a", 1), ("b", 2)]);
				}
				4 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 9);
					check_is_leaf_node(node.node, vec![("d", 4), ("e", 5), ("f", 6)]);
				}
				5 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 2);
					check_is_leaf_node(node.node, vec![("j", 10), ("k", 11), ("l", 12)]);
				}
				6 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 3);
					check_is_leaf_node(node.node, vec![("n", 14), ("o", 15)]);
				}
				7 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 4);
					check_is_leaf_node(node.node, vec![("q", 17), ("r", 18), ("s", 19)]);
				}
				8 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 6);
					check_is_leaf_node(node.node, vec![("u", 21), ("v", 22)]);
				}
				9 => {
					assert_eq!(depth, 3);
					assert_eq!(node_id, 5);
					check_is_leaf_node(node.node, vec![("y", 25), ("z", 26)]);
				}
				_ => assert!(false, "This node should not exist {}", count),
			})
			.await
			.unwrap();
		assert_eq!(nodes_count, 10);
	}

	#[test(tokio::test)]
	// This check the possible deletion cases. CRLS, Figure 18.8, pages 500-501
	async fn clrs_deletion_test() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut t = BTree::new(TestKeyProvider {}, State::new(3));
		let mut tx = ds.transaction(true, false).await.unwrap();
		for (key, payload) in CLRS_EXAMPLE {
			t.insert::<TrieKeys>(&mut tx, key.into(), payload).await.unwrap();
		}
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(true, false).await.unwrap();
		for (key, payload) in [("f", 6) /* ("m", 13) */] {
			assert_eq!(t.delete::<TrieKeys>(&mut tx, key.into()).await.unwrap(), Some(payload));
		}
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(false, false).await.unwrap();
		// let s = t.statistics::<TrieKeys>(&mut tx).await.unwrap();
		//assert_eq!(s.keys_count, 23);
		//assert_eq!(s.max_depth, 3);
		// assert_eq!(s.nodes_count, 10);

		let nodes_count = t
			.inspect_nodes::<TrieKeys, _>(&mut tx, |_count, depth, node_id, node| {
				debug!("{} -> {}", depth, node_id);
				node.node.keys().debug(|k| Ok(String::from_utf8(k)?)).unwrap();
				// match count {
				// 	0 => {
				// 		assert_eq!(depth, 1);
				// 		// assert_eq!(node_id, 44);
				// 		node.node.keys().debug(|k| Ok(String::from_utf8(k)?)).unwrap();
				// 		check_is_internal_node(node.node, vec![("p", 16)], vec![]);
				// 	}
				// 	2 => {
				// 		assert_eq!(depth, 2);
				// 		assert_eq!(node_id, 2);
				// 		check_is_leaf_node(node.node, vec![("a", 1), ("b", 2)]);
				// 	}
				// 	_ => assert!(false, "This node should not exist {}", count),
				// }
			})
			.await
			.unwrap();
		assert_eq!(nodes_count, 10);
	}

	fn check_is_internal_node<BK>(
		node: Node<BK>,
		expected_keys: Vec<(&str, i32)>,
		expected_children: Vec<NodeId>,
	) where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		if let Node::Internal(keys, children) = node {
			check_keys(keys, expected_keys);
			assert_eq!(children, expected_children, "The children are not matching");
		} else {
			assert!(false, "An internal node was expected, we got a leaf node");
		}
	}

	fn check_is_leaf_node<BK>(node: Node<BK>, expected_keys: Vec<(&str, i32)>)
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		if let Node::Leaf(keys) = node {
			check_keys(keys, expected_keys);
		} else {
			assert!(false, "An internal node was expected, we got a leaf node");
		}
	}

	fn check_keys<BK>(keys: BK, expected_keys: Vec<(&str, i32)>)
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		assert_eq!(keys.len(), expected_keys.len(), "The number of keys does not match");
		for (key, payload) in expected_keys {
			assert_eq!(
				keys.get(&key.into()),
				Some(payload as Payload),
				"The key {} does not match",
				key
			);
		}
	}
}
