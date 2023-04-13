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
	updated: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct State {
	order: usize,
	root: Option<NodeId>,
	next_node_id: NodeId,
}

impl SerdeState for State {}

impl State {
	pub(super) fn new(order: usize) -> Self {
		Self {
			order,
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
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = self.state.root {
			node_queue.push_front(node_id);
		}
		while let Some(node_id) = node_queue.pop_front() {
			let node = StoredNode::<BK>::read(tx, self.keys.get_node_key(node_id)).await?.node;
			if let Some(value) = node.keys().get(searched_key) {
				return Ok(Some(value));
			}
			if let Node::Internal(keys, children) = node {
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
			if root.is_full(self.state.order * 2) {
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
					let child_node = if child_node.is_full(self.state.order * 2) {
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

	pub(super) async fn debug<F, BK>(&self, tx: &mut Transaction, to_string: F) -> Result<(), Error>
	where
		F: Fn(Key) -> Result<String, Error>,
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let mut node_queue = VecDeque::new();
		if let Some(node_id) = self.state.root {
			node_queue.push_front((node_id, 0));
		}
		while let Some((node_id, depth)) = node_queue.pop_front() {
			let node = StoredNode::<BK>::read(tx, self.keys.get_node_key(node_id)).await?.node;
			debug!("Node: {} - depth: {} -  keys: ", node_id, depth);
			node.keys().debug(|k| to_string(k))?;
			if let Node::Internal(_, children) = node {
				debug!("children: {:?}", children);
				let depth = depth + 1;
				for child_id in &children {
					node_queue.push_front((*child_id, depth));
				}
			}
		}
		Ok(())
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
			Err(Error::CorruptedIndex(None))
		}
	}

	async fn write(tx: &mut Transaction, key: Key, mut node: Node<BK>) -> Result<Self, Error> {
		node.keys_mut().compile();
		let val = node.try_to_val()?;
		let size = val.len();
		tx.set(key.clone(), val).await?;
		Ok(Self {
			key,
			size,
			node,
		})
	}

	fn is_full(&self, full_size: usize) -> bool {
		self.size >= full_size
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
		let s = State::new(75);
		let val = s.try_to_val().unwrap();
		let s: State = State::try_from_val(val).unwrap();
		assert_eq!(s.order, 75);
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
		t.debug::<_, FstKeys>(&mut tx, |k| Ok(String::from_utf8(k)?)).await.unwrap();
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
		t.debug::<_, TrieKeys>(&mut tx, |k| Ok(String::from_utf8(k)?)).await.unwrap();
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
		t.debug::<_, FstKeys>(&mut tx, |k| Ok(String::from_utf8(k)?)).await.unwrap();
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
		t.debug::<_, TrieKeys>(&mut tx, |k| Ok(String::from_utf8(k)?)).await.unwrap();
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
		let s = test_btree_read_world_insertions::<FstKeys>(70).await;
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
		let s = test_btree_read_world_insertions::<FstKeys>(1000).await;
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
		let s = test_btree_read_world_insertions::<TrieKeys>(70).await;
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
		let s = test_btree_read_world_insertions::<TrieKeys>(1000).await;
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
			let (t, s) = test_btree_search_by_prefix(&ds, 45, true, samples).await;

			// For this test to be relevant, we expect the BTree to match the following properties:
			assert!(s.max_depth > 1, "Tree depth should be > 1");
			assert!(s.nodes_count > 2, "The number of node should be > 2");

			let mut tx = ds.transaction(false, false).await.unwrap();
			t.debug::<_, TrieKeys>(&mut tx, |k| Ok(String::from_utf8(k)?)).await.unwrap();

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
			let (t, s) = test_btree_search_by_prefix(&ds, 75, true, samples).await;

			// For this test to be relevant, we expect the BTree to match the following properties:
			assert!(s.max_depth > 1, "Tree depth should be > 1");
			assert!(s.nodes_count > 2, "The number of node should be > 2");

			let mut tx = ds.transaction(false, false).await.unwrap();
			t.debug::<_, TrieKeys>(&mut tx, |k| Ok(String::from_utf8(k)?)).await.unwrap();

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
}
