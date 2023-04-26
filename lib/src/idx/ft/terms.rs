use crate::err::Error;
use crate::idx::bkeys::FstKeys;
use crate::idx::btree::{BTree, KeyProvider, NodeId, Statistics};
use crate::idx::ft::postings::TermFrequency;
use crate::idx::{btree, IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(crate) type TermId = u64;

pub(super) struct Terms {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<TermsKeyProvider>,
	available_ids: Option<RoaringTreemap>,
	next_term_id: TermId,
	updated: bool,
}

impl Terms {
	pub(super) async fn new(
		tx: &mut Transaction,
		index_key_base: IndexKeyBase,
		default_btree_order: usize,
	) -> Result<Self, Error> {
		let keys = TermsKeyProvider {
			index_key_base: index_key_base.clone(),
		};
		let state_key: Key = keys.get_state_key();
		let state: State = if let Some(val) = tx.get(state_key.clone()).await? {
			State::try_from_val(val)?
		} else {
			State::new(default_btree_order)
		};
		Ok(Self {
			state_key,
			index_key_base,
			btree: BTree::new(keys, state.btree),
			available_ids: state.available_ids,
			next_term_id: state.next_term_id,
			updated: false,
		})
	}

	fn get_next_term_id(&mut self) -> TermId {
		// We check first if there is any available id
		if let Some(available_ids) = &mut self.available_ids {
			if let Some(available_id) = available_ids.iter().next() {
				available_ids.remove(available_id);
				if available_ids.is_empty() {
					self.available_ids = None;
				}
				return available_id;
			}
		}
		// If not, we use the sequence
		let term_id = self.next_term_id;
		self.next_term_id += 1;
		term_id
	}

	pub(super) async fn resolve_term_ids(
		&mut self,
		tx: &mut Transaction,
		terms_frequencies: HashMap<&str, TermFrequency>,
	) -> Result<HashMap<TermId, TermFrequency>, Error> {
		let mut res = HashMap::with_capacity(terms_frequencies.len());
		for (term, freq) in terms_frequencies {
			res.insert(self.resolve_term_id(tx, term).await?, freq);
		}
		Ok(res)
	}

	async fn resolve_term_id(&mut self, tx: &mut Transaction, term: &str) -> Result<TermId, Error> {
		let term_key = term.into();
		if let Some(term_id) = self.btree.search::<FstKeys>(tx, &term_key).await? {
			Ok(term_id)
		} else {
			let term_id = self.get_next_term_id();
			tx.set(self.index_key_base.new_bu_key(term_id), term_key.clone()).await?;
			self.btree.insert::<FstKeys>(tx, term_key, term_id).await?;
			self.updated = true;
			Ok(term_id)
		}
	}

	pub(super) async fn get_term_id(
		&self,
		tx: &mut Transaction,
		term: &str,
	) -> Result<Option<TermId>, Error> {
		self.btree.search::<FstKeys>(tx, &term.into()).await
	}

	pub(super) async fn remove_term_id(
		&mut self,
		tx: &mut Transaction,
		term_id: TermId,
	) -> Result<(), Error> {
		let term_id_key = self.index_key_base.new_bu_key(term_id);
		if let Some(term_key) = tx.get(term_id_key.clone()).await? {
			debug!("Delete In {}", String::from_utf8(term_key.clone()).unwrap());
			self.btree.delete::<FstKeys>(tx, term_key.clone()).await?;
			debug!("Delete Out {}", String::from_utf8(term_key.clone()).unwrap());
			tx.del(term_id_key).await?;
			if let Some(available_ids) = &mut self.available_ids {
				available_ids.insert(term_id);
			} else {
				let mut available_ids = RoaringTreemap::new();
				available_ids.insert(term_id);
				self.available_ids = Some(available_ids);
			}
			self.updated = true;
		}
		Ok(())
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<Statistics, Error> {
		self.btree.statistics::<FstKeys>(tx).await
	}

	pub(super) async fn finish(self, tx: &mut Transaction) -> Result<(), Error> {
		if self.updated || self.btree.is_updated() {
			let state = State {
				btree: self.btree.get_state().clone(),
				available_ids: self.available_ids,
				next_term_id: self.next_term_id,
			};
			tx.set(self.state_key, state.try_to_val()?).await?;
		}
		Ok(())
	}
}

#[derive(Serialize, Deserialize)]
struct State {
	btree: btree::State,
	available_ids: Option<RoaringTreemap>,
	next_term_id: TermId,
}

impl SerdeState for State {}

impl State {
	fn new(default_btree_order: usize) -> Self {
		Self {
			btree: btree::State::new(default_btree_order),
			available_ids: None,
			next_term_id: 0,
		}
	}
}

struct TermsKeyProvider {
	index_key_base: IndexKeyBase,
}

impl KeyProvider for TermsKeyProvider {
	fn get_node_key(&self, node_id: NodeId) -> Key {
		self.index_key_base.new_bt_key(Some(node_id))
	}
	fn get_state_key(&self) -> Key {
		self.index_key_base.new_bt_key(None)
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::postings::TermFrequency;
	use crate::idx::ft::terms::Terms;
	use crate::idx::IndexKeyBase;
	use crate::kvs::Datastore;
	use rand::{thread_rng, Rng};
	use std::collections::{HashMap, HashSet};

	fn random_term(key_length: usize) -> String {
		thread_rng()
			.sample_iter(&rand::distributions::Alphanumeric)
			.take(key_length)
			.map(char::from)
			.collect()
	}

	fn unique_terms(key_length: usize, count: usize) -> HashSet<String> {
		let mut set = HashSet::new();
		while set.len() < count {
			set.insert(random_term(key_length));
		}
		set
	}

	#[tokio::test]
	async fn test_resolve_terms() {
		const BTREE_ORDER: usize = 7;

		let idx = IndexKeyBase::default();

		let ds = Datastore::new("memory").await.unwrap();

		let mut tx = ds.transaction(true, false).await.unwrap();
		let t = Terms::new(&mut tx, idx.clone(), BTREE_ORDER).await.unwrap();
		t.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		// Resolve a first term
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = Terms::new(&mut tx, idx.clone(), BTREE_ORDER).await.unwrap();
		let res = t.resolve_term_ids(&mut tx, HashMap::from([("C", 103)])).await.unwrap();
		assert_eq!(t.statistics(&mut tx).await.unwrap().keys_count, 1);
		t.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(res, HashMap::from([(0, 103)]));

		// Resolve a second term
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = Terms::new(&mut tx, idx.clone(), BTREE_ORDER).await.unwrap();
		let res = t.resolve_term_ids(&mut tx, HashMap::from([("D", 104)])).await.unwrap();
		assert_eq!(t.statistics(&mut tx).await.unwrap().keys_count, 2);
		t.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(res, HashMap::from([(1, 104)]));

		// Resolve two existing terms with new frequencies
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = Terms::new(&mut tx, idx.clone(), BTREE_ORDER).await.unwrap();
		let res =
			t.resolve_term_ids(&mut tx, HashMap::from([("C", 113), ("D", 114)])).await.unwrap();
		assert_eq!(t.statistics(&mut tx).await.unwrap().keys_count, 2);
		t.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(res, HashMap::from([(0, 113), (1, 114)]));

		// Resolve one existing terms and two new terms
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = Terms::new(&mut tx, idx.clone(), BTREE_ORDER).await.unwrap();
		let res = t
			.resolve_term_ids(&mut tx, HashMap::from([("A", 101), ("C", 123), ("E", 105)]))
			.await
			.unwrap();
		assert_eq!(t.statistics(&mut tx).await.unwrap().keys_count, 4);
		t.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
		assert!(
			res.eq(&HashMap::from([(3, 101), (0, 123), (2, 105)]))
				|| res.eq(&HashMap::from([(2, 101), (0, 123), (3, 105)]))
		);
	}

	#[tokio::test]
	async fn test_deletion() {
		const BTREE_ORDER: usize = 7;

		let idx = IndexKeyBase::default();

		let ds = Datastore::new("memory").await.unwrap();

		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = Terms::new(&mut tx, idx.clone(), BTREE_ORDER).await.unwrap();

		// Check removing an non-existing term id returns None
		assert!(t.remove_term_id(&mut tx, 0).await.is_ok());

		// Create few terms
		t.resolve_term_ids(&mut tx, HashMap::from([("A", 101), ("C", 123), ("E", 105)]))
			.await
			.unwrap();

		for term in ["A", "C", "E"] {
			let term_id = t.get_term_id(&mut tx, term).await.unwrap();
			if let Some(term_id) = term_id {
				t.remove_term_id(&mut tx, term_id).await.unwrap();
				assert_eq!(t.get_term_id(&mut tx, term).await.unwrap(), None);
			} else {
				assert!(false, "Term ID not found: {}", term);
			}
		}

		// Check id recycling
		let res =
			t.resolve_term_ids(&mut tx, HashMap::from([("B", 102), ("D", 104)])).await.unwrap();
		assert!(
			res.eq(&HashMap::from([(0, 102), (1, 104)]))
				|| res.eq(&HashMap::from([(0, 104), (1, 102)]))
		);

		t.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
	}

	fn random_term_freq_vec(term_count: usize) -> Vec<(String, TermFrequency)> {
		let mut i = 1;
		let mut vec = Vec::with_capacity(term_count);
		for term in unique_terms(5, term_count) {
			vec.push((term, i));
			i += 1;
		}
		vec
	}

	#[tokio::test]
	async fn test_resolve_100_docs_with_50_words_one_by_one() {
		let ds = Datastore::new("memory").await.unwrap();
		for _ in 0..100 {
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut t = Terms::new(&mut tx, IndexKeyBase::default(), 100).await.unwrap();
			let terms_string = random_term_freq_vec(50);
			let terms_str: HashMap<&str, TermFrequency> =
				terms_string.iter().map(|(t, f)| (t.as_str(), *f)).collect();
			t.resolve_term_ids(&mut tx, terms_str).await.unwrap();
			t.finish(&mut tx).await.unwrap();
			tx.commit().await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_resolve_100_docs_with_50_words_batch_of_10() {
		let ds = Datastore::new("memory").await.unwrap();
		for _ in 0..10 {
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut t = Terms::new(&mut tx, IndexKeyBase::default(), 100).await.unwrap();
			for _ in 0..10 {
				let terms_string = random_term_freq_vec(50);
				let terms_str: HashMap<&str, TermFrequency> =
					terms_string.iter().map(|(t, f)| (t.as_str(), *f)).collect();
				t.resolve_term_ids(&mut tx, terms_str).await.unwrap();
			}
			t.finish(&mut tx).await.unwrap();
			tx.commit().await.unwrap();
		}
	}
}
