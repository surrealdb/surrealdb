use crate::err::Error;
use crate::idx::bkeys::FstKeys;
use crate::idx::btree::{BTree, Statistics};
use crate::idx::ft::postings::TermFrequency;
use crate::idx::{BaseStateKey, IndexId, TERMS_DOMAIN};
use crate::kvs::{Key, Transaction, Val};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(super) type TermId = u64;

pub(super) struct Terms {
	state_key: Key,
	state: State,
	updated: bool,
}

#[derive(Serialize, Deserialize)]
struct State {
	btree: BTree,
	next_term_id: TermId,
}

impl State {
	fn new(index_id: IndexId, btree_order: usize) -> Self {
		Self {
			btree: BTree::new(TERMS_DOMAIN, index_id, btree_order),
			next_term_id: 0,
		}
	}
}

impl TryFrom<Val> for State {
	type Error = bincode::Error;

	fn try_from(val: Val) -> Result<State, Self::Error> {
		bincode::deserialize(val.as_slice())
	}
}

impl TryInto<Val> for State {
	type Error = bincode::Error;

	fn try_into(self) -> Result<Val, Self::Error> {
		bincode::serialize(&self)
	}
}

impl Terms {
	pub(super) async fn new(
		tx: &mut Transaction,
		index_id: IndexId,
		default_btree_order: usize,
	) -> Result<Self, Error> {
		let state_key: Key = BaseStateKey::new(TERMS_DOMAIN, index_id).into();
		let state: State = if let Some(val) = tx.get(state_key.clone()).await? {
			State::try_from(val)?
		} else {
			State::new(index_id, default_btree_order)
		};
		Ok(Self {
			state,
			updated: false,
			state_key,
		})
	}

	pub(super) async fn resolve_terms(
		&mut self,
		tx: &mut Transaction,
		terms_frequencies: HashMap<&str, TermFrequency>,
	) -> Result<HashMap<TermId, TermFrequency>, Error> {
		let mut res = HashMap::with_capacity(terms_frequencies.len());
		for (term, freq) in terms_frequencies {
			res.insert(self.resolve_term(tx, term).await?, freq);
		}
		Ok(res)
	}

	async fn resolve_term(&mut self, tx: &mut Transaction, term: &str) -> Result<TermId, Error> {
		let term = term.into();
		if let Some(term_id) = self.state.btree.search::<FstKeys>(tx, &term).await? {
			Ok(term_id)
		} else {
			let term_id = self.state.next_term_id;
			self.state.btree.insert::<FstKeys>(tx, term, term_id).await?;
			self.state.next_term_id += 1;
			self.updated = true;
			Ok(term_id)
		}
	}

	pub(super) async fn find_term(
		&self,
		tx: &mut Transaction,
		term: &str,
	) -> Result<Option<TermId>, Error> {
		self.state.btree.search::<FstKeys>(tx, &term.into()).await
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<Statistics, Error> {
		self.state.btree.statistics::<FstKeys>(tx).await
	}

	pub(super) async fn debug(&self, tx: &mut Transaction) -> Result<(), Error> {
		let state_key: BaseStateKey = self.state_key.clone().into();
		debug!("TERMS {:?}", state_key);
		self.state.btree.debug::<_, FstKeys>(tx, |k| Ok(String::from_utf8(k)?)).await
	}

	pub(super) async fn finish(self, tx: &mut Transaction) -> Result<(), Error> {
		if self.updated {
			let val: Vec<u8> = self.state.try_into()?;
			tx.set(self.state_key, val).await?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::postings::TermFrequency;
	use crate::idx::ft::terms::Terms;
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
		const BTREE_ORDER: usize = 75;

		let ds = Datastore::new("memory").await.unwrap();

		let mut tx = ds.transaction(true, false).await.unwrap();
		let t = Terms::new(&mut tx, 0, BTREE_ORDER).await.unwrap();
		t.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		// Resolve a first term
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = Terms::new(&mut tx, 0, BTREE_ORDER).await.unwrap();
		let res = t.resolve_terms(&mut tx, HashMap::from([("C", 103)])).await.unwrap();
		assert_eq!(t.statistics(&mut tx).await.unwrap().keys_count, 1);
		t.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(res, HashMap::from([(0, 103)]));

		// Resolve a second term
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = Terms::new(&mut tx, 0, BTREE_ORDER).await.unwrap();
		let res = t.resolve_terms(&mut tx, HashMap::from([("D", 104)])).await.unwrap();
		assert_eq!(t.statistics(&mut tx).await.unwrap().keys_count, 2);
		t.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(res, HashMap::from([(1, 104)]));

		// Resolve two existing terms with new frequencies
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = Terms::new(&mut tx, 0, BTREE_ORDER).await.unwrap();
		let res = t.resolve_terms(&mut tx, HashMap::from([("C", 113), ("D", 114)])).await.unwrap();
		assert_eq!(t.statistics(&mut tx).await.unwrap().keys_count, 2);
		t.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(res, HashMap::from([(0, 113), (1, 114)]));

		// Resolve one existing terms and two new terms
		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut t = Terms::new(&mut tx, 0, BTREE_ORDER).await.unwrap();
		let res = t
			.resolve_terms(&mut tx, HashMap::from([("A", 101), ("C", 123), ("E", 105)]))
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
			let mut t = Terms::new(&mut tx, 0, 100).await.unwrap();
			let terms_string = random_term_freq_vec(50);
			let terms_str: HashMap<&str, TermFrequency> =
				terms_string.iter().map(|(t, f)| (t.as_str(), *f)).collect();
			t.resolve_terms(&mut tx, terms_str).await.unwrap();
			t.finish(&mut tx).await.unwrap();
			tx.commit().await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_resolve_100_docs_with_50_words_batch_of_10() {
		let ds = Datastore::new("memory").await.unwrap();
		for _ in 0..10 {
			let mut tx = ds.transaction(true, false).await.unwrap();
			let mut t = Terms::new(&mut tx, 0, 100).await.unwrap();
			for _ in 0..10 {
				let terms_string = random_term_freq_vec(50);
				let terms_str: HashMap<&str, TermFrequency> =
					terms_string.iter().map(|(t, f)| (t.as_str(), *f)).collect();
				t.resolve_terms(&mut tx, terms_str).await.unwrap();
			}
			t.finish(&mut tx).await.unwrap();
			tx.commit().await.unwrap();
		}
	}
}
