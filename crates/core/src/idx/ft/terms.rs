use crate::err::Error;
use crate::idx::trees::bkeys::FstKeys;
use crate::idx::trees::btree::{BState, BState1, BState1skip, BStatistics, BTree, BTreeStore};
use crate::idx::trees::store::TreeNodeProvider;
use crate::idx::{IndexKeyBase, VersionedStore};
use crate::kvs::{Key, Transaction, TransactionType, Val};
use revision::{revisioned, Revisioned};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

pub(crate) type TermId = u64;
pub(crate) type TermLen = u32;

pub(in crate::idx) struct Terms {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<FstKeys>,
	store: BTreeStore<FstKeys>,
	available_ids: Option<RoaringTreemap>,
	next_term_id: TermId,
}

impl Terms {
	pub(super) async fn new(
		tx: &Transaction,
		index_key_base: IndexKeyBase,
		default_btree_order: u32,
		tt: TransactionType,
		cache_size: u32,
	) -> Result<Self, Error> {
		let state_key: Key = index_key_base.new_bt_key(None);
		let state: State = if let Some(val) = tx.get(state_key.clone(), None).await? {
			VersionedStore::try_from(val)?
		} else {
			State::new(default_btree_order)
		};
		let store = tx
			.index_caches()
			.get_store_btree_fst(
				TreeNodeProvider::Terms(index_key_base.clone()),
				state.btree.generation(),
				tt,
				cache_size as usize,
			)
			.await;
		Ok(Self {
			state_key,
			index_key_base,
			btree: BTree::new(state.btree),
			store,
			available_ids: state.available_ids,
			next_term_id: state.next_term_id,
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

	pub(super) async fn resolve_term_id(
		&mut self,
		tx: &Transaction,
		term: &str,
	) -> Result<TermId, Error> {
		let term_key = term.into();
		{
			if let Some(term_id) = self.btree.search_mut(tx, &mut self.store, &term_key).await? {
				return Ok(term_id);
			}
		}
		let term_id = self.get_next_term_id();
		tx.set(self.index_key_base.new_bu_key(term_id), term_key.clone(), None).await?;
		self.btree.insert(tx, &mut self.store, term_key, term_id).await?;
		Ok(term_id)
	}

	pub(super) async fn get_term_id(
		&self,
		tx: &Transaction,
		term: &str,
	) -> Result<Option<TermId>, Error> {
		self.btree.search(tx, &self.store, &term.into()).await
	}

	pub(super) async fn remove_term_id(
		&mut self,
		tx: &Transaction,
		term_id: TermId,
	) -> Result<(), Error> {
		let term_id_key = self.index_key_base.new_bu_key(term_id);
		if let Some(term_key) = tx.get(term_id_key.clone(), None).await? {
			self.btree.delete(tx, &mut self.store, term_key.clone()).await?;
			tx.del(term_id_key).await?;
			if let Some(available_ids) = &mut self.available_ids {
				available_ids.insert(term_id);
			} else {
				let mut available_ids = RoaringTreemap::new();
				available_ids.insert(term_id);
				self.available_ids = Some(available_ids);
			}
		}
		Ok(())
	}

	pub(super) async fn statistics(&self, tx: &Transaction) -> Result<BStatistics, Error> {
		self.btree.statistics(tx, &self.store).await
	}

	pub(super) async fn finish(&mut self, tx: &Transaction) -> Result<(), Error> {
		if let Some(new_cache) = self.store.finish(tx).await? {
			let btree = self.btree.inc_generation().clone();
			let state = State {
				btree,
				available_ids: self.available_ids.take(),
				next_term_id: self.next_term_id,
			};
			tx.set(self.state_key.clone(), VersionedStore::try_into(&state)?, None).await?;
			tx.index_caches().advance_store_btree_fst(new_cache);
		}
		Ok(())
	}
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
struct State {
	btree: BState,
	available_ids: Option<RoaringTreemap>,
	next_term_id: TermId,
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
struct State1 {
	btree: BState1,
	available_ids: Option<RoaringTreemap>,
	next_term_id: TermId,
}

impl VersionedStore for State1 {}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
struct State1skip {
	btree: BState1skip,
	available_ids: Option<RoaringTreemap>,
	next_term_id: TermId,
}

impl VersionedStore for State1skip {}

impl From<State1> for State {
	fn from(state: State1) -> Self {
		Self {
			btree: state.btree.into(),
			available_ids: state.available_ids,
			next_term_id: state.next_term_id,
		}
	}
}

impl From<State1skip> for State {
	fn from(state: State1skip) -> Self {
		Self {
			btree: state.btree.into(),
			available_ids: state.available_ids,
			next_term_id: state.next_term_id,
		}
	}
}

impl State {
	fn new(default_btree_order: u32) -> Self {
		Self {
			btree: BState::new(default_btree_order),
			available_ids: None,
			next_term_id: 0,
		}
	}
}

impl VersionedStore for State {
	fn try_from(val: Val) -> Result<Self, Error> {
		match Self::deserialize_revisioned(&mut val.as_slice()) {
			Ok(r) => Ok(r),
			// If it fails here, there is the chance it was an old version of BState
			// that included the #[serde[skip]] updated parameter
			Err(e) => match State1skip::deserialize_revisioned(&mut val.as_slice()) {
				Ok(b_old) => Ok(b_old.into()),
				Err(_) => match State1::deserialize_revisioned(&mut val.as_slice()) {
					Ok(b_old) => Ok(b_old.into()),
					// Otherwise we return the initial error
					Err(_) => Err(Error::Revision(e)),
				},
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::postings::TermFrequency;
	use crate::idx::ft::terms::{State, Terms};
	use crate::idx::{IndexKeyBase, VersionedStore};
	use crate::kvs::TransactionType::{Read, Write};
	use crate::kvs::{Datastore, LockType::*, Transaction, TransactionType};
	use rand::{thread_rng, Rng};
	use std::collections::HashSet;
	use test_log::test;

	#[test]
	fn test_state_serde() {
		let s = State::new(3);
		let val = VersionedStore::try_into(&s).unwrap();
		let s: State = VersionedStore::try_from(val).unwrap();
		assert_eq!(s.btree.generation(), 0);
		assert_eq!(s.next_term_id, 0);
	}

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

	async fn new_operation(
		ds: &Datastore,
		order: u32,
		tt: TransactionType,
	) -> (Transaction, Terms) {
		let tx = ds.transaction(tt, Optimistic).await.unwrap();
		let t = Terms::new(&tx, IndexKeyBase::default(), order, tt, 100).await.unwrap();
		(tx, t)
	}

	async fn finish(tx: Transaction, mut t: Terms) {
		t.finish(&tx).await.unwrap();
		tx.commit().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_resolve_terms() {
		const BTREE_ORDER: u32 = 7;

		let ds = Datastore::new("memory").await.unwrap();

		{
			// Empty operation
			let (tx, t) = new_operation(&ds, BTREE_ORDER, Write).await;
			finish(tx, t).await;
		}

		// Resolve a first term
		{
			let (tx, mut t) = new_operation(&ds, BTREE_ORDER, Write).await;
			assert_eq!(t.resolve_term_id(&tx, "C").await.unwrap(), 0);
			finish(tx, t).await;
			let (tx, t) = new_operation(&ds, BTREE_ORDER, Read).await;
			assert_eq!(t.statistics(&tx).await.unwrap().keys_count, 1);
		}

		// Resolve a second term
		{
			let (tx, mut t) = new_operation(&ds, BTREE_ORDER, Write).await;
			assert_eq!(t.resolve_term_id(&tx, "D").await.unwrap(), 1);
			finish(tx, t).await;
			let (tx, t) = new_operation(&ds, BTREE_ORDER, Read).await;
			assert_eq!(t.statistics(&tx).await.unwrap().keys_count, 2);
		}

		// Resolve two existing terms with new frequencies
		{
			let (tx, mut t) = new_operation(&ds, BTREE_ORDER, Write).await;
			assert_eq!(t.resolve_term_id(&tx, "C").await.unwrap(), 0);
			assert_eq!(t.resolve_term_id(&tx, "D").await.unwrap(), 1);
			finish(tx, t).await;

			let (tx, t) = new_operation(&ds, BTREE_ORDER, Read).await;
			assert_eq!(t.statistics(&tx).await.unwrap().keys_count, 2);
		}

		// Resolve one existing terms and two new terms
		{
			let (tx, mut t) = new_operation(&ds, BTREE_ORDER, Write).await;
			assert_eq!(t.resolve_term_id(&tx, "A").await.unwrap(), 2);
			assert_eq!(t.resolve_term_id(&tx, "C").await.unwrap(), 0);
			assert_eq!(t.resolve_term_id(&tx, "E").await.unwrap(), 3);
			finish(tx, t).await;

			let (tx, t) = new_operation(&ds, BTREE_ORDER, Read).await;
			assert_eq!(t.statistics(&tx).await.unwrap().keys_count, 4);
		}
	}

	#[test(tokio::test)]
	async fn test_deletion() {
		const BTREE_ORDER: u32 = 7;

		let ds = Datastore::new("memory").await.unwrap();

		{
			let (tx, mut t) = new_operation(&ds, BTREE_ORDER, Write).await;

			// Check removing an non-existing term id returns None
			assert!(t.remove_term_id(&tx, 0).await.is_ok());

			// Create few terms
			t.resolve_term_id(&tx, "A").await.unwrap();
			t.resolve_term_id(&tx, "C").await.unwrap();
			t.resolve_term_id(&tx, "E").await.unwrap();
			finish(tx, t).await;
		}

		for term in ["A", "C", "E"] {
			let (tx, t) = new_operation(&ds, BTREE_ORDER, Read).await;
			let term_id = t.get_term_id(&tx, term).await.unwrap();

			if let Some(term_id) = term_id {
				let (tx, mut t) = new_operation(&ds, BTREE_ORDER, Write).await;
				t.remove_term_id(&tx, term_id).await.unwrap();
				finish(tx, t).await;

				let (tx, t) = new_operation(&ds, BTREE_ORDER, Read).await;
				assert_eq!(t.get_term_id(&tx, term).await.unwrap(), None);
			} else {
				panic!("Term ID not found: {}", term);
			}
		}

		// Check id recycling
		let (tx, mut t) = new_operation(&ds, BTREE_ORDER, Write).await;
		assert_eq!(t.resolve_term_id(&tx, "B").await.unwrap(), 0);
		assert_eq!(t.resolve_term_id(&tx, "D").await.unwrap(), 1);
		finish(tx, t).await;
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

	#[test(tokio::test)]
	async fn test_resolve_100_docs_with_50_words_one_by_one() {
		let ds = Datastore::new("memory").await.unwrap();
		for _ in 0..100 {
			let (tx, mut t) = new_operation(&ds, 100, Write).await;
			let terms_string = random_term_freq_vec(50);
			for (term, _) in terms_string {
				t.resolve_term_id(&tx, &term).await.unwrap();
			}
			finish(tx, t).await;
		}
	}

	#[test(tokio::test)]
	async fn test_resolve_100_docs_with_50_words_batch_of_10() {
		let ds = Datastore::new("memory").await.unwrap();
		for _ in 0..10 {
			let (tx, mut t) = new_operation(&ds, 100, Write).await;
			for _ in 0..10 {
				let terms_string = random_term_freq_vec(50);
				for (term, _) in terms_string {
					t.resolve_term_id(&tx, &term).await.unwrap();
				}
			}
			finish(tx, t).await;
		}
	}
}
