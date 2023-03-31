use crate::idx::bkeys::FstKeys;
use crate::idx::btree::BTree;
use crate::idx::ft::termfreq::TermFrequency;
use crate::idx::kvsim::KVSimulator;
use crate::kvs::Key;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(super) type TermId = u64;

pub(super) struct Terms {
	state_key: Key,
	state: TermsState,
	updated: bool,
}

#[derive(Serialize, Deserialize)]
struct TermsState {
	btree: BTree,
	next_term_id: TermId,
}

impl TermsState {
	fn new(btree_order: usize) -> Self {
		Self {
			btree: BTree::new(btree_order),
			next_term_id: 0,
		}
	}
}

impl Terms {
	pub(super) fn new(kv: &mut KVSimulator, state_key: Key, btree_order: usize) -> Self {
		Self {
			state: kv.get(&state_key).unwrap_or_else(|| TermsState::new(btree_order)),
			updated: false,
			state_key,
		}
	}

	fn resolve_term(&mut self, kv: &mut KVSimulator, term: &str) -> TermId {
		let term = term.into();
		if let Some(term_id) = self.state.btree.search::<FstKeys>(kv, &term) {
			term_id
		} else {
			let term_id = self.state.next_term_id;
			self.state.btree.insert::<FstKeys>(kv, term, term_id);
			self.state.next_term_id += 1;
			self.updated = true;
			term_id
		}
	}

	pub(super) fn resolve_terms(
		&mut self,
		kv: &mut KVSimulator,
		terms_frequencies: HashMap<&str, TermFrequency>,
	) -> HashMap<TermId, TermFrequency> {
		let mut res = HashMap::with_capacity(terms_frequencies.len());
		for (term, freq) in terms_frequencies {
			res.insert(self.resolve_term(kv, term), freq);
		}
		res
	}

	pub(super) fn count(&self, kv: &mut KVSimulator) -> usize {
		self.state.btree.count::<FstKeys>(kv)
	}

	pub(super) fn finish(self, kv: &mut KVSimulator) {
		if self.updated {
			kv.set(self.state_key, &self.state);
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::termfreq::TermFrequency;
	use crate::idx::ft::terms::Terms;
	use crate::idx::kvsim::KVSimulator;
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

	#[test]
	fn test_resolve_terms() {
		const BTREE_ORDER: usize = 75;

		let mut kv = KVSimulator::default();
		Terms::new(&mut kv, "T".into(), BTREE_ORDER).finish(&mut kv);

		// Resolve a first term
		let mut t = Terms::new(&mut kv, "T".into(), BTREE_ORDER);
		let res = t.resolve_terms(&mut kv, HashMap::from([("C", 103)]));
		assert_eq!(t.count(&mut kv), 1);
		t.finish(&mut kv);
		assert_eq!(res, HashMap::from([(0, 103)]));

		// Resolve a second term
		let mut t = Terms::new(&mut kv, "T".into(), BTREE_ORDER);
		let res = t.resolve_terms(&mut kv, HashMap::from([("D", 104)]));
		assert_eq!(t.count(&mut kv), 2);
		t.finish(&mut kv);
		assert_eq!(res, HashMap::from([(1, 104)]));

		// Resolve two existing terms with new frequencies
		let mut t = Terms::new(&mut kv, "T".into(), BTREE_ORDER);
		let res = t.resolve_terms(&mut kv, HashMap::from([("C", 113), ("D", 114)]));
		assert_eq!(t.count(&mut kv), 2);
		t.finish(&mut kv);
		assert_eq!(res, HashMap::from([(0, 113), (1, 114)]));

		// Resolve one existing terms and two new terms
		let mut t = Terms::new(&mut kv, "T".into(), BTREE_ORDER);
		let res = t.resolve_terms(&mut kv, HashMap::from([("A", 101), ("C", 123), ("E", 105)]));
		assert_eq!(t.count(&mut kv), 4);
		t.finish(&mut kv);
		assert!(
			res.eq(&HashMap::from([(3, 101), (0, 123), (2, 105)]))
				|| res.eq(&HashMap::from([(2, 101), (0, 123), (3, 105)]))
		);

		kv.print_stats();
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

	#[test]
	fn test_resolve_100_docs_with_50_words_one_by_one() {
		let mut kv = KVSimulator::default();
		for _ in 0..100 {
			let mut t = Terms::new(&mut kv, "T".into(), 100);
			let terms_string = random_term_freq_vec(50);
			let terms_str: HashMap<&str, TermFrequency> =
				terms_string.iter().map(|(t, f)| (t.as_str(), *f)).collect();
			t.resolve_terms(&mut kv, terms_str);
			t.finish(&mut kv);
		}
		kv.print_stats();
	}

	#[test]
	fn test_resolve_100_docs_with_50_words_batch_of_10() {
		let mut kv = KVSimulator::default();
		for _ in 0..10 {
			let mut t = Terms::new(&mut kv, "T".into(), 100);
			for _ in 0..10 {
				let terms_string = random_term_freq_vec(50);
				let terms_str: HashMap<&str, TermFrequency> =
					terms_string.iter().map(|(t, f)| (t.as_str(), *f)).collect();
				t.resolve_terms(&mut kv, terms_str);
			}
			t.finish(&mut kv);
		}
		kv.print_stats();
	}
}
