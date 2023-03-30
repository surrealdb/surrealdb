use crate::idx::bkeys::FstKeys;
use crate::idx::btree::BTree;
use crate::idx::ft::termfreq::TermFrequency;
use crate::idx::kvsim::KVSimulator;
use crate::kvs::Key;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(super) type TermId = u64;

const TERMS_BTREE_ORDER: usize = 100;

#[derive(Serialize, Deserialize)]
pub(super) struct Terms {
	btree: BTree,
	next_term_id: TermId,
	#[serde(skip)]
	updated: bool,
}

impl Default for Terms {
	fn default() -> Self {
		Self {
			btree: BTree::new(TERMS_BTREE_ORDER),
			next_term_id: 0,
			updated: false,
		}
	}
}

impl Terms {
	fn resolve_term(&mut self, kv: &mut KVSimulator, term: &str) -> TermId {
		let term = term.into();
		if let Some(term_id) = self.btree.search::<FstKeys>(kv, &term) {
			term_id
		} else {
			let term_id = self.next_term_id;
			self.btree.insert::<FstKeys>(kv, term, term_id);
			self.next_term_id += 1;
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

	pub(super) fn finish(self, kv: &mut KVSimulator, key: Key) {
		if self.updated {
			kv.set(key, &self);
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::termfreq::TermFrequency;
	use crate::idx::ft::terms::Terms;
	use crate::idx::kvsim::KVSimulator;
	use crate::kvs::Key;
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
		let mut kv = KVSimulator::default();
		let terms_key: Key = "T".into();
		let terms = Terms::default();
		kv.set(terms_key.clone(), &terms);

		// Resolve a first term
		let mut terms: Terms = kv.get(&terms_key).unwrap();
		let res = terms.resolve_terms(&mut kv, HashMap::from([("C", 103)]));
		terms.finish(&mut kv, terms_key.clone());
		assert_eq!(res, HashMap::from([(0, 103)]));

		// Resolve a second term
		let mut terms: Terms = kv.get(&terms_key).unwrap();
		let res = terms.resolve_terms(&mut kv, HashMap::from([("D", 104)]));
		terms.finish(&mut kv, terms_key.clone());
		assert_eq!(res, HashMap::from([(1, 104)]));

		// Resolve two existing terms with new frequencies
		let mut terms: Terms = kv.get(&terms_key).unwrap();
		let res = terms.resolve_terms(&mut kv, HashMap::from([("C", 113), ("D", 114)]));
		terms.finish(&mut kv, terms_key.clone());
		assert_eq!(res, HashMap::from([(0, 113), (1, 114)]));

		// Resolve one existing terms and two new terms
		let mut terms: Terms = kv.get(&terms_key).unwrap();
		let res = terms.resolve_terms(&mut kv, HashMap::from([("A", 101), ("C", 123), ("E", 105)]));
		terms.finish(&mut kv, terms_key.clone());
		assert_eq!(res, HashMap::from([(3, 101), (0, 123), (2, 105)]));

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
		let terms_key: Key = "T".into();
		let mut kv = KVSimulator::default();
		for _ in 0..100 {
			let mut terms: Terms = kv.get(&terms_key).unwrap_or_else(|| Terms::default());
			let terms_string = random_term_freq_vec(50);
			let terms_str: HashMap<&str, TermFrequency> =
				terms_string.iter().map(|(t, f)| (t.as_str(), *f)).collect();
			terms.resolve_terms(&mut kv, terms_str);
			terms.finish(&mut kv, terms_key.clone());
		}
		kv.print_stats();
	}

	#[test]
	fn test_resolve_100_docs_with_50_words_batch_of_10() {
		let terms_key: Key = "T".into();
		let mut kv = KVSimulator::default();
		for _ in 0..10 {
			let mut terms: Terms = kv.get(&terms_key).unwrap_or_else(|| Terms::default());
			for _ in 0..10 {
				let terms_string = random_term_freq_vec(50);
				let terms_str: HashMap<&str, TermFrequency> =
					terms_string.iter().map(|(t, f)| (t.as_str(), *f)).collect();
				terms.resolve_terms(&mut kv, terms_str);
			}
			terms.finish(&mut kv, terms_key.clone());
		}
		kv.print_stats();
	}
}
