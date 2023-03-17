use crate::idx::ft::fstmap::FstMap;
use crate::idx::kvsim::KVSimulator;
use crate::idx::partition::{PartitionMap, _MAX_PARTITION_SIZE};
use fst::Streamer;
use radix_trie::{Trie, TrieCommon};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(super) type TermId = u64;
pub(super) type TermFrequency = u64;

const TERMS_MAP_KEY: Vec<u8> = vec![];

#[derive(Default, Serialize, Deserialize)]
pub(super) struct Terms {
	map: PartitionMap,
	next_term_id: u64,
	#[serde(skip)]
	// In memory partitions
	partitions: HashMap<u32, TermsPartition>,
	#[serde(skip)]
	updated: bool,
}

impl Terms {
	pub(super) fn new(kv: &mut KVSimulator) -> Terms {
		let mut map = kv.get(&TERMS_MAP_KEY).map_or_else(|| Terms::default(), |m| m);
		map.map.remap();
		map
	}

	pub(super) fn finish(mut self, kv: &mut KVSimulator) {
		for (part_id, partition) in &mut self.partitions {
			if partition.updated {
				kv.set(part_id.to_be_bytes().to_vec(), partition);
			}
		}
		if self.updated {
			// If so, we can write the new version of the map
			kv.set(TERMS_MAP_KEY, &self);
		}
	}

	pub(super) fn resolve_terms(
		&mut self,
		kv: &mut KVSimulator,
		terms: Vec<(&str, TermFrequency)>,
	) -> Vec<(TermId, TermFrequency)> {
		// This vector stores the resolved terms
		let mut resolved_terms = Vec::with_capacity(terms.len());

		// We iterate over every term and dispatch them to the partitions
		for (term_str, term_freq) in terms {
			// Either finding an existing partition
			let part_id = if let Some(part_id) = self.map.find_partition_id(term_str) {
				part_id
			} else {
				// Or we create a new partition
				let new_partition_id = self.map.new_partition_id(term_str);
				self.partitions.insert(new_partition_id, TermsPartition::default());
				self.updated = true;
				new_partition_id
			};
			// Add the term to the chosen partition
			self.check_terms_partition(kv, part_id);
			let term_id = self.resolve_term(part_id, term_str);
			resolved_terms.push((term_id, term_freq));
		}

		// Rebuild every updated partitions
		self.partitions.values_mut().filter(|p| p.updated).for_each(|p| p.rebuild());

		// We check the status of the partition and see if some of them should be split.
		// TODO

		// And return the result
		resolved_terms
	}

	fn resolve_term(&mut self, part_id: u32, term_str: &str) -> u64 {
		let p = self
			.partitions
			.get_mut(&part_id)
			.unwrap_or_else(|| panic!("Index is corrupted. Terms Partition is missing."));
		if let Some(term_id) = p.get_term_id(term_str) {
			term_id
		} else {
			self.updated = true;
			let new_term_id = self.next_term_id;
			p.add_term_id(term_str, new_term_id);
			self.map.extends_partition_bounds(part_id, term_str);
			self.next_term_id += 1;
			new_term_id
		}
	}

	fn check_terms_partition(&mut self, kv: &mut KVSimulator, part_id: u32) {
		if !self.partitions.contains_key(&part_id) {
			if let Some(p) = kv.get::<TermsPartition>(&part_id.to_be_bytes()) {
				self.partitions.insert(part_id, p);
			}
		}
	}
}

#[derive(Default, Serialize, Deserialize)]
struct TermsPartition {
	terms: FstMap,
	#[serde(skip)]
	additions: Trie<Vec<u8>, u64>,
	#[serde(skip)]
	updated: bool,
}

impl TermsPartition {
	fn get_term_id(&self, term: &str) -> Option<TermId> {
		self.terms.get(term)
	}

	fn add_term_id(&mut self, term: &str, term_id: TermId) {
		self.additions.insert(term.as_bytes().to_vec(), term_id);
		self.updated = true;
	}

	fn _is_full(&self) -> bool {
		self.terms.size() >= _MAX_PARTITION_SIZE
	}

	/// Rebuild the FST by merging the previously existing terms with the additional terms
	fn rebuild(&mut self) {
		let mut existing_terms = self.terms.stream();
		let mut new_terms = self.additions.iter();
		let mut current_existing = existing_terms.next();
		let mut current_new = new_terms.next();

		let mut builder = FstMap::builder();
		// We use a double iterator because the map as to be filled with sorted terms
		loop {
			match current_new {
				None => break,
				Some((new_term_vec, new_term_id)) => match current_existing {
					None => break,
					Some((existing_term_vec, existing_term_id)) => {
						if new_term_vec.as_slice().ge(existing_term_vec) {
							builder.insert(existing_term_vec, existing_term_id).unwrap();
							current_existing = existing_terms.next();
						} else {
							builder.insert(new_term_vec, *new_term_id).unwrap();
							current_new = new_terms.next();
						}
					}
				},
			};
		}

		// Insert any existing term left over
		while let Some((term_vec, term_id)) = current_existing {
			builder.insert(term_vec, term_id).unwrap();
			current_existing = existing_terms.next();
		}
		// Insert any new term left over
		while let Some((new_term_vec, new_term_id)) = current_new {
			builder.insert(new_term_vec, *new_term_id).unwrap();
			current_new = new_terms.next();
		}

		self.terms = FstMap::try_from(builder).unwrap();
		self.additions = Trie::default();
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::terms::{TermFrequency, Terms, TermsPartition};
	use crate::idx::kvsim::KVSimulator;
	use radix_trie::TrieCommon;
	use rand::{thread_rng, Rng};
	use std::collections::HashSet;

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

	fn create_terms_partition(key_length: usize) -> TermsPartition {
		let set = unique_terms(key_length, 1000);
		let mut tp = TermsPartition::default();
		let mut i = 0;
		for key in &set {
			tp.add_term_id(key, i);
			i += 1;
		}
		assert_eq!(tp.additions.len(), 1000);
		assert_eq!(tp.terms.len(), 0);
		tp.rebuild();
		assert_eq!(tp.additions.len(), 0);
		assert_eq!(tp.terms.len(), 1000);
		println!("{}: {} {}", key_length, tp.terms.size(), tp.terms.size() / key_length);
		tp
	}

	#[test]
	fn compute_term_partition_size_ratio() {
		let tp1 = create_terms_partition(4);
		let tp2 = create_terms_partition(8);
		let tp3 = create_terms_partition(20);
		let tp4 = create_terms_partition(50);
		let tp5 = create_terms_partition(100);
		assert!(tp2.terms.size() > tp1.terms.size());
		assert!(tp3.terms.size() > tp2.terms.size());
		assert!(tp4.terms.size() > tp3.terms.size());
		assert!(tp5.terms.size() > tp4.terms.size());
	}

	#[test]
	fn test_resolve_terms() {
		let mut kv = KVSimulator::default();

		// Resolve a first term
		let mut terms = Terms::new(&mut kv);
		let res = terms.resolve_terms(&mut kv, vec![("C", 103)]);
		terms.finish(&mut kv);
		assert_eq!(res, vec![(0, 103)], "C");

		// Resolve a second term
		let mut terms = Terms::new(&mut kv);
		let res = terms.resolve_terms(&mut kv, vec![("D", 104)]);
		terms.finish(&mut kv);
		assert_eq!(res, vec![(1, 104)], "D");

		// Resolve two existing terms with new frequencies
		let mut terms = Terms::new(&mut kv);
		let res = terms.resolve_terms(&mut kv, vec![("C", 105), ("D", 106)]);
		terms.finish(&mut kv);
		assert_eq!(res, vec![(0, 105), (1, 106)], "C + D");

		// Resolve one existing terms and two new terms
		let mut terms = Terms::new(&mut kv);
		let res = terms.resolve_terms(&mut kv, vec![("A", 101), ("C", 107), ("E", 105)]);
		terms.finish(&mut kv);
		assert_eq!(res, vec![(2, 101), (0, 107), (3, 105)], "A + C + D");

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
			let mut terms = Terms::new(&mut kv);
			let terms_string = random_term_freq_vec(50);
			let terms_str: Vec<(&str, TermFrequency)> =
				terms_string.iter().map(|(t, f)| (t.as_str(), *f)).collect();
			terms.resolve_terms(&mut kv, terms_str);
			terms.finish(&mut kv);
		}
		kv.print_stats();
	}

	#[test]
	fn test_resolve_100_docs_with_50_words_batch_of_10() {
		let mut kv = KVSimulator::default();
		for _ in 0..10 {
			let mut terms = Terms::new(&mut kv);
			for _ in 0..10 {
				let terms_string = random_term_freq_vec(50);
				let terms_str: Vec<(&str, TermFrequency)> =
					terms_string.iter().map(|(t, f)| (t.as_str(), *f)).collect();
				terms.resolve_terms(&mut kv, terms_str);
			}
			terms.finish(&mut kv);
		}
		kv.print_stats();
	}
}
