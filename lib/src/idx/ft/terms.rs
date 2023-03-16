use crate::idx::ft::fstmap::FstMap;
use crate::idx::kvsim::KVSimulator;
use crate::idx::{MAX_PARTITION_SIZE, _HALF_PARTITION_SIZE};
use fst::Streamer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub(super) const TERMS_CHUNK_SIZE: usize = 10;
pub(super) type TermFrequency = u64;

#[derive(Eq, Hash, PartialEq, Serialize, Deserialize, Clone)]
pub(super) struct TermId(pub u64);

impl From<u64> for TermId {
	fn from(id: u64) -> Self {
		Self(id)
	}
}

impl TermId {
	fn inc(&mut self) -> u64 {
		self.0 += 1;
		self.0
	}
}

#[derive(Default)]
pub(super) struct Terms {
	kv: KVSimulator,
}

impl Terms {
	pub(super) fn resolve_terms(
		&mut self,
		mut terms: Vec<(&str, TermFrequency)>,
	) -> Vec<(TermId, TermFrequency)> {
		let mut resolved_terms = Vec::with_capacity(terms.len());
		let mut non_full_partition_cache = HashMap::new();
		// Resolve existing terms
		for partition_id in 0..self.kv.len() {
			let partition_id = partition_id.to_be_bytes().to_vec();
			if let Some(partition) = self.kv.get::<TermsPartition>(&partition_id) {
				partition.resolve_existing_terms(&mut terms, &mut resolved_terms);
				if !partition.is_full() {
					non_full_partition_cache.insert(partition_id, partition);
				}
			}
			if terms.is_empty() {
				// We have resolved all the terms.
				return resolved_terms;
			}
		}
		// We have new terms
		for terms_chunk in terms.chunks(TERMS_CHUNK_SIZE) {
			let mut partition_to_remove = None;
			let mut partition_to_insert = None;
			if let Some((partition_id, partition)) = non_full_partition_cache.iter_mut().next() {
				// Either we add the terms in the first existing non full partition
				partition.add_new_terms(terms_chunk, &mut resolved_terms);
				self.kv.set(partition_id.clone(), &partition);
				if partition.is_full() {
					partition_to_remove = Some(partition_id.clone());
				}
			} else {
				// Or we create a new partition
				let new_partition = TermsPartition::new(terms_chunk, &mut resolved_terms);
				let new_partition_id = self.kv.len().to_be_bytes().to_vec();
				self.kv.set(new_partition_id.clone(), &new_partition);
				if !new_partition.is_full() {
					partition_to_insert = Some((new_partition_id, new_partition));
				}
			}
			if let Some(partition_id) = partition_to_remove {
				non_full_partition_cache.remove(&partition_id);
			};
			if let Some((partition_id, partition)) = partition_to_insert {
				non_full_partition_cache.insert(partition_id, partition);
			}
		}
		resolved_terms
	}
}

#[derive(Serialize, Deserialize)]
struct TermsPartition {
	terms: FstMap,
	highest_term_id: TermId,
}

impl TermsPartition {
	/// Build a new TermsPartition containing the given terms.
	/// It also returns the list of terms translated with the term_id.
	pub(super) fn new(
		terms: &[(&str, TermFrequency)],
		resolved_terms: &mut Vec<(TermId, TermFrequency)>,
	) -> Self {
		let mut builder = FstMap::builder();
		let mut highest_term_id = 0;
		for (term, frequency) in terms {
			highest_term_id += 1;
			builder.insert(term, highest_term_id).unwrap();
			resolved_terms.push((highest_term_id.into(), *frequency));
		}
		let terms = FstMap::try_from(builder).unwrap();
		Self {
			terms,
			highest_term_id: highest_term_id.into(),
		}
	}

	pub(super) fn is_full(&self) -> bool {
		self.terms.size() >= MAX_PARTITION_SIZE
	}

	pub(super) fn _can_be_merged(&self) -> bool {
		self.terms.size() <= _HALF_PARTITION_SIZE
	}

	pub(super) fn _get_term_id(&self, term: &str) -> Option<u64> {
		self.terms.get(term)
	}

	fn resolve_existing_terms(
		&self,
		terms: &mut Vec<(&str, TermFrequency)>,
		resolved_terms: &mut Vec<(TermId, TermFrequency)>,
	) {
		terms.retain(|(term, freq)| {
			return if let Some(term_id) = self.terms.get(term) {
				resolved_terms.push((TermId::from(term_id), *freq));
				false
			} else {
				true
			};
		});
	}

	pub(super) fn add_new_terms(
		&mut self,
		new_terms: &[(&str, TermFrequency)],
		resolved_terms: &mut Vec<(TermId, TermFrequency)>,
	) {
		// Rebuild the FST by merging the previously existing terms with the new terms
		let mut existing_terms = self.terms.stream();
		let mut new_terms = new_terms.into_iter();
		let mut current_existing = existing_terms.next();
		let mut current_new = new_terms.next();

		let mut builder = FstMap::builder();
		// We use a double iterator because the map as to be filled with sorted terms
		loop {
			match current_new {
				None => break,
				Some((new_term_str, new_term_freq)) => match current_existing {
					None => break,
					Some((existing_term_vec, existing_term_id)) => {
						let new_term_vec = new_term_str.as_bytes();
						if new_term_vec.ge(existing_term_vec) {
							builder.insert(existing_term_vec, existing_term_id).unwrap();
							current_existing = existing_terms.next();
						} else {
							builder.insert(new_term_vec, self.highest_term_id.inc()).unwrap();
							resolved_terms
								.push((self.highest_term_id.clone(), new_term_freq.clone()));
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
		while let Some((new_term_str, new_term_freq)) = current_new {
			let new_term_vec = new_term_str.as_bytes();
			builder.insert(new_term_vec, self.highest_term_id.inc()).unwrap();
			resolved_terms.push((self.highest_term_id.clone(), new_term_freq.clone()));
			current_new = new_terms.next();
		}

		self.terms = FstMap::try_from(builder).unwrap();
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::terms::{TermFrequency, TermsPartition};
	use rand::{thread_rng, Rng};
	use std::collections::BTreeSet;

	fn create_terms_partition(key_length: usize) -> TermsPartition {
		let mut set = BTreeSet::new();
		while set.len() < 1000 {
			let rng = thread_rng();
			let random_key: String = rng
				.sample_iter(&rand::distributions::Alphanumeric)
				.take(key_length)
				.map(char::from)
				.collect();
			set.insert(random_key);
		}
		let vec_ref: Vec<(&str, TermFrequency)> = set.iter().map(|s| (s.as_str(), 0)).collect();
		let tp = TermsPartition::new(&vec_ref, &mut vec![]);
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
}
