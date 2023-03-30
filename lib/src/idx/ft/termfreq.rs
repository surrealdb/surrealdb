use crate::idx::bkeys::FstKeys;
use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub(super) type TermFrequency = u32;

#[derive(Default)]
pub(super) struct TermFrequencies {
	terms: HashMap<TermId, TermFrequenciesPartitions>,
}

struct TermFrequencyPartition {
	_doc_freq: FstKeys,
}

impl TermFrequencies {
	pub(super) fn update_posting(
		&mut self,
		term_id: TermId,
		doc_id: DocId,
		term_freq: TermFrequency,
	) {
		match self.terms.entry(term_id) {
			Entry::Occupied(e) => {
				e.get().update_term_frequency(doc_id, term_freq);
			}
			Entry::Vacant(e) => {
				e.insert(TermFrequenciesPartitions::new(doc_id, term_freq));
			}
		};
	}

	fn _update_term_frequency(&self, _doc_id: DocId, _term_freq: TermFrequency) {
		todo!()
	}
}

struct TermFrequenciesPartitions(Vec<TermFrequencyPartition>);

impl TermFrequenciesPartitions {
	fn new(doc_id: DocId, term_freq: TermFrequency) -> Self {
		TermFrequenciesPartitions(vec![TermFrequencyPartition::new(doc_id, term_freq)])
	}

	fn update_term_frequency(&self, _doc_id: DocId, _term_freq: TermFrequency) {
		todo!()
	}
}

impl TermFrequencyPartition {
	fn new(_doc_id: DocId, _term_freq: TermFrequency) -> Self {
		Self {
			_doc_freq: FstKeys::default(),
		}
	}
}
