mod docids;
mod doclength;
mod termfreq;
mod terms;

#[cfg(test)]
mod tests {
	use crate::idx::ft::docids::DocIds;
	use crate::idx::ft::doclength::{DocLength, DocLengths};
	use crate::idx::ft::termfreq::{TermFrequencies, TermFrequency};
	use crate::idx::ft::terms::Terms;
	use crate::idx::kvsim::KVSimulator;
	use crate::sql::error::IResult;
	use nom::bytes::complete::take_while;
	use nom::character::complete::multispace0;
	use std::collections::hash_map::Entry;
	use std::collections::HashMap;

	#[derive(Default)]
	struct FtIndex {
		tf: TermFrequencies,
		dl: DocLengths,
	}

	impl FtIndex {
		fn add_document(&mut self, kv: &mut KVSimulator, doc_key: &str, field_content: &str) {
			// Resolve the doc_id
			let mut d = DocIds::new(kv, "D".into(), 100);
			let doc_id = d.resolve_doc_id(kv, doc_key);

			// Extract the doc_lengths, terms en frequencies
			let mut t: Terms = Terms::new(kv, "T".into(), 100);
			let (doc_length, terms_and_frequencies) =
				Self::extract_sorted_terms_with_frequencies(field_content);

			self.dl.set_doc_length(doc_id, doc_length);

			// Update the terms
			let terms = t.resolve_terms(kv, terms_and_frequencies);
			for (term_id, term_freq) in terms {
				self.tf.update_posting(term_id, doc_id, term_freq);
			}

			// Update the states
			d.finish(kv);
			t.finish(kv);
		}

		fn extract_sorted_terms_with_frequencies(
			input: &str,
		) -> (DocLength, HashMap<&str, TermFrequency>) {
			let mut doc_length = 0;
			let mut terms = HashMap::new();
			let mut rest = input;
			loop {
				// Skip whitespace
				let (remaining_input, _) =
					multispace0::<_, ()>(rest).unwrap_or_else(|e| panic!("multispace0 {:?}", e));
				if remaining_input.is_empty() {
					break;
				}
				rest = remaining_input;

				// Tokenize
				let (remaining_input, token) = Self::tokenize(rest).unwrap();
				if !input.is_empty() {
					doc_length += 1;
					match terms.entry(token) {
						Entry::Vacant(e) => {
							e.insert(1);
						}
						Entry::Occupied(mut e) => {
							e.insert(*e.get() + 1);
						}
					}
				}
				rest = remaining_input;
			}
			(doc_length, terms)
		}

		fn tokenize(i: &str) -> IResult<&str, &str> {
			take_while(|c| c != ' ' && c != '\n' && c != '\t')(i)
		}
	}

	#[test]
	fn test_ft_index() {
		let mut fti = FtIndex::default();
		let mut kv = KVSimulator::default();
		fti.add_document(&mut kv, "Foo", "Hello world!");
	}
}
