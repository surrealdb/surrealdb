mod docids;
mod doclength;
mod postings;
mod terms;

#[cfg(test)]
mod tests {
	use crate::idx::ft::docids::DocIds;
	use crate::idx::ft::doclength::{DocLength, DocLengths};
	use crate::idx::ft::postings::{Postings, TermFrequency};
	use crate::idx::ft::terms::Terms;
	use crate::idx::kvsim::KVSimulator;
	use crate::idx::IndexId;
	use crate::sql::error::IResult;
	use nom::bytes::complete::take_while;
	use nom::character::complete::multispace0;
	use std::collections::hash_map::Entry;
	use std::collections::HashMap;

	#[derive(Default)]
	struct FtIndex {}

	impl FtIndex {
		fn add_document(
			kv: &mut KVSimulator,
			index_id: IndexId,
			doc_key: &str,
			field_content: &str,
		) {
			// Resolve the doc_id
			let mut d = DocIds::new(kv, 0, 100);
			let doc_id = d.resolve_doc_id(kv, doc_key);

			// Extract the doc_lengths, terms en frequencies
			let mut t: Terms = Terms::new(kv, 0, 100);
			let (doc_length, terms_and_frequencies) =
				Self::extract_sorted_terms_with_frequencies(field_content);

			// Set the doc length
			let mut l = DocLengths::new(kv, 0, 100);
			l.set_doc_length(kv, doc_id, doc_length);

			// Set the terms postings
			let terms = t.resolve_terms(kv, terms_and_frequencies);
			let mut p = Postings::new(kv, index_id, 100);
			for (term_id, term_freq) in terms {
				p.update_posting(kv, term_id, doc_id, term_freq);
			}

			// Update the states
			d.finish(kv);
			l.finish(kv);
			p.finish(kv);
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

		fn search(_kv: &mut KVSimulator, _term: &str) -> Vec<String> {
			todo!()
		}
	}

	#[test]
	fn test_ft_index() {
		let mut kv = KVSimulator::default();
		FtIndex::add_document(&mut kv, 0, "Foo", "Hello world!");
		FtIndex::add_document(&mut kv, 0, "Bar", "Yellow Hello!");
		assert_eq!(FtIndex::search(&mut kv, "Hello"), vec!["Foo", "Bar"]);
		assert_eq!(FtIndex::search(&mut kv, "world"), vec!["Foo"]);
		assert_eq!(FtIndex::search(&mut kv, "Yellow"), vec!["Bar"]);
		assert_eq!(FtIndex::search(&mut kv, "Dummy"), Vec::<String>::new());
	}
}
