mod doclength;
mod fstmap;
mod termfreq;
mod terms;

#[cfg(test)]
mod tests {
	use crate::idx::docids::DocId;
	use crate::idx::ft::doclength::{DocLength, DocLengths};
	use crate::idx::ft::termfreq::TermFrequencies;
	use crate::idx::ft::terms::{TermFrequency, Terms};
	use crate::sql::error::IResult;
	use nom::bytes::complete::take_while;
	use nom::character::complete::multispace0;
	use std::collections::btree_map::Entry as BEntry;
	use std::collections::BTreeMap;

	#[derive(Default)]
	struct FtIndex {
		tf: TermFrequencies,
		dl: DocLengths,
		terms: Terms,
	}

	impl FtIndex {
		fn add_document(&mut self, doc_id: &DocId, field_content: &str) {
			let (doc_length, terms) = Self::extract_sorted_terms_with_frequencies(field_content);

			self.dl.set_doc_length(doc_id, doc_length);
			let terms = self.terms.resolve_terms(terms);
			for (term_id, term_freq) in terms {
				self.tf.update_posting(term_id, doc_id, term_freq);
			}
		}

		fn extract_sorted_terms_with_frequencies(
			input: &str,
		) -> (DocLength, Vec<(&str, TermFrequency)>) {
			let mut doc_length = 0;
			let mut terms = BTreeMap::new();
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
						BEntry::Vacant(e) => {
							e.insert(1);
						}
						BEntry::Occupied(mut e) => {
							e.insert(*e.get() + 1);
						}
					}
				}
				rest = remaining_input;
			}
			let res = terms.into_iter().collect();
			(doc_length, res)
		}

		fn tokenize(i: &str) -> IResult<&str, &str> {
			take_while(|c| c != ' ' && c != '\n' && c != '\t')(i)
		}
	}

	#[test]
	fn test_ft_index() {
		let mut fti = FtIndex::default();
		fti.add_document(&DocId::from(0), "Hello world!");
	}
}
