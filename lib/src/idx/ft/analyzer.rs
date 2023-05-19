use crate::err::Error;
use crate::err::Error::AnalyzerError;
use crate::idx::ft::doclength::DocLength;
use crate::idx::ft::postings::TermFrequency;
use crate::idx::ft::terms::{TermId, Terms};
use crate::kvs::Transaction;
use crate::sql::Array;
use nom::bytes::complete::take_while;
use nom::character::complete::multispace0;
use nom::IResult;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub(super) struct Analyzer {}

impl Analyzer {
	// TODO: This is currently a place holder. It has to be replaced by the full analyzer/token/filter logic.
	pub(super) async fn extract_terms_with_frequencies(
		terms: &mut Terms,
		tx: &mut Transaction,
		field_content: &Array,
	) -> Result<(DocLength, HashMap<TermId, TermFrequency>), Error> {
		let mut doc_length = 0;
		let mut terms_map = HashMap::new();
		for v in &field_content.0 {
			let input = v.clone().convert_to_string()?;
			let mut rest = input.as_str();
			while !rest.is_empty() {
				// Extract the next token
				match Self::next_token(rest) {
					Ok((remaining_input, token)) => {
						if !token.is_empty() {
							let term_id = terms.resolve_term_id(tx, token).await?;
							doc_length += 1;
							match terms_map.entry(term_id) {
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
					Err(e) => return Err(AnalyzerError(e.to_string())),
				}
			}
		}
		Ok((doc_length, terms_map))
	}

	/// Extracting the next token. The string is left trimmed first.
	fn next_token(i: &str) -> IResult<&str, &str> {
		let (i, _) = multispace0(i)?;
		take_while(|c| c != ' ' && c != '\n' && c != '\t')(i)
	}
}
