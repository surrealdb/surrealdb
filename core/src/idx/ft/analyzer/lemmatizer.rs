use crate::idx::ft::analyzer::filter::FilterResult;
use vart::art::Tree;
use vart::VariableSizeKey;

pub(super) struct Lemmatizer {
	_terms: Tree<VariableSizeKey, String>,
}

impl Lemmatizer {
	pub(super) fn get(_path: &str) -> Self {
		todo!()
	}

	pub(super) fn lemme(&self, _s: &str) -> FilterResult {
		todo!()
	}
}
