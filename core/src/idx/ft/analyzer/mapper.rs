use crate::idx::ft::analyzer::filter::FilterResult;
use vart::art::Tree;
use vart::VariableSizeKey;

pub(super) struct Mapper {
	_terms: Tree<VariableSizeKey, String>,
}

impl Mapper {
	pub(super) fn get(_path: &str) -> Self {
		todo!()
	}

	pub(super) fn map(&self, _s: &str) -> FilterResult {
		todo!()
	}
}
