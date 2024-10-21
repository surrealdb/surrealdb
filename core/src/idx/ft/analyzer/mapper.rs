use crate::idx::ft::analyzer::filter::FilterResult;
use std::sync::Arc;
use vart::art::Tree;
use vart::VariableSizeKey;

#[derive(Clone)]
pub(in crate::idx) struct Mapper {
	_terms: Arc<Tree<VariableSizeKey, String>>,
}

impl Mapper {
	pub(super) fn map(&self, _s: &str) -> FilterResult {
		todo!()
	}
}
