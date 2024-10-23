use crate::err::Error;
use crate::idx::ft::analyzer::filter::FilterResult;
use std::path::Path;
use std::sync::Arc;
use vart::art::Tree;
use vart::VariableSizeKey;

#[derive(Clone, Default)]
pub(in crate::idx) struct Mapper {
	_terms: Arc<Tree<VariableSizeKey, String>>,
}

impl Mapper {
	pub(in crate::idx) fn new(_path: &Path) -> Result<Self, Error> {
		todo!()
	}

	pub(super) fn map(&self, _s: &str) -> FilterResult {
		todo!()
	}
}
