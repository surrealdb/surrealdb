use crate::idx::ft::analyzer::mapper::Mapper;
use crate::kvs::Key;
use crate::sql::statements::DefineAnalyzerStatement;
use dashmap::DashMap;

#[derive(Default)]
pub(crate) struct Mappers(DashMap<Key, Mapper>);

impl Mappers {
	pub(in crate::idx) fn get(&self, _path: &str) -> Mapper {
		todo!()
	}

	pub(crate) fn cleanup(&self, _azs: &[DefineAnalyzerStatement]) {
		todo!()
	}
}
