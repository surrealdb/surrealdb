use crate::err::Error;
use crate::idx::ft::analyzer::mapper::Mapper;
use crate::sql::statements::DefineAnalyzerStatement;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::path::Path;

#[derive(Default)]
pub(crate) struct Mappers(DashMap<String, Mapper>);

impl Mappers {
	pub(in crate::idx) fn get(&self, path: &str) -> Result<Mapper, Error> {
		if let Some(r) = self.0.get(path) {
			Ok(r.value().clone())
		} else {
			match self.0.entry(path.to_string()) {
				Entry::Occupied(e) => Ok(e.get().clone()),
				Entry::Vacant(e) => {
					let mapper = Mapper::new(Path::new(path))?;
					e.insert(mapper.clone());
					Ok(mapper)
				}
			}
		}
	}

	pub(crate) fn cleanup(&self, _azs: &[DefineAnalyzerStatement]) {
		todo!()
	}
}
