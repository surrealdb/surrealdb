use crate::err::Error;
use crate::idx::ft::analyzer::mapper::Mapper;
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::Filter;
use dashmap::DashMap;
use std::path::Path;

#[derive(Default)]
pub(crate) struct Mappers(DashMap<String, Mapper>);

impl Mappers {
	/// Ensure that if any mapper is defined, that it is loaded in memory
	pub(crate) async fn preload(&self, az: &DefineAnalyzerStatement) -> Result<(), Error> {
		if let Some(filters) = &az.filters {
			for f in filters {
				if let Filter::Mapper(path) = f {
					let p = Path::new(path);
					if !p.exists() || !p.is_file() {
						return Err(Error::Internal(format!("Invalid mapper path: {p:?}")));
					}
					let mapper = Mapper::new(p).await?;
					self.0.insert(path.to_string(), mapper);
				}
			}
		}
		Ok(())
	}

	pub(in crate::idx) fn get(&self, path: &str) -> Result<Mapper, Error> {
		match self.0.get(path) {
			None => Err(Error::Internal(format!("Mapper not found for {path}"))),
			Some(e) => Ok(e.value().clone()),
		}
	}

	pub(crate) fn cleanup(&self, _azs: &[DefineAnalyzerStatement]) {
		todo!()
	}
}
