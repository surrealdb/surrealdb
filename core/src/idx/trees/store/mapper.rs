use crate::err::Error;
use crate::idx::ft::analyzer::mapper::Mapper;
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::Filter;
use ahash::HashSet;
use dashmap::DashMap;
use std::path::Path;

#[derive(Default)]
pub(crate) struct Mappers(DashMap<String, Mapper>);

impl Mappers {
	/// If any mapper is defined, it will be loaded in memory.
	pub(crate) async fn load(&self, az: &DefineAnalyzerStatement) -> Result<(), Error> {
		if let Some(filters) = &az.filters {
			for f in filters {
				if let Filter::Mapper(path) = f {
					self.insert(path).await?;
				}
			}
		}
		Ok(())
	}

	/// Ensure that if a mapper is defined, that it is also loaded in memory.
	/// This method does not reload a mapper if it is already in memory.
	pub(crate) async fn check(&self, az: &DefineAnalyzerStatement) -> Result<(), Error> {
		if let Some(filters) = &az.filters {
			for f in filters {
				if let Filter::Mapper(path) = f {
					if !self.0.contains_key(path) {
						self.insert(path).await?;
					}
				}
			}
		}
		Ok(())
	}

	async fn insert(&self, path: &str) -> Result<(), Error> {
		let p = Path::new(path);
		if !p.exists() || !p.is_file() {
			return Err(Error::Internal(format!("Invalid mapper path: {p:?}")));
		}
		let mapper = Mapper::new(p).await?;
		self.0.insert(path.to_string(), mapper);
		Ok(())
	}

	pub(in crate::idx) fn get(&self, path: &str) -> Result<Mapper, Error> {
		match self.0.get(path) {
			None => Err(Error::Internal(format!("Mapper not found for {path}"))),
			Some(e) => Ok(e.value().clone()),
		}
	}

	pub(crate) fn cleanup(&self, azs: &[DefineAnalyzerStatement]) {
		// Collect every existing mapper
		let mut keys: HashSet<String> = self.0.iter().map(|e| e.key().to_string()).collect();
		// Remove keys that still exist in the definitions
		for az in azs {
			if let Some(filters) = &az.filters {
				for f in filters {
					if let Filter::Mapper(path) = f {
						keys.remove(path);
					}
				}
			}
		}
		// Any left key can be removed
		for key in keys {
			self.0.remove(&key);
		}
	}
}
