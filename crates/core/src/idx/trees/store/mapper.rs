use std::path::Path;

use ahash::HashSet;
use anyhow::{Result, bail};
use dashmap::DashMap;

use crate::catalog;
use crate::err::Error;
use crate::expr::Filter;
use crate::iam::file::is_path_allowed;
use crate::idx::ft::analyzer::mapper::Mapper;

#[derive(Default)]
pub(crate) struct Mappers(DashMap<String, Mapper>);

impl Mappers {
	/// If any mapper is defined, it will be loaded in memory.
	pub(crate) async fn load(&self, az: &catalog::AnalyzerDefinition) -> Result<()> {
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
	pub(crate) async fn check(&self, az: &catalog::AnalyzerDefinition) -> Result<()> {
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

	async fn insert(&self, path: &str) -> Result<()> {
		let p = Path::new(path);
		// Check the path is allowed
		is_path_allowed(p)?;
		if !p.exists() || !p.is_file() {
			bail!(Error::Internal(format!("Invalid mapper path: {p:?}")));
		}
		let mapper = Mapper::new(p).await?;
		self.0.insert(path.to_string(), mapper);
		Ok(())
	}

	pub(in crate::idx) fn get(&self, path: &str) -> Result<Mapper> {
		match self.0.get(path) {
			None => {
				Err(anyhow::Error::new(Error::Internal(format!("Mapper not found for {path}"))))
			}
			Some(e) => Ok(e.value().clone()),
		}
	}

	pub(crate) fn cleanup(&self, azs: &[catalog::AnalyzerDefinition]) {
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
