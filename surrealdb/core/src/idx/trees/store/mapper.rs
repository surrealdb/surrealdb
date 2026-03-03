use std::path::{Path, PathBuf};

use ahash::HashSet;
use anyhow::{Result, bail};
use dashmap::DashMap;

use crate::catalog;
use crate::err::Error;
use crate::expr::Filter;
use crate::iam::file::is_path_allowed;
use crate::idx::ft::analyzer::mapper::Mapper;

pub(crate) struct Mappers {
	map: DashMap<String, Mapper>,
	file_allowlist: Vec<PathBuf>,
}

impl Mappers {
	pub(crate) fn new(file_allowlist: Vec<PathBuf>) -> Self {
		Self {
			map: DashMap::default(),
			file_allowlist,
		}
	}

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
				if let Filter::Mapper(path) = f
					&& !self.map.contains_key(path)
				{
					self.insert(path).await?;
				}
			}
		}
		Ok(())
	}

	async fn insert(&self, path: &str) -> Result<()> {
		let p = Path::new(path);
		is_path_allowed(p, &self.file_allowlist)?;
		if !p.exists() || !p.is_file() {
			bail!(Error::Internal(format!("Invalid mapper path: {p:?}")));
		}
		let mapper = Mapper::new(p, &self.file_allowlist).await?;
		self.map.insert(path.to_string(), mapper);
		Ok(())
	}

	pub(in crate::idx) fn get(&self, path: &str) -> Result<Mapper> {
		match self.map.get(path) {
			None => {
				Err(anyhow::Error::new(Error::Internal(format!("Mapper not found for {path}"))))
			}
			Some(e) => Ok(e.value().clone()),
		}
	}

	pub(crate) fn cleanup(&self, azs: &[catalog::AnalyzerDefinition]) {
		let mut keys: HashSet<String> = self.map.iter().map(|e| e.key().clone()).collect();
		for az in azs {
			if let Some(filters) = &az.filters {
				for f in filters {
					if let Filter::Mapper(path) = f {
						keys.remove(path);
					}
				}
			}
		}
		for key in keys {
			self.map.remove(&key);
		}
	}
}
