use super::store::{file::FileStore, memory::MemoryStore, ObjectStore};
use crate::err::Error;
use std::sync::Arc;

pub fn connect(url: &str, _global: bool, _readonly: bool) -> Result<Arc<dyn ObjectStore>, Error> {
	if MemoryStore::parse_url(url) {
		return Ok(Arc::new(MemoryStore::new()));
	}

	if let Some(opts) = FileStore::parse_url(url)? {
		return Ok(Arc::new(FileStore::new(opts)));
	}

	Err(Error::UnsupportedBackend)
}
