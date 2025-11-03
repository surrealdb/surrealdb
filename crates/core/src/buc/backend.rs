use std::sync::Arc;

use super::store::ObjectStore;
#[cfg(not(target_arch = "wasm32"))]
use super::store::file::FileStore;
use super::store::memory::MemoryStore;
use crate::err::Error;

pub(crate) async fn connect(
	url: &str,
	_global: bool,
	_readonly: bool,
) -> Result<Arc<dyn ObjectStore>, Error> {
	if MemoryStore::parse_url(url) {
		return Ok(Arc::new(MemoryStore::new()));
	}

	#[cfg(not(target_arch = "wasm32"))]
	if let Some(opts) = FileStore::parse_url(url).await? {
		return Ok(Arc::new(FileStore::new(opts)));
	}

	Err(Error::UnsupportedBackend)
}
