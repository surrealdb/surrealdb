use std::sync::Arc;

mod controller;
use anyhow::{Result, bail};
pub(crate) use controller::BucketController;
pub use controller::BucketOperation;

use crate::CommunityComposer;
use crate::buc::store::ObjectStore;
#[cfg(not(target_arch = "wasm32"))]
use crate::buc::store::file::FileStore;
use crate::buc::store::memory::MemoryStore;
use crate::err::Error;

pub(crate) mod manager;
pub mod store;

#[cfg(target_family = "wasm")]
pub trait BucketStoreProviderRequirements {}

#[cfg(not(target_family = "wasm"))]
pub trait BucketStoreProviderRequirements: Send + Sync + 'static {}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub trait BucketStoreProvider: BucketStoreProviderRequirements {
	async fn connect(
		&self,
		url: &str,
		global: bool,
		readonly: bool,
	) -> Result<Arc<dyn ObjectStore>>;
}

impl BucketStoreProviderRequirements for CommunityComposer {}
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl BucketStoreProvider for CommunityComposer {
	async fn connect(
		&self,
		url: &str,
		_global: bool,
		_readonly: bool,
	) -> Result<Arc<dyn ObjectStore>> {
		if MemoryStore::parse_url(url) {
			return Ok(Arc::new(MemoryStore::new()));
		}

		#[cfg(not(target_arch = "wasm32"))]
		if let Some(opts) = FileStore::parse_url(url).await? {
			return Ok(Arc::new(FileStore::new(opts)));
		}

		bail!(Error::UnsupportedBackend)
	}
}
