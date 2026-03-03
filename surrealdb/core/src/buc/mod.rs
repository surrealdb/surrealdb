//! Bucket storage module for SurrealDB.
//!
//! This module provides abstractions for object storage backends (buckets) that can be used
//! to store and retrieve binary data (files). It supports multiple storage backends including
//! in-memory storage and local filesystem storage.
//!
//! The module is organized into:
//! - `BucketController` - Controls bucket operations with permission checking
//! - `BucketsManager` - Manages bucket connections and caching
//! - [`store`] - Object store trait and implementations

use std::path::PathBuf;
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

/// Marker trait for bucket store provider requirements.
///
/// This trait defines platform-specific requirements for bucket store providers.
/// On non-WASM targets, providers must be `Send + Sync + 'static` to support
/// concurrent access across threads.
#[cfg(target_family = "wasm")]
pub trait BucketStoreProviderRequirements {}

/// Marker trait for bucket store provider requirements.
///
/// This trait defines platform-specific requirements for bucket store providers.
/// On non-WASM targets, providers must be `Send + Sync + 'static` to support
/// concurrent access across threads.
#[cfg(not(target_family = "wasm"))]
pub trait BucketStoreProviderRequirements: Send + Sync + 'static {}

/// Trait for creating connections to bucket storage backends.
///
/// Implementors of this trait can parse storage URLs and create appropriate
/// [`ObjectStore`] instances. The community edition supports `memory://` and
/// `file://` backends, while enterprise editions may support additional backends
/// like S3, GCS, or Azure Blob Storage.
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
pub trait BucketStoreProvider: BucketStoreProviderRequirements {
	/// Connect to a bucket storage backend.
	///
	/// # Arguments
	/// * `url` - The storage backend URL (e.g., `memory://`, `file:///path/to/dir`)
	/// * `global` - Whether this is a global bucket connection
	/// * `readonly` - Whether the bucket should be opened in read-only mode
	///
	/// # Returns
	/// An `Arc<dyn ObjectStore>` on success, or an error if the URL is invalid
	/// or the backend is not supported.
	async fn connect(
		&self,
		url: &str,
		global: bool,
		readonly: bool,
		bucket_folder_allowlist: &[PathBuf],
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
		bucket_folder_allowlist: &[PathBuf],
	) -> Result<Arc<dyn ObjectStore>> {
		if MemoryStore::parse_url(url) {
			return Ok(Arc::new(MemoryStore::new()));
		}

		#[cfg(not(target_arch = "wasm32"))]
		if let Some(opts) = FileStore::parse_url(url, bucket_folder_allowlist.to_vec()).await? {
			return Ok(Arc::new(FileStore::new(opts)));
		}

		bail!(Error::UnsupportedBackend)
	}
}
