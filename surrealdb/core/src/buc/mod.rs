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
use std::pin::Pin;
use std::sync::Arc;

mod controller;
use anyhow::{Result, bail};
pub(crate) use controller::BucketController;
pub use controller::BucketOperation;

use crate::buc::store::ObjectStore;
#[cfg(not(target_arch = "wasm32"))]
use crate::buc::store::file::FileStore;
use crate::buc::store::memory::MemoryStore;
use crate::err::Error;
use crate::iam::file::extract_allowed_paths;
use crate::{CommunityComposer, cnf};

pub mod manager;
pub mod store;

#[derive(Clone, Debug, Default)]
pub struct Config {
	bucket_list: Vec<PathBuf>,
	only_global: bool,
	global_bucket: Option<String>,
}

impl cnf::Config for Config {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key_with("bucket_folder_allowlist", &mut self.bucket_list, |x| {
			Some(extract_allowed_paths(x, false, "bucket folder"))
		})
		.parse_key_with("global_bucket", &mut self.global_bucket, |x| Some(Some(x.to_owned())))
		.parse_key("global_bucket_enforced", &mut self.only_global);
	}
}

#[cfg(test)]
impl Config {
	/// Test-only helper for building a `Config` with a custom bucket allowlist
	/// without going through the `cnf::Config` parser.
	pub(crate) fn for_test(bucket_list: Vec<PathBuf>) -> Self {
		Self {
			bucket_list,
			..Self::default()
		}
	}
}

/// Marker trait for bucket store provider requirements.
pub trait BucketStoreProviderRequirements: Send + Sync + 'static {}

type BoxFuture<'a, R> = Pin<Box<dyn Future<Output = R> + 'a + Send + Sync>>;

/// Trait for creating connections to bucket storage backends.
///
/// Implementors of this trait can parse storage URLs and create appropriate
/// [`ObjectStore`] instances. The community edition supports `memory://` and
/// `file://` backends, while enterprise editions may support additional backends
/// like S3, GCS, or Azure Blob Storage.
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
	fn connect<'a>(
		&self,
		url: &'a str,
		global: bool,
		readonly: bool,
		config: Config,
	) -> BoxFuture<'a, Result<Arc<dyn ObjectStore>>>;
}

impl BucketStoreProviderRequirements for CommunityComposer {}

impl BucketStoreProvider for CommunityComposer {
	fn connect<'a>(
		&self,
		url: &'a str,
		_global: bool,
		_readonly: bool,
		config: Config,
	) -> BoxFuture<'a, Result<Arc<dyn ObjectStore>>> {
		Box::pin(async {
			#[cfg(target_arch = "wasm32")]
			let _ = config;
			if MemoryStore::parse_url(url) {
				return Ok(Arc::new(MemoryStore::new()) as Arc<dyn ObjectStore>);
			}

			#[cfg(not(target_arch = "wasm32"))]
			if let Some(opts) = FileStore::parse_url(url, &config).await? {
				return Ok(Arc::new(FileStore::new(opts, config)) as Arc<dyn ObjectStore>);
			}

			bail!(Error::UnsupportedBackend)
		})
	}
}
