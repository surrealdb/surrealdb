#![cfg_attr(not(feature = "files"), allow(unused_imports, dead_code, unreachable_code))]

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
//!
//! An object store is not transactional: a write reaches it when it is made and
//! is not undone when the transaction that made it is cancelled or fails to
//! commit.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use common::future::BoxFut;
use common::str::ParseBytes as _;
use surrealdb_cnf as cnf;

use crate::CommunityComposer;
use crate::buc::store::ObjectStore;
#[cfg(not(target_arch = "wasm32"))]
use crate::buc::store::file::FileStore;
use crate::buc::store::memory::MemoryStore;

pub mod access;
mod controller;
mod error;
pub mod manager;
pub(crate) mod presign;
pub mod store;

pub(crate) use controller::BucketController;
pub use controller::BucketOperation;
pub(crate) use error::Error;

/// The longest lifetime a presigned URL may be given, whatever a bucket's
/// `PERMISSIONS` clause allows.
///
/// This is the ceiling that protects an operator who never considered
/// presigning: a bucket with `PERMISSIONS FULL` can mint URLs, and without a
/// node-level bound the only limit would be what the caller asked for.
const DEFAULT_PRESIGN_MAX_TTL: Duration = Duration::from_secs(60 * 60);

/// The byte ceiling a presigned upload gets when the caller names none.
///
/// Concrete rather than unlimited, so `$max_size` is always a number a
/// `PERMISSIONS` clause can compare. Bound to `NONE` instead, a clause reading
/// `$max_size <= 5242880` would pass for an upload with no limit at all —
/// failing open, in the one place that must not.
///
/// Five gibibytes matches the largest object S3 accepts in a single `PUT`.
const DEFAULT_PRESIGN_MAX_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// The largest value `SURREAL_BUCKET_PRESIGN_MAX_TTL` may be set to.
///
/// A presigned URL cannot be recalled, so its lifetime is the window in which a
/// leaked URL stays useful. Seven days matches what S3 and GCS allow their own
/// signed URLs and is already generous.
const PRESIGN_MAX_TTL_CEILING: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The shortest `SURREAL_BUCKET_PRESIGN_SECRET` a node accepts, in bytes.
///
/// Matches the output of the HMAC-SHA256 the secret keys. A shorter secret
/// is refused rather than stretched: stretching adds no entropy, and a
/// guessable secret lets anyone mint a URL for any object in any bucket.
const PRESIGN_SECRET_MIN_LEN: usize = 32;

/// The node-wide secret that presigned URLs are signed with.
///
/// Every node behind one load balancer must hold the same value, or a URL
/// minted on one node fails to verify on another. Changing it invalidates
/// every outstanding URL at once.
///
/// Held behind an `Arc` because [`Config`] is cloned into every bucket
/// connection, and `Debug` prints nothing so the config can still be logged.
#[derive(Clone)]
pub(crate) struct PresignSecret(Arc<[u8]>);

impl PresignSecret {
	pub(crate) fn as_bytes(&self) -> &[u8] {
		&self.0
	}

	/// Accepts the raw bytes of `value`, and refuses one shorter than
	/// [`PRESIGN_SECRET_MIN_LEN`].
	fn parse(value: &str) -> Option<Self> {
		(value.len() >= PRESIGN_SECRET_MIN_LEN).then(|| Self(Arc::from(value.as_bytes())))
	}
}

#[cfg(test)]
impl PresignSecret {
	pub(crate) fn for_test(bytes: &[u8]) -> Self {
		Self(Arc::from(bytes))
	}
}

impl std::fmt::Debug for PresignSecret {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("PresignSecret(<redacted>)")
	}
}

#[derive(Clone, Debug)]
pub struct Config {
	bucket_list: Vec<PathBuf>,
	only_global: bool,
	global_bucket: Option<String>,
	presign_max_ttl: Duration,
	presign_max_size: u64,
	public_origin: Option<String>,
	presign_secret: Option<PresignSecret>,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			bucket_list: Vec::new(),
			only_global: false,
			global_bucket: None,
			presign_max_ttl: DEFAULT_PRESIGN_MAX_TTL,
			presign_max_size: DEFAULT_PRESIGN_MAX_SIZE,
			public_origin: None,
			presign_secret: None,
		}
	}
}

impl Config {
	/// The ceiling on a presigned URL's lifetime.
	pub(crate) fn presign_max_ttl(&self) -> Duration {
		self.presign_max_ttl
	}

	/// The byte ceiling on a presigned upload.
	pub(crate) fn presign_max_size(&self) -> u64 {
		self.presign_max_size
	}

	/// The origin a presigned URL is prefixed with, when one is configured.
	///
	/// Node-level rather than bucket-level, hence `SURREAL_PUBLIC_ORIGIN`
	/// rather than a `SURREAL_BUCKET_*` name: it describes where clients reach
	/// this deployment. It is read here because buckets are its only reader.
	pub(crate) fn public_origin(&self) -> Option<&str> {
		self.public_origin.as_deref()
	}

	/// The secret presigned URLs are signed with, when one is configured.
	///
	/// `None` disables presigning: minting fails and a signed request is
	/// refused. There is no fallback to a generated per-node key, which would
	/// work on one node and fail intermittently behind a load balancer.
	pub(crate) fn presign_secret(&self) -> Option<&PresignSecret> {
		self.presign_secret.as_ref()
	}
}

impl cnf::Config for Config {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key_with("bucket_folder_allowlist", &mut self.bucket_list, |x| {
			Some(cnf::extract_allowed_paths(x, false, "bucket folder"))
		})
		.parse_key_with("global_bucket", &mut self.global_bucket, |x| Some(Some(x.to_owned())))
		.parse_key("global_bucket_enforced", &mut self.only_global)
		.parse_key_with("public_origin", &mut self.public_origin, |x| {
			// Must carry a scheme and no path. A bare host cannot be joined to a
			// path to make a URL, and a value with a path would produce
			// `https://host/base/bucket/...`, which the signature does not
			// cover and the route does not serve. A value that fails this is
			// refused rather than repaired: the node logs that it could not
			// parse the key and leaves the origin unset, so `origin` reads
			// `NONE` and `href` stays relative rather than naming a host the
			// signature does not cover.
			let trimmed = x.trim_end_matches('/');
			let valid = url::Url::parse(trimmed).is_ok_and(|u| {
				matches!(u.scheme(), "http" | "https") && u.path() == "/" && u.query().is_none()
			});
			valid.then(|| Some(trimmed.to_owned()))
		})
		// `bucket_` prefixed, because the key is the environment variable minus
		// `SURREAL_`: these are `SURREAL_BUCKET_PRESIGN_MAX_SIZE` and
		// `SURREAL_BUCKET_PRESIGN_MAX_TTL`, the names the errors that name a
		// ceiling tell an operator to raise.
		.parse_key_with("bucket_presign_max_size", &mut self.presign_max_size, |x| {
			// Byte suffixes accepted, as every other size setting in the tree
			// does. A ceiling written `100MB` that parsed to nothing would leave
			// the permissive default in place, which is the wrong direction for
			// a limit to fail in.
			x.parse_bytes::<u64>().ok()
		})
		.parse_key_with("bucket_presign_max_ttl", &mut self.presign_max_ttl, |x| {
			// Clamped rather than rejected: a node that refused to start over a
			// too-generous value would take the whole deployment down for a
			// setting that has a safe interpretation.
			//
			// Parsed as a duration, so `15m` and `1h` work and not only a bare
			// second count. The same reasoning as the size ceiling: a value that
			// failed to parse would silently keep the more permissive default.
			cnf::parse_duration(x).ok().map(|ttl| ttl.min(PRESIGN_MAX_TTL_CEILING))
		})
		// `SURREAL_BUCKET_PRESIGN_SECRET`. A value that is too short is
		// refused, which leaves presigning disabled: failing closed, since a
		// weak secret is worse than none.
		.parse_key_with("bucket_presign_secret", &mut self.presign_secret, |x| {
			PresignSecret::parse(x).map(Some)
		});
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

/// Trait for creating connections to bucket storage backends.
///
/// Implementors of this trait can parse storage URLs and create appropriate
/// [`ObjectStore`] instances. Supported backends are `memory://`, `file://`,
/// and cloud object storage via the `object_store` crate: S3 / S3-compatible
/// (`s3://`, `s3+http://`, `s3+https://`), Google Cloud Storage (`gs://`,
/// `gcs://`), and Azure Blob Storage (`az://`, `azure://`).
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
	) -> BoxFut<'a, Result<Arc<dyn ObjectStore>>>;
}

impl BucketStoreProviderRequirements for CommunityComposer {}

impl BucketStoreProvider for CommunityComposer {
	fn connect<'a>(
		&self,
		url: &'a str,
		_global: bool,
		_readonly: bool,
		config: Config,
	) -> BoxFut<'a, Result<Arc<dyn ObjectStore>>> {
		Box::pin(async move {
			// Buckets back `file::*`; a build without them connects none.
			#[cfg(not(feature = "files"))]
			{
				let _ = (url, config);
				bail!(Error::FeatureDisabled)
			}
			#[cfg(feature = "files")]
			{
				#[cfg(target_arch = "wasm32")]
				let _ = config;
				if MemoryStore::parse_url(url) {
					return Ok(Arc::new(MemoryStore::new()) as Arc<dyn ObjectStore>);
				}

				#[cfg(not(target_arch = "wasm32"))]
				if let Some(opts) = FileStore::parse_url(url, &config).await? {
					return Ok(Arc::new(FileStore::new(opts, config)) as Arc<dyn ObjectStore>);
				}

				// Cloud object storage — AWS S3 (and S3-compatible services like
				// MinIO, Backblaze B2, Wasabi, Cloudflare R2), Google Cloud Storage,
				// and Azure Blob Storage — all via the `object_store` crate. See
				// `store::cloud` for the supported URL formats.
				#[cfg(not(target_arch = "wasm32"))]
				if let Some(store) = crate::buc::store::cloud::connect(url)? {
					return Ok(store);
				}

				bail!(Error::UnsupportedBackend)
			}
		})
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_cnf::{Config as _, ConfigMap};

	use super::*;

	fn parse(secret: &str) -> Config {
		let mut config = Config::default();
		config.parse(&ConfigMap::empty().with_key_value("bucket_presign_secret", secret));
		config
	}

	#[test]
	fn presigning_is_off_without_a_secret() {
		assert!(Config::default().presign_secret().is_none());
	}

	#[test]
	fn a_secret_of_the_minimum_length_is_accepted() {
		let secret = "s".repeat(PRESIGN_SECRET_MIN_LEN);
		let config = parse(&secret);
		assert_eq!(config.presign_secret().unwrap().as_bytes(), secret.as_bytes());
	}

	#[test]
	fn a_short_secret_leaves_presigning_off() {
		let config = parse(&"s".repeat(PRESIGN_SECRET_MIN_LEN - 1));
		assert!(config.presign_secret().is_none());
	}

	#[test]
	fn the_secret_never_reaches_debug_output() {
		let config = parse("a-secret-that-must-not-be-logged-anywhere");
		let rendered = format!("{config:?}");
		assert!(!rendered.contains("a-secret"), "{rendered}");
		assert!(rendered.contains("PresignSecret(<redacted>)"), "{rendered}");
	}
}
