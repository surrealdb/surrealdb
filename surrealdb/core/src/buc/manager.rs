use std::sync::Arc;

use anyhow::{Result, bail};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use crate::buc::store::prefixed::PrefixedStore;
use crate::buc::store::{ObjectKey, ObjectStore};
use crate::buc::{BucketStoreProvider, Config};
use crate::catalog::providers::BucketProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::err::Error;
use crate::kvs::Transaction;

struct Inner {
	connections: DashMap<BucketConnectionKey, Arc<dyn ObjectStore>>,
	provider: Box<dyn BucketStoreProvider>,
	config: Config,
}

/// Manages bucket storage connections with caching.
///
/// The `BucketsManager` is responsible for:
/// - Creating and caching connections to bucket storage backends
/// - Managing global bucket connections with automatic namespacing
/// - Enforcing global bucket policies when configured
///
/// Connections are cached by namespace, database, and bucket name to avoid
/// redundant connection establishment.
#[derive(Clone)]
pub(crate) struct BucketsManager(Arc<Inner>);

impl BucketsManager {
	/// Creates a new `BucketsManager` with the given storage provider.
	///
	/// # Arguments
	/// * `provider` - The bucket store provider used to create new connections
	pub(crate) fn new(provider: Box<dyn BucketStoreProvider>, config: Config) -> Self {
		BucketsManager(Arc::new(Inner {
			connections: DashMap::new(),
			provider,
			config,
		}))
	}

	/// Clears all cached bucket connections.
	///
	/// This is typically called during datastore restart to ensure fresh connections.
	pub(crate) fn clear(&self) {
		self.0.connections.clear();
	}

	/// Connects to a bucket storage backend.
	///
	/// If global bucket enforcement is enabled and this is not a global connection,
	/// returns `GlobalBucketEnforced` error.
	async fn connect(
		&self,
		url: &str,
		global: bool,
		readonly: bool,
	) -> Result<Arc<dyn ObjectStore>> {
		// Check if the global bucket is enforced
		if !global && self.0.config.only_global {
			bail!(Error::GlobalBucketEnforced);
		}
		self.0.provider.connect(url, global, readonly, self.0.config.clone()).await
	}

	/// Connects to a global bucket with automatic namespacing.
	///
	/// If no global bucket is configured, returns `NoGlobalBucket` error.
	/// The returned store is wrapped in a `PrefixedStore` with the key pattern:
	/// `/{ns}/{db}/{bu}` to isolate data between namespaces, databases, and buckets.
	async fn connect_global(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &str,
	) -> Result<Arc<dyn ObjectStore>> {
		// The name occupies one segment of the prefix below, and the prefix is
		// never re-checked downstream: `PrefixedStore` only validates the object
		// key, and the filesystem backend only checks containment in its own
		// root. A name carrying extra segments resolves onto another bucket,
		// database or namespace sharing the backend once the key is normalised.
		check_prefix_segment(bu)?;
		// Obtain the URL for the global bucket
		let Some(ref url) = self.0.config.global_bucket else {
			bail!(Error::NoGlobalBucket);
		};

		// Connect to the global bucket
		let global = self.0.provider.connect(url, true, false, self.0.config.clone()).await?;

		// Create a prefixstore for the specified bucket
		let key = ObjectKey::new(format!("/{ns}/{db}/{bu}"));
		Ok(Arc::new(PrefixedStore::new(global, key)))
	}

	/// Gets or creates a connection to a bucket's object store.
	///
	/// This method first checks the cache for an existing connection. If not found,
	/// it retrieves the bucket definition from the transaction, establishes a new
	/// connection (either to the specified backend or the global bucket), and caches it.
	///
	/// # Arguments
	/// * `tx` - The transaction to use for fetching the bucket definition
	/// * `ns` - The namespace ID
	/// * `db` - The database ID
	/// * `bu` - The bucket name
	pub(crate) async fn get_bucket_store(
		&self,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &str,
	) -> Result<Arc<dyn ObjectStore>> {
		// Attempt to obtain an existing bucket connection
		let key = BucketConnectionKey::new(ns, db, bu);
		match self.0.connections.entry(key) {
			Entry::Occupied(e) => Ok(Arc::clone(e.get())),
			Entry::Vacant(e) => {
				// Obtain the bucket definition
				let bd = tx.expect_db_bucket(ns, db, bu).await?;
				// Connect to the bucket
				let store = if let Some(ref backend) = bd.backend {
					self.connect(backend, false, bd.readonly).await?
				} else {
					self.connect_global(ns, db, bu).await?
				};
				// Persist the bucket connection
				e.insert(Arc::clone(&store));
				Ok(store)
			}
		}
	}

	/// Creates and caches a new backend connection for a bucket.
	///
	/// This is called when defining a new bucket to validate the backend URL
	/// and pre-populate the connection cache.
	///
	/// # Arguments
	/// * `ns` - The namespace ID
	/// * `db` - The database ID
	/// * `bu` - The bucket name
	/// * `read_only` - Whether the bucket should be read-only
	/// * `backend` - Optional backend URL; if `None`, uses the global bucket
	pub(crate) async fn new_backend(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &str,
		read_only: bool,
		backend: Option<&str>,
	) -> Result<()> {
		// Validate the store
		let store = if let Some(backend) = backend {
			self.connect(backend, false, read_only).await?
		} else {
			self.connect_global(ns, db, bu).await?
		};

		// Persist the store to cache
		let key = BucketConnectionKey::new(ns, db, bu);
		self.0.connections.insert(key, store);
		Ok(())
	}
}

/// Rejects a bucket name that would not stay within one segment of the global
/// bucket's key prefix.
fn check_prefix_segment(bu: &str) -> Result<()> {
	if bu.is_empty() || bu == "." || bu == ".." || bu.contains('/') || bu.contains('\\') {
		bail!(Error::InvalidBucketName(bu.to_owned()));
	}
	Ok(())
}

/// Key for caching bucket connections.
///
/// Uniquely identifies a bucket by its namespace, database, and bucket name.
#[derive(Hash, PartialEq, Eq)]
pub(super) struct BucketConnectionKey {
	ns: NamespaceId,
	db: DatabaseId,
	bu: String,
}

impl BucketConnectionKey {
	/// Creates a new bucket connection key.
	pub fn new(ns: NamespaceId, db: DatabaseId, bu: &str) -> Self {
		Self {
			ns,
			db,
			bu: bu.into(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::buc::store::memory::MemoryStore;
	use crate::buc::{BoxFuture, BucketStoreProviderRequirements};

	struct MemoryProvider;

	impl BucketStoreProviderRequirements for MemoryProvider {}

	impl BucketStoreProvider for MemoryProvider {
		fn connect<'a>(
			&self,
			_url: &'a str,
			_global: bool,
			_readonly: bool,
			_config: Config,
		) -> BoxFuture<'a, Result<Arc<dyn ObjectStore>>> {
			Box::pin(async { Ok(Arc::new(MemoryStore::new()) as Arc<dyn ObjectStore>) })
		}
	}

	fn manager() -> BucketsManager {
		let config = Config {
			global_bucket: Some("memory".to_owned()),
			..Config::default()
		};
		BucketsManager::new(Box::new(MemoryProvider), config)
	}

	/// Regression: the bucket name becomes one segment of the global bucket's
	/// `/{ns}/{db}/{bu}` key prefix, so a name carrying a separator or a `..`
	/// segment would resolve onto another bucket, database or namespace that
	/// shares the backend.
	#[tokio::test]
	async fn global_bucket_rejects_name_that_is_not_a_single_segment() {
		let manager = manager();
		for name in ["", ".", "..", "../../2/1/victim", "a/b", "a\\b"] {
			let Err(err) = manager.connect_global(NamespaceId(1), DatabaseId(1), name).await else {
				panic!("name {name:?} should be rejected");
			};
			assert!(
				err.to_string().contains("must be a single path segment"),
				"unexpected error for name {name:?}: {err}"
			);
		}
	}

	#[tokio::test]
	async fn global_bucket_accepts_ordinary_name() {
		assert!(manager().connect_global(NamespaceId(1), DatabaseId(1), "data").await.is_ok());
	}
}
