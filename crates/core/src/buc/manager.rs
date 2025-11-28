use std::sync::Arc;

use anyhow::{Result, bail};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use crate::buc::BucketStoreProvider;
use crate::buc::store::prefixed::PrefixedStore;
use crate::buc::store::{ObjectKey, ObjectStore};
use crate::catalog::providers::BucketProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::cnf::{GLOBAL_BUCKET, GLOBAL_BUCKET_ENFORCED};
use crate::err::Error;
use crate::kvs::Transaction;

/// Type alias for the concurrent map of bucket connections.
type BucketConnections = Arc<DashMap<BucketConnectionKey, Arc<dyn ObjectStore>>>;

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
pub(crate) struct BucketsManager {
	buckets: BucketConnections,
	provider: Arc<dyn BucketStoreProvider>,
}

impl BucketsManager {
	/// Creates a new `BucketsManager` with the given storage provider.
	///
	/// # Arguments
	/// * `provider` - The bucket store provider used to create new connections
	pub(crate) fn new(provider: Arc<dyn BucketStoreProvider>) -> Self {
		Self {
			buckets: Default::default(),
			provider,
		}
	}

	/// Clears all cached bucket connections.
	///
	/// This is typically called during datastore restart to ensure fresh connections.
	pub(crate) fn clear(&self) {
		self.buckets.clear();
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
		if !global && *GLOBAL_BUCKET_ENFORCED {
			bail!(Error::GlobalBucketEnforced);
		}
		self.provider.connect(url, global, readonly).await
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
		// Obtain the URL for the global bucket
		let Some(ref url) = *GLOBAL_BUCKET else {
			bail!(Error::NoGlobalBucket);
		};

		// Connect to the global bucket
		let global = self.provider.connect(url, true, false).await?;

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
		match self.buckets.entry(key) {
			Entry::Occupied(e) => Ok(e.get().clone()),
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
				e.insert(store.clone());
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
		self.buckets.insert(key, store);
		Ok(())
	}
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
