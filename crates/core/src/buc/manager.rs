use std::sync::Arc;

use anyhow::{Result, bail};
use dashmap::DashMap;

use crate::buc::BucketStoreProvider;
use crate::buc::store::prefixed::PrefixedStore;
use crate::buc::store::{ObjectKey, ObjectStore};
use crate::catalog::providers::BucketProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::cnf::{GLOBAL_BUCKET, GLOBAL_BUCKET_ENFORCED};
use crate::err::Error;
use crate::kvs::Transaction;

type BucketConnections = Arc<DashMap<BucketConnectionKey, Arc<dyn ObjectStore>>>;

#[derive(Clone)]
pub(crate) struct BucketsManager {
	buckets: BucketConnections,
	provider: Arc<dyn BucketStoreProvider>,
}

impl BucketsManager {
	pub(crate) fn new(provider: Arc<dyn BucketStoreProvider>) -> Self {
		Self {
			buckets: Default::default(),
			provider,
		}
	}

	pub(crate) fn clear(&self) {
		self.buckets.clear();
	}

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
		self.provider.connect(url, readonly).await
	}

	/// Connect to a global bucket, if one is configured
	/// If no global bucket is configured, the NoGlobalBucket error will be returned
	/// The key in the global bucket will be: `{ns}/{db}/{bu}`
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
		let global = self.connect(url, true, false).await?;

		// Create a prefixstore for the specified bucket
		let key = ObjectKey::new(format!("/{ns}/{db}/{bu}"));
		Ok(Arc::new(PrefixedStore::new(global, key)))
	}

	pub(crate) async fn get_bucket_store(
		&self,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &str,
	) -> Result<Arc<dyn ObjectStore>> {
		// Attempt to obtain an existing bucket connection
		let key = BucketConnectionKey::new(ns, db, bu);
		if let Some(bucket_ref) = self.buckets.get(&key) {
			Ok((*bucket_ref).clone())
		} else {
			// Obtain the bucket definition
			let bd = tx.expect_db_bucket(ns, db, bu).await?;

			// Connect to the bucket
			let store = if let Some(ref backend) = bd.backend {
				self.connect(backend, false, bd.readonly).await?
			} else {
				self.connect_global(ns, db, bu).await?
			};

			// Persist the bucket connection
			self.buckets.insert(key, store.clone());
			Ok(store)
		}
	}

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

#[derive(Hash, PartialEq, Eq)]
pub(super) struct BucketConnectionKey {
	ns: NamespaceId,
	db: DatabaseId,
	bu: String,
}

impl BucketConnectionKey {
	pub fn new(ns: NamespaceId, db: DatabaseId, bu: &str) -> Self {
		Self {
			ns,
			db,
			bu: bu.into(),
		}
	}
}
