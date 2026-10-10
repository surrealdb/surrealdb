use std::sync::Arc;

use anyhow::{Result, bail};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use crate::buc::store::metered::MeteredObjectStore;
use crate::buc::store::prefixed::PrefixedStore;
use crate::buc::store::{ObjectKey, ObjectStore};
use crate::buc::{BucketStoreProvider, Config, Error};
use crate::catalog::providers::BucketProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::kvs::Transaction;
use crate::observe::ExecutionObserver;

struct Inner {
	connections: DashMap<BucketConnectionKey, Arc<dyn ObjectStore>>,
	provider: Box<dyn BucketStoreProvider>,
	config: Config,
	observer: Arc<dyn ExecutionObserver>,
}

/// Map a backend URL to a fixed, low-cardinality provider label for metrics.
fn backend_label(url: &str) -> &'static str {
	let scheme = url
		.split_once("://")
		.map(|(s, _)| s)
		.or_else(|| url.split_once(':').map(|(s, _)| s))
		.unwrap_or(url);
	match scheme {
		"s3" | "s3+http" | "s3+https" => "s3",
		"gs" | "gcs" => "gcs",
		"az" | "azure" => "azure",
		"file" => "file",
		"memory" => "memory",
		_ => "other",
	}
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
	/// * `config` - Bucket configuration (allowlist, global bucket, …)
	/// * `observer` - Execution observer that receives per-operation traffic metrics; connections
	///   are wrapped in a [`MeteredObjectStore`]
	pub(crate) fn new(
		provider: Box<dyn BucketStoreProvider>,
		config: Config,
		observer: Arc<dyn ExecutionObserver>,
	) -> Self {
		BucketsManager(Arc::new(Inner {
			connections: DashMap::new(),
			provider,
			config,
			observer,
		}))
	}

	/// Wrap a freshly-connected store so its operations are reported to the
	/// datastore observer as `surrealdb.bucket.*` metrics.
	fn meter(&self, store: Arc<dyn ObjectStore>, url: &str) -> Arc<dyn ObjectStore> {
		Arc::new(MeteredObjectStore::new(store, Arc::clone(&self.0.observer), backend_label(url)))
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
		let store = self.0.provider.connect(url, global, readonly, self.0.config.clone()).await?;
		Ok(self.meter(store, url))
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
		let Some(ref url) = self.0.config.global_bucket else {
			bail!(Error::NoGlobalBucket);
		};

		// Connect to the global bucket
		let global = self.0.provider.connect(url, true, false, self.0.config.clone()).await?;

		// Create a prefixstore for the specified bucket
		let key = ObjectKey::new(format!("/{ns}/{db}/{bu}"));
		let prefixed: Arc<dyn ObjectStore> = Arc::new(PrefixedStore::new(global, key));
		Ok(self.meter(prefixed, url))
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
		#[cfg(not(feature = "files"))]
		{
			let _ = (tx, ns, db, bu);
			bail!(crate::buc::Error::FeatureDisabled)
		}
		let key = BucketConnectionKey::new(ns, db, bu);
		// Attempt to obtain an existing bucket connection, dropping the guard
		// before the connect below.
		if let Some(existing) = self.0.connections.get(&key) {
			return Ok(Arc::clone(existing.value()));
		}
		// Obtain the bucket definition and connect holding no shard lock. A
		// `DashMap` guard is not await-safe: held across a yield it stalls every
		// other caller hashing to that shard, and on a single-threaded runtime
		// the holder can never be polled again, so the deadlock is permanent.
		let bd = tx.expect_db_bucket(ns, db, bu).await?;
		let store = if let Some(ref backend) = bd.backend {
			self.connect(backend, false, bd.readonly).await?
		} else {
			self.connect_global(ns, db, bu).await?
		};
		// Persist the bucket connection. Callers racing to resolve the same cold
		// bucket all adopt whichever connection lands first, so a bucket is never
		// served by two stores at once; the connections that lost are dropped here.
		match self.0.connections.entry(key) {
			Entry::Occupied(e) => Ok(Arc::clone(e.get())),
			Entry::Vacant(e) => {
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
		#[cfg(not(feature = "files"))]
		{
			let _ = (ns, db, bu, read_only, backend);
			bail!(crate::buc::Error::FeatureDisabled)
		}
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
	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::time::Duration;

	use anyhow::Result;
	use common::future::BoxFut;
	use tokio::sync::Notify;

	use super::BucketsManager;
	use crate::buc::store::ObjectStore;
	use crate::buc::store::memory::MemoryStore;
	use crate::buc::{BucketStoreProvider, BucketStoreProviderRequirements, Config};
	use crate::catalog::providers::DatabaseProvider;
	use crate::dbs::Session;
	use crate::dbs::capabilities::{Capabilities, Targets};
	use crate::kvs::{Datastore, TransactionType};
	use crate::observe::NoopObserver;

	/// A provider whose first `connect` parks until `release` is notified,
	/// announcing on `entered` that it has started; later connects return at
	/// once.
	struct GatedProvider {
		calls: AtomicUsize,
		entered: Arc<Notify>,
		release: Arc<Notify>,
	}

	impl BucketStoreProviderRequirements for GatedProvider {}

	impl BucketStoreProvider for GatedProvider {
		fn connect<'a>(
			&self,
			_url: &'a str,
			_global: bool,
			_readonly: bool,
			_config: Config,
		) -> BoxFut<'a, Result<Arc<dyn ObjectStore>>> {
			let gated = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
			let entered = Arc::clone(&self.entered);
			let release = Arc::clone(&self.release);
			Box::pin(async move {
				if gated {
					entered.notify_one();
					release.notified().await;
				}
				Ok(Arc::new(MemoryStore::new()) as Arc<dyn ObjectStore>)
			})
		}
	}

	/// Resolving a cold bucket holds no lock across its connect: while one
	/// caller is parked inside a slow connect, a second caller for the same
	/// bucket resolves it on a single-threaded runtime, and the parked caller
	/// then adopts the connection the second one cached.
	///
	/// The scenario runs on its own `current_thread` runtime, so a caller that
	/// blocked its thread on the cache would stop the runtime outright; the
	/// test thread observes that as a timeout instead of hanging.
	#[test]
	fn a_slow_connect_does_not_block_other_callers() {
		let (done_tx, done_rx) = std::sync::mpsc::channel();
		std::thread::spawn(move || {
			let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
			done_tx.send(rt.block_on(slow_connect_scenario())).ok();
		});
		match done_rx.recv_timeout(Duration::from_secs(10)) {
			Ok(result) => result.unwrap(),
			Err(_) => panic!("a caller blocked the runtime while another was connecting"),
		}
	}

	async fn slow_connect_scenario() -> Result<()> {
		let caps = Capabilities::all().with_experimental(Targets::All);
		let ds = Datastore::builder().with_capabilities(caps).build_with_path("memory").await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		for res in ds.execute("DEFINE BUCKET b BACKEND 'memory';", &ses, None).await? {
			res.result?;
		}
		let tx = Arc::new(ds.transaction(TransactionType::Read).await?);
		let db = tx.expect_db_by_name("test", "test").await?;
		let (ns, db) = (db.namespace_id, db.database_id);

		// A manager with an empty cache, as on a node that has not yet served
		// the bucket.
		let entered = Arc::new(Notify::new());
		let release = Arc::new(Notify::new());
		let buckets = BucketsManager::new(
			Box::new(GatedProvider {
				calls: AtomicUsize::new(0),
				entered: Arc::clone(&entered),
				release: Arc::clone(&release),
			}),
			Config::default(),
			Arc::new(NoopObserver),
		);
		let resolve = |buckets: &BucketsManager, tx: &Arc<_>| {
			let (buckets, tx) = (buckets.clone(), Arc::clone(tx));
			tokio::spawn(async move { buckets.get_bucket_store(&tx, ns, db, "b").await })
		};

		let first = resolve(&buckets, &tx);
		entered.notified().await;
		let second = resolve(&buckets, &tx).await??;
		assert!(!first.is_finished(), "the first connect is still parked");

		release.notify_one();
		let first = first.await??;
		assert!(Arc::ptr_eq(&first, &second), "the parked caller adopts the cached connection");
		let cached = buckets.get_bucket_store(&tx, ns, db, "b").await?;
		assert!(Arc::ptr_eq(&cached, &second), "later callers are served from the cache");
		tx.cancel().await?;
		Ok(())
	}
}
