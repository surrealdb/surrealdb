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
		// Obtain the URL for the global bucket
		let Some(ref url) = self.0.config.global_bucket else {
			bail!(Error::NoGlobalBucket);
		};

		// SECURITY: `bu` becomes the last segment of the key prefix below, so it
		// has to be a single path segment. A name carrying a path separator or a
		// `..` segment reshapes the prefix, and the bucket's keys then resolve
		// under a different `/{ns}/{db}/{bu}` triple on any backend that
		// normalises paths: `FileStore::to_os_path` collapses `..` through
		// `path_clean` before its root check, so the rewritten path stays inside
		// the allowlisted root and one database can read, write and delete
		// another namespace's or database's objects. Object keys are already
		// checked in `PrefixedStore`; the prefix needs the same guarantee.
		if bu.contains(['/', '\\']) || bu == "." || bu == ".." {
			bail!(Error::InvalidBucketName(bu.to_owned()));
		}

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

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use surrealdb_types::vars;
	use temp_dir::TempDir;

	use crate::cnf::ConfigMap;
	use crate::dbs::Session;
	use crate::dbs::capabilities::{Capabilities, ExperimentalTarget, Targets};
	use crate::kvs::{Builder, Datastore};

	/// A datastore whose global bucket is a `file://` backend rooted at `root`.
	async fn datastore(root: &str) -> Datastore {
		let config = ConfigMap::empty()
			.with_key_value("global_bucket", format!("file://{root}"))
			.with_key_value("bucket_folder_allowlist", root.to_owned());
		Builder::new()
			.with_config(config)
			.with_capabilities(
				Capabilities::all().with_experimental(Targets::<ExperimentalTarget>::All),
			)
			.build_with_path("memory")
			.await
			.expect("in-memory datastore with a file:// global bucket")
	}

	/// Buckets on the global backend are separated only by the `/{ns}/{db}/{bu}`
	/// key prefix, so a name that is not a single path segment escapes that
	/// separation: the filesystem backend collapses `..` before checking its
	/// root, which puts `../../9/9/other` in namespace 9 / database 9's keyspace
	/// and makes `x/../victim` an alias for bucket `victim`.
	#[tokio::test]
	async fn global_bucket_name_must_be_a_single_path_segment() {
		let dir = TempDir::new().unwrap();
		let root = tokio::fs::canonicalize(dir.path()).await.unwrap();
		let ds = datastore(&root.to_string_lossy()).await;
		let ses = Session::owner().with_ns("tenant").with_db("app");

		for name in ["x/../victim", "../../9/9/other", "..", ".", "a\\..\\b"] {
			let vars = vars! { "name": name };
			let res = ds.execute("DEFINE BUCKET $name", &ses, Some(vars)).await.unwrap();
			let err = res
				.into_iter()
				.next()
				.expect("one statement")
				.result
				.expect_err(&format!("bucket name `{name}` should be rejected"));
			assert!(
				err.to_string().contains("single path segment"),
				"unexpected error for `{name}`: {err}"
			);
		}

		// A single-segment name still round-trips through the global bucket.
		let res = ds
			.execute(
				r#"
				DEFINE BUCKET victim;
				file::put(type::file("victim", "/secret.txt"), "s3cret");
				file::get(type::file("victim", "/secret.txt"));
				"#,
				&ses,
				None,
			)
			.await
			.unwrap();
		let got = res.into_iter().next_back().expect("three statements").result.unwrap();
		assert_eq!(got, surrealdb_types::Value::Bytes(b"s3cret".to_vec().into()));
	}
}
