use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, bail};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use surrealdb_datastore::close::{ActionScope, CommitAction};

use crate::buc::store::metered::MeteredObjectStore;
use crate::buc::store::prefixed::PrefixedStore;
use crate::buc::store::{ObjectKey, ObjectStore};
use crate::buc::{BucketStoreProvider, Config, Error};
use crate::catalog::BucketLevel;
use crate::catalog::providers::BucketProvider;
use crate::kvs::Transaction;
use crate::observe::ExecutionObserver;

struct Inner {
	connections: DashMap<BucketConnectionKey, Arc<dyn ObjectStore>>,
	/// The configured global bucket, opened once and shared by every
	/// `global://` bucket, which each see it through their own prefix.
	global: std::sync::Mutex<Option<Arc<dyn ObjectStore>>>,
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

/// Validates that a bucket name is a single non-empty segment, and contains
/// no control characters, path separators, or leading hyphens. A database
/// bucket stored with no backend is served under `/{ns}/{db}/{bu}` in the
/// global bucket, so a name must not traverse out of that prefix.
fn validate_bucket_name(bu: &str) -> Result<()> {
	if bu.is_empty()
		|| bu == "."
		|| bu == ".."
		|| bu.starts_with('-')
		|| bu.contains('/')
		|| bu.contains('\\')
		|| bu.chars().any(|c| c.is_control())
	{
		bail!(Error::InvalidBucketUrl(format!("Invalid bucket name `{bu}`")));
	}
	Ok(())
}

/// The scheme of a backend served from the configured global bucket.
const GLOBAL_SCHEME: &str = "global://";

/// Whether `url` names a path in the global bucket.
pub(crate) fn is_global_backend(url: &str) -> bool {
	url.starts_with(GLOBAL_SCHEME)
}

/// The prefix a `global://` path selects in the global bucket, or `None` for
/// the whole bucket. Each segment must be a name: `.`, `..`, a backslash and a
/// control character are refused, so the prefix cannot leave the path it names.
fn global_prefix(path: &str) -> Result<Option<ObjectKey>> {
	let mut segments = path.split('/').filter(|s| !s.is_empty()).peekable();
	if segments.peek().is_none() {
		return Ok(None);
	}
	for segment in segments {
		if segment == "."
			|| segment == ".."
			|| segment.contains('\\')
			|| segment.chars().any(|c| c.is_control())
		{
			bail!(Error::InvalidBucketUrl(format!("Invalid global bucket path `{path}`")));
		}
	}
	Ok(Some(ObjectKey::new(format!("/{path}"))))
}

/// Manages bucket storage connections with caching.
///
/// The `BucketsManager` is responsible for:
/// - Creating and caching connections to bucket storage backends
/// - Serving `global://` backends from the configured global bucket
/// - Enforcing global bucket policies when configured
///
/// Connections are cached by the bucket's level and name to avoid redundant
/// connection establishment.
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
			global: std::sync::Mutex::new(None),
			provider,
			config,
			observer,
		}))
	}

	/// The origin presigned URLs are prefixed with, when one is configured.
	pub(crate) fn public_origin(&self) -> Option<&str> {
		self.0.config.public_origin()
	}

	/// The secret presigned URLs are signed with, when this node has one.
	pub(crate) fn presign_secret(&self) -> Option<&crate::buc::PresignSecret> {
		self.0.config.presign_secret()
	}

	/// The byte ceiling on a presigned upload for this node.
	pub(crate) fn presign_max_size(&self) -> u64 {
		self.0.config.presign_max_size()
	}

	/// The ceiling on a presigned URL's lifetime for this node.
	pub(crate) fn presign_max_ttl(&self) -> std::time::Duration {
		self.0.config.presign_max_ttl()
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
		*self.0.global.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
	}

	/// Connects to a bucket storage backend.
	///
	/// A `global://` url is served from the global bucket. Any other url is
	/// refused with `GlobalBucketEnforced` when only the global bucket may be
	/// used.
	async fn connect(&self, url: &str, readonly: bool) -> Result<Arc<dyn ObjectStore>> {
		if let Some(path) = url.strip_prefix(GLOBAL_SCHEME) {
			return self.connect_global(path).await;
		}
		if self.0.config.only_global {
			bail!(Error::GlobalBucketEnforced);
		}
		let store = self.0.provider.connect(url, false, readonly, self.0.config.clone()).await?;
		Ok(self.meter(store, url))
	}

	/// Connects to the global bucket, at the prefix `path` selects in it.
	///
	/// If no global bucket is configured, returns `NoGlobalBucket` error. Every
	/// `global://` bucket is served by the one store the global bucket is opened
	/// as, read-write since `READONLY` is enforced from each definition, so they
	/// all see the same objects; a non-empty path wraps it in a `PrefixedStore`
	/// at `/{path}`.
	async fn connect_global(&self, path: &str) -> Result<Arc<dyn ObjectStore>> {
		let prefix = global_prefix(path)?;
		let Some(ref url) = self.0.config.global_bucket else {
			bail!(Error::NoGlobalBucket);
		};
		let opened =
			self.0.global.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
		let global = match opened {
			Some(global) => global,
			None => {
				let store =
					self.0.provider.connect(url, true, false, self.0.config.clone()).await?;
				// Callers racing to open it adopt whichever store lands first.
				let mut slot =
					self.0.global.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
				Arc::clone(slot.get_or_insert(store))
			}
		};
		let store: Arc<dyn ObjectStore> = match prefix {
			Some(prefix) => Arc::new(PrefixedStore::new(global, prefix)),
			None => global,
		};
		Ok(self.meter(store, url))
	}

	/// Gets or creates a connection to a bucket's object store.
	///
	/// The bucket definition the transaction reads names the backend, and the
	/// connection is looked up by that backend, so a transaction is served the
	/// store its own snapshot of the catalog names: a definition or backend
	/// change another transaction has made but not committed is never seen
	/// through the cache.
	///
	/// # Arguments
	/// * `tx` - The transaction to use for fetching the bucket definition
	/// * `level` - The level the bucket is defined at
	/// * `bu` - The bucket name
	pub(crate) async fn get_bucket_store(
		&self,
		tx: &Transaction,
		level: BucketLevel,
		bu: &str,
	) -> Result<Arc<dyn ObjectStore>> {
		#[cfg(not(feature = "files"))]
		{
			let _ = (tx, level, bu);
			bail!(crate::buc::Error::FeatureDisabled)
		}
		validate_bucket_name(bu)?;
		let bd = tx.expect_bucket(level, bu).await?;
		// The catalog gives every database bucket a backend, and a bucket at the
		// root or a namespace is only ever stored with one.
		let Some(ref backend) = bd.backend else {
			bail!(Error::BucketUnavailable(bu.to_owned()));
		};
		let key = BucketConnectionKey::new(level, bu, backend, bd.readonly);
		// Attempt to obtain an existing bucket connection, dropping the guard
		// before the connect below.
		if let Some(existing) = self.0.connections.get(&key) {
			return Ok(Arc::clone(existing.value()));
		}
		// Connect holding no shard lock. A `DashMap` guard is not await-safe:
		// held across a yield it stalls every other caller hashing to that shard,
		// and on a single-threaded runtime the holder can never be polled again,
		// so the deadlock is permanent.
		let store = self.connect(backend, bd.readonly).await?;
		// Persist the bucket connection. Callers racing to resolve the same cold
		// bucket all adopt whichever connection lands first, so a bucket is never
		// served by two stores at once; the connections that lost are dropped here.
		let store = match self.0.connections.entry(key) {
			Entry::Occupied(e) => Arc::clone(e.get()),
			Entry::Vacant(e) => {
				e.insert(Arc::clone(&store));
				store
			}
		};
		Ok(store)
	}

	/// The work a statement that connected bucket `bu` at `level` to `backend`,
	/// opened `readonly`, leaves for its transaction's commit: dropping the
	/// bucket's cached connections to any other backend or mode. Until then
	/// the definition the catalog holds may still name one of them, and a
	/// connection dropped early cannot always be opened again on the same
	/// objects (a `memory` backend opens empty), so a statement that does not
	/// commit leaves them all in place.
	pub(crate) fn evict_on_commit(
		&self,
		level: BucketLevel,
		bu: &str,
		backend: &str,
		readonly: bool,
	) -> Box<dyn CommitAction> {
		Box::new(EvictOtherConnections {
			buckets: self.clone(),
			key: BucketConnectionKey::new(level, bu, backend, readonly),
		})
	}

	/// Creates and caches a new backend connection for a bucket.
	///
	/// This is called when defining a bucket, or altering its backend, to
	/// validate the backend URL. The connection is cached under the backend it
	/// serves, so it is reached only by a transaction whose catalog names that
	/// backend. The bucket's connections to other backends stay cached: the
	/// caller drops them once its statement commits ([`Self::evict_on_commit`]).
	///
	/// # Arguments
	/// * `level` - The level the bucket is defined at
	/// * `bu` - The bucket name
	/// * `read_only` - Whether the bucket should be read-only
	/// * `backend` - The backend URL
	pub(crate) async fn new_backend(
		&self,
		level: BucketLevel,
		bu: &str,
		read_only: bool,
		backend: &str,
	) -> Result<()> {
		#[cfg(not(feature = "files"))]
		{
			let _ = (level, bu, read_only, backend);
			bail!(crate::buc::Error::FeatureDisabled)
		}
		validate_bucket_name(bu)?;
		// Validate the store
		let store = self.connect(backend, read_only).await?;

		// Persist the store to cache
		let key = BucketConnectionKey::new(level, bu, backend, read_only);
		self.0.connections.insert(key, store);
		Ok(())
	}
}

/// Drops a bucket's cached connections to every backend and mode but one,
/// once the statement that connected that one has committed.
struct EvictOtherConnections {
	buckets: BucketsManager,
	key: BucketConnectionKey,
}

impl CommitAction for EvictOtherConnections {
	fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
		Box::pin(async move {
			let kept = &self.key;
			self.buckets.0.connections.retain(|key, _| {
				key.level != kept.level
					|| key.bu != kept.bu
					|| (key.backend == kept.backend && key.readonly == kept.readonly)
			});
		})
	}

	/// The connection belongs to the statement that defined or altered the
	/// bucket, which a save point rollback undoes.
	fn scope(&self) -> ActionScope {
		ActionScope::SavePoint
	}
}

/// Key for caching bucket connections.
///
/// Identifies a connection by the bucket's level and name and by the backend
/// and mode it was opened with, since a provider may open a read-only bucket
/// as a read-only handle.
#[derive(Hash, PartialEq, Eq)]
pub(super) struct BucketConnectionKey {
	level: BucketLevel,
	bu: String,
	backend: String,
	readonly: bool,
}

impl BucketConnectionKey {
	/// Creates a new bucket connection key.
	pub fn new(level: BucketLevel, bu: &str, backend: &str, readonly: bool) -> Self {
		Self {
			level,
			bu: bu.into(),
			backend: backend.into(),
			readonly,
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
	use crate::catalog::BucketLevel;
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
	/// test thread observes that as a timeout instead of hanging. The bound
	/// only decides how long a blocked runtime takes to be reported, so it is
	/// far beyond what the scenario takes on a runner whose cores other tests
	/// hold.
	#[test]
	fn a_slow_connect_does_not_block_other_callers() {
		let (done_tx, done_rx) = std::sync::mpsc::channel();
		std::thread::spawn(move || {
			let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
			done_tx.send(rt.block_on(slow_connect_scenario())).ok();
		});
		match done_rx.recv_timeout(Duration::from_secs(120)) {
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
			tokio::spawn(async move {
				buckets.get_bucket_store(&tx, BucketLevel::Db(ns, db), "b").await
			})
		};

		let first = resolve(&buckets, &tx);
		entered.notified().await;
		let second = resolve(&buckets, &tx).await??;
		assert!(!first.is_finished(), "the first connect is still parked");

		release.notify_one();
		let first = first.await??;
		assert!(Arc::ptr_eq(&first, &second), "the parked caller adopts the cached connection");
		let cached = buckets.get_bucket_store(&tx, BucketLevel::Db(ns, db), "b").await?;
		assert!(Arc::ptr_eq(&cached, &second), "later callers are served from the cache");
		tx.cancel().await?;
		Ok(())
	}

	#[test]
	fn rejects_bucket_names_with_path_separators_or_traversal() {
		assert!(super::validate_bucket_name("valid_bucket-1").is_ok());
		for invalid in [
			"",
			".",
			"..",
			"../other",
			"a/b",
			"a\\b",
			"bad\0name",
			"bad\nname",
			"bad\rname",
			"bad\x7fname",
			"-leading-hyphen",
		] {
			assert!(
				super::validate_bucket_name(invalid).is_err(),
				"bucket name {invalid:?} must be rejected"
			);
		}
	}

	#[tokio::test]
	async fn bucket_permission_clause_reading_same_bucket_does_not_recurse() -> Result<()> {
		let caps = Capabilities::all().with_experimental(Targets::All);
		let ds = Datastore::builder()
			.with_capabilities(caps)
			.with_auth(true)
			.build_with_path("memory")
			.await?;
		let owner = Session::owner().with_ns("test").with_db("test");
		let setup = "
			DEFINE BUCKET b BACKEND 'memory' PERMISSIONS WHERE f'b:/allow' != NONE AND f'b:/allow'.exists();
			f'b:/allow'.put('ok');
			f'b:/hello'.put('world');
		";
		for res in ds.execute(setup, &owner, None).await? {
			res.result?;
		}
		let record_user = Session::for_record(
			"test",
			"test",
			"ac",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "u1")),
		);
		let mut results =
			ds.execute("RETURN <string> f'b:/hello'.get();", &record_user, None).await?;
		let val = results.remove(0).result?;
		assert_eq!(val, surrealdb_types::Value::String("world".to_owned()));
		Ok(())
	}

	/// A datastore whose global bucket is a directory of `dir`, as a root owner
	/// sees it from the database `test`/`test`.
	async fn global_datastore(dir: &std::path::Path) -> Result<(Arc<Datastore>, Session)> {
		// The allowlist holds canonical paths, so the bucket's root must be one.
		let dir = dir.canonicalize()?;
		let caps = Capabilities::all().with_experimental(Targets::All);
		let config = surrealdb_cnf::ConfigMap::empty()
			.with_key_value("bucket_folder_allowlist", dir.to_str().unwrap())
			.with_key_value("global_bucket", &format!("file://{}", dir.display()));
		let ds = Datastore::builder()
			.with_capabilities(caps)
			.with_config(config)
			.build_with_path("memory")
			.await?;
		Ok((ds, Session::owner().with_ns("test").with_db("test")))
	}

	/// Each statement's single result, failing on the first error.
	async fn values(
		ds: &Datastore,
		ses: &Session,
		sql: &str,
	) -> Result<Vec<surrealdb_types::Value>> {
		ds.execute(sql, ses, None)
			.await?
			.into_iter()
			.map(|res| res.result.map_err(Into::into))
			.collect()
	}

	/// A `global://` backend names a path in the global bucket and adds no
	/// prefix of its own: a bucket over the whole of it reads what a bucket over
	/// a path in it wrote, under that path, and the reverse.
	#[tokio::test]
	async fn global_backends_share_the_global_bucket() -> Result<()> {
		let dir = temp_dir::TempDir::new()?;
		let (ds, ses) = global_datastore(dir.path()).await?;
		let out = values(
			&ds,
			&ses,
			r#"
			DEFINE BUCKET whole BACKEND "global://";
			DEFINE BUCKET sub BACKEND "global://app/sub";
			f"sub:/a.txt".put("from sub");
			f"whole:/app/sub/b.txt".put("from whole");
			<string> f"whole:/app/sub/a.txt".get();
			<string> f"sub:/b.txt".get();
			f"whole:/a.txt".exists();
			"#,
		)
		.await?;
		assert_eq!(out[4], surrealdb_types::Value::String("from sub".to_owned()));
		assert_eq!(out[5], surrealdb_types::Value::String("from whole".to_owned()));
		assert_eq!(out[6], surrealdb_types::Value::Bool(false));
		Ok(())
	}

	/// A database bucket stored with no backend, as an older release or an
	/// import writes one, is served from the global bucket under
	/// `/{ns}/{db}/{bu}`, so the objects written there stay reachable, and it
	/// reads back with that path as its backend.
	#[tokio::test]
	async fn a_bucket_stored_without_a_backend_reads_its_global_prefix() -> Result<()> {
		let dir = temp_dir::TempDir::new()?;
		let (ds, ses) = global_datastore(dir.path()).await?;
		values(&ds, &ses, r#"DEFINE BUCKET whole BACKEND "global://";"#).await?;
		let tx = ds.transaction(TransactionType::Write).await?;
		let db = tx.expect_db_by_name("test", "test").await?;
		let (ns, db) = (db.namespace_id, db.database_id);
		let key = crate::key::schema::BucketKey {
			ns,
			db,
			bu: std::borrow::Cow::Borrowed("legacy"),
		};
		let stored = crate::catalog::StoredBucketDefinition {
			name: "legacy".into(),
			..Default::default()
		};
		tx.set_key(&key, &stored).await?;
		tx.commit().await?;
		let out = values(
			&ds,
			&ses,
			&format!(
				r#"
				f"whole:/{ns}/{db}/legacy/old.txt".put("written under the prefix");
				<string> f"legacy:/old.txt".get();
				(INFO FOR DB).buckets.legacy;
				"#
			),
		)
		.await?;
		assert_eq!(out[1], surrealdb_types::Value::String("written under the prefix".to_owned()));
		assert_eq!(
			out[2],
			surrealdb_types::Value::String(format!(
				"DEFINE BUCKET legacy BACKEND 'global://{ns}/{db}/legacy' PERMISSIONS FULL"
			))
		);
		Ok(())
	}

	/// The global bucket holds every namespace's objects, so a principal below
	/// the root can neither define nor alter a bucket onto a `global://`
	/// backend, and a refused statement stores nothing; a backend of its own
	/// stays open to it.
	#[tokio::test]
	async fn a_global_backend_needs_a_root_principal() -> Result<()> {
		use crate::iam::{Level, Role};
		let dir = temp_dir::TempDir::new()?;
		let dir = dir.path().canonicalize()?;
		let caps = Capabilities::all().with_experimental(Targets::All);
		let config = surrealdb_cnf::ConfigMap::empty()
			.with_key_value("global_bucket", &format!("file://{}", dir.display()))
			.with_key_value("bucket_folder_allowlist", dir.to_str().unwrap());
		let ds = Datastore::builder()
			.with_capabilities(caps)
			.with_auth(true)
			.with_config(config)
			.build_with_path("memory")
			.await?;
		let owner = Session::owner().with_ns("test").with_db("test");
		values(&ds, &owner, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await?;
		for level in [
			Level::Namespace("test".to_owned()),
			Level::Database("test".to_owned(), "test".to_owned()),
		] {
			let mut ses = Session::for_level(level.clone(), Role::Owner);
			ses.ns = Some("test".to_owned());
			ses.db = Some("test".to_owned());
			let out = ds
				.execute(
					r#"
					DEFINE BUCKET g BACKEND "global://";
					DEFINE BUCKET l BACKEND "memory";
					ALTER BUCKET l BACKEND "global://test";
					(INFO FOR DB).buckets;
					REMOVE BUCKET l;
					"#,
					&ses,
					None,
				)
				.await?;
			let refused = "Only a root user can give a bucket a `global://` backend";
			let err = |i: usize| out[i].result.as_ref().map_err(|e| e.to_string()).err();
			assert_eq!(err(0).as_deref(), Some(refused), "{level:?} defines onto global://");
			assert_eq!(err(1), None, "{level:?} defines onto its own backend");
			assert_eq!(err(2).as_deref(), Some(refused), "{level:?} alters onto global://");
			assert_eq!(
				out[3].result.as_ref().ok().cloned(),
				Some(surrealdb_types::Value::from_t(surrealdb_types::object! {
					l: "DEFINE BUCKET l BACKEND 'memory' PERMISSIONS FULL".to_owned(),
				})),
				"{level:?} stores only what it was allowed"
			);
		}
		Ok(())
	}

	/// A principal reaches the buckets of the level its session selects, with
	/// the authority it holds there: an editor writes and a viewer reads a root
	/// bucket from a session with no namespace, a namespace owner a bucket of
	/// its namespace from a session with no database, and a guest at the root
	/// is refused even by a bucket with `PERMISSIONS FULL`.
	#[tokio::test]
	async fn a_bucket_above_the_database_is_reached_with_the_sessions_authority() -> Result<()> {
		use crate::iam::{Level, Role};
		let caps = Capabilities::all().with_experimental(Targets::All);
		let ds = Datastore::builder()
			.with_capabilities(caps)
			.with_auth(true)
			.build_with_path("memory")
			.await?;
		values(
			&ds,
			&Session::owner(),
			r#"
			DEFINE BUCKET top ON ROOT BACKEND "memory" PERMISSIONS FULL;
			DEFINE NAMESPACE test;
			USE NS test;
			DEFINE BUCKET mid ON NAMESPACE BACKEND "memory";
			"#,
		)
		.await?;
		let err = |out: &[crate::dbs::QueryResult], i: usize| {
			out[i].result.as_ref().map_err(|e| e.to_string()).err()
		};
		let out = ds
			.execute(
				r#"f"top:/a.txt".put("by an editor"); <string> f"top:/a.txt".get();"#,
				&Session::editor(),
				None,
			)
			.await?;
		assert_eq!(err(&out, 0), None, "a root editor writes a root bucket");
		assert_eq!(
			out[1].result.as_ref().ok().cloned(),
			Some(surrealdb_types::Value::String("by an editor".to_owned()))
		);
		let out = ds
			.execute(
				r#"<string> f"top:/a.txt".get(); f"top:/b.txt".put("x");"#,
				&Session::viewer(),
				None,
			)
			.await?;
		assert_eq!(err(&out, 0), None, "a root viewer reads a root bucket");
		assert!(err(&out, 1).is_some(), "a root viewer does not write a root bucket");
		let mut ns_owner = Session::for_level(Level::Namespace("test".to_owned()), Role::Owner);
		ns_owner.ns = Some("test".to_owned());
		let out = ds
			.execute(r#"f"mid:/a.txt".put("x"); <string> f"mid:/a.txt".get();"#, &ns_owner, None)
			.await?;
		assert_eq!(err(&out, 0), None, "a namespace owner writes its namespace's bucket");
		let out = ds.execute(r#"<string> f"top:/a.txt".get();"#, &Session::default(), None).await?;
		assert!(err(&out, 0).is_some(), "a guest at the root reads no root bucket");
		Ok(())
	}
}
