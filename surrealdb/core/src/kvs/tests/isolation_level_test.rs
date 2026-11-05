//! `BEGIN ISOLATION LEVEL { SNAPSHOT | SERIALIZABLE }`: `SNAPSHOT` is the default
//! on every backend, and any other level needs a backend that offers it. The
//! memory backend offers both; a wrapper that hides its handle stands for a
//! backend that offers `SNAPSHOT` alone.

use std::any::{Any, TypeId};
use std::sync::{Arc, Mutex};

use common::future::BoxFut;
use surrealdb_kvs::{
	Isolation, IsolationLevels, IsolationLevelsHandle, Metrics, Result as KvsResult, Transactable,
	TransactionBuilder, TransactionType,
};
use tokio_util::sync::CancellationToken;

use crate::CommunityComposer;
use crate::catalog::providers::CatalogProvider;
use crate::dbs::Session;
use crate::kvs::{Datastore, TransactionBuilderFactory, TransactionBuilderParts};

fn failed(results: &[crate::dbs::QueryResult]) -> Option<String> {
	results.iter().find_map(|r| r.result.as_ref().err().map(|e| e.to_string()))
}

async fn setup(ds: &Datastore) -> Session {
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	tx.ensure_ns_db(None, "test", "test").await.unwrap();
	tx.commit().await.unwrap();
	Session::owner().with_ns("test").with_db("test")
}

#[tokio::test]
async fn the_memory_backend_offers_serializable() {
	let ds = Datastore::new("memory").await.unwrap();
	let ses = setup(&ds).await;
	for sql in [
		"BEGIN ISOLATION LEVEL SERIALIZABLE; RETURN 1; COMMIT;",
		"BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY; RETURN 1; COMMIT;",
	] {
		let results = ds.execute(sql, &ses, None).await.unwrap();
		assert_eq!(failed(&results), None, "{sql}");
	}
}

#[tokio::test]
async fn snapshot_is_the_default_and_serializable_needs_a_backend_that_offers_it() {
	let slot = Arc::new(Mutex::new(None));
	let ds = Datastore::builder()
		.build_with_factory_path("memory", LevelsComposer(Arc::clone(&slot), false))
		.await
		.unwrap();
	let ses = setup(&ds).await;
	for sql in [
		"BEGIN; RETURN 1; COMMIT;",
		"BEGIN ISOLATION LEVEL SNAPSHOT; RETURN 1; COMMIT;",
		"BEGIN ISOLATION LEVEL SNAPSHOT READ ONLY; RETURN 1; COMMIT;",
	] {
		let results = ds.execute(sql, &ses, None).await.unwrap();
		assert_eq!(failed(&results), None, "{sql}");
	}
	let results = ds
		.execute("BEGIN ISOLATION LEVEL SERIALIZABLE; RETURN 1; COMMIT;", &ses, None)
		.await
		.unwrap();
	let message = failed(&results).expect("SERIALIZABLE is refused");
	assert!(
		message.contains("SERIALIZABLE")
			&& message.contains("not available")
			&& message.contains("SNAPSHOT"),
		"{message}"
	);
}

/// A backend that offers both levels and records the level each transaction was
/// opened at.
struct Levels {
	inner: Arc<dyn TransactionBuilder>,
	opened: Mutex<Vec<(bool, Isolation)>>,
}

impl IsolationLevels for Levels {
	fn supported(&self) -> &'static [Isolation] {
		&[Isolation::Snapshot, Isolation::Serializable]
	}

	fn open(
		&self,
		write: TransactionType,
		isolation: Isolation,
	) -> BoxFut<'_, KvsResult<(Box<dyn Transactable>, bool)>> {
		Box::pin(async move {
			self.opened.lock().unwrap().push((matches!(write, TransactionType::Write), isolation));
			self.inner.new_transaction(write).await
		})
	}
}

struct LevelsBuilder {
	inner: Arc<dyn TransactionBuilder>,
	levels: Arc<Levels>,
	/// Whether the backend publishes the handle. When it does not, the wrapped
	/// backend's own handle is hidden too.
	offer: bool,
}

impl TransactionBuilder for LevelsBuilder {
	fn name(&self) -> &'static str {
		self.inner.name()
	}

	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, KvsResult<(Box<dyn Transactable>, bool)>> {
		self.inner.new_transaction(write)
	}

	fn shutdown(&self) -> BoxFut<'_, KvsResult<()>> {
		self.inner.shutdown()
	}

	fn register_metrics(&self) -> Option<Metrics> {
		self.inner.register_metrics()
	}

	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.inner.collect_u64_metric(metric)
	}

	fn wait_until_serve_ready(&self) -> BoxFut<'_, KvsResult<()>> {
		self.inner.wait_until_serve_ready()
	}

	fn extension(&self, id: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
		if id != TypeId::of::<IsolationLevelsHandle>() {
			return self.inner.extension(id);
		}
		self.offer.then(|| {
			Arc::new(IsolationLevelsHandle(Arc::clone(&self.levels) as Arc<dyn IsolationLevels>))
				as Arc<dyn Any + Send + Sync>
		})
	}
}

struct LevelsComposer(Arc<Mutex<Option<Arc<Levels>>>>, bool);

impl TransactionBuilderFactory for LevelsComposer {
	type RouterState = ();

	async fn new_transaction_builder(
		&self,
		path: &str,
		canceller: CancellationToken,
		config: surrealdb_cnf::ConfigMap,
	) -> anyhow::Result<TransactionBuilderParts<Self::RouterState>> {
		let parts = CommunityComposer().new_transaction_builder(path, canceller, config).await?;
		let inner: Arc<dyn TransactionBuilder> = Arc::from(parts.builder);
		let levels = Arc::new(Levels {
			inner: Arc::clone(&inner),
			opened: Mutex::new(Vec::new()),
		});
		*self.0.lock().unwrap() = Some(Arc::clone(&levels));
		Ok(TransactionBuilderParts::without_router_state(Box::new(LevelsBuilder {
			inner,
			levels,
			offer: self.1,
		})))
	}

	fn path_valid(&self, v: &str) -> anyhow::Result<String> {
		CommunityComposer().path_valid(v)
	}
}

#[tokio::test]
async fn a_backend_that_offers_the_level_opens_the_transaction_at_it() {
	let slot = Arc::new(Mutex::new(None));
	let ds = Datastore::builder()
		.build_with_factory_path("memory", LevelsComposer(Arc::clone(&slot), true))
		.await
		.unwrap();
	let levels = slot.lock().unwrap().take().unwrap();
	let ses = setup(&ds).await;
	levels.opened.lock().unwrap().clear();

	for sql in [
		"BEGIN ISOLATION LEVEL SERIALIZABLE; RETURN 1; COMMIT;",
		"BEGIN; RETURN 1; COMMIT;",
		"BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY; RETURN 1; COMMIT;",
	] {
		let results = ds.execute(sql, &ses, None).await.unwrap();
		assert_eq!(failed(&results), None, "{sql}");
	}
	let opened = levels.opened.lock().unwrap().clone();
	assert!(opened.contains(&(true, Isolation::Serializable)), "{opened:?}");
	assert!(opened.contains(&(true, Isolation::Snapshot)), "{opened:?}");
	assert!(opened.contains(&(false, Isolation::Serializable)), "{opened:?}");

	// A serializable read must not come from a snapshot older than the data it
	// has to conflict with.
	let results = ds
		.execute(
			"BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY MAX STALENESS 5s; RETURN 1; COMMIT;",
			&ses,
			None,
		)
		.await
		.unwrap();
	let message = failed(&results).expect("refused");
	assert!(message.contains("cannot be combined with a MAX STALENESS"), "{message}");
}
