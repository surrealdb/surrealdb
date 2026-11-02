//! Two nodes of a cluster over one store score a full-text query as the store
//! holds it, whatever each node has cached and whichever node wrote, compacted
//! or redefined the index.
//!
//! Each node keeps its own full-text cache ([`crate::idx::ft::cache`]), keyed by
//! the state a read-only snapshot reads: the table's catalog version and the
//! index's `!tv` generation. Every step below changes that state from the
//! other node, and every check compares both nodes against the same query in a
//! write transaction on a node built for that check, which reads around every
//! cache and holds no compiled statement.
//!
//! A write transaction on an index that keeps its postings in blocks scores
//! compacted documents from those blocks too, so the steps run twice: on a
//! store at the Legacy level, whose indexes keep one bitmap per term and score
//! every document from its postings, and on one above it, whose indexes keep
//! blocks. Every check's answer must be the same in both runs.
//!
//! The steps are run over one in-memory store shared by the datastores, and
//! again over TiKV in the lane that has a cluster.

use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::api::{BoxFut, ScanChunkStats};
use surrealdb_kvs::err::Result as KvsResult;
use surrealdb_kvs::snapshot::ViewCapabilities;
use surrealdb_kvs::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use surrealdb_kvs::{
	Batch, CountCost, Direction, Estimate, GetMultiResult, Key, KeyRange, KeysResult, Metrics,
	RangeKeyVisitor, ScanCursorKeys, ScanCursorVals, ScanResult, Snapshot, SplitTarget,
	Transactable, TransactionBuilder, TransactionType, Val,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::Session;
use crate::kvs::Datastore;
use crate::kvs::ds::{TransactionBuilderFactory, TransactionBuilderParts};

/// One backend handed to every datastore built through [`SharedBackend`].
///
/// Shutting a datastore down leaves the backend open: it is shared, so the
/// first datastore to drop would otherwise close it under the others. Its
/// transactions are [`SharedTx`]s.
struct SharedBuilder(Arc<dyn TransactionBuilder>);

/// A transaction of a store several datastores write, as TiKV's are: it
/// answers every read and write as the store's own transaction does, but never
/// claims its datastore is the store's sole writer
/// ([`Transactable::sole_writer`]). A datastore that did would serve the
/// catalog from what its own commits told it, and never see another node's
/// definitions.
struct SharedTx(Box<dyn Transactable>);

impl Transactable for SharedTx {
	fn kind(&self) -> &'static str {
		self.0.kind()
	}
	fn sole_writer(&self) -> bool {
		false
	}
	fn validates_reads(&self) -> bool {
		self.0.validates_reads()
	}
	fn parallel_count(&self) -> bool {
		self.0.parallel_count()
	}
	fn bounds_write_buffer(&self) -> bool {
		self.0.bounds_write_buffer()
	}
	fn count_cost(&self) -> CountCost {
		self.0.count_cost()
	}
	fn closed(&self) -> bool {
		self.0.closed()
	}
	fn writeable(&self) -> bool {
		self.0.writeable()
	}
	fn cancel<'a>(&'a self) -> BoxFut<'a, KvsResult<()>> {
		self.0.cancel()
	}
	fn commit<'a>(&'a self) -> BoxFut<'a, KvsResult<()>> {
		self.0.commit()
	}
	fn commit_unsynced<'a>(&'a self) -> BoxFut<'a, KvsResult<()>> {
		self.0.commit_unsynced()
	}
	fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, KvsResult<bool>> {
		self.0.exists(key, version)
	}
	fn get<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, KvsResult<Option<Val>>> {
		self.0.get(key, version)
	}
	fn getu<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, KvsResult<Option<Val>>> {
		self.0.getu(key)
	}
	fn shared_locked_reads(&self) -> bool {
		self.0.shared_locked_reads()
	}
	fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
		self.0.set(key, val)
	}
	fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
		self.0.put(key, val)
	}
	fn putc<'a>(&'a self, key: Key<'a>, val: Val, chk: Option<Val>) -> BoxFut<'a, KvsResult<()>> {
		self.0.putc(key, val, chk)
	}
	fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, KvsResult<()>> {
		self.0.del(key)
	}
	fn delc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, KvsResult<()>> {
		self.0.delc(key, chk)
	}
	fn keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<KeysResult>> {
		self.0.keys(rng, limit, skip, version)
	}
	fn keysr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<KeysResult>> {
		self.0.keysr(rng, limit, skip, version)
	}
	fn scan<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<ScanResult>> {
		self.0.scan(rng, limit, skip, version)
	}
	fn scanr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<ScanResult>> {
		self.0.scanr(rng, limit, skip, version)
	}
	fn open_keys_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Box<dyn ScanCursorKeys + 'a>>> {
		self.0.open_keys_cursor(rng, dir, skip, version)
	}
	fn visit_key_ranges<'a>(
		&'a self,
		ranges: &'a [KeyRange<'a>],
		limit: u32,
		version: Option<u64>,
		f: &'a mut dyn RangeKeyVisitor,
	) -> BoxFut<'a, KvsResult<ScanChunkStats>> {
		self.0.visit_key_ranges(ranges, limit, version, f)
	}
	fn open_vals_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Box<dyn ScanCursorVals + 'a>>> {
		self.0.open_vals_cursor(rng, dir, skip, version)
	}
	fn replace<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
		self.0.replace(key, val)
	}
	fn clr<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, KvsResult<()>> {
		self.0.clr(key)
	}
	fn clrc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, KvsResult<()>> {
		self.0.clrc(key, chk)
	}
	fn getm<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<GetMultiResult>> {
		self.0.getm(keys, version)
	}
	fn getr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<ScanResult>> {
		self.0.getr(rng, version)
	}
	fn delr<'a>(&'a self, rng: KeyRange<'a>) -> BoxFut<'a, KvsResult<()>> {
		self.0.delr(rng)
	}
	fn clrr<'a>(&'a self, rng: KeyRange<'a>) -> BoxFut<'a, KvsResult<()>> {
		self.0.clrr(rng)
	}
	fn count<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<usize>> {
		self.0.count(rng, version)
	}
	fn count_bounded<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
		budget: u32,
	) -> BoxFut<'a, KvsResult<(usize, Option<Key<'static>>)>> {
		self.0.count_bounded(rng, version, budget)
	}
	fn snapshot(&self) -> Option<Arc<dyn Snapshot>> {
		self.0.snapshot()
	}
	fn view_capabilities(&self) -> ViewCapabilities {
		self.0.view_capabilities()
	}
	fn plain_reads_tracked(&self) -> bool {
		self.0.plain_reads_tracked()
	}
	fn scan_read_ahead(&self) -> bool {
		self.0.scan_read_ahead()
	}
	fn multi_read(&self) -> bool {
		self.0.multi_read()
	}
	fn estimate<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Estimate>> {
		self.0.estimate(rng, version)
	}
	fn split<'a>(
		&'a self,
		rng: KeyRange<'a>,
		target: SplitTarget,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Vec<Key<'static>>>> {
		self.0.split(rng, target, version)
	}
	fn batch_keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		batch: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Batch<Vec<u8>>>> {
		self.0.batch_keys(rng, batch, version)
	}
	fn batch_keys_vals<'a>(
		&'a self,
		rng: KeyRange<'a>,
		batch: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<Batch<(Vec<u8>, Vec<u8>)>>> {
		self.0.batch_keys_vals(rng, batch, version)
	}
	fn new_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
		self.0.new_save_point()
	}
	fn release_last_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
		self.0.release_last_save_point()
	}
	fn rollback_to_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
		self.0.rollback_to_save_point()
	}
	fn timestamp(&self) -> BoxFut<'_, KvsResult<BoxTimeStamp>> {
		self.0.timestamp()
	}
	fn safe_timestamp(&self) -> BoxFut<'_, KvsResult<BoxTimeStamp>> {
		self.0.safe_timestamp()
	}
	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		self.0.timestamp_impl()
	}
	fn version_impl(&self) -> BoxTimeStampImpl {
		self.0.version_impl()
	}
	fn compact<'a>(&'a self, range: Option<KeyRange<'a>>) -> BoxFut<'a, KvsResult<()>> {
		self.0.compact(range)
	}
}

impl TransactionBuilder for SharedBuilder {
	fn name(&self) -> &'static str {
		self.0.name()
	}

	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, surrealdb_kvs::Result<(Box<dyn Transactable>, bool)>> {
		Box::pin(async move {
			let (tx, flag) = self.0.new_transaction(write).await?;
			Ok((Box::new(SharedTx(tx)) as Box<dyn Transactable>, flag))
		})
	}

	fn shutdown(&self) -> BoxFut<'_, surrealdb_kvs::Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn register_metrics(&self) -> Option<Metrics> {
		self.0.register_metrics()
	}

	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.0.collect_u64_metric(metric)
	}

	fn wait_until_serve_ready(&self) -> BoxFut<'_, surrealdb_kvs::Result<()>> {
		self.0.wait_until_serve_ready()
	}

	fn extension(&self, id: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
		self.0.extension(id)
	}
}

/// A composer handing every datastore it builds the same backend.
pub(super) struct SharedBackend(Arc<dyn TransactionBuilder>);

impl SharedBackend {
	#[cfg(feature = "kv-mem")]
	pub(super) async fn memory() -> Self {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder("memory", CancellationToken::new(), ConfigMap::empty())
			.await
			.expect("an in-memory backend");
		Self(Arc::from(builder))
	}
}

impl TransactionBuilderFactory for SharedBackend {
	type RouterState = ();

	async fn new_transaction_builder(
		&self,
		_path: &str,
		_canceller: CancellationToken,
		_config: ConfigMap,
	) -> anyhow::Result<TransactionBuilderParts<()>> {
		Ok(TransactionBuilderParts::without_router_state(Box::new(SharedBuilder(Arc::clone(
			&self.0,
		)))))
	}

	fn path_valid(&self, v: &str) -> anyhow::Result<String> {
		Ok(v.to_owned())
	}
}

/// A node: a datastore of its own, with its own node id, caches and tasks.
/// Where `legacy`, it keeps a store at the Legacy level there, records no level
/// of its own on a store it finds new, and opts in to upgrading a store a 3.x
/// release wrote.
#[cfg(feature = "kv-mem")]
pub(super) async fn node(backend: &SharedBackend, legacy: bool) -> Arc<Datastore> {
	let builder = Datastore::builder().with_id(Uuid::new_v4()).without_maintenance_tasks();
	let builder = if legacy {
		builder.with_legacy_feature_level().without_startup_transactions().with_config(
			surrealdb_cnf::ConfigMap::empty()
				.with_key_value("experimental_upgrade_from_3x", "true"),
		)
	} else {
		builder
	};
	builder
		.build_with_factory_path("memory", SharedBackend(Arc::clone(&backend.0)))
		.await
		.expect("a node over the shared backend")
}

/// Records on `ds`'s store that a 3.3 release wrote it, so a node that keeps
/// a store at the Legacy level finds it there.
async fn upgraded(ds: &Datastore) {
	use crate::key::schema::{StorageVersionKey, VersionKey};
	use crate::kvs::version::{MajorVersion, StorageVersion};

	let txn = ds.transaction(TransactionType::Write).await.expect("a write transaction");
	txn.set_key(&VersionKey {}, &MajorVersion::latest()).await.expect("the version");
	let storage = StorageVersion::parse("3.3.0").expect("a storage version");
	txn.set_key(&StorageVersionKey {}, &storage).await.expect("the storage version");
	txn.commit().await.expect("commit the versions");
	ds.check_version().await.expect("the store opens");
	assert!(ds.feature_level().await.expect("a level").level.is_legacy(), "the store left Legacy");
}

/// The format `d_body` on `d` is stamped with.
async fn stamped(ds: &Datastore) -> u16 {
	use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};

	let tx = ds.transaction(TransactionType::Read).await.expect("a read transaction");
	let ns = tx.get_ns_by_name("test", None).await.expect("a read").expect("the namespace");
	let db = tx.get_db_by_name("test", "test", None).await.expect("a read").expect("the database");
	let tb: crate::val::TableName = "d".into();
	let index = tx
		.get_tb_index(ns.namespace_id, db.database_id, &tb, "d_body", None)
		.await
		.expect("a read")
		.expect("the index");
	tx.cancel().await.expect("cancel");
	index.format_version
}

fn owner() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// Runs `sql` on `ds`, failing on any statement that fails.
async fn run(ds: &Datastore, sql: &str) {
	for response in ds.execute(sql, &owner(), None).await.expect("the query runs") {
		response.result.unwrap_or_else(|e| panic!("{sql}: {e}"));
	}
}

/// Folds every term change into the index on `ds`'s side of the store.
async fn compact(ds: &Arc<Datastore>) {
	Datastore::index_compaction(
		Arc::clone(ds),
		std::time::Duration::from_secs(1),
		CancellationToken::new(),
	)
	.await
	.expect("compaction runs");
}

/// The scored query every check runs, in full and bounded by a score sort.
const SCORED: &str = "SELECT id, search::score(1) AS s FROM d WHERE body @1@ 'hello'";
const TOP: &str = "SELECT id, search::score(1) AS s FROM d WHERE body @1@ 'hello' \
	ORDER BY s DESC LIMIT 3";

/// Each row's score by its record id, from the last row set `sql` answers on
/// the node `who` names.
async fn scores(who: &str, ds: &Datastore, sql: &str) -> BTreeMap<String, String> {
	let responses = ds.execute(sql, &owner(), None).await.expect("the query runs");
	let rows = responses
		.into_iter()
		.map(|response| response.result.unwrap_or_else(|e| panic!("{who}: {sql}: {e}")))
		.filter(|rows| matches!(rows, surrealdb_types::Value::Array(_)))
		.last()
		.expect("the select's rows");
	let surrealdb_types::Value::Array(rows) = rows else {
		unreachable!("filtered to arrays");
	};
	rows.iter()
		.map(|row| {
			let surrealdb_types::Value::Object(row) = row else {
				panic!("a row is an object: {row:?}");
			};
			(format!("{:?}", row.get("id")), format!("{:?}", row.get("s")))
		})
		.collect()
}

/// What the store holds: `sql` on a node that has served nothing before, in a
/// write transaction, which no node's cache serves.
async fn stored(oracle: &Datastore, sql: &str) -> BTreeMap<String, String> {
	scores("the oracle", oracle, &format!("BEGIN; UPSERT oracle:1; {sql}; COMMIT;")).await
}

/// Both nodes answer the scored query, in full and bounded, as a node of its
/// own, new to the store, answers it; that answer is added to `answers`.
async fn agree<O, F>(
	step: &str,
	a: &Datastore,
	b: &Datastore,
	oracle: &O,
	answers: &mut Vec<BTreeMap<String, String>>,
) where
	O: Fn() -> F,
	F: Future<Output = Arc<Datastore>>,
{
	for sql in [SCORED, TOP] {
		let expected = stored(&*oracle().await, sql).await;
		answers.push(expected.clone());
		assert_eq!(
			scores(&format!("{step}: node A"), a, sql).await,
			expected,
			"{step}: node A, {sql}"
		);
		assert_eq!(
			scores(&format!("{step}: node B"), b, sql).await,
			expected,
			"{step}: node B, {sql}"
		);
	}
}

const SCHEMA: &str = "DEFINE ANALYZER IF NOT EXISTS simple TOKENIZERS blank FILTERS lowercase; \
	DEFINE TABLE d SCHEMALESS; \
	DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25;";

/// `count` documents, `hello` in a third of them at a frequency and in a body
/// of a length of its own, the rest filler.
fn documents(count: usize, offset: usize) -> String {
	let mut sql = String::new();
	for i in 0..count {
		let body = if i % 3 == 0 {
			format!("{}{}", "hello ".repeat(1 + i % 4), "x ".repeat(i % 7))
		} else {
			format!("some other words {}", "y ".repeat(i % 5))
		};
		sql.push_str(&format!("CREATE d:{} SET body = '{}';", offset + i, body.trim()));
	}
	sql
}

/// The steps, run over the store `a`, `b` and the nodes `oracle` builds share,
/// whose indexes are stamped with `format`: every check's answer, in order.
async fn nodes_score_as_the_store_holds_it<O, F>(
	a: Arc<Datastore>,
	b: Arc<Datastore>,
	oracle: O,
	format: u16,
) -> Vec<BTreeMap<String, String>>
where
	O: Fn() -> F,
	F: Future<Output = Arc<Datastore>>,
{
	let mut answers = Vec::new();
	for ds in [&a, &b] {
		let txn = ds.transaction(TransactionType::Write).await.expect("a write transaction");
		txn.ensure_ns_db(None, "test", "test").await.expect("ensure test/test");
		txn.commit().await.expect("commit ns/db");
	}

	// A indexes and folds; B's first scored queries fill B's caches.
	run(&a, SCHEMA).await;
	assert_eq!(stamped(&a).await, format, "the index's format");
	run(&a, &documents(30, 0)).await;
	compact(&a).await;
	agree("after A indexed and compacted", &a, &b, &oracle, &mut answers).await;

	// B rewrites a document B has cached: the same length, the query term at
	// another frequency.
	run(&b, "UPDATE d:0 SET body = 'hello hello hello world'").await;
	run(&b, "UPDATE d:3 SET body = 'hello world world world'").await;
	agree("after B rewrote cached documents", &a, &b, &oracle, &mut answers).await;

	// A folds the rewrites, which moves the generation B's caches are keyed by.
	compact(&a).await;
	agree("after A compacted B's rewrites", &a, &b, &oracle, &mut answers).await;

	// A drops and defines the index again under the same name, with other
	// content behind it: the index id is reused and the generation restarts.
	run(&a, "REMOVE INDEX d_body ON d").await;
	run(&a, "UPDATE d:6 SET body = 'hello'; DELETE d:9;").await;
	run(&a, "DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25").await;
	compact(&a).await;
	agree("after A redefined the index", &a, &b, &oracle, &mut answers).await;

	// A drops the whole table and builds it again with other documents.
	run(&a, "REMOVE TABLE d").await;
	run(&a, SCHEMA).await;
	assert_eq!(stamped(&a).await, format, "the index's format");
	run(&a, &documents(24, 100)).await;
	compact(&a).await;
	agree("after A redefined the table", &a, &b, &oracle, &mut answers).await;

	// Both nodes write, rewriting compacted documents among others, while both
	// query, then the store is folded.
	let writer = |ds: Arc<Datastore>, offset: usize| async move {
		for i in 0..12 {
			let id = offset + i;
			// Each node deletes compacted documents and rewrites others, which
			// the other node neither deletes nor rewrites, so the end state is
			// the same however the two interleave.
			let sql = if i % 4 == 3 {
				format!("DELETE d:{};", 100 + (id % 24))
			} else if i % 4 == 2 {
				format!("UPDATE d:{} SET body = 'hello hello world';", 100 + (id % 24))
			} else {
				format!("UPSERT d:{id} SET body = '{}';", "hello ".repeat(1 + i % 3).trim())
			};
			// Two nodes writing one index can conflict; a conflict is retried.
			let mut attempts = 0;
			while let Some(e) = ds
				.execute(&sql, &owner(), None)
				.await
				.expect("the query runs")
				.into_iter()
				.find_map(|response| response.result.err())
			{
				attempts += 1;
				assert!(attempts < 16, "{sql}: {e}");
			}
			let _ = scores("a writing node", &ds, SCORED).await;
		}
	};
	tokio::join!(writer(Arc::clone(&a), 200), writer(Arc::clone(&b), 300));
	compact(&b).await;
	compact(&a).await;
	agree("after both nodes wrote and queried", &a, &b, &oracle, &mut answers).await;
	answers
}

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn two_nodes_over_one_memory_store_score_as_it_holds() {
	let bitmaps = {
		let backend = SharedBackend::memory().await;
		let a = node(&backend, true).await;
		upgraded(&a).await;
		let b = node(&backend, true).await;
		let format = crate::catalog::LEGACY_LEVEL_INDEX_FORMAT_VERSION;
		nodes_score_as_the_store_holds_it(a, b, || node(&backend, true), format).await
	};
	let blocks = {
		let backend = SharedBackend::memory().await;
		let (a, b) = (node(&backend, false).await, node(&backend, false).await);
		let format = crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION;
		nodes_score_as_the_store_holds_it(a, b, || node(&backend, false), format).await
	};
	assert_eq!(blocks, bitmaps, "the block indexes answered as the bitmap indexes");
}
