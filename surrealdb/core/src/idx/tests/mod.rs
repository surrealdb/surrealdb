//! Index-engine tests that drive the engines through the datastore.
//!
//! The engines' own tests are white-box and live beside the code they exercise,
//! reaching an engine directly with nothing above it in scope. The tests here
//! are the opposite: they assert on engine behaviour reached through
//! `DEFINE INDEX` / `SELECT … <|k,ef|> …` and the compaction task, so a
//! `Datastore`, a `Session` and the query executor are the point rather than
//! scaffolding. They belong with the driver. The one exception drives the
//! search a statement's access path ends in directly, for a case no statement
//! reaches.

#[cfg(diskann)]
mod diskann_query_test;
mod hnsw_query_test;

/// Runs the ANN search a statement's access path ends in over a versioned
/// SurrealKV store, with a condition judged on each candidate inside the
/// search, as of the current state and as of a stamp, and asserts that the
/// search at the stamp reads each candidate record once.
///
/// A statement whose `VERSION` is known when it is planned is the brute-force
/// search over the records at the stamp, so none reaches this search at a
/// version; it is driven directly. Versioned reads bypass the transaction
/// record cache, so the records the search's prefetch reads must be the ones
/// its filter evaluates; a second, per-candidate read would double the record
/// keys the search reads. The same search at the current state reads each
/// record once through the cache. The versioned search additionally reads its
/// catalog at the version, so it may exceed the current count by at most 2K
/// keys: far below the hundreds a second read of every visited candidate adds.
#[cfg(feature = "kv-surrealkv")]
async fn assert_versioned_search_reads_each_candidate_once(index_ddl: &str) -> anyhow::Result<()> {
	use std::sync::Arc;

	use crate::catalog::Record;
	use crate::catalog::providers::{CatalogProvider, TableProvider};
	use crate::exec::operators::scan::knn_candidates;
	use crate::exec::operators::test_util::TestDb;
	use crate::idx::trees::KnnCondFilter;
	use crate::idx::trees::gate::{BoxGateFut, CachedTableSelect, CandidateCondition};
	use crate::kvs::{Datastore, TransactionType};
	use crate::val::{Datetime, Number, RecordId, TableName, Value};

	/// Admits the records of category 7.
	struct CategorySeven;

	impl CandidateCondition for CategorySeven {
		fn matches<'a>(
			&'a self,
			_rid: &'a Arc<RecordId>,
			record: &'a Arc<Record>,
		) -> BoxGateFut<'a> {
			Box::pin(async move {
				Ok(matches!(
					&record.data,
					Value::Object(object)
						if object.get("category") == Some(&Value::Number(Number::Int(7)))
				))
			})
		}
	}

	const K: u64 = 10;
	const ROWS: u32 = 500;
	let query: Vec<Number> = (0..8).map(|_| Number::Float(0.5)).collect();

	let dir = temp_dir::TempDir::new()?;
	let path = format!("surrealkv://{}?versioned=true&retention=1h", dir.path().to_string_lossy());
	// Compaction happens only where this test calls it.
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path(&path).await?;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let db = TestDb::over(Arc::clone(&ds));
	let mut setup = format!("{index_ddl}\n");
	for i in 0..ROWS {
		let mut v = String::new();
		for j in 0..8u32 {
			if j > 0 {
				v.push_str(", ");
			}
			let f = ((i.wrapping_mul(7).wrapping_add(j.wrapping_mul(131))) % 1000) as f32 / 1000.0;
			v.push_str(&format!("{f}f"));
		}
		setup.push_str(&format!("CREATE pts:{i} SET vec = [{v}], category = {};\n", i % 20));
	}
	db.run(&setup).await;
	Datastore::index_compaction(
		Arc::clone(&ds),
		std::time::Duration::from_secs(1),
		tokio_util::sync::CancellationToken::new(),
	)
	.await?;
	let stamp = Datetime::now();

	/// The records the search returns and the record keys it read, as of
	/// `stamp` or, with none, the current state.
	async fn search(
		db: &TestDb,
		query: &[Number],
		stamp: Option<Datetime>,
	) -> anyhow::Result<(usize, u64)> {
		let ctx = db.exec_ctx().await;
		let txn = ctx.txn();
		let (ns, db_def) = {
			let db_ctx = ctx.database()?;
			(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id)
		};
		let table = TableName::from("pts");
		let indexes = txn.all_tb_indexes(ns, db_def, &table, None).await?;
		let index = indexes.first().ok_or_else(|| anyhow::anyhow!("no index on pts"))?;
		let version = match stamp {
			Some(stamp) => Some(stamp.to_version_stamp(txn.version_impl().as_ref())?),
			None => None,
		};
		let filter = KnnCondFilter {
			select_gate: CachedTableSelect::Skip,
			cond: Some(Arc::new(CategorySeven)),
			metrics: None,
			version,
		};
		let found =
			knn_candidates(&ctx, &table, index, query, K as u32, 400, version, Some(filter), None)
				.await
				.map_err(|flow| {
					flow.into_error_or(|_| anyhow::anyhow!("the search signalled control flow"))
				})?;
		Ok((found.len(), txn.metrics_snapshot_for_test().keys_read))
	}

	// The first search loads the graph into the process-wide cache; warm it so
	// both measured runs read only candidate records and doc-id maps.
	search(&db, &query, None).await?;
	search(&db, &query, Some(stamp)).await?;
	let (current_len, current_keys) = search(&db, &query, None).await?;
	let (versioned_len, versioned_keys) = search(&db, &query, Some(stamp)).await?;
	assert_eq!(current_len as u64, K, "the filtered search should return K matches");
	assert_eq!(versioned_len as u64, K, "the versioned filtered search should return K matches");
	assert!(
		versioned_keys <= current_keys + 2 * K,
		"a versioned filtered search should read each candidate record once: \
		 versioned keys_read={versioned_keys}, current keys_read={current_keys}"
	);
	ds.shutdown().await?;
	Ok(())
}
