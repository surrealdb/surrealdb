//! HNSW behaviour observed through the query driver.

use std::sync::Arc;

use anyhow::Result;
use test_log::test;

use crate::catalog::providers::{CatalogProvider, TableProvider};
use crate::dbs::Session;
use crate::idx::IndexKeyBase;
use crate::idx::trees::hnsw::HnswState;
use crate::kvs::{Datastore, TransactionType};

#[test(tokio::test(flavor = "multi_thread"))]
async fn test_hnsw_inner_product_smoke() -> Result<()> {
	let ds = Datastore::new("memory").await?;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let session = Session::owner().with_ns("test").with_db("test");
	let sql = "
		DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 2 DIST INNER_PRODUCT TYPE F32 EFC 100 M 12;
		CREATE pts:1 SET point = [1f, 0f];
		CREATE pts:2 SET point = [2f, 0f];
		CREATE pts:3 SET point = [0f, 1f];
	";
	for response in ds.execute(sql, &session, None).await? {
		response.result?;
	}

	let mut response =
		ds.execute("SELECT id FROM pts WHERE point <|2,40|> [1f, 0f];", &session, None).await?;
	let result = response.remove(0).result?;
	let surrealdb_types::Value::Array(result) = result else {
		panic!("Expected array result");
	};
	assert_eq!(result.len(), 2);
	Ok(())
}

/// A filtered KNN search evaluates the residual `WHERE` against each visited
/// candidate's record. This change batches those fetches — one multi-get per
/// neighbourhood for the committed graph (`search_with_filter`), and one
/// multi-get for the whole pending set (`search_pendings`) — so the search
/// issues far fewer KV *get operations* than the records it reads
/// (`ops_get` well below `keys_read`). The pre-batching code fetched one
/// record per get, so the two were ~equal. Results are unchanged.
///
/// The assertions are structural — K matches are found and `ops_get` sits well
/// below `keys_read` — so they hold for any graph; no fixed build seed is
/// needed, which keeps the test free of a process-global `set_var` (the seed
/// is exercised out-of-process by the benchmark harness instead).
#[test(tokio::test(flavor = "multi_thread"))]
async fn test_hnsw_filtered_knn_batches_record_fetches() -> Result<()> {
	let ds = Arc::new(Datastore::new("memory").await?);
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let session = Session::owner()
		.with_ns("test")
		.with_db("test")
		.new_planner_strategy(crate::dbs::NewPlannerStrategy::AllReadOnlyStatements);

	// 500 deterministic 8-d points with a selective `category` (1-in-20),
	// plus an HNSW index. A selective filter makes the search visit many
	// candidates before finding K matches — the case batching helps.
	let n = 500u32;
	let cats = 20u32;
	let mut setup = String::from(
		"DEFINE INDEX emb ON pts FIELDS vec HNSW DIMENSION 8 DIST EUCLIDEAN TYPE F32 EFC 200 M 12;\n",
	);
	for i in 0..n {
		let mut v = String::new();
		for j in 0..8u32 {
			if j > 0 {
				v.push_str(", ");
			}
			let f = ((i.wrapping_mul(7).wrapping_add(j.wrapping_mul(131))) % 1000) as f32 / 1000.0;
			v.push_str(&format!("{f}f"));
		}
		setup.push_str(&format!("CREATE pts:{i} SET vec = [{v}], category = {};\n", i % cats));
	}
	for response in ds.execute(&setup, &session, None).await? {
		response.result?;
	}

	let query = "SELECT id FROM pts \
		WHERE vec <|10,400|> [0.5f,0.5f,0.5f,0.5f,0.5f,0.5f,0.5f,0.5f] AND category = 7;";

	// Run the query on an owned read transaction so we can read its KV
	// metrics, returning (result_count, metrics).
	async fn run(
		ds: &Arc<Datastore>,
		session: &Session,
		query: &str,
	) -> Result<(usize, crate::observe::TransactionMetricsSnapshot)> {
		let tx = Arc::new(ds.transaction(TransactionType::Read).await?);
		let mut response =
			ds.execute_with_transaction(query, session, None, Arc::clone(&tx)).await?;
		let len = match response.remove(0).result? {
			surrealdb_types::Value::Array(a) => a.len(),
			_ => 0,
		};
		Ok((len, tx.metrics_snapshot_for_test()))
	}

	// Before compaction the data is in the pending set, searched by
	// `search_pendings` (batched here too).
	let (pending_len, pending_m) = run(&ds, &session, query).await?;
	eprintln!(
		"PENDING   ops_get={} keys_read={} results={pending_len}",
		pending_m.ops_get, pending_m.keys_read
	);

	// Compact pending updates into the committed graph, then query again —
	// now `search_with_filter` (per-neighbourhood batching) handles it.
	Datastore::index_compaction(
		Arc::clone(&ds),
		std::time::Duration::from_secs(1),
		tokio_util::sync::CancellationToken::new(),
	)
	.await?;
	let (committed_len, committed_m) = run(&ds, &session, query).await?;
	eprintln!(
		"COMMITTED ops_get={} keys_read={} results={committed_len}",
		committed_m.ops_get, committed_m.keys_read
	);

	// Both paths return the same K matching records...
	assert_eq!(pending_len, 10, "pending filtered KNN should return K matches");
	assert_eq!(committed_len, 10, "committed filtered KNN should return K matches");
	// ...and both batch their record fetches: one-per-get fetching gives
	// `ops_get` ~ `keys_read`; batching pulls `ops_get` well below it.
	assert!(
		u64::from(pending_m.ops_get) * 4 < pending_m.keys_read * 3,
		"pending path should batch: ops_get={} keys_read={}",
		pending_m.ops_get,
		pending_m.keys_read
	);
	assert!(
		u64::from(committed_m.ops_get) * 4 < committed_m.keys_read * 3,
		"committed path should batch: ops_get={} keys_read={}",
		committed_m.ops_get,
		committed_m.keys_read
	);
	Ok(())
}

#[test(tokio::test(flavor = "multi_thread"))]
async fn hnsw_blocking_define_index_compacts_pending_vectors() -> Result<()> {
	let ds = Datastore::new("memory").await?;
	let db = {
		let tx = ds.transaction(TransactionType::Write).await?;
		let db = tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
		db
	};
	let session = Session::owner().with_ns("test").with_db("test");
	let sql = "
		CREATE pts:1 SET point = [1f, 2f];
		CREATE pts:2 SET point = [2f, 3f];
		CREATE pts:3 SET point = [3f, 4f];
		DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 100 M 12;
	";
	for response in ds.execute(sql, &session, None).await? {
		response.result?;
	}

	let tx = ds.transaction(TransactionType::Read).await?;
	let tb = "pts".into();
	let ix =
		tx.get_tb_index(db.namespace_id, db.database_id, &tb, "hnsw_pts", None).await?.unwrap();
	let ikb = IndexKeyBase::new(db.namespace_id, db.database_id, tb, ix.index_id);
	let pending_records = tx.getr(ikb.new_hr_range()?, None).await?;
	let pending_appends = tx.getr_raw(ikb.new_hp_range()?, None).await?;
	let state: HnswState = tx.get_key(&ikb.new_hs_key(), None).await?.unwrap();
	tx.cancel().await?;

	assert!(pending_records.is_empty());
	assert!(pending_appends.is_empty());
	assert_eq!(state.next_element_id, 3);
	Ok(())
}

#[test(tokio::test(flavor = "multi_thread"))]
async fn hnsw_query_reads_record_keyed_pending_vectors() -> Result<()> {
	let ds = Datastore::new("memory").await?;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let session = Session::owner().with_ns("test").with_db("test");
	let sql = "
		DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 100 M 12;
		CREATE pts:1 SET point = [1f, 2f];
		CREATE pts:2 SET point = [2f, 3f];
		CREATE pts:3 SET point = [3f, 4f];
	";
	for response in ds.execute(sql, &session, None).await? {
		response.result?;
	}

	let mut response =
		ds.execute("SELECT id FROM pts WHERE point <|2,40|> [1f, 2f];", &session, None).await?;
	let result = response.remove(0).result?;
	let surrealdb_types::Value::Array(result) = result else {
		panic!("Expected array result");
	};
	assert_eq!(result.len(), 2);
	Ok(())
}
