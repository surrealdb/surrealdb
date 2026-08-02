//! DiskANN behaviour observed through the query driver.

use std::sync::Arc;

use anyhow::Result;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::{NewPlannerStrategy, Session};
use crate::key::schema::RecordKey;
use crate::kvs::{Datastore, TransactionType};
use crate::val::RecordIdKey;

/// DiskANN counterpart of the HNSW filtered-KNN batching test. A filtered KNN
/// evaluates the residual `WHERE` against each visited candidate's record; the
/// prefetch batches those fetches so the search issues far fewer KV *get
/// operations* than the records it reads (`ops_get` well below `keys_read`).
///
/// It also prints the committed-path over-fetch under a NON-selective filter:
/// the candidate list is prefetched up front, but the result builder fills
/// quickly so most candidates are never evaluated — the gap between
/// `keys_read` and the rows actually needed quantifies the over-fetch the
/// windowed prefetch bounds. DiskANN graph construction is deterministic, so
/// no build seed is needed; the assertions are structural so they hold for any
/// graph.
#[tokio::test(flavor = "multi_thread")]
async fn test_diskann_filtered_knn_batches_record_fetches() -> Result<()> {
	let ds = Arc::new(Datastore::new("memory").await?);
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let session = Session::owner()
		.with_ns("test")
		.with_db("test")
		.new_planner_strategy(NewPlannerStrategy::AllReadOnlyStatements);

	// 500 deterministic 8-d points with a low-cardinality `category`, plus a
	// DiskANN index. A selective filter makes the search visit many candidates
	// before finding K matches — the case batching helps.
	let n = 500u32;
	let cats = 20u32;
	let mut setup = String::from(
		"DEFINE INDEX emb ON pts FIELDS vec DISKANN DIMENSION 8 DIST EUCLIDEAN TYPE F32;\n",
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

	// Run a query on an owned read transaction so we can read its KV metrics.
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

	// Selective filter (category = 7, 1-in-20).
	let selective = "SELECT id FROM pts \
		WHERE vec <|10,400|> [0.5f,0.5f,0.5f,0.5f,0.5f,0.5f,0.5f,0.5f] AND category = 7;";

	// Before compaction the data is in the pending set (`search_pendings`).
	let (pending_len, pending_m) = run(&ds, &session, selective).await?;
	eprintln!(
		"PENDING      ops_get={} keys_read={} value_bytes_read={} results={pending_len}",
		pending_m.ops_get, pending_m.keys_read, pending_m.value_bytes_read
	);

	// Compact into the committed graph, then query again (`search_graph`).
	Datastore::index_compaction(
		Arc::clone(&ds),
		std::time::Duration::from_secs(1),
		tokio_util::sync::CancellationToken::new(),
	)
	.await?;
	let (committed_len, committed_m) = run(&ds, &session, selective).await?;
	eprintln!(
		"COMMITTED    ops_get={} keys_read={} value_bytes_read={} results={committed_len}",
		committed_m.ops_get, committed_m.keys_read, committed_m.value_bytes_read
	);

	// Non-selective filter (category < 10, ~50%) with small k: the builder
	// fills early so most prefetched candidates are never evaluated. Prints the
	// over-fetch (keys_read >> rows needed) that the windowed prefetch bounds.
	let nonselective = "SELECT id FROM pts \
		WHERE vec <|5,400|> [0.5f,0.5f,0.5f,0.5f,0.5f,0.5f,0.5f,0.5f] AND category < 10;";
	let (ns_len, ns_m) = run(&ds, &session, nonselective).await?;
	eprintln!(
		"NONSELECTIVE ops_get={} keys_read={} value_bytes_read={} results={ns_len}",
		ns_m.ops_get, ns_m.keys_read, ns_m.value_bytes_read
	);

	// Both selective paths return K matching records...
	assert_eq!(pending_len, 10, "pending filtered KNN should return K matches");
	assert_eq!(committed_len, 10, "committed filtered KNN should return K matches");
	// ...and both batch their record fetches: one-per-get gives `ops_get` ~
	// `keys_read`; batching pulls `ops_get` well below it.
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
	assert_eq!(ns_len, 5, "non-selective filtered KNN should return K matches");
	// The windowed prefetch bounds the committed-path over-fetch: a
	// non-selective filter fills the result builder inside the first window,
	// so the search stops fetching once the distance gate closes and reads far
	// fewer keys than a full candidate-list walk (the selective query above,
	// whose builder rarely fills). Before windowing the non-selective path
	// prefetched ~all candidates (measured keys_read ~4x today's); this guards
	// against reintroducing that without pinning a golden number.
	assert!(
		ns_m.keys_read * 4 < committed_m.keys_read,
		"windowed prefetch should bound non-selective over-fetch: \
		 non-selective keys_read={} vs selective keys_read={}",
		ns_m.keys_read,
		committed_m.keys_read
	);
	Ok(())
}

/// C4: a committed candidate whose underlying record row is missing (deleted
/// out from under a not-yet-recompacted graph) must be skipped — `is_record_truthy`
/// already returns not-truthy for a nullish record, and `prefetch_records` marks
/// such ids not-found during the batch warm so the eval loop never issues a
/// redundant per-candidate `get_record`. Here we force the scenario by deleting
/// a record's KV row directly (bypassing the index, which would otherwise drop
/// the graph entry too) and assert the filtered KNN excludes it and backfills.
#[tokio::test(flavor = "multi_thread")]
async fn test_diskann_filtered_knn_skips_missing_record() -> Result<()> {
	let ds = Arc::new(Datastore::new("memory").await?);
	let db_def = {
		let tx = ds.transaction(TransactionType::Write).await?;
		let db = tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
		db
	};
	let session = Session::owner()
		.with_ns("test")
		.with_db("test")
		.new_planner_strategy(NewPlannerStrategy::AllReadOnlyStatements);

	// 1-D points 10,20,…,120 with alternating category; "a" selects the odd-id
	// points (10,30,50,…). Nearest "a" matches to query [0] are pts:1 (10),
	// pts:3 (30), pts:5 (50).
	let mut setup = String::from(
		"DEFINE INDEX pt ON pts FIELDS point DISKANN DIMENSION 1 DIST EUCLIDEAN TYPE F32;\n",
	);
	for i in 1..=12u32 {
		let cat = if i % 2 == 1 {
			"a"
		} else {
			"b"
		};
		setup.push_str(&format!("CREATE pts:{i} SET point = [{}f], category = '{cat}';\n", i * 10));
	}
	for response in ds.execute(&setup, &session, None).await? {
		response.result?;
	}

	// Compact so the search runs over the committed graph (`search_graph`).
	Datastore::index_compaction(
		Arc::clone(&ds),
		std::time::Duration::from_secs(1),
		tokio_util::sync::CancellationToken::new(),
	)
	.await?;

	// Delete pts:1's record ROW at the KV layer, leaving the graph entry
	// intact — the committed candidate now resolves to a missing record.
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		let tb = surrealdb_strand::TableName::from("pts");
		let key = RecordKey {
			ns: db_def.namespace_id,
			db: db_def.database_id,
			tb: std::borrow::Cow::Borrowed(&tb),
			id: std::borrow::Cow::Owned(RecordIdKey::Number(1)),
		};
		tx.del_key(&key).await?;
		tx.commit().await?;
	}

	// Top-2 "a" matches to [0]: pts:1 (distance 10) is now missing, so the
	// result must skip it and backfill with pts:3 (30) then pts:5 (50) — and
	// must not error. If the missing record leaked in, the nearest distance
	// would be 10.
	let query = "SELECT VALUE vector::distance::knn() FROM pts \
		WHERE point <|2,40|> [0f] AND category = 'a';";
	let mut dists: Vec<f64> =
		ds.execute(query, &session, None).await?.remove(0).result?.into_t::<Vec<f64>>()?;
	dists.sort_by(f64::total_cmp);
	assert_eq!(
		dists,
		vec![30.0, 50.0],
		"missing pts:1 (dist 10) must be excluded and backfilled, got {dists:?}"
	);
	Ok(())
}
