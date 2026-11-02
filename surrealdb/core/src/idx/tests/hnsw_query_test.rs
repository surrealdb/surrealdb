//! HNSW behaviour observed through the query driver.

use std::sync::Arc;

use anyhow::Result;
use test_log::test;

use crate::catalog::providers::{CatalogProvider, TableProvider};
use crate::dbs::Session;
use crate::idx::IndexKeyBase;
use crate::idx::trees::hnsw::HnswState;
use crate::kvs::{Datastore, QueryRequest, TransactionType};

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
/// candidate's record, and fetches those records in batches: one multi-get per
/// neighbourhood for the committed graph (`search_with_filter`), and one per
/// materialisation batch of the pending queue (`search_pendings`). Both paths
/// therefore issue far fewer KV *get operations* than the records they read —
/// `ops_get` well below `keys_read`, where one fetch per candidate would make
/// the two roughly equal.
///
/// The assertions are structural — K matches are found and `ops_get` sits well
/// below `keys_read` — so they hold for any graph; no fixed build seed is
/// needed, which keeps the test free of a process-global `set_var` (the seed
/// is exercised out-of-process by the benchmark harness instead).
#[test(tokio::test(flavor = "multi_thread"))]
async fn test_hnsw_filtered_knn_batches_record_fetches() -> Result<()> {
	// Compaction happens only where this test calls it: the assertions compare
	// the pending set against the compacted graph, which a background
	// compactor would fold away before the first phase reads it.
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let session = Session::owner().with_ns("test").with_db("test");

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
			ds.run(QueryRequest::new(query, session).with_transaction(Arc::clone(&tx))).await?;
		let len = match response.remove(0).result? {
			surrealdb_types::Value::Array(a) => a.len(),
			surrealdb_types::Value::None
			| surrealdb_types::Value::Null
			| surrealdb_types::Value::Bool(_)
			| surrealdb_types::Value::Number(_)
			| surrealdb_types::Value::String(_)
			| surrealdb_types::Value::Bytes(_)
			| surrealdb_types::Value::Duration(_)
			| surrealdb_types::Value::Datetime(_)
			| surrealdb_types::Value::Date(_)
			| surrealdb_types::Value::Time(_)
			| surrealdb_types::Value::Timestamp(_)
			| surrealdb_types::Value::Interval(_)
			| surrealdb_types::Value::Uuid(_)
			| surrealdb_types::Value::Geometry(_)
			| surrealdb_types::Value::Table(_)
			| surrealdb_types::Value::RecordId(_)
			| surrealdb_types::Value::File(_)
			| surrealdb_types::Value::Range(_)
			| surrealdb_types::Value::Regex(_)
			| surrealdb_types::Value::Object(_)
			| surrealdb_types::Value::Set(_) => 0,
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
	// The first search after a compaction reads each element's doc set from the
	// store; the next shares them through the cache.
	run(&ds, &session, query).await?;
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

/// A pending queue deeper than one materialisation batch is scored in several
/// batches, but the top-K is taken across the whole queue. The second query's
/// nearest neighbour is the queue's last record and its runners-up are far
/// earlier ones, so a result kept per batch — or a residual `WHERE` whose
/// per-candidate verdicts did not survive the batch that produced them —
/// returns a different set.
#[test(tokio::test(flavor = "multi_thread"))]
async fn hnsw_filtered_knn_spans_pending_materialisation_batches() -> Result<()> {
	// The queue must still be uncompacted when the queries run.
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let session = Session::owner().with_ns("test").with_db("test");

	// Record `i` sits at (i, i) and carries `category = i % 100`, so a
	// category selects an evenly spread 1-in-100 subset of the queue. The
	// count is deliberately above the pending materialisation batch size, so
	// the scan needs more than one batch.
	let n = 1100u32;
	let mut setup = String::from(
		"DEFINE INDEX emb ON pts FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 100 M 12;\n",
	);
	for i in 1..=n {
		setup
			.push_str(&format!("CREATE pts:{i} SET vec = [{i}f, {i}f], category = {};\n", i % 100));
	}
	for response in ds.execute(&setup, &session, None).await? {
		response.result?;
	}

	async fn ids(ds: &Arc<Datastore>, session: &Session, query: &str) -> Result<Vec<i64>> {
		let mut response = ds.execute(query, session, None).await?;
		let surrealdb_types::Value::Array(rows) = response.remove(0).result? else {
			panic!("expected an array result");
		};
		let mut ids: Vec<i64> = rows
			.iter()
			.map(|row| match row {
				surrealdb_types::Value::Object(o) => match o.get("id") {
					Some(surrealdb_types::Value::RecordId(rid)) => match &rid.key {
						surrealdb_types::RecordIdKey::Number(n) => *n,
						other @ (surrealdb_types::RecordIdKey::String(_)
						| surrealdb_types::RecordIdKey::Uuid(_)
						| surrealdb_types::RecordIdKey::Array(_)
						| surrealdb_types::RecordIdKey::Object(_)
						| surrealdb_types::RecordIdKey::Range(_)) => {
							panic!("unexpected record key: {other:?}")
						}
					},
					other => panic!("unexpected id field: {other:?}"),
				},
				other @ (surrealdb_types::Value::None
				| surrealdb_types::Value::Null
				| surrealdb_types::Value::Bool(_)
				| surrealdb_types::Value::Number(_)
				| surrealdb_types::Value::String(_)
				| surrealdb_types::Value::Bytes(_)
				| surrealdb_types::Value::Duration(_)
				| surrealdb_types::Value::Datetime(_)
				| surrealdb_types::Value::Date(_)
				| surrealdb_types::Value::Time(_)
				| surrealdb_types::Value::Timestamp(_)
				| surrealdb_types::Value::Interval(_)
				| surrealdb_types::Value::Uuid(_)
				| surrealdb_types::Value::Geometry(_)
				| surrealdb_types::Value::Table(_)
				| surrealdb_types::Value::RecordId(_)
				| surrealdb_types::Value::File(_)
				| surrealdb_types::Value::Range(_)
				| surrealdb_types::Value::Regex(_)
				| surrealdb_types::Value::Array(_)
				| surrealdb_types::Value::Set(_)) => panic!("unexpected row: {other:?}"),
			})
			.collect();
		ids.sort();
		Ok(ids)
	}

	// Winners at the head of the queue, wholly inside the first batch.
	assert_eq!(
		ids(&ds, &session, "SELECT id FROM pts WHERE vec <|5,400|> [0f, 0f] AND category = 7;")
			.await?,
		vec![7, 107, 207, 307, 407]
	);
	// Winners straddling the batch boundary: pts:1025 is read in the second
	// batch, its two runners-up in the first.
	assert_eq!(
		ids(
			&ds,
			&session,
			"SELECT id FROM pts WHERE vec <|3,400|> [1025f, 1025f] AND category = 25;"
		)
		.await?,
		vec![825, 925, 1025]
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
	let mut pending_records = tx.getr(ikb.new_hr_range()?, None).await?;
	pending_records.extend(tx.getr(ikb.new_hw_range()?, None).await?);
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

/// A filtered KNN at a `VERSION` is the brute-force search over the records
/// read at the stamp, since an ANN index describes the present: it reads each
/// record of the table once and returns K rows. The same query unversioned
/// walks the index and reads the candidates it visits. The versioned run
/// additionally reads its catalog at the version, so it may exceed the
/// table's record count by at most 2K keys.
#[cfg(feature = "kv-surrealkv")]
#[test(tokio::test(flavor = "multi_thread"))]
async fn hnsw_versioned_filtered_knn_reads_each_candidate_once() -> Result<()> {
	let dir = temp_dir::TempDir::new()?;
	let path = format!("surrealkv://{}?versioned=true&retention=1h", dir.path().to_string_lossy());
	// Compaction happens only where this test calls it.
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path(&path).await?;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let session = Session::owner().with_ns("test").with_db("test");

	let mut setup = String::from(
		"DEFINE INDEX emb ON pts FIELDS vec HNSW DIMENSION 8 DIST EUCLIDEAN TYPE F32 EFC 200 M 12;\n",
	);
	for i in 0..ROWS as u32 {
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
	for response in ds.execute(&setup, &session, None).await? {
		response.result?;
	}
	Datastore::index_compaction(
		Arc::clone(&ds),
		std::time::Duration::from_secs(1),
		tokio_util::sync::CancellationToken::new(),
	)
	.await?;
	let mut response = ds.execute("RETURN <string> time::now();", &session, None).await?;
	let surrealdb_types::Value::String(stamp) = response.remove(0).result? else {
		panic!("Expected a datetime string");
	};

	async fn run(ds: &Arc<Datastore>, session: &Session, query: &str) -> Result<(usize, u64)> {
		let tx = Arc::new(ds.transaction(TransactionType::Read).await?);
		let mut response =
			ds.run(QueryRequest::new(query, session).with_transaction(Arc::clone(&tx))).await?;
		let len = match response.remove(0).result? {
			surrealdb_types::Value::Array(a) => a.len(),
			surrealdb_types::Value::None
			| surrealdb_types::Value::Null
			| surrealdb_types::Value::Bool(_)
			| surrealdb_types::Value::Number(_)
			| surrealdb_types::Value::String(_)
			| surrealdb_types::Value::Bytes(_)
			| surrealdb_types::Value::Duration(_)
			| surrealdb_types::Value::Datetime(_)
			| surrealdb_types::Value::Date(_)
			| surrealdb_types::Value::Time(_)
			| surrealdb_types::Value::Timestamp(_)
			| surrealdb_types::Value::Interval(_)
			| surrealdb_types::Value::Uuid(_)
			| surrealdb_types::Value::Geometry(_)
			| surrealdb_types::Value::Table(_)
			| surrealdb_types::Value::RecordId(_)
			| surrealdb_types::Value::File(_)
			| surrealdb_types::Value::Range(_)
			| surrealdb_types::Value::Regex(_)
			| surrealdb_types::Value::Object(_)
			| surrealdb_types::Value::Set(_) => 0,
		};
		Ok((len, tx.metrics_snapshot_for_test().keys_read))
	}

	const K: u64 = 10;
	const ROWS: u64 = 500;
	let query = format!(
		"SELECT id FROM pts \
		 WHERE vec <|{K},400|> [0.5f,0.5f,0.5f,0.5f,0.5f,0.5f,0.5f,0.5f] AND category = 7"
	);
	let current = format!("{query};");
	let versioned = format!("{query} VERSION d'{stamp}';");
	// The first search loads the graph into the process-wide HNSW cache; warm
	// it so both measured runs read only candidate records and doc-id maps.
	run(&ds, &session, &current).await?;
	run(&ds, &session, &versioned).await?;
	let (current_len, current_keys) = run(&ds, &session, &current).await?;
	let (versioned_len, versioned_keys) = run(&ds, &session, &versioned).await?;
	eprintln!("CURRENT keys_read={current_keys} VERSIONED keys_read={versioned_keys}");

	assert_eq!(current_len as u64, K, "filtered KNN should return K matches");
	assert_eq!(versioned_len as u64, K, "versioned filtered KNN should return K matches");
	assert!(
		versioned_keys <= ROWS + 2 * K,
		"a versioned filtered KNN should read each record of the table once: \
		 versioned keys_read={versioned_keys}, table rows={ROWS}"
	);
	ds.shutdown().await?;
	Ok(())
}

/// The HNSW search a statement's access path ends in reads each candidate's
/// record once at a `VERSION` (see
/// [`super::assert_versioned_search_reads_each_candidate_once`]).
#[cfg(feature = "kv-surrealkv")]
#[test(tokio::test(flavor = "multi_thread"))]
async fn hnsw_versioned_filtered_search_reads_each_candidate_once() -> Result<()> {
	super::assert_versioned_search_reads_each_candidate_once(
		"DEFINE INDEX emb ON pts FIELDS vec HNSW DIMENSION 8 DIST EUCLIDEAN TYPE F32 EFC 200 M 12;",
	)
	.await
}

/// Filtered kNN returns every qualifying record the bare operator returns at
/// the same `k` and `ef`, through delete / re-create / overwrite churn that
/// recycles element and doc ids, whether the churn is still queued or has been
/// compacted into the graph, and whether the query's snapshot is current or
/// predates the round's churn and compaction.
///
/// The churn also gives records duplicates of other records' vectors, so a
/// compaction changes an element's doc set without changing the graph. The
/// snapshot opened before a round is probed only after a current query has
/// read the compacted doc sets back into the cache: a cache that served a
/// reader a doc set from another generation would hand the old snapshot a
/// set it cannot resolve.
///
/// The narrowest filter is the strictest detector: `tag = <id>` qualifies one
/// record, so a graph walk that skips the element holding it yields an empty
/// result. Every snapshot holds far more than 10 live records, so a bare
/// operator answering fewer than 10 is itself a drop, recorded with id `-1`. The bare operator's
/// nearest records for each probe are filtered for in turn; a bare hit the filtered operator misses
/// is a drop. In the old snapshot, every record the round moved or deleted is also filtered for at
/// the vector that snapshot holds for it, where it is the nearest record.
#[test(tokio::test(flavor = "multi_thread"))]
async fn hnsw_filtered_knn_keeps_what_bare_knn_returns_through_churn() -> Result<()> {
	// Compaction runs only where the test calls it.
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let session = Session::owner().with_ns("test").with_db("test");

	const DIM: usize = 8;
	const ROWS: u64 = 240;
	let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
	let mut next = move || {
		state ^= state << 13;
		state ^= state >> 7;
		state ^= state << 17;
		state
	};
	let vector = |next: &mut dyn FnMut() -> u64| -> String {
		let parts: Vec<String> =
			(0..DIM).map(|_| format!("{}f", (next() % 10_000) as f32 / 1000.0)).collect();
		format!("[{}]", parts.join(", "))
	};

	// The vector each live record holds, for the duplicates the churn writes.
	let mut live: std::collections::BTreeMap<u64, String> = Default::default();
	let mut setup = String::from(
		"DEFINE INDEX emb ON pts FIELDS vec HNSW DIMENSION 8 DIST EUCLIDEAN TYPE F32 EFC 100 M 12;\n",
	);
	for i in 0..ROWS {
		let v = vector(&mut next);
		setup.push_str(&format!("CREATE pts:{i} SET vec = {v}, tag = {i};\n"));
		live.insert(i, v);
	}
	for response in ds.execute(&setup, &session, None).await? {
		response.result?;
	}

	/// The record ids `query` returns, run on `tx` when given, else on a
	/// transaction of its own.
	async fn ids(
		ds: &Arc<Datastore>,
		session: &Session,
		tx: Option<&Arc<crate::kvs::Transaction>>,
		query: &str,
	) -> Result<Vec<i64>> {
		let mut request = QueryRequest::new(query, session);
		if let Some(tx) = tx {
			request = request.with_transaction(Arc::clone(tx));
		}
		let mut response = ds.run(request).await?;
		let surrealdb_types::Value::Array(rows) = response.remove(0).result? else {
			panic!("expected an array result");
		};
		Ok(rows
			.iter()
			.map(|row| match row {
				surrealdb_types::Value::Object(o) => match o.get("id") {
					Some(surrealdb_types::Value::RecordId(rid)) => match &rid.key {
						surrealdb_types::RecordIdKey::Number(n) => *n,
						other @ (surrealdb_types::RecordIdKey::String(_)
						| surrealdb_types::RecordIdKey::Uuid(_)
						| surrealdb_types::RecordIdKey::Array(_)
						| surrealdb_types::RecordIdKey::Object(_)
						| surrealdb_types::RecordIdKey::Range(_)) => {
							panic!("unexpected record key: {other:?}")
						}
					},
					other => panic!("unexpected id field: {other:?}"),
				},
				other @ (surrealdb_types::Value::None
				| surrealdb_types::Value::Null
				| surrealdb_types::Value::Bool(_)
				| surrealdb_types::Value::Number(_)
				| surrealdb_types::Value::String(_)
				| surrealdb_types::Value::Bytes(_)
				| surrealdb_types::Value::Duration(_)
				| surrealdb_types::Value::Datetime(_)
				| surrealdb_types::Value::Date(_)
				| surrealdb_types::Value::Time(_)
				| surrealdb_types::Value::Timestamp(_)
				| surrealdb_types::Value::Interval(_)
				| surrealdb_types::Value::Uuid(_)
				| surrealdb_types::Value::Geometry(_)
				| surrealdb_types::Value::Table(_)
				| surrealdb_types::Value::RecordId(_)
				| surrealdb_types::Value::File(_)
				| surrealdb_types::Value::Range(_)
				| surrealdb_types::Value::Regex(_)
				| surrealdb_types::Value::Array(_)
				| surrealdb_types::Value::Set(_)) => panic!("unexpected row: {other:?}"),
			})
			.collect())
	}
	async fn compact(ds: &Arc<Datastore>) -> Result<()> {
		Datastore::index_compaction(
			Arc::clone(ds),
			std::time::Duration::from_secs(1),
			tokio_util::sync::CancellationToken::new(),
		)
		.await?;
		Ok(())
	}

	compact(&ds).await?;
	let mut drops = Vec::new();
	let mut probed = 0usize;
	for round in 0..10u32 {
		// A snapshot from before this round's churn and compaction.
		let old = Arc::new(ds.transaction(TransactionType::Read).await?);
		let before = live.clone();
		let mut touched = std::collections::BTreeSet::new();
		let mut churn = String::new();
		for _ in 0..ROWS / 5 {
			let id = next() % ROWS;
			churn.push_str(&format!("DELETE pts:{id};\n"));
			live.remove(&id);
			touched.insert(id);
		}
		for _ in 0..ROWS / 5 {
			let id = next() % ROWS;
			let v = vector(&mut next);
			churn.push_str(&format!("UPSERT pts:{id} SET vec = {v}, tag = {id};\n"));
			live.insert(id, v);
			touched.insert(id);
		}
		for _ in 0..ROWS / 10 {
			let id = next() % ROWS;
			if let Some(held) = live.get_mut(&id) {
				let v = vector(&mut next);
				churn.push_str(&format!("UPDATE pts:{id} SET vec = {v};\n"));
				*held = v;
				touched.insert(id);
			}
		}
		// Duplicates of other live records' vectors: a compaction adds their
		// docs to elements already in the graph.
		let keys: Vec<u64> = live.keys().copied().collect();
		for _ in 0..ROWS / 20 {
			let id = next() % ROWS;
			let source = keys[(next() % keys.len() as u64) as usize];
			let v = live[&source].clone();
			churn.push_str(&format!("UPSERT pts:{id} SET vec = {v}, tag = {id};\n"));
			live.insert(id, v);
			touched.insert(id);
		}
		for response in ds.execute(&churn, &session, None).await? {
			response.result?;
		}
		// Odd rounds are probed with the churn still queued.
		if round % 2 == 0 {
			compact(&ds).await?;
		}
		let queries: Vec<String> = (0..10).map(|_| vector(&mut next)).collect();
		// Current snapshots first, so the cache holds what this round's
		// compaction committed when the old snapshot is probed.
		for snapshot in [None, Some(&old)] {
			for q in &queries {
				let bare = ids(
					&ds,
					&session,
					snapshot,
					&format!("SELECT id FROM pts WHERE vec <|10,40|> {q};"),
				)
				.await?;
				// Far more than 10 records are live in every snapshot, so a
				// short answer is a walk that lost its way, and would
				// otherwise leave nothing to probe.
				if bare.len() < 10 {
					drops.push((round, snapshot.is_some(), q.clone(), -1, bare.clone()));
				}
				for id in bare.iter().take(3) {
					probed += 1;
					let filtered = ids(
						&ds,
						&session,
						snapshot,
						&format!("SELECT id FROM pts WHERE vec <|10,40|> {q} AND tag = {id};"),
					)
					.await?;
					if filtered != vec![*id] {
						drops.push((round, snapshot.is_some(), q.clone(), *id, filtered));
					}
				}
			}
		}
		// Every record this round moved or deleted, queried in the old
		// snapshot at the vector that snapshot holds for it: at distance 0 it
		// is the nearest, whichever doc sets the cache holds by now.
		for id in touched.iter().filter(|id| before.contains_key(id)) {
			probed += 1;
			let q = &before[id];
			let filtered = ids(
				&ds,
				&session,
				Some(&old),
				&format!("SELECT id FROM pts WHERE vec <|10,40|> {q} AND tag = {id};"),
			)
			.await?;
			if filtered != vec![*id as i64] {
				drops.push((round, true, q.clone(), *id as i64, filtered));
			}
		}
		old.cancel().await?;
	}
	assert!(probed > 500, "the probe must run: {probed}");
	assert!(
		drops.is_empty(),
		"{} of {probed} bare hits dropped by the filter, or bare answers short (id -1) (round, old snapshot, query, id, rows): {drops:#?}",
		drops.len()
	);
	Ok(())
}
