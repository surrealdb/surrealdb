//! DiskANN behaviour observed through the query driver.

use std::sync::Arc;

use anyhow::Result;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::Session;
use crate::key::schema::RecordKey;
use crate::kvs::{Datastore, QueryRequest, TransactionType};
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
	// Compaction happens only where this test calls it: the assertions compare
	// the pending set against the compacted graph, which a background
	// compactor would fold away before the first phase reads it.
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
	let db_def = {
		let tx = ds.transaction(TransactionType::Write).await?;
		let db = tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
		db
	};
	let session = Session::owner().with_ns("test").with_db("test");

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

/// One completed transaction: when it finished relative to the window, whether
/// it was writable, and what it cost.
#[cfg(feature = "kv-surrealkv")]
struct TxRecord {
	at: std::time::Duration,
	write: bool,
	metrics: crate::observe::TransactionMetricsSnapshot,
}

/// Records every transaction completed inside an open window.
///
/// Per-transaction rather than aggregate: a build's transactions are not all
/// alike — the backfill scan commits one bounded batch per group of records
/// while the pending drain commits one compaction frame at a time — and the
/// shape of the sequence is what says where one phase ended and the next began.
/// No phase marker is observable from the driver, so the timeline stands in for
/// one.
#[cfg(feature = "kv-surrealkv")]
#[derive(Default)]
struct BuildTxTimeline {
	start: std::sync::Mutex<Option<web_time::Instant>>,
	entries: std::sync::Mutex<Vec<TxRecord>>,
}

#[cfg(feature = "kv-surrealkv")]
impl crate::observe::ExecutionObserver for BuildTxTimeline {
	fn on_transaction_complete(&self, event: &crate::observe::TransactionEvent) {
		// Transactions completing outside an open window belong to the test's
		// own setup, not to the job being described.
		let Some(start) = *self.start.lock().unwrap() else {
			return;
		};
		self.entries.lock().unwrap().push(TxRecord {
			at: start.elapsed(),
			write: event.safe.write,
			metrics: event.safe.metrics,
		});
	}
}

#[cfg(feature = "kv-surrealkv")]
impl BuildTxTimeline {
	/// Opens a recording window, discarding anything recorded before it.
	fn open(&self) {
		self.entries.lock().unwrap().clear();
		*self.start.lock().unwrap() = Some(web_time::Instant::now());
	}

	/// Closes the window and takes what it recorded.
	fn close(&self) -> Vec<TxRecord> {
		*self.start.lock().unwrap() = None;
		std::mem::take(&mut *self.entries.lock().unwrap())
	}
}

/// Prints `entries` as ten equal slices of `window`, so the shape of a long job
/// — where its transactions cluster, how large they are, how many reads each
/// costs — is readable without a profiler.
///
/// Two things to know when reading the output. A transaction lands in the slice
/// it *completed* in, so a compaction frame that spent a minute mutating the
/// graph shows up as one large entry at its commit, and the slices it occupied
/// look empty rather than busy. And the datastore's background maintenance runs
/// throughout: the node heartbeat and the other periodic jobs commit small
/// transactions of their own, which are the small constant term in every slice.
#[cfg(feature = "kv-surrealkv")]
fn print_tx_timeline(window: std::time::Duration, entries: &[TxRecord]) {
	let slice = (window / 10).max(std::time::Duration::from_millis(1));
	eprintln!("timeline       ten slices of {slice:?}");
	eprintln!(
		"  {:>10}  {:>7} {:>7} {:>13} {:>12} {:>13}",
		"through", "writes", "reads", "keys_written", "ops_get", "keys_read"
	);
	for i in 1..=10 {
		let lo = slice * (i - 1);
		let hi = slice * i;
		let mut writes = 0u64;
		let mut reads = 0u64;
		let mut keys_written = 0u64;
		let mut ops_get = 0u64;
		let mut keys_read = 0u64;
		for tx in entries.iter().filter(|tx| tx.at >= lo && tx.at < hi) {
			if tx.write {
				writes += 1;
			} else {
				reads += 1;
			}
			keys_written += tx.metrics.keys_written;
			ops_get += u64::from(tx.metrics.ops_get);
			keys_read += tx.metrics.keys_read;
		}
		eprintln!(
			"  {:>10?}  {writes:>7} {reads:>7} {keys_written:>13} {ops_get:>12} {keys_read:>13}",
			hi
		);
	}
}

/// Diagnostic: the whole reference workload — 200 000 × 768-dim `F32` vectors
/// indexed by `DEFINE INDEX … DISKANN DIMENSION 768 DIST COSINE TYPE F32` at the
/// DDL defaults (`DEGREE 64`, `L_BUILD 100`, `ALPHA 1.2`) — on SurrealKV, end to
/// end.
///
/// Reports; asserts nothing. What it prints is the baseline a change to the
/// build path is judged against:
///
/// - wall-clock for the row load and for the `DEFINE INDEX` that builds the graph;
/// - the KV operation and key counts the build issued, as a per-slice timeline. The backfill scan
///   commits a bounded batch per group of records and the pending drain commits one compaction
///   frame at a time, so the slice where transaction shape changes is the scan/drain boundary — the
///   driver has no phase marker to read instead;
/// - process RSS before the build, at its peak across it, and the growth between — next to the
///   DiskANN cache budget the graph is competing for. The growth is the figure that describes the
///   build: the process is also holding the corpus for the ground truth below;
/// - Recall@10 over held-out queries, so a build that got faster by building a worse graph is
///   visible in the same run as the time it saved.
///
/// The corpus is clustered rather than uniform, and its clusters are small
/// relative to `k`: at 768 dimensions a uniform draw puts every pair of points
/// at nearly the same distance, while clusters of `k` or more members make the
/// top-`k` one cluster's interior and the query trivial. Neither says anything
/// about how well the graph connects.
///
/// Ignored: tens of minutes and tens of gigabytes of SurrealKV. Run it as
///
/// ```text
/// cargo test --release -p surrealdb-core --lib --features kv-surrealkv \
///     diag_diskann_build_surrealkv_200k -- --ignored --nocapture
/// ```
///
/// `--lib` is required, not tidiness: without it cargo also builds this package's integration
/// test targets, and `count_index_backlog_repro` exceeds rustc's query depth limit in release,
/// so the run fails to compile before reaching this test. This diagnostic lives in the lib's
/// `cfg(test)` tree, so restricting the target set costs nothing.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "diagnostic (tens of minutes, tens of GiB): the 200k x 768 DiskANN build on SurrealKV"]
async fn diag_diskann_build_surrealkv_200k() -> Result<()> {
	let dir = tempfile::tempdir()?;
	let path = format!("surrealkv:{}", dir.path().display());
	let timeline = Arc::new(BuildTxTimeline::default());
	let ds = Datastore::builder()
		.with_observer(Arc::clone(&timeline) as Arc<dyn crate::observe::ExecutionObserver>)
		.build_with_path(&path)
		.await?;
	let measured = measure_reference_build(&ds, &timeline, &path).await;
	// A datastore that is only dropped keeps SurrealKV open for the rest of the
	// process, so it is closed — whether or not the measurement succeeded —
	// before `dir` drops and removes the directory under it.
	let closed = ds.shutdown().await;
	measured.and(closed)
}

/// The body of [`diag_diskann_build_surrealkv_200k`], run against the datastore
/// that test owns and closes.
#[cfg(feature = "kv-surrealkv")]
async fn measure_reference_build(
	ds: &Datastore,
	timeline: &BuildTxTimeline,
	path: &str,
) -> Result<()> {
	use std::sync::atomic::{AtomicU64, Ordering};
	use std::time::Duration;

	use rand::rngs::SmallRng;
	use rand::{Rng, SeedableRng};
	use surrealdb_types::{Array, Value, Variables, object};
	use web_time::Instant;

	/// The reference workload. Lowering `ROWS` keeps the shape and makes the run
	/// affordable, at the cost of leaving the cache-miss regime the figures are
	/// about.
	const ROWS: usize = 200_000;
	const DIM: usize = 768;
	const QUERIES: usize = 500;
	/// Far more clusters than the graph's 64-edge budget, and far fewer members
	/// per cluster than `K`, so answering a query means hopping between clusters
	/// rather than walking one cluster's interior.
	const CLUSTERS: usize = 80_000;
	const SPREAD: f32 = 0.35;
	/// Rows per `INSERT`. Bounds the load phase's peak — one statement carrying
	/// every row would hold the whole corpus as `Value`s at once.
	const LOAD_BATCH: usize = 1_000;
	const K: usize = 10;
	const EF: usize = 100;
	const SEED: u64 = 0x0D15_C0FF_EE5E_ED01;
	/// Separate stream for the centroids. Deriving them from `SEED` itself would
	/// give centroid 0 the same draw sequence as the member noise, making the
	/// first member a scaled copy of its own centroid rather than a perturbed
	/// one.
	const CENTROID_SEED: u64 = 0x51E5_C0DE_0DDB_0A11;

	let session = Session::owner().with_ns("test").with_db("test");
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}

	// One RNG stream for the corpus and the queries, so the queries come from the
	// same distribution without being copies of indexed rows — a query that is
	// its own nearest neighbour scores a guaranteed hit.
	//
	// A centroid is derived from its index rather than stored: at the reference
	// size the centroid table would be a quarter of a gigabyte of the process's
	// own memory, and process memory is one of the figures this run reports.
	let centroid = |cluster: usize| -> Vec<f32> {
		let mut rng = SmallRng::seed_from_u64(CENTROID_SEED.wrapping_add(cluster as u64));
		(0..DIM).map(|_| rng.random_range(-1.0f32..=1.0)).collect()
	};
	let mut rng = SmallRng::seed_from_u64(SEED);
	let mut drawn = 0usize;
	let draw = |rng: &mut SmallRng, drawn: &mut usize| -> Vec<f32> {
		let centroid = centroid(*drawn % CLUSTERS);
		*drawn += 1;
		centroid.iter().map(|c| c + rng.random_range(-SPREAD..=SPREAD)).collect()
	};
	let as_value = |v: &[f32]| {
		Value::Array(Array::from(v.iter().map(|c| f64::from(*c)).collect::<Vec<f64>>()))
	};

	eprintln!("Loading {ROWS} x {DIM} rows into {path}");
	let load_start = Instant::now();
	let mut corpus: Vec<Vec<f32>> = Vec::with_capacity(ROWS);
	let mut batch: Vec<Value> = Vec::with_capacity(LOAD_BATCH);
	for id in 0..ROWS {
		let vector = draw(&mut rng, &mut drawn);
		batch.push(Value::Object(object! {
			id: id as i64,
			vec: as_value(&vector),
		}));
		corpus.push(vector);
		if batch.len() == LOAD_BATCH || id + 1 == ROWS {
			let mut vars = Variables::new();
			vars.insert("rows", Value::Array(Array::from(std::mem::take(&mut batch))));
			batch = Vec::with_capacity(LOAD_BATCH);
			for response in ds.execute("INSERT INTO emb $rows", &session, Some(vars)).await? {
				response.result?;
			}
		}
	}
	let load_elapsed = load_start.elapsed();
	eprintln!("Loaded in {load_elapsed:?}");

	// RSS the build starts from. The test process is holding the corpus for the
	// ground truth below — around 600 MiB at the reference size — so what the
	// build itself costs is the growth above this line, not the peak.
	let base_rss = crate::observe::refresh_process_snapshot().await.memory_bytes;

	// Peak RSS across the build, sampled from a task of its own: the build is one
	// awaited statement, with nothing inside it to hang a sample off.
	let peak_rss = Arc::new(AtomicU64::new(0));
	let sampler_stop = tokio_util::sync::CancellationToken::new();
	let sampler = tokio::spawn({
		let peak_rss = Arc::clone(&peak_rss);
		let stop = sampler_stop.clone();
		async move {
			loop {
				let snapshot = crate::observe::refresh_process_snapshot().await;
				peak_rss.fetch_max(snapshot.memory_bytes, Ordering::Relaxed);
				tokio::select! {
					() = stop.cancelled() => break,
					_ = tokio::time::sleep(Duration::from_secs(1)) => {}
				}
			}
		}
	});

	eprintln!("Building the index");
	timeline.open();
	let build_start = Instant::now();
	for response in ds
		.execute(
			"DEFINE INDEX emb_diskann ON emb FIELDS vec \
			 DISKANN DIMENSION 768 DIST COSINE TYPE F32",
			&session,
			None,
		)
		.await?
	{
		response.result?;
	}
	let build_elapsed = build_start.elapsed();
	let entries = timeline.close();
	sampler_stop.cancel();
	let _ = sampler.await;

	let mut write_txs = 0u64;
	let mut read_txs = 0u64;
	let mut keys_read = 0u64;
	let mut keys_written = 0u64;
	let mut bytes_read = 0u64;
	let mut bytes_written = 0u64;
	let mut ops_get = 0u64;
	let mut ops_scan = 0u64;
	let mut ops_write = 0u64;
	for tx in &entries {
		if tx.write {
			write_txs += 1;
		} else {
			read_txs += 1;
		}
		keys_read += tx.metrics.keys_read;
		keys_written += tx.metrics.keys_written;
		bytes_read += tx.metrics.value_bytes_read;
		bytes_written += tx.metrics.value_bytes_written;
		ops_get += u64::from(tx.metrics.ops_get);
		ops_scan += u64::from(tx.metrics.ops_scan);
		ops_write += u64::from(tx.metrics.ops_set)
			+ u64::from(tx.metrics.ops_put)
			+ u64::from(tx.metrics.ops_del);
	}

	eprintln!("=== diag_diskann_build_surrealkv_200k ===");
	eprintln!(
		"workload       {ROWS} x {DIM} DEGREE=64 L_BUILD=100 ALPHA=1.2 DIST=COSINE surrealkv"
	);
	eprintln!("load           {load_elapsed:?}");
	eprintln!("DEFINE INDEX   {build_elapsed:?}");
	eprintln!("transactions   {write_txs} write, {read_txs} read");
	eprintln!("kv ops         get={ops_get} scan={ops_scan} write={ops_write}");
	eprintln!(
		"kv keys        read={keys_read} written={keys_written} ({:.1} reads per row)",
		keys_read as f64 / ROWS as f64
	);
	eprintln!("kv value bytes read={bytes_read} written={bytes_written}");
	let peak_rss = peak_rss.load(Ordering::Relaxed);
	eprintln!(
		"RSS            {} MiB peak, {} MiB before the build, {} MiB growth \
		 (DiskANN cache budget {} MiB)",
		peak_rss / (1024 * 1024),
		base_rss / (1024 * 1024),
		peak_rss.saturating_sub(base_rss) / (1024 * 1024),
		ds.config().idx.diskann_cache_size / (1024 * 1024),
	);
	print_tx_timeline(build_elapsed, &entries);

	// Recall@K against brute force over the same corpus and metric. A build that
	// got faster by producing a worse graph shows up here, in the same run as
	// the time it saved.
	let knn = format!("SELECT VALUE record::id(id) FROM emb WHERE vec <|{K},{EF}|> $q");

	// The plan, once. A KNN whose right-hand side does not resolve to a value at
	// planning time falls back to a table scan with the operator evaluated per
	// row: exact by construction, so recall would read 1.0 while measuring
	// nothing about the graph.
	{
		let mut vars = Variables::new();
		vars.insert("q", as_value(&draw(&mut rng, &mut drawn)));
		let plan =
			ds.execute(&format!("{knn} EXPLAIN"), &session, Some(vars)).await?.remove(0).result?;
		eprintln!("knn plan       {plan:?}");
	}

	eprintln!("Measuring Recall@{K} at ef={EF} over {QUERIES} held-out queries");
	let mut total_recall = 0.0;
	let mut scored: Vec<(f64, usize)> = Vec::with_capacity(ROWS);
	for _ in 0..QUERIES {
		let query = draw(&mut rng, &mut drawn);
		scored.clear();
		scored.extend(corpus.iter().enumerate().map(|(id, v)| (cosine_distance(v, &query), id)));
		// Partial selection, not a sort: fully ordering every row for each of the
		// queries costs more than the searches being measured, and only the
		// top-`K` set is compared. Ties at the `K`-th distance do not arise on
		// continuous float draws.
		scored.select_nth_unstable_by(K, |a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
		let truth: std::collections::HashSet<usize> =
			scored[..K].iter().map(|(_, id)| *id).collect();

		let mut vars = Variables::new();
		vars.insert("q", as_value(&query));
		let got =
			ds.execute(&knn, &session, Some(vars)).await?.remove(0).result?.into_t::<Vec<i64>>()?;
		// Set intersection, not matching entries: counting entries would score a graph that
		// returns one true neighbour repeatedly as perfect. A duplicate is a defect, so it fails.
		let seen: std::collections::HashSet<usize> = got.iter().map(|id| *id as usize).collect();
		assert_eq!(seen.len(), got.len(), "index returned duplicate ids: {got:?}");
		let hits = seen.iter().filter(|id| truth.contains(id)).count();
		total_recall += hits as f64 / K as f64;
	}
	eprintln!("Recall@{K} (ef={EF}) = {:.4}", total_recall / QUERIES as f64);
	Ok(())
}

/// Cosine distance, matching `DIST COSINE`: `1 - cos(a, b)`, over `TYPE F32` vectors.
///
/// The index's graph search scores candidates with the `diskann-vector` crate's SIMD cosine
/// kernel, which this crate does not depend on. This mirrors that kernel's precision — the dot
/// product and both squared norms accumulate in `f32` — but not its summation order, and it
/// divides in `f64`. The ground truth and the index can therefore rank two candidates
/// differently only when their distances agree to within `f32` rounding: a near-tie at the
/// `k`th neighbour, which continuous random draws make rare.
#[cfg(feature = "kv-surrealkv")]
fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
	let mut dot = 0.0f32;
	let mut norm_a = 0.0f32;
	let mut norm_b = 0.0f32;
	for (x, y) in a.iter().zip(b.iter()) {
		dot += *x * *y;
		norm_a += *x * *x;
		norm_b += *y * *y;
	}
	1.0 - f64::from(dot) / (f64::from(norm_a).sqrt() * f64::from(norm_b).sqrt())
}

/// A filtered KNN at a `VERSION` is the brute-force search over the records
/// read at the stamp, since an ANN index describes the present: it reads each
/// record of the table once and returns K rows. The same query unversioned
/// walks the index and reads the candidates it visits. The versioned run
/// additionally reads its catalog at the version, so it may exceed the
/// table's record count by at most 2K keys.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_versioned_filtered_knn_reads_each_candidate_once() -> Result<()> {
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
		"DEFINE INDEX emb ON pts FIELDS vec DISKANN DIMENSION 8 DIST EUCLIDEAN TYPE F32;\n",
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
	// The first search loads the graph into the process-wide DiskANN cache; warm
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

/// A filtered KNN over a compacted DiskANN graph finds the admitted documents
/// nearest the query even when the filter admits 1% of the table: the walk
/// evaluates the filter on every element it reaches rather than on the `l`
/// nearest overall, which here hold one of the five. Both truthy filters take
/// that walk — the table's per-record SELECT permission for a record user's
/// bare KNN, and a residual condition for the owner — and both match the
/// brute-force answer. A search list too narrow for one walk to admit K is
/// widened until it does.
#[tokio::test(flavor = "multi_thread")]
async fn diskann_selective_filter_finds_admitted_neighbours_past_the_search_list() -> Result<()> {
	// Compaction happens only where this test calls it, so the queries below
	// search the compacted graph rather than the pending queue.
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
	{
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.ensure_ns_db(None, "test", "test").await?;
		tx.commit().await?;
	}
	let owner = Session::owner().with_ns("test").with_db("test");
	let record = Session::for_record(
		"test",
		"test",
		"user",
		crate::types::PublicValue::String("user:1".to_owned()),
	);

	// 1000 pseudo-random 8-d points from a fixed-seed generator, spread like
	// real embeddings rather than along the few lines a modular lattice traces;
	// the permission and the residual condition both admit `category = 7`, one
	// row in a hundred.
	let mut setup = String::from(
		"DEFINE TABLE pts SCHEMALESS PERMISSIONS FOR select WHERE category = 7 \
		 FOR create, update, delete NONE;\n\
		 DEFINE INDEX emb ON pts FIELDS vec DISKANN DIMENSION 8 DIST EUCLIDEAN TYPE F32;\n",
	);
	let mut state = 0x9E37_79B9_7F4A_7C15u64;
	for i in 0..1000u32 {
		let mut v = String::new();
		for j in 0..8u32 {
			if j > 0 {
				v.push_str(", ");
			}
			// xorshift64*
			state ^= state >> 12;
			state ^= state << 25;
			state ^= state >> 27;
			let f = (state.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 40) as f32 / (1u64 << 24) as f32;
			v.push_str(&format!("{f}f"));
		}
		setup.push_str(&format!("CREATE pts:{i} SET vec = [{v}], category = {};\n", i % 100));
	}
	for response in ds.execute(&setup, &owner, None).await? {
		response.result?;
	}
	Datastore::index_compaction(
		Arc::clone(&ds),
		std::time::Duration::from_secs(1),
		tokio_util::sync::CancellationToken::new(),
	)
	.await?;

	async fn ids(ds: &Datastore, session: &Session, query: &str) -> Result<Vec<String>> {
		let mut response = ds.execute(query, session, None).await?;
		let surrealdb_types::Value::Array(rows) = response.remove(0).result? else {
			panic!("Expected array result");
		};
		let mut ids: Vec<String> = rows.into_iter().map(|v| format!("{v:?}")).collect();
		ids.sort();
		Ok(ids)
	}

	let q = "[0.5f,0.5f,0.5f,0.5f,0.5f,0.5f,0.5f,0.5f]";
	let truth = ids(
		&ds,
		&owner,
		&format!("SELECT VALUE id FROM pts WHERE vec <|5,EUCLIDEAN|> {q} AND category = 7;"),
	)
	.await?;
	assert_eq!(truth.len(), 5, "brute force should find K admitted rows");

	// A search list too narrow for one walk to reach k visible rows widens and
	// walks again until it holds k.
	let narrow =
		ids(&ds, &record, &format!("SELECT VALUE id FROM pts WHERE vec <|5,10|> {q};")).await?;
	assert_eq!(narrow.len(), 5, "a narrow search list should still return K visible rows");

	let permitted =
		ids(&ds, &record, &format!("SELECT VALUE id FROM pts WHERE vec <|5,100|> {q};")).await?;
	assert_eq!(
		permitted, truth,
		"a record user's bare KNN should return its K nearest visible rows"
	);

	let conditioned = ids(
		&ds,
		&owner,
		&format!("SELECT VALUE id FROM pts WHERE vec <|5,100|> {q} AND category = 7;"),
	)
	.await?;
	assert_eq!(conditioned, truth, "a residual condition should return its K nearest matches");
	Ok(())
}

/// The DiskANN search a statement's access path ends in reads each
/// candidate's record once at a `VERSION` (see
/// [`super::assert_versioned_search_reads_each_candidate_once`]).
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn diskann_versioned_filtered_search_reads_each_candidate_once() -> Result<()> {
	super::assert_versioned_search_reads_each_candidate_once(
		"DEFINE INDEX emb ON pts FIELDS vec DISKANN DIMENSION 8 DIST EUCLIDEAN TYPE F32;",
	)
	.await
}
