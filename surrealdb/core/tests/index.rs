mod helpers;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use flate2::read::GzDecoder;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng, random};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{Array, RecordId, RecordIdKey, ToSql, Value};
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::helpers::{new_ds, new_ns_db};

async fn concurrent_tasks(
	dbs: Arc<Datastore>,
	session: &Session,
	task_count: usize,
	sql_func: impl Fn(usize) -> &'static str,
) -> Result<()> {
	let mut tasks = Vec::with_capacity(task_count);

	for i in 0..task_count {
		let dbs = dbs.clone();
		let session = session.clone();
		let sql = sql_func(i);
		tasks.push(tokio::spawn(async move {
			let mut res = dbs.execute(sql, &session, None).await?;
			// Ignore errors from aborted/cancelled transactions.
			if let Err(e) = res.remove(0).result
				&& !sql.contains("aborting transaction test")
				&& !sql.ends_with("CANCEL;")
			{
				error!("Concurrent task error: {sql} - {e}");
				panic!("Concurrent task error: {sql} - {e}")
			}
			Ok::<(), anyhow::Error>(())
		}));
	}

	for task in tasks {
		task.await??;
	}
	Ok(())
}

const SQL_CREATE_COMMIT: &str = "
		BEGIN;
			CREATE |aaa:10| CONTENT {
				field1: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
				field2: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
				field3: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
				field4: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
				field5: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
			} RETURN NONE;
			if (rand() < 0.1) {
				THROW 'aborting transaction test';
			};
		COMMIT;
	";
const SQL_CREATE_CANCEL: &str = "
        BEGIN;
            CREATE |aaa:10| CONTENT
                { field1: '-', field2: '-', field3: '-', field4: '-', field5: '-' }
                RETURN NONE;
	    CANCEL;";
const SQL_UPDATE_COMMIT: &str = "
        BEGIN;
			LET $before = (SELECT * FROM (SELECT * FROM aaa LIMIT 10000) ORDER rand() LIMIT 10);
			LET $ret = [
				$before,
				(UPDATE $before.id CONTENT {
					field1: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
					field2: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
					field3: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
					field4: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
					field5: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
				}),
			];
			if (rand() < 0.1) {
				THROW 'aborting transaction test';
			};
		COMMIT;";
const SQL_DELETE_COMMIT: &str = "
		BEGIN;
			LET $before = (SELECT * FROM (SELECT * FROM aaa LIMIT 10000) ORDER rand() LIMIT 10);
			LET $ret = [
				(DELETE $before.id RETURN before),
				[],
			];
			if (rand() < 0.1) {
				THROW 'aborting transaction test';
			};
		COMMIT;";

type SqlDistribution = [(f32, &'static str)];

async fn batch_ingestion(
	dbs: Arc<Datastore>,
	ses: &Session,
	batch_count: usize,
	sql_distribution: &SqlDistribution,
) -> Result<()> {
	info!("Inserting {batch_count} batches concurrently");

	let seed: u64 = std::env::var("SURREALDB_TEST_SEED")
		.ok()
		.and_then(|s| s.parse().ok())
		.unwrap_or_else(random);
	info!("Using random seed: {seed}");
	let mut rng = SmallRng::seed_from_u64(seed);
	let mut batch = Vec::with_capacity(batch_count);
	let mut map = HashMap::with_capacity(batch_count);
	for _ in 0..batch_count {
		let action = rng.r#gen::<f32>();
		for (threshold, sql) in sql_distribution {
			if action <= *threshold {
				batch.push(*sql);
				let count: &mut usize = map.entry(*sql).or_default();
				*count += 1;
				break;
			}
		}
	}
	assert_eq!(
		map.len(),
		sql_distribution.len(),
		"SQL distribution map should have same length as distribution"
	);
	concurrent_tasks(dbs.clone(), ses, batch.len(), |i| batch[i]).await
}

/// Spawns a background task that continuously runs index compaction on the given datastore
/// until the `abort_compaction` token is cancelled.
///
/// Each iteration calls [`Datastore::index_compaction`] with a 1-second timeout. The loop
/// accumulates the total number of compaction iterations and bails immediately if any
/// iteration reports errors. Once cancelled, it logs and returns the total iteration count.
async fn start_compaction_loop(
	dbs: Arc<Datastore>,
	abort_compaction: CancellationToken,
) -> JoinHandle<Result<usize>> {
	task::spawn(async move {
		let mut compaction_count = 0;
		while !abort_compaction.is_cancelled() {
			let (count_iterator, count_error) =
				Datastore::index_compaction(dbs.clone(), Duration::from_secs(1)).await?;
			compaction_count += count_iterator;
			if count_error > 0 {
				bail!(
					"{count_error} compaction tasks over {compaction_count} failed - check the logs"
				);
			}
		}
		info!("Ran {compaction_count} compaction iterations");
		Ok(compaction_count)
	})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[test_log::test]
// Regression test for https://github.com/surrealdb/surrealdb/issues/6837
async fn multi_index_concurrent_test_create_update_delete() -> Result<()> {
	#[cfg(not(debug_assertions))]
	let count = 1000;
	#[cfg(debug_assertions)]
	let count = 200;
	multi_index_concurrent_test(
		count,
		&[
			(0.33, SQL_CREATE_COMMIT),
			(0.40, SQL_CREATE_CANCEL),
			(0.70, SQL_UPDATE_COMMIT),
			(1.0, SQL_DELETE_COMMIT),
		],
	)
	.await
}

async fn multi_index_concurrent_test(
	batch_count: usize,
	sql_distribution: &SqlDistribution,
) -> Result<()> {
	let sql = ";
	DEFINE TABLE aaa;
	DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase, ascii, edgengram(1, 10);
	DEFINE INDEX field1 ON aaa FIELDS field1 FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;
	DEFINE INDEX field2 ON aaa FIELDS field2 FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;
	DEFINE INDEX field3 ON aaa FIELDS field3 FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;
	DEFINE INDEX field4 ON aaa FIELDS field4 FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;
	DEFINE INDEX field5 ON aaa FIELDS field5 FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;
";

	let (_, dbs) = new_ds("test", "test", false).await?;
	let dbs = Arc::new(dbs);

	// Start the index compaction
	let abort_compaction = CancellationToken::new();
	let compaction_loop = start_compaction_loop(dbs.clone(), abort_compaction.clone()).await;

	let ses = Session::owner().with_ns("test").with_db("test");
	// Define analyzer and indexes.
	dbs.execute(sql, &ses, None).await?;

	// Ingest records.
	batch_ingestion(dbs.clone(), &ses, batch_count, sql_distribution).await?;

	info!("Waiting for index to be built");
	timeout(Duration::from_secs(600), async {
        loop {
            let res = &mut dbs
                .execute(
                    "INFO FOR INDEX field1 ON aaa;\
			count(SELECT * FROM aaa WHERE field1 @@ 'cupcake' OR field1 @@ 'cakecup' OR field1 @@ 'cheese' OR field1 @@ 'pie' OR field1 @@ 'noms');\
			SELECT VALUE [field1, count] FROM (SELECT field1, count() FROM aaa GROUP field1)",
                    &ses,
                    None,
                )
                .await?;
            // INFO FOR INDEX result.
            let val = res.remove(0).result?;
            info!("{}", val.to_sql());
            let Value::Object(o) = val else {
                panic!("Invalid result format: {}", val.to_sql_pretty())
            };
            let building = o.get("building").expect("building field not found");
            let Value::Object(building) = building else {
                panic!("Invalid result format: {}", building.to_sql_pretty())
            };
            let status = building.get("status").expect("status field not found");
            let Value::String(status) = status else {
                panic!("Invalid result format: {}", status.to_sql_pretty())
            };
            let pending = building.get("pending").expect("pending field not found");
            let Value::Number(pending) = pending else {
                panic!("Invalid pending format: {}", building.to_sql_pretty())
            };
            // Ensure the status is valid (no error).
            if status != "ready" && status != "indexing" && status != "cleaning" {
                panic!("Invalid index status: {}", status.to_sql_pretty())
            }
            // Collect the index count.
            let val = res.remove(0).result?;
            let Value::Number(index_count) = val else {
                panic!("Invalid result: {}", val.to_sql_pretty())
            };
            // Collect the real count.
            let val = res.remove(0).result?;
            let Value::Array(a) = &val else {
                panic!("Invalid result format: {}", val.to_sql_pretty())
            };
            let mut real_total_count = 0;
            // Sum counts for the different values of field1.
            for item in a.iter() {
                let Value::Array(record) = item else {
                    panic!("Invalid result format: {}", item.to_sql_pretty())
                };
                let count = record.get(1).expect("count field not found");
                let Value::Number(count) = count else {
                    panic!("Invalid result format: {}", count.to_sql_pretty())
                };
                real_total_count += count.into_int()?;
            }
            info!("Real count: {real_total_count} - Index: count: {index_count} - Index status: {status} - Pending: {pending}");
            if index_count.into_int()? == real_total_count {
                if status != "ready" {
                    panic!("Invalid index status: {}", status.to_sql_pretty())
                }
                if pending.into_int()? != 0 {
                    panic!("Invalid pending number: {}", pending.to_sql_pretty())
                }
                // Success.
                break;
            }
            // Wait before retrying.
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
        Ok::<(), anyhow::Error>(())
    })
        .await??;

	// Stop the compaction loop
	abort_compaction.cancel();
	assert!(compaction_loop.await?? > 0);

	Ok(())
}

const INGESTING_SOURCE: &str = "../../tests/data/hnsw-random-9000-20-euclidean.gz";
const QUERYING_SOURCE: &str = "../../tests/data/hnsw-random-5000-20-euclidean.gz";

fn new_vectors_from_file(path: &str, mut limit: usize) -> Result<Vec<(RecordId, String)>> {
	// Open the gzip file
	let file = File::open(path)?;

	// Create a GzDecoder to read the file
	let gz = GzDecoder::new(file);

	// Wrap the decoder in a BufReader
	let reader = BufReader::new(gz);

	let mut res = Vec::new();
	// Iterate over each line in the file
	for (i, line_result) in reader.lines().enumerate() {
		if limit == 0 {
			break;
		}
		limit -= 1;
		let line = line_result?;
		res.push((RecordId::new("t".to_owned(), RecordIdKey::from(i as i64)), line));
	}
	Ok(res)
}

async fn collect_query(
	dbs: &Datastore,
	session: &Session,
	vectors: &[(RecordId, String)],
) -> Result<Vec<Array>> {
	let mut results = Vec::with_capacity(vectors.len());
	for (_, v) in vectors {
		let mut res = dbs
			.execute(&format!("SELECT id FROM t WHERE v <|10,150|> {v};"), session, None)
			.await?;
		let res = res.remove(0).result?;
		let Value::Array(res) = res else {
			panic!("Expected array result: {}", res.to_sql_pretty());
		};
		assert_eq!(res.len(), 10, "{}", res.to_sql_pretty());
		results.push(res);
	}
	Ok(results)
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn hnsw_concurrent_writes() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", false).await?;
	let dbs = Arc::new(dbs);
	let session = Session::owner().with_ns("test").with_db("test");

	// Define the table and the index.
	dbs.execute(
        "DEFINE TABLE t; DEFINE INDEX ix ON t FIELDS v HNSW DIMENSION 20 DIST EUCLIDEAN TYPE F32 EFC 150 M 8;",
        &session,
        None,
    )
        .await?;

	let vectors = new_vectors_from_file(INGESTING_SOURCE, 1000)?;
	let chunks: Vec<_> = vectors.chunks(vectors.len() / 4).map(|s| s.to_vec()).collect();
	let mut tasks = Vec::with_capacity(chunks.len());
	for chunk in chunks {
		let dbs = dbs.clone();
		let session = session.clone();
		tasks.push(tokio::spawn(async move {
			for (r, v) in chunk {
				let mut res = dbs
					.execute(
						&format!("CREATE {} SET v={v} RETURN NONE;", r.to_sql()),
						&session,
						None,
					)
					.await?;
				res.remove(0).result?;
			}
			Ok::<(), anyhow::Error>(())
		}));
	}
	for task in tasks {
		task.await??;
	}

	let vectors = new_vectors_from_file(QUERYING_SOURCE, 10)?;

	// Check we got results with the pending queue not cleaned
	let results1 = collect_query(&dbs, &session, &vectors).await?;

	// Clean the HNSW compaction queue
	Datastore::index_compaction(dbs.clone(), Duration::from_secs(1)).await?;

	// Collect the results once the pending queue is cleaned
	let results2 = collect_query(&dbs, &session, &vectors).await?;

	// The results should match
	assert_eq!(results1, results2);

	Ok(())
}

/// Continuously executes write queries against the datastore until the cancellation token
/// is triggered. This helper is used to generate sustained write pressure during stress tests.
///
/// Each task is assigned a `prefix` character (e.g. `'a'`, `'b'`, `'c'`) that is forwarded
/// to the `sql_func` closure together with an incrementing counter. The closure uses both
/// values to build record keys (e.g. `user-a-1@test.com`), so that tasks writing to the
/// same table produce distinct keys and reduce unnecessary unique-index collisions.
///
/// Any error returned by the datastore is propagated immediately to the caller.
async fn write_loop_until_cancellation<F>(
	prefix: char,
	dbs: Arc<Datastore>,
	session: Session,
	cancellation: CancellationToken,
	sql_func: F,
) -> Result<()>
where
	F: Fn(char, usize) -> String,
{
	let mut count = 1;
	while !cancellation.is_cancelled() {
		count += 1;
		let sql = sql_func(prefix, count);
		dbs.execute(&sql, &session, None).await?.remove(0).result?;
	}
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[test_log::test]
// Regression test for https://github.com/surrealdb/surrealdb/issues/7072
async fn multi_index_concurrent_test_index_compaction() -> Result<()> {
	// Step 1: Create a shared datastore and set up 3 namespaces × 3 databases = 9 isolated
	// environments. Each combination gets its own owner session, simulating a multi-tenant setup.
	let (_, dbs) = new_ds("test", "test", false).await?;
	let dbs = Arc::new(dbs);
	let mut sessions = Vec::new();
	for ns in ["ns1", "ns2", "ns3"] {
		for db in ["db1", "db2", "db3"] {
			new_ns_db(dbs.as_ref(), ns, db).await?;
			sessions.push(Session::owner().with_ns(ns).with_db(db));
		}
	}

	// Step 2: Start a background index compaction loop that continuously triggers compaction
	// on the datastore. A cancellation token is used to gracefully stop it later.
	let cancellation = CancellationToken::new();
	let compaction_loop = start_compaction_loop(dbs.clone(), cancellation.clone()).await;

	// Step 3: For each session (namespace/database pair), define multiple indexes of different
	// types on two tables (`user` and `scope`):
	// - UNIQUE indexes (idx_user_email, idx_scope_name, and a composite idx_scope_kind on
	//   kind+name)
	// - A normal (non-unique) index (idx_scope_kind on `kind`)
	// - COUNT indexes (idx_user_count, idx_scope_count)
	// - HNSW vector index (ixd_user_vector on `vector`)
	// - FULLTEXT index with a custom analyzer using BM25 scoring (idx_scope_name_ft)
	// This ensures that index compaction is exercised against a variety of index types.
	for session in &sessions {
		let results = dbs.execute(
			"
		DEFINE TABLE user SCHEMALESS;
        DEFINE TABLE scope SCHEMALESS;
		DEFINE INDEX idx_user_email ON user FIELDS email UNIQUE;
		DEFINE INDEX idx_user_count ON user COUNT;
		DEFINE INDEX idx_scope_name ON scope FIELDS name UNIQUE;
		DEFINE INDEX idx_scope_kind ON scope FIELDS kind;
		DEFINE INDEX idx_scope_count ON scope COUNT;
		DEFINE INDEX ixd_user_vector ON user FIELDS vector HNSW DIMENSION 20 DIST EUCLIDEAN TYPE F32 EFC 150 M 8;
		DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase, ascii, edgengram(1, 10);
		DEFINE INDEX idx_scope_name_ft ON scope FIELDS name FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
		DEFINE INDEX idx_scope_kind_name ON scope FIELDS kind, name UNIQUE;",
			session,
			None,
		)
		.await?;
		for result in results {
			result.result?;
		}
	}

	// Step 4: For each of the 9 sessions, spawn 6 concurrent write tasks (3 for `user` records
	// and 3 for `scope` records), totalling 54 parallel writers. Each task is assigned a
	// distinct prefix ('a', 'b', or 'c') so that concurrent writers targeting the same table
	// produce non-overlapping record keys, reducing spurious unique-index collisions.
	// Every task continuously creates new records until the cancellation token is triggered,
	// generating heavy concurrent write pressure on all index types while the compaction loop
	// is running in the background.
	let mut tasks = Vec::new();

	let vectors = Arc::new(new_vectors_from_file(INGESTING_SOURCE, 5000)?);

	for session in &sessions {
		// Spawn 3 tasks writing `user` records with unique emails (exercises UNIQUE, COUNT,
		// and HNSW indexes).
		for prefix in ['a', 'b', 'c'] {
			let vectors = vectors.clone();
			tasks.push(tokio::spawn(write_loop_until_cancellation(
				prefix,
				dbs.clone(),
				session.clone(),
				cancellation.clone(),
				move |p, c| {
					let vector = &vectors.as_ref()[c % 5000].1;
					format!(
						"CREATE user SET email = 'user-{p}-{c}@test.com', vector = {vector} RETURN NONE;"
					)
				},
			)));
		}

		// Spawn 3 tasks writing `scope` records (exercises UNIQUE, COUNT, FULLTEXT, and
		// composite UNIQUE indexes).
		for prefix in ['a', 'b', 'c'] {
			tasks.push(tokio::spawn(write_loop_until_cancellation(
				prefix,
				dbs.clone(),
				session.clone(),
				cancellation.clone(),
				|p, c| {
					format!(
						"CREATE scope SET name = 'tenant-{p}-{c}', kind = 'tenant' RETURN NONE;"
					)
				},
			)));
		}
	}

	// Step 5: Let the concurrent writes and index compaction run together for 2 seconds,
	// then signal all tasks and the compaction loop to stop via the cancellation token.
	sleep(Duration::from_secs(10)).await;
	cancellation.cancel();

	// Step 6: Await all write tasks and the compaction loop, propagating any errors.
	// If any task panicked or returned an unexpected error, the test fails here.
	for task in tasks {
		task.await??;
	}

	// Step 7: Verify that the background compaction loop actually performed work.
	// The loop returns the number of compaction iterations it completed; asserting > 0
	// ensures that index compaction was genuinely exercised during the stress period.
	match compaction_loop.await? {
		Ok(compaction_count) => {
			assert!(compaction_count > 0, "Compaction is 0");
		}
		Err(e) => {
			panic!("Compaction failed: {e}")
		}
	}
	Ok(())
}
