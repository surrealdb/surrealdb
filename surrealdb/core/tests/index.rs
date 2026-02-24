mod helpers;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use flate2::read::GzDecoder;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{Array, RecordId, RecordIdKey, ToSql, Value};
use tokio::time::timeout;
use tracing::info;

use crate::helpers::new_ds;

async fn concurrent_tasks(
	dbs: Arc<Datastore>,
	session: &Session,
	batch: Vec<&'static str>,
) -> Result<()> {
	let mut tasks = Vec::with_capacity(batch.len());

	for sql in batch {
		let dbs = dbs.clone();
		let session = session.clone();
		tasks.push(tokio::spawn(async move {
			let mut res = dbs.execute(sql, &session, None).await?;
			res.remove(0).result?;
			Ok::<(), anyhow::Error>(())
		}));
	}

	for task in tasks {
		task.await??;
	}
	Ok(())
}

async fn batch_ingestion(dbs: Arc<Datastore>, ses: &Session, batch_count: usize) -> Result<()> {
	info!("Inserting {batch_count} batches concurrently");
	// Create records and commit.
	let sql_create_commit = "CREATE |aaa:10| CONTENT {
			field1: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
			field2: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
			field3: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
			field4: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
			field5: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
		} RETURN NONE;";
	// Create records and cancel
	let sql_create_throw = "
        BEGIN;
            CREATE |aaa:10| CONTENT
                { field1: '-', field2: '-', field3: '-', field4: '-', field5: '-' }
                RETURN NONE;
			THROW 'Ooch!';
	    COMMIT;";
	let sql_create_cancel = "
        BEGIN;
            CREATE |aaa:10| CONTENT
                { field1: '-', field2: '-', field3: '-', field4: '-', field5: '-' }
                RETURN NONE;
	    CANCEL;";
	let task_sequence = [
		// 10 normal committed transaction
		sql_create_commit,
		sql_create_commit,
		sql_create_commit,
		sql_create_commit,
		sql_create_commit,
		sql_create_commit,
		sql_create_commit,
		sql_create_commit,
		sql_create_commit,
		sql_create_commit,
		// 5 explicitely thrown transactions
		sql_create_throw,
		sql_create_throw,
		sql_create_throw,
		sql_create_throw,
		sql_create_throw,
		// 5 explicitely cancelled transactions
		sql_create_cancel,
		sql_create_cancel,
		sql_create_cancel,
		sql_create_cancel,
		sql_create_cancel,
	];
	let mut batch = Vec::new();
	for _ in 0..batch_count {
		batch.extend(task_sequence);
	}
	concurrent_tasks(dbs.clone(), ses, batch).await
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
// Regression test for https://github.com/surrealdb/surrealdb/issues/6837
async fn multi_index_concurrent_test() -> Result<()> {
	let dbs = Arc::new(new_ds("test", "test").await?);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Define the table.
	dbs.execute("DEFINE TABLE aaa", &ses, None).await?;

	// Ingest 500 records (including cancellation transaction)
	batch_ingestion(dbs.clone(), &ses, 5).await?;

	let sql = ";
	DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase, ascii, edgengram(1, 10);
	DEFINE INDEX field1 ON aaa FIELDS field1 FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;
	DEFINE INDEX field2 ON aaa FIELDS field2 FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;
	DEFINE INDEX field3 ON aaa FIELDS field3 FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;
	DEFINE INDEX field4 ON aaa FIELDS field4 FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;
	DEFINE INDEX field5 ON aaa FIELDS field5 FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;
";
	// Define analyzer and indexes.
	dbs.execute(sql, &ses, None).await?;

	// Ingest another 5 records (including cancellation transaction)
	batch_ingestion(dbs.clone(), &ses, 5).await?;

	let expected_total_count = 1000;

	info!("Waiting for index to be built");
	timeout(Duration::from_secs(30), async {
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
            let building = o.get("building").unwrap();
            let Value::Object(building) = building else {
                panic!("Invalid result format: {}", building.to_sql_pretty())
            };
            let status = building.get("status").unwrap();
            let Value::String(status) =  status else {
                panic!("Invalid result format: {}", status.to_sql_pretty())
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
                let count = record.get(1).unwrap();
                let Value::Number(count) = count else {
                    panic!("Invalid result format: {}", count.to_sql_pretty())
                };
                real_total_count += count.into_int()?;
            }
            if real_total_count == expected_total_count {
                info!("Real count: {real_total_count} - Index: count: {index_count} - Index status: {status}");
                if index_count.into_int()? == expected_total_count {
                    // Success.
                    break;
                }
            }
            // Wait before retrying.
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
        Ok::<(), anyhow::Error>(())
    })
        .await??;
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
	let dbs = Arc::new(new_ds("test", "test").await?);
	let session = Session::owner().with_ns("test").with_db("test");

	// Define the table and the index.
	dbs.execute(
		"DEFINE TABLE t; DEFINE INDEX ix ON t FIELDS v HNSW DIMENSION 20 DIST EUCLIDEAN TYPE F32 EFC 150 M 8; ",
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
