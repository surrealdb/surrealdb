mod helpers;

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{ToSql, Value};
use tokio::time::timeout;
use tracing::info;

use crate::helpers::new_ds;

async fn concurrent_tasks<F>(
	dbs: Arc<Datastore>,
	session: &Session,
	task_count: usize,
	sql_func: F,
) -> Result<()>
where
	F: Fn(usize) -> Cow<'static, str>,
{
	let mut tasks = Vec::with_capacity(task_count);

	for i in 0..task_count {
		let dbs = dbs.clone();
		let session = session.clone();
		let sql = sql_func(i);
		tasks.push(tokio::spawn(async move {
			let mut res = dbs.execute(&sql, &session, None).await?;
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
	// Insert content concurrently.
	let sql = "CREATE |aaa:10| CONTENT {
			field1: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
			field2: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
			field3: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
			field4: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
			field5: rand::enum(['cupcake', 'cakecup', 'cheese', 'pie', 'noms']),
		} RETURN NONE;";
	concurrent_tasks(dbs.clone(), ses, batch_count, |_| Cow::Borrowed(sql)).await
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
// Regression test for https://github.com/surrealdb/surrealdb/issues/6837
async fn multi_index_concurrent_test() -> Result<()> {
	let dbs = Arc::new(new_ds("test", "test").await?);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Define the table.
	dbs.execute("DEFINE TABLE aaa", &ses, None).await?;

	// Ingest 500 records.
	batch_ingestion(dbs.clone(), &ses, 50).await?;

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

	// Ingest another 500 records.
	batch_ingestion(dbs.clone(), &ses, 50).await?;

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
