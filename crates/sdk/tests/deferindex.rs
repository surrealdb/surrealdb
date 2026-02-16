mod helpers;

use helpers::new_ds;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;
use surrealdb_core::kvs::Datastore;
use tokio::time::timeout;
use tracing::{info, trace};

async fn concurrent_tasks<F>(
	dbs: Arc<Datastore>,
	session: &Session,
	task_count: usize,
	sql_func: F,
) -> Result<(), Error>
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
			// Ignore errors
			if let Err(e) = res.remove(0).result {
				trace!("Concurrent task error: {sql} - {e}")
			}
			Ok::<(), Error>(())
		}));
	}

	for task in tasks {
		task.await.unwrap()?;
	}
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn insert_parallel_full_text() -> Result<(), Error> {
	let dbs = Arc::new(new_ds().await?);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Define analyzer and index
	let sql = "
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX title_index ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS DEFER;
	";
	dbs.execute(sql, &ses, None).await?;

	// Insert records concurrently
	concurrent_tasks(dbs.clone(), &ses, 100, |i| {
		Cow::Owned(format!("INSERT INTO blog {{ title: 'Title {}' }};", i))
	})
	.await?;

	// Verify counts
	let expected = surrealdb_core::syn::value("[{ count: 100 }]").unwrap();
	timeout(Duration::from_secs(60), async {
		loop {
			let mut res = dbs.execute("SELECT count() FROM blog GROUP ALL;", &ses, None).await?;
			let val = res.remove(0).result?;
			if expected.equal(&val) {
				break;
			}
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
		Ok::<(), Error>(())
	})
	.await
	.map_err(|_| Error::QueryTimedout)??;

	// Verify index works again (optional, but keeps original structure)
	timeout(Duration::from_secs(60), async {
		loop {
			let res = &mut dbs
				.execute("SELECT * FROM blog WHERE title @0@ 'Title 50';", &ses, None)
				.await?;
			let result = res.remove(0).result?;
			if let Value::Array(arr) = result {
				if arr.len() == 1 {
					break;
				}
			}
		}
		Ok::<(), Error>(())
	})
	.await
	.map_err(|_| Error::QueryTimedout)??;

	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn deferred_index_survives_restart() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	// Define analyzer and index
	let sql = "
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX title_index ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS DEFER;
	";
	dbs.execute(sql, &ses, None).await?;

	for i in 0..100 {
		let sql = format!("INSERT INTO blog {{ title: 'Title {}' }};", i);
		let mut res = dbs.execute(&sql, &ses, None).await?;
		res.remove(0).result?;
	}

	let dbs = dbs.restart();

	let expected = surrealdb_core::syn::value("[{ count: 100 }]")?;
	timeout(Duration::from_secs(60), async {
		loop {
			let mut res = dbs.execute("SELECT count() FROM blog GROUP ALL;", &ses, None).await?;
			let val = res.remove(0).result?;
			if expected.equal(&val) {
				break;
			}
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
		Ok::<(), Error>(())
	})
	.await
	.map_err(|_| Error::QueryTimedout)??;

	timeout(Duration::from_secs(60), async {
		loop {
			let res = &mut dbs
				.execute("SELECT * FROM blog WHERE title @0@ 'Title 50';", &ses, None)
				.await?;
			let result = res.remove(0).result?;
			if let Value::Array(arr) = result {
				if arr.len() == 1 {
					break;
				}
			}
			tokio::time::sleep(Duration::from_millis(100)).await;
		}
		Ok::<(), Error>(())
	})
	.await
	.map_err(|_| Error::QueryTimedout)??;

	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
// Check this issue: https://github.com/surrealdb/surrealdb/issues/6837
async fn multi_index_concurrent_test() -> Result<(), Error> {
	let sql = "DEFINE TABLE aaa;
	DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase, ascii, edgengram(1, 10);;
	DEFINE INDEX field1 ON aaa FIELDS field1 SEARCH ANALYZER simple BM25 HIGHLIGHTS DEFER;
	DEFINE INDEX field2 ON aaa FIELDS field2 SEARCH ANALYZER simple BM25 HIGHLIGHTS DEFER;
	DEFINE INDEX field3 ON aaa FIELDS field3 SEARCH ANALYZER simple BM25 HIGHLIGHTS DEFER;
	DEFINE INDEX field4 ON aaa FIELDS field4 SEARCH ANALYZER simple BM25 HIGHLIGHTS DEFER;
	DEFINE INDEX field5 ON aaa FIELDS field5 SEARCH ANALYZER simple BM25 HIGHLIGHTS DEFER;
";
	let dbs = Arc::new(new_ds().await?);
	let ses = Session::owner().with_ns("test").with_db("test");
	// Define analyzer and indexes
	dbs.execute(sql, &ses, None).await?;

	batch_ingestion(dbs.clone(), &ses, 10).await?;
	let expected_total_count = 1000;

	info!("Waiting for index to be built");
	timeout(Duration::from_secs(300), async {
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
			// INFO FOR INDEX
			let val = res.remove(0).result?;
			let Value::Object(o) = val else {
				panic!("Invalid result format: {val:#}")
			};
			let building = o.get("building").unwrap();
			let Value::Object(building) = building else {
				panic!("Invalid result format: {building:#}")
			};
			let status = building.get("status").unwrap();
			let Value::Strand(status) =  status else {
				panic!("Invalid result format: {status:#}")
			};
			// Check that the status is valid (no error)
			if status.0 != "ready" && status.0 != "indexing" && status.0 != "cleaning" {
				panic!("Invalid index status: {status:#}")
			}
			// Collect the index count
			let val = res.remove(0).result?;
			let Value::Number(index_count) = val else {
				panic!("Invalid result: {val:#}")
			};
			// Collect the real count
			let val = res.remove(0).result?;
			let Value::Array(a) = &val else {
				panic!("Invalid result format: {val:#}")
			};
			let mut real_total_count = 0;
			// Collect count for the different values of field1 and compute the total
			for item in a.iter() {
				let Value::Array(record) = item else {
					panic!("Invalid result format: {item:#}")
				};
				let count = record.get(1).unwrap();
				let Value::Number(count) = count else {
					panic!("Invalid result format: {count:#}")
				};
				real_total_count += count.as_usize();
			}
			info!("Real count: {real_total_count} - Index: count: {index_count} - Index status: {status}");
			if real_total_count == expected_total_count {
				if index_count.as_usize() == expected_total_count {
					// SUCCESS!
					break;
				}
			}
			// Temporisation
			tokio::time::sleep(Duration::from_millis(1000)).await;
		}
		Ok::<(), Error>(())
	})
	.await
	.map_err(|_| Error::QueryTimedout)??;
	Ok(())
}

async fn batch_ingestion(
	dbs: Arc<Datastore>,
	ses: &Session,
	batch_count: usize,
) -> Result<(), Error> {
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
			THROW 'Ooch';
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
	concurrent_tasks(dbs.clone(), ses, batch.len(), |i| Cow::Borrowed(batch.get(i).unwrap())).await
}
