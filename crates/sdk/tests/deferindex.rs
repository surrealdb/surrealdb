mod helpers;

use helpers::new_ds;
use std::sync::Arc;
use std::time::Duration;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread")]
async fn insert_parallel_full_text() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	// Define analyzer and index
	let sql = "
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX title_index ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS DEFER;
	";
	dbs.execute(sql, &ses, None).await?;

	let dbs = Arc::new(dbs);
	let mut tasks = Vec::new();

	for i in 0..100 {
		let dbs = dbs.clone();
		let ses = ses.clone();
		tasks.push(tokio::spawn(async move {
			let sql = format!("INSERT INTO blog {{ title: 'Title {}' }};", i);
			let mut res = dbs.execute(&sql, &ses, None).await?;
			res.remove(0).result?;
			Ok::<(), Error>(())
		}));
	}

	for task in tasks {
		task.await.unwrap()?;
	}

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
