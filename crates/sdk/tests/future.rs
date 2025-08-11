mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::err::Error;

#[tokio::test]
#[ignore]
async fn concurrency() -> Result<()> {
	// cargo test --package surrealdb --test future --features kv-mem --release --
	// concurrency --nocapture

	const MILLIS: usize = 50;

	// If all futures complete in less than double `MILLIS`, then they must have
	// executed concurrently. Otherwise, some executed sequentially.
	const TIMEOUT: usize = MILLIS * 19 / 10;

	/// Returns a query that will execute `count` futures that each wait for
	/// `millis`
	fn query(count: usize, millis: usize) -> String {
		// TODO: Find a simpler way to trigger the concurrent future case.
		format!(
			"SELECT foo FROM [[{}]] TIMEOUT {TIMEOUT}ms;",
			(0..count)
				.map(|i| format!("{{[sleep({millis}ms), {{foo: {i}}}]}}"))
				.collect::<Vec<_>>()
				.join(", ")
		)
	}

	/// Returns `true` if `limit` futures are concurrently executed.
	async fn test_limit(limit: usize) -> Result<bool> {
		let sql = query(limit, MILLIS);
		let dbs = new_ds().await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		let res = dbs.execute(&sql, &ses, None).await;

		match res {
			Err(err) => {
				if matches!(err.downcast_ref(), Some(Error::QueryTimedout)) {
					Ok(false)
				} else {
					Err(err)
				}
			}
			Ok(res) => {
				assert_eq!(res.len(), 1);

				let res = res.into_iter().next().unwrap();

				let elapsed = res.time.as_millis() as usize;

				Ok(elapsed < TIMEOUT)
			}
		}
	}

	// Diagnostics.
	/*
	for i in (1..=80).step_by(8) {
		println!("{i} futures => {}", test_limit(i).await?);
	}
	*/

	assert!(test_limit(3).await?);

	// Too slow to *parse* query in debug mode.
	#[cfg(not(debug_assertions))]
	assert!(!test_limit(64 /* surrealdb::cnf::MAX_CONCURRENT_TASKS */ + 1).await?);

	Ok(())
}
