#![allow(clippy::unwrap_used)]
mod helpers;
use anyhow::Result;
use helpers::*;
use std::time::{Duration, SystemTime};
use surrealdb_core::dbs::Session;
use surrealdb_types::Value;
use tokio::time::sleep;

async fn alter_statement_index_decommissioned(def_index: &str, skip_def: usize) -> Result<()> {
	let session = Session::owner().with_ns("test").with_db("test");
	let ds = new_ds().await?;

	// Populate initial records
	// We need enough records so we reach at least 5 seconds
	let mut r = ds
		.execute(
			"CREATE |user:40000| SET
						   email = string::concat('user', rand::string(8), '@example.com'),
						   created_at = time::now() RETURN NONE;",
			&session,
			None,
		)
		.await?;
	skip_ok(&mut r, 1)?;

	// Create the index concurrently
	let mut r = ds.execute(def_index, &session, None).await?;
	assert_eq!(r.len(), skip_def);
	skip_ok(&mut r, skip_def)?;

	// Let's wait a bit
	sleep(Duration::from_millis(500)).await;

	// Decommissions then index
	let mut r = ds.execute("ALTER INDEX test ON user DECOMMISSION", &session, None).await?;
	skip_ok(&mut r, 1)?;

	// Loop until the index built is in error
	let now = SystemTime::now();
	// While the concurrent indexing is running, we update and delete records
	let time_out = Duration::from_secs(300);
	loop {
		if now.elapsed().map_err(|e| anyhow::anyhow!(e.to_string()))?.gt(&time_out) {
			panic!("Time-out {time_out:?}");
		}

		// We monitor the status
		let mut r = ds.execute("INFO FOR INDEX test ON user", &session, None).await?;
		let tmp = r.remove(0).result?;
		if let Value::Object(o) = &tmp
			&& let Some(Value::Object(o)) = o.get("building")
			&& let Some(Value::String(s)) = o.get("status")
		{
			match s.as_str() {
				"started" | "cleaning" | "indexing" => {
					sleep(Duration::from_millis(200)).await;
					continue;
				}
				"error" => {
					// We expect the decommission to be reported
					assert_eq!(
						tmp.into_json_value().to_string(),
						"{\"building\":{\"error\":\"Index building has been cancelled: Decommissioned.\",\"status\":\"error\"}}"
					);
					break;
				}
				"ready" => {
					panic!("We should not be ready!");
				}
				_ => {}
			}
		}
		panic!("Invalid info: {tmp:#?}");
	}
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_statement_index_concurrently_full_text() -> Result<()> {
	alter_statement_index_decommissioned(
		"DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX test ON user FIELDS email FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;",
		2,
	)
	.await
}
