mod helpers;
use std::time::Duration;

use anyhow::Result;
use helpers::*;
use surrealdb_core::dbs::Session;
use surrealdb_types::Value;
use tokio::time::sleep;
use web_time::SystemTime;

async fn alter_statement_index_prepare_remove(def_index: &str, skip_def: usize) -> Result<()> {
	let session = Session::owner().with_ns("test").with_db("test");
	let ds = new_ds("test", "test").await?;

	// Populate initial records
	// We need enough records so that the indexation should last more than 5 seconds
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
	let mut r = ds.execute("ALTER INDEX test ON user PREPARE REMOVE", &session, None).await?;
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
					// We expect "prepare remove" to be reported
					assert_eq!(
						tmp.into_json_value().to_string(),
						"{\"building\":{\"error\":\"Index building has been cancelled: Prepare remove.\",\"status\":\"error\"}}"
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
	alter_statement_index_prepare_remove(
		"DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX test ON user FIELDS email FULLTEXT ANALYZER simple BM25 HIGHLIGHTS CONCURRENTLY;",
		2,
	)
	.await
}
