mod helpers;

use std::time::Duration;

use anyhow::Result;
use helpers::Test;
use surrealdb_core::doc::AsyncEventRecord;
use surrealdb_core::kvs::Datastore;
use tokio::time::{sleep, timeout};

async fn wait_for_events_processing(ds: &Datastore) -> Result<()> {
	timeout(Duration::from_secs(10), async {
		while AsyncEventRecord::process_next_events_batch(ds, None).await? != 0 {
			sleep(Duration::from_millis(100)).await;
		}
		Ok::<_, anyhow::Error>(())
	})
	.await?
}

#[tokio::test]
#[test_log::test]
async fn test_async_event() -> Result<()> {
	let sql = r#"
		DEFINE EVENT test ON TABLE user ASYNC RETRY 2 MAXDEPTH 0 WHEN true THEN (
			CREATE activity SET user = $parent.id, value = $after.email, action = $event, time = time::now()
		);
		INFO FOR TABLE user;
		UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now() RETURN id, email;
		UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now() RETURN id, email;
		UPSERT user:test SET email = 'test@surrealdb.com', updated_at = time::now() RETURN id, email;
	"#;

	let mut t = Test::new(sql).await?;
	t.expect_size(5)?;
	t.expect_val("NONE")?;
	t.expect_val(
		"{ events: { test: 'DEFINE EVENT test ON user ASYNC RETRY 2 MAXDEPTH 0 WHEN true THEN (CREATE activity SET user = $parent.id, `value` = $after.email, action = $event, time = time::now())' }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }",
	)?;
	t.expect_val("[{ email: 'info@surrealdb.com', id: user:test }]")?;
	t.expect_val("[{ email: 'info@surrealdb.com', id: user:test }]")?;
	t.expect_val("[{ email: 'test@surrealdb.com', id: user:test }]")?;

	// Process the event asynchronously
	wait_for_events_processing(&t.ds).await?;

	let mut t =
		t.new_sql("(SELECT * FROM activity ORDER BY time).{ action, user, value };").await?;
	t.expect_size(1)?;
	t.expect_val(
		"[{ action: 'CREATE', user: user:test, value: 'info@surrealdb.com' }, { action: 'UPDATE', user: user:test, value: 'info@surrealdb.com' }, { action: 'UPDATE', user: user:test, value: 'test@surrealdb.com' }]",
	)?;

	Ok(())
}

#[tokio::test]
#[test_log::test]
async fn test_async_event_max_depth() -> Result<()> {
	let sql = r#"
	    DEFINE EVENT test ON TABLE user ASYNC RETRY 1 MAXDEPTH 5 WHEN true THEN (
			CREATE activity SET user = $parent.id, value = $after.email, action = $event, time = time::now()
		);
		DEFINE EVENT activity_event ON TABLE activity ASYNC RETRY 1 MAXDEPTH 5 WHEN true THEN (
			UPSERT user:test SET email = 'test@surrealdb.com', updated_at = time::now()
		);
		UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now() RETURN id, email;
	"#;

	let mut t = Test::new(sql).await?;
	t.expect_size(3)?;
	t.expect_val("NONE")?;
	t.expect_val("NONE")?;
	t.expect_val("[{ email: 'info@surrealdb.com', id: user:test }]")?;

	// Process the event asynchronously
	wait_for_events_processing(&t.ds).await?;

	let mut t =
		t.new_sql("(SELECT * FROM activity ORDER BY time).{ action, user, value };").await?;
	t.expect_size(1)?;
	t.expect_val(
		"[{ action: 'CREATE', user: user:test, value: 'info@surrealdb.com' }, { action: 'UPDATE', user: user:test, value: 'test@surrealdb.com' }, { action: 'UPDATE', user: user:test, value: 'test@surrealdb.com' }]",
	)?;

	Ok(())
}

#[tokio::test]
#[test_log::test]
async fn test_async_event_retry() -> Result<()> {
	let sql = r#"
		DEFINE EVENT throw_it ON person ASYNC THEN { CREATE blah; };
		CREATE |person:20| RETURN NONE;
	"#;

	let mut t = Test::new(sql).await?;
	t.expect_size(2)?;
	t.expect_vals(&["NONE", "[]"])?;

	// Process the event asynchronously
	wait_for_events_processing(&t.ds).await?;

	let sql = r#"
		REMOVE EVENT throw_it ON person;
	    DEFINE EVENT throw_it ON person ASYNC THEN {
	    	CREATE blah;
	    	THROW "See you in the logs!";
		};
		CREATE |person:10| RETURN NONE;
	"#;

	let mut t = t.new_sql(sql).await?;
	t.expect_size(3)?;
	t.expect_vals(&["NONE", "NONE", "[]"])?;

	// Process the event asynchronously
	wait_for_events_processing(&t.ds).await?;

	let sql = r#"
		count(SELECT * FROM blah);
	"#;
	let mut t = t.new_sql(sql).await?;
	t.expect_size(1)?;
	t.expect_val("20")?;
	Ok(())
}
