mod helpers;

use std::time::Duration;

use anyhow::Result;
use helpers::Test;
use surrealdb_core::doc::AsyncEventRecord;
use tokio::time::{sleep, timeout};

#[tokio::test]
#[test_log::test]
async fn test_async_event() -> Result<()> {
	let sql = r#"
		DEFINE EVENT test ON TABLE user ASYNC RETRY 3 MAXDEPTH 10 WHEN true THEN (
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
		"{ events: { test: 'DEFINE EVENT test ON user ASYNC RETRY 3 MAXDEPTH 10 WHEN true THEN (CREATE activity SET user = $parent.id, `value` = $after.email, action = $event, time = time::now())' }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }",
	)?;
	t.expect_val("[{ email: 'info@surrealdb.com', id: user:test }]")?;
	t.expect_val("[{ email: 'info@surrealdb.com', id: user:test }]")?;
	t.expect_val("[{ email: 'test@surrealdb.com', id: user:test }]")?;

	let Test {
		ds,
		session,
		..
	} = t;

	// Process the event asynchronously
	timeout(Duration::from_secs(10), async {
		while AsyncEventRecord::process_next_events_batch(&ds).await? != 0 {
			sleep(Duration::from_millis(100)).await;
		}
		Ok::<_, anyhow::Error>(())
	})
	.await??;

	let mut t = Test::new_ds_session(
		ds,
		session,
		"(SELECT * FROM activity ORDER BY time).{ action, user, value };",
	)
	.await?;
	t.expect_size(1)?;
	t.expect_val(
		"[{ action: 'CREATE', user: user:test, value: 'info@surrealdb.com' }, { action: 'UPDATE', user: user:test, value: 'info@surrealdb.com' }, { action: 'UPDATE', user: user:test, value: 'test@surrealdb.com' }]",
	)?;

	Ok(())
}
