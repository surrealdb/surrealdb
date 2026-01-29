mod helpers;

use anyhow::Result;
use helpers::Test;
use surrealdb_core::doc::AsyncEventRecord;

#[tokio::test]
async fn test_async_event() -> Result<()> {
	let sql = r#"
		DEFINE EVENT test ON TABLE user ASYNC RETRY 1 MAXDEPTH 6 WHEN true THEN (
			CREATE activity SET user = $parent.id, value = $after.email, action = $event, time = time::now()
		);
		INFO FOR TABLE user;
		UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now() RETURN id, email;
		UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now() RETURN id, email;
		UPSERT user:test SET email = 'test@surrealdb.com', updated_at = time::now() RETURN id, email;
		SLEEP 1s;
		(SELECT * FROM activity ORDER BY time).{ action, user, value };
	"#;

	let mut t = Test::new(sql).await?;
	t.expect_size(7)?;
	t.expect_val("NONE")?;
	t.expect_val(
		"{ events: { test: 'DEFINE EVENT test ON user ASYNC RETRY 1 MAXDEPTH 6 WHEN true THEN (CREATE activity SET user = $parent.id, `value` = $after.email, action = $event, time = time::now())' }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }",
	)?;
	t.expect_val("[{ email: 'info@surrealdb.com', id: user:test }]")?;
	t.expect_val("[{ email: 'info@surrealdb.com', id: user:test }]")?;
	t.expect_val("[{ email: 'test@surrealdb.com', id: user:test }]")?;
	t.expect_val("NONE")?;

	let Test {
		ds,
		session,
		..
	} = t;

	// Process the event asynchronously
	assert_eq!(AsyncEventRecord::process_next_events_batch(&ds).await?, 3);

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
