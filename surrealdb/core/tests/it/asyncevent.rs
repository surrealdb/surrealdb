use std::time::Duration;

use anyhow::Result;
use surrealdb_core::kvs::Datastore;
use tokio::time::timeout;

use crate::helpers::Test;

/// Drains this node's async event queue: one drain runs the events an event
/// queues as it runs, so it returns with the queue empty.
async fn wait_for_events_processing(ds: &Datastore) -> Result<()> {
	timeout(Duration::from_secs(10), ds.drain_async_events()).await??;
	Ok(())
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

	// The three events run on concurrent workers, so the order their bodies
	// evaluate `time::now()` in is not fixed; the rows are ordered by what
	// each event recorded instead.
	let mut t = t
		.new_sql("(SELECT * FROM activity ORDER BY action, value).{ action, user, value };")
		.await?;
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

/// An async event's write is an ordinary write: a live query on the table it
/// writes to is notified of it.
#[tokio::test]
async fn an_async_event_write_notifies_a_live_query() -> Result<()> {
	use surrealdb_core::dbs::Session;
	use surrealdb_types::{Action, Kind};

	use crate::helpers::new_ds;

	let (channel, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let mut res = ds
		.execute(
			"DEFINE TABLE test SCHEMALESS;
			LIVE SELECT * FROM test;
			DEFINE EVENT evt ON TABLE test ASYNC MAXDEPTH 1 WHEN true THEN {
				UPDATE $after.id SET data = 'set by the event';
			};
			CREATE test:1;",
			&ses,
			None,
		)
		.await?;
	assert_eq!(res.len(), 4);
	assert_eq!(res.remove(1).result?.kind(), Kind::Uuid);

	let created = timeout(Duration::from_secs(5), channel.recv()).await??;
	assert_eq!(created.action, Action::Create);

	wait_for_events_processing(&ds).await?;

	let updated = timeout(Duration::from_secs(5), channel.recv())
		.await
		.map_err(|_| anyhow::anyhow!("the event's UPDATE never reached the live query"))??;
	assert_eq!(updated.action, Action::Update);
	Ok(())
}

/// Two transactions that each append to the same array through an event
/// overlap, and run the schema of issue 6246 with the event `kind` given.
///
/// The event body sleeps after its write, so two runs of it overlap whichever
/// transaction they belong to: under `ASYNC` both queued runs have written the
/// shared record before either commits. Answers how many writing transactions
/// lost a conflict, and the shared array, sorted, as SurrealQL.
async fn concurrent_appends_through_an_event(kind: &str) -> Result<(usize, String)> {
	use surrealdb_core::dbs::Session;
	use surrealdb_types::{QueryError, ToSql};

	let (_notify, ds) = crate::helpers::new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let setup = format!(
		"DEFINE TABLE user SCHEMALESS;
		 DEFINE TABLE spaceevent SCHEMALESS;
		 DEFINE EVENT link ON spaceevent {kind} WHEN $before.author != $after.author THEN {{
			IF $after.author {{ UPDATE $after.author SET events += $after.id }};
			SLEEP 200ms;
		 }};
		 CREATE user:1 SET events = [];"
	);
	for res in ds.execute(&setup, &ses, None).await? {
		res.result?;
	}
	let tasks: Vec<_> = ["a", "b"]
		.into_iter()
		.map(|id| {
			let ds = ds.clone();
			let ses = ses.clone();
			let sql =
				format!("BEGIN; CREATE spaceevent:{id} SET author = user:1; SLEEP 300ms; COMMIT;");
			tokio::spawn(async move { ds.execute(&sql, &ses, None).await })
		})
		.collect();
	let mut conflicts = 0;
	for task in tasks {
		let responses = task.await??;
		if responses.iter().any(|r| {
			r.result
				.as_ref()
				.is_err_and(|e| e.query_details() == Some(&QueryError::TransactionConflict))
		}) {
			conflicts += 1;
		}
	}
	wait_for_events_processing(&ds).await?;
	let events =
		ds.execute("RETURN array::sort(user:1.events)", &ses, None).await?.remove(0).result?;
	Ok((conflicts, events.to_sql()))
}

/// A synchronous event's write is part of the writing transaction, so two
/// transactions that append to the same record through one conflict at commit,
/// and the loser is told so that it can run again.
#[tokio::test(flavor = "multi_thread")]
async fn a_sync_event_appending_to_a_shared_record_conflicts() -> Result<()> {
	let (conflicts, events) = concurrent_appends_through_an_event("").await?;
	assert_eq!(conflicts, 1, "exactly one of two overlapping transactions loses");
	assert!(
		events == "[spaceevent:a]" || events == "[spaceevent:b]",
		"only the winner's append is stored: {events}"
	);
	Ok(())
}

/// An `ASYNC` event runs in a transaction of its own, which is run again in
/// place when it loses a conflict, so both appends land.
#[tokio::test(flavor = "multi_thread")]
async fn an_async_event_appending_to_a_shared_record_applies_both() -> Result<()> {
	let (conflicts, events) = concurrent_appends_through_an_event("ASYNC").await?;
	assert_eq!(conflicts, 0, "the writing transactions do not touch the shared record");
	assert_eq!(events, "[spaceevent:a, spaceevent:b]", "each event's append is applied once");
	Ok(())
}
