mod helpers;
mod parse;

use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::fflags::FFLAGS;
use surrealdb::sql::Value;

#[tokio::test]
async fn live_query_fails_if_no_change_feed() -> Result<(), Error> {
	if !FFLAGS.change_feed_live_queries.enabled() {
		return Ok(());
	}
	let sql = "
		LIVE SELECT * FROM lq_test_123;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let res = res.remove(0).result;
	assert!(res.is_err(), "{:?}", res);
	let err = res.as_ref().err().unwrap();
	assert_eq!(
		format!("{}", err),
		"Failed to process Live Query: The Live Query must have a change feed for it it work"
	);
	Ok(())
}

#[tokio::test]
async fn live_query_fails_if_change_feed_missing_diff() -> Result<(), Error> {
	if !FFLAGS.change_feed_live_queries.enabled() {
		return Ok(());
	}
	let sql = "
		DEFINE TABLE lq_test_123 CHANGEFEED 10m;
		LIVE SELECT * FROM lq_test_123;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	res.remove(0).result.unwrap();
	let res = res.remove(0).result;
	assert!(res.is_err(), "{:?}", res);
	let err = res.as_ref().err().unwrap();
	assert_eq!(
		format!("{}", err),
		"Failed to process Live Query: The Live Query must have a change feed that includes relative changes"
	);
	Ok(())
}

#[tokio::test]
async fn live_query_sends_registered_lq_details() -> Result<(), Error> {
	if !FFLAGS.change_feed_live_queries.enabled() {
		return Ok(());
	}
	let sql = "
		DEFINE TABLE lq_test_123 CHANGEFEED 10m INCLUDE ORIGINAL;
		LIVE SELECT * FROM lq_test_123;
	";
	let mut stack = reblessive::tree::TreeStack::new();
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);

	// Define table didnt fail
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());

	// Live query returned a valid uuid
	let actual = res.remove(0).result.unwrap();
	let live_id = match actual {
		Value::Uuid(live_id) => live_id,
		_ => panic!("Expected a UUID"),
	};
	assert!(!live_id.is_nil());

	// Create some data
	let res = &mut dbs.execute("CREATE lq_test_123", &ses, None).await?;
	assert_eq!(res.len(), 1);

	let result = res.remove(0);
	assert!(result.result.is_ok());

	let def = Default::default();
	stack.enter(|stk| dbs.process_lq_notifications(stk, &def)).finish().await?;

	let notifications_chan = dbs.notifications().unwrap();

	assert!(notifications_chan.try_recv().is_ok());
	assert!(notifications_chan.try_recv().is_err());

	Ok(())
}

#[tokio::test]
async fn live_query_sends_notification_function_update() -> Result<(), Error> {
	let table = "lq_fn_update_test_123";
	let table_def = match FFLAGS.change_feed_live_queries.enabled() {
		true => format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL;"),
		false => format!("DEFINE TABLE {table};"),
	};
	let sql = format!(
		"
		{table_def}
		CREATE other_table:123;
		CREATE {table};
		DEFINE FUNCTION fn::update_lq_test() {{
			LET $record = SELECT VALUE id FROM {table} LIMIT 1;
			UPDATE $record SET link = other_table:123;
			RETURN $record;
		}};
		LIVE SELECT * FROM {table};
	"
	);
	let mut stack = reblessive::tree::TreeStack::new();
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	println!("SQL: {}", sql);
	let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 5);

	// Define table didnt fail
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());

	// Create other table didn't fail
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());

	// Create table to update didn't fail
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());

	// Define function didnt fail
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());

	// Live query returned a valid uuid
	let actual = res.remove(0).result.unwrap();
	let live_id = match actual {
		Value::Uuid(live_id) => live_id,
		_ => panic!("Expected a UUID"),
	};
	assert!(!live_id.is_nil());

	// Invoke the function to cause an update
	let res = &mut dbs.execute("RETURN fn::update_lq_test();", &ses, None).await?;
	assert_eq!(res.len(), 1);

	let result = res.remove(0);
	assert!(result.result.is_ok());

	// Check there is an update notification
	let def = Default::default();
	stack.enter(|stk| dbs.process_lq_notifications(stk, &def)).finish().await?;

	let notifications_chan = dbs.notifications().unwrap();

	assert!(notifications_chan.try_recv().is_ok());
	assert!(notifications_chan.try_recv().is_err());

	Ok(())
}
