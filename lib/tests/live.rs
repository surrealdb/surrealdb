mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;
use surrealdb_core::fflags::FFLAGS;
use surrealdb_core::kvs::LockType::Optimistic;
use surrealdb_core::kvs::TransactionType::Write;

// RUST_LOG=trace cargo test -p surrealdb --features kv-mem --test live -- --nocapture

#[tokio::test]
async fn live_query_sends_registered_lq_details() -> Result<(), Error> {
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

	// Define table didnt fail
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());

	// Live query returned a valid uuid
	let actual = res.remove(0).result?;
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

	dbs.process_lq_notifications(&Default::default()).await?;

	let notifications_chan = dbs.notifications().unwrap();

	assert!(notifications_chan.try_recv().is_ok());
	assert!(notifications_chan.try_recv().is_err());

	Ok(())
}
