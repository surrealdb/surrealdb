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

	// Verify stm block worked
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());

	// Verify the only result in the stm block is the live query ID
	let actual = res.remove(0).result?;
	let live_query_id = match actual {
		Value::Uuid(uuid) => uuid,
		_ => panic!("Expected a UUID, got {:?}", actual),
	};
	assert!(!live_query_id.is_nil());

	// Create a notification
	let res = &mut dbs.execute("CREATE lq_test_123;", &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());

	dbs.process_lq_notifications().await?;
	let notifs_recv = dbs.notifications().expect("notifications should be set");
	assert!(notifs_recv.recv().await.is_ok());
	assert!(notifs_recv.recv().await.is_err());

	Ok(())
}
