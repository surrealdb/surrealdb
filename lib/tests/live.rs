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
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let actual = res.remove(0).result?;
	let expected = Value::parse("{}");
	assert_eq!(actual, expected);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[12345]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[56789]");
	assert_eq!(tmp, val);
	//
	Ok(())
}
