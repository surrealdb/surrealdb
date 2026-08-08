use std::time::Duration;

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;
use surrealdb_types::{Action, Kind, RecordId, Value, vars};

use crate::helpers::{new_ds, skip_ok};

#[tokio::test]
async fn live_permissions() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;

	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let sql = "
			DEFINE TABLE test SCHEMAFULL PERMISSIONS
				FOR create WHERE { THROW 'create' }
				FOR select WHERE { THROW 'select' }
				FOR update WHERE { THROW 'update' }
				FOR delete WHERE { THROW 'delete' };
			CREATE test:1;
		";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	skip_ok(res, 1)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:1,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let ses = Session::for_record(
		"test",
		"test",
		"test",
		Value::RecordId(RecordId::new("user".to_owned(), "test".to_owned())),
	)
	.with_rt(true);
	let sql = "
		LIVE SELECT * FROM type::table('test');
		CREATE test:2;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	skip_ok(res, 1)?;
	//
	let tmp = res.remove(0).result.unwrap_err().to_string();
	let val = "An error occurred: create".to_string();
	assert_eq!(tmp, val);
	//
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let sql = "CREATE test:3;";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:3,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn live_document_reduction() -> Result<()> {
	// Create a new datastore with notifications enabled
	let (channel, dbs) = new_ds("test", "test", true).await?;

	// Create sessions for owner and record user
	let ses_owner = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let ses_record = Session::for_record(
		"test",
		"test",
		"test",
		Value::RecordId(RecordId::new("user".to_owned(), "test".to_owned())),
	)
	.with_rt(true);

	// Setup the scenario
	let sql = "
			DEFINE TABLE test SCHEMAFULL PERMISSIONS FULL;
			DEFINE FIELD visible ON test PERMISSIONS FULL;
			DEFINE FIELD hidden ON test PERMISSIONS NONE;
		";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 3);
	skip_ok(res, 3)?;

	////////////////////////////////////////////////////////////

	// Create a simple live query
	let sql = "LIVE SELECT * FROM test;";
	let res = &mut dbs.execute(sql, &ses_record, None).await?;
	assert_eq!(res.len(), 1);
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	////////////////////////////////////////////////////////////

	// Create a record
	let sql = "CREATE test:1 SET hidden = 123, visible = 'abc';";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:1,
				visible: 'abc',
				hidden: 123,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	// Receive the notification
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Create);

	// Check the notification
	let val = syn::value(
		"{
			id: test:1,
			visible: 'abc',
		}",
	)
	.unwrap();
	assert_eq!(tmp.result, val);

	////////////////////////////////////////////////////////////

	// Update the record
	let sql = "UPDATE test:1 SET hidden = 456, visible = 'def';";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:1,
				visible: 'def',
				hidden: 456,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	// Receive the notification
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Update);

	// Check the notification
	let val = syn::value(
		"{
			id: test:1,
			visible: 'def',
		}",
	)
	.unwrap();
	assert_eq!(tmp.result, val);

	////////////////////////////////////////////////////////////

	// Delete the record
	let sql = "DELETE test:1;";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	// Receive the notification
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Delete);

	// Check the notification
	let val = syn::value(
		"{
			id: test:1,
			visible: 'def',
		}",
	)
	.unwrap();
	assert_eq!(tmp.result, val);

	////////////////////////////////////////////////////////////

	// Kill the live query
	let sql = "KILL $uuid";
	let res = &mut dbs.execute(sql, &ses_owner, Some(vars! { uuid: lqid })).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	// Receive the notification
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Killed);

	// Create a live query with a WHERE clause
	let sql = "LIVE SELECT * FROM test WHERE hidden = 123;";
	let res = &mut dbs.execute(sql, &ses_record, None).await?;
	assert_eq!(res.len(), 1);
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	////////////////////////////////////////////////////////////

	// Create a record
	let sql = "CREATE test:2 SET hidden = 123, visible = 'abc';";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:2,
				visible: 'abc',
				hidden: 123,
			},
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	// Assert no notification is received
	tokio::time::sleep(Duration::from_secs(1)).await;
	let res = channel.try_recv();
	assert!(res.is_err());

	////////////////////////////////////////////////////////////

	// Test passed!
	Ok(())
}

#[tokio::test]
async fn test_live_with_variables() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;

	// Setup
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let sql = "
		DEFINE TABLE test;
		DEFINE FIELD num ON test TYPE number;
	";

	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	skip_ok(res, 2)?;

	// Start live query
	let sql = "LIVE SELECT * FROM test WHERE num = $num;";
	let res = &mut dbs.execute(sql, &ses, Some(vars!("num": 123))).await?;
	assert_eq!(res.len(), 1);
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	// Triggers notification
	let sql = "CREATE test:1 SET num = 123;";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	// Does not trigger notification
	let sql = "UPDATE test:1 SET num = 456;";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	// Kill live query
	let sql = "KILL $uuid";
	let res = &mut dbs.execute(sql, &ses, Some(vars!("uuid": lqid))).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	// Receive notification
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Create);
	assert_eq!(tmp.result, syn::value("{ id: test:1, num: 123 }")?);
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Killed);

	Ok(())
}

// REMOVE TABLE must invalidate the datastore-wide live-query cache, otherwise
// a write to a same-name recreated table will be processed against the killed
// subscription's cached entry and deliver a notification to a client whose
// LIVE query the server has already declared Killed.
#[tokio::test]
async fn test_remove_table_invalidates_live_cache() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;

	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	// Define the table and start a live query against it.
	let sql = "
		DEFINE TABLE tb;
		LIVE SELECT * FROM tb;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	skip_ok(res, 1)?;
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	// First write warms the datastore live-queries cache via `Document::lv()`.
	let sql = "CREATE tb:1;";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Create);

	// REMOVE TABLE sends a Killed notification...
	let sql = "REMOVE TABLE tb;";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Killed);

	// ...and must also bump the datastore live-query cache version, so a write
	// to a recreated same-name table does not see the stale subscription.
	let sql = "
		DEFINE TABLE tb;
		CREATE tb:2;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	skip_ok(res, 2)?;

	// No further notification must be delivered: the live query was killed,
	// and the recreated table has no live queries registered against it.
	tokio::time::sleep(Duration::from_millis(500)).await;
	assert!(channel.try_recv().is_err());

	Ok(())
}

// REMOVE DATABASE destroys every `lq` row in the database as part of the
// deferred prefix delete, so every subscriber in it is owed a Killed. Before
// this was wired up the statement notified nobody and every LIVE client on
// every table in the database waited forever on keys that no longer existed.
#[tokio::test]
async fn test_remove_database_kills_live_queries() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;

	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	// Two tables, one subscription each, so the walk over `all_tb` is exercised
	// rather than just the single-table path.
	let sql = "
		DEFINE TABLE tb1;
		DEFINE TABLE tb2;
		LIVE SELECT * FROM tb1;
		LIVE SELECT * FROM tb2;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	skip_ok(res, 2)?;
	let lq1 = res.remove(0).result?;
	let lq2 = res.remove(0).result?;
	assert_eq!(lq1.kind(), Kind::Uuid);
	assert_eq!(lq2.kind(), Kind::Uuid);

	let sql = "REMOVE DATABASE test;";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	// Exactly one Killed per subscription, in either order.
	let mut killed = vec![];
	for _ in 0..2 {
		let tmp = channel.recv().await?;
		assert_eq!(tmp.action, Action::Killed);
		killed.push(Value::Uuid(tmp.id));
	}
	killed.sort();
	let mut expected = vec![lq1, lq2];
	expected.sort();
	assert_eq!(killed, expected);

	// And nothing further.
	tokio::time::sleep(Duration::from_millis(500)).await;
	assert!(channel.try_recv().is_err());

	Ok(())
}

// The same for REMOVE NAMESPACE, which additionally walks `all_db`.
#[tokio::test]
async fn test_remove_namespace_kills_live_queries() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;

	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let sql = "
		DEFINE TABLE tb;
		LIVE SELECT * FROM tb;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	skip_ok(res, 1)?;
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	let sql = "REMOVE NAMESPACE test;";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Killed);
	assert_eq!(Value::Uuid(tmp.id), lqid);

	tokio::time::sleep(Duration::from_millis(500)).await;
	assert!(channel.try_recv().is_err());

	Ok(())
}

// The Killed notifications are owed only if the removal becomes durable.
// Sending them eagerly would tear down every subscription in the database for
// a statement a later rollback undoes, leaving the restored `lq` rows with no
// listener.
#[tokio::test]
async fn test_cancelled_remove_database_sends_no_kill() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;

	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let sql = "
		DEFINE TABLE tb;
		LIVE SELECT * FROM tb;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	skip_ok(res, 2)?;

	let sql = "
		BEGIN;
		REMOVE DATABASE test;
		CANCEL;
	";
	let _ = dbs.execute(sql, &ses, None).await?;

	tokio::time::sleep(Duration::from_millis(500)).await;
	assert!(
		channel.try_recv().is_err(),
		"a cancelled REMOVE DATABASE must not tell subscribers they are dead"
	);

	Ok(())
}

// Redefining a table as a view wipes its whole key range, `lq` rows included,
// so it owes its subscribers a Killed for the same reason `REMOVE TABLE` does.
// The obligation follows the key range rather than the statement, which is why
// it was missed when the fan-out was first written at the REMOVE statements.
#[tokio::test]
async fn test_overwrite_table_as_view_kills_live_queries() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;

	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let sql = "
		DEFINE TABLE src;
		DEFINE TABLE tb;
		LIVE SELECT * FROM tb;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	skip_ok(res, 2)?;
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	let sql = "DEFINE TABLE OVERWRITE tb AS SELECT count() FROM src GROUP ALL;";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Killed);
	assert_eq!(Value::Uuid(tmp.id), lqid);

	tokio::time::sleep(Duration::from_millis(500)).await;
	assert!(channel.try_recv().is_err());

	Ok(())
}

// A subscription captures the `Auth` of whoever ran the LIVE and replays it on
// every notification, so revoking that principal does not stop delivery on its
// own: without teardown, `REMOVE USER u` leaves u's subscriptions streaming
// rows to a still-open socket, evaluated under u's permissions.
#[tokio::test]
async fn test_remove_user_kills_their_live_queries() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;

	let owner = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let sql = "
		DEFINE TABLE tb PERMISSIONS FULL;
		DEFINE USER alice ON DATABASE PASSWORD 'x' ROLES OWNER;
	";
	let res = &mut dbs.execute(sql, &owner, None).await?;
	assert_eq!(res.len(), 2);
	skip_ok(res, 2)?;

	// alice subscribes.
	let alice = Session::for_level(
		surrealdb_iam::Level::Database("test".into(), "test".into()),
		surrealdb_iam::Role::Owner,
	)
	.with_ns("test")
	.with_db("test")
	.with_rt(true);
	let alice = Session {
		au: std::sync::Arc::new(surrealdb_iam::Auth::new(surrealdb_iam::Actor::new(
			"alice".to_string(),
			vec![surrealdb_iam::Role::Owner],
			surrealdb_iam::Level::Database("test".into(), "test".into()),
		))),
		..alice
	};
	let res = &mut dbs.execute("LIVE SELECT * FROM tb", &alice, None).await?;
	assert_eq!(res.len(), 1);
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	// Revoking alice must tear her subscription down.
	let res = &mut dbs.execute("REMOVE USER alice ON DATABASE", &owner, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Killed);
	assert_eq!(Value::Uuid(tmp.id), lqid);

	// And no further notification reaches her.
	let res = &mut dbs.execute("CREATE tb:1", &owner, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;
	tokio::time::sleep(Duration::from_millis(500)).await;
	assert!(channel.try_recv().is_err(), "a revoked principal must stop receiving rows");

	Ok(())
}
