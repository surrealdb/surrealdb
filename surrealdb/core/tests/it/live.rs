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

// `LIVE` statements sent together over one task, as one connection's requests
// are, each register in a write transaction that does not yield to the next:
// they may not conflict with one another on the table's stored definition.
// Thirty per round, resumed in the order they are woken, share one task's
// cooperative budget, which runs out within a round: a registration that
// yields when it does lets a later one commit first. A storage read offloaded
// under load still yields, hence the small tolerance.
#[cfg(feature = "kv-rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_lives_on_one_task_do_not_conflict() -> Result<()> {
	use futures::StreamExt;
	use surrealdb_core::kvs::Datastore;
	use surrealdb_rpc::capabilities::Capabilities;

	const ROUNDS: usize = 20;
	const PER_ROUND: usize = 30;

	let (send, _recv) = surrealdb_core::channel::bounded(1000);
	let dir = temp_dir::TempDir::new()?;
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_capabilities(Capabilities::all())
		.with_notify(send)
		.build_with_path(&format!("rocksdb://{}", dir.path().display()))
		.await?;
	crate::helpers::new_ns_db(&ds, "test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	let mut conflicts = 0;
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
					let message = e.to_string().to_lowercase();
					if message.contains("conflict") || message.contains("resource busy") {
						conflicts += 1;
					} else {
						return Err(anyhow::anyhow!("a LIVE failed: {message}"));
					}
				}
			}
		}
	}
	println!("LIVE conflicts: {conflicts} of {}", ROUNDS * PER_ROUND);
	assert!(
		conflicts <= ROUNDS * PER_ROUND / 100,
		"{conflicts} of {} LIVE statements conflicted",
		ROUNDS * PER_ROUND
	);
	Ok(())
}
