//! Live-query notification behaviour observed through the datastore: when a
//! notification is delivered relative to the writing transaction's commit, and
//! what a subscriber is and is not shown.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use surrealdb_kvs::TransactionType::Write;

use crate::catalog::providers::CatalogProvider;
use crate::channel::Receiver;
use crate::dbs::{Capabilities, Session};
use crate::kvs::Datastore;
use crate::types::{
	PublicAction, PublicNotification, PublicRecordId, PublicRecordIdKey, PublicValue,
};

async fn new_ds_with_broker() -> Result<(Receiver<PublicNotification>, Arc<Datastore>)> {
	let (send, recv) = crate::channel::bounded(1000);
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_auth(false)
		.with_notify(send)
		.build_with_path("memory")
		.await?;
	Ok((recv, ds))
}

/// A write inside a caller-owned transaction notifies live subscribers once
/// the caller commits, and not before.
#[tokio::test]
async fn test_live_notification_from_caller_owned_transaction() {
	use surrealdb_kvs::TransactionType;

	use crate::kvs::QueryRequest;

	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db, tb) = ("test", "test", "person");
	setup_ns_db_table(&ds, ns, db, tb).await;

	let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	ds.execute(&format!("LIVE SELECT * FROM {tb}"), &ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	let tx = Arc::new(ds.transaction(TransactionType::Write).await.unwrap());
	let ses_write = Session::owner().with_ns(ns).with_db(db);
	let mut res = ds
		.run(QueryRequest::new("CREATE person:42", &ses_write).with_transaction(Arc::clone(&tx)))
		.await
		.expect("run should not return an Err");
	res.remove(0).result.expect("CREATE should succeed");

	let early = tokio::time::timeout(tokio::time::Duration::from_millis(200), recv.recv()).await;
	assert!(early.is_err(), "notification delivered before the caller committed");

	tx.commit().await.unwrap();

	let notification = tokio::time::timeout(tokio::time::Duration::from_secs(3), recv.recv())
		.await
		.expect("timed out waiting for the notification")
		.expect("notification channel closed");
	assert_eq!(notification.action, PublicAction::Create);
}

/// A rolled-back caller-owned transaction delivers nothing. The committed
/// write afterwards proves the subscription was live, so its record must be
/// the first one to arrive.
#[tokio::test]
async fn test_no_live_notification_from_rolled_back_caller_owned_transaction() {
	use surrealdb_kvs::TransactionType;

	use crate::kvs::QueryRequest;

	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db, tb) = ("test", "test", "person");
	setup_ns_db_table(&ds, ns, db, tb).await;

	let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	ds.execute(&format!("LIVE SELECT * FROM {tb}"), &ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	let tx = Arc::new(ds.transaction(TransactionType::Write).await.unwrap());
	let ses_write = Session::owner().with_ns(ns).with_db(db);
	let mut res = ds
		.run(QueryRequest::new("CREATE person:99", &ses_write).with_transaction(Arc::clone(&tx)))
		.await
		.expect("run should not return an Err");
	res.remove(0).result.expect("CREATE should succeed");

	tx.cancel().await.unwrap();

	ds.execute("CREATE person:100", &ses_write, None).await.unwrap();

	let notification = tokio::time::timeout(tokio::time::Duration::from_secs(3), recv.recv())
		.await
		.expect("timed out: the subscription delivered nothing at all")
		.expect("notification channel closed");
	assert_eq!(
		notification.record,
		PublicValue::RecordId(PublicRecordId {
			table: tb.to_string().into(),
			key: PublicRecordIdKey::Number(100),
		}),
		"the first notification must be the committed record, not the rolled back one",
	);
}

/// Every batch run against one caller-owned transaction queues its own
/// flush, and a single commit delivers all of them.
#[tokio::test]
async fn test_live_notifications_from_several_batches_in_one_transaction() {
	use surrealdb_kvs::TransactionType;

	use crate::kvs::QueryRequest;

	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db, tb) = ("test", "test", "person");
	setup_ns_db_table(&ds, ns, db, tb).await;

	let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	ds.execute(&format!("LIVE SELECT * FROM {tb}"), &ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	let tx = Arc::new(ds.transaction(TransactionType::Write).await.unwrap());
	let ses_write = Session::owner().with_ns(ns).with_db(db);
	for sql in ["CREATE person:1", "CREATE person:2"] {
		let mut res = ds
			.run(QueryRequest::new(sql, &ses_write).with_transaction(Arc::clone(&tx)))
			.await
			.expect("run should not return an Err");
		res.remove(0).result.expect("CREATE should succeed");
	}

	tx.commit().await.unwrap();

	// Each batch's flush is its own task, so arrival order is not fixed.
	let mut seen = Vec::new();
	for _ in 0..2 {
		let notification = tokio::time::timeout(tokio::time::Duration::from_secs(3), recv.recv())
			.await
			.expect("timed out waiting for a notification")
			.expect("notification channel closed");
		assert_eq!(notification.action, PublicAction::Create);
		seen.push(notification.record);
	}
	let expected = |n: i64| {
		PublicValue::RecordId(PublicRecordId {
			table: tb.to_string().into(),
			key: PublicRecordIdKey::Number(n),
		})
	};
	assert!(seen.contains(&expected(1)), "missing the first batch's notification: {seen:?}");
	assert!(seen.contains(&expected(2)), "missing the second batch's notification: {seen:?}");
}

async fn setup_ns_db_table(ds: &Datastore, ns: &str, db: &str, tb: &str) {
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();
	let ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(&format!("DEFINE TABLE {tb}"), &ses, None).await.unwrap();
}

fn record_user_session(ns: &str, db: &str, user_key: &str) -> Session {
	Session::for_record(
		ns,
		db,
		"user",
		PublicValue::RecordId(PublicRecordId {
			table: "user".to_string().into(),
			key: PublicRecordIdKey::String(user_key.to_string()),
		}),
	)
	.with_rt(true)
}

/// SECURITY (#101): the trusted LIVE `$value` / `$before` / `$after` / `$event` bindings must
/// NOT be shadowed by a user-captured `$value` in `live_subscription.vars`. A user may bind
/// `$value` — by `LET`, by the session, or as a parameter of the request — before registering a
/// LIVE query whose WHERE reads `$value.ok`; if the user's binding won, the WHERE would match
/// every notification whatever the real document said.
///
/// Two things keep it from winning: the subscription's unit is planned with the subscriber's
/// names shadowed, so none of them folds the registering session's value, and
/// [`crate::exec::operators::write::stages::lives::subscriber_frame`] adds the captured
/// variables before the trusted bindings, so the trusted ones overwrite.
#[tokio::test]
async fn test_live_user_value_does_not_shadow_real_document() {
	for bound_by_request in [false, true] {
		let (recv, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_ns_db_table(&ds, ns, db, tb).await;

		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let (sql, vars) = match bound_by_request {
			false => (
				"LET $value = { ok: true }; LIVE SELECT * FROM person WHERE $value.ok = true",
				None,
			),
			true => (
				"LIVE SELECT * FROM person WHERE $value.ok = true",
				Some(surrealdb_types::vars! { value: surrealdb_types::object! { ok: true } }),
			),
		};
		for response in ds.execute(sql, &ses, vars).await.unwrap() {
			response.result.expect("the LIVE query registers");
		}

		let ses_write = Session::owner().with_ns(ns).with_db(db);
		let mut res = ds
			.execute("CREATE person:42 SET ok = false", &ses_write, None)
			.await
			.expect("execute should not return an Err");
		res.remove(0).result.expect("CREATE should succeed");

		// The system `$value` (the real document, ok = false) wins, so the WHERE is false and no
		// notification fires.
		let result =
			tokio::time::timeout(tokio::time::Duration::from_millis(300), recv.recv()).await;
		assert!(
			result.is_err(),
			"no notification should fire when the real document does not match the WHERE \
			 (bound_by_request = {bound_by_request})",
		);
	}
}

/// SECURITY: a LIVE query whose originating session has expired via TTL
/// must not receive notifications after the TTL passes. We set `exp` to
/// the current integer second so the session is technically still valid
/// at registration time, then sleep ≥1.1s so the integer second counter
/// has advanced past `exp`.
#[tokio::test]
async fn test_live_expired_session_suppresses_notification() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db, tb) = ("test", "test", "person");
	setup_ns_db_table(&ds, ns, db, tb).await;

	let mut live_ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	live_ses.exp = Some(Utc::now().timestamp());
	ds.execute(&format!("LIVE SELECT * FROM {tb}"), &live_ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	let res = ds.execute(&format!("CREATE {tb}"), &owner_ses, None).await.unwrap();
	assert!(res[0].result.is_ok(), "CREATE must succeed regardless of LIVE query TTL");

	let spurious = tokio::time::timeout(tokio::time::Duration::from_millis(200), recv.recv()).await;
	assert!(
		spurious.is_err(),
		"no notification must be sent after the LIVE query's session TTL has expired",
	);
}

/// Sanity check companion to the expiry test: a LIVE query with a
/// non-expiring session receives notifications normally.
#[tokio::test]
async fn test_live_active_session_sends_notification() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db, tb) = ("test", "test", "person");
	setup_ns_db_table(&ds, ns, db, tb).await;

	let live_ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	ds.execute(&format!("LIVE SELECT * FROM {tb}"), &live_ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(&format!("CREATE {tb}"), &owner_ses, None).await.unwrap();

	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("notification should arrive within timeout")
		.expect("channel should not be closed");
	assert_eq!(notif.action, PublicAction::Create);
}

/// A subscription's field list is the notification's result: the record
/// projected by it, and nothing it does not name.
#[tokio::test]
async fn test_live_field_list_projects_the_record() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db, tb) = ("test", "test", "person");
	setup_ns_db_table(&ds, ns, db, tb).await;

	let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	ds.execute("LIVE SELECT name, age + 1 AS next FROM person", &ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	let ses_write = Session::owner().with_ns(ns).with_db(db);
	let mut res = ds
		.execute("CREATE person:1 SET name = 'a', age = 2, other = true", &ses_write, None)
		.await
		.expect("execute should not return an Err");
	res.remove(0).result.expect("CREATE should succeed");

	let notif = tokio::time::timeout(tokio::time::Duration::from_secs(3), recv.recv())
		.await
		.expect("notification should arrive within timeout")
		.expect("channel should not be closed");
	assert_eq!(notif.action, PublicAction::Create);
	let PublicValue::Object(obj) = notif.result else {
		panic!("CREATE result should be an object, got: {:?}", notif.result);
	};
	assert_eq!(obj.get("name"), Some(&PublicValue::String("a".to_string())));
	assert_eq!(obj.get("next"), Some(&PublicValue::Number(surrealdb_types::Number::Int(3))));
	assert!(!obj.contains_key("other"), "a field the list does not name: {obj:?}");
}

/// A subscription's `FETCH` path ends at a `.@`, as a statement's does: the
/// link the walk stands on there is fetched into the notification.
#[tokio::test]
async fn test_live_fetch_path_ends_at_a_recursion() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db, tb) = ("test", "test", "post");
	setup_ns_db_table(&ds, ns, db, tb).await;

	let ses_write = Session::owner().with_ns(ns).with_db(db);
	let mut res = ds
		.execute("CREATE person:1 SET name = 'a'", &ses_write, None)
		.await
		.expect("execute should not return an Err");
	res.remove(0).result.expect("CREATE should succeed");

	let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	let mut res = ds.execute("LIVE SELECT * FROM post FETCH author.@", &ses, None).await.unwrap();
	res.remove(0).result.expect("the LIVE query registers");
	while recv.try_recv().is_ok() {}

	let mut res = ds
		.execute("CREATE post:1 SET author = person:1", &ses_write, None)
		.await
		.expect("execute should not return an Err");
	res.remove(0).result.expect("CREATE should succeed");

	let notif = tokio::time::timeout(tokio::time::Duration::from_secs(3), recv.recv())
		.await
		.expect("notification should arrive within timeout")
		.expect("channel should not be closed");
	assert_eq!(notif.action, PublicAction::Create);
	let PublicValue::Object(obj) = notif.result else {
		panic!("CREATE result should be an object, got: {:?}", notif.result);
	};
	let Some(PublicValue::Object(author)) = obj.get("author") else {
		panic!("the author should be fetched, got: {obj:?}");
	};
	assert_eq!(author.get("name"), Some(&PublicValue::String("a".to_string())));
}

/// SECURITY: a subscription's field list runs as the subscriber, so a
/// field its principal may not select is not reached through an alias.
#[tokio::test]
async fn test_live_field_list_does_not_leak_a_restricted_field_through_an_alias() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE TABLE person PERMISSIONS FOR select FULL; \
		 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE FIELD secret ON person TYPE string PERMISSIONS FOR select NONE",
		&owner_ses,
		None,
	)
	.await
	.unwrap();

	let live_ses = record_user_session(ns, db, "alice");
	ds.execute("LIVE SELECT secret AS s, id FROM person", &live_ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	ds.execute("CREATE person:1 SET secret = 'shh'", &owner_ses, None).await.unwrap();

	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("notification should arrive within timeout")
		.expect("channel should not be closed");
	let PublicValue::Object(obj) = notif.result else {
		panic!("CREATE result should be an object, got: {:?}", notif.result);
	};
	assert!(
		!matches!(obj.get("s"), Some(PublicValue::String(_))),
		"the restricted field must not reach the subscriber: {obj:?}",
	);
}

/// SECURITY (#120): `reduce_to_owned` runs *before* computed-field
/// evaluation, so a computed field marked `PERMISSIONS FOR select NONE`
/// would never be touched by the table-side reduction. The CREATE
/// notification delivered to a record subscriber must not contain the
/// computed field's value — that's what
/// `filter_computed_field_permissions` enforces.
#[tokio::test]
async fn test_live_create_does_not_leak_restricted_computed_field() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE TABLE person PERMISSIONS FOR select FULL; \
		 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE FIELD secret ON person TYPE string; \
		 DEFINE FIELD derived ON person TYPE string \
		     COMPUTED string::concat('derived_', secret) \
		     PERMISSIONS FOR select NONE",
		&owner_ses,
		None,
	)
	.await
	.unwrap();

	let live_ses = record_user_session(ns, db, "alice");
	ds.execute("LIVE SELECT * FROM person", &live_ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	ds.execute("CREATE person:1 SET secret = 'shh'", &owner_ses, None).await.unwrap();

	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("notification should arrive within timeout")
		.expect("channel should not be closed");
	assert_eq!(notif.action, PublicAction::Create);
	let PublicValue::Object(obj) = notif.result else {
		panic!("CREATE result should be an object, got: {:?}", notif.result);
	};
	assert!(
		!obj.contains_key("derived"),
		"CREATE notification must not include the restricted computed field; got: {obj:?}",
	);
}

/// SECURITY (#120): for DELETE events `lq_compute` uses `&self.initial`
/// (the pre-delete view) as the payload source; without
/// `filter_computed_field_permissions` the DELETE notification would
/// carry the unfiltered computed field from the pre-delete state.
#[tokio::test]
async fn test_live_delete_does_not_leak_restricted_computed_field() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE TABLE person PERMISSIONS FOR select FULL; \
		 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE FIELD secret ON person TYPE string; \
		 DEFINE FIELD derived ON person TYPE string \
		     COMPUTED string::concat('derived_', secret) \
		     PERMISSIONS FOR select NONE",
		&owner_ses,
		None,
	)
	.await
	.unwrap();
	ds.execute("CREATE person:1 SET secret = 'shh'", &owner_ses, None).await.unwrap();

	let live_ses = record_user_session(ns, db, "alice");
	ds.execute("LIVE SELECT * FROM person", &live_ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	ds.execute("DELETE person:1", &owner_ses, None).await.unwrap();

	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("notification should arrive within timeout")
		.expect("channel should not be closed");
	assert_eq!(notif.action, PublicAction::Delete);
	let PublicValue::Object(obj) = notif.result else {
		panic!("DELETE result should be an object, got: {:?}", notif.result);
	};
	assert!(
		!obj.contains_key("derived"),
		"DELETE notification must not include the restricted computed field; got: {obj:?}",
	);
}

/// SECURITY (#120): on `LIVE SELECT DIFF`, the LHS of the patch
/// computation must be prepared the same way as the RHS (reduce +
/// compute + filter). Otherwise a `Remove /derived` or
/// `Replace /derived` op would leak the restricted computed field's
/// existence or value through the diff payload.
#[tokio::test]
async fn test_live_diff_does_not_leak_restricted_computed_field() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE TABLE person PERMISSIONS FOR select FULL; \
		 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE FIELD name ON person TYPE string; \
		 DEFINE FIELD derived ON person TYPE string \
		     COMPUTED string::concat('derived_', name) \
		     PERMISSIONS FOR select NONE",
		&owner_ses,
		None,
	)
	.await
	.unwrap();

	// Pre-create so the subsequent UPDATE produces an UPDATE notification
	// (not a CREATE) and exercises the LHS filtering path.
	ds.execute("CREATE person:1 SET name = 'foo'", &owner_ses, None).await.unwrap();

	let live_ses = record_user_session(ns, db, "alice");
	ds.execute("LIVE SELECT DIFF FROM person", &live_ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	ds.execute("UPDATE person:1 SET name = 'bar'", &owner_ses, None).await.unwrap();

	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("notification should arrive within timeout")
		.expect("channel should not be closed");
	let PublicValue::Array(ops) = notif.result else {
		panic!("DIFF result should be a Value::Array, got: {:?}", notif.result);
	};
	for op in ops.iter() {
		let PublicValue::Object(obj) = op else {
			continue;
		};
		let path = obj.get("path");
		let targets_derived = path == Some(&PublicValue::String("/derived".to_string()));
		assert!(
			!targets_derived,
			"DIFF UPDATE must not reference the restricted computed field; ops: {ops:?}",
		);
	}
}

/// SECURITY (#120): conditional (`Specific`) field permissions on
/// computed fields must also be enforced on LIVE delivery. A subscriber
/// whose record satisfies the predicate sees the computed field; one
/// whose record does not satisfy it must NOT see it.
#[tokio::test]
async fn test_live_conditional_permission_filters_computed_field() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE TABLE doc PERMISSIONS FOR select FULL; \
		 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE FIELD owner ON doc TYPE record<user>; \
		 DEFINE FIELD name ON doc TYPE string; \
		 DEFINE FIELD mine ON doc TYPE string \
		     COMPUTED string::concat('hi_', name) \
		     PERMISSIONS FOR select WHERE owner = $auth",
		&owner_ses,
		None,
	)
	.await
	.unwrap();

	let live_ses = record_user_session(ns, db, "alice");
	ds.execute("LIVE SELECT * FROM doc", &live_ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	// Record owned by the subscriber → `mine` is visible.
	ds.execute("CREATE doc:1 SET owner = user:alice, name = 'one'", &owner_ses, None)
		.await
		.unwrap();
	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("notification should arrive within timeout")
		.expect("channel should not be closed");
	let PublicValue::Object(obj) = notif.result else {
		panic!("CREATE result should be an object, got: {:?}", notif.result);
	};
	assert!(
		obj.contains_key("mine"),
		"CREATE for own record must include the conditional computed field; got: {obj:?}",
	);

	// Record owned by someone else → `mine` is hidden.
	ds.execute("CREATE doc:2 SET owner = user:bob, name = 'two'", &owner_ses, None).await.unwrap();
	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("notification should arrive within timeout")
		.expect("channel should not be closed");
	let PublicValue::Object(obj) = notif.result else {
		panic!("CREATE result should be an object, got: {:?}", notif.result);
	};
	assert!(
		!obj.contains_key("mine"),
		"CREATE for someone else's record must NOT include the conditional computed field; got: {obj:?}",
	);
}

/// SECURITY: on `LIVE SELECT DIFF` with a non-Owner subscriber, no
/// `Remove` op should appear for a stored field the subscriber cannot
/// `SELECT`. Before the fix, `self.initial` (LHS of the diff) was not
/// reduced under the subscriber's auth, so any field present in the
/// initial but absent from the reduced RHS would surface as a spurious
/// `Remove` op — leaking the field name to a subscriber with no SELECT
/// permission on it.
#[tokio::test]
async fn test_live_diff_does_not_leak_restricted_field_name() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE TABLE person PERMISSIONS FOR select FULL; \
		 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE FIELD name ON person TYPE string; \
		 DEFINE FIELD secret ON person TYPE string PERMISSIONS FOR select WHERE false",
		&owner_ses,
		None,
	)
	.await
	.unwrap();

	// Pre-create so the subsequent UPDATE is a diff rather than a CREATE.
	ds.execute("CREATE person:50 SET name = 'foo', secret = 'shh'", &owner_ses, None)
		.await
		.unwrap();

	let live_ses = record_user_session(ns, db, "alice");
	ds.execute("LIVE SELECT DIFF FROM person", &live_ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	ds.execute("UPDATE person:50 SET name = 'bar'", &owner_ses, None).await.unwrap();

	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("notification should arrive within timeout")
		.expect("channel should not be closed");
	let PublicValue::Array(ops) = notif.result else {
		panic!("DIFF result should be a Value::Array, got: {:?}", notif.result);
	};
	for op in ops.iter() {
		let PublicValue::Object(obj) = op else {
			continue;
		};
		let is_remove = obj.get("op") == Some(&PublicValue::String("remove".to_string()));
		let targets_secret = obj.get("path") == Some(&PublicValue::String("/secret".to_string()));
		assert!(
			!(is_remove && targets_secret),
			"DIFF UPDATE must not leak a Remove op for the restricted /secret field; ops: {ops:?}",
		);
	}
}

/// CONTRACT (defense in depth): the DELETE arm of `LIVE SELECT DIFF`
/// must not emit patch ops that reference fields the subscriber has
/// no SELECT permission for. With today's `Value::diff` semantics
/// (`Object → None` collapses to a single `Replace { path: "" }`)
/// this is already true regardless of which source is diffed, but
/// `lq_compute` uses the prepared view (`doc`) so that a future
/// change to per-field diff semantics inherits the LIVE owner's
/// permissions automatically. This test pins the invariant.
#[tokio::test]
async fn test_live_diff_delete_does_not_leak_restricted_field_name() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE TABLE person PERMISSIONS FOR select FULL; \
		 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE FIELD name ON person TYPE string; \
		 DEFINE FIELD secret ON person TYPE string PERMISSIONS FOR select WHERE false; \
		 DEFINE FIELD derived ON person TYPE string \
		     COMPUTED string::concat('derived_', name) \
		     PERMISSIONS FOR select NONE",
		&owner_ses,
		None,
	)
	.await
	.unwrap();

	ds.execute("CREATE person:51 SET name = 'foo', secret = 'shh'", &owner_ses, None)
		.await
		.unwrap();

	let live_ses = record_user_session(ns, db, "alice");
	ds.execute("LIVE SELECT DIFF FROM person", &live_ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	ds.execute("DELETE person:51", &owner_ses, None).await.unwrap();

	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("notification should arrive within timeout")
		.expect("channel should not be closed");
	assert_eq!(notif.action, PublicAction::Delete);
	let PublicValue::Array(ops) = notif.result else {
		panic!("DIFF result should be a Value::Array, got: {:?}", notif.result);
	};
	for op in ops.iter() {
		let PublicValue::Object(obj) = op else {
			continue;
		};
		let path = obj.get("path");
		let targets_secret = path == Some(&PublicValue::String("/secret".to_string()));
		let targets_derived = path == Some(&PublicValue::String("/derived".to_string()));
		assert!(
			!targets_secret,
			"DIFF DELETE must not reference the restricted /secret field; ops: {ops:?}",
		);
		assert!(
			!targets_derived,
			"DIFF DELETE must not reference the restricted /derived computed field; ops: {ops:?}",
		);
	}
}

/// ISOLATION: a LIVE query whose WHERE clause errors per-document
/// (`string::len(name)` when `name` is missing) must not abort the
/// triggering CREATE. PR #214 chose silent ignore (no Action::Error
/// notification), so we just confirm CREATE succeeds and no
/// notification fires.
#[tokio::test]
async fn test_live_where_doc_dependent_error_does_not_abort_create() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db, tb) = ("test", "test", "person");
	setup_ns_db_table(&ds, ns, db, tb).await;

	let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	ds.execute("LIVE SELECT * FROM person WHERE string::len(name) > 3", &ses, None).await.unwrap();

	let ses_write = Session::owner().with_ns(ns).with_db(db);
	let mut res = ds
		.execute("CREATE person:1", &ses_write, None)
		.await
		.expect("execute should not return an Err");
	res.remove(0).result.expect("CREATE should succeed, not be aborted by LIVE WHERE error");

	let spurious = tokio::time::timeout(tokio::time::Duration::from_millis(200), recv.recv()).await;
	assert!(
		spurious.is_err(),
		"per-PR-214 design: WHERE errors silently skip the notification, no Action::Error",
	);
}

/// ISOLATION: a LIVE query whose SELECT projection always errors
/// (`string::len(NONE)`) must not abort the triggering CREATE.
#[tokio::test]
async fn test_live_projection_type_error_does_not_abort_create() {
	let (_, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db, tb) = ("test", "test", "person");
	setup_ns_db_table(&ds, ns, db, tb).await;

	let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	ds.execute("LIVE SELECT string::len(NONE) FROM person", &ses, None).await.unwrap();

	let ses_write = Session::owner().with_ns(ns).with_db(db);
	let mut res = ds
		.execute("CREATE person:2", &ses_write, None)
		.await
		.expect("execute should not return an Err");
	res.remove(0).result.expect("CREATE should succeed, not be aborted by LIVE projection error");
}

/// ISOLATION: a LIVE query with a FETCH clause that always errors
/// (`type::field(NONE)`) must not abort subsequent writes. Before the
/// fix, this path was unprotected (raw `?` propagation).
#[tokio::test]
async fn test_live_fetch_error_does_not_abort_create() {
	let (_, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db, tb) = ("test", "test", "person");
	setup_ns_db_table(&ds, ns, db, tb).await;

	let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	ds.execute("LIVE SELECT * FROM person FETCH type::field(NONE)", &ses, None).await.unwrap();

	let ses_write = Session::owner().with_ns(ns).with_db(db);
	let mut res = ds
		.execute("CREATE person:12", &ses_write, None)
		.await
		.expect("execute should not return an Err");
	res.remove(0).result.expect("CREATE should succeed despite invalid FETCH expression");
}

/// ISOLATION: a LIVE subscriber whose per-field SELECT permission
/// always errors must not abort writes. `reduce_to_owned` runs under
/// the LIVE owner's auth (here a record-access user), so an erroring
/// field permission only affects this subscription — not the writer.
#[tokio::test]
async fn test_live_field_permission_error_does_not_abort_create() {
	let (_, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE TABLE person; \
		 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE FIELD secret ON person PERMISSIONS FOR select WHERE string::len(NONE)",
		&owner_ses,
		None,
	)
	.await
	.unwrap();

	let live_ses = record_user_session(ns, db, "alice");
	ds.execute("LIVE SELECT * FROM person", &live_ses, None).await.unwrap();

	let ses_write = Session::owner().with_ns(ns).with_db(db);
	let mut res = ds
		.execute("CREATE person:20 SET secret = 'shh'", &ses_write, None)
		.await
		.expect("execute should not return an Err");
	res.remove(0).result.expect("CREATE should succeed despite erroring field SELECT permission");
}

/// ISOLATION: multiple LIVE queries with document-dependent erroring
/// WHERE clauses on the same table must not prevent CREATE, UPDATE, or
/// DELETE from succeeding. This is the multi-subscriber stress test.
#[tokio::test]
async fn test_multiple_erroring_live_queries_do_not_abort_writes() {
	let (_, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db, tb) = ("test", "test", "person");
	setup_ns_db_table(&ds, ns, db, tb).await;

	let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	ds.execute("LIVE SELECT * FROM person WHERE string::len(name) > 3", &ses, None).await.unwrap();
	ds.execute("LIVE SELECT * FROM person WHERE string::uppercase(name) = 'ALICE'", &ses, None)
		.await
		.unwrap();

	let ses_write = Session::owner().with_ns(ns).with_db(db);

	let mut res =
		ds.execute("CREATE person:3", &ses_write, None).await.expect("execute should not Err");
	res.remove(0).result.expect("CREATE should succeed");

	let mut res = ds
		.execute("UPDATE person:3 SET name = 'Alice'", &ses_write, None)
		.await
		.expect("execute should not Err");
	res.remove(0).result.expect("UPDATE should succeed");

	let mut res =
		ds.execute("DELETE person:3", &ses_write, None).await.expect("execute should not Err");
	res.remove(0).result.expect("DELETE should succeed");
}

/// A CREATE notification must still fire for a table with a TYPE-constrained
/// COMPUTED field. The `$before` image of a create is absent, and deriving
/// the computed field against it coerces `NONE` to the declared type and
/// fails; the counterpart must be bound as the empty view rather than run
/// through computed-field evaluation, or the whole notification is dropped.
#[tokio::test]
async fn test_live_create_notification_survives_typed_computed_field() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	// `derived` computes to a plain `string`, so it is `NONE` on the absent
	// pre-image and fails the `TYPE string` coercion there.
	ds.execute(
		"DEFINE TABLE person PERMISSIONS FOR select FULL; \
		 DEFINE FIELD name ON person TYPE string; \
		 DEFINE FIELD derived ON person TYPE string COMPUTED name",
		&owner_ses,
		None,
	)
	.await
	.unwrap();

	let live_ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	ds.execute("LIVE SELECT * FROM person", &live_ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	ds.execute("CREATE person:1 SET name = 'foo'", &owner_ses, None).await.unwrap();

	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("a CREATE notification must fire despite the typed computed field")
		.expect("channel should not be closed");
	assert_eq!(notif.action, PublicAction::Create);
	let PublicValue::Object(obj) = notif.result else {
		panic!("CREATE result should be an object, got: {:?}", notif.result);
	};
	assert_eq!(
		obj.get("derived"),
		Some(&PublicValue::String("foo".to_string())),
		"the present record's computed field must be delivered; got: {obj:?}",
	);
}

/// The DELETE counterpart (`$after`) is likewise absent, so the same drop
/// applies to deletes: the notification must still fire.
#[tokio::test]
async fn test_live_delete_notification_survives_typed_computed_field() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE TABLE person PERMISSIONS FOR select FULL; \
		 DEFINE FIELD name ON person TYPE string; \
		 DEFINE FIELD derived ON person TYPE string COMPUTED name",
		&owner_ses,
		None,
	)
	.await
	.unwrap();
	ds.execute("CREATE person:1 SET name = 'foo'", &owner_ses, None).await.unwrap();

	let live_ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	ds.execute("LIVE SELECT * FROM person", &live_ses, None).await.unwrap();
	while recv.try_recv().is_ok() {}

	ds.execute("DELETE person:1", &owner_ses, None).await.unwrap();

	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("a DELETE notification must fire despite the typed computed field")
		.expect("channel should not be closed");
	assert_eq!(notif.action, PublicAction::Delete);
}

/// SECURITY: reduction is per-subscriber, not per-record. One write reaches
/// every subscription on the table, and each one reduces it under the auth
/// that registered it — so a field one subscriber may read and another may
/// not is present in one notification and absent from the other. Sharing a
/// single reduced image, or a single resolver memoised against one
/// subscriber's permissions, collapses the two views into whichever
/// subscriber happened to be reduced first.
#[tokio::test]
async fn test_live_reduction_is_per_subscriber() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE TABLE user PERMISSIONS FOR select FULL; \
		 DEFINE TABLE item PERMISSIONS FOR select FULL; \
		 DEFINE FIELD owner ON item TYPE record<user>; \
		 DEFINE FIELD open ON item TYPE int; \
		 DEFINE FIELD secret ON item TYPE string \
		     PERMISSIONS FOR select WHERE owner = $auth; \
		 CREATE user:alice, user:bob",
		&owner_ses,
		None,
	)
	.await
	.unwrap();

	// Two subscriptions to the same table, registered under two record users.
	let alice_lq = live_query_id(&ds, &record_user_session(ns, db, "alice")).await;
	let bob_lq = live_query_id(&ds, &record_user_session(ns, db, "bob")).await;
	assert_ne!(alice_lq, bob_lq, "the two subscriptions must be distinct");
	while recv.try_recv().is_ok() {}

	// One write, owned by alice.
	ds.execute("CREATE item:1 SET owner = user:alice, open = 1, secret = 'shh'", &owner_ses, None)
		.await
		.unwrap();

	let (mut alice_seen, mut bob_seen) = (false, false);
	for _ in 0..2 {
		let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
			.await
			.expect("both notifications should arrive within timeout")
			.expect("channel should not be closed");
		assert_eq!(notif.action, PublicAction::Create);
		let PublicValue::Object(obj) = notif.result else {
			panic!("CREATE result should be an object, got: {:?}", notif.result);
		};
		assert!(obj.contains_key("open"), "the unguarded field reaches both: {obj:?}");
		if notif.id == alice_lq {
			alice_seen = true;
			assert!(
				obj.contains_key("secret"),
				"alice owns the record and must be given the guarded field; got: {obj:?}",
			);
		} else if notif.id == bob_lq {
			bob_seen = true;
			assert!(
				!obj.contains_key("secret"),
				"bob does not own the record and must not be given the guarded field; \
				 got: {obj:?}",
			);
		} else {
			panic!("notification for an unknown subscription: {:?}", notif.id);
		}
	}
	assert!(alice_seen && bob_seen, "each subscription receives its own notification");
}

/// Register a `LIVE SELECT` on `item` and return the subscription's id.
async fn live_query_id(ds: &Datastore, ses: &Session) -> crate::types::PublicUuid {
	let mut res = ds.execute("LIVE SELECT * FROM item", ses, None).await.unwrap();
	let val = res.remove(0).result.unwrap();
	let PublicValue::Uuid(id) = val else {
		panic!("LIVE SELECT should answer with a subscription id, got: {val:?}");
	};
	id
}

/// An `UPDATE` that transitions a record from hidden under the subscriber's
/// `PERMISSIONS FOR select` to visible must still deliver the notification even
/// though the pre-update counterpart is rejected by `reduce`.
#[tokio::test]
async fn test_live_update_entering_subscriber_permission_fires_notification() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE TABLE user PERMISSIONS FOR select FULL; \
		 DEFINE TABLE item SCHEMALESS \
		     PERMISSIONS FOR select WHERE owner = $auth; \
		 CREATE user:alice, user:bob; \
		 CREATE item:1 SET owner = user:bob, label = 'moved'",
		&owner_ses,
		None,
	)
	.await
	.unwrap();

	let alice_lq = live_query_id(&ds, &record_user_session(ns, db, "alice")).await;
	while recv.try_recv().is_ok() {}

	// Transfer ownership to alice so the pre-image (`owner = user:bob`) fails
	// alice's table select permission while the post-image (`owner = user:alice`) passes.
	ds.execute("UPDATE item:1 SET owner = user:alice", &owner_ses, None).await.unwrap();

	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("notification should arrive when record enters subscriber's permission")
		.expect("channel should not be closed");
	assert_eq!(notif.id, alice_lq);
	assert_eq!(notif.action, PublicAction::Update);
}

/// An `UPDATE` that transitions a record from visible under the subscriber's
/// `PERMISSIONS FOR select` to hidden must still deliver the notification so
/// the subscriber sees the record leave their scope, with the post-update
/// image bound to `NONE`.
#[tokio::test]
async fn test_live_update_leaving_subscriber_permission_fires_notification() {
	let (recv, ds) = new_ds_with_broker().await.unwrap();
	let (ns, db) = ("test", "test");
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, ns, db).await.unwrap();
	tx.commit().await.unwrap();

	let owner_ses = Session::owner().with_ns(ns).with_db(db);
	ds.execute(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		 DEFINE TABLE user PERMISSIONS FOR select FULL; \
		 DEFINE TABLE item SCHEMALESS \
		     PERMISSIONS FOR select WHERE owner = $auth; \
		 CREATE user:alice, user:bob; \
		 CREATE item:1 SET owner = user:alice, label = 'moved'",
		&owner_ses,
		None,
	)
	.await
	.unwrap();

	let alice_lq = live_query_id(&ds, &record_user_session(ns, db, "alice")).await;
	while recv.try_recv().is_ok() {}

	// Transfer ownership from alice to bob so the pre-image (`owner = user:alice`)
	// passes alice's table select permission while the post-image (`owner = user:bob`)
	// is withheld and bound to `NONE`.
	ds.execute("UPDATE item:1 SET owner = user:bob", &owner_ses, None).await.unwrap();

	let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
		.await
		.expect("notification should arrive when record leaves subscriber's permission")
		.expect("channel should not be closed");
	assert_eq!(notif.id, alice_lq);
	assert_eq!(notif.action, PublicAction::Update);
	assert_eq!(notif.result, PublicValue::None);
}
