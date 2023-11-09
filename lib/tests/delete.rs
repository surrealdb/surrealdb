mod parse;
use parse::Parse;

use channel::{Receiver, TryRecvError};
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::node::Timestamp;
use surrealdb::dbs::{KvsAction, KvsNotification, Session};
use surrealdb::err::Error;
use surrealdb::iam::Role;
use surrealdb::sql;
use surrealdb::sql::Value;

#[tokio::test]
async fn delete() -> Result<(), Error> {
	let sql = "
		CREATE person:test SET name = 'Tester';
		DELETE person:test;
		SELECT * FROM person;
	";
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result.unwrap();
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Tester'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result.unwrap();
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result.unwrap();
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

//
// Permissions
//

async fn common_permissions_checks(auth_enabled: bool) {
	let tests = vec![
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true, "owner at root level should be able to delete a record"),
		((().into(), Role::Editor), ("NS", "DB"), true, "editor at root level should be able to delete a record"),
		((().into(), Role::Viewer), ("NS", "DB"), false, "viewer at root level should not be able to delete a record"),

		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true, "owner at namespace level should be able to delete a record on its namespace"),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at namespace level should not be able to delete a record on another namespace"),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true, "editor at namespace level should be able to delete a record on its namespace"),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at namespace level should not be able to delete a record on another namespace"),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false, "viewer at namespace level should not be able to delete a record on its namespace"),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at namespace level should not be able to delete a record on another namespace"),

		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true, "owner at database level should be able to delete a record on its database"),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false, "owner at database level should not be able to delete a record on another database"),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at database level should not be able to delete a record on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true, "editor at database level should be able to delete a record on its database"),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false, "editor at database level should not be able to delete a record on another database"),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at database level should not be able to delete a record on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false, "viewer at database level should not be able to delete a record on its database"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false, "viewer at database level should not be able to delete a record on another database"),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at database level should not be able to delete a record on another namespace even if the database name matches"),
	];

	let statement = "DELETE person:test";

	for ((level, role), (ns, db), should_succeed, msg) in tests.into_iter() {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		{
			let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

			let mut resp = ds
				.execute("CREATE person:test", &Session::owner().with_ns("NS").with_db("DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::parse("[]"),
				"unexpected error creating person record"
			);

			let mut resp = ds
				.execute(
					"CREATE person:test",
					&Session::owner().with_ns("OTHER_NS").with_db("DB"),
					None,
				)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::parse("[]"),
				"unexpected error creating person record"
			);

			let mut resp = ds
				.execute(
					"CREATE person:test",
					&Session::owner().with_ns("NS").with_db("OTHER_DB"),
					None,
				)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::parse("[]"),
				"unexpected error creating person record"
			);

			// Run the test
			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();
			assert!(res.is_ok(), "delete should not fail");

			if should_succeed {
				// Verify the record has been deleted
				let mut resp = ds
					.execute(
						"SELECT * FROM person:test",
						&Session::owner().with_ns("NS").with_db("DB"),
						None,
					)
					.await
					.unwrap();
				let res = resp.remove(0).output();
				assert!(res.is_ok() && res.unwrap() == Value::parse("[]"), "{}", msg);
			} else {
				// Verify the record has not been deleted in any DB
				let mut resp = ds
					.execute(
						"SELECT * FROM person:test",
						&Session::owner().with_ns("NS").with_db("DB"),
						None,
					)
					.await
					.unwrap();
				let res = resp.remove(0).output();
				assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", msg);

				let mut resp = ds
					.execute(
						"SELECT * FROM person:test",
						&Session::owner().with_ns("OTHER_NS").with_db("DB"),
						None,
					)
					.await
					.unwrap();
				let res = resp.remove(0).output();
				assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", msg);

				let mut resp = ds
					.execute(
						"SELECT * FROM person:test",
						&Session::owner().with_ns("NS").with_db("OTHER_DB"),
						None,
					)
					.await
					.unwrap();
				let res = resp.remove(0).output();
				assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", msg);
			}
		}
	}
}

#[tokio::test]
async fn check_permissions_auth_enabled() {
	let auth_enabled = true;
	//
	// Test common scenarios
	//
	common_permissions_checks(auth_enabled).await;

	//
	// Test Anonymous user
	//

	let statement = "DELETE person:test";

	// When the table exists but grants no permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS NONE; CREATE person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);
		let res = resp.remove(0).output();
		assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", "failed to create record");

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.is_ok(), "delete should succeed even if it doesn't really delete anything");

		// Verify the record has not been deleted
		let mut resp = ds
			.execute(
				"SELECT * FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(
			res.is_ok() && res.unwrap() != Value::parse("[]"),
			"{}",
			"anonymous user should not be able to delete a record if the table has no permissions"
		);
	}

	// When the table exists and grants full permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS FULL; CREATE person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);
		let res = resp.remove(0).output();
		assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", "failed to create record");

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.is_ok(), "delete should succeed even if it doesn't really delete anything");

		// Verify the record has been deleted
		let mut resp = ds
			.execute(
				"SELECT * FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(
			res.is_ok() && res.unwrap() == Value::parse("[]"),
			"{}",
			"anonymous user should be able to delete a record if the table has full permissions"
		);
	}
}

#[tokio::test]
async fn check_permissions_auth_disabled() {
	let auth_enabled = false;
	//
	// Test common scenarios
	//
	common_permissions_checks(auth_enabled).await;

	//
	// Test Anonymous user
	//

	let statement = "DELETE person:test";

	// When the table exists but grants no permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS NONE; CREATE person:test;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);
		let res = resp.remove(0).output();
		assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", "failed to create record");

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.is_ok(), "delete should succeed even if it doesn't really delete anything");

		// Verify the record has been deleted
		let mut resp = ds
			.execute(
				"SELECT * FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(
			res.is_ok() && res.unwrap() == Value::parse("[]"),
			"{}",
			"anonymous user should be able to delete a record if the table has no permissions"
		);
	}

	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		// When the table exists and grants full permissions
		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS FULL; CREATE person:test;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);
		let res = resp.remove(0).output();
		assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", "failed to create record");

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.is_ok(), "delete should succeed even if it doesn't really delete anything");

		// Verify the record has been deleted
		let mut resp = ds
			.execute(
				"SELECT * FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(
			res.is_ok() && res.unwrap() == Value::parse("[]"),
			"{}",
			"anonymous user should be able to delete a record if the table has full permissions"
		);
	}
}

#[tokio::test]
async fn delete_filtered_live_notification() -> Result<(), Error> {
	let node_id = uuid::Uuid::parse_str("bc52f447-831e-4fa2-834c-ed3e5c4e95bc").unwrap();
	let dbs = new_ds().await.unwrap().with_notifications().with_node_id(sql::Uuid::from(node_id));
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let res =
		&mut dbs.execute("CREATE person:test_true SET condition = true", &ses, None).await.unwrap();
	assert_eq!(res.len(), 1);
	// validate create response
	let tmp = res.remove(0).result.unwrap();
	let expected_record = Value::parse(
		"[
			{
				id: person:test_true,
				condition: true,
			}
		]",
	);
	assert_eq!(tmp, expected_record);

	// Validate live query response
	let res = &mut dbs
		.execute("LIVE SELECT * FROM person WHERE condition = true", &ses, None)
		.await
		.unwrap();
	assert_eq!(res.len(), 1);
	let live_id = res.remove(0).result.unwrap();
	let live_id = match live_id {
		Value::Uuid(id) => id,
		_ => panic!("expected uuid"),
	};

	// Validate delete response
	let res = &mut dbs.execute("DELETE person:test_true", &ses, None).await.unwrap();
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result.unwrap();
	let val = Value::parse("[]");
	assert_eq!(tmp, val);

	// Validate notification
	let notifications = dbs.notifications().unwrap();
	let mut not =
		recv_notification(&notifications, 10, std::time::Duration::from_millis(100)).unwrap();
	// We cannot easily determine the timestamp
	assert!(not.timestamp.value > 0);
	not.timestamp = Timestamp::default();
	// We cannot easily determine the notification ID either
	assert_ne!(not.notification_id, sql::Uuid::default());
	not.notification_id = sql::Uuid::default();
	assert_eq!(
		not,
		KvsNotification {
			live_id,
			node_id: sql::Uuid::from(node_id),
			action: KvsAction::Delete,
			result: Value::parse(
				"{
					id: person:test_true,
					condition: true,
				}"
			),
			notification_id: Default::default(),
			timestamp: Timestamp::default(),
		}
	);
	Ok(())
}

fn recv_notification(
	notifications: &Receiver<KvsNotification>,
	tries: u8,
	poll_rate: std::time::Duration,
) -> Result<KvsNotification, TryRecvError> {
	for _ in 0..tries {
		if let Ok(not) = notifications.try_recv() {
			return Ok(not);
		}
		std::thread::sleep(poll_rate);
	}
	notifications.try_recv()
}
