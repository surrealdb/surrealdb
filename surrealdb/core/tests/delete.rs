#![recursion_limit = "256"]
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)] // Concrete observer `Arc` clones coerce to `Arc<dyn ExecutionObserver>`

mod helpers;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use helpers::{new_ds, new_ns_db};
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::Capabilities;
use surrealdb_core::iam::{Level, Role};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::observe::{
	ExecutionObserver, Outcome, TransactionEvent, TransactionMetricsSnapshot,
};
use surrealdb_core::syn;
use surrealdb_types::{Action, Array, Notification, RecordId, Value};

// Capture transaction delete counts so tests can distinguish point deletes
// from staged range deletes without reaching into document internals.
#[derive(Default)]
struct CapturingTransactionObserver {
	metrics: Mutex<Vec<TransactionMetricsSnapshot>>,
}

impl CapturingTransactionObserver {
	fn clear(&self) {
		self.metrics.lock().unwrap().clear();
	}

	fn snapshot(&self) -> Vec<TransactionMetricsSnapshot> {
		self.metrics.lock().unwrap().clone()
	}
}

impl ExecutionObserver for CapturingTransactionObserver {
	fn on_transaction_complete(&self, event: &TransactionEvent) {
		if event.safe.write && event.safe.outcome == Outcome::Success {
			self.metrics.lock().unwrap().push(event.safe.metrics);
		}
	}
}

async fn new_observed_ds() -> Result<(Datastore, Arc<CapturingTransactionObserver>)> {
	let observer = Arc::new(CapturingTransactionObserver::default());
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_observer(observer.clone())
		.build_with_path("memory")
		.await?;
	new_ns_db(&ds, "test", "test").await?;
	observer.clear();
	Ok((ds, observer))
}

#[tokio::test]
async fn delete_without_references_skips_reference_range_delete() -> Result<()> {
	let (ds, observer) = new_observed_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let mut res = ds.execute("CREATE ordinary:one", &ses, None).await?;
	res.remove(0).output()?;
	observer.clear();

	let mut res = ds.execute("DELETE ordinary:one", &ses, None).await?;
	res.remove(0).output()?;

	let metrics = observer.snapshot();
	assert_eq!(metrics.len(), 1, "expected one successful write transaction: {metrics:?}");
	assert_eq!(metrics[0].ops_del, 1, "plain record delete should only record the point delete");

	Ok(())
}

#[tokio::test]
async fn delete_with_reference_keys_still_deletes_reference_range() -> Result<()> {
	let (ds, observer) = new_observed_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let mut res = ds
		.execute(
			"
			DEFINE TABLE user;
			DEFINE TABLE message;
			DEFINE FIELD author ON message TYPE record<user> REFERENCE ON DELETE IGNORE;
			CREATE user:alice;
			CREATE message:one SET author = user:alice;
			",
			&ses,
			None,
		)
		.await?;
	for response in res.drain(..) {
		response.output()?;
	}
	observer.clear();

	let mut res = ds.execute("DELETE user:alice", &ses, None).await?;
	res.remove(0).output()?;

	let metrics = observer.snapshot();
	assert_eq!(metrics.len(), 1, "expected one successful write transaction: {metrics:?}");
	assert_eq!(
		metrics[0].ops_del, 2,
		"referenced record delete should record the point delete and reference range delete"
	);

	let mut res = ds.execute("RETURN user:alice<~(message FIELD author)", &ses, None).await?;
	let reverse_references = res.remove(0).output()?;
	assert_eq!(reverse_references, syn::value("[]")?);

	Ok(())
}

//
// Permissions
//

fn level_root() -> Level {
	Level::Root
}
fn level_ns() -> Level {
	Level::Namespace("NS".to_owned())
}
fn level_db() -> Level {
	Level::Database("NS".to_owned(), "DB".to_owned())
}

async fn common_permissions_checks(auth_enabled: bool) {
	let tests = vec![
		// Root level
		(
			(level_root(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at root level should be able to delete a record",
		),
		(
			(level_root(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at root level should be able to delete a record",
		),
		(
			(level_root(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at root level should not be able to delete a record",
		),
		// Namespace level
		(
			(level_ns(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at namespace level should be able to delete a record on its namespace",
		),
		(
			(level_ns(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			"owner at namespace level should not be able to delete a record on another namespace",
		),
		(
			(level_ns(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at namespace level should be able to delete a record on its namespace",
		),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			"editor at namespace level should not be able to delete a record on another namespace",
		),
		(
			(level_ns(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at namespace level should not be able to delete a record on its namespace",
		),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			"viewer at namespace level should not be able to delete a record on another namespace",
		),
		// Database level
		(
			(level_db(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at database level should be able to delete a record on its database",
		),
		(
			(level_db(), Role::Owner),
			("NS", "OTHER_DB"),
			false,
			"owner at database level should not be able to delete a record on another database",
		),
		(
			(level_db(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			"owner at database level should not be able to delete a record on another namespace even if the database name matches",
		),
		(
			(level_db(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at database level should be able to delete a record on its database",
		),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			"editor at database level should not be able to delete a record on another database",
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			"editor at database level should not be able to delete a record on another namespace even if the database name matches",
		),
		(
			(level_db(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at database level should not be able to delete a record on its database",
		),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			"viewer at database level should not be able to delete a record on another database",
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			"viewer at database level should not be able to delete a record on another namespace even if the database name matches",
		),
	];

	let statement = "DELETE person:test";

	for ((level, role), (ns, db), should_succeed, msg) in tests {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		{
			let (_, ds) = new_ds("NS", "DB", auth_enabled).await.unwrap();

			let mut resp = ds
				.execute("CREATE person:test", &Session::owner().with_ns("NS").with_db("DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::Array(Array::new()),
				"unexpected error creating person record"
			);

			// Create the other NS & DBs for cross-namespace tests.
			let mut resp = ds
				.execute(
					"USE NS OTHER_NS DB DB; USE NS NS DB OTHER_DB",
					&Session::owner().with_ns("OTHER_NS"),
					None,
				)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::Array(Array::new()),
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
				res.unwrap() != Value::Array(Array::new()),
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
				res.unwrap() != Value::Array(Array::new()),
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
				assert!(res.is_ok() && res.unwrap() == Value::Array(Array::new()), "{}", msg);
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
				assert!(res.is_ok() && res.unwrap() != Value::Array(Array::new()), "{}", msg);

				let mut resp = ds
					.execute(
						"SELECT * FROM person:test",
						&Session::owner().with_ns("OTHER_NS").with_db("DB"),
						None,
					)
					.await
					.unwrap();
				let res = resp.remove(0).output();
				assert!(res.is_ok() && res.unwrap() != Value::Array(Array::new()), "{}", msg);

				let mut resp = ds
					.execute(
						"SELECT * FROM person:test",
						&Session::owner().with_ns("NS").with_db("OTHER_DB"),
						None,
					)
					.await
					.unwrap();
				let res = resp.remove(0).output();
				assert!(res.is_ok() && res.unwrap() != Value::Array(Array::new()), "{}", msg);
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
		let (_, ds) = new_ds("NS", "DB", auth_enabled).await.unwrap();

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
		assert!(
			res.is_ok() && res.unwrap() != Value::Array(Array::new()),
			"{}",
			"failed to create record"
		);

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
			res.is_ok() && res.unwrap() != Value::Array(Array::new()),
			"{}",
			"anonymous user should not be able to delete a record if the table has no permissions"
		);
	}

	// When the table exists and grants full permissions
	{
		let (_, ds) = new_ds("NS", "DB", auth_enabled).await.unwrap();

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
		assert!(
			res.is_ok() && res.unwrap() != Value::Array(Array::new()),
			"{}",
			"failed to create record"
		);

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
			res.is_ok() && res.unwrap() == Value::Array(Array::new()),
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
		let (_, ds) = new_ds("NS", "DB", auth_enabled).await.unwrap();

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
		assert!(
			res.is_ok() && res.unwrap() != Value::Array(Array::new()),
			"{}",
			"failed to create record"
		);

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
			res.is_ok() && res.unwrap() == Value::Array(Array::new()),
			"{}",
			"anonymous user should be able to delete a record if the table has no permissions"
		);
	}

	{
		let (_, ds) = new_ds("NS", "DB", auth_enabled).await.unwrap();

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
		assert!(
			res.is_ok() && res.unwrap() != Value::Array(Array::new()),
			"{}",
			"failed to create record"
		);

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
			res.is_ok() && res.unwrap() == Value::Array(Array::new()),
			"{}",
			"anonymous user should be able to delete a record if the table has full permissions"
		);
	}
}

#[tokio::test]
async fn delete_filtered_live_notification() -> Result<()> {
	let (notifications, dbs) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	let res = &mut dbs.execute("CREATE person:test_true SET condition = true", &ses, None).await?;
	assert_eq!(res.len(), 1);
	// validate create response
	let tmp = res.remove(0).result?;
	let expected_record = syn::value(
		"[
			{
				id: person:test_true,
				condition: true,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, expected_record);

	// Validate live query response
	let res =
		&mut dbs.execute("LIVE SELECT * FROM person WHERE condition = true", &ses, None).await?;
	assert_eq!(res.len(), 1);
	let live_id = res.remove(0).result?;
	let live_id = match live_id {
		Value::Uuid(id) => id,
		_ => panic!("expected uuid"),
	};

	// Validate delete response
	let res = &mut dbs.execute("DELETE person:test_true", &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = Value::Array(Array::new());
	assert_eq!(tmp, val);

	// Validate notification
	let notification = notifications.recv().await.unwrap();
	assert_eq!(
		notification,
		Notification::new(
			live_id,
			None,
			Action::Delete,
			Value::RecordId(RecordId::new("person".to_owned(), "test_true".to_owned())),
			syn::value(
				"{
					id: person:test_true,
					condition: true,
				}"
			)
			.unwrap(),
		)
	);
	Ok(())
}
