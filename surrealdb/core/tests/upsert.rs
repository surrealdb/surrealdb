#![allow(clippy::unwrap_used)]

use surrealdb_core::iam::Level;
use surrealdb_types::{Array, ToSql, Value};

mod helpers;
use helpers::new_ds;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::Role;

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
			"owner at root level should be able to update a record",
		),
		(
			(level_root(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at root level should be able to update a record",
		),
		(
			(level_root(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at root level should not be able to update a record",
		),
		// Namespace level
		(
			(level_ns(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at namespace level should be able to update a record on its namespace",
		),
		(
			(level_ns(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			"owner at namespace level should not be able to update a record on another namespace",
		),
		(
			(level_ns(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at namespace level should be able to update a record on its namespace",
		),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			"editor at namespace level should not be able to update a record on another namespace",
		),
		(
			(level_ns(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at namespace level should not be able to update a record on its namespace",
		),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			"viewer at namespace level should not be able to update a record on another namespace",
		),
		// Database level
		(
			(level_db(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at database level should be able to update a record on its database",
		),
		(
			(level_db(), Role::Owner),
			("NS", "OTHER_DB"),
			false,
			"owner at database level should not be able to update a record on another database",
		),
		(
			(level_db(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			"owner at database level should not be able to update a record on another namespace even if the database name matches",
		),
		(
			(level_db(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at database level should be able to update a record on its database",
		),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			"editor at database level should not be able to update a record on another database",
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			"editor at database level should not be able to update a record on another namespace even if the database name matches",
		),
		(
			(level_db(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at database level should not be able to update a record on its database",
		),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			"viewer at database level should not be able to update a record on another database",
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			"viewer at database level should not be able to update a record on another namespace even if the database name matches",
		),
	];
	let statement = "UPSERT person:test CONTENT { name: 'Name' };";

	for ((level, role), (ns, db), should_succeed, msg) in tests {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		// Test the statement when the table has to be created

		{
			let (_, ds) = new_ds("NS", "DB", auth_enabled).await.unwrap();

			// Define additional namespaces/databases for cross-namespace tests
			ds.execute(
				"DEFINE NS OTHER_NS; USE NS OTHER_NS; DEFINE DB DB; USE NS NS; DEFINE DB OTHER_DB;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();

			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			if should_succeed {
				assert!(res.is_ok() && res.unwrap() != Value::Array(Array::new()), "{}", msg);
			} else if res.is_ok() {
				assert!(res.unwrap() == Value::Array(Array::new()), "{}", msg);
			} else {
				// Not allowed to create a table
				let err = res.unwrap_err().to_string();
				assert!(
					err.contains("Not enough permissions to perform this action"),
					"{}: {}",
					msg,
					err
				)
			}
		}

		// Test the statement when the table already exists
		{
			let (_, ds) = new_ds("NS", "DB", auth_enabled).await.unwrap();

			// Define additional namespaces/databases for cross-namespace tests
			ds.execute(
				"DEFINE NS OTHER_NS; USE NS OTHER_NS; DEFINE DB DB; USE NS NS; DEFINE DB OTHER_DB;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();

			// Prepare datastore
			let mut resp = ds
				.execute("CREATE person:test", &Session::owner().with_ns("NS").with_db("DB"), None)
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
				res.is_ok() && res.unwrap() != Value::Array(Array::new()),
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
				res.is_ok() && res.unwrap() != Value::Array(Array::new()),
				"unexpected error creating person record"
			);

			// Run the test
			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			if should_succeed {
				assert!(res.unwrap() != Value::Array(Array::new()), "{}", msg);

				// Verify the update was persisted
				let mut resp = ds
					.execute(
						"SELECT name FROM person:test",
						&Session::owner().with_ns("NS").with_db("DB"),
						None,
					)
					.await
					.unwrap();
				let res = resp.remove(0).output();
				let res = res.unwrap().to_sql();
				assert!(res.contains("Name"), "{}: {:?}", msg, res);
			} else {
				let res = res.unwrap();
				assert!(res == Value::Array(Array::new()), "{}: {:?}", msg, res);

				// Verify the update was not persisted
				let mut resp = ds
					.execute(
						"SELECT name FROM person:test",
						&Session::owner().with_ns("NS").with_db("DB"),
						None,
					)
					.await
					.unwrap();
				let res = resp.remove(0).output();
				let res = res.unwrap().to_sql();
				assert!(!res.contains("Name"), "{}: {:?}", msg, res);
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

	let statement = "UPSERT person:test CONTENT { name: 'Name' };";

	// When the table doesn't exist
	{
		let (_, ds) = new_ds("NS", "DB", auth_enabled).await.unwrap();

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		// With auth enabled, anonymous users can create tables (implicitly creating them)
		// but get empty results due to default permissions
		assert_eq!(
			res.unwrap(),
			Value::Array(Array::new()),
			"anonymous user should get empty result when creating table with auth enabled"
		);
	}

	// When the table grants no permissions
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

		assert_eq!(
			res.unwrap(),
			Value::Array(Array::new()),
			"{}",
			"anonymous user should not be able to select if the table has no permissions"
		);

		// Verify the update was not persisted
		let mut resp = ds
			.execute(
				"SELECT name FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		let res = res.unwrap().to_sql();
		assert!(
			!res.contains("Name"),
			"{}: {:?}",
			"anonymous user should not be able to update a record if the table has no permissions",
			res
		);
	}

	// When the table exists and grants full permissions
	{
		let (_, ds) = new_ds("NS", "DB", auth_enabled).await.unwrap();

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

		assert!(
			res.unwrap() != Value::Array(Array::new()),
			"{}",
			"anonymous user should be able to select if the table has full permissions"
		);

		// Verify the update was persisted
		let mut resp = ds
			.execute(
				"SELECT name FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		let res = res.unwrap().to_sql();
		assert!(
			res.contains("Name"),
			"{}: {:?}",
			"anonymous user should be able to update a record if the table has full permissions",
			res
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

	let statement = "UPSERT person:test CONTENT { name: 'Name' };";

	// When the table doesn't exist
	{
		let (_, ds) = new_ds("NS", "DB", auth_enabled).await.unwrap();

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Value::Array(Array::new()),
			"{}",
			"anonymous user should be able to create the table"
		);
	}

	// When the table grants no permissions
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

		assert!(
			res.unwrap() != Value::Array(Array::new()),
			"{}",
			"anonymous user should be able to update a record if the table has no permissions"
		);

		// Verify the update was persisted
		let mut resp = ds
			.execute(
				"SELECT name FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		let res = res.unwrap().to_sql();
		assert!(
			res.contains("Name"),
			"{}: {:?}",
			"anonymous user should be able to update a record if the table has no permissions",
			res
		);
	}

	// When the table exists and grants full permissions
	{
		let (_, ds) = new_ds("NS", "DB", auth_enabled).await.unwrap();

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

		assert!(
			res.unwrap() != Value::Array(Array::new()),
			"{}",
			"anonymous user should be able to select if the table has full permissions"
		);

		// Verify the update was persisted
		let mut resp = ds
			.execute(
				"SELECT name FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		let res = res.unwrap().to_sql();
		assert!(
			res.contains("Name"),
			"{}: {:?}",
			"anonymous user should be able to update a record if the table has full permissions",
			res
		);
	}
}
