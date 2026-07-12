#![recursion_limit = "256"]
#![allow(clippy::unwrap_used)]

use surrealdb_core::iam::Level;
use surrealdb_types::{Array, ToSql, Value};

mod helpers;
use helpers::new_ds;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::Role;
use surrealdb_core::syn;

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
	let statement = "UPDATE person:test CONTENT { name: 'Name' };";

	for ((level, role), (ns, db), should_succeed, msg) in tests {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

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

			// Select always succeeds, but the result may be empty

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
				assert!(res.unwrap() == Value::Array(Array::new()), "{}", msg);

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

	let statement = "UPDATE person:test CONTENT { name: 'Name' };";

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
			res.unwrap() == Value::Array(Array::new()),
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

	let statement = "UPDATE person:test CONTENT { name: 'Name' };";

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

#[tokio::test]
async fn soft_delete_table_permissions() {
	let (_, ds) = new_ds("NS", "DB", true).await.unwrap();
	let owner = Session::owner().with_ns("NS").with_db("DB");
	let anon = Session::default().with_ns("NS").with_db("DB");

	let mut resp = ds
		.execute(
			"DEFINE TABLE item SCHEMAFULL PERMISSIONS
				FOR select WHERE deleted IS NONE
				FOR create FULL
				FOR update WHERE $before.deleted IS NONE
				FOR delete FULL;
			DEFINE FIELD name ON item TYPE string;
			DEFINE FIELD deleted ON item TYPE option<datetime>;
			CREATE item:one SET name = 'one';",
			&owner,
			None,
		)
		.await
		.unwrap();
	assert!(resp.remove(0).output().is_ok(), "failed to define table");
	assert!(resp.remove(0).output().is_ok(), "failed to define field name");
	assert!(resp.remove(0).output().is_ok(), "failed to define field deleted");
	let created = resp.remove(0).output().unwrap();
	assert_eq!(created.into_array().unwrap().len(), 1, "failed to create record");

	let mut resp = ds.execute("SELECT * FROM item", &anon, None).await.unwrap();
	assert_eq!(
		resp.remove(0).output().unwrap(),
		syn::value("[{ id: item:one, name: 'one' }]").unwrap(),
		"anonymous user should see active records"
	);

	let mut resp =
		ds.execute("UPDATE item:one SET deleted = time::now();", &anon, None).await.unwrap();
	assert_eq!(
		resp.remove(0).output().unwrap(),
		Value::Array(Array::new()),
		"soft-deleted record should not be returned from UPDATE output"
	);

	let mut resp = ds.execute("SELECT deleted FROM item:one", &owner, None).await.unwrap();
	let res = resp.remove(0).output().unwrap();
	assert_eq!(
		res.clone().into_array().unwrap().len(),
		1,
		"owner should see the soft-deleted record"
	);
	assert_eq!(res.to_sql().contains("NONE"), false, "deleted timestamp should be persisted");

	let mut resp = ds.execute("SELECT * FROM item", &anon, None).await.unwrap();
	assert_eq!(
		resp.remove(0).output().unwrap(),
		Value::Array(Array::new()),
		"anonymous user should not see soft-deleted records"
	);

	let mut resp = ds.execute("SELECT * FROM item", &owner, None).await.unwrap();
	let res = resp.remove(0).output().unwrap();
	assert_eq!(
		res.into_array().unwrap().len(),
		1,
		"owner should still see soft-deleted records via table scan"
	);
}
