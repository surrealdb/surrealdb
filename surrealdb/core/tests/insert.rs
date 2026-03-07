#![allow(clippy::unwrap_used)]

use surrealdb_core::iam::Level;
use surrealdb_core::syn;
use surrealdb_types::{Array, Value};

mod helpers;
use anyhow::Result;
use helpers::new_ds;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::Role;

use crate::helpers::Test;

#[tokio::test]
async fn insert_statement_object_single() -> Result<()> {
	let sql = "
		INSERT INTO `test-table` {
			id: 'tester',
			test: true,
			something: 'other',
		};
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ id: `test-table`:tester, test: true, something: 'other' }]").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

/// Test that INSERT RELATION preserves extra fields beyond in/out
/// Regression test for bug where RelateThrough::Table variant dropped extra fields
#[tokio::test]
async fn insert_relation_with_extra_fields() -> Result<()> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD name ON person TYPE string;
		
		DEFINE TABLE friendship TYPE RELATION IN person OUT person SCHEMAFULL;
		DEFINE FIELD strength ON friendship TYPE int;
		DEFINE FIELD since ON friendship TYPE string;
		
		INSERT INTO person [
			{ id: person:alice, name: 'Alice' },
			{ id: person:bob, name: 'Bob' }
		];
		
		-- Test single object with extra fields
		INSERT RELATION INTO friendship { 
			in: person:alice, 
			out: person:bob, 
			strength: 100,
			since: '2024-01-01'
		};
		
		SELECT strength, since FROM friendship;
	";
	let mut t = Test::new(sql).await?;

	// Skip: 2 DEFINE TABLE + 3 DEFINE FIELD + 1 INSERT INTO person = 6
	t.skip_ok(6)?;

	// Check the INSERT RELATION result has 1 record with extra fields
	let result = t.next()?.result?;
	let records = result.into_array().unwrap();
	assert_eq!(records.len(), 1, "Expected 1 relation record, got {}", records.len());

	// Check the SELECT result - extra fields should be preserved
	t.expect_val("[{ strength: 100, since: '2024-01-01' }]")?;

	Ok(())
}

/// Test INSERT RELATION with array of objects with extra fields
#[tokio::test]
async fn insert_relation_array_with_extra_fields() -> Result<()> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD name ON person TYPE string;
		
		DEFINE TABLE likes TYPE RELATION IN person OUT person SCHEMAFULL;
		DEFINE FIELD rating ON likes TYPE int;
		
		INSERT INTO person [
			{ id: person:a, name: 'A' },
			{ id: person:b, name: 'B' },
			{ id: person:c, name: 'C' }
		];
		
		INSERT RELATION INTO likes [
			{ in: person:a, out: person:b, rating: 5 },
			{ in: person:b, out: person:c, rating: 3 }
		];
		
		SELECT rating FROM likes ORDER BY rating DESC;
	";
	let mut t = Test::new(sql).await?;

	// Skip: 2 DEFINE TABLE + 1 DEFINE FIELD + 1 INSERT INTO person = 4
	// But INSERT INTO person is response 4, so skip 5 to get to INSERT RELATION
	t.skip_ok(5)?;

	// Check the INSERT RELATION array result
	let result = t.next()?.result?;
	let records = result.into_array().unwrap();
	assert_eq!(records.len(), 2, "Expected 2 relation records, got {}", records.len());

	// Check the SELECT result (ordered by rating DESC) - extra fields preserved
	t.expect_val("[{ rating: 5 }, { rating: 3 }]")?;

	Ok(())
}

/// Test INSERT RELATION with ON DUPLICATE KEY UPDATE and extra fields
#[tokio::test]
async fn insert_relation_on_duplicate_key_update_extra_fields() -> Result<()> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD name ON person TYPE string;
		
		DEFINE TABLE follows TYPE RELATION IN person OUT person SCHEMAFULL;
		DEFINE FIELD priority ON follows TYPE int;
		DEFINE INDEX idx_follows_unique ON follows FIELDS in, out UNIQUE;
		
		INSERT INTO person [
			{ id: person:x, name: 'X' },
			{ id: person:y, name: 'Y' }
		];
		
		-- Initial insert
		INSERT RELATION INTO follows { in: person:x, out: person:y, priority: 1 };
		
		-- Update with ON DUPLICATE KEY UPDATE  
		INSERT RELATION INTO follows { in: person:x, out: person:y, priority: 99 }
			ON DUPLICATE KEY UPDATE priority = $input.priority;
		
		SELECT priority FROM follows;
	";
	let mut t = Test::new(sql).await?;

	// Skip DEFINE statements and INSERT INTO person
	t.skip_ok(6)?;

	// Check initial insert has 1 record
	let result = t.next()?.result?;
	let records = result.into_array().unwrap();
	assert_eq!(records.len(), 1);

	// Check update result has 1 record
	let update_result = t.next()?.result?;
	let updated = update_result.into_array().unwrap();
	assert_eq!(updated.len(), 1);

	// Check final SELECT - priority should be updated to 99
	t.expect_val("[{ priority: 99 }]")?;

	Ok(())
}

#[tokio::test]
async fn insert_statement_values_single() -> Result<()> {
	let sql = "
		INSERT INTO test (id, test, something) VALUES ('tester', true, 'other');
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ id: test:tester, test: true, something: 'other' }]").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_statement_values_multiple() -> Result<()> {
	let sql = "
		INSERT INTO test (id, test, something) VALUES (1, true, 'other'), (2, false, 'else');
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{ id: test:1, test: true, something: 'other' },
			{ id: test:2, test: false, something: 'else' }
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_statement_values_retable_id() -> Result<()> {
	let sql = "
		INSERT INTO test (id, test, something) VALUES (person:1, true, 'other'), (person:2, false, 'else');
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{ id: test:1, test: true, something: 'other' },
			{ id: test:2, test: false, something: 'else' }
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_statement_on_duplicate_key() -> Result<()> {
	let sql = "
		INSERT INTO test (id, test, something) VALUES ('tester', true, 'other');
		INSERT INTO test (id, test, something) VALUES ('tester', true, 'other') ON DUPLICATE KEY UPDATE something = 'else';
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ id: test:tester, test: true, something: 'other' }]").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ id: test:tester, test: true, something: 'else' }]").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_with_savepoint() -> Result<()> {
	let sql = "
		DEFINE INDEX one ON pokemon FIELDS one UNIQUE;
		DEFINE INDEX two ON pokemon FIELDS two UNIQUE;
		-- This will INSERT a record with a specific id
		INSERT INTO pokemon (id, two) VALUES (1, 'two');
		-- This will INSERT a record with a random id
		INSERT INTO pokemon (id, one) VALUES ('test', 'one');
		-- This will fail, because a UNIQUE index value already exists
		INSERT INTO pokemon (two) VALUES ('two');
		-- This will fail, because a UNIQUE index value already exists
		INSERT INTO pokemon (id, one, two) VALUES (2, 'one', 'two');
		-- This will fail, because we are specifying a specific id even though we also have an ON DUPLICATE KEY UPDATE clause
		INSERT INTO pokemon (id, one, two) VALUES (2, 'one', 'two') ON DUPLICATE KEY UPDATE two = 'changed';
		-- This will succeed, because we are not specifying a specific id and we also have an ON DUPLICATE KEY UPDATE clause
		INSERT INTO pokemon (one, two) VALUES ('one', 'two') ON DUPLICATE KEY UPDATE two = 'changed';
		SELECT * FROM pokemon;
	";
	let mut t = Test::new(sql).await?;
	t.expect_size(9)?;
	t.skip_ok(2)?;
	t.expect_val(
		"[
			{
				id: pokemon:1,
				two: 'two'
			}
		]",
	)?;
	t.expect_val(
		"[
			{
				id: pokemon:test,
				one: 'one'
			}
		]",
	)?;
	t.expect_error("Database index `two` already contains 'two', with record `pokemon:1`")?;
	t.expect_error("Database index `one` already contains 'one', with record `pokemon:test`")?;
	t.expect_error("Database index `one` already contains 'one', with record `pokemon:test`")?;
	t.expect_val(
		"[
			{
				id: pokemon:test,
				one: 'one',
				two: 'changed'
			}
		]",
	)?;
	t.expect_val(
		"[
			{
				id: pokemon:1,
				two: 'two'
			},
			{
				id: pokemon:test,
				one: 'one',
				two: 'changed'
			}
		]",
	)?;
	Ok(())
}

#[tokio::test]
async fn insert_statement_output() -> Result<()> {
	let sql = "
		INSERT INTO test (id, test, something) VALUES ('tester', true, 'other') RETURN something;
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ something: 'other' }]").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_statement_duplicate_key_update() -> Result<()> {
	let sql = "
		DEFINE INDEX name ON TABLE company COLUMNS name UNIQUE;
		INSERT INTO company (name, founded) VALUES ('SurrealDB', '2021-09-10') ON DUPLICATE KEY UPDATE founded = $input.founded;
		INSERT INTO company (name, founded) VALUES ('SurrealDB', '2021-09-11') ON DUPLICATE KEY UPDATE founded = $input.founded;
		INSERT INTO company (name, founded) VALUES ('SurrealDB', '2021-09-12') ON DUPLICATE KEY UPDATE founded = $input.founded;
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp.first().unwrap().get("name"), Value::from_t("SurrealDB".to_owned()));
	assert_eq!(tmp.first().unwrap().get("founded"), Value::from_t("2021-09-10".to_owned()));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp.first().unwrap().get("name"), Value::from_t("SurrealDB".to_owned()));
	assert_eq!(tmp.first().unwrap().get("founded"), Value::from_t("2021-09-11".to_owned()));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp.first().unwrap().get("name"), Value::from_t("SurrealDB".to_owned()));
	assert_eq!(tmp.first().unwrap().get("founded"), Value::from_t("2021-09-12".to_owned()));
	//
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
			"owner at root level should be able to insert a new record",
		),
		(
			(level_root(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at root level should be able to insert a new record",
		),
		(
			(level_root(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at root level should not be able to insert a new record",
		),
		// Namespace level
		(
			(level_ns(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at namespace level should be able to insert a new record on its namespace",
		),
		(
			(level_ns(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			"owner at namespace level should not be able to insert a new record on another namespace",
		),
		(
			(level_ns(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at namespace level should be able to insert a new record on its namespace",
		),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			"editor at namespace level should not be able to insert a new record on another namespace",
		),
		(
			(level_ns(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at namespace level should not be able to insert a new record on its namespace",
		),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			"viewer at namespace level should not be able to insert a new record on another namespace",
		),
		// Database level
		(
			(level_db(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at database level should be able to insert a new record on its database",
		),
		(
			(level_db(), Role::Owner),
			("NS", "OTHER_DB"),
			false,
			"owner at database level should not be able to insert a new record on another database",
		),
		(
			(level_db(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			"owner at database level should not be able to insert a new record on another namespace even if the database name matches",
		),
		(
			(level_db(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at database level should be able to insert a new record on its database",
		),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			"editor at database level should not be able to insert a new record on another database",
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			"editor at database level should not be able to insert a new record on another namespace even if the database name matches",
		),
		(
			(level_db(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at database level should not be able to insert a new record on its database",
		),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			"viewer at database level should not be able to insert a new record on another database",
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			"viewer at database level should not be able to insert a new record on another namespace even if the database name matches",
		),
	];
	let statement = "INSERT INTO person (id) VALUES ('id')";

	for ((level, role), (ns, db), should_succeed, msg) in tests {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		// Test the INSERT statement when the table has to be created
		{
			let ds = new_ds("NS", "DB").await.unwrap().with_auth_enabled(auth_enabled);

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
				let err = res.unwrap_err();
				assert!(err.is_not_allowed(), "{}: expected NotAllowed, got {}", msg, err)
			}
		}

		// Test the INSERT statement when the table already exists
		{
			let ds = new_ds("NS", "DB").await.unwrap().with_auth_enabled(auth_enabled);

			// Define additional namespaces/databases for cross-namespace tests
			ds.execute(
				"DEFINE NS OTHER_NS; USE NS OTHER_NS; DEFINE DB DB; USE NS NS; DEFINE DB OTHER_DB;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();

			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("NS").with_db("DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::Array(Array::new()),
				"unexpected error creating person record"
			);

			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("OTHER_NS").with_db("DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::Array(Array::new()),
				"unexpected error creating person record"
			);

			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("NS").with_db("OTHER_DB"), None)
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
				assert!(res.is_ok() && res.unwrap() != Value::Array(Array::new()), "{}", msg);
			} else if res.is_ok() {
				assert!(res.unwrap() == Value::Array(Array::new()), "{}", msg);
			} else {
				// Not allowed to create a table
				let err = res.unwrap_err();
				assert!(err.is_not_allowed(), "{}: expected NotAllowed, got {}", msg, err)
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

	// When the table doesn't exist
	{
		let ds = new_ds("NS", "DB").await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
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

	// When the table exists but grants no permissions
	{
		let ds = new_ds("NS", "DB").await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS NONE",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() == Value::Array(Array::new()),
			"{}",
			"anonymous user should not be able to insert a new record if the table exists but has no permissions"
		);
	}

	// When the table exists and grants full permissions
	{
		let ds = new_ds("NS", "DB").await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS FULL",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Value::Array(Array::new()),
			"{}",
			"anonymous user should be able to insert a new record if the table exists and grants full permissions"
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

	// When the table doesn't exist
	{
		let ds = new_ds("NS", "DB").await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Value::Array(Array::new()),
			"{}",
			"anonymous user should be able to create the table"
		);
	}

	// When the table exists but grants no permissions
	{
		let ds = new_ds("NS", "DB").await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS NONE",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Value::Array(Array::new()),
			"{}",
			"anonymous user should not be able to insert a new record if the table exists but has no permissions"
		);
	}

	// When the table exists and grants full permissions
	{
		let ds = new_ds("NS", "DB").await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS FULL",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Value::Array(Array::new()),
			"{}",
			"anonymous user should be able to insert a new record if the table exists and grants full permissions"
		);
	}
}

#[tokio::test]
async fn insert_relation() -> Result<()> {
	let sql = "
		INSERT INTO person [
			{ id: person:1 },
			{ id: person:2 },
			{ id: person:3 },
		];
		INSERT RELATION INTO likes {
			in: person:1,
			id: 'object',
			out: person:2,
		};
		INSERT RELATION INTO likes [
			{
				in: person:1,
				id: 'array',
				out: person:2,
			},
			{
				in: person:2,
				id: 'array_twoo',
				out: person:3,
			}
		];
		INSERT RELATION INTO likes (in, id, out)
			VALUES (person:1, 'values', person:2);
		SELECT VALUE ->likes FROM person;
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ id: person:1 }, { id: person:2 }, { id: person:3 }]").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"
		[
			{
					id: likes:object,
					in: person:1,
					out: person:2
			}
		]
	",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"
		[
			{
                id: likes:array,
                in: person:1,
                out: person:2
			},
			{
				id: likes:array_twoo,
				in: person:2,
				out: person:3
			}
		]
	",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"
		[
			{
                id: likes:values,
                in: person:1,
                out: person:2
       		}
		]
	",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"
		[
			[
                likes:array,
                likes:object,
                likes:values
			],
			[
				likes:array_twoo
			],
			[]
		]
	",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_ignore() -> Result<()> {
	let sql = "
		USE NS test DB test;
		INSERT INTO user { id: 1, name: 'foo' };
		INSERT IGNORE INTO user { id: 1, name: 'bar' };
		";
	let mut t = Test::new(sql).await?;
	// USE NS test DB test;
	let tmp = t.next()?.result;
	tmp.unwrap();
	//
	t.expect_size(2)?;
	t.expect_vals(&["[{ id: user:1, name: 'foo' }]", "[]"])?;
	Ok(())
}

#[tokio::test]
async fn insert_relation_ignore_unique_index_fix_test() -> Result<()> {
	let sql = "
        USE NS test DB test;
        DEFINE INDEX key ON wrote FIELDS in, out UNIQUE;
        INSERT RELATION IGNORE INTO wrote [
            { in: author:one, out: blog:one },
            { in: author:one, out: blog:one }
        ];
        INSERT RELATION IGNORE INTO wrote { in: author:one, out: blog:one };
        SELECT * FROM wrote;
    ";
	let mut t = Test::new(sql).await?;

	// Skip USE and DEFINE INDEX
	t.skip_ok(2)?;

	let first_result = t.next()?.result?;
	assert_eq!(first_result.into_array().unwrap().len(), 1);

	t.expect_val("[]")?;

	let select_result = t.next()?.result?;
	let records = select_result.into_array().unwrap();

	assert_eq!(
		records.len(),
		1,
		"INSERT IGNORE bug: Expected 1 record, got {}. Duplicates not ignored!",
		records.len()
	);

	Ok(())
}
