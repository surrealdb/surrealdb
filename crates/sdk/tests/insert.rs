use surrealdb_core::iam::Level;
use surrealdb_core::syn;
use surrealdb_core::val::{Array, Strand, Value};
mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::expr::Part;
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
	let dbs = new_ds().await?;
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

#[tokio::test]
async fn insert_statement_object_multiple() -> Result<()> {
	let sql = "
		INSERT INTO test [
			{
				id: 1,
				test: true,
				something: 'other',
			},
			{
				id: 2,
				test: false,
				something: 'else',
			},
		];
	";
	let dbs = new_ds().await?;
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
async fn insert_statement_values_single() -> Result<()> {
	let sql = "
		INSERT INTO test (id, test, something) VALUES ('tester', true, 'other');
	";
	let dbs = new_ds().await?;
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
	let dbs = new_ds().await?;
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
	let dbs = new_ds().await?;
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
	let dbs = new_ds().await?;
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
	let dbs = new_ds().await?;
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
		INSERT INTO company (name, founded) VALUES ('SurrealDB', '2021-09-12') ON DUPLICATE KEY UPDATE founded = $input.founded PARALLEL;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	assert_eq!(
		tmp.first().pick(&[Part::field("name".to_owned()).unwrap()]),
		Value::from(Strand::new("SurrealDB".to_owned()).unwrap())
	);
	assert_eq!(
		tmp.first().pick(&[Part::field("founded".to_owned()).unwrap()]),
		Value::from(Strand::new("2021-09-10".to_owned()).unwrap())
	);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(
		tmp.first().pick(&[Part::field("name".to_owned()).unwrap()]),
		Value::from(Strand::new("SurrealDB".to_owned()).unwrap())
	);
	assert_eq!(
		tmp.first().pick(&[Part::field("founded".to_owned()).unwrap()]),
		Value::from(Strand::new("2021-09-11".to_owned()).unwrap())
	);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(
		tmp.first().pick(&[Part::field("name".to_owned()).unwrap()]),
		Value::from(Strand::new("SurrealDB".to_owned()).unwrap())
	);
	assert_eq!(
		tmp.first().pick(&[Part::field("founded".to_owned()).unwrap()]),
		Value::from(Strand::new("2021-09-12".to_owned()).unwrap())
	);
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

	for ((level, role), (ns, db), should_succeed, msg) in tests.into_iter() {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		// Test the INSERT statement when the table has to be created
		{
			let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			if should_succeed {
				assert!(res.is_ok() && res.unwrap() != Array::new().into(), "{}", msg);
			} else if res.is_ok() {
				assert!(res.unwrap() == Array::new().into(), "{}", msg);
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

		// Test the INSERT statement when the table already exists
		{
			let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("NS").with_db("DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Array::new().into(),
				"unexpected error creating person record"
			);

			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("OTHER_NS").with_db("DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Array::new().into(),
				"unexpected error creating person record"
			);

			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("NS").with_db("OTHER_DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Array::new().into(),
				"unexpected error creating person record"
			);

			// Run the test
			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			if should_succeed {
				assert!(res.is_ok() && res.unwrap() != Array::new().into(), "{}", msg);
			} else if res.is_ok() {
				assert!(res.unwrap() == Array::new().into(), "{}", msg);
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
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		let err = res.unwrap_err().to_string();
		assert!(
			err.contains("Not enough permissions to perform this action"),
			"anonymous user should not be able to create the table: {}",
			err
		);
	}

	// When the table exists but grants no permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

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
			res.unwrap() == Array::new().into(),
			"{}",
			"anonymous user should not be able to insert a new record if the table exists but has no permissions"
		);
	}

	// When the table exists and grants full permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

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
			res.unwrap() != Array::new().into(),
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
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

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
			res.unwrap() != Array::new().into(),
			"{}",
			"anonymous user should be able to create the table"
		);
	}

	// When the table exists but grants no permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

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
			res.unwrap() != Array::new().into(),
			"{}",
			"anonymous user should not be able to insert a new record if the table exists but has no permissions"
		);
	}

	// When the table exists and grants full permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

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
			res.unwrap() != Array::new().into(),
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
	let dbs = new_ds().await?;
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
