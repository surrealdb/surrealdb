mod parse;

use parse::Parse;

mod helpers;
use helpers::*;

use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::iam::Role;
use surrealdb::kvs::{LockType, TransactionType};
use surrealdb::sql::Idiom;
use surrealdb::sql::{Part, Value};
use surrealdb_core::cnf::{INDEXING_BATCH_SIZE, NORMAL_FETCH_SIZE};
use test_log::test;
use tracing::info;

#[tokio::test]
async fn define_statement_namespace() -> Result<(), Error> {
	let sql = "
		DEFINE NAMESPACE test;
		INFO FOR ROOT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok(), "{:?}", tmp);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			namespaces: { test: 'DEFINE NAMESPACE test' },
			nodes: {},
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_database() -> Result<(), Error> {
	let sql = "
		DEFINE DATABASE test;
		INFO FOR NS;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			databases: { test: 'DEFINE DATABASE test' },
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_function() -> Result<(), Error> {
	let sql = "
		DEFINE FUNCTION fn::test($first: string, $last: string) {
			RETURN $first + $last;
		};
		INFO FOR DB;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
			configs: {},
			functions: { test: 'DEFINE FUNCTION fn::test($first: string, $last: string) { RETURN $first + $last; } PERMISSIONS FULL' },
			models: {},
			params: {},
			tables: {},
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_table_drop() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE test DROP;
		INFO FOR DB;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			tables: { test: 'DEFINE TABLE test TYPE ANY DROP SCHEMALESS PERMISSIONS NONE' },
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_table_schemaless() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE test SCHEMALESS;
		INFO FOR DB;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMALESS PERMISSIONS NONE' },
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_table_schemafull() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE test SCHEMAFUL;
		REMOVE TABLE test;
		DEFINE TABLE test SCHEMAFULL;
		INFO FOR DB;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(3)?;
	t.expect_val(
		"{
			accesses: {},
			analyzers: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			tables: { test: 'DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS NONE' },
			users: {},
		}",
	)?;
	Ok(())
}

#[tokio::test]
async fn define_statement_table_schemaful() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE test SCHEMAFUL;
		INFO FOR DB;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			tables: { test: 'DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS NONE' },
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_table_foreigntable() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE test SCHEMAFUL;
		DEFINE TABLE view AS SELECT count() FROM test GROUP ALL;
		INFO FOR DB;
		INFO FOR TB test;
		REMOVE TABLE view;
		INFO FOR DB;
		INFO FOR TB test;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			tables: {
				test: 'DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS NONE',
				view: 'DEFINE TABLE view TYPE ANY SCHEMALESS AS SELECT count() FROM test GROUP ALL PERMISSIONS NONE',
			},
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: { view: 'DEFINE TABLE view TYPE ANY SCHEMALESS AS SELECT count() FROM test GROUP ALL PERMISSIONS NONE' },
			indexes: {},
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			tables: {
				test: 'DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS NONE',
			},
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: {},
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_event() -> Result<(), Error> {
	let sql = "
		DEFINE EVENT test ON user WHEN true THEN (
			CREATE activity SET user = $this, value = $after.email, action = $event
		);
		REMOVE EVENT test ON user;
		DEFINE EVENT test ON TABLE user WHEN true THEN (
			CREATE activity SET user = $this, value = $after.email, action = $event
		);
		INFO FOR TABLE user;
		UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPSERT user:test SET email = 'test@surrealdb.com', updated_at = time::now();
		SELECT count() FROM activity GROUP ALL;
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(3)?;
	t.expect_val(
		"{
			events: { test: 'DEFINE EVENT test ON user WHEN true THEN (CREATE activity SET user = $this, `value` = $after.email, action = $event)' },
			fields: {},
			tables: {},
			indexes: {},
			lives: {},
		}",
	)?;
	t.skip_ok(3)?;
	t.expect_val(
		"[{
			count: 3
		}]",
	)?;
	//
	Ok(())
}

// Confirms that a DEFINE EVENT with no WHERE clause will produce WHEN true
#[tokio::test]
async fn define_statement_event_no_when_clause() -> Result<(), Error> {
	let sql = "
		DEFINE EVENT some_event ON TABLE user THEN {};
		INFO FOR TABLE user;
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(1)?;
	let val = Value::parse(
		"{
	events: {
		some_event: 'DEFINE EVENT some_event ON user WHEN true THEN {  }'
	},
	fields: {},
	indexes: {},
	lives: {},
	tables: {}
}",
	);
	t.expect_value(val)?;
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_event_when_event() -> Result<(), Error> {
	let sql = "
		DEFINE EVENT test ON user WHEN $event = 'CREATE' THEN (
			CREATE activity SET user = $this, value = $after.email, action = $event
		);
		REMOVE EVENT test ON user;
		DEFINE EVENT test ON TABLE user WHEN $event = 'CREATE' THEN (
			CREATE activity SET user = $this, value = $after.email, action = $event
		);
		INFO FOR TABLE user;
		UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPSERT user:test SET email = 'test@surrealdb.com', updated_at = time::now();
		SELECT count() FROM activity GROUP ALL;
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(3)?;
	t.expect_val(
		r#"{
			events: { test: "DEFINE EVENT test ON user WHEN $event = 'CREATE' THEN (CREATE activity SET user = $this, `value` = $after.email, action = $event)" },
			fields: {},
			tables: {},
			indexes: {},
			lives: {},
		}"#,
	)?;
	t.skip_ok(3)?;
	t.expect_val(
		"[{
			count: 1
		}]",
	)?;
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_event_check_doc_always_populated() -> Result<(), Error> {
	let sql = "
		DEFINE EVENT test ON test WHEN true THEN {
			LET $doc = $this;
			CREATE type::thing('log', $event) SET this = $doc, value = $value, before = $before, after = $after;
		};
		CREATE test:1 SET num = 1;
		UPSERT test:1 set num = 2;
		DELETE test:1;
		SELECT * FROM log;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
					after: { id: test:1, num: 1 },
					id: log:CREATE,
					this: { id: test:1, num: 1 },
					value: { id: test:1, num: 1 }
			},
			{
					before: { id: test:1, num: 2 },
					id: log:DELETE,
					this: { id: test:1, num: 2 },
					value: { id: test:1, num: 2 }
			},
			{
					after: { id: test:1, num: 2 },
					before: { id: test:1, num: 1 },
					id: log:UPDATE,
					this: { id: test:1, num: 2 },
					value: { id: test:1, num: 2 }
			}
	]"#,
	);
	assert_eq!(tmp, val, "{tmp} != {val}");
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_event_when_logic() -> Result<(), Error> {
	let sql = "
		DEFINE EVENT test ON user WHEN $before.email != $after.email THEN (
			CREATE activity SET user = $this, value = $after.email, action = $event
		);
		REMOVE EVENT test ON user;
		DEFINE EVENT test ON TABLE user WHEN $before.email != $after.email THEN (
			CREATE activity SET user = $this, value = $after.email, action = $event
		);
		INFO FOR TABLE user;
		UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPSERT user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPSERT user:test SET email = 'test@surrealdb.com', updated_at = time::now();
		SELECT count() FROM activity GROUP ALL;
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(3)?;
	t.expect_val(
		"{
			events: { test: 'DEFINE EVENT test ON user WHEN $before.email != $after.email THEN (CREATE activity SET user = $this, `value` = $after.email, action = $event)' },
			fields: {},
			tables: {},
			indexes: {},
			lives: {},
		}",
	)?;
	t.skip_ok(3)?;
	t.expect_val(
		"[{
			count: 2
		}]",
	)?;
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_field() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON user;
		REMOVE FIELD test ON user;
		DEFINE FIELD test ON TABLE user;
		INFO FOR TABLE user;
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(3)?;
	t.expect_val(
		"{
			events: {},
			fields: { test: 'DEFINE FIELD test ON user PERMISSIONS FULL' },
			tables: {},
			indexes: {},
			lives: {},
		}",
	)?;
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_field_type() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON user TYPE string;
		REMOVE FIELD test ON user;
		DEFINE FIELD test ON TABLE user TYPE string;
		INFO FOR TABLE user;
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(3)?;
	t.expect_val(
		"{
			events: {},
			fields: { test: 'DEFINE FIELD test ON user TYPE string PERMISSIONS FULL' },
			tables: {},
			indexes: {},
			lives: {},
		}",
	)?;
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_field_value() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON user VALUE $value OR 'GBR';
		REMOVE FIELD test ON user;
		DEFINE FIELD test ON TABLE user VALUE $value OR 'GBR';
		INFO FOR TABLE user;
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(3)?;
	t.expect_val(
		r#"{
			events: {},
			fields: { test: "DEFINE FIELD test ON user VALUE $value OR 'GBR' PERMISSIONS FULL" },
			tables: {},
			indexes: {},
			lives: {},
		}"#,
	)?;
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_field_assert() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON user ASSERT $value != NONE AND $value = /[A-Z]{3}/;
		REMOVE FIELD test ON user;
		DEFINE FIELD test ON TABLE user ASSERT $value != NONE AND $value = /[A-Z]{3}/;
		INFO FOR TABLE user;
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(3)?;
	t.expect_val(
		"{
			events: {},
			fields: { test: 'DEFINE FIELD test ON user ASSERT $value != NONE AND $value = /[A-Z]{3}/ PERMISSIONS FULL' },
			tables: {},
			indexes: {},
			lives: {},
		}",
	)?;
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_field_type_value_assert() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON user TYPE string VALUE $value OR 'GBR' ASSERT $value != NONE AND $value = /[A-Z]{3}/;
		REMOVE FIELD test ON user;
		DEFINE FIELD test ON TABLE user TYPE string VALUE $value OR 'GBR' ASSERT $value != NONE AND $value = /[A-Z]{3}/;
		INFO FOR TABLE user;
	";
	let mut t = Test::new(sql).await?;
	//
	t.skip_ok(3)?;
	t.expect_val(
		r#"{
			events: {},
			fields: { test: "DEFINE FIELD test ON user TYPE string VALUE $value OR 'GBR' ASSERT $value != NONE AND $value = /[A-Z]{3}/ PERMISSIONS FULL" },
			tables: {},
			indexes: {},
			lives: {},
		}"#,
	)?;
	//
	Ok(())
}

#[tokio::test]
async fn define_field_with_recursive_types() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE bar;
		// defining a type for the parent type should overwrite permissions for the child.
		DEFINE FIELD foo.*.* ON bar TYPE number PERMISSIONS FOR UPDATE NONE;
		// this should recursively define types for foo, foo.*, and foo.*.*
		DEFINE FIELD foo ON bar TYPE array<float | array<bool>> | set<number>;
		INFO FOR TABLE bar;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();

	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"{
			events: {},
			fields: {
				"foo": "DEFINE FIELD foo ON bar TYPE array<float | array<bool>> | set<number> PERMISSIONS FULL",
				"foo[*]": "DEFINE FIELD foo[*] ON bar TYPE float | array<bool> | number PERMISSIONS FULL",
				"foo[*][*]": "DEFINE FIELD foo[*][*] ON bar TYPE bool PERMISSIONS FOR select, create FULL, FOR update NONE"
			},
			indexes: {},
			lives: {},
			tables: {}
		}"#,
	);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn define_statement_field_permissions() -> Result<(), Error> {
	// Specific permissions
	// Delete permission should be ignored
	{
		let sql = "
			DEFINE FIELD test ON user PERMISSIONS FOR select, create, update, delete WHERE true;
			INFO FOR TABLE user;
		";
		let mut t = Test::new(sql).await?;
		//
		t.skip_ok(1)?;
		t.expect_val(
			r#"{
				events: {},
				fields: { test: "DEFINE FIELD test ON user PERMISSIONS FOR select, create, update WHERE true" },
				tables: {},
				indexes: {},
				lives: {},
			}"#,
		)?;
	}
	// Full permissions
	// Delete none should still show as full permissions
	{
		let sql = "
			DEFINE FIELD test ON user PERMISSIONS FOR delete NONE, FOR select, create, update FULL;
			INFO FOR TABLE user;
		";
		let mut t = Test::new(sql).await?;
		//
		t.skip_ok(1)?;
		t.expect_val(
			r#"{
				events: {},
				fields: { test: "DEFINE FIELD test ON user PERMISSIONS FULL" },
				tables: {},
				indexes: {},
				lives: {},
			}"#,
		)?;
	}
	// No permissions
	// Delete full not be printed, as it is effectively full
	// TODO(gguillemas): This should display simply as PERMISSIONS NONE in 3.0.0.
	{
		let sql = "
			DEFINE FIELD test ON user PERMISSIONS FOR DELETE FULL, FOR SELECT, CREATE, UPDATE NONE;
			INFO FOR TABLE user;
		";
		let mut t = Test::new(sql).await?;
		//
		t.skip_ok(1)?;
		t.expect_val(
			r#"{
				events: {},
				fields: { test: "DEFINE FIELD test ON user PERMISSIONS FOR select, create, update NONE" },
				tables: {},
				indexes: {},
				lives: {},
			}"#,
		)?;
	}

	Ok(())
}

#[tokio::test]
async fn define_statement_index_single_simple() -> Result<(), Error> {
	let sql = "
		CREATE user:1 SET age = 23;
		CREATE user:2 SET age = 10;
		DEFINE INDEX test ON user FIELDS age;
		REMOVE INDEX test ON user;
		DEFINE INDEX test ON user COLUMNS age;
		INFO FOR TABLE user;
		UPSERT user:1 SET age = 24;
		UPSERT user:2 SET age = 11;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(5)?;
	t.expect_val(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS age' },
			lives: {},
		}",
	)?;
	t.expect_vals(&["[{ id: user:1, age: 24 }]", "[{ id: user:2, age: 11 }]"])?;
	Ok(())
}

#[tokio::test]
async fn define_statement_index_single() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS email;
		REMOVE INDEX test ON user;
		DEFINE INDEX test ON user COLUMNS email;
		INFO FOR TABLE user;
		CREATE user:1 SET email = 'test@surrealdb.com';
		CREATE user:2 SET email = 'test@surrealdb.com';
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(3)?;
	t.expect_val(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS email' },
			lives: {},
		}",
	)?;
	t.expect_vals(&[
		"[{ id: user:1, email: 'test@surrealdb.com' }]",
		"[{ id: user:2, email: 'test@surrealdb.com' }]",
	])?;
	Ok(())
}

#[tokio::test]
async fn define_statement_index_numbers() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX index ON TABLE test COLUMNS number;
		CREATE test:int SET number = 0;
		CREATE test:float SET number = 0.0;
		-- TODO: CREATE test:dec_int SET number = 0dec;
		-- TODO: CREATE test:dec_dec SET number = 0.0dec;
		SELECT * FROM test WITH NOINDEX WHERE number = 0 ORDER BY id;
		SELECT * FROM test WHERE number = 0 ORDER BY id;
		SELECT * FROM test WHERE number = 0.0 ORDER BY id;
		-- TODO: SELECT * FROM test WHERE number = 0dec ORDER BY id;
		-- TODO: SELECT * FROM test WHERE number = 0.0dec ORDER BY id;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(3)?;
	for _ in 0..3 {
		t.expect_val(
			"[
				// {
				// 	id: test:dec,
				// 	number: 0.0dec
				// },
				// {
				// 	id: test:int,
				// 	number: 0dec
				// },
				{
					id: test:float,
					number: 0f
				},
				{
					id: test:int,
					number: 0
				}
			]",
		)?;
	}
	Ok(())
}

#[tokio::test]
async fn define_statement_unique_index_numbers() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX index ON TABLE test COLUMNS number UNIQUE;
		CREATE test:int SET number = 0;
		CREATE test:float SET number = 0.0;
		-- TODO: CREATE test:dec_int SET number = 0dec;
		-- TODO: CREATE test:dec_dec SET number = 0.0dec;
		SELECT * FROM test WITH NOINDEX WHERE number = 0 ORDER BY id;
		SELECT * FROM test WHERE number = 0 ORDER BY id;
		SELECT * FROM test WHERE number = 0.0 ORDER BY id;
		-- TODO: SELECT * FROM test WHERE number = 0dec ORDER BY id;
		-- TODO: SELECT * FROM test WHERE number = 0.0dec ORDER BY id;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(3)?;
	for _ in 0..3 {
		t.expect_val(
			"[
				// {
				// 	id: test:dec,
				// 	number: 0.0dec
				// },
				// {
				// 	id: test:int,
				// 	number: 0dec
				// },
				{
					id: test:float,
					number: 0f
				},
				{
					id: test:int,
					number: 0
				}
			]",
		)?;
	}
	Ok(())
}

fn check_range(t: &mut Test, result: &str, explain: &str) -> Result<(), Error> {
	for _ in 0..2 {
		t.expect_val(result)?;
	}
	t.expect_val(explain)?;
	Ok(())
}

async fn define_statement_mixed_numbers_range(unique: bool) -> Result<(), Error> {
	let sql = format!(
		"
		DEFINE INDEX index ON TABLE test COLUMNS number {};
		CREATE test:int0 SET number = 0;
		CREATE test:float0 SET number = 0.5;
		CREATE test:int1 SET number = 1;
		CREATE test:float1 SET number = 1.5;
		CREATE test:int2 SET number = 2;
		SELECT * FROM test WITH NOINDEX WHERE number > 0 AND number < 2 ORDER BY id;
		SELECT * FROM test WHERE number > 0 AND number < 2 ORDER BY id;
		SELECT * FROM test WHERE number > 0 AND number < 2 ORDER BY id EXPLAIN;
		SELECT * FROM test WITH NOINDEX WHERE number > 0.1 AND number < 2 ORDER BY id;
		SELECT * FROM test WHERE number > 0.1 AND number < 2 ORDER BY id;
		SELECT * FROM test WHERE number > 0.1 AND number < 2 ORDER BY id EXPLAIN;
		SELECT * FROM test WITH NOINDEX WHERE number > 0.1 AND number < 1.5 ORDER BY id;
		SELECT * FROM test WHERE number > 0.1 AND number < 1.5 ORDER BY id;
		SELECT * FROM test WHERE number > 0.1 AND number < 1.5 ORDER BY id EXPLAIN;
		SELECT * FROM test WITH NOINDEX WHERE number > 0 AND number < 1.5 ORDER BY id;
		SELECT * FROM test WHERE number > 0 AND number < 1.5 ORDER BY id;
		SELECT * FROM test WHERE number > 0 AND number < 1.5 ORDER BY id EXPLAIN;
		SELECT * FROM test WITH NOINDEX WHERE number > 0.1 ORDER BY id;
		SELECT * FROM test WHERE number > 0.1 ORDER BY id;
		SELECT * FROM test WHERE number > 0.1 ORDER BY id EXPLAIN;
		SELECT * FROM test WITH NOINDEX WHERE number > 0 ORDER BY id;
		SELECT * FROM test WHERE number > 0 ORDER BY id;
		SELECT * FROM test WHERE number > 0 ORDER BY id EXPLAIN;
		SELECT * FROM test WITH NOINDEX WHERE number < 2 ORDER BY id;
		SELECT * FROM test WHERE number < 2 ORDER BY id;
		SELECT * FROM test WHERE number < 2 ORDER BY id EXPLAIN;
		SELECT * FROM test WITH NOINDEX WHERE number < 1.9 ORDER BY id;
		SELECT * FROM test WHERE number < 1.9 ORDER BY id;
		SELECT * FROM test WHERE number < 1.9 ORDER BY id EXPLAIN;
		",
		if unique {
			"UNIQUE"
		} else {
			""
		}
	);
	let mut t = Test::new(&sql).await?;
	t.expect_size(30)?;
	t.skip_ok(6)?;
	// number > 0 AND number < 2
	check_range(
		&mut t,
		"[
					{
						id: test:float0,
						number: 0.5f
					},
					{
						id: test:float1,
						number: 1.5f
					},
					{
						id: test:int1,
						number: 1
					}
				]",
		"[
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: 0
								},
								index: 'index',
								to: {
									inclusive: false,
									value: 2
								}
							},
							table: 'test'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'MemoryOrdered'
						},
						operation: 'Collector'
					}
				]",
	)?;
	// number > 0.1 AND number < 2
	check_range(
		&mut t,
		"[
					{
						id: test:float0,
						number: 0.5f
					},
					{
						id: test:float1,
						number: 1.5f
					},
					{
						id: test:int1,
						number: 1
					}
				]",
		"[
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: 0.1f
								},
								index: 'index',
								to: {
									inclusive: false,
									value: 2
								}
							},
							table: 'test'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'MemoryOrdered'
						},
						operation: 'Collector'
					}
				]",
	)?;
	// number > 0.1 AND number < 1.5
	check_range(
		&mut t,
		"[
					{
						id: test:float0,
						number: 0.5f
					},
					{
						id: test:int1,
						number: 1
					}
				]",
		"[
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: 0.1f
								},
								index: 'index',
								to: {
									inclusive: false,
									value: 1.5f
								}
							},
							table: 'test'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'MemoryOrdered'
						},
						operation: 'Collector'
					}
				]",
	)?;
	// number > 0 AND number < 1.5
	check_range(
		&mut t,
		"[
					{
						id: test:float0,
						number: 0.5f
					},
					{
						id: test:int1,
						number: 1
					}
				]",
		"[
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: 0
								},
								index: 'index',
								to: {
									inclusive: false,
									value: 1.5f
								}
							},
							table: 'test'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'MemoryOrdered'
						},
						operation: 'Collector'
					}
				]",
	)?;
	// number > 0.1
	check_range(
		&mut t,
		"[
					{
						id: test:float0,
						number: 0.5f
					},
					{
						id: test:float1,
						number: 1.5f
					},
					{
						id: test:int1,
						number: 1
					},
					{
						id: test:int2,
						number: 2
					}
				]",
		"[
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: 0.1f
								},
								index: 'index',
								to: {
									inclusive: false,
									value: NONE
								}
							},
							table: 'test'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'MemoryOrdered'
						},
						operation: 'Collector'
					}
				]",
	)?;
	// number > 0
	check_range(
		&mut t,
		"[
					{
						id: test:float0,
						number: 0.5f
					},
					{
						id: test:float1,
						number: 1.5f
					},
					{
						id: test:int1,
						number: 1
					},
					{
						id: test:int2,
						number: 2
					}
				]",
		"[
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: 0
								},
								index: 'index',
								to: {
									inclusive: false,
									value: NONE
								}
							},
							table: 'test'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'MemoryOrdered'
						},
						operation: 'Collector'
					}
				]",
	)?;
	// number < 2
	check_range(
		&mut t,
		"[
					{
						id: test:float0,
						number: 0.5f
					},
					{
						id: test:float1,
						number: 1.5f
					},
					{
						id: test:int0,
						number: 0
					},
					{
						id: test:int1,
						number: 1
					}
				]",
		"[
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: NONE
								},
								index: 'index',
								to: {
									inclusive: false,
									value: 2
								}
							},
							table: 'test'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'MemoryOrdered'
						},
						operation: 'Collector'
					}
				]",
	)?;
	// number < 1.9
	check_range(
		&mut t,
		"[
					{
						id: test:float0,
						number: 0.5f
					},
					{
						id: test:float1,
						number: 1.5f
					},
					{
						id: test:int0,
						number: 0
					},
					{
						id: test:int1,
						number: 1
					}
				]",
		"[
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: NONE
								},
								index: 'index',
								to: {
									inclusive: false,
									value: 1.9f
								}
							},
							table: 'test'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'MemoryOrdered'
						},
						operation: 'Collector'
					}
				]",
	)?;
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_index_mixed_numbers_range() -> Result<(), Error> {
	define_statement_mixed_numbers_range(false).await
}

#[tokio::test]
async fn define_statement_unique_mixed_numbers_range() -> Result<(), Error> {
	define_statement_mixed_numbers_range(true).await
}

#[tokio::test]
async fn define_statement_index_concurrently() -> Result<(), Error> {
	let sql = "
		CREATE user:1 SET email = 'testA@surrealdb.com';
		CREATE user:2 SET email = 'testA@surrealdb.com';
		CREATE user:3 SET email = 'testB@surrealdb.com';
		DEFINE INDEX test ON user FIELDS email CONCURRENTLY;
		SLEEP 1s;
		INFO FOR TABLE user;
		INFO FOR INDEX test ON user;
		SELECT * FROM user WHERE email = 'testA@surrealdb.com' EXPLAIN;
		SELECT * FROM user WHERE email = 'testA@surrealdb.com';
		SELECT * FROM user WHERE email = 'testB@surrealdb.com';
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(5)?;
	t.expect_val(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: {
				test: 'DEFINE INDEX test ON user FIELDS email CONCURRENTLY',
			},
			lives: {},
		}",
	)?;
	t.expect_val(
		"{
			building: { status: 'built' }
		}",
	)?;
	t.expect_val(
		" [
				{
					detail: {
						plan: {
							index: 'test',
							operator: '=',
							value: 'testA@surrealdb.com'
						},
						table: 'user'
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]",
	)?;
	t.expect_val(
		"[
				{
					email: 'testA@surrealdb.com',
					id: user:1
				},
				{
					email: 'testA@surrealdb.com',
					id: user:2
				}
			]",
	)?;
	t.expect_val(
		"[
				{
					email: 'testB@surrealdb.com',
					id: user:3
				}
			]",
	)?;
	Ok(())
}

#[test(tokio::test)]
async fn define_statement_index_concurrently_building_status() -> Result<(), Error> {
	let session = Session::owner().with_ns("test").with_db("test");
	let ds = new_ds().await?;
	// Populate initial records
	let initial_count = *INDEXING_BATCH_SIZE * 3 / 2;
	info!("Populate: {}", initial_count);
	for i in 0..initial_count {
		let mut responses = ds
			.execute(
				&format!("CREATE user:{i} SET email = 'test{i}@surrealdb.com';"),
				&session,
				None,
			)
			.await?;
		skip_ok(&mut responses, 1)?;
	}
	// Create the index concurrently
	info!("Indexing starts");
	let mut r =
		ds.execute("DEFINE INDEX test ON user FIELDS email CONCURRENTLY", &session, None).await?;
	skip_ok(&mut r, 1)?;
	//
	let mut appending_count = *NORMAL_FETCH_SIZE * 3 / 2;
	info!("Appending: {}", appending_count);
	// Loop until the index is built
	let now = SystemTime::now();
	let mut initial_count = None;
	let mut updates_count = None;
	// While the concurrent indexing is running, we update and delete records
	let time_out = Duration::from_secs(120);
	loop {
		if now.elapsed().map_err(|e| Error::Internal(e.to_string()))?.gt(&time_out) {
			panic!("Time-out {time_out:?}");
		}
		if appending_count > 0 {
			let sql = if appending_count % 2 != 0 {
				format!("UPDATE user:{appending_count} SET email = 'new{appending_count}@surrealdb.com';")
			} else {
				format!("DELETE user:{appending_count}")
			};
			let mut responses = ds.execute(&sql, &session, None).await?;
			skip_ok(&mut responses, 1)?;
			appending_count -= 1;
		}
		// We monitor the status
		let mut r = ds.execute("INFO FOR INDEX test ON user", &session, None).await?;
		let tmp = r.remove(0).result?;
		if let Value::Object(o) = &tmp {
			if let Some(Value::Object(b)) = o.get("building") {
				if let Some(v) = b.get("status") {
					if Value::from("started").eq(v) {
						continue;
					}
					let new_count = b.get("count").cloned();
					if Value::from("initial").eq(v) {
						if new_count != initial_count {
							assert!(new_count > initial_count, "{new_count:?}");
							info!("New initial count: {:?}", new_count);
							initial_count = new_count;
						}
						continue;
					}
					if Value::from("updates").eq(v) {
						if new_count != updates_count {
							assert!(new_count > updates_count, "{new_count:?}");
							info!("New updates count: {:?}", new_count);
							updates_count = new_count;
						}
						continue;
					}
					let val = Value::parse(
						"{
										building: {
											status: 'built'
										}
									}",
					);
					assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
					break;
				}
			}
		}
		panic!("Unexpected value {tmp:#}");
	}
	Ok(())
}

#[tokio::test]
async fn define_statement_index_multiple() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS account, email;
		REMOVE INDEX test ON user;
		DEFINE INDEX test ON user COLUMNS account, email;
		INFO FOR TABLE user;
		CREATE user:1 SET account = 'apple', email = 'test@surrealdb.com';
		CREATE user:2 SET account = 'tesla', email = 'test@surrealdb.com';
		CREATE user:3 SET account = 'apple', email = 'test@surrealdb.com';
		CREATE user:4 SET account = 'tesla', email = 'test@surrealdb.com';
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(3)?;
	t.expect_val(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS account, email' },
			lives: {},
		}",
	)?;
	t.expect_vals(&[
		"[{ id: user:1, account: 'apple', email: 'test@surrealdb.com' }]",
		"[{ id: user:2, account: 'tesla', email: 'test@surrealdb.com' }]",
		"[{ id: user:3, account: 'apple', email: 'test@surrealdb.com' }]",
		"[{ id: user:4, account: 'tesla', email: 'test@surrealdb.com' }]",
	])?;
	Ok(())
}

#[tokio::test]
async fn define_statement_index_single_unique() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS email UNIQUE;
		REMOVE INDEX test ON user;
		DEFINE INDEX test ON user COLUMNS email UNIQUE;
		INFO FOR TABLE user;
		CREATE user:1 SET email = 'test@surrealdb.com';
		CREATE user:2 SET email = 'test@surrealdb.com';
		DELETE user:1;
		CREATE user:2 SET email = 'test@surrealdb.com';
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(3)?;
	t.expect_val(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS email UNIQUE' },
			lives: {},
		}",
	)?;
	t.expect_val("[{ id: user:1, email: 'test@surrealdb.com' }]")?;
	t.expect_error(
		r#"Database index `test` already contains 'test@surrealdb.com', with record `user:1`"#,
	)?;
	t.skip_ok(1)?;
	t.expect_val("[{ id: user:2, email: 'test@surrealdb.com' }]")?;
	Ok(())
}

#[tokio::test]
async fn define_statement_index_multiple_unique() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS account, email UNIQUE;
		DEFINE INDEX test ON user COLUMNS account, email UNIQUE;
		DEFINE INDEX IF NOT EXISTS test ON user COLUMNS account, email UNIQUE;
		REMOVE INDEX test ON user;
		DEFINE INDEX IF NOT EXISTS test ON user COLUMNS account, email UNIQUE;
		INFO FOR TABLE user;
		CREATE user:1 SET account = 'apple', email = 'test@surrealdb.com';
		CREATE user:2 SET account = 'tesla', email = 'test@surrealdb.com';
		CREATE user:3 SET account = 'apple', email = 'test@surrealdb.com';
		CREATE user:4 SET account = 'tesla', email = 'test@surrealdb.com';
		DELETE user:1;
		CREATE user:3 SET account = 'apple', email = 'test@surrealdb.com';
		CREATE user:4 SET account = 'tesla', email = 'test@surrealdb.com';
		DELETE user:2;
		CREATE user:4 SET account = 'tesla', email = 'test@surrealdb.com';
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_error("The index 'test' already exists")?;
	t.expect_val("None")?;
	t.skip_ok(2)?;
	t.expect_val(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS account, email UNIQUE' },
			lives: {},
		}",
	)?;
	t.expect_val("[{ id: user:1, account: 'apple', email: 'test@surrealdb.com' }]")?;
	t.expect_val("[{ id: user:2, account: 'tesla', email: 'test@surrealdb.com' }]")?;
	t.expect_error(
		r#"Database index `test` already contains ['apple', 'test@surrealdb.com'], with record `user:1`"#,
	)?;
	t.expect_error(
		r#"Database index `test` already contains ['tesla', 'test@surrealdb.com'], with record `user:2`"#,
	)?;
	t.skip_ok(1)?;
	t.expect_val("[{ id: user:3, account: 'apple', email: 'test@surrealdb.com' }]")?;
	t.expect_error(
		r#"Database index `test` already contains ['tesla', 'test@surrealdb.com'], with record `user:2`"#,
	)?;
	t.skip_ok(1)?;
	t.expect_val("[{ id: user:4, account: 'tesla', email: 'test@surrealdb.com' }]")?;
	Ok(())
}

#[tokio::test]
async fn define_statement_index_single_unique_existing() -> Result<(), Error> {
	let sql = "
		CREATE user:1 SET email = 'info@surrealdb.com';
		CREATE user:2 SET email = 'test@surrealdb.com';
		CREATE user:3 SET email = 'test@surrealdb.com';
		DEFINE INDEX test ON user FIELDS email UNIQUE;
		DEFINE INDEX test ON user COLUMNS email UNIQUE;
		INFO FOR TABLE user;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	for _ in 0..3 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
	}
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database index `test` already contains 'test@surrealdb.com', with record `user:2`"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database index `test` already contains 'test@surrealdb.com', with record `user:2`"#
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: {},
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_index_multiple_unique_existing() -> Result<(), Error> {
	let sql = "
		CREATE user:1 SET account = 'apple', email = 'test@surrealdb.com';
		CREATE user:2 SET account = 'tesla', email = 'test@surrealdb.com';
		CREATE user:3 SET account = 'apple', email = 'test@surrealdb.com';
		CREATE user:4 SET account = 'tesla', email = 'test@surrealdb.com';
		DEFINE INDEX test ON user FIELDS account, email UNIQUE;
		DEFINE INDEX test ON user COLUMNS account, email UNIQUE;
		INFO FOR TABLE user;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(4)?;
	t.expect_error(
		r#"Database index `test` already contains ['apple', 'test@surrealdb.com'], with record `user:1`"#,
	)?;
	t.expect_error(
		r#"Database index `test` already contains ['apple', 'test@surrealdb.com'], with record `user:1`"#,
	)?;
	t.expect_val(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: {},
			lives: {},
		}",
	)?;
	Ok(())
}

#[tokio::test]
async fn define_statement_index_single_unique_embedded_multiple() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS tags UNIQUE;
		REMOVE INDEX test ON user;
		DEFINE INDEX test ON user COLUMNS tags UNIQUE;
		INFO FOR TABLE user;
		CREATE user:1 SET tags = ['one', 'two'];
		CREATE user:2 SET tags = ['two', 'three'];
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(3)?;
	t.expect_val(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS tags UNIQUE' },
			lives: {},
		}",
	)?;
	t.expect_val("[{ id: user:1, tags: ['one', 'two'] }]")?;
	t.expect_error("Database index `test` already contains 'two', with record `user:1`")?;
	Ok(())
}

#[tokio::test]
async fn define_statement_index_multiple_unique_embedded_multiple() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS account, tags UNIQUE;
		REMOVE INDEX test ON user;
		DEFINE INDEX test ON user COLUMNS account, tags UNIQUE;
		INFO FOR TABLE user;
		CREATE user:1 SET account = 'apple', tags = ['one', 'two'];
		CREATE user:2 SET account = 'tesla', tags = ['one', 'two'];
		CREATE user:3 SET account = 'apple', tags = ['two', 'three'];
		CREATE user:4 SET account = 'tesla', tags = ['two', 'three'];
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(3)?;
	t.expect_val(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS account, tags UNIQUE' },
			lives: {},
		}",
	)?;
	t.expect_val("[{ id: user:1, account: 'apple', tags: ['one', 'two'] }]")?;
	t.expect_val("[{ id: user:2, account: 'tesla', tags: ['one', 'two'] }]")?;
	t.expect_error(
		"Database index `test` already contains ['apple', 'two'], with record `user:1`",
	)?;
	t.expect_error(
		"Database index `test` already contains ['tesla', 'two'], with record `user:2`",
	)?;
	Ok(())
}

#[tokio::test]
async fn define_statement_index_multiple_hnsw() -> Result<(), Error> {
	let sql = "
		CREATE pts:3 SET point = [8,9,10,11];
		DEFINE INDEX IF NOT EXISTS hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
		DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
		DEFINE INDEX IF NOT EXISTS hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
		REMOVE INDEX hnsw_pts ON pts;
		DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
		INFO FOR TABLE pts;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(2)?;
	t.expect_error("The index 'hnsw_pts' already exists")?;
	t.expect_val("None")?;
	t.skip_ok(2)?;
	t.expect_val(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: {
				hnsw_pts: 'DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12 M0 24 LM 0.40242960438184466f'
			},
			lives: {},
		}",
	)?;
	Ok(())
}

#[tokio::test]
async fn define_statement_index_on_schemafull_without_permission() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE test SCHEMAFULL PERMISSIONS NONE;
		DEFINE INDEX idx ON test FIELDS foo;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	skip_ok(res, 1)?;
	//
	let tmp = res.remove(0).result;
	let s = format!("{:?}", tmp);
	assert!(
		tmp.is_err_and(|e| {
			if let Error::FdNotFound {
				value,
			} = e
			{
				assert_eq!(value, "foo", "Wrong field: {value}");
				true
			} else {
				false
			}
		}),
		"Expected error, but got: {:?}",
		s
	);
	Ok(())
}

#[tokio::test]
async fn define_statement_analyzer() -> Result<(), Error> {
	let sql = r#"
		DEFINE ANALYZER english TOKENIZERS blank,class FILTERS lowercase,snowball(english);
		DEFINE ANALYZER autocomplete FILTERS lowercase,edgengram(2,10);
        DEFINE FUNCTION fn::stripHtml($html: string) {
            RETURN string::replace($html, /<[^>]*>/, "");
        };
        DEFINE ANALYZER htmlAnalyzer FUNCTION fn::stripHtml TOKENIZERS blank,class;
        DEFINE ANALYZER englishLemmatizer TOKENIZERS blank,class FILTERS mapper('../../tests/data/lemmatization-en.txt');
		INFO FOR DB;
	"#;
	let mut t = Test::new(sql).await?;
	t.expect_size(6)?;
	t.skip_ok(5)?;
	t.expect_val(
		r#"{
			accesses: {},
			analyzers: {
				autocomplete: 'DEFINE ANALYZER autocomplete FILTERS LOWERCASE,EDGENGRAM(2,10)',
				english: 'DEFINE ANALYZER english TOKENIZERS BLANK,CLASS FILTERS LOWERCASE,SNOWBALL(ENGLISH)',
				englishLemmatizer: 'DEFINE ANALYZER englishLemmatizer TOKENIZERS BLANK,CLASS FILTERS MAPPER(../../tests/data/lemmatization-en.txt)',
				htmlAnalyzer: 'DEFINE ANALYZER htmlAnalyzer FUNCTION fn::stripHtml TOKENIZERS BLANK,CLASS'
			},
			configs: {},
			functions: {
				stripHtml: "DEFINE FUNCTION fn::stripHtml($html: string) { RETURN string::replace($html, /<[^>]*>/, ''); } PERMISSIONS FULL"
			},
			models: {},
			params: {},
			tables: {},
			users: {},
		}"#,
	)?;
	Ok(())
}

#[tokio::test]
async fn define_statement_search_index() -> Result<(), Error> {
	let sql = r#"
		CREATE blog:1 SET title = 'Understanding SurrealQL and how it is different from PostgreSQL';
		CREATE blog:3 SET title = 'This blog is going to be deleted';
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX blog_title ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		CREATE blog:2 SET title = 'Behind the scenes of the exciting beta 9 release';
		DELETE blog:3;
		INFO FOR TABLE blog;
		ANALYZE INDEX blog_title ON blog;
	"#;

	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 8);
	//
	for i in 0..6 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok(), "{}", i);
	}

	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { blog_title: 'DEFINE INDEX blog_title ON blog FIELDS title \
			SEARCH ANALYZER simple BM25(1.2,0.75) \
			DOC_IDS_ORDER 100 DOC_LENGTHS_ORDER 100 POSTINGS_ORDER 100 TERMS_ORDER 100 \
			DOC_IDS_CACHE 100 DOC_LENGTHS_CACHE 100 POSTINGS_CACHE 100 TERMS_CACHE 100 HIGHLIGHTS' },
			lives: {},
		}",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));

	let tmp = res.remove(0).result?;

	check_path(&tmp, &["doc_ids", "keys_count"], |v| assert_eq!(v, Value::from(2)));
	check_path(&tmp, &["doc_ids", "max_depth"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["doc_ids", "nodes_count"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["doc_ids", "total_size"], |v| assert_eq!(v, Value::from(62)));

	check_path(&tmp, &["doc_lengths", "keys_count"], |v| assert_eq!(v, Value::from(2)));
	check_path(&tmp, &["doc_lengths", "max_depth"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["doc_lengths", "nodes_count"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["doc_lengths", "total_size"], |v| assert_eq!(v, Value::from(56)));

	check_path(&tmp, &["postings", "keys_count"], |v| assert_eq!(v, Value::from(17)));
	check_path(&tmp, &["postings", "max_depth"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["postings", "nodes_count"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["postings", "total_size"], |v| assert!(v > Value::from(150)));

	check_path(&tmp, &["terms", "keys_count"], |v| assert_eq!(v, Value::from(17)));
	check_path(&tmp, &["terms", "max_depth"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["terms", "nodes_count"], |v| assert_eq!(v, Value::from(1)));
	check_path(&tmp, &["terms", "total_size"], |v| assert!(v.gt(&Value::from(150))));

	Ok(())
}

#[tokio::test]
async fn define_statement_user_root() -> Result<(), Error> {
	let sql = "
		DEFINE USER test ON ROOT PASSWORD 'test';

		INFO FOR ROOT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner();
	let res = &mut dbs.execute(sql, &ses, None).await?;

	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;

	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let define_str = tmp.pick(&["users".into(), "test".into()]).to_string();

	assert!(define_str
		.strip_prefix('\"')
		.unwrap()
		.starts_with("DEFINE USER test ON ROOT PASSHASH '$argon2id$"));
	Ok(())
}

#[tokio::test]
async fn define_statement_user_ns() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner();

	// Create a NS user and retrieve it.
	let sql = "
		USE NS ns;
		DEFINE USER test ON NS PASSWORD 'test';

		INFO FOR USER test;
		INFO FOR USER test ON NS;
		INFO FOR USER test ON NAMESPACE;
		INFO FOR USER test ON ROOT;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;

	assert!(res[1].result.is_ok());
	assert!(res[2].result.is_ok());
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert_eq!(
		res[5].result.as_ref().unwrap_err().to_string(),
		"The root user 'test' does not exist"
	); // User doesn't exist at the NS level

	assert!(res[2]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test ON NAMESPACE PASSHASH '$argon2id$"));
	assert!(res[3]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test ON NAMESPACE PASSHASH '$argon2id$"));
	assert!(res[4]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test ON NAMESPACE PASSHASH '$argon2id$"));

	// If it tries to create a NS user without specifying a NS, it should fail
	let sql = "
		DEFINE USER test ON NS PASSWORD 'test';
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;

	assert!(res.remove(0).result.is_err());

	Ok(())
}

#[tokio::test]
async fn define_statement_user_db() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner();

	// Create a NS user and retrieve it.
	let sql = "
		USE NS ns;
		USE DB db;
		DEFINE USER test ON DB PASSWORD 'test';

		INFO FOR USER test;
		INFO FOR USER test ON DB;
		INFO FOR USER test ON DATABASE;
		INFO FOR USER test ON NS;
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;

	assert!(res[2].result.is_ok());
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert!(res[5].result.is_ok());
	assert_eq!(
		res[6].result.as_ref().unwrap_err().to_string(),
		"The user 'test' does not exist in the namespace 'ns'"
	); // User doesn't exist at the NS level

	assert!(res[3]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test ON DATABASE PASSHASH '$argon2id$"));
	assert!(res[4]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test ON DATABASE PASSHASH '$argon2id$"));
	assert!(res[5]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test ON DATABASE PASSHASH '$argon2id$"));

	// If it tries to create a NS user without specifying a NS, it should fail
	let sql = "
		DEFINE USER test ON DB PASSWORD 'test';
	";
	let res = &mut dbs.execute(sql, &ses, None).await?;

	assert!(res.remove(0).result.is_err());

	Ok(())
}

fn check_path<F>(val: &Value, path: &[&str], check: F)
where
	F: Fn(Value),
{
	let part: Vec<Part> = path.iter().map(|p| Part::from(*p)).collect();
	let res = val.walk(&part);
	for (i, v) in res {
		let mut idiom = Idiom::default();
		idiom.0.clone_from(&part);
		assert_eq!(idiom, i);
		check(v);
	}
}

//
// Permissions
//

#[tokio::test]
async fn permissions_checks_define_ns() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE NAMESPACE NS"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
		vec!["{ accesses: {  }, namespaces: { NS: 'DEFINE NAMESPACE NS' }, nodes: {  }, users: {  } }"],
		vec!["{ accesses: {  }, namespaces: {  }, nodes: {  }, users: {  } }"],
	];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), false),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_db() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "DEFINE DATABASE DB"), ("check", "INFO FOR NS")]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
		vec!["{ accesses: {  }, databases: { DB: 'DEFINE DATABASE DB' }, users: {  } }"],
		vec!["{ accesses: {  }, databases: {  }, users: {  } }"],
	];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_function() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE FUNCTION fn::greet() {RETURN \"Hello\";}"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: { greet: \"DEFINE FUNCTION fn::greet() { RETURN 'Hello'; } PERMISSIONS FULL\" }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_analyzer() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ANALYZER analyzer TOKENIZERS BLANK"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, analyzers: { analyzer: 'DEFINE ANALYZER analyzer TOKENIZERS BLANK' }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_root() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS access ON ROOT TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: { access: \"DEFINE ACCESS access ON ROOT TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE\" }, namespaces: {  }, nodes: {  }, users: {  } }"],
		vec!["{ accesses: {  }, namespaces: {  }, nodes: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), false),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_ns() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS access ON NS TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: { access: \"DEFINE ACCESS access ON NAMESPACE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE\" }, databases: {  }, users: {  } }"],
		vec!["{ accesses: {  }, databases: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_db() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS access ON DB TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: { access: \"DEFINE ACCESS access ON DATABASE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE\" }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_user_root() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, namespaces: {  }, nodes: {  }, users: { user: \"DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h\" } }"],
		vec!["{ accesses: {  }, namespaces: {  }, nodes: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), false),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_user_ns() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE USER user ON NS PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, databases: {  }, users: { user: \"DEFINE USER user ON NAMESPACE PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h\" } }"],
		vec!["{ accesses: {  }, databases: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_user_db() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE USER user ON DB PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: { user: \"DEFINE USER user ON DATABASE PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h\" } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_record() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS account ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY 'secret' DURATION FOR TOKEN 15m, FOR SESSION 12h"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: { account: \"DEFINE ACCESS account ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 15m, FOR SESSION 12h\" }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_param() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE PARAM $param VALUE 'foo'"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: { param: \"DEFINE PARAM $param VALUE 'foo' PERMISSIONS FULL\" }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_table() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "DEFINE TABLE TB"), ("check", "INFO FOR DB")]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: { TB: 'DEFINE TABLE TB TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_event() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE EVENT event ON TABLE TB WHEN true THEN RETURN 'foo'"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ events: { event: \"DEFINE EVENT event ON TB WHEN true THEN (RETURN 'foo')\" }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"],
		vec!["{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_field() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE FIELD field ON TABLE TB"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ events: {  }, fields: { field: 'DEFINE FIELD field ON TB PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"],
		vec!["{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_index() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE INDEX index ON TABLE TB FIELDS field"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ events: {  }, fields: {  }, indexes: { index: 'DEFINE INDEX index ON TB FIELDS field' }, lives: {  }, tables: {  } }"],
		vec!["{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn define_statement_table_permissions() -> Result<(), Error> {
	// Permissions for tables, unlike other resources, are restrictive (NONE) by default.
	// This test ensures that behaviour
	let sql = "
		DEFINE TABLE default;
		DEFINE TABLE select_full PERMISSIONS FOR select FULL;
		DEFINE TABLE full PERMISSIONS FULL;
		INFO FOR DB;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
 			configs: {},
			functions: {},
			models: {},
			params: {},
			tables: {
					default: 'DEFINE TABLE default TYPE ANY SCHEMALESS PERMISSIONS NONE',
					full: 'DEFINE TABLE full TYPE ANY SCHEMALESS PERMISSIONS FULL',
					select_full: 'DEFINE TABLE select_full TYPE ANY SCHEMALESS PERMISSIONS FOR select FULL, FOR create, update, delete NONE'
			},
			users: {}
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_remove_analyzer() -> Result<(), Error> {
	let sql = "
		DEFINE ANALYZER example_blank TOKENIZERS blank;
		DEFINE ANALYZER IF NOT EXISTS example_blank TOKENIZERS blank;
		DEFINE ANALYZER OVERWRITE example_blank TOKENIZERS blank;
		DEFINE ANALYZER example_blank TOKENIZERS blank;
		REMOVE ANALYZER IF EXISTS example_blank;
		REMOVE ANALYZER example_blank;
		REMOVE ANALYZER IF EXISTS example_blank;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val("None")?;
	t.skip_ok(1)?;
	t.expect_error("The analyzer 'example_blank' already exists")?;
	t.skip_ok(1)?;
	t.expect_error("The analyzer 'example_blank' does not exist")?;
	t.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn define_remove_database() -> Result<(), Error> {
	let sql = "
		DEFINE DATABASE example;
		DEFINE DATABASE IF NOT EXISTS example;
		DEFINE DATABASE OVERWRITE example;
		DEFINE DATABASE example;
		REMOVE DATABASE IF EXISTS example;
		REMOVE DATABASE example;
		REMOVE DATABASE IF EXISTS example;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val("None")?;
	t.skip_ok(1)?;
	t.expect_error("The database 'example' already exists")?;
	t.skip_ok(1)?;
	t.expect_error("The database 'example' does not exist")?;
	t.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn define_remove_event() -> Result<(), Error> {
	let sql = "
		DEFINE EVENT example ON example THEN {};
		DEFINE EVENT IF NOT EXISTS example ON example THEN {};
		DEFINE EVENT OVERWRITE example ON example THEN {};
		DEFINE EVENT example ON example THEN {};
		REMOVE EVENT IF EXISTS example ON example;
		REMOVE EVENT example ON example;
		REMOVE EVENT IF EXISTS example ON example;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val("None")?;
	t.skip_ok(1)?;
	t.expect_error("The event 'example' already exists")?;
	t.skip_ok(1)?;
	t.expect_error("The event 'example' does not exist")?;
	t.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn define_remove_field() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD example ON example;
		DEFINE FIELD IF NOT EXISTS example ON example;
		DEFINE FIELD OVERWRITE example ON example;
		DEFINE FIELD example ON example;
		REMOVE FIELD IF EXISTS example ON example;
		REMOVE FIELD example ON example;
		REMOVE FIELD IF EXISTS example ON example;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val("None")?;
	t.skip_ok(1)?;
	t.expect_error("The field 'example' already exists")?;
	t.skip_ok(1)?;
	t.expect_error("The field 'example' does not exist")?;
	t.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn define_remove_function() -> Result<(), Error> {
	let sql = "
		DEFINE FUNCTION fn::example() {};
		DEFINE FUNCTION IF NOT EXISTS fn::example() {};
		DEFINE FUNCTION OVERWRITE fn::example() {};
		DEFINE FUNCTION fn::example() {};
		REMOVE FUNCTION IF EXISTS fn::example();
		REMOVE FUNCTION fn::example();
		REMOVE FUNCTION IF EXISTS fn::example();
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val("None")?;
	t.skip_ok(1)?;
	t.expect_error("The function 'fn::example' already exists")?;
	t.skip_ok(1)?;
	t.expect_error("The function 'fn::example' does not exist")?;
	t.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn define_remove_indexes() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX example ON example FIELDS example;
		DEFINE INDEX IF NOT EXISTS example ON example FIELDS example;
		DEFINE INDEX OVERWRITE example ON example FIELDS example;
		DEFINE INDEX example ON example FIELDS example;
		REMOVE INDEX IF EXISTS example ON example;
		REMOVE INDEX example ON example;
		REMOVE INDEX IF EXISTS example ON example;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val("None")?;
	t.skip_ok(1)?;
	t.expect_error("The index 'example' already exists")?;
	t.skip_ok(1)?;
	t.expect_error("The index 'example' does not exist")?;
	t.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn define_remove_namespace() -> Result<(), Error> {
	let sql = "
		DEFINE NAMESPACE example;
		DEFINE NAMESPACE IF NOT EXISTS example;
		DEFINE NAMESPACE OVERWRITE example;
		DEFINE NAMESPACE example;
		REMOVE NAMESPACE IF EXISTS example;
		REMOVE NAMESPACE example;
		REMOVE NAMESPACE IF EXISTS example;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val("None")?;
	t.skip_ok(1)?;
	t.expect_error("The namespace 'example' already exists")?;
	t.skip_ok(1)?;
	t.expect_error("The namespace 'example' does not exist")?;
	t.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn define_remove_param() -> Result<(), Error> {
	let sql = "
		DEFINE PARAM $example VALUE 123;
		DEFINE PARAM IF NOT EXISTS $example VALUE 123;
		DEFINE PARAM OVERWRITE $example VALUE 123;
		DEFINE PARAM $example VALUE 123;
		REMOVE PARAM IF EXISTS $example;
		REMOVE PARAM $example;
		REMOVE PARAM IF EXISTS $example;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val("None")?;
	t.skip_ok(1)?;
	t.expect_error("The param '$example' already exists")?;
	t.skip_ok(1)?;
	t.expect_error("The param '$example' does not exist")?;
	t.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn define_remove_access() -> Result<(), Error> {
	let sql = "
		DEFINE ACCESS example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
		DEFINE ACCESS IF NOT EXISTS example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
		DEFINE ACCESS OVERWRITE example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
		DEFINE ACCESS example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
		REMOVE ACCESS IF EXISTS example ON DB;
		REMOVE ACCESS example ON DB;
		REMOVE ACCESS IF EXISTS example ON DB;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val("None")?;
	t.skip_ok(1)?;
	t.expect_error("The access method 'example' already exists in the database 'test'")?;
	t.skip_ok(1)?;
	t.expect_error("The access method 'example' does not exist in the database 'test'")?;
	t.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn define_remove_tables() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE example;
		DEFINE TABLE IF NOT EXISTS example;
		DEFINE TABLE OVERWRITE example;
		DEFINE TABLE example;
		REMOVE TABLE IF EXISTS example;
		REMOVE TABLE example;
		REMOVE TABLE IF EXISTS example;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val("None")?;
	t.skip_ok(1)?;
	t.expect_error("The table 'example' already exists")?;
	t.skip_ok(1)?;
	t.expect_error("The table 'example' does not exist")?;
	t.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn define_remove_users() -> Result<(), Error> {
	let sql = "
		DEFINE USER example ON ROOT PASSWORD \"example\" ROLES OWNER DURATION FOR TOKEN 15m, FOR SESSION 6h;
		DEFINE USER IF NOT EXISTS example ON ROOT PASSWORD \"example\" ROLES OWNER DURATION FOR TOKEN 15m, FOR SESSION 6h;
		DEFINE USER OVERWRITE example ON ROOT PASSWORD \"example\" ROLES OWNER DURATION FOR TOKEN 15m, FOR SESSION 6h;
		DEFINE USER example ON ROOT PASSWORD \"example\" ROLES OWNER DURATION FOR TOKEN 15m, FOR SESSION 6h;
		REMOVE USER IF EXISTS example ON ROOT;
		REMOVE USER example ON ROOT;
		REMOVE USER IF EXISTS example ON ROOT;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val("None")?;
	t.skip_ok(1)?;
	t.expect_error("The root user 'example' already exists")?;
	t.skip_ok(1)?;
	t.expect_error("The root user 'example' does not exist")?;
	t.expect_val("None")?;
	Ok(())
}

#[tokio::test]
async fn define_table_relation() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE likes TYPE RELATION;
		CREATE person:raphael, person:tobie;
		RELATE person:raphael->likes->person:tobie;
		CREATE likes:1;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_err());
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_index_empty_array() -> Result<(), Error> {
	let sql = r"
		DEFINE TABLE indexTest;
		INSERT INTO indexTest { arr: [] };
		DEFINE INDEX idx_arr ON TABLE indexTest COLUMNS arr;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	for _ in 0..3 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
	}
	//
	Ok(())
}

#[tokio::test]
async fn define_table_relation_in_out() -> Result<(), Error> {
	let sql = r"
		DEFINE TABLE likes TYPE RELATION FROM person TO person | thing SCHEMAFUL;
		LET $first_p = CREATE person SET name = 'first person';
		LET $second_p = CREATE person SET name = 'second person';
		LET $thing = CREATE thing SET name = 'rust';
		LET $other = CREATE other;
		RELATE $first_p->likes->$thing;
		RELATE $first_p->likes->$second_p;
		CREATE likes;
		RELATE $first_p->likes->$other;
		RELATE $thing->likes->$first_p;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 10);
	//
	for _ in 0..7 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
	}
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp,
		Err(crate::Error::TableCheck {
			thing: _,
			relation: _,
			target_type: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_err());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_err());
	//

	Ok(())
}

#[tokio::test]
async fn define_table_relation_redefinition() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE likes TYPE RELATION IN person OUT person;
		LET $person = CREATE person;
		LET $thing = CREATE thing;
		LET $other = CREATE other;
		RELATE $person->likes->$thing;
		REMOVE TABLE likes;
		DEFINE TABLE likes TYPE RELATION IN person OUT person | thing;
		RELATE $person->likes->$thing;
		RELATE $person->likes->$other;
		REMOVE FIELD out ON TABLE likes;
		DEFINE FIELD out ON TABLE likes TYPE record<person | thing | other>;
		RELATE $person->likes->$other;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(4)?;
	t.expect_error_func(|e| matches!(e, Error::FieldCheck { .. }))?;
	t.skip_ok(3)?;
	t.expect_error_func(|e| matches!(e, Error::FieldCheck { .. }))?;
	t.skip_ok(3)?;
	Ok(())
}

#[tokio::test]
async fn define_table_relation_redefinition_info() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE likes TYPE RELATION IN person OUT person;
		INFO FOR TABLE likes;
		INFO FOR DB;
		REMOVE TABLE likes;
		DEFINE TABLE likes TYPE RELATION IN person OUT person | thing;
		INFO FOR TABLE likes;
		INFO FOR DB;
		REMOVE FIELD out ON TABLE likes;
		DEFINE FIELD out ON TABLE likes TYPE record<person | thing | other>;
		INFO FOR TABLE likes;
		INFO FOR DB;
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(1)?;
	t.expect_val(
		"{
			events: {},
			fields: {
				in: 'DEFINE FIELD in ON likes TYPE record<person> PERMISSIONS FULL',
				out: 'DEFINE FIELD out ON likes TYPE record<person> PERMISSIONS FULL'
			},
			tables: {},
			indexes: {},
			lives: {},
		}",
	)?;
	t.expect_val(
		"{
			accesses: {},
			analyzers: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			tables: {
				likes: 'DEFINE TABLE likes TYPE RELATION IN person OUT person SCHEMALESS PERMISSIONS NONE'
			},
			users: {},
		}",
	)?;
	t.skip_ok(2)?;
	t.expect_val(
		"{
			events: {},
			fields: {
				in: 'DEFINE FIELD in ON likes TYPE record<person> PERMISSIONS FULL',
				out: 'DEFINE FIELD out ON likes TYPE record<person | thing> PERMISSIONS FULL'
			},
			tables: {},
			indexes: {},
			lives: {},
		}",
	)?;
	t.expect_val(
		"{
			accesses: {},
			analyzers: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			tables: { likes: 'DEFINE TABLE likes TYPE RELATION IN person OUT person | thing SCHEMALESS PERMISSIONS NONE' },
			users: {},
		}",
	)?;
	t.skip_ok(2)?;
	t.expect_val(
		"{
			events: {},
			fields: {
				in: 'DEFINE FIELD in ON likes TYPE record<person> PERMISSIONS FULL',
				out: 'DEFINE FIELD out ON likes TYPE record<person | thing | other> PERMISSIONS FULL'
			},
			tables: {},
			indexes: {},
			lives: {},
		}",
	)?;
	t.expect_val(
		"{
			accesses: {},
			analyzers: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			tables: {
				likes: 'DEFINE TABLE likes TYPE RELATION IN person OUT person | thing | other SCHEMALESS PERMISSIONS NONE'
			},
			users: {},
		}",
	)?;
	Ok(())
}

#[tokio::test]
async fn define_table_type_normal() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE thing TYPE NORMAL;
		CREATE thing;
		RELATE foo:one->thing->foo:two;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_err());
	//
	Ok(())
}

#[tokio::test]
async fn define_table_type_any() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE thing TYPE ANY;
		CREATE thing;
		RELATE foo:one->thing->foo:two;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	Ok(())
}

#[tokio::test]
async fn cross_transaction_caching_uuids_updated() -> Result<(), Error> {
	let ds = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	// Define the table, set the initial uuids
	let sql = r"DEFINE TABLE test;".to_owned();
	let res = &mut ds.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	assert!(res.remove(0).result.is_ok());
	// Obtain the initial uuids
	let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
	let initial = txn.get_tb(&"test", &"test", &"test").await?;
	drop(txn);

	// Define some resources to refresh the UUIDs
	let sql = r"
		DEFINE FIELD test ON test;
		DEFINE EVENT test ON test WHEN {} THEN {};
		DEFINE TABLE view AS SELECT * FROM test;
		DEFINE INDEX test ON test FIELDS test;
		LIVE SELECT * FROM test;
	"
	.to_owned();
	let res = &mut ds.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	assert!(res.remove(0).result.is_ok());
	assert!(res.remove(0).result.is_ok());
	assert!(res.remove(0).result.is_ok());
	assert!(res.remove(0).result.is_ok());
	let lqid = res.remove(0).result?;
	assert!(matches!(lqid, Value::Uuid(_)));
	// Obtain the uuids after definitions
	let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
	let after_define = txn.get_tb(&"test", &"test", &"test").await?;
	drop(txn);
	// Compare uuids after definitions
	assert_ne!(initial.cache_fields_ts, after_define.cache_fields_ts);
	assert_ne!(initial.cache_events_ts, after_define.cache_events_ts);
	assert_ne!(initial.cache_tables_ts, after_define.cache_tables_ts);
	assert_ne!(initial.cache_indexes_ts, after_define.cache_indexes_ts);
	assert_ne!(initial.cache_lives_ts, after_define.cache_lives_ts);

	// Remove the defined resources to refresh the UUIDs
	let sql = r"
		REMOVE FIELD test ON test;
		REMOVE EVENT test ON test;
		REMOVE TABLE view;
		REMOVE INDEX test ON test;
		KILL $lqid;
	"
	.to_owned();
	let vars = map! { "lqid".to_string() => lqid };
	let res = &mut ds.execute(&sql, &ses, Some(vars)).await?;
	assert_eq!(res.len(), 5);
	assert!(res.remove(0).result.is_ok());
	assert!(res.remove(0).result.is_ok());
	assert!(res.remove(0).result.is_ok());
	assert!(res.remove(0).result.is_ok());
	assert!(res.remove(0).result.is_ok());
	// Obtain the uuids after definitions
	let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
	let after_remove = txn.get_tb(&"test", &"test", &"test").await?;
	drop(txn);
	// Compare uuids after definitions
	assert_ne!(after_define.cache_fields_ts, after_remove.cache_fields_ts);
	assert_ne!(after_define.cache_events_ts, after_remove.cache_events_ts);
	assert_ne!(after_define.cache_tables_ts, after_remove.cache_tables_ts);
	assert_ne!(after_define.cache_indexes_ts, after_remove.cache_indexes_ts);
	assert_ne!(after_define.cache_lives_ts, after_remove.cache_lives_ts);
	//
	Ok(())
}
