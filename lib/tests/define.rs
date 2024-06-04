mod parse;
use parse::Parse;

mod helpers;
use helpers::*;

use std::collections::HashMap;

use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::iam::Role;
use surrealdb::sql::Idiom;
use surrealdb::sql::{Part, Value};

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
			namespaces: { test: 'DEFINE NAMESPACE test' },
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
		DEFINE TABLE test SCHEMAFULL;
		INFO FOR DB;
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
			functions: {},
			models: {},
			params: {},
			tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMAFULL PERMISSIONS NONE' },
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
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
			functions: {},
			models: {},
			params: {},
			tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMAFULL PERMISSIONS NONE' },
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
			functions: {},
			models: {},
			params: {},
			tables: {
				test: 'DEFINE TABLE test TYPE ANY SCHEMAFULL PERMISSIONS NONE',
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
			functions: {},
			models: {},
			params: {},
			tables: {
				test: 'DEFINE TABLE test TYPE ANY SCHEMAFULL PERMISSIONS NONE',
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
		DEFINE EVENT test ON TABLE user WHEN true THEN (
			CREATE activity SET user = $this, value = $after.email, action = $event
		);
		INFO FOR TABLE user;
		UPDATE user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPDATE user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPDATE user:test SET email = 'test@surrealdb.com', updated_at = time::now();
		SELECT count() FROM activity GROUP ALL;
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
			events: { test: 'DEFINE EVENT test ON user WHEN true THEN (CREATE activity SET user = $this, `value` = $after.email, action = $event)' },
			fields: {},
			tables: {},
			indexes: {},
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
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
		"[{
			count: 3
		}]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_event_when_event() -> Result<(), Error> {
	let sql = "
		DEFINE EVENT test ON user WHEN $event = 'CREATE' THEN (
			CREATE activity SET user = $this, value = $after.email, action = $event
		);
		DEFINE EVENT test ON TABLE user WHEN $event = 'CREATE' THEN (
			CREATE activity SET user = $this, value = $after.email, action = $event
		);
		INFO FOR TABLE user;
		UPDATE user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPDATE user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPDATE user:test SET email = 'test@surrealdb.com', updated_at = time::now();
		SELECT count() FROM activity GROUP ALL;
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
		r#"{
			events: { test: "DEFINE EVENT test ON user WHEN $event = 'CREATE' THEN (CREATE activity SET user = $this, `value` = $after.email, action = $event)" },
			fields: {},
			tables: {},
			indexes: {},
			lives: {},
		}"#,
	);
	assert_eq!(tmp, val);
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
		"[{
			count: 1
		}]",
	);
	assert_eq!(tmp, val);
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
		UPDATE test:1 set num = 2;
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
		DEFINE EVENT test ON TABLE user WHEN $before.email != $after.email THEN (
			CREATE activity SET user = $this, value = $after.email, action = $event
		);
		INFO FOR TABLE user;
		UPDATE user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPDATE user:test SET email = 'info@surrealdb.com', updated_at = time::now();
		UPDATE user:test SET email = 'test@surrealdb.com', updated_at = time::now();
		SELECT count() FROM activity GROUP ALL;
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
			events: { test: 'DEFINE EVENT test ON user WHEN $before.email != $after.email THEN (CREATE activity SET user = $this, `value` = $after.email, action = $event)' },
			fields: {},
			tables: {},
			indexes: {},
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
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
		"[{
			count: 2
		}]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_field() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON user;
		DEFINE FIELD test ON TABLE user;
		INFO FOR TABLE user;
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: { test: 'DEFINE FIELD test ON user PERMISSIONS FULL' },
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
async fn define_statement_field_type() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON user TYPE string;
		DEFINE FIELD test ON TABLE user TYPE string;
		INFO FOR TABLE user;
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: { test: 'DEFINE FIELD test ON user TYPE string PERMISSIONS FULL' },
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
async fn define_statement_field_value() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON user VALUE $value OR 'GBR';
		DEFINE FIELD test ON TABLE user VALUE $value OR 'GBR';
		INFO FOR TABLE user;
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"{
			events: {},
			fields: { test: "DEFINE FIELD test ON user VALUE $value OR 'GBR' PERMISSIONS FULL" },
			tables: {},
			indexes: {},
			lives: {},
		}"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_field_assert() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON user ASSERT $value != NONE AND $value = /[A-Z]{3}/;
		DEFINE FIELD test ON TABLE user ASSERT $value != NONE AND $value = /[A-Z]{3}/;
		INFO FOR TABLE user;
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: { test: 'DEFINE FIELD test ON user ASSERT $value != NONE AND $value = /[A-Z]{3}/ PERMISSIONS FULL' },
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
async fn define_statement_field_type_value_assert() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON user TYPE string VALUE $value OR 'GBR' ASSERT $value != NONE AND $value = /[A-Z]{3}/;
		DEFINE FIELD test ON TABLE user TYPE string VALUE $value OR 'GBR' ASSERT $value != NONE AND $value = /[A-Z]{3}/;
		INFO FOR TABLE user;
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"{
			events: {},
			fields: { test: "DEFINE FIELD test ON user TYPE string VALUE $value OR 'GBR' ASSERT $value != NONE AND $value = /[A-Z]{3}/ PERMISSIONS FULL" },
			tables: {},
			indexes: {},
			lives: {},
		}"#,
	);
	assert_eq!(tmp, val);
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
				"foo[*][*]": "DEFINE FIELD foo[*][*] ON bar TYPE bool PERMISSIONS FOR select, create, delete FULL, FOR update NONE"
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
async fn define_statement_index_single_simple() -> Result<(), Error> {
	let sql = "
		CREATE user:1 SET age = 23;
		CREATE user:2 SET age = 10;
		DEFINE INDEX test ON user FIELDS age;
		DEFINE INDEX test ON user COLUMNS age;
		INFO FOR TABLE user;
		UPDATE user:1 SET age = 24;
		UPDATE user:2 SET age = 11;
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
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS age' },
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:1, age: 24 }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:2, age: 11 }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_index_single() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS email;
		DEFINE INDEX test ON user COLUMNS email;
		INFO FOR TABLE user;
		CREATE user:1 SET email = 'test@surrealdb.com';
		CREATE user:2 SET email = 'test@surrealdb.com';
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS email' },
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:1, email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:2, email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_index_multiple() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS account, email;
		DEFINE INDEX test ON user COLUMNS account, email;
		INFO FOR TABLE user;
		CREATE user:1 SET account = 'apple', email = 'test@surrealdb.com';
		CREATE user:2 SET account = 'tesla', email = 'test@surrealdb.com';
		CREATE user:3 SET account = 'apple', email = 'test@surrealdb.com';
		CREATE user:4 SET account = 'tesla', email = 'test@surrealdb.com';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	skip_ok(res, 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS account, email' },
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:1, account: 'apple', email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:2, account: 'tesla', email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:3, account: 'apple', email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:4, account: 'tesla', email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_index_single_unique() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS email UNIQUE;
		DEFINE INDEX test ON user COLUMNS email UNIQUE;
		INFO FOR TABLE user;
		CREATE user:1 SET email = 'test@surrealdb.com';
		CREATE user:2 SET email = 'test@surrealdb.com';
		DELETE user:1;
		CREATE user:2 SET email = 'test@surrealdb.com';
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
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS email UNIQUE' },
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:1, email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database index `test` already contains 'test@surrealdb.com', with record `user:1`"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:2, email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_index_multiple_unique() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS account, email UNIQUE;
		DEFINE INDEX test ON user COLUMNS account, email UNIQUE;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 12);
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
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS account, email UNIQUE' },
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:1, account: 'apple', email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:2, account: 'tesla', email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database index `test` already contains ['apple', 'test@surrealdb.com'], with record `user:1`"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database index `test` already contains ['tesla', 'test@surrealdb.com'], with record `user:2`"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:3, account: 'apple', email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database index `test` already contains ['tesla', 'test@surrealdb.com'], with record `user:2`"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:4, account: 'tesla', email: 'test@surrealdb.com' }]");
	assert_eq!(tmp, val);
	//
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	for _ in 0..4 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
	}
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database index `test` already contains ['apple', 'test@surrealdb.com'], with record `user:1`"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database index `test` already contains ['apple', 'test@surrealdb.com'], with record `user:1`"#
	));

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
async fn define_statement_index_single_unique_embedded_multiple() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS tags UNIQUE;
		DEFINE INDEX test ON user COLUMNS tags UNIQUE;
		INFO FOR TABLE user;
		CREATE user:1 SET tags = ['one', 'two'];
		CREATE user:2 SET tags = ['two', 'three'];
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS tags UNIQUE' },
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:1, tags: ['one', 'two'] }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	if let Err(e) = tmp {
		assert_eq!(
			e.to_string(),
			"Database index `test` already contains 'two', with record `user:1`"
		);
	} else {
		panic!("An error was expected.")
	}
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_index_multiple_unique_embedded_multiple() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX test ON user FIELDS account, tags UNIQUE;
		DEFINE INDEX test ON user COLUMNS account, tags UNIQUE;
		INFO FOR TABLE user;
		CREATE user:1 SET account = 'apple', tags = ['one', 'two'];
		CREATE user:2 SET account = 'tesla', tags = ['one', 'two'];
		CREATE user:3 SET account = 'apple', tags = ['two', 'three'];
		CREATE user:4 SET account = 'tesla', tags = ['two', 'three'];
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
			events: {},
			fields: {},
			tables: {},
			indexes: { test: 'DEFINE INDEX test ON user FIELDS account, tags UNIQUE' },
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:1, account: 'apple', tags: ['one', 'two'] }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: user:2, account: 'tesla', tags: ['one', 'two'] }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	if let Err(e) = tmp {
		assert_eq!(
			e.to_string(),
			"Database index `test` already contains ['apple', 'two'], with record `user:1`"
		);
	} else {
		panic!("An error was expected.")
	}
	//
	let tmp = res.remove(0).result;
	if let Err(e) = tmp {
		assert_eq!(
			e.to_string(),
			"Database index `test` already contains ['tesla', 'two'], with record `user:2`"
		);
	} else {
		panic!("An error was expected.")
	}
	//
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
	let mut res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	skip_ok(&mut res, 1);
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
		INFO FOR DB;
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	for _ in 0..4 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"{
			accesses: {},
			analyzers: {
				autocomplete: 'DEFINE ANALYZER autocomplete FILTERS LOWERCASE,EDGENGRAM(2,10)',
				english: 'DEFINE ANALYZER english TOKENIZERS BLANK,CLASS FILTERS LOWERCASE,SNOWBALL(ENGLISH)',
				htmlAnalyzer: 'DEFINE ANALYZER htmlAnalyzer FUNCTION fn::stripHtml TOKENIZERS BLANK,CLASS'
			},
			functions: {
				stripHtml: "DEFINE FUNCTION fn::stripHtml($html: string) { RETURN string::replace($html, /<[^>]*>/, ''); } PERMISSIONS FULL"
			},
			models: {},
			params: {},
			tables: {},
			users: {},
		}"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
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
	check_path(&tmp, &["doc_ids", "total_size"], |v| assert_eq!(v, Value::from(63)));

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
		idiom.0 = part.clone();
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
		vec!["{ namespaces: { NS: 'DEFINE NAMESPACE NS' }, users: {  } }"],
		vec!["{ namespaces: {  }, users: {  } }"],
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
        vec!["{ accesses: {  }, analyzers: {  }, functions: { greet: \"DEFINE FUNCTION fn::greet() { RETURN 'Hello'; } PERMISSIONS FULL\" }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
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
        vec!["{ accesses: {  }, analyzers: { analyzer: 'DEFINE ANALYZER analyzer TOKENIZERS BLANK' }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
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
async fn permissions_checks_define_access_ns() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS access ON NS TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: { access: \"DEFINE ACCESS access ON NAMESPACE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION 1h\" }, databases: {  }, users: {  } }"],
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
        vec!["{ accesses: { access: \"DEFINE ACCESS access ON DATABASE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION 1h\" }, analyzers: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
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
		("test", "DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER SESSION 1d"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ namespaces: {  }, users: { user: \"DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER SESSION 1d\" } }"],
		vec!["{ namespaces: {  }, users: {  } }"]
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
		("test", "DEFINE USER user ON NS PASSHASH 'secret' ROLES VIEWER SESSION 1d"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, databases: {  }, users: { user: \"DEFINE USER user ON NAMESPACE PASSHASH 'secret' ROLES VIEWER SESSION 1d\" } }"],
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
		("test", "DEFINE USER user ON DB PASSHASH 'secret' ROLES VIEWER SESSION 1d"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, analyzers: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: { user: \"DEFINE USER user ON DATABASE PASSHASH 'secret' ROLES VIEWER SESSION 1d\" } }"],
		vec!["{ accesses: {  }, analyzers: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
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
		("test", "DEFINE ACCESS account ON DATABASE TYPE RECORD DURATION 1h WITH JWT ALGORITHM HS512 KEY 'secret'"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: { account: \"DEFINE ACCESS account ON DATABASE TYPE RECORD DURATION 1h WITH JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION 1h\" }, analyzers: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
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
        vec!["{ accesses: {  }, analyzers: {  }, functions: {  }, models: {  }, params: { param: \"DEFINE PARAM $param VALUE 'foo' PERMISSIONS FULL\" }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
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
        vec!["{ accesses: {  }, analyzers: {  }, functions: {  }, models: {  }, params: {  }, tables: { TB: 'DEFINE TABLE TB TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
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
async fn redefining_existing_analyzer_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE ANALYZER example_blank TOKENIZERS blank;
		DEFINE ANALYZER example_blank TOKENIZERS blank;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_analyzer_with_if_not_exists_should_error() -> Result<(), Error> {
	let sql = "
		DEFINE ANALYZER IF NOT EXISTS example TOKENIZERS blank;
		DEFINE ANALYZER IF NOT EXISTS example TOKENIZERS blank;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp, Error::AzAlreadyExists { .. }),);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_database_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE DATABASE example;
		DEFINE DATABASE example;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_database_with_if_not_exists_should_error() -> Result<(), Error> {
	let sql = "
		DEFINE DATABASE IF NOT EXISTS example;
		DEFINE DATABASE IF NOT EXISTS example;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp, Error::DbAlreadyExists { .. }),);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_event_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE EVENT example ON example THEN {};
		DEFINE EVENT example ON example THEN {};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_event_with_if_not_exists_should_error() -> Result<(), Error> {
	let sql = "
		DEFINE EVENT IF NOT EXISTS example ON example THEN {};
		DEFINE EVENT IF NOT EXISTS example ON example THEN {};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp, Error::EvAlreadyExists { .. }),);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_field_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD example ON example;
		DEFINE FIELD example ON example;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_field_with_if_not_exists_should_error() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD IF NOT EXISTS example ON example;
		DEFINE FIELD IF NOT EXISTS example ON example;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp, Error::FdAlreadyExists { .. }),);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_function_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE FUNCTION fn::example() {};
		DEFINE FUNCTION fn::example() {};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_function_with_if_not_exists_should_error() -> Result<(), Error> {
	let sql = "
		DEFINE FUNCTION IF NOT EXISTS fn::example() {};
		DEFINE FUNCTION IF NOT EXISTS fn::example() {};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp, Error::FcAlreadyExists { .. }),);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_index_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX example ON example FIELDS example;
		DEFINE INDEX example ON example FIELDS example;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_index_with_if_not_exists_should_error() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX IF NOT EXISTS example ON example FIELDS example;
		DEFINE INDEX IF NOT EXISTS example ON example FIELDS example;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp, Error::IxAlreadyExists { .. }),);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_namespace_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE NAMESPACE example;
		DEFINE NAMESPACE example;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_namespace_with_if_not_exists_should_error() -> Result<(), Error> {
	let sql = "
		DEFINE NAMESPACE IF NOT EXISTS example;
		DEFINE NAMESPACE IF NOT EXISTS example;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp, Error::NsAlreadyExists { .. }),);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_param_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE PARAM $example VALUE 123;
		DEFINE PARAM $example VALUE 123;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_param_with_if_not_exists_should_error() -> Result<(), Error> {
	let sql = "
		DEFINE PARAM IF NOT EXISTS $example VALUE 123;
		DEFINE PARAM IF NOT EXISTS $example VALUE 123;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp, Error::PaAlreadyExists { .. }),);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_access_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE ACCESS example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
		DEFINE ACCESS example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_access_with_if_not_exists_should_error() -> Result<(), Error> {
	let sql = "
		DEFINE ACCESS IF NOT EXISTS example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
		DEFINE ACCESS IF NOT EXISTS example ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp, Error::AccessDbAlreadyExists { .. }),);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_table_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE example;
		DEFINE TABLE example;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_table_with_if_not_exists_should_error() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE IF NOT EXISTS example;
		DEFINE TABLE IF NOT EXISTS example;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp, Error::TbAlreadyExists { .. }),);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_user_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE USER example ON ROOT PASSWORD \"example\" ROLES OWNER SESSION 1d;
		DEFINE USER example ON ROOT PASSWORD \"example\" ROLES OWNER SESSION 1d;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn redefining_existing_user_with_if_not_exists_should_error() -> Result<(), Error> {
	let sql = "
		DEFINE USER IF NOT EXISTS example ON ROOT PASSWORD \"example\" ROLES OWNER SESSION 1d;
		DEFINE USER IF NOT EXISTS example ON ROOT PASSWORD \"example\" ROLES OWNER SESSION 1d;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert!(matches!(tmp, Error::UserRootAlreadyExists { .. }),);
	//
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
		DEFINE TABLE likes TYPE RELATION IN person OUT person | thing;
		RELATE $person->likes->$thing;
		RELATE $person->likes->$other;
		DEFINE FIELD out ON TABLE likes TYPE record<person | thing | other>;
		RELATE $person->likes->$other;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 10);
	//
	for _ in 0..4 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
	}
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_err());
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
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	Ok(())
}

#[tokio::test]
async fn define_table_relation_redefinition_info() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE likes TYPE RELATION IN person OUT person;
		INFO FOR TABLE likes;
		INFO FOR DB;
		DEFINE TABLE likes TYPE RELATION IN person OUT person | thing;
		INFO FOR TABLE likes;
		INFO FOR DB;
		DEFINE FIELD out ON TABLE likes TYPE record<person | thing | other>;
		INFO FOR TABLE likes;
		INFO FOR DB;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: { in: 'DEFINE FIELD in ON likes TYPE record<person> PERMISSIONS FULL', out: 'DEFINE FIELD out ON likes TYPE record<person> PERMISSIONS FULL' },
			tables: {},
			indexes: {},
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
			functions: {},
			models: {},
			params: {},
			tables: { likes: 'DEFINE TABLE likes TYPE RELATION IN person OUT person SCHEMALESS PERMISSIONS NONE' },
			users: {},
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
			events: {},
			fields: { in: 'DEFINE FIELD in ON likes TYPE record<person> PERMISSIONS FULL', out: 'DEFINE FIELD out ON likes TYPE record<person | thing> PERMISSIONS FULL' },
			tables: {},
			indexes: {},
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
			functions: {},
			models: {},
			params: {},
			tables: { likes: 'DEFINE TABLE likes TYPE RELATION IN person OUT person | thing SCHEMALESS PERMISSIONS NONE' },
			users: {},
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
			events: {},
			fields: { in: 'DEFINE FIELD in ON likes TYPE record<person> PERMISSIONS FULL', out: 'DEFINE FIELD out ON likes TYPE record<person | thing | other> PERMISSIONS FULL' },
			tables: {},
			indexes: {},
			lives: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			accesses: {},
			analyzers: {},
			functions: {},
			models: {},
			params: {},
			tables: { likes: 'DEFINE TABLE likes TYPE RELATION IN person OUT person | thing | other SCHEMALESS PERMISSIONS NONE' },
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
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
