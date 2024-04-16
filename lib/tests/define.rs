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
			databases: { test: 'DEFINE DATABASE test' },
			tokens: {},
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
			analyzers: {},
			tokens: {},
			functions: { test: 'DEFINE FUNCTION fn::test($first: string, $last: string) { RETURN $first + $last; } PERMISSIONS FULL' },
			models: {},
			params: {},
			scopes: {},
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
			analyzers: {},
			tokens: {},
			functions: {},
			models: {},
			params: {},
			scopes: {},
			tables: { test: 'DEFINE TABLE test DROP SCHEMALESS PERMISSIONS NONE' },
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
			analyzers: {},
			tokens: {},
			functions: {},
			models: {},
			params: {},
			scopes: {},
			tables: { test: 'DEFINE TABLE test SCHEMALESS PERMISSIONS NONE' },
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
			analyzers: {},
			tokens: {},
			functions: {},
			models: {},
			params: {},
			scopes: {},
			tables: { test: 'DEFINE TABLE test SCHEMAFULL PERMISSIONS NONE' },
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
			analyzers: {},
			tokens: {},
			functions: {},
			models: {},
			params: {},
			scopes: {},
			tables: { test: 'DEFINE TABLE test SCHEMAFULL PERMISSIONS NONE' },
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
			analyzers: {},
			tokens: {},
			functions: {},
			models: {},
			params: {},
			scopes: {},
			tables: {
				test: 'DEFINE TABLE test SCHEMAFULL PERMISSIONS NONE',
				view: 'DEFINE TABLE view SCHEMALESS AS SELECT count() FROM test GROUP ALL PERMISSIONS NONE',
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
			tables: { view: 'DEFINE TABLE view SCHEMALESS AS SELECT count() FROM test GROUP ALL PERMISSIONS NONE' },
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
			analyzers: {},
			tokens: {},
			functions: {},
			models: {},
			params: {},
			scopes: {},
			tables: {
				test: 'DEFINE TABLE test SCHEMAFULL PERMISSIONS NONE',
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
			events: { test: 'DEFINE EVENT test ON user WHEN true THEN (CREATE activity SET user = $this, value = $after.email, action = $event)' },
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
			events: { test: "DEFINE EVENT test ON user WHEN $event = 'CREATE' THEN (CREATE activity SET user = $this, value = $after.email, action = $event)" },
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
	assert_eq!(tmp, val);
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
			events: { test: 'DEFINE EVENT test ON user WHEN $before.email != $after.email THEN (CREATE activity SET user = $this, value = $after.email, action = $event)' },
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
		assert_eq!(Idiom(part.clone()), i);
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
		vec!["{ databases: { DB: 'DEFINE DATABASE DB' }, tokens: {  }, users: {  } }"],
		vec!["{ databases: {  }, tokens: {  }, users: {  } }"],
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
        vec!["{ analyzers: {  }, functions: { greet: \"DEFINE FUNCTION fn::greet() { RETURN 'Hello'; } PERMISSIONS FULL\" }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"],
		vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"]
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
        vec!["{ analyzers: { analyzer: 'DEFINE ANALYZER analyzer TOKENIZERS BLANK' }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"],
		vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"]
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
async fn permissions_checks_define_token_ns() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE TOKEN token ON NS TYPE HS512 VALUE 'secret'"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ databases: {  }, tokens: { token: \"DEFINE TOKEN token ON NAMESPACE TYPE HS512 VALUE 'secret'\" }, users: {  } }"],
		vec!["{ databases: {  }, tokens: {  }, users: {  } }"]
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
async fn permissions_checks_define_token_db() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE TOKEN token ON DB TYPE HS512 VALUE 'secret'"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: { token: \"DEFINE TOKEN token ON DATABASE TYPE HS512 VALUE 'secret'\" }, users: {  } }"],
		vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"]
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
		("test", "DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ namespaces: {  }, users: { user: \"DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER\" } }"],
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
		("test", "DEFINE USER user ON NS PASSHASH 'secret' ROLES VIEWER"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ databases: {  }, tokens: {  }, users: { user: \"DEFINE USER user ON NAMESPACE PASSHASH 'secret' ROLES VIEWER\" } }"],
		vec!["{ databases: {  }, tokens: {  }, users: {  } }"]
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
		("test", "DEFINE USER user ON DB PASSHASH 'secret' ROLES VIEWER"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: { user: \"DEFINE USER user ON DATABASE PASSHASH 'secret' ROLES VIEWER\" } }"],
		vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"]
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
async fn permissions_checks_define_scope() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE SCOPE account SESSION 1h;"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: { account: 'DEFINE SCOPE account SESSION 1h' }, tables: {  }, tokens: {  }, users: {  } }"],
		vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"]
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
async fn permissions_checks_define_param() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE PARAM $param VALUE 'foo'"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: { param: \"DEFINE PARAM $param VALUE 'foo' PERMISSIONS FULL\" }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"],
		vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"]
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
        vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: { TB: 'DEFINE TABLE TB SCHEMALESS PERMISSIONS NONE' }, tokens: {  }, users: {  } }"],
		vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"]
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
			analyzers: {},
			functions: {},
			models: {},
			params: {},
			scopes: {},
			tables: {
					default: 'DEFINE TABLE default SCHEMALESS PERMISSIONS NONE',
					full: 'DEFINE TABLE full SCHEMALESS PERMISSIONS FULL',
					select_full: 'DEFINE TABLE select_full SCHEMALESS PERMISSIONS FOR select FULL, FOR create, update, delete NONE'
			},
			tokens: {},
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
async fn redefining_existing_scope_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE SCOPE example;
		DEFINE SCOPE example;
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
async fn redefining_existing_token_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE TOKEN example ON SCOPE example TYPE HS512 VALUE \"example\";
		DEFINE TOKEN example ON SCOPE example TYPE HS512 VALUE \"example\";
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
async fn redefining_existing_user_should_not_error() -> Result<(), Error> {
	let sql = "
		DEFINE USER example ON ROOT PASSWORD \"example\" ROLES OWNER;
		DEFINE USER example ON ROOT PASSWORD \"example\" ROLES OWNER;
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
