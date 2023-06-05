mod parse;

use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn define_statement_namespace() -> Result<(), Error> {
	let sql = "
		DEFINE NAMESPACE test;
		INFO FOR KV;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			databases: { test: 'DEFINE DATABASE test' },
			logins: {},
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			analyzers: {},
			logins: {},
			tokens: {},
			functions: { test: 'DEFINE FUNCTION fn::test($first: string, $last: string) { RETURN $first + $last; }' },
			params: {},
			scopes: {},
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			analyzers: {},
			logins: {},
			tokens: {},
			functions: {},
			params: {},
			scopes: {},
			tables: { test: 'DEFINE TABLE test DROP SCHEMALESS' },
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			analyzers: {},
			logins: {},
			tokens: {},
			functions: {},
			params: {},
			scopes: {},
			tables: { test: 'DEFINE TABLE test SCHEMALESS' },
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
			logins: {},
			tokens: {},
			functions: {},
			params: {},
			scopes: {},
			tables: { test: 'DEFINE TABLE test SCHEMAFULL' },
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			analyzers: {},
			logins: {},
			tokens: {},
			functions: {},
			params: {},
			scopes: {},
			tables: { test: 'DEFINE TABLE test SCHEMAFULL' },
			users: {},
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
			fields: { test: 'DEFINE FIELD test ON user' },
			tables: {},
			indexes: {},
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
			fields: { test: 'DEFINE FIELD test ON user TYPE string' },
			tables: {},
			indexes: {},
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
			fields: { test: "DEFINE FIELD test ON user VALUE $value OR 'GBR'" },
			tables: {},
			indexes: {},
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
			fields: { test: 'DEFINE FIELD test ON user ASSERT $value != NONE AND $value = /[A-Z]{3}/' },
			tables: {},
			indexes: {},
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
			fields: { test: "DEFINE FIELD test ON user TYPE string VALUE $value OR 'GBR' ASSERT $value != NONE AND $value = /[A-Z]{3}/" },
			tables: {},
			indexes: {},
		}"#,
	);
	assert_eq!(tmp, val);
	//
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
		Some(e) if e.to_string() == r#"Database index `test` already contains 'test@surrealdb.com', with record `user:2`"#
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
		Some(e) if e.to_string() == r#"Database index `test` already contains ['apple', 'test@surrealdb.com'], with record `user:3`"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database index `test` already contains ['tesla', 'test@surrealdb.com'], with record `user:4`"#
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
		Some(e) if e.to_string() == r#"Database index `test` already contains ['tesla', 'test@surrealdb.com'], with record `user:4`"#
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
		Some(e) if e.to_string() == r#"Database index `test` already contains 'test@surrealdb.com', with record `user:3`"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database index `test` already contains 'test@surrealdb.com', with record `user:3`"#
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: {},
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
		Some(e) if e.to_string() == r#"Database index `test` already contains ['apple', 'test@surrealdb.com'], with record `user:3`"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Database index `test` already contains ['apple', 'test@surrealdb.com'], with record `user:3`"#
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_statement_analyzer() -> Result<(), Error> {
	let sql = "
		DEFINE ANALYZER english TOKENIZERS space,case FILTERS lowercase,snowball(english);
		DEFINE ANALYZER autocomplete FILTERS lowercase,edgengram(2,10);
		INFO FOR DB;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
			analyzers: {
				autocomplete: 'DEFINE ANALYZER autocomplete FILTERS LOWERCASE,EDGENGRAM(2,10)',
				english: 'DEFINE ANALYZER english TOKENIZERS SPACE,CASE FILTERS LOWERCASE,SNOWBALL(ENGLISH)',
			},
			logins: {},
			tokens: {},
			functions: {},
			params: {},
			scopes: {},
			tables: {},
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn define_statement_search_index() -> Result<(), Error> {
	let sql = r#"
		CREATE blog:1 SET title = 'Understanding SurrealQL and how it is different from PostgreSQL';
		CREATE blog:3 SET title = 'This blog is going to be deleted';
		DEFINE ANALYZER english TOKENIZERS space,case FILTERS lowercase,snowball(english);
		DEFINE INDEX blog_title ON blog FIELDS title SEARCH english BM25(1.2,0.75,100) HIGHLIGHTS;
		CREATE blog:2 SET title = 'Behind the scenes of the exciting beta 9 release';
		DELETE blog:3;
		INFO FOR TABLE blog;
	"#;

	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 7);
	//
	for _ in 0..6 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
	}

	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			tables: {},
			indexes: { blog_title: 'DEFINE INDEX blog_title ON blog FIELDS title SEARCH english BM25(1.2,0.75,100) HIGHLIGHTS' },
		}",
	);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn define_statement_user_kv() -> Result<(), Error> {
	let sql = "
		DEFINE USER test ON KV PASSWORD 'test';

		INFO FOR KV;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;

	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;

	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let define_str = tmp.pick(&["users".into(), "test".into()]).to_string();

	assert!(define_str
		.strip_prefix("\"")
		.unwrap()
		.starts_with("DEFINE USER test ON KV PASSHASH '$argon2id$"));
	Ok(())
}

#[tokio::test]
async fn define_statement_user_ns() -> Result<(), Error> {
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();

	// Create a NS user and retrieve it.
	let sql = "
		USE NS ns;
		DEFINE USER test ON NS PASSWORD 'test';
		
		INFO FOR USER test;
		INFO FOR USER test ON NS;
		INFO FOR USER test ON NAMESPACE;
		INFO FOR USER test ON KV;
	";
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;

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
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;

	assert!(res.remove(0).result.is_err());

	Ok(())
}

#[tokio::test]
async fn define_statement_user_db() -> Result<(), Error> {
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();

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
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;

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
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;

	assert!(res.remove(0).result.is_err());

	Ok(())
}

#[tokio::test]
async fn define_statement_user_check_permissions_kv() -> Result<(), Error> {
	//
	// Check permissions using a KV session
	//

	let sql = [
		// Create users
		"DEFINE USER test_kv ON KV PASSWORD 'test';
		
		USE NS ns;
		DEFINE USER test_ns ON NS PASSWORD 'test';
		
		USE NS ns;
		USE DB db;
		DEFINE USER test_db ON DB PASSWORD 'test';",
		// Query users
		"INFO FOR USER test_kv ON KV;
			
		USE NS ns;
		INFO FOR USER test_ns;
		INFO FOR USER test_kv ON KV;
		
		USE NS ns;
		USE DB db;
		INFO FOR USER test_db;
		INFO FOR USER test_ns ON NS;
		INFO FOR USER test_kv ON KV;",
	];
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();

	// Create users
	let res = &mut dbs.execute(&sql[0], &ses, None, false).await?;
	assert!(res[0].result.is_ok());
	assert!(res[1].result.is_ok());
	assert!(res[2].result.is_ok());
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert!(res[5].result.is_ok());

	// Query users
	let res = &mut dbs.execute(&sql[1], &ses, None, false).await?;

	assert!(res[0]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_kv ON KV PASSHASH '$argon2id$"));
	assert!(res[2]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_ns ON NAMESPACE PASSHASH '$argon2id$"));
	assert!(res[3]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_kv ON KV PASSHASH '$argon2id$"));
	assert!(res[6]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_db ON DATABASE PASSHASH '$argon2id$"));
	assert!(res[7]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_ns ON NAMESPACE PASSHASH '$argon2id$"));
	assert!(res[8]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_kv ON KV PASSHASH '$argon2id$"));

	Ok(())
}

#[tokio::test]
async fn define_statement_user_check_permissions_ns() -> Result<(), Error> {
	//
	// Check permissions using a NS session
	//
	let sql = [
		// Create users
		"DEFINE USER test_kv ON KV PASSWORD 'test';
		
		USE NS ns;
		DEFINE USER test_ns ON NS PASSWORD 'test';
		
		USE NS ns;
		USE DB db;
		DEFINE USER test_db ON DB PASSWORD 'test';",
		// Query users
		"INFO FOR USER test_kv ON KV;
		INFO FOR USER test_ns;
		INFO FOR USER test_ns ON NS;
		
		USE DB db;
		INFO FOR USER test_db;
		INFO FOR USER test_ns ON NS;
		INFO FOR USER test_kv ON KV;",
	];

	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_ns("ns");

	// Test create users with the NS sessions
	let res = &mut dbs.execute(&sql[0], &ses, None, false).await?;
	assert_eq!(
		res[0].result.as_ref().unwrap_err().to_string(),
		"You don't have permission to perform this query type"
	); // NS users can't create KV users
	assert!(res[1].result.is_ok());
	assert_eq!(
		res[2].result.as_ref().unwrap_err().to_string(),
		"You don't have permission to perform this query type"
	); // NS users can't create NS users
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert!(res[5].result.is_ok());

	// Prepare datastore
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();
	let _ = &mut dbs.execute(&sql[0], &ses, None, false).await?;

	// Test query users with the NS session
	let ses = Session::for_ns("ns");
	let res = &mut dbs.execute(&sql[1], &ses, None, false).await?;

	assert_eq!(
		res[0].result.as_ref().unwrap_err().to_string(),
		"You don't have permission to perform this query type"
	); // NS users can't query KV users
	assert!(res[1]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_ns ON NAMESPACE PASSHASH '$argon2id$"));
	assert!(res[2]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_ns ON NAMESPACE PASSHASH '$argon2id$"));
	assert!(res[4]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_db ON DATABASE PASSHASH '$argon2id$"));
	assert!(res[5]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_ns ON NAMESPACE PASSHASH '$argon2id$"));
	assert_eq!(
		res[6].result.as_ref().unwrap_err().to_string(),
		"You don't have permission to perform this query type"
	); // NS users can't query KV users

	Ok(())
}

#[tokio::test]
async fn define_statement_user_check_permissions_db() -> Result<(), Error> {
	//
	// Check permissions using a DB session
	//
	let sql = [
		// Create users
		"DEFINE USER test_kv ON KV PASSWORD 'test';
		
		USE NS ns;
		DEFINE USER test_ns ON NS PASSWORD 'test';
		
		USE NS ns;
		USE DB db;
		DEFINE USER test_db ON DB PASSWORD 'test';",
		// Query users
		"INFO FOR USER test_kv ON KV;
		INFO FOR USER test_ns ON NS;
		INFO FOR USER test_db;
		INFO FOR USER test_db ON DB;",
	];

	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_db("ns", "db");

	// Test create users with the NS sessions
	let res = &mut dbs.execute(&sql[0], &ses, None, false).await?;
	assert_eq!(
		res[0].result.as_ref().unwrap_err().to_string(),
		"You don't have permission to perform this query type"
	); // DB users can't create KV users
	assert!(res[1].result.is_ok());
	assert_eq!(
		res[2].result.as_ref().unwrap_err().to_string(),
		"You don't have permission to perform this query type"
	); // DB users can't create NS users
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert_eq!(
		res[5].result.as_ref().unwrap_err().to_string(),
		"You don't have permission to perform this query type"
	); // DB users can't create DB users

	// Prepare datastore
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();
	let _ = &mut dbs.execute(&sql[0], &ses, None, false).await?;

	// Test query users with the NS session
	let ses = Session::for_db("ns", "db");
	let res = &mut dbs.execute(&sql[1], &ses, None, false).await?;

	assert_eq!(
		res[0].result.as_ref().unwrap_err().to_string(),
		"You don't have permission to perform this query type"
	); // DB users can't query KV users
	assert_eq!(
		res[1].result.as_ref().unwrap_err().to_string(),
		"You don't have permission to perform this query type"
	); // DB users can't query NS users
	assert!(res[2]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_db ON DATABASE PASSHASH '$argon2id$"));
	assert!(res[3]
		.result
		.as_ref()
		.unwrap()
		.to_string()
		.starts_with("\"DEFINE USER test_db ON DATABASE PASSHASH '$argon2id$"));

	Ok(())
}
