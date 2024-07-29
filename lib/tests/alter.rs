mod parse;
use parse::Parse;

mod helpers;
use helpers::*;

use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn define_alter_table() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE test;
		INFO FOR DB;

		ALTER TABLE test
		    DROP
		    SCHEMALESS
			PERMISSIONS FOR create FULL
			CHANGEFEED 1d
			COMMENT 'test'
			TYPE NORMAL;
		INFO FOR DB;

		ALTER TABLE test
		    DROP false
		    SCHEMAFULL
			PERMISSIONS NONE
			CHANGEFEED NONE
			COMMENT NONE
			TYPE ANY;
		INFO FOR DB;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
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
			tables: { test: 'DEFINE TABLE test TYPE NORMAL DROP SCHEMALESS COMMENT 'test' CHANGEFEED 1d PERMISSIONS FOR select, update, delete NONE, FOR create FULL' },
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
async fn define_alter_table_if_exists() -> Result<(), Error> {
	let sql = "
		ALTER TABLE test COMMENT 'bla';
		ALTER TABLE IF EXISTS test COMMENT 'bla';
		INFO FOR DB
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	let _err = Error::TbNotFound {
		value: "test".to_string(),
	};
	assert!(matches!(tmp, Err(_err)));
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
			tables: {},
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
