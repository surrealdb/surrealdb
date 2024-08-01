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
			CHANGEFEED UNSET
			COMMENT UNSET
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
			tables: { test: 'DEFINE TABLE test TYPE NORMAL DROP SCHEMALESS COMMENT \\'test\\' CHANGEFEED 1d PERMISSIONS FOR select, update, delete NONE, FOR create FULL' },
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

#[tokio::test]
async fn define_alter_field() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD test ON test;
		INFO FOR TB test;

		ALTER FIELD test ON test
		    FLEXIBLE
			TYPE string
			DEFAULT 'test'
			READONLY true
			VALUE 'bla'
			ASSERT string::len($value) > 0
			PERMISSIONS NONE
			COMMENT 'bla';
		INFO FOR TB test;

		ALTER FIELD test ON test
		    FLEXIBLE false
			TYPE UNSET
			DEFAULT UNSET
			READONLY false
			VALUE UNSET
			ASSERT UNSET
			PERMISSIONS FULL
			COMMENT UNSET;
		INFO FOR TB test;
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
            events: {},
            fields: {
                test: 'DEFINE FIELD test ON test PERMISSIONS FULL'
            },
            indexes: {},
            lives: {},
            tables: {}
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
            fields: {
                test: \"DEFINE FIELD test ON test FLEXIBLE TYPE string DEFAULT 'test' READONLY VALUE 'bla' ASSERT string::len($value) > 0 COMMENT 'bla' PERMISSIONS NONE\"
            },
            indexes: {},
            lives: {},
            tables: {}
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
            fields: {
                test: 'DEFINE FIELD test ON test PERMISSIONS FULL'
            },
            indexes: {},
            lives: {},
            tables: {}
        }",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_alter_field_if_exists() -> Result<(), Error> {
	let sql = "
		ALTER FIELD test ON test COMMENT 'bla';
		ALTER FIELD IF EXISTS test ON test COMMENT 'bla';
		INFO FOR TB test;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	let _err = Error::FdNotFound {
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
		    events: {},
            fields: {},
            indexes: {},
            lives: {},
            tables: {}
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_alter_param() -> Result<(), Error> {
	let sql = "
		DEFINE PARAM $test VALUE 123;
		INFO FOR DB;

		ALTER PARAM $test
		    VALUE 456
			PERMISSIONS NONE
			COMMENT 'bla';
		INFO FOR DB;

		ALTER PARAM $test
		    VALUE 123
			PERMISSIONS FULL
			COMMENT UNSET;
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
            params: {
                test: 'DEFINE PARAM $test VALUE 123 PERMISSIONS FULL'
            },
            tables: {},
            users: {}
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
            params: {
                test: \"DEFINE PARAM $test VALUE 456 COMMENT 'bla' PERMISSIONS NONE\"
            },
            tables: {},
            users: {}
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
            params: {
                test: 'DEFINE PARAM $test VALUE 123 PERMISSIONS FULL'
            },
            tables: {},
            users: {}
        }",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn define_alter_param_if_exists() -> Result<(), Error> {
	let sql = "
		ALTER PARAM $test COMMENT 'bla';
		ALTER PARAM IF EXISTS $test COMMENT 'bla';
		INFO FOR DB;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	let _err = Error::PaNotFound {
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
            users: {}
        }",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
