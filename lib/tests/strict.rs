mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn strict_mode_no_namespace() -> Result<(), Error> {
	let sql = "
		-- DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		DEFINE TABLE test;
		DEFINE FIELD extra ON test VALUE true;
		CREATE test:tester;
		SELECT * FROM test;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, true).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::NsNotFound)));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::NsNotFound)));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::NsNotFound)));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::NsNotFound)));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::NsNotFound)));
	//
	Ok(())
}

#[tokio::test]
async fn strict_mode_no_database() -> Result<(), Error> {
	let sql = "
		DEFINE NAMESPACE test;
		-- DEFINE DATABASE test;
		DEFINE TABLE test;
		DEFINE FIELD extra ON test VALUE true;
		CREATE test:tester;
		SELECT * FROM test;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, true).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::DbNotFound)));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::DbNotFound)));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::DbNotFound)));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::DbNotFound)));
	//
	Ok(())
}

#[tokio::test]
async fn strict_mode_no_table() -> Result<(), Error> {
	let sql = "
		DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		-- DEFINE TABLE test;
		DEFINE FIELD extra ON test VALUE true;
		CREATE test:tester;
		SELECT * FROM test;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, true).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::TbNotFound)));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::TbNotFound)));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(tmp.err(), Some(Error::TbNotFound)));
	//
	Ok(())
}

#[tokio::test]
async fn strict_mode_all_ok() -> Result<(), Error> {
	let sql = "
		DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		DEFINE TABLE test;
		DEFINE FIELD extra ON test VALUE true;
		CREATE test:tester;
		SELECT * FROM test;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, true).await?;
	assert_eq!(res.len(), 6);
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
	let val = Value::parse("[{ id: test:tester, extra: true }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: test:tester, extra: true }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn loose_mode_all_ok() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD extra ON test VALUE true;
		CREATE test:tester;
		SELECT * FROM test;
		INFO FOR KV;
		INFO FOR NS;
		INFO FOR DB;
		INFO FOR TABLE test;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: test:tester, extra: true }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: test:tester, extra: true }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			ns: { test: 'DEFINE NAMESPACE test' },
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			db: { test: 'DEFINE DATABASE test' },
			nl: {},
			nt: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			dl: {},
			dt: {},
			sc: {},
			tb: { test: 'DEFINE TABLE test SCHEMALESS PERMISSIONS NONE' },
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			ev: {},
			fd: { extra: 'DEFINE FIELD extra ON test VALUE true' },
			ft: {},
			ix: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
