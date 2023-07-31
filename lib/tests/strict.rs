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
	let dbs = Datastore::new("memory").await?.with_strict_mode(true);
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::NsNotFound {
			value: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::NsNotFound {
			value: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::NsNotFound {
			value: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::NsNotFound {
			value: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::NsNotFound {
			value: _
		})
	));
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
	let dbs = Datastore::new("memory").await?.with_strict_mode(true);
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::DbNotFound {
			value: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::DbNotFound {
			value: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::DbNotFound {
			value: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::DbNotFound {
			value: _
		})
	));
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
	let dbs = Datastore::new("memory").await?.with_strict_mode(true);
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
	assert!(matches!(
		tmp.err(),
		Some(Error::TbNotFound {
			value: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::TbNotFound {
			value: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::TbNotFound {
			value: _
		})
	));
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
	let dbs = Datastore::new("memory").await?.with_strict_mode(true);
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		INFO FOR ROOT;
		INFO FOR NS;
		INFO FOR DB;
		INFO FOR TABLE test;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
			namespaces: { test: 'DEFINE NAMESPACE test' },
			users: {},
		}",
	);
	assert_eq!(tmp, val);
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			analyzers: {},
			logins: {},
			tokens: {},
			functions: {},
			params: {},
			scopes: {},
			tables: { test: 'DEFINE TABLE test SCHEMALESS PERMISSIONS NONE' },
			users: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: { extra: 'DEFINE FIELD extra ON test VALUE true' },
			tables: {},
			indexes: {},
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
