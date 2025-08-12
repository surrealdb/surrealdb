mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;

/*
#[tokio::test]
async fn strict_mode_no_namespace() -> Result<()> {
	let sql = "
		-- DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		DEFINE TABLE test;
		DEFINE FIELD extra ON test VALUE true;
		CREATE test:tester;
		SELECT * FROM test;
	";
	let dbs = new_ds().await?.with_strict_mode(true);
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::NsNotFound {
			name: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::NsNotFound {
			name: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::NsNotFound {
			name: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::NsNotFound {
			name: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::NsNotFound {
			name: _
		})
	));
	//
	Ok(())
}

#[tokio::test]
async fn strict_mode_no_database() -> Result<()> {
	let sql = "
		DEFINE NAMESPACE test;
		-- DEFINE DATABASE test;
		DEFINE TABLE test;
		DEFINE FIELD extra ON test VALUE true;
		CREATE test:tester;
		SELECT * FROM test;
	";
	let dbs = new_ds().await?.with_strict_mode(true);
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::DbNotFound {
			name: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::DbNotFound {
			name: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::DbNotFound {
			name: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::DbNotFound {
			name: _
		})
	));
	//
	Ok(())
}

#[tokio::test]
async fn strict_mode_no_table() -> Result<()> {
	let sql = "
		DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		-- DEFINE TABLE test;
		DEFINE FIELD extra ON test VALUE true;
		CREATE test:tester;
		SELECT * FROM test;
	";
	let dbs = new_ds().await?.with_strict_mode(true);
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::TbNotFound {
			name: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::TbNotFound {
			name: _
		})
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(Error::TbNotFound {
			name: _
		})
	));
	//
	Ok(())
}
*/

#[tokio::test]
async fn strict_mode_all_ok() -> Result<()> {
	let sql = "
		DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		DEFINE TABLE test;
		DEFINE FIELD extra ON test VALUE true;
		CREATE test:tester;
		SELECT * FROM test;
	";
	let dbs = new_ds().await?.with_strict_mode(true);
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ id: test:tester, extra: true }]").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ id: test:tester, extra: true }]").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn loose_mode_all_ok() -> Result<()> {
	let sql = "
		DEFINE FIELD extra ON test VALUE true;
		CREATE test:tester;
		SELECT * FROM test;
		INFO FOR ROOT;
		INFO FOR NS;
		INFO FOR DB;
		INFO FOR TABLE test;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ id: test:tester, extra: true }]").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ id: test:tester, extra: true }]").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			accesses: { },
			namespaces: { test: 'DEFINE NAMESPACE test' },
			nodes: { },
			system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 },
			users: { },
		}"
	).unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			accesses: {},
			databases: { test: 'DEFINE DATABASE test' },
			users: {},
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			accesses: {},
			analyzers: {},
			apis: {},
			buckets: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			sequences: {},
			tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMALESS PERMISSIONS NONE' },
			users: {},
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			events: {},
			fields: { extra: 'DEFINE FIELD extra ON test VALUE true PERMISSIONS FULL' },
			tables: {},
			indexes: {},
			lives: {},
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn strict_define_in_transaction() -> Result<()> {
	let sql = r"
		DEFINE NS test; DEFINE DB test;
		USE NS test DB test;
		BEGIN;
		DEFINE TABLE test;
		DEFINE FIELD test ON test; -- Panic used to be caused when you add this query within the transaction
		COMMIT;
	";
	let dbs = new_ds().await?.with_strict_mode(true);
	let ses = Session::owner().with_ns("test").with_db("test");
	dbs.execute(sql, &ses, None).await?;
	Ok(())
}
