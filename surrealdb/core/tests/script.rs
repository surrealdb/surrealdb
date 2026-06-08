#![recursion_limit = "256"]
#![cfg(feature = "scripting")]

use std::time::{Duration, Instant};

mod helpers;
use anyhow::Result;
use helpers::new_ds;
use surrealdb_core::cnf::ConfigMap;
use surrealdb_core::dbs::{Capabilities, Session};
use surrealdb_core::kvs::Datastore;

#[tokio::test]
async fn script_function_module_os() -> Result<()> {
	let sql = "
		CREATE platform:test SET version = function() {
			const { platform } = await import('os');
			return platform();
		};
	";
	let (_, dbs) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	Ok(())
}

#[tokio::test]
async fn script_run_too_long() -> Result<()> {
	let sql = r#"
		RETURN function() {
			for(let i = 0;i < 10000000;i++){
				for(let j = 0;j < 10000000;j++){
					for(let k = 0;k < 10000000;k++){
						if(globalThis.test){
							globalThis.test();
						}
					}
				}
			}
		}
	"#;

	let timeout = 500;
	let flex = 100;

	let config = ConfigMap::empty().with_key_value("scripting_max_time_limit", timeout.to_string());

	let dbs = Datastore::builder()
		.with_config(config)
		.with_capabilities(Capabilities::all())
		.build_with_path("memory")
		.await?;
	let setup_sess = Session::owner().with_ns("test");
	dbs.execute("DEFINE NS test", &Session::owner(), None).await?;
	dbs.execute("DEFINE DB test", &setup_sess, None).await?;

	let ses = Session::owner().with_ns("test").with_db("test");

	let before = Instant::now();
	let time =
		tokio::time::timeout(Duration::from_millis(timeout), dbs.execute(sql, &ses, None)).await;

	if before.elapsed() > Duration::from_millis(timeout + flex) {
		panic!("Scripting function didn't timeout properly")
	}
	// This should timeout within surreal not from the above timeout.
	let mut resp = time.unwrap().unwrap();
	resp.pop().unwrap().result.unwrap_err();

	Ok(())
}

#[tokio::test]
async fn script_limit_massive_parallel() -> Result<()> {
	let sql = r#"
		define function fn::crashcat() {
			return function() {
				let x = surrealdb.query("return fn::crashcat()");
				let y = surrealdb.query("return fn::crashcat()");
				return await x+y;
			};
		};
		return fn::crashcat();
	"#;
	let (_, dbs) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	dbs.execute(sql, &ses, None).await?;
	Ok(())
}
