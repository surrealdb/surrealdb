#![cfg(feature = "scripting")]

use std::time::{Duration, Instant};

mod helpers;
use anyhow::Result;
use helpers::new_ds;
use surrealdb_core::dbs::Session;

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
	let (_, dbs) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut timeout = *surrealdb_core::cnf::SCRIPTING_MAX_TIME_LIMIT;
	timeout += timeout / 2;

	let before = Instant::now();
	let time =
		tokio::time::timeout(Duration::from_millis(timeout as u64), dbs.execute(sql, &ses, None))
			.await;
	if before.elapsed() > Duration::from_millis(timeout as u64) {
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
