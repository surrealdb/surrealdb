//! The SDK's embedded engine in the browser.

#![cfg(all(target_family = "wasm", target_vendor = "unknown", target_os = "unknown"))]
#![allow(clippy::unwrap_used)]

use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn timers_work() {
	let db = Surreal::new::<Mem>(()).await.unwrap();
	db.use_ns("test").use_db("test").await.unwrap();
	let mut res =
		db.query("SLEEP 10ms; SELECT * FROM 1 TIMEOUT 1s;").await.unwrap().check().unwrap();
	assert_eq!(res.take::<Vec<i64>>(1).unwrap(), [1]);
}

#[wasm_bindgen_test]
async fn capabilities_apply() {
	let capabilities = Capabilities::all().with_function_denied("string::len").unwrap();
	let db = Surreal::new::<Mem>(Config::new().capabilities(capabilities)).await.unwrap();
	db.use_ns("test").use_db("test").await.unwrap();
	let err = db.query("string::len('abc')").await.unwrap().take::<Option<i64>>(0).unwrap_err();
	assert!(err.to_string().contains("not allowed"), "{err}");
}
