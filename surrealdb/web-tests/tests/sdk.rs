//! The SDK's embedded engine in the browser.

#![cfg(all(target_family = "wasm", target_vendor = "unknown", target_os = "unknown"))]
#![allow(clippy::unwrap_used)]

use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
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
