//! The SDK's embedded engine in the browser.

#![cfg(all(target_family = "wasm", target_vendor = "unknown", target_os = "unknown"))]
#![allow(clippy::unwrap_used)]

use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::{Capabilities, ExperimentalFeature};
use surrealdb_web_tests::DIR;
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

#[wasm_bindgen_test]
async fn file_buckets_use_allowed_folders() {
	let capabilities =
		Capabilities::new().with_experimental_feature_allowed(ExperimentalFeature::Files);
	let config =
		Config::new().capabilities(capabilities).bucket_folder_allowlist([format!("/{DIR}")]);
	let db = Surreal::new::<Mem>(config).await.unwrap();
	db.use_ns("test").use_db("test").await.unwrap();
	let sql = format!(
		"DEFINE BUCKET b BACKEND 'file:///{DIR}/' + <string> rand::uuid();
		file::put(f\"b:/a.txt\", 'x');
		file::get(f\"b:/a.txt\").to_string();"
	);
	let mut res = db.query(sql).await.unwrap().check().unwrap();
	assert_eq!(res.take::<Option<String>>(2).unwrap().as_deref(), Some("x"));
}
