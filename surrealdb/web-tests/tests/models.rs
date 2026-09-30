//! Machine learning models, whose files the browser keeps in memory.

#![cfg(all(target_family = "wasm", target_vendor = "unknown", target_os = "unknown"))]
#![recursion_limit = "256"]
#![allow(clippy::unwrap_used)]

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn stores_model_files() {
	let ds = Datastore::new("memory").await.unwrap();
	let sess = Session::owner().with_ns("test").with_db("test");
	ds.put_ml_model(&sess, "model", "1.0.0", "", vec![1]).await.unwrap();
}
