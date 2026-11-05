//! Runs the shared `surrealdb-kvs-test` suite against the SurrealKV engine
//! mounted on the browser's origin private file system. The OPFS only exists
//! in a dedicated worker, so the runner is configured accordingly.
#![cfg(target_family = "wasm")]

use std::sync::atomic::{AtomicUsize, Ordering};

use common::future::assert_send;
use surrealdb_kvs_surrealkv::{Datastore, SurrealKvConfig};
use surrealdb_kvs_test::{TestBackend, TestDs};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[wasm_bindgen_test]
async fn kvs_suite() {
	// Every test gets its own uniquely-named database: OPFS state persists per
	// browser profile, and an open database holds a lock on its directory.
	static COUNTER: AtomicUsize = AtomicUsize::new(0);
	let backend = TestBackend::new("surrealkv", || {
		// The OPFS futures are not `Send`; the factory future must be. Sound
		// because wasm without `atomics` is single-threaded.
		assert_send(async {
			let name = format!("kvs-suite-{}", COUNTER.fetch_add(1, Ordering::Relaxed));
			let ds = Datastore::new(&name, SurrealKvConfig::default())
				.await
				.expect("failed to open the test database");
			TestDs::from_builder(Box::new(ds))
		})
	});
	let ran = surrealdb_kvs_test::run_all(&[backend], |line| console_log!("{line}")).await;
	assert!(ran > 0, "inventory registered no tests under the wasm runner");
}
