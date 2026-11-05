#![allow(clippy::unwrap_used)]
//! The first request on a freshly created embedded engine finds its session,
//! however the engine's registration of it is scheduled against the request.
#![cfg(feature = "kv-mem")]

use surrealdb::Surreal;
use surrealdb::engine::local::Mem;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_request_after_connecting_finds_its_session() {
	for round in 0..200 {
		let db = Surreal::new::<Mem>(()).await.unwrap();
		db.use_ns("ns").use_db("main").await.unwrap_or_else(|e| panic!("round {round}: {e}"));
		db.query("DEFINE TABLE t SCHEMALESS").await.unwrap().check().unwrap();
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_fresh_engines_all_find_their_sessions() {
	let tasks: Vec<_> = (0..32)
		.map(|n| {
			tokio::spawn(async move {
				let db = Surreal::new::<Mem>(()).await.unwrap();
				db.use_ns("ns").use_db("main").await.unwrap_or_else(|e| panic!("task {n}: {e}"));
				db.query("RETURN 1").await.unwrap().check().unwrap();
			})
		})
		.collect();
	for task in tasks {
		task.await.unwrap();
	}
}
