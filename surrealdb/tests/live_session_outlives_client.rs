#![allow(clippy::unwrap_used)]
//! A live stream keeps the session it was registered on open, so it still
//! receives notifications after the client that created it is dropped.
use std::time::Duration;

use futures::StreamExt;
use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
use surrealdb::opt::Resource;
use tokio::time::timeout;

#[tokio::test]
async fn live_stream_outlives_the_client_that_created_it() {
	let db = Surreal::new::<Mem>(()).await.unwrap();
	db.use_ns("main").use_db("main").await.unwrap();
	db.query("DEFINE TABLE t;").await.unwrap();
	let id = Resource::from(("t", 1));
	let mut live = db.clone().select(id.clone()).live().await.unwrap();
	db.create(id).await.unwrap();
	let next = timeout(Duration::from_secs(5), live.next()).await.expect("a notification arrives");
	assert!(next.is_some(), "the stream delivers the change");
}

#[tokio::test]
async fn typed_query_stream_outlives_its_response_and_client() {
	use surrealdb::types::Value;
	let db = Surreal::new::<Mem>(()).await.unwrap();
	db.use_ns("main").use_db("main").await.unwrap();
	db.query("DEFINE TABLE t;").await.unwrap();
	let mut response = db.clone().query("LIVE SELECT * FROM t").await.unwrap();
	let mut live = response.stream::<surrealdb::Notification<Value>>(0).unwrap();
	drop(response);
	db.query("CREATE t:1").await.unwrap();
	let next = timeout(Duration::from_secs(5), live.next()).await.expect("a notification arrives");
	assert!(next.is_some(), "the typed stream delivers the change");
}
