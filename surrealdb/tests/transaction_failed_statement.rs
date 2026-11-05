#![allow(clippy::unwrap_used)]
//! A client-owned transaction whose statement fails part-way through its
//! writes refuses to commit, so none of its writes become visible.
#![cfg(feature = "kv-mem")]

use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
use surrealdb::types::{RecordId, SurrealValue};

#[derive(Debug, Clone, SurrealValue)]
struct User {
	id: RecordId,
	name: String,
}

impl User {
	fn new(name: &str) -> Self {
		Self {
			id: RecordId::new("user", name),
			name: name.to_owned(),
		}
	}
}

#[tokio::test]
async fn commit_is_refused_after_an_insert_with_a_duplicate_id_failed() {
	let db = Surreal::new::<Mem>(()).await.unwrap();
	db.use_ns("main").use_db("main").await.unwrap();
	db.query("DEFINE TABLE user").await.unwrap().check().unwrap();

	let tx = db.clone().begin().await.unwrap();
	let failed = tx
		.insert::<Vec<User>>("user")
		.content(vec![
			User::new("test_4"),
			User::new("test_5"),
			User::new("test_4"),
			User::new("test_6"),
		])
		.await;
	assert!(failed.is_err(), "the duplicate id fails the statement");
	// A later statement in the same transaction still runs.
	tx.insert::<Vec<User>>("user")
		.content(vec![User::new("test_7"), User::new("test_8")])
		.await
		.unwrap();

	assert!(tx.commit().await.is_err(), "a transaction with a part-way failure must not commit");

	let users: Vec<User> = db.select("user").await.unwrap();
	assert!(users.is_empty(), "nothing the transaction wrote is kept, found {users:?}");
}
