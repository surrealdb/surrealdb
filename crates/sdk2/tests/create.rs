// Tests for Create method

use sdk2::Surreal;
use surrealdb_core::embedded::EmbeddedSurrealEngine;
use surrealdb_types::{RecordId, SurrealValue};

async fn setup() -> Surreal {
	let surreal = Surreal::new().attach_engine::<EmbeddedSurrealEngine>();
	surreal.connect("memory://").await.unwrap();
	surreal.use_ns("test").use_db("test").await.unwrap();
	surreal
}

#[derive(Debug, Clone, PartialEq, SurrealValue)]
struct User {
	id: RecordId,
	name: String,
	age: i64,
}

#[derive(Debug, Clone, PartialEq, SurrealValue)]
struct OptionalUser {
	id: RecordId,
	name: String,
	#[surreal(default)]
	age: Option<i64>,
}

#[tokio::test]
async fn test_create_with_content() {
	let db = setup().await;

	let users = db
		.create("user:alice")
		.content(User {
			id: RecordId::new("user", "alice"),
			name: "Alice".to_string(),
			age: 25,
		})
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(users.len(), 1);
	let user = &users[0];
	assert_eq!(user.name, "Alice");
	assert_eq!(user.age, 25);
}

#[tokio::test]
async fn test_create_without_content() {
	let db = setup().await;

	// Create a record without content (empty record)
	let users = db
		.create("user:bob")
		.collect::<Vec<OptionalUser>>()
		.await
		.unwrap();

	assert_eq!(users.len(), 1);
	let user = &users[0];
	// The record exists - verify it was created
	assert_eq!(user.id.table.to_string(), "user");
	match &user.id.key {
		surrealdb_types::RecordIdKey::String(s) => assert_eq!(s, "bob"),
		_ => panic!("Expected string key"),
	}
}

#[tokio::test]
async fn test_create_table_with_content() {
	let db = setup().await;

	// Create a record in a table (random ID) - returns array
	let users = db
		.create("user")
		.content(User {
			id: RecordId::new("user", "temp"), // This will be replaced
			name: "Charlie".to_string(),
			age: 30,
		})
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(users.len(), 1);
	let user = &users[0];
	assert_eq!(user.name, "Charlie");
	assert_eq!(user.age, 30);
	// ID should be auto-generated
	assert_eq!(user.id.table.to_string(), "user");
}

#[tokio::test]
async fn test_create_with_timeout() {
	let db = setup().await;

	let users = db
		.create("user:dave")
		.content(User {
			id: RecordId::new("user", "dave"),
			name: "Dave".to_string(),
			age: 35,
		})
		.timeout(std::time::Duration::from_secs(5))
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(users.len(), 1);
	assert_eq!(users[0].name, "Dave");
}
