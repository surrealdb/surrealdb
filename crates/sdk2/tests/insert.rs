// Tests for Insert method

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

#[tokio::test]
async fn test_insert_single() {
	let db = setup().await;

	// Insert a single record
	let user = db
		.insert("user")
		.content(User {
			id: RecordId::new("user", "temp"), // Will be auto-generated
			name: "Alice".to_string(),
			age: 25,
		})
		.collect::<Option<User>>()
		.await
		.unwrap();

	assert!(user.is_some());
	let user = user.unwrap();
	assert_eq!(user.name, "Alice");
	assert_eq!(user.age, 25);
	assert_eq!(user.id.table.to_string(), "user");
}

#[tokio::test]
async fn test_insert_multiple() {
	let db = setup().await;

	// Insert multiple records
	let users = db
		.insert("user")
		.content(vec![
			User {
				id: RecordId::new("user", "temp"),
				name: "Alice".to_string(),
				age: 25,
			},
			User {
				id: RecordId::new("user", "temp"),
				name: "Bob".to_string(),
				age: 30,
			},
		])
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(users.len(), 2);
	assert_eq!(users[0].name, "Alice");
	assert_eq!(users[1].name, "Bob");
}

#[tokio::test]
async fn test_insert_with_specific_id() {
	let db = setup().await;

	// Insert with specific record ID
	let user = db
		.insert("user:alice")
		.content(User {
			id: RecordId::new("user", "alice"),
			name: "Alice".to_string(),
			age: 25,
		})
		.collect::<Option<User>>()
		.await
		.unwrap();

	assert!(user.is_some());
	let user = user.unwrap();
	match &user.id.key {
		surrealdb_types::RecordIdKey::String(s) => assert_eq!(s, "alice"),
		_ => panic!("Expected string key"),
	}
	assert_eq!(user.name, "Alice");
}

#[tokio::test]
async fn test_insert_empty() {
	let db = setup().await;

	// Insert without content (creates empty record)
	let user = db
		.insert("user:bob")
		.collect::<Option<User>>()
		.await
		.unwrap();

	assert!(user.is_some());
	// The record exists but with default/empty values
}
