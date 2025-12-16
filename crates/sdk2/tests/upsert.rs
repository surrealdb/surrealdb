// Tests for Upsert method

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
async fn test_upsert_create() {
	let db = setup().await;

	// Upsert a non-existent record (should create)
	let users = db
		.upsert("user:alice")
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
async fn test_upsert_update() {
	let db = setup().await;

	// Create a record first
	db.query("CREATE user:alice SET name = 'Alice', age = 25")
		.await
		.unwrap();

	// Upsert the existing record (should update)
	let users = db
		.upsert("user:alice")
		.content(User {
			id: RecordId::new("user", "alice"),
			name: "Alice Updated".to_string(),
			age: 26,
		})
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(users.len(), 1);
	let user = &users[0];
	assert_eq!(user.name, "Alice Updated");
	assert_eq!(user.age, 26);
}

#[tokio::test]
async fn test_upsert_with_cond() {
	let db = setup().await;

	// Create a record
	db.query("CREATE user:alice SET name = 'Alice', age = 25")
		.await
		.unwrap();

	// Upsert with condition
	#[derive(Debug, Clone, PartialEq, SurrealValue)]
	struct UserUpdate {
		name: String,
		age: i64,
	}
	
	let user = db
		.upsert("user")
		.cond("age = 25")
		.content(UserUpdate {
			name: "Alice Upserted".to_string(),
			age: 27,
		})
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(user.len(), 1);
	assert_eq!(user[0].name, "Alice Upserted");
}

#[tokio::test]
async fn test_upsert_with_where() {
	let db = setup().await;

	// Upsert using where builder
	#[derive(Debug, Clone, PartialEq, SurrealValue)]
	struct UserUpdate {
		name: String,
		age: i64,
	}
	
	let user = db
		.upsert("user")
		.r#where(|w| w.field("name").eq("Bob"))
		.content(UserUpdate {
			name: "Bob".to_string(),
			age: 30,
		})
		.collect::<Vec<User>>()
		.await
		.unwrap();

	// Should create since no matching record exists
	assert_eq!(user.len(), 1);
	assert_eq!(user[0].name, "Bob");
}
