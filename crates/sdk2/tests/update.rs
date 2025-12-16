// Tests for Update method

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
async fn test_update_by_id() {
	let db = setup().await;

	// Create a record first
	db.query("CREATE user:alice SET name = 'Alice', age = 25")
		.await
		.unwrap();

	// Update the record (don't include id in content when updating specific record)
	#[derive(Debug, Clone, PartialEq, SurrealValue)]
	struct UserUpdate {
		name: String,
		age: i64,
	}
	
	let updated = db
		.update("user:alice")
		.content(UserUpdate {
			name: "Alice Updated".to_string(),
			age: 26,
		})
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(updated.len(), 1);
	let updated = &updated[0];
	assert_eq!(updated.name, "Alice Updated");
	assert_eq!(updated.age, 26);
}

#[tokio::test]
async fn test_update_with_cond() {
	let db = setup().await;

	// Create multiple records
	db.query("CREATE user:alice SET name = 'Alice', age = 25")
		.await
		.unwrap();
	db.query("CREATE user:bob SET name = 'Bob', age = 30")
		.await
		.unwrap();

	// Update records where age > 25
	#[derive(Debug, Clone, PartialEq, SurrealValue)]
	struct UserUpdate {
		name: String,
		age: i64,
	}
	
	let updated = db
		.update("user")
		.cond("age > 25")
		.content(UserUpdate {
			name: "Bob Updated".to_string(),
			age: 31,
		})
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(updated.len(), 1);
	assert_eq!(updated[0].name, "Bob Updated");
	assert_eq!(updated[0].age, 31);
}

#[tokio::test]
async fn test_update_with_where() {
	let db = setup().await;

	// Create a record
	db.query("CREATE user:alice SET name = 'Alice', age = 25")
		.await
		.unwrap();

	// Update using where builder
	#[derive(Debug, Clone, PartialEq, SurrealValue)]
	struct UserUpdate {
		name: String,
		age: i64,
	}
	
	let updated = db
		.update("user")
		.r#where(|w| w.field("age").eq(25))
		.content(UserUpdate {
			name: "Alice Changed".to_string(),
			age: 26,
		})
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(updated.len(), 1);
	assert_eq!(updated[0].name, "Alice Changed");
}

#[tokio::test]
async fn test_update_table_all() {
	let db = setup().await;

	// Create a record
	db.query("CREATE user:alice SET name = 'Alice', age = 25")
		.await
		.unwrap();

	// Update all records in table
	#[derive(Debug, Clone, PartialEq, SurrealValue)]
	struct UserUpdate {
		name: String,
		age: i64,
	}
	
	let updated = db
		.update("user")
		.content(UserUpdate {
			name: "Updated".to_string(),
			age: 100,
		})
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(updated.len(), 1);
	assert_eq!(updated[0].age, 100);
}
