// Tests for Delete method

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
async fn test_delete_by_id() {
	let db = setup().await;

	// Create a record first
	db.query("CREATE user:alice SET name = 'Alice', age = 25")
		.await
		.unwrap();

	// Delete the record - DELETE returns the deleted records
	let deleted = db
		.delete("user:alice")
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(deleted.len(), 1);
	assert_eq!(deleted[0].name, "Alice");

	// Verify it's deleted
	let users = db.select("user:alice").collect::<Vec<User>>().await.unwrap();
	assert_eq!(users.len(), 0);
}

#[tokio::test]
async fn test_delete_with_cond() {
	let db = setup().await;

	// Create multiple records
	db.query("CREATE user:alice SET name = 'Alice', age = 25")
		.await
		.unwrap();
	db.query("CREATE user:bob SET name = 'Bob', age = 30")
		.await
		.unwrap();
	db.query("CREATE user:charlie SET name = 'Charlie', age = 20")
		.await
		.unwrap();

	// Delete records where age > 25
	let deleted = db
		.delete("user")
		.cond("age > 25")
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(deleted.len(), 1);
	assert_eq!(deleted[0].name, "Bob");

	// Verify remaining records
	let users = db.select("user").collect::<Vec<User>>().await.unwrap();
	assert_eq!(users.len(), 2);
}

#[tokio::test]
async fn test_delete_with_where() {
	let db = setup().await;

	// Create multiple records
	db.query("CREATE user:alice SET name = 'Alice', age = 25")
		.await
		.unwrap();
	db.query("CREATE user:bob SET name = 'Bob', age = 30")
		.await
		.unwrap();

	// Delete records using where builder
	let deleted = db
		.delete("user")
		.r#where(|w| w.field("age").gt(25))
		.collect::<Vec<User>>()
		.await
		.unwrap();

	assert_eq!(deleted.len(), 1);
	assert_eq!(deleted[0].name, "Bob");
}

#[tokio::test]
async fn test_delete_table_all() {
	let db = setup().await;

	// Create multiple records
	db.query("CREATE user:alice SET name = 'Alice', age = 25")
		.await
		.unwrap();
	db.query("CREATE user:bob SET name = 'Bob', age = 30")
		.await
		.unwrap();

	// Delete all records in table
	let deleted = db.delete("user").collect::<Vec<User>>().await.unwrap();

	assert_eq!(deleted.len(), 2);

	// Verify table is empty
	let users = db.select("user").collect::<Vec<User>>().await.unwrap();
	assert_eq!(users.len(), 0);
}
