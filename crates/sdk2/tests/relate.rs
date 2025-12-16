// Tests for Relate method

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
struct Person {
	id: RecordId,
	name: String,
}

#[derive(Debug, Clone, PartialEq, SurrealValue)]
struct Company {
	id: RecordId,
	name: String,
}

#[derive(Debug, Clone, PartialEq, SurrealValue)]
struct Founded {
	id: RecordId,
	#[surreal(rename = "in")]
	from: RecordId,
	#[surreal(rename = "out")]
	to: RecordId,
	#[surreal(default)]
	since: Option<String>,
}

#[tokio::test]
async fn test_relate_basic() {
	let db = setup().await;

	// Create records first
	db.query("CREATE person:tobie SET name = 'Tobie'")
		.await
		.unwrap();
	db.query("CREATE company:surrealdb SET name = 'SurrealDB'")
		.await
		.unwrap();

	// Create a relation
	let relation = db
		.relate("person:tobie", "founded", "company:surrealdb")
		.collect::<Option<Founded>>()
		.await
		.unwrap();

	assert!(relation.is_some());
	let relation = relation.unwrap();
	match &relation.from.key {
		surrealdb_types::RecordIdKey::String(s) => assert_eq!(s, "tobie"),
		_ => panic!("Expected string key"),
	}
	match &relation.to.key {
		surrealdb_types::RecordIdKey::String(s) => assert_eq!(s, "surrealdb"),
		_ => panic!("Expected string key"),
	}
}

#[tokio::test]
async fn test_relate_with_content() {
	let db = setup().await;

	// Create records first
	db.query("CREATE person:jaime SET name = 'Jaime'")
		.await
		.unwrap();
	db.query("CREATE company:surrealdb SET name = 'SurrealDB'")
		.await
		.unwrap();

	// Create a relation with content
	let relation = db
		.relate("person:jaime", "founded", "company:surrealdb")
		.content(Founded {
			id: RecordId::new("founded", "temp"),
			from: RecordId::new("person", "jaime"),
			to: RecordId::new("company", "surrealdb"),
			since: Some("2021".to_string()),
		})
		.collect::<Option<Founded>>()
		.await
		.unwrap();

	assert!(relation.is_some());
	let relation = relation.unwrap();
	assert_eq!(relation.since, Some("2021".to_string()));
}

#[tokio::test]
async fn test_relate_table_to_table() {
	let db = setup().await;

	// Create records
	db.query("CREATE person:alice SET name = 'Alice'")
		.await
		.unwrap();
	db.query("CREATE person:bob SET name = 'Bob'")
		.await
		.unwrap();

	// Create a relation between tables
	let relation = db
		.relate("person:alice", "knows", "person:bob")
		.collect::<Option<Founded>>()
		.await
		.unwrap();

	assert!(relation.is_some());
}

#[tokio::test]
async fn test_relate_with_timeout() {
	let db = setup().await;

	// Create records
	db.query("CREATE person:charlie SET name = 'Charlie'")
		.await
		.unwrap();
	db.query("CREATE company:test SET name = 'Test'")
		.await
		.unwrap();

	// Create relation with timeout
	let relation = db
		.relate("person:charlie", "works_at", "company:test")
		.timeout(std::time::Duration::from_secs(5))
		.collect::<Option<Founded>>()
		.await
		.unwrap();

	assert!(relation.is_some());
}
