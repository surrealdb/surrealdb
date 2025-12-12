// SDK2 integration tests

use sdk2::Surreal;
use surrealdb_core::embedded::EmbeddedSurrealEngine;
use surrealdb_types::{RecordId, SurrealValue};

#[tokio::test]
async fn test_example() {
    #[derive(Debug, SurrealValue)]
    struct User {
        id: RecordId,
        name: String,
    }

	let surreal = Surreal::new().attach_engine::<EmbeddedSurrealEngine>();
    surreal.connect("memory://").await.unwrap();
    surreal.r#use().namespace("test").database("test").await.unwrap();
    surreal.query("CREATE user:1 SET name = 'John Doe'").await.unwrap();
    let user = surreal.query("SELECT * FROM user:1").await.unwrap();
    let user: User = user.first().unwrap().clone().into_t().unwrap();
    assert_eq!(user.name, "John Doe");
    assert_eq!(user.id, RecordId::new("user", 1));
}

