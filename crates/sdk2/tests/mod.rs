// SDK2 integration tests

mod select;

use sdk2::Surreal;
use surrealdb_core::embedded::EmbeddedSurrealEngine;
use surrealdb_types::{RecordId, SurrealValue, Value};

#[tokio::test]
async fn test_example() {
    #[derive(Debug, SurrealValue)]
    struct User {
        id: RecordId,
        name: String,
    }

    let surreal = Surreal::new().attach_engine::<EmbeddedSurrealEngine>();
    surreal.connect("memory://").await.unwrap();
    surreal.use_ns("test").use_db("test").await.unwrap();
    surreal.query("CREATE user:1 SET name = 'John Doe'").await.unwrap();
    
    let results = surreal.query("SELECT * FROM user:1").await.unwrap();
    let value: Value = results.into_iter().next().unwrap().take().unwrap();
    let user: User = value.into_t().unwrap();
    
    assert_eq!(user.name, "John Doe");
    assert_eq!(user.id, RecordId::new("user", 1));
}
