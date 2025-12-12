// Tests for QueryStream and streaming query results

use futures::StreamExt;
use sdk2::utils::{QueryFrame, QueryStream};
use sdk2::Surreal;
use surrealdb_core::embedded::EmbeddedSurrealEngine;
use surrealdb_types::{RecordId, SurrealValue};

async fn setup() -> Surreal {
    let surreal = Surreal::new().attach_engine::<EmbeddedSurrealEngine>();
    surreal.connect("memory://").await.unwrap();
    surreal.use_ns("test").use_db("test").await.unwrap();
    surreal
}

#[tokio::test]
async fn test_query_stream_basic() {
    let db = setup().await;

    // Create some test data
    db.query("CREATE user:1 SET name = 'Alice'").await.unwrap();
    db.query("CREATE user:2 SET name = 'Bob'").await.unwrap();

    // Stream results
    let mut stream = db.query("SELECT * FROM user ORDER BY name").stream().await.unwrap();

    let mut values = Vec::new();
    let mut done_count = 0;

    while let Some(frame) = stream.next().await {
        match frame {
            QueryFrame::Value { value, .. } => values.push(value),
            QueryFrame::Done { .. } => done_count += 1,
            QueryFrame::Error { error, .. } => panic!("Unexpected error: {error}"),
        }
    }

    assert_eq!(values.len(), 2);
    assert_eq!(done_count, 1);
}

#[tokio::test]
async fn test_query_stream_multiple_statements() {
    let db = setup().await;

    db.query("CREATE user:1 SET name = 'Alice'").await.unwrap();
    db.query("CREATE post:1 SET title = 'Hello'").await.unwrap();

    // Execute multiple statements
    let mut stream = db
        .query("SELECT * FROM user; SELECT * FROM post")
        .stream()
        .await
        .unwrap();

    let mut query0_values = 0;
    let mut query1_values = 0;
    let mut done_queries = Vec::new();

    while let Some(frame) = stream.next().await {
        match frame {
            QueryFrame::Value { query, .. } => {
                if query == 0 {
                    query0_values += 1;
                } else {
                    query1_values += 1;
                }
            }
            QueryFrame::Done { query, .. } => done_queries.push(query),
            QueryFrame::Error { error, .. } => panic!("Unexpected error: {error}"),
        }
    }

    assert_eq!(query0_values, 1, "Should have 1 user");
    assert_eq!(query1_values, 1, "Should have 1 post");
    assert_eq!(done_queries, vec![0, 1], "Both queries should complete");
}

#[tokio::test]
async fn test_query_stream_index_typed() {
    #[derive(Debug, Clone, PartialEq, SurrealValue)]
    struct User {
        id: RecordId,
        name: String,
    }

    let db = setup().await;

    db.query("CREATE user:1 SET name = 'Alice'").await.unwrap();
    db.query("CREATE user:2 SET name = 'Bob'").await.unwrap();
    db.query("CREATE post:1 SET title = 'Hello'").await.unwrap();

    // Execute multiple statements but only get users with type conversion
    let stream = db
        .query("SELECT * FROM user ORDER BY name; SELECT * FROM post")
        .stream()
        .await
        .unwrap();

    // Filter to only query index 0 (users) with type conversion
    let mut user_stream = stream.index::<User>(0);

    let mut users = Vec::new();
    while let Some(frame) = user_stream.next().await {
        if let Some(user) = frame.into_value() {
            users.push(user);
        }
    }

    assert_eq!(users.len(), 2);
    assert_eq!(users[0].name, "Alice");
    assert_eq!(users[1].name, "Bob");
}

#[tokio::test]
async fn test_query_stream_handles_errors() {
    let db = setup().await;

    // Query with an error (invalid syntax in second statement)
    let mut stream = db
        .query("RETURN 1; THROW 'test error'; RETURN 3")
        .stream()
        .await
        .unwrap();

    let mut has_error = false;
    let mut values = Vec::new();

    while let Some(frame) = stream.next().await {
        match frame {
            QueryFrame::Value { value, .. } => values.push(value),
            QueryFrame::Error { .. } => has_error = true,
            QueryFrame::Done { .. } => {}
        }
    }

    assert!(has_error, "Should have received an error frame");
}

#[tokio::test]
async fn test_query_stream_empty_result() {
    let db = setup().await;

    let mut stream = db.query("SELECT * FROM nonexistent").stream().await.unwrap();

    let mut value_count = 0;
    let mut done_count = 0;

    while let Some(frame) = stream.next().await {
        match frame {
            QueryFrame::Value { .. } => value_count += 1,
            QueryFrame::Done { .. } => done_count += 1,
            QueryFrame::Error { error, .. } => panic!("Unexpected error: {error}"),
        }
    }

    assert_eq!(value_count, 0, "Should have no values");
    assert_eq!(done_count, 1, "Should still have a Done frame");
}

