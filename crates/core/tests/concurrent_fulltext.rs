mod helpers;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use helpers::new_ds;
use surrealdb_core::dbs::Session;
use surrealdb_types::{Number, ToSql, Value};
use tokio::sync::Barrier;
use tokio::time::sleep;

use crate::helpers::Test;

/// Helper to extract count from a GROUP ALL query result
fn extract_count(res: Vec<surrealdb_core::dbs::QueryResult>) -> Result<i64> {
	let val = res
		.into_iter()
		.next()
		.ok_or_else(|| anyhow::anyhow!("Expected at least one result"))?
		.result?;
	let first = val.first().ok_or_else(|| anyhow::anyhow!("Expected non-empty result"))?;
	let count_val = first.get("count");
	match count_val {
		Value::Number(Number::Int(n)) => Ok(*n),
		Value::Number(Number::Float(n)) => Ok(*n as i64),
		_ => anyhow::bail!("Expected number, got {:?}", count_val),
	}
}

/// Test concurrent inserts while a fulltext index is being built.
/// This verifies that the appending queue works correctly when documents
/// are inserted during index building.
#[tokio::test]
async fn concurrent_insert_during_index_building() -> Result<()> {
	let ds = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	// First, create a table with many documents (without index)
	// Using 5000 documents to properly stress-test bulk indexing
	ds.execute("DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase", &ses, None)
		.await?;

	// Insert in batches to avoid huge SQL strings
	for batch in 0..50 {
		let mut sql = String::new();
		for i in 0..100 {
			let id = batch * 100 + i;
			sql.push_str(&format!(
				"CREATE article:{id} SET title = 'Document number {id} with searchable content';\n"
			));
		}
		ds.execute(&sql, &ses, None).await?;
	}

	// Now spawn tasks: one to create the index, others to insert new documents
	let ds = Arc::new(ds);
	let barrier = Arc::new(Barrier::new(5));

	// Task 1: Create the fulltext index (this triggers bulk indexing of 5000 docs)
	let ds1 = ds.clone();
	let barrier1 = barrier.clone();
	let index_task = tokio::spawn(async move {
		let ses = Session::owner().with_ns("test").with_db("test");
		barrier1.wait().await;
		ds1.execute(
			"DEFINE INDEX article_title ON article FIELDS title FULLTEXT ANALYZER simple BM25",
			&ses,
			None,
		)
		.await
	});

	// Tasks 2-5: Insert new documents during indexing (4 parallel inserters)
	let mut insert_handles = Vec::new();
	for task_id in 0..4 {
		let ds = ds.clone();
		let barrier = barrier.clone();
		insert_handles.push(tokio::spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");
			barrier.wait().await;
			// Small delay to let indexing start
			sleep(Duration::from_millis(5 + task_id as u64 * 2)).await;
			let mut results = Vec::new();
			let start_id = 5000 + task_id * 250;
			for i in 0..250 {
				let id = start_id + i;
				let sql = format!(
					"CREATE article:{id} SET title = 'New document {id} inserted during indexing task {task_id}'"
				);
				results.push(ds.execute(&sql, &ses, None).await);
			}
			results
		}));
	}

	// Wait for all tasks
	let index_result = index_task.await;
	index_result??;

	for handle in insert_handles {
		for r in handle.await? {
			r?;
		}
	}

	// Verify: total documents should be 5000 + 4*250 = 6000
	let res = ds.execute("SELECT count() FROM article GROUP ALL", &ses, None).await?;
	let count = extract_count(res)?;
	assert_eq!(count, 6000, "Expected 6000 total documents, got {count}");

	// Verify: all documents should be searchable
	let res = ds
		.execute("SELECT count() FROM article WHERE title @@ 'document' GROUP ALL", &ses, None)
		.await?;
	let count = extract_count(res)?;
	assert_eq!(count, 6000, "Expected 6000 searchable documents, got {count}");

	// Verify: search for documents inserted during indexing
	let res = ds
		.execute("SELECT count() FROM article WHERE title @@ 'inserted' GROUP ALL", &ses, None)
		.await?;
	let count = extract_count(res)?;
	assert_eq!(count, 1000, "Expected 1000 'inserted' documents, got {count}");

	// Verify: original documents (first 5000) contain 'number'
	let res = ds
		.execute("SELECT count() FROM article WHERE title @@ 'number' GROUP ALL", &ses, None)
		.await?;
	let count = extract_count(res)?;
	assert_eq!(count, 5000, "Expected 5000 'number' documents, got {count}");

	Ok(())
}

/// Test parallel search queries during and after index building.
#[tokio::test]
async fn parallel_search_during_index_build() -> Result<()> {
	let ds = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	// Create documents
	let mut sql = String::new();
	sql.push_str("DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;\n");
	for i in 0..200 {
		sql.push_str(&format!(
			"CREATE post:{i} SET body = 'Post number {i} about rust programming language and databases';\n"
		));
	}
	ds.execute(&sql, &ses, None).await?;

	let ds = Arc::new(ds);

	// Create index
	ds.execute(
		"DEFINE INDEX post_body ON post FIELDS body FULLTEXT ANALYZER simple BM25",
		&ses,
		None,
	)
	.await?;

	// Run multiple parallel search queries
	let barrier = Arc::new(Barrier::new(5));
	let mut handles = Vec::new();

	for term in ["rust", "programming", "databases", "post", "number"] {
		let ds = ds.clone();
		let barrier = barrier.clone();
		let term = term.to_string();
		handles.push(tokio::spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");
			barrier.wait().await;
			let sql = format!("SELECT count() FROM post WHERE body @@ '{term}' GROUP ALL");
			ds.execute(&sql, &ses, None).await
		}));
	}

	// All searches should return 200 documents
	for handle in handles {
		let res = handle.await??;
		let count = extract_count(res)?;
		assert_eq!(count, 200, "Expected 200 documents for each term");
	}

	Ok(())
}

/// Test batch doc_id allocation with multiple concurrent indexing operations.
#[tokio::test]
async fn batch_doc_id_allocation() -> Result<()> {
	let ds = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	// Create multiple tables with fulltext indexes
	ds.execute("DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase", &ses, None)
		.await?;

	let ds = Arc::new(ds);

	// Create documents in parallel across multiple tables
	let mut handles = Vec::new();
	for table_idx in 0..3 {
		let ds = ds.clone();
		handles.push(tokio::spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");
			let mut sql = String::new();
			for i in 0..100 {
				sql.push_str(&format!(
					"CREATE table{table_idx}:{i} SET content = 'Content for table {table_idx} document {i}';\n"
				));
			}
			ds.execute(&sql, &ses, None).await
		}));
	}

	for handle in handles {
		handle.await??;
	}

	// Create indexes on all tables in parallel
	let mut handles = Vec::new();
	for table_idx in 0..3 {
		let ds = ds.clone();
		handles.push(tokio::spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");
			let sql = format!(
				"DEFINE INDEX idx{table_idx} ON table{table_idx} FIELDS content FULLTEXT ANALYZER simple BM25"
			);
			ds.execute(&sql, &ses, None).await
		}));
	}

	for handle in handles {
		handle.await??;
	}

	// Verify all tables are searchable
	for table_idx in 0..3 {
		let sql =
			format!("SELECT count() FROM table{table_idx} WHERE content @@ 'content' GROUP ALL");
		let res = ds.execute(&sql, &ses, None).await?;
		let count = extract_count(res)?;
		assert_eq!(count, 100, "Expected 100 documents in table{table_idx}, got {count}");
	}

	Ok(())
}

/// Test that highlights work correctly after batch indexing.
#[tokio::test]
async fn batch_indexing_highlights() -> Result<()> {
	let sql = r#"
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		CREATE doc:1 SET text = 'Hello World from SurrealDB';
		CREATE doc:2 SET text = 'World of databases is amazing';
		CREATE doc:3 SET text = 'Hello from the other side';
		DEFINE INDEX doc_text ON doc FIELDS text FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
		SELECT id, search::highlight('<b>', '</b>', 1) AS text FROM doc WHERE text @1@ 'Hello' ORDER BY id;
	"#;

	let mut t = Test::new(sql).await?;
	t.expect_size(6)?;
	t.skip_ok(5)?;

	let val = t.next_value()?;
	let arr = val.as_array().expect("Expected array result");
	assert_eq!(arr.len(), 2, "Expected 2 results with 'Hello'");

	// Check highlights are present
	let text1 = arr[0].get("text").to_sql();
	let text2 = arr[1].get("text").to_sql();
	assert!(text1.contains("<b>Hello</b>"), "Expected highlight in first result: {text1}");
	assert!(text2.contains("<b>Hello</b>"), "Expected highlight in second result: {text2}");

	Ok(())
}

/// Stress test: 5000 documents with bulk indexing followed by parallel searches.
/// Tests that documents created in bulk are correctly indexed and searchable.
#[tokio::test]
async fn stress_concurrent_fulltext_operations() -> Result<()> {
	let ds = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase", &ses, None)
		.await?;

	// Create 5000 documents (1000 per keyword)
	let keywords = ["alpha", "beta", "gamma", "delta", "epsilon"];
	for (writer_id, keyword) in keywords.iter().enumerate() {
		// Insert in batches of 100 to avoid huge SQL strings
		for batch in 0..10 {
			let mut sql = String::new();
			for i in 0..100 {
				let id = writer_id * 1000 + batch * 100 + i;
				sql.push_str(&format!(
					"CREATE content:{id} SET body = 'Writer document {id} with {keyword} marker';\n"
				));
			}
			ds.execute(&sql, &ses, None).await?;
		}
	}

	// Now create the index (triggers bulk indexing of 5000 docs)
	ds.execute(
		"DEFINE INDEX content_idx ON content FIELDS body FULLTEXT ANALYZER simple BM25",
		&ses,
		None,
	)
	.await?;

	let ds = Arc::new(ds);

	// Run parallel searches to stress-test the index
	let barrier = Arc::new(Barrier::new(5));
	let mut handles = Vec::new();

	for keyword in keywords {
		let ds = ds.clone();
		let barrier = barrier.clone();
		let keyword = keyword.to_string();
		handles.push(tokio::spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");
			barrier.wait().await;
			for _ in 0..10 {
				let sql =
					format!("SELECT count() FROM content WHERE body @@ '{keyword}' GROUP ALL");
				let res = ds.execute(&sql, &ses, None).await?;
				let count = extract_count(res)?;
				assert_eq!(count, 1000, "Expected 1000 for {keyword}, got {count}");
			}
			Ok::<_, anyhow::Error>(())
		}));
	}

	// Wait for all search operations
	for handle in handles {
		handle.await??;
	}

	// Final verification
	let res = ds.execute("SELECT count() FROM content GROUP ALL", &ses, None).await?;
	let count = extract_count(res)?;
	assert_eq!(count, 5000, "Expected 5000 total documents, got {count}");

	let res = ds
		.execute("SELECT count() FROM content WHERE body @@ 'writer' GROUP ALL", &ses, None)
		.await?;
	let count = extract_count(res)?;
	assert_eq!(count, 5000, "Expected 5000 documents for 'writer', got {count}");

	Ok(())
}

/// Test that updates AFTER index building correctly update the fulltext index.
/// This is a simpler test to verify the basic update path works.
#[tokio::test]
async fn updates_after_index_building() -> Result<()> {
	let ds = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	// Setup
	ds.execute("DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase", &ses, None)
		.await?;

	// Create initial documents
	ds.execute("CREATE doc:1 SET text = 'Original text for document one'", &ses, None).await?;
	ds.execute("CREATE doc:2 SET text = 'Original text for document two'", &ses, None).await?;
	ds.execute("CREATE doc:3 SET text = 'Original text for document three'", &ses, None).await?;

	// Create fulltext index
	ds.execute(
		"DEFINE INDEX doc_text ON doc FIELDS text FULLTEXT ANALYZER simple BM25",
		&ses,
		None,
	)
	.await?;

	// Verify initial state - all 3 should have "Original"
	let res = ds
		.execute("SELECT count() FROM doc WHERE text @@ 'Original' GROUP ALL", &ses, None)
		.await?;
	let count = extract_count(res)?;
	assert_eq!(count, 3, "Expected 3 'Original' items before update, got {count}");

	// Update one document
	ds.execute("UPDATE doc:1 SET text = 'Modified content for document one'", &ses, None).await?;

	// Verify "Original" now only matches 2
	let res = ds
		.execute("SELECT count() FROM doc WHERE text @@ 'Original' GROUP ALL", &ses, None)
		.await?;
	let count = extract_count(res)?;
	assert_eq!(count, 2, "Expected 2 'Original' items after update, got {count}");

	// Verify "Modified" now matches 1
	let res = ds
		.execute("SELECT count() FROM doc WHERE text @@ 'Modified' GROUP ALL", &ses, None)
		.await?;
	let count = extract_count(res)?;
	assert_eq!(count, 1, "Expected 1 'Modified' item after update, got {count}");

	Ok(())
}
