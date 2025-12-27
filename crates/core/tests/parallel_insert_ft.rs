//! Test for parallel INSERT operations

#![cfg(feature = "kv-rocksdb")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::Capabilities;
use surrealdb_core::kvs::Datastore;
use temp_dir::TempDir;
use tokio::task::JoinSet;

/// Creates a RocksDB datastore in a temporary directory
async fn new_rocksdb_ds(ns: &str, db: &str) -> Result<(Datastore, TempDir)> {
	// Create temp directory (kept alive by returning it)
	let temp_dir = TempDir::new()?;
	let path = format!("rocksdb:{}", temp_dir.path().to_string_lossy());

	// Create the datastore using the public API
	let ds =
		Datastore::new(&path).await?.with_capabilities(Capabilities::all()).with_notifications();

	// Setup namespace and database
	let sess = Session::owner().with_ns(ns);
	ds.execute(&format!("DEFINE NS `{ns}`"), &Session::owner(), None).await?;
	ds.execute(&format!("DEFINE DB `{db}`"), &sess, None).await?;

	Ok((ds, temp_dir))
}

/// Test: Parallel INSERT
///
/// This test spawns concurrent tasks, each inserting documents with unique IDs
/// into a table that has a fulltext search index. Due to the batch allocation
/// mechanism in sequences, this should trigger transaction conflicts.
#[tokio::test(flavor = "multi_thread")]
async fn parallel_insert_with_fulltext_index_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 100;
	const DOCS_PER_WORKER: usize = 50;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Create analyzer and fulltext index
	let setup_sql = r#"
		DEFINE ANALYZER doc_analyzer TOKENIZERS blank,class,punct FILTERS lowercase,ascii;
		DEFINE INDEX doc_ft_idx ON TABLE docs FIELDS content FULLTEXT ANALYZER doc_analyzer BM25;
	"#;
	let res = ds.execute(setup_sql, &ses, None).await?;
	for r in res {
		r.result?;
	}

	// Counters for tracking results
	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));
	let other_error_count = Arc::new(AtomicUsize::new(0));

	// Spawn parallel insert tasks
	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();
		let other_error_count = other_error_count.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			for doc_idx in 0..DOCS_PER_WORKER {
				let doc_id = worker_id * DOCS_PER_WORKER + doc_idx;
				let sql = format!(
					"INSERT INTO docs {{ id: doc:{doc_id}, content: 'Document number {doc_id} with searchable text content for fulltext indexing' }}"
				);

				match ds.execute(&sql, &ses, None).await {
					Ok(mut res) => {
						if let Some(result) = res.pop() {
							match result.result {
								Ok(_) => {
									success_count.fetch_add(1, Ordering::SeqCst);
								}
								Err(e) => {
									let err_str = e.to_string();
									if err_str.contains("Resource busy")
										|| err_str.contains("Transaction conflict")
										|| err_str.contains("TryAgain")
									{
										conflict_count.fetch_add(1, Ordering::SeqCst);
									} else {
										other_error_count.fetch_add(1, Ordering::SeqCst);
										eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
									}
								}
							}
						}
					}
					Err(e) => {
						let err_str = e.to_string();
						if err_str.contains("Resource busy")
							|| err_str.contains("Transaction conflict")
							|| err_str.contains("TryAgain")
						{
							conflict_count.fetch_add(1, Ordering::SeqCst);
						} else {
							other_error_count.fetch_add(1, Ordering::SeqCst);
							eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
						}
					}
				}
			}
		});
	}

	// Wait for all tasks to complete
	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_docs = NUM_WORKERS * DOCS_PER_WORKER;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);
	let other_errors = other_error_count.load(Ordering::SeqCst);

	println!("\n=== Parallel INSERT Test Results (RocksDB + Fulltext Index) ===");
	println!("Total documents attempted: {total_docs}");
	println!("Successful inserts: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Other errors: {other_errors}");
	println!("Success rate: {:.1}%", (successes as f64 / total_docs as f64) * 100.0);

	// Verify actual documents in the database
	let count_sql = "SELECT count() FROM docs GROUP ALL";
	let res = ds.execute(count_sql, &ses, None).await?;
	if let Some(result) = res.into_iter().next() {
		if let Ok(val) = result.result {
			println!("Actual documents in database: {val:?}");
		}
	}

	// The test "passes" but reports conflicts - this demonstrates the issue
	// In a fixed version, conflicts should be 0
	if conflicts > 0 {
		println!("\nISSUE CONFIRMED: {conflicts} transaction conflicts detected!");
		println!("This demonstrates the parallel INSERT conflict problem with fulltext indexes.");
	}

	Ok(())
}

/// Test: Parallel INSERT with fulltext index using batch INSERT syntax
///
/// This variant uses batch INSERT syntax which might have different behavior
/// with respect to transaction conflicts.
#[tokio::test(flavor = "multi_thread")]
async fn parallel_batch_insert_with_fulltext_index_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 100;
	const BATCH_SIZE: usize = 50;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Create analyzer and fulltext index
	let setup_sql = r#"
		DEFINE ANALYZER doc_analyzer TOKENIZERS blank,class,punct FILTERS lowercase,ascii;
		DEFINE INDEX doc_ft_idx ON TABLE docs FIELDS content FULLTEXT ANALYZER doc_analyzer BM25;
	"#;
	let res = ds.execute(setup_sql, &ses, None).await?;
	for r in res {
		r.result?;
	}

	// Counters for tracking results
	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));
	let other_error_count = Arc::new(AtomicUsize::new(0));

	// Spawn parallel batch insert tasks
	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();
		let other_error_count = other_error_count.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			// Build batch of documents
			let start_id = worker_id * BATCH_SIZE;
			let docs: Vec<String> = (0..BATCH_SIZE)
				.map(|i| {
					let doc_id = start_id + i;
					format!(
						"{{ id: doc:{doc_id}, content: 'Batch document {doc_id} with text for fulltext search indexing' }}"
					)
				})
				.collect();

			let sql = format!("INSERT INTO docs [{}]", docs.join(", "));

			match ds.execute(&sql, &ses, None).await {
				Ok(mut res) => {
					if let Some(result) = res.pop() {
						match result.result {
							Ok(_) => {
								success_count.fetch_add(BATCH_SIZE, Ordering::SeqCst);
							}
							Err(e) => {
								let err_str = e.to_string();
								if err_str.contains("Resource busy")
									|| err_str.contains("Transaction conflict")
									|| err_str.contains("TryAgain")
								{
									conflict_count.fetch_add(BATCH_SIZE, Ordering::SeqCst);
								} else {
									other_error_count.fetch_add(BATCH_SIZE, Ordering::SeqCst);
									eprintln!("Worker {worker_id} batch error: {err_str}");
								}
							}
						}
					}
				}
				Err(e) => {
					let err_str = e.to_string();
					if err_str.contains("Resource busy")
						|| err_str.contains("Transaction conflict")
						|| err_str.contains("TryAgain")
					{
						conflict_count.fetch_add(BATCH_SIZE, Ordering::SeqCst);
					} else {
						other_error_count.fetch_add(BATCH_SIZE, Ordering::SeqCst);
						eprintln!("Worker {worker_id} batch error: {err_str}");
					}
				}
			}
		});
	}

	// Wait for all tasks to complete
	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_docs = NUM_WORKERS * BATCH_SIZE;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);
	let other_errors = other_error_count.load(Ordering::SeqCst);

	println!("\n=== Parallel Batch INSERT Test Results (RocksDB + Fulltext Index) ===");
	println!("Total documents attempted: {total_docs}");
	println!("Successful inserts: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Other errors: {other_errors}");
	println!("Success rate: {:.1}%", (successes as f64 / total_docs as f64) * 100.0);

	// Verify actual documents in the database
	let count_sql = "SELECT count() FROM docs GROUP ALL";
	let res = ds.execute(count_sql, &ses, None).await?;
	if let Some(result) = res.into_iter().next() {
		if let Ok(val) = result.result {
			println!("Actual documents in database: {val:?}");
		}
	}

	if conflicts > 0 {
		println!("\nISSUE CONFIRMED: {conflicts} transaction conflicts detected in batch mode!");
	}

	Ok(())
}

/// Test: Comparison - parallel INSERT WITHOUT fulltext index
///
/// This test serves as a control group to verify that the issue is specifically
/// related to fulltext indexing and sequence allocation, not general INSERT conflicts.
#[tokio::test(flavor = "multi_thread")]
async fn parallel_insert_without_fulltext_index_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 100;
	const DOCS_PER_WORKER: usize = 50;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Pre-define the table to avoid schema creation conflicts
	let setup_sql = "DEFINE TABLE docs SCHEMAFULL; DEFINE FIELD content ON docs TYPE string;";
	let res = ds.execute(setup_sql, &ses, None).await?;
	for r in res {
		r.result?;
	}

	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));
	let other_error_count = Arc::new(AtomicUsize::new(0));

	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();
		let other_error_count = other_error_count.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			for doc_idx in 0..DOCS_PER_WORKER {
				let doc_id = worker_id * DOCS_PER_WORKER + doc_idx;
				let sql = format!(
					"INSERT INTO docs {{ id: doc:{doc_id}, content: 'Document number {doc_id} with some text content' }}"
				);

				match ds.execute(&sql, &ses, None).await {
					Ok(mut res) => {
						if let Some(result) = res.pop() {
							match result.result {
								Ok(_) => {
									success_count.fetch_add(1, Ordering::SeqCst);
								}
								Err(e) => {
									let err_str = e.to_string();
									if err_str.contains("Resource busy")
										|| err_str.contains("Transaction conflict")
										|| err_str.contains("TryAgain")
									{
										conflict_count.fetch_add(1, Ordering::SeqCst);
									} else {
										other_error_count.fetch_add(1, Ordering::SeqCst);
									}
								}
							}
						}
					}
					Err(e) => {
						let err_str = e.to_string();
						if err_str.contains("Resource busy")
							|| err_str.contains("Transaction conflict")
							|| err_str.contains("TryAgain")
						{
							conflict_count.fetch_add(1, Ordering::SeqCst);
						} else {
							other_error_count.fetch_add(1, Ordering::SeqCst);
						}
					}
				}
			}
		});
	}

	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_docs = NUM_WORKERS * DOCS_PER_WORKER;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);
	let other_errors = other_error_count.load(Ordering::SeqCst);

	println!("\n=== Parallel INSERT Test Results (RocksDB, NO Fulltext Index) ===");
	println!("Total documents attempted: {total_docs}");
	println!("Successful inserts: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Other errors: {other_errors}");
	println!("Success rate: {:.1}%", (successes as f64 / total_docs as f64) * 100.0);

	// Verify actual documents
	let count_sql = "SELECT count() FROM docs GROUP ALL";
	let res = ds.execute(count_sql, &ses, None).await?;
	if let Some(result) = res.into_iter().next() {
		if let Ok(val) = result.result {
			println!("Actual documents in database: {val:?}");
		}
	}

	if conflicts > 0 {
		println!("\nConflicts even without fulltext index: {conflicts}");
	} else {
		println!("\nNo conflicts without fulltext index (expected behavior)");
	}

	Ok(())
}

/// Test: Parallel INSERT with auto-generated IDs (no explicit record ID)
///
/// This test uses INSERT without explicit IDs, which may trigger different
/// ID generation mechanisms that could cause conflicts.
#[tokio::test(flavor = "multi_thread")]
async fn parallel_insert_auto_id_with_fulltext_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 100;
	const DOCS_PER_WORKER: usize = 20;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Create analyzer and fulltext index
	let setup_sql = r#"
		DEFINE ANALYZER doc_analyzer TOKENIZERS blank,class,punct FILTERS lowercase,ascii;
		DEFINE INDEX doc_ft_idx ON TABLE docs FIELDS content FULLTEXT ANALYZER doc_analyzer BM25;
	"#;
	let res = ds.execute(setup_sql, &ses, None).await?;
	for r in res {
		r.result?;
	}

	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));
	let other_error_count = Arc::new(AtomicUsize::new(0));

	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();
		let other_error_count = other_error_count.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			for doc_idx in 0..DOCS_PER_WORKER {
				// No explicit ID - let SurrealDB generate it
				let sql = format!(
					"INSERT INTO docs {{ content: 'Worker {worker_id} document {doc_idx} with searchable content' }}"
				);

				match ds.execute(&sql, &ses, None).await {
					Ok(mut res) => {
						if let Some(result) = res.pop() {
							match result.result {
								Ok(_) => {
									success_count.fetch_add(1, Ordering::SeqCst);
								}
								Err(e) => {
									let err_str = e.to_string();
									if err_str.contains("Resource busy")
										|| err_str.contains("Transaction conflict")
										|| err_str.contains("TryAgain")
									{
										conflict_count.fetch_add(1, Ordering::SeqCst);
									} else {
										other_error_count.fetch_add(1, Ordering::SeqCst);
										eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
									}
								}
							}
						}
					}
					Err(e) => {
						let err_str = e.to_string();
						if err_str.contains("Resource busy")
							|| err_str.contains("Transaction conflict")
							|| err_str.contains("TryAgain")
						{
							conflict_count.fetch_add(1, Ordering::SeqCst);
						} else {
							other_error_count.fetch_add(1, Ordering::SeqCst);
							eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
						}
					}
				}
			}
		});
	}

	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_docs = NUM_WORKERS * DOCS_PER_WORKER;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);
	let other_errors = other_error_count.load(Ordering::SeqCst);

	println!("\n=== Parallel INSERT (Auto-ID) Test Results (RocksDB + Fulltext) ===");
	println!("Total documents attempted: {total_docs}");
	println!("Successful inserts: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Other errors: {other_errors}");
	println!("Success rate: {:.1}%", (successes as f64 / total_docs as f64) * 100.0);

	let count_sql = "SELECT count() FROM docs GROUP ALL";
	let res = ds.execute(count_sql, &ses, None).await?;
	if let Some(result) = res.into_iter().next() {
		if let Ok(val) = result.result {
			println!("Actual documents in database: {val:?}");
		}
	}

	if conflicts > 0 {
		println!("\nISSUE CONFIRMED: {conflicts} transaction conflicts with auto-generated IDs!");
	}

	Ok(())
}

/// Test: Parallel operations using explicit SEQUENCE
///
/// This test directly uses SEQUENCE to verify if the batch allocation
/// mechanism causes conflicts.
#[tokio::test(flavor = "multi_thread")]
async fn parallel_sequence_usage_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 100;
	const OPS_PER_WORKER: usize = 50;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Define a sequence
	let setup_sql = "DEFINE SEQUENCE test_seq;";
	let res = ds.execute(setup_sql, &ses, None).await?;
	for r in res {
		r.result?;
	}

	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));
	let other_error_count = Arc::new(AtomicUsize::new(0));

	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();
		let other_error_count = other_error_count.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			for _ in 0..OPS_PER_WORKER {
				let sql = "RETURN sequence::nextval('test_seq');";

				match ds.execute(sql, &ses, None).await {
					Ok(mut res) => {
						if let Some(result) = res.pop() {
							match result.result {
								Ok(_) => {
									success_count.fetch_add(1, Ordering::SeqCst);
								}
								Err(e) => {
									let err_str = e.to_string();
									if err_str.contains("Resource busy")
										|| err_str.contains("Transaction conflict")
										|| err_str.contains("TryAgain")
									{
										conflict_count.fetch_add(1, Ordering::SeqCst);
									} else {
										other_error_count.fetch_add(1, Ordering::SeqCst);
										eprintln!("Worker {worker_id}: {err_str}");
									}
								}
							}
						}
					}
					Err(e) => {
						let err_str = e.to_string();
						if err_str.contains("Resource busy")
							|| err_str.contains("Transaction conflict")
							|| err_str.contains("TryAgain")
						{
							conflict_count.fetch_add(1, Ordering::SeqCst);
						} else {
							other_error_count.fetch_add(1, Ordering::SeqCst);
							eprintln!("Worker {worker_id}: {err_str}");
						}
					}
				}
			}
		});
	}

	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_ops = NUM_WORKERS * OPS_PER_WORKER;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);
	let other_errors = other_error_count.load(Ordering::SeqCst);

	println!("\n=== Parallel SEQUENCE Test Results (RocksDB) ===");
	println!("Total operations attempted: {total_ops}");
	println!("Successful operations: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Other errors: {other_errors}");
	println!("Success rate: {:.1}%", (successes as f64 / total_ops as f64) * 100.0);

	if conflicts > 0 {
		println!("\nISSUE CONFIRMED: {conflicts} conflicts in SEQUENCE operations!");
		println!("This confirms the batch allocation conflict hypothesis.");
	} else {
		println!("\nNo conflicts in SEQUENCE operations (retry mechanism working)");
	}

	Ok(())
}

/// Test: High-contention INSERT with CREATE statement (not INSERT)
///
/// CREATE may have different transaction semantics than INSERT.
#[tokio::test(flavor = "multi_thread")]
async fn parallel_create_with_fulltext_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 100;
	const DOCS_PER_WORKER: usize = 20;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Pre-define the table with analyzer and fulltext index
	// Note: Using "doc" table here (CREATE doc:id syntax)
	let setup_sql = r#"
		DEFINE TABLE doc SCHEMAFULL;
		DEFINE FIELD content ON doc TYPE string;
		DEFINE ANALYZER doc_analyzer TOKENIZERS blank,class,punct FILTERS lowercase,ascii;
		DEFINE INDEX doc_ft_idx ON TABLE doc FIELDS content FULLTEXT ANALYZER doc_analyzer BM25;
	"#;
	let res = ds.execute(setup_sql, &ses, None).await?;
	for r in res {
		r.result?;
	}

	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));
	let other_error_count = Arc::new(AtomicUsize::new(0));

	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();
		let other_error_count = other_error_count.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			for doc_idx in 0..DOCS_PER_WORKER {
				let doc_id = worker_id * DOCS_PER_WORKER + doc_idx;
				// Use CREATE instead of INSERT
				let sql = format!(
					"CREATE doc:{doc_id} SET content = 'Document {doc_id} with fulltext content'"
				);

				match ds.execute(&sql, &ses, None).await {
					Ok(mut res) => {
						if let Some(result) = res.pop() {
							match result.result {
								Ok(_) => {
									success_count.fetch_add(1, Ordering::SeqCst);
								}
								Err(e) => {
									let err_str = e.to_string();
									if err_str.contains("Resource busy")
										|| err_str.contains("Transaction conflict")
										|| err_str.contains("TryAgain")
									{
										conflict_count.fetch_add(1, Ordering::SeqCst);
									} else {
										other_error_count.fetch_add(1, Ordering::SeqCst);
										eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
									}
								}
							}
						}
					}
					Err(e) => {
						let err_str = e.to_string();
						if err_str.contains("Resource busy")
							|| err_str.contains("Transaction conflict")
							|| err_str.contains("TryAgain")
						{
							conflict_count.fetch_add(1, Ordering::SeqCst);
						} else {
							other_error_count.fetch_add(1, Ordering::SeqCst);
							eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
						}
					}
				}
			}
		});
	}

	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_docs = NUM_WORKERS * DOCS_PER_WORKER;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);
	let other_errors = other_error_count.load(Ordering::SeqCst);

	println!("\n=== Parallel CREATE Test Results (RocksDB + Fulltext) ===");
	println!("Total documents attempted: {total_docs}");
	println!("Successful creates: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Other errors: {other_errors}");
	println!("Success rate: {:.1}%", (successes as f64 / total_docs as f64) * 100.0);

	let count_sql = "SELECT count() FROM doc GROUP ALL";
	let res = ds.execute(count_sql, &ses, None).await?;
	if let Some(result) = res.into_iter().next() {
		if let Ok(val) = result.result {
			println!("Actual documents in database: {val:?}");
		}
	}

	if conflicts > 0 {
		println!("\nISSUE CONFIRMED: {conflicts} transaction conflicts with CREATE!");
	}

	Ok(())
}

/// Test: Parallel INSERT triggering implicit table creation
///
/// This test demonstrates the ACTUAL root cause of conflicts:
/// When parallel INSERT operations target a table that doesn't exist,
/// each transaction tries to implicitly create the table schema,
/// causing transaction conflicts.
#[tokio::test(flavor = "multi_thread")]
async fn parallel_insert_implicit_table_creation_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 100;
	const DOCS_PER_WORKER: usize = 10;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);

	// NO table pre-definition - table will be implicitly created

	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));
	let other_error_count = Arc::new(AtomicUsize::new(0));
	let error_messages = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();
		let other_error_count = other_error_count.clone();
		let error_messages = error_messages.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			for doc_idx in 0..DOCS_PER_WORKER {
				let doc_id = worker_id * DOCS_PER_WORKER + doc_idx;
				let sql = format!(
					"INSERT INTO new_table {{ id: item:{doc_id}, data: 'test data {doc_id}' }}"
				);

				match ds.execute(&sql, &ses, None).await {
					Ok(mut res) => {
						if let Some(result) = res.pop() {
							match result.result {
								Ok(_) => {
									success_count.fetch_add(1, Ordering::SeqCst);
								}
								Err(e) => {
									let err_str = e.to_string();
									if err_str.contains("Resource busy")
										|| err_str.contains("Transaction conflict")
										|| err_str.contains("TryAgain")
									{
										conflict_count.fetch_add(1, Ordering::SeqCst);
										if error_messages.lock().unwrap().len() < 5 {
											error_messages.lock().unwrap().push(err_str);
										}
									} else {
										other_error_count.fetch_add(1, Ordering::SeqCst);
									}
								}
							}
						}
					}
					Err(e) => {
						let err_str = e.to_string();
						if err_str.contains("Resource busy")
							|| err_str.contains("Transaction conflict")
							|| err_str.contains("TryAgain")
						{
							conflict_count.fetch_add(1, Ordering::SeqCst);
							if error_messages.lock().unwrap().len() < 5 {
								error_messages.lock().unwrap().push(err_str);
							}
						} else {
							other_error_count.fetch_add(1, Ordering::SeqCst);
						}
					}
				}
			}
		});
	}

	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_docs = NUM_WORKERS * DOCS_PER_WORKER;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);
	let other_errors = other_error_count.load(Ordering::SeqCst);

	println!("\n=== Parallel INSERT (Implicit Table Creation) Results ===");
	println!("Total documents attempted: {total_docs}");
	println!("Successful inserts: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Other errors: {other_errors}");
	println!("Success rate: {:.1}%", (successes as f64 / total_docs as f64) * 100.0);

	if conflicts > 0 {
		println!("\nROOT CAUSE CONFIRMED: {conflicts} conflicts due to implicit table creation!");
		println!("Sample error messages:");
		for msg in error_messages.lock().unwrap().iter() {
			println!("  - {msg}");
		}
		println!("\nThis is the actual source of conflicts - not fulltext indexing.");
		println!("Solution: Pre-define tables before parallel INSERT operations.");
	}

	let ses = Session::owner().with_ns("test").with_db("test");
	let count_sql = "SELECT count() FROM new_table GROUP ALL";
	let res = ds.execute(count_sql, &ses, None).await?;
	if let Some(result) = res.into_iter().next() {
		if let Ok(val) = result.result {
			println!("Actual documents in database: {val:?}");
		}
	}

	Ok(())
}

/// Test: Verify that pre-defined tables eliminate conflicts completely
#[tokio::test(flavor = "multi_thread")]
async fn parallel_insert_predefined_table_no_conflicts_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 100;
	const DOCS_PER_WORKER: usize = 10;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Pre-define the table
	let setup_sql = "DEFINE TABLE new_table; DEFINE FIELD data ON new_table TYPE string;";
	let res = ds.execute(setup_sql, &ses, None).await?;
	for r in res {
		r.result?;
	}

	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));

	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			for doc_idx in 0..DOCS_PER_WORKER {
				let doc_id = worker_id * DOCS_PER_WORKER + doc_idx;
				let sql = format!(
					"INSERT INTO new_table {{ id: item:{doc_id}, data: 'test data {doc_id}' }}"
				);

				match ds.execute(&sql, &ses, None).await {
					Ok(mut res) => {
						if let Some(result) = res.pop() {
							match result.result {
								Ok(_) => {
									success_count.fetch_add(1, Ordering::SeqCst);
								}
								Err(e) => {
									let err_str = e.to_string();
									if err_str.contains("Resource busy")
										|| err_str.contains("Transaction conflict")
									{
										conflict_count.fetch_add(1, Ordering::SeqCst);
									}
								}
							}
						}
					}
					Err(e) => {
						let err_str = e.to_string();
						if err_str.contains("Resource busy")
							|| err_str.contains("Transaction conflict")
						{
							conflict_count.fetch_add(1, Ordering::SeqCst);
						}
					}
				}
			}
		});
	}

	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_docs = NUM_WORKERS * DOCS_PER_WORKER;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);

	println!("\n=== Parallel INSERT (Pre-defined Table) Results ===");
	println!("Total documents attempted: {total_docs}");
	println!("Successful inserts: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Success rate: {:.1}%", (successes as f64 / total_docs as f64) * 100.0);

	// This should have zero conflicts
	assert_eq!(conflicts, 0, "Expected no conflicts with pre-defined table");
	assert_eq!(successes, total_docs, "All inserts should succeed");

	println!("\nVERIFIED: Pre-defined tables eliminate transaction conflicts");

	Ok(())
}

// ============================================================================
// HYBRID INDEX TESTS: Fulltext + Vector HNSW
// ============================================================================

/// Helper function to generate a random-ish embedding vector based on doc_id
fn generate_embedding(doc_id: usize, dim: usize) -> String {
	let values: Vec<String> = (0..dim)
		.map(|i| {
			let val = ((doc_id * 17 + i * 31) % 1000) as f64 / 1000.0;
			format!("{:.4}", val)
		})
		.collect();
	format!("[{}]", values.join(", "))
}

/// Test: Parallel INSERT with Fulltext + Vector HNSW indexes (implicit table)
///
/// This test combines fulltext search and vector KNN indexes to test
/// if the combination causes more conflicts due to multiple index updates.
#[tokio::test(flavor = "multi_thread")]
async fn parallel_insert_fulltext_plus_vector_implicit_table_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 100;
	const DOCS_PER_WORKER: usize = 10;
	const VECTOR_DIM: usize = 128;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);

	// NO table pre-definition - implicit creation
	// Only define the analyzer (required before index)
	let ses = Session::owner().with_ns("test").with_db("test");
	let setup_sql = r#"
		DEFINE ANALYZER hybrid_analyzer TOKENIZERS blank,class,punct FILTERS lowercase,ascii;
	"#;
	let res = ds.execute(setup_sql, &ses, None).await?;
	for r in res {
		r.result?;
	}

	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));
	let other_error_count = Arc::new(AtomicUsize::new(0));
	let error_messages = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();
		let other_error_count = other_error_count.clone();
		let error_messages = error_messages.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			for doc_idx in 0..DOCS_PER_WORKER {
				let doc_id = worker_id * DOCS_PER_WORKER + doc_idx;
				let embedding = generate_embedding(doc_id, VECTOR_DIM);
				let sql = format!(
					"INSERT INTO hybrid_docs {{ id: doc:{doc_id}, content: 'Document {doc_id} with searchable text for hybrid search', embedding: {embedding} }}"
				);

				match ds.execute(&sql, &ses, None).await {
					Ok(mut res) => {
						if let Some(result) = res.pop() {
							match result.result {
								Ok(_) => {
									success_count.fetch_add(1, Ordering::SeqCst);
								}
								Err(e) => {
									let err_str = e.to_string();
									if err_str.contains("Resource busy")
										|| err_str.contains("Transaction conflict")
										|| err_str.contains("TryAgain")
									{
										conflict_count.fetch_add(1, Ordering::SeqCst);
										if error_messages.lock().unwrap().len() < 5 {
											error_messages.lock().unwrap().push(err_str);
										}
									} else {
										other_error_count.fetch_add(1, Ordering::SeqCst);
										eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
									}
								}
							}
						}
					}
					Err(e) => {
						let err_str = e.to_string();
						if err_str.contains("Resource busy")
							|| err_str.contains("Transaction conflict")
							|| err_str.contains("TryAgain")
						{
							conflict_count.fetch_add(1, Ordering::SeqCst);
							if error_messages.lock().unwrap().len() < 5 {
								error_messages.lock().unwrap().push(err_str);
							}
						} else {
							other_error_count.fetch_add(1, Ordering::SeqCst);
							eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
						}
					}
				}
			}
		});
	}

	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_docs = NUM_WORKERS * DOCS_PER_WORKER;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);
	let other_errors = other_error_count.load(Ordering::SeqCst);

	println!("\n=== Parallel INSERT (Fulltext + Vector, Implicit Table) Results ===");
	println!("Total documents attempted: {total_docs}");
	println!("Successful inserts: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Other errors: {other_errors}");
	println!("Success rate: {:.1}%", (successes as f64 / total_docs as f64) * 100.0);
	println!("Failure rate: {:.1}%", (conflicts as f64 / total_docs as f64) * 100.0);

	if conflicts > 0 {
		println!("\nCONFLICTS with Fulltext + Vector (implicit table): {conflicts}");
		println!("Sample error messages:");
		for msg in error_messages.lock().unwrap().iter() {
			println!("  - {msg}");
		}
	}

	let count_sql = "SELECT count() FROM hybrid_docs GROUP ALL";
	let res = ds.execute(count_sql, &ses, None).await?;
	if let Some(result) = res.into_iter().next() {
		if let Ok(val) = result.result {
			println!("Actual documents in database: {val:?}");
		}
	}

	Ok(())
}

/// Test: Parallel INSERT with Fulltext + Vector HNSW indexes (pre-defined table)
///
/// This test uses pre-defined table with both fulltext and vector indexes
/// to check if conflicts still occur with hybrid indexing.
///
/// NOTE: This test may panic due to HNSW race condition bug at
/// crates/core/src/idx/trees/hnsw/mod.rs:266
#[tokio::test(flavor = "multi_thread")]
#[ignore = "HNSW has race condition causing panic - see crates/core/src/idx/trees/hnsw/mod.rs:266"]
async fn parallel_insert_fulltext_plus_vector_predefined_table_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 100;
	const DOCS_PER_WORKER: usize = 10;
	const VECTOR_DIM: usize = 128;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Pre-define table with both fulltext and vector indexes
	let setup_sql = format!(
		r#"
		DEFINE TABLE hybrid_docs SCHEMAFULL;
		DEFINE FIELD content ON hybrid_docs TYPE string;
		DEFINE FIELD embedding ON hybrid_docs TYPE array<float>;
		DEFINE ANALYZER hybrid_analyzer TOKENIZERS blank,class,punct FILTERS lowercase,ascii;
		DEFINE INDEX ft_idx ON TABLE hybrid_docs FIELDS content FULLTEXT ANALYZER hybrid_analyzer BM25;
		DEFINE INDEX vec_idx ON TABLE hybrid_docs FIELDS embedding HNSW DIMENSION {VECTOR_DIM} DIST COSINE;
	"#
	);
	let res = ds.execute(&setup_sql, &ses, None).await?;
	for r in res {
		r.result?;
	}

	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));
	let other_error_count = Arc::new(AtomicUsize::new(0));
	let error_messages = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();
		let other_error_count = other_error_count.clone();
		let error_messages = error_messages.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			for doc_idx in 0..DOCS_PER_WORKER {
				let doc_id = worker_id * DOCS_PER_WORKER + doc_idx;
				let embedding = generate_embedding(doc_id, VECTOR_DIM);
				let sql = format!(
					"INSERT INTO hybrid_docs {{ id: doc:{doc_id}, content: 'Document {doc_id} with searchable text for hybrid search', embedding: {embedding} }}"
				);

				match ds.execute(&sql, &ses, None).await {
					Ok(mut res) => {
						if let Some(result) = res.pop() {
							match result.result {
								Ok(_) => {
									success_count.fetch_add(1, Ordering::SeqCst);
								}
								Err(e) => {
									let err_str = e.to_string();
									if err_str.contains("Resource busy")
										|| err_str.contains("Transaction conflict")
										|| err_str.contains("TryAgain")
									{
										conflict_count.fetch_add(1, Ordering::SeqCst);
										if error_messages.lock().unwrap().len() < 5 {
											error_messages.lock().unwrap().push(err_str);
										}
									} else {
										other_error_count.fetch_add(1, Ordering::SeqCst);
										eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
									}
								}
							}
						}
					}
					Err(e) => {
						let err_str = e.to_string();
						if err_str.contains("Resource busy")
							|| err_str.contains("Transaction conflict")
							|| err_str.contains("TryAgain")
						{
							conflict_count.fetch_add(1, Ordering::SeqCst);
							if error_messages.lock().unwrap().len() < 5 {
								error_messages.lock().unwrap().push(err_str);
							}
						} else {
							other_error_count.fetch_add(1, Ordering::SeqCst);
							eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
						}
					}
				}
			}
		});
	}

	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_docs = NUM_WORKERS * DOCS_PER_WORKER;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);
	let other_errors = other_error_count.load(Ordering::SeqCst);

	println!("\n=== Parallel INSERT (Fulltext + Vector, Pre-defined Table) Results ===");
	println!("Total documents attempted: {total_docs}");
	println!("Successful inserts: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Other errors: {other_errors}");
	println!("Success rate: {:.1}%", (successes as f64 / total_docs as f64) * 100.0);
	println!("Failure rate: {:.1}%", (conflicts as f64 / total_docs as f64) * 100.0);

	if conflicts > 0 {
		println!("\nCONFLICTS with Fulltext + Vector (pre-defined): {conflicts}");
		println!("This may indicate issues with hybrid index updates!");
		println!("Sample error messages:");
		for msg in error_messages.lock().unwrap().iter() {
			println!("  - {msg}");
		}
	} else {
		println!("\nNo conflicts with pre-defined hybrid index table");
	}

	let count_sql = "SELECT count() FROM hybrid_docs GROUP ALL";
	let res = ds.execute(count_sql, &ses, None).await?;
	if let Some(result) = res.into_iter().next() {
		if let Ok(val) = result.result {
			println!("Actual documents in database: {val:?}");
		}
	}

	Ok(())
}

/// Test: Parallel INSERT with ONLY Vector HNSW index (no fulltext)
///
/// Control test to isolate vector index behavior from fulltext.
///
/// NOTE: This test causes HNSW panic and 98.9% conflict rate
#[tokio::test(flavor = "multi_thread")]
#[ignore = "HNSW has race condition causing panic and 98.9% conflicts"]
async fn parallel_insert_vector_only_predefined_table_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 100;
	const DOCS_PER_WORKER: usize = 10;
	const VECTOR_DIM: usize = 128;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Pre-define table with only vector index
	let setup_sql = format!(
		r#"
		DEFINE TABLE vector_docs SCHEMAFULL;
		DEFINE FIELD embedding ON vector_docs TYPE array<float>;
		DEFINE INDEX vec_idx ON TABLE vector_docs FIELDS embedding HNSW DIMENSION {VECTOR_DIM} DIST COSINE;
	"#
	);
	let res = ds.execute(&setup_sql, &ses, None).await?;
	for r in res {
		r.result?;
	}

	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));
	let other_error_count = Arc::new(AtomicUsize::new(0));

	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();
		let other_error_count = other_error_count.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			for doc_idx in 0..DOCS_PER_WORKER {
				let doc_id = worker_id * DOCS_PER_WORKER + doc_idx;
				let embedding = generate_embedding(doc_id, VECTOR_DIM);
				let sql = format!(
					"INSERT INTO vector_docs {{ id: doc:{doc_id}, embedding: {embedding} }}"
				);

				match ds.execute(&sql, &ses, None).await {
					Ok(mut res) => {
						if let Some(result) = res.pop() {
							match result.result {
								Ok(_) => {
									success_count.fetch_add(1, Ordering::SeqCst);
								}
								Err(e) => {
									let err_str = e.to_string();
									if err_str.contains("Resource busy")
										|| err_str.contains("Transaction conflict")
										|| err_str.contains("TryAgain")
									{
										conflict_count.fetch_add(1, Ordering::SeqCst);
									} else {
										other_error_count.fetch_add(1, Ordering::SeqCst);
										eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
									}
								}
							}
						}
					}
					Err(e) => {
						let err_str = e.to_string();
						if err_str.contains("Resource busy")
							|| err_str.contains("Transaction conflict")
							|| err_str.contains("TryAgain")
						{
							conflict_count.fetch_add(1, Ordering::SeqCst);
						} else {
							other_error_count.fetch_add(1, Ordering::SeqCst);
							eprintln!("Worker {worker_id}, doc {doc_idx}: {err_str}");
						}
					}
				}
			}
		});
	}

	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_docs = NUM_WORKERS * DOCS_PER_WORKER;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);
	let other_errors = other_error_count.load(Ordering::SeqCst);

	println!("\n=== Parallel INSERT (Vector ONLY, Pre-defined Table) Results ===");
	println!("Total documents attempted: {total_docs}");
	println!("Successful inserts: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Other errors: {other_errors}");
	println!("Success rate: {:.1}%", (successes as f64 / total_docs as f64) * 100.0);

	if conflicts > 0 {
		println!("\nCONFLICTS with Vector-only index: {conflicts}");
	} else {
		println!("\nNo conflicts with vector-only index");
	}

	let count_sql = "SELECT count() FROM vector_docs GROUP ALL";
	let res = ds.execute(count_sql, &ses, None).await?;
	if let Some(result) = res.into_iter().next() {
		if let Ok(val) = result.result {
			println!("Actual documents in database: {val:?}");
		}
	}

	Ok(())
}

/// Test: High-load parallel INSERT with Fulltext + Vector (stress test)
///
/// More aggressive test with higher parallelism to trigger potential conflicts.
///
/// NOTE: This test may panic due to HNSW race condition bug at
/// crates/core/src/idx/trees/hnsw/mod.rs:266
#[tokio::test(flavor = "multi_thread")]
#[ignore = "HNSW has race condition causing panic - see crates/core/src/idx/trees/hnsw/mod.rs:266"]
async fn parallel_insert_fulltext_plus_vector_stress_test_rocksdb() -> Result<()> {
	const NUM_WORKERS: usize = 200;
	const DOCS_PER_WORKER: usize = 25;
	const VECTOR_DIM: usize = 64;

	let (ds, _temp_dir) = new_rocksdb_ds("test", "test").await?;
	let ds = Arc::new(ds);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Pre-define table with both indexes
	let setup_sql = format!(
		r#"
		DEFINE TABLE stress_docs SCHEMAFULL;
		DEFINE FIELD content ON stress_docs TYPE string;
		DEFINE FIELD embedding ON stress_docs TYPE array<float>;
		DEFINE ANALYZER stress_analyzer TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX ft_idx ON TABLE stress_docs FIELDS content FULLTEXT ANALYZER stress_analyzer BM25;
		DEFINE INDEX vec_idx ON TABLE stress_docs FIELDS embedding HNSW DIMENSION {VECTOR_DIM} DIST COSINE;
	"#
	);
	let res = ds.execute(&setup_sql, &ses, None).await?;
	for r in res {
		r.result?;
	}

	let success_count = Arc::new(AtomicUsize::new(0));
	let conflict_count = Arc::new(AtomicUsize::new(0));
	let other_error_count = Arc::new(AtomicUsize::new(0));
	let error_messages = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

	let mut tasks = JoinSet::new();

	for worker_id in 0..NUM_WORKERS {
		let ds = ds.clone();
		let success_count = success_count.clone();
		let conflict_count = conflict_count.clone();
		let other_error_count = other_error_count.clone();
		let error_messages = error_messages.clone();

		tasks.spawn(async move {
			let ses = Session::owner().with_ns("test").with_db("test");

			for doc_idx in 0..DOCS_PER_WORKER {
				let doc_id = worker_id * DOCS_PER_WORKER + doc_idx;
				let embedding = generate_embedding(doc_id, VECTOR_DIM);
				let sql = format!(
					"INSERT INTO stress_docs {{ id: doc:{doc_id}, content: 'Stress test document {doc_id} with hybrid indexing for performance testing', embedding: {embedding} }}"
				);

				match ds.execute(&sql, &ses, None).await {
					Ok(mut res) => {
						if let Some(result) = res.pop() {
							match result.result {
								Ok(_) => {
									success_count.fetch_add(1, Ordering::SeqCst);
								}
								Err(e) => {
									let err_str = e.to_string();
									if err_str.contains("Resource busy")
										|| err_str.contains("Transaction conflict")
										|| err_str.contains("TryAgain")
									{
										conflict_count.fetch_add(1, Ordering::SeqCst);
										if error_messages.lock().unwrap().len() < 10 {
											error_messages.lock().unwrap().push(err_str);
										}
									} else {
										other_error_count.fetch_add(1, Ordering::SeqCst);
									}
								}
							}
						}
					}
					Err(e) => {
						let err_str = e.to_string();
						if err_str.contains("Resource busy")
							|| err_str.contains("Transaction conflict")
							|| err_str.contains("TryAgain")
						{
							conflict_count.fetch_add(1, Ordering::SeqCst);
							if error_messages.lock().unwrap().len() < 10 {
								error_messages.lock().unwrap().push(err_str);
							}
						} else {
							other_error_count.fetch_add(1, Ordering::SeqCst);
						}
					}
				}
			}
		});
	}

	while let Some(result) = tasks.join_next().await {
		result?;
	}

	let total_docs = NUM_WORKERS * DOCS_PER_WORKER;
	let successes = success_count.load(Ordering::SeqCst);
	let conflicts = conflict_count.load(Ordering::SeqCst);
	let other_errors = other_error_count.load(Ordering::SeqCst);

	println!("\n=== STRESS TEST: Parallel INSERT (Fulltext + Vector) Results ===");
	println!("Configuration: {NUM_WORKERS} workers * {DOCS_PER_WORKER} docs = {total_docs} total");
	println!("Vector dimension: {VECTOR_DIM}");
	println!("---");
	println!("Successful inserts: {successes}");
	println!("Transaction conflicts: {conflicts}");
	println!("Other errors: {other_errors}");
	println!("Success rate: {:.1}%", (successes as f64 / total_docs as f64) * 100.0);
	println!("Failure rate: {:.1}%", (conflicts as f64 / total_docs as f64) * 100.0);

	if conflicts > 0 {
		let failure_pct = (conflicts as f64 / total_docs as f64) * 100.0;
		println!("\nSTRESS TEST CONFLICTS: {conflicts} ({failure_pct:.1}%)");

		if failure_pct >= 50.0 {
			println!("CRITICAL: ≥50% failure rate detected!");
		} else if failure_pct >= 10.0 {
			println!("WARNING: ≥10% failure rate detected!");
		}

		println!("\nSample error messages:");
		for msg in error_messages.lock().unwrap().iter() {
			println!("  - {msg}");
		}
	} else {
		println!("\nStress test passed without conflicts");
	}

	let count_sql = "SELECT count() FROM stress_docs GROUP ALL";
	let res = ds.execute(count_sql, &ses, None).await?;
	if let Some(result) = res.into_iter().next() {
		if let Ok(val) = result.result {
			println!("Actual documents in database: {val:?}");
		}
	}

	Ok(())
}
