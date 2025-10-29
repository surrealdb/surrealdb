//! Tests for RocksDB SST file manager feature
//!
//! This module showcases the SST file manager space monitoring feature that:
//! - Limits disk space usage for SST files via environment variables
//! - Configures compaction buffer to prevent write stalls
//! - Transitions to read-only mode when space limit is reached (without deletion-only threshold)
//! - Supports read-and-deletion-only mode for gradual space recovery (with deletion-only threshold)
//! - Automatically recovers to normal mode when space is freed

use temp_dir::TempDir;

use crate::kvs::Datastore;
use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::*;

#[tokio::test]
async fn test_sst_file_manager_read_and_deletion_only_mode() {
	// This test demonstrates the read-and-deletion-only mode configuration.
	// When both MAX_ALLOWED_SPACE_USAGE and MAX_ALLOWED_SPACE_USAGE_DELETION_ONLY
	// are set, the datastore transitions to read-and-deletion-only mode instead
	// of read-only mode when the initial limit is reached.
	//
	// State Machine:
	// Normal -> ReadAndDeletionOnly (when max space is reached and deletion-only threshold is set)
	// ReadAndDeletionOnly -> Normal (when space drops below original limit)
	//
	// In ReadAndDeletionOnly mode:
	// - Read operations are allowed
	// - Write operations return Error::DbReadAndDeleteOnly
	// - The error message indicates that deleting data would help free space
	// - RocksDB automatically resumes with increased threshold
	// - When space drops below the original limit, normal mode is restored

	let temp_dir = TempDir::new().unwrap();
	let path = format!("rocksdb:{}", temp_dir.path().to_string_lossy());

	// Set space limit of 10MB - When the limit is reached, it transitions to deletion-only mode
	unsafe {
		std::env::set_var("SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE", "10485760");
		// Set a small write buffer size (10KB) to force frequent flushes to SST files
		// This ensures the SST file manager can track the data
		std::env::set_var("SURREAL_ROCKSDB_WRITE_BUFFER_SIZE", "10240");
		// Set a small WAL size limit (1MB) to force data to flush to SST files
		std::env::set_var("SURREAL_ROCKSDB_WAL_SIZE_LIMIT", "1");
	}

	// Create datastore with read-and-deletion-only mode configured
	let ds = Datastore::new(&path).await.unwrap();

	// Perform some initial writes that should succeed
	{
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
		tx.set(&"initial_key", &"initial_value".as_bytes().to_vec(), None).await.unwrap();
		tx.commit().await.unwrap();
	}

	// This loop should reach the size limit and transition to deletion-only mode
	let mut count_err = 0;
	for j in 0..200 {
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
		for i in 0..100 {
			// 100KB per transaction
			let key = format!("unlimited_key_{}_{}", i, j);
			let value = vec![0u8; 1024]; // 1KB per value
			if let Err(e) = tx.set(&key, &value, None).await {
				assert!(
					e.to_string().starts_with("The datastore is in read-and-deletion-only mode"),
					"{e}"
				);
				count_err += 1;
			}
		}
		tx.commit().await.unwrap();
	}
	assert!(count_err > 50, "Count error: {}", count_err);

	// More write should not be possible
	{
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
		let res = tx.put(&"other_key", &"other_value".as_bytes().to_vec(), None).await;
		assert!(
			res.unwrap_err()
				.to_string()
				.starts_with("The datastore is in read-and-deletion-only mode")
		);
		tx.cancel().await.unwrap();
	}

	// Verify reads work
	{
		let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
		let val = tx.get(&"initial_key", None).await.unwrap();
		assert!(matches!(val.as_deref(), Some(b"initial_value")));
		tx.cancel().await.unwrap();
	}

	// Verify we can delete data
	for j in 0..200 {
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
		for i in 0..100 {
			let key = format!("unlimited_key_{}_{}", i, j);
			tx.del(&key).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// More writes should be possible again
	{
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
		tx.put(&"other_key", &"other_value".as_bytes().to_vec(), None).await.unwrap();
		tx.commit().await.unwrap();
	}

	// Clean up
	unsafe {
		std::env::remove_var("SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE");
		std::env::remove_var("SURREAL_ROCKSDB_WRITE_BUFFER_SIZE");
		std::env::remove_var("SURREAL_ROCKSDB_WAL_SIZE_LIMIT");
	}
}
