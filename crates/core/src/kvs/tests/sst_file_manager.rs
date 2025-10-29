//! Tests for RocksDB SST file manager feature
//!
//! This module tests the SST file manager space monitoring feature that:
//! - Limits disk space usage for SST files via the `SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE` environment variable
//! - Transitions to read-and-deletion-only mode when the space limit is reached
//! - Allows read and delete operations during read-and-deletion-only mode (but blocks writes)
//! - Automatically recovers to normal mode when space drops below the limit after deletions and compaction

use temp_dir::TempDir;

use crate::kvs::Datastore;
use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::*;

#[tokio::test]
async fn test_sst_file_manager_read_and_deletion_only_mode() {
	// This test demonstrates the read-and-deletion-only mode behavior.
	// When SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE is set, the datastore transitions
	// to read-and-deletion-only mode when SST file space usage reaches the configured limit.
	//
	// State Machine:
	// Normal -> ReadAndDeletionOnly (when SST space usage reaches the configured limit)
	// ReadAndDeletionOnly -> Normal (when space usage drops below the limit after deletions)
	//
	// In ReadAndDeletionOnly mode:
	// - Read operations are allowed
	// - Delete operations are allowed (to free up space)
	// - Write operations return Error::DbReadAndDeleteOnly
	// - The error message indicates that deleting data will free space
	// - When space drops below the limit (after deletions and compaction), normal mode is restored

	let temp_dir = TempDir::new().unwrap();
	let path = format!("rocksdb:{}", temp_dir.path().to_string_lossy());

	// Set space limit of 10MB - When the limit is reached, it transitions to read-and-deletion-only mode
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
