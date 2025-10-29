//! Tests for RocksDB SST file manager feature
//!
//! This module showcases the SST file manager space monitoring feature that:
//! - Limits disk space usage for SST files via environment variables
//! - Configures compaction buffer to prevent write stalls
//! - Transitions to read-only mode when space limit is reached (without deletion-only threshold)
//! - Supports read-and-deletion-only mode for gradual space recovery (with deletion-only threshold)
//! - Automatically recovers to normal mode when space is freed

use std::time::Instant;

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

	// Set space limit of 100KB and deletion-only threshold of 150KB
	// When 100KB is reached, it transitions to deletion-only mode with 150KB limit
	unsafe {
		std::env::set_var("SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE", "102400");
		std::env::set_var("SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE_DELETION_ONLY", "153600");
	}

	// Create datastore with read-and-deletion-only mode configured
	let ds = Datastore::new(&path).await.unwrap();

	let t = Instant::now();
	println!("{} Insert initial data", t.elapsed().as_secs_f32());
	// Perform some initial writes that should succeed
	{
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
		tx.set(&"initial_key", &"initial_value".as_bytes().to_vec(), None).await.unwrap();
		tx.commit().await.unwrap();
	}

	// This loop should reach the size limit and transition to deletion-only mode
	println!("{} Insert data until size limit is reached", t.elapsed().as_secs_f32());
	let mut res = Ok(());
	let mut final_j = 0;
	for j in 0..1000 {
		let mut tx = match ds.transaction(Write, Optimistic).await {
			Ok(tx) => tx.inner(),
			Err(e) => {
				final_j = j;
				assert!(
					e.to_string().starts_with("The datastore is in read-and-deletion-only mode"),
					"{e}"
				);
				res = Err(e);
				break;
			}
		};
		for i in 0..100 {
			let key = format!("unlimited_key_{}_{}", i, j);
			let value = vec![0u8; 10240]; // 1KB per value
			tx.set(&key, &value, None).await.unwrap();
		}
		tx.commit().await.unwrap();
	}
	assert!(res.is_err(), "{res:?}");

	println!("{} Verify writes fail", t.elapsed().as_secs_f32());
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

	println!("{} Verify reads work", t.elapsed().as_secs_f32());
	// Verify reads work
	{
		let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
		let val = tx.get(&"initial_key", None).await.unwrap();
		assert!(matches!(val.as_deref(), Some(b"initial_value")));
		tx.cancel().await.unwrap();
	}

	println!("{} Delete data until space limit is freed", t.elapsed().as_secs_f32());
	// Verify we can delete data
	for j in 0..final_j {
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
		for i in 0..100 {
			let key = format!("unlimited_key_{}_{}", i, j);
			tx.del(&key).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	println!("{} Verify writes work again", t.elapsed().as_secs_f32());
	// More writes should be possible again
	{
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
		tx.put(&"other_key", &"other_value".as_bytes().to_vec(), None).await.unwrap();
		tx.commit().await.unwrap();
	}

	println!("{} End", t.elapsed().as_secs_f32());
	// Clean up
	unsafe {
		std::env::remove_var("SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE");
		std::env::remove_var("SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE_DELETION_ONLY");
	}
}
