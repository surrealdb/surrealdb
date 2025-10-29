//! Tests for RocksDB SST file manager feature
//!
//! This module tests the SST file manager space monitoring feature that:
//! - Limits disk space usage for SST files via the `SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE`
//!   environment variable
//! - Transitions to read-and-deletion-only mode when the space limit is reached
//! - Allows read and delete operations during read-and-deletion-only mode (but blocks writes)
//! - Automatically recovers to normal mode when space drops below the limit after deletions and
//!   compaction

use std::sync::Arc;

use crate::dbs::node::Timestamp;
use crate::kvs::LockType::Optimistic;
use crate::kvs::SizedClock;
use crate::kvs::TransactionType::*;
use crate::kvs::clock::FakeClock;
use crate::kvs::tests::CreateDs;
use crate::val::Uuid;

pub async fn read_and_deletion_only(new_ds: impl CreateDs) {
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

	// This test relies on the following environment variables to be set
	// Set space limit of 10MB - When the limit is reached, it transitions to read-and-deletion-only
	// SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE = 10485760
	// SURREAL_ROCKSDB_WRITE_BUFFER_SIZE = 10240
	// SURREAL_ROCKSDB_WAL_SIZE_LIMIT = 1

	// Create datastore with read-and-deletion-only mode configured
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(Uuid::new_v7().into(), clock).await;

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

	// More writes should not be possible
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
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn read_and_deletion_only() {
			super::read_and_deletion_only::read_and_deletion_only($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
