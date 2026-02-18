//! Tests for RocksDB SST file manager feature
//!
//! This module tests the SST file manager space monitoring feature that:
//! - Limits disk space usage for SST files via the `SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE`
//!   environment variable
//! - Transitions to read-and-deletion-only mode when the space limit is reached
//! - Allows read and delete operations during read-and-deletion-only mode (but blocks writes)
//! - Automatically recovers to normal mode when space drops below the limit after deletions and
//!   compaction

use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::*;
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
	// - Write operations return kvs::Error::ReadAndDeleteOnly
	// - The error message indicates that deleting data will free space
	// - When space drops below the limit (after deletions and compaction), normal mode is restored

	// Required environment variables for this test:
	// - SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE = 10485760 (10MB space limit)
	// - SURREAL_ROCKSDB_WRITE_BUFFER_SIZE = 10240 (controls flush frequency)
	// - SURREAL_ROCKSDB_WAL_SIZE_LIMIT = 1 (forces frequent WAL flushes)

	// Create datastore (read-and-deletion-only mode is triggered by environment variables)
	let (ds, _) = new_ds.create_ds(Uuid::new_v7().into()).await;

	// Phase 1: Initial writes in normal mode (before reaching space limit)
	{
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.set(&"initial_key", &"initial_value".as_bytes().to_vec(), None).await.unwrap();
		tx.commit().await.unwrap();
	}

	// Start a transaction that will be left uncommitted until after mode transition
	let ongoing_tx = ds.transaction(Write, Optimistic).await.unwrap();
	ongoing_tx.set(&"ongoing_key", &"ongoing_value".as_bytes().to_vec(), None).await.unwrap();

	// Phase 2: Write data until space limit is reached and mode transitions to
	// read-and-deletion-only Write ~20MB of data (200 transactions × 100 keys × 1KB each)
	// Some transactions will succeed before the limit, then failures will occur after transition
	let mut count_err = 0;
	for j in 0..200 {
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		for i in 0..100 {
			let key = format!("unlimited_key_{}_{}", i, j);
			let value = vec![0u8; 1024]; // 1KB per value
			if let Err(e) = tx.set(&key, &value, None).await {
				assert!(
					e.to_string().contains("read-and-deletion-only mode"),
					"Unexpected error: {e}"
				);
				count_err += 1;
			}
		}
		if let Err(e) = tx.commit().await {
			assert!(e.to_string().contains("read-and-deletion-only mode"), "Unexpected error: {e}");
			count_err += 1;
		}
	}
	// Verify that mode transition occurred (expect significant number of errors)
	assert!(count_err > 50, "Count error: {}", count_err);

	// Phase 3: Verify behavior in read-and-deletion-only mode

	// Confirm new write transactions are blocked
	{
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		let res = tx.put(&"other_key", &"other_value".as_bytes().to_vec(), None).await;
		assert!(
			res.unwrap_err().to_string().contains("read-and-deletion-only mode"),
			"Expected read-and-deletion-only error"
		);
		tx.cancel().await.unwrap();
	}

	// Confirm pre-existing uncommitted transaction is rejected on commit
	{
		let res = ongoing_tx.commit().await;
		assert!(
			res.unwrap_err().to_string().contains("read-and-deletion-only mode"),
			"Expected read-and-deletion-only error"
		);
	}

	// Confirm read operations still work
	{
		let tx = ds.transaction(Read, Optimistic).await.unwrap();
		let val = tx.get(&"initial_key", None).await.unwrap();
		assert!(matches!(val.as_deref(), Some(b"initial_value")));
		tx.cancel().await.unwrap();
	}

	// Phase 4: Delete data to free space and trigger recovery to normal mode
	// Delete all keys that were successfully written (this frees space below the limit)
	for j in 0..200 {
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		for i in 0..100 {
			let key = format!("unlimited_key_{}_{}", i, j);
			tx.del(&key).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Phase 5: Verify recovery to normal mode
	// Confirm writes are allowed again after space usage drops below limit
	{
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
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
