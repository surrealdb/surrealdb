//! RocksDB datastore-level tests: exercise RocksDB-specific configuration
//! and behaviour through the `surrealdb-kvs-any` facade (connection-string
//! paths, config maps, read-and-deletion-only mode).

use common::config::ConfigMap;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use surrealdb_kvs::TransactionType::*;
use temp_dir::TempDir;

use super::LockType::Optimistic;
use super::TestDs;

#[tokio::test]
pub async fn read_and_deletion_only() {
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
	let config = ConfigMap::empty()
		.with_key_value("rocksdb_sst_max_allowed_space_usage", "10485760")
		.with_key_value("rocksdb_write_buffer_size", "10240")
		.with_key_value("rocksdb_wal_size_limit", "1");

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let path = format!("rocksdb:{path}");

	// Setup the RocksDB datastore
	let ds = TestDs::new_with_config(&path, config).await.unwrap();

	// Phase 1: Initial writes in normal mode (before reaching space limit)
	{
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.set("initial_key".as_bytes().into(), "initial_value".as_bytes().to_vec()).await.unwrap();
		tx.commit().await.unwrap();
	}

	// Start a transaction that will be left uncommitted until after mode transition
	let ongoing_tx = ds.transaction(Write, Optimistic).await.unwrap();
	ongoing_tx
		.set("ongoing_key".as_bytes().into(), "ongoing_value".as_bytes().to_vec())
		.await
		.unwrap();

	// Phase 2: Write data until space limit is reached and mode transitions to
	// read-and-deletion-only Write ~20MB of data (200 transactions × 100 keys × 1KB each)
	// Some transactions will succeed before the limit, then failures will occur after transition.
	//
	// Values are filled from a deterministic PRNG so they are effectively
	// incompressible: otherwise any per-level compression (e.g. Lz4/Zstd) would
	// shrink a zero-filled payload far below the configured SST limit and the
	// read-and-deletion-only transition would never fire.
	let mut rng = StdRng::seed_from_u64(0xA5A5_A5A5_A5A5_A5A5);
	let mut count_err = 0;
	for j in 0..200 {
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		for i in 0..100 {
			let key = format!("unlimited_key_{}_{}", i, j);
			let mut value = vec![0u8; 1024]; // 1KB per value
			rng.fill_bytes(&mut value);
			if let Err(e) = tx.set(key.as_bytes().into(), value).await {
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
		let res = tx.put("other_key".as_bytes().into(), "other_value".as_bytes().to_vec()).await;
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
		let val = tx.get("initial_key".as_bytes().into(), None).await.unwrap();
		assert!(matches!(val.as_deref(), Some(b"initial_value")));
		tx.cancel().await.unwrap();
	}

	// Phase 4: Delete data to free space and trigger recovery to normal mode
	// Delete all keys that were successfully written (this frees space below the limit)
	for j in 0..200 {
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		for i in 0..100 {
			let key = format!("unlimited_key_{}_{}", i, j);
			tx.del(key.as_bytes().into()).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Phase 5: Verify recovery to normal mode
	// Confirm writes are allowed again after space usage drops below limit
	{
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.put("other_key".as_bytes().into(), "other_value".as_bytes().to_vec()).await.unwrap();
		tx.commit().await.unwrap();
	}
}

/// Verifies that the memory manager clamps `min_write_buffer_number_to_merge`
/// against `max_write_buffer_number`. Without the clamp, setting
/// `min_write_buffer_number_to_merge > max_write_buffer_number` (for example
/// when an operator lowers `max_write_buffer_number=1` alongside the default
/// merge-of-2) causes RocksDB to wait indefinitely for a memtable merge that
/// can never happen, stalling every writer.
///
/// With the clamp in place the datastore must open cleanly and a write +
/// commit must succeed within a short budget; if the clamp regresses the
/// commit here will hang until the test timeout fires.
#[tokio::test(flavor = "multi_thread")]
async fn memtable_merge_count_clamp_non_versioned() {
	memtable_merge_count_clamp_inner(false).await;
}

/// Same invariant as `memtable_merge_count_clamp_non_versioned`, but with
/// versioning enabled so the default column family is opened through an
/// explicit `ColumnFamilyDescriptor`. Exercises the versioned open path
/// (`apply_cf_level_options` + `MemoryManager::apply_to_cf_options` on the
/// CF descriptor's `Options`) to confirm it runs cleanly end-to-end with a
/// misconfigured memtable setup.
#[tokio::test(flavor = "multi_thread")]
async fn memtable_merge_count_clamp_versioned() {
	memtable_merge_count_clamp_inner(true).await;
}

async fn memtable_merge_count_clamp_inner(versioned: bool) {
	// Configure a deliberately misconfigured memtable setup: the merge
	// target (2) exceeds the maximum number of memtables (1). Without
	// the clamp this would stall writers indefinitely.
	let mut config = ConfigMap::empty()
		.with_key_value("rocksdb_max_write_buffer_number", "1")
		.with_key_value("rocksdb_min_write_buffer_number_to_merge", "2");
	if versioned {
		config = config.with_key_value("datastore_versioned", "true");
	}

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let path = format!("rocksdb:{path}");

	let ds = TestDs::new_with_config(&path, config).await.unwrap();

	// A successful write + commit within the timeout proves the clamp
	// ran and was applied to whichever CF RocksDB ended up using (the
	// implicit default for the non-versioned case, or the explicit
	// `ColumnFamilyDescriptor` for the versioned case).
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set("clamp_key".as_bytes().into(), "clamp_value".as_bytes().to_vec()).await.unwrap();
	tokio::time::timeout(std::time::Duration::from_secs(10), tx.commit())
		.await
		.expect("commit stalled: min_write_buffer_number_to_merge clamp regressed")
		.unwrap();
}

/// Sanity-check that `rocksdb_periodic_compaction_seconds` is wired into
/// the column-family options without crashing the open path.
///
/// The behavioural property (compactions actually firing after N seconds)
/// is timer-driven and intentionally not asserted here — this just
/// confirms the setter is reachable for both default-CF and explicit-CF
/// (versioned) open paths.
#[tokio::test(flavor = "multi_thread")]
async fn periodic_compaction_seconds_wired() {
	for versioned in [false, true] {
		let mut config =
			ConfigMap::empty().with_key_value("rocksdb_periodic_compaction_seconds", "60");
		if versioned {
			config = config.with_key_value("datastore_versioned", "true");
		}

		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		let path = format!("rocksdb:{path}");

		let _ds = TestDs::new_with_config(&path, config)
			.await
			.expect("periodic_compaction_seconds should not break the open path");
	}
}

/// Sanity-check that `compaction_style=universal` plus the universal-
/// specific tunables open cleanly. Mirrors the periodic-compaction test
/// above: the assertion is that the open path doesn't error and a
/// follow-up write succeeds, not that universal compaction fires.
#[tokio::test(flavor = "multi_thread")]
async fn universal_compaction_options_wired() {
	let config = ConfigMap::empty()
		.with_key_value("rocksdb_compaction_style", "universal")
		.with_key_value("rocksdb_universal_size_ratio", "5")
		.with_key_value("rocksdb_universal_min_merge_width", "3")
		.with_key_value("rocksdb_universal_max_merge_width", "16")
		.with_key_value("rocksdb_universal_max_size_amplification_percent", "150")
		.with_key_value("rocksdb_universal_compression_size_percent", "75")
		.with_key_value("rocksdb_universal_stop_style", "similar_size");

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let path = format!("rocksdb:{path}");

	let ds = TestDs::new_with_config(&path, config)
		.await
		.expect("universal compaction options should not break the open path");

	// A round-trip write proves the configured CF is healthy.
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set("universal_key".as_bytes().into(), "universal_value".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
}
