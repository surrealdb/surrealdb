//! Tests for RocksDB SST file manager feature
//!
//! This module tests the SST file manager space monitoring feature that:
//! - Limits disk space usage for SST files via the `SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE`
//!   environment variable
//! - Transitions to read-and-deletion-only mode when the space limit is reached
//! - Allows read and delete operations during read-and-deletion-only mode (but blocks writes)
//! - Automatically recovers to normal mode when space drops below the limit after deletions and
//!   compaction

use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use temp_dir::TempDir;

use crate::CommunityComposer;
use crate::cnf::ConfigMap;
use crate::kvs::Datastore;
use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::*;

/// Build a half-open byte range that covers every key starting with
/// `prefix`. `end` is `prefix` with a trailing `0xff` byte so RocksDB's
/// iterate bounds match keys `[prefix, prefix\xff)` — used throughout the
/// cursor tests below to compose disjoint ranges.
fn prefix_byte_range(prefix: &str) -> std::ops::Range<Vec<u8>> {
	let start = prefix.as_bytes().to_vec();
	let end = start.iter().copied().chain(std::iter::once(0xff)).collect::<Vec<u8>>();
	start..end
}

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
	let ds = Datastore::builder()
		.with_config(config)
		.build_with_factory_path(&path, CommunityComposer())
		.await
		.unwrap();

	// Phase 1: Initial writes in normal mode (before reaching space limit)
	{
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.set(&"initial_key", &"initial_value".as_bytes().to_vec()).await.unwrap();
		tx.commit().await.unwrap();
	}

	// Start a transaction that will be left uncommitted until after mode transition
	let ongoing_tx = ds.transaction(Write, Optimistic).await.unwrap();
	ongoing_tx.set(&"ongoing_key", &"ongoing_value".as_bytes().to_vec()).await.unwrap();

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
			if let Err(e) = tx.set(&key, &value).await {
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
		let res = tx.put(&"other_key", &"other_value".as_bytes().to_vec()).await;
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
		tx.put(&"other_key", &"other_value".as_bytes().to_vec()).await.unwrap();
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

	let ds = Datastore::builder()
		.with_config(config)
		.build_with_factory_path(&path, CommunityComposer())
		.await
		.unwrap();

	// A successful write + commit within the timeout proves the clamp
	// ran and was applied to whichever CF RocksDB ended up using (the
	// implicit default for the non-versioned case, or the explicit
	// `ColumnFamilyDescriptor` for the versioned case).
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(&"clamp_key", &"clamp_value".as_bytes().to_vec()).await.unwrap();
	tokio::time::timeout(std::time::Duration::from_secs(10), tx.commit())
		.await
		.expect("commit stalled: min_write_buffer_number_to_merge clamp regressed")
		.unwrap();
}

/// Sanity-check that `ALTER SYSTEM COMPACT` (which routes through
/// `Datastore::compact`) pushes all live data to the bottommost level and
/// drains the upper levels.
///
/// We construct the underlying RocksDB datastore directly so we can read
/// `rocksdb.num-files-at-levelN` for every level via `property_int_value`
/// and assert that L0–L5 are empty and L6 carries the SSTs after the
/// compaction completes.
#[tokio::test(flavor = "multi_thread")]
async fn compact_pushes_data_to_bottommost() {
	use rand::rngs::StdRng;
	use rand::{RngCore, SeedableRng};

	use crate::kvs::api::Transactable;
	use crate::kvs::rocksdb::{Datastore as RocksDbDatastore, RocksDbConfig};

	// Write enough deterministically-random data that compaction has at
	// least one SST to move. The values are random so per-level
	// compression cannot collapse the dataset to nothing.
	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	// Tiny SSTs so we exercise multi-level promotion even on a small
	// dataset; the bottommost-target assertion below is the invariant
	// under test, not the absolute file count.
	let config = RocksDbConfig {
		target_file_size_base: 64 * 1024,
		write_buffer_size: 64 * 1024,
		..RocksDbConfig::default()
	};

	let ds = RocksDbDatastore::new(&path, config).await.unwrap();

	// Drive a few dozen small commits so the WAL → memtable → L0 path
	// produces multiple SSTs before we compact.
	let mut rng = StdRng::seed_from_u64(0xC0FF_EEC0_FFEE_u64);
	for batch in 0..32u32 {
		let tx = ds.transaction(true, true).await.unwrap();
		for i in 0..32u32 {
			let key: Vec<u8> = format!("bottommost_test_{batch:04}_{i:04}").into_bytes();
			let mut value = vec![0u8; 256];
			rng.fill_bytes(&mut value);
			tx.set(key, value).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Run the manual compaction. `None` range covers the full keyspace.
	// `compact` is on the `Transactable` impl for `Transaction`, so spin up
	// a short-lived transaction just to dispatch it.
	{
		let tx = ds.transaction(false, true).await.unwrap();
		Transactable::compact(tx.as_ref(), None).await.unwrap();
		tx.cancel().await.unwrap();
	}

	// `num-files-at-levelN` is a per-CF integer property. The default CF
	// is what every Transactable scan touches.
	let level_file_count = |level: usize| -> u64 {
		let name = format!("rocksdb.num-files-at-level{level}");
		ds.db.property_int_value(&name).unwrap_or_default().unwrap_or_default()
	};

	for level in 0..6 {
		let n = level_file_count(level);
		assert_eq!(n, 0, "expected level {level} to be empty after compact, got {n} files");
	}
	let bottom = level_file_count(6);
	assert!(bottom > 0, "expected bottommost level (L6) to carry SSTs, got 0");
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

		let _ds = Datastore::builder()
			.with_config(config)
			.build_with_factory_path(&path, CommunityComposer())
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

	let ds = Datastore::builder()
		.with_config(config)
		.build_with_factory_path(&path, CommunityComposer())
		.await
		.expect("universal compaction options should not break the open path");

	// A round-trip write proves the configured CF is healthy.
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(&"universal_key", &"universal_value".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
}

/// Verify the default-shutdown path: flush memtables, drain in-flight
/// compactions, cancel background work, shut down the memory manager.
/// Asserts the path returns `Ok` and that any auto-scheduled compaction
/// from the post-flush L0 file has either drained or been cancelled
/// before the bg workers are stopped.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drains_cleanly_with_defaults() {
	use crate::kvs::rocksdb::{Datastore as RocksDbDatastore, RocksDbConfig};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let config = RocksDbConfig {
		// Tiny SSTs + small write buffer so the very first commit
		// trips the auto-compaction trigger after the shutdown flush,
		// exercising the wait_for_compact branch.
		target_file_size_base: 64 * 1024,
		write_buffer_size: 64 * 1024,
		..RocksDbConfig::default()
	};

	let ds = RocksDbDatastore::new(&path, config).await.unwrap();
	{
		let tx = ds.transaction(true, true).await.unwrap();
		for i in 0..256u32 {
			let key: Vec<u8> = format!("shutdown_default_{i:04}").into_bytes();
			let value = vec![0u8; 256];
			tx.set(key, value).await.unwrap();
		}
		tx.commit().await.unwrap();
	}
	// Shutdown must succeed and not panic.
	ds.shutdown().await.expect("default shutdown should succeed");
	// And `num-running-compactions` should be zero — the cancel +
	// wait_for_compact pair drained everything.
	let running = ds
		.db
		.property_int_value("rocksdb.num-running-compactions")
		.unwrap_or_default()
		.unwrap_or_default();
	assert_eq!(
		running, 0,
		"expected no background compactions running after shutdown, got {running}"
	);
}

/// Verify the opt-in `compact_on_shutdown` path lands the dataset at the
/// bottommost level, exactly like a manual `ALTER SYSTEM COMPACT`. Same
/// assertion machinery as `compact_pushes_data_to_bottommost` but driven
/// through the shutdown path.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_compacts_to_bottommost_when_opted_in() {
	use rand::rngs::StdRng;
	use rand::{RngCore, SeedableRng};

	use crate::kvs::rocksdb::{Datastore as RocksDbDatastore, RocksDbConfig};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let config = RocksDbConfig {
		target_file_size_base: 64 * 1024,
		write_buffer_size: 64 * 1024,
		compact_on_shutdown: true,
		// Plenty of time on a local test box.
		shutdown_wait_for_compact_seconds: 30,
		..RocksDbConfig::default()
	};

	let ds = RocksDbDatastore::new(&path, config).await.unwrap();

	// Write enough deterministically-random data that at least one SST
	// is produced (random values defeat per-level compression collapse).
	let mut rng = StdRng::seed_from_u64(0xD15C_0DED_BEEF_FACE_u64);
	for batch in 0..32u32 {
		let tx = ds.transaction(true, true).await.unwrap();
		for i in 0..32u32 {
			let key: Vec<u8> = format!("shutdown_compact_{batch:04}_{i:04}").into_bytes();
			let mut value = vec![0u8; 256];
			rng.fill_bytes(&mut value);
			tx.set(key, value).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Shutdown must succeed and push everything down to L6.
	ds.shutdown().await.expect("compact-on-shutdown should succeed");

	let level_file_count = |level: usize| -> u64 {
		let name = format!("rocksdb.num-files-at-level{level}");
		ds.db.property_int_value(&name).unwrap_or_default().unwrap_or_default()
	};
	for level in 0..6 {
		let n = level_file_count(level);
		assert_eq!(n, 0, "expected level {level} empty after compact_on_shutdown, got {n} files",);
	}
	let bottom = level_file_count(6);
	assert!(bottom > 0, "expected bottommost (L6) to carry SSTs, got 0");
}

/// Verifies that many concurrent scan cursors can be opened on the same
/// transaction without interfering with each other.
///
/// This is the regression test for the LRU-thrash failure mode in PR 179
/// (`surrealdb/surrealdb-private#179`): a bounded iterator cache at
/// capacity 4 evicts the outer iterator when 5+ inner prefixes are walked
/// concurrently, e.g. `SELECT ->knows FROM person` with > 4 outer rows. In
/// the cursor design the caller owns the iterator for the duration of
/// each logical scan, so N concurrent prefixes ⇒ N live cursors with no
/// eviction.
///
/// The test opens 8 cursors over disjoint key prefixes, advances each
/// in a strided interleave (round-robin one batch per cursor) so that no
/// cursor is the "most recently used" for very long, and asserts every
/// cursor returns exactly the keys in its prefix.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cursors_do_not_evict() {
	use crate::kvs::api::Transactable;
	use crate::kvs::rocksdb::{Datastore as RocksDbDatastore, RocksDbConfig};
	use crate::kvs::{Direction, NORMAL_BATCH_SIZE, ScanLimit};

	const PREFIX_COUNT: usize = 8;
	const KEYS_PER_PREFIX: usize = 1500;

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	// Seed: PREFIX_COUNT prefixes, each with KEYS_PER_PREFIX keys.
	// Keys are `prefix_NN/key_MMMM` so each prefix is a contiguous range
	// `prefix_NN/` ..= `prefix_NN/\xff`.
	{
		let tx = ds.transaction(true, true).await.unwrap();
		for p in 0..PREFIX_COUNT {
			for k in 0..KEYS_PER_PREFIX {
				let key = format!("prefix_{p:02}/key_{k:04}").into_bytes();
				let value = format!("v_{p}_{k}").into_bytes();
				tx.set(key, value).await.unwrap();
			}
		}
		tx.commit().await.unwrap();
	}

	// Open one cursor per prefix on a single read-only transaction.
	let tx = ds.transaction(false, true).await.unwrap();
	let tx_ref = tx.as_ref();

	let mut cursors = Vec::with_capacity(PREFIX_COUNT);
	let mut collected: Vec<Vec<Vec<u8>>> = vec![Vec::new(); PREFIX_COUNT];
	for p in 0..PREFIX_COUNT {
		let rng = prefix_byte_range(&format!("prefix_{p:02}/"));
		let cursor =
			Transactable::open_keys_cursor(tx_ref, rng, Direction::Forward, 0, None).await.unwrap();
		cursors.push(cursor);
	}

	// Pump round-robin: one batch per cursor per round. With a small
	// batch size we force several rounds, so each cursor's iterator
	// would be evicted under an LRU(4) policy long before the prefix is
	// exhausted.
	let batch_limit = ScanLimit::Count(NORMAL_BATCH_SIZE.min(200));
	let mut active = [true; PREFIX_COUNT];
	while active.iter().any(|a| *a) {
		for (p, cursor) in cursors.iter_mut().enumerate() {
			if !active[p] {
				continue;
			}
			let batch = cursor.next_batch(batch_limit).await.unwrap();
			if batch.is_empty() {
				active[p] = false;
			} else {
				let batch_len = batch.len();
				// the borrowed slices are valid only until the next `next_batch` call from the
				// cursor; copy each slice into an owned `Vec<u8>` so we can hold
				// it after the next `next_batch` invalidates the borrow.
				collected[p].extend(batch.iter().map(|k| k.to_vec()));
				if matches!(batch_limit, ScanLimit::Count(c) if batch_len < c as usize) {
					active[p] = false;
				}
			}
		}
	}

	// Every prefix must have yielded exactly KEYS_PER_PREFIX keys, all
	// in lexicographic (ascending) order, and all distinct.
	for (p, keys) in collected.iter().enumerate() {
		assert_eq!(
			keys.len(),
			KEYS_PER_PREFIX,
			"prefix {p}: expected {KEYS_PER_PREFIX} keys, got {}",
			keys.len()
		);
		for window in keys.windows(2) {
			assert!(window[0] < window[1], "prefix {p}: keys not strictly ascending");
		}
		let expected_prefix = format!("prefix_{p:02}/").into_bytes();
		for k in keys {
			assert!(k.starts_with(&expected_prefix), "prefix {p}: key outside its range");
		}
	}

	// Drop cursors before cancelling so the slot cleanup runs through
	// the cursor `Drop`, not the `cancel()` clear path. Both are
	// supported; this exercises the former.
	drop(cursors);
	tx.cancel().await.unwrap();
}

/// Same as `concurrent_cursors_do_not_evict` but on a writeable
/// transaction. Confirms the writable-variant `ScanIter::Tx` cursor path
/// (iterator built on `inner.tx`) also handles concurrent slots without
/// interference.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cursors_on_writable_tx() {
	use crate::kvs::api::Transactable;
	use crate::kvs::rocksdb::{Datastore as RocksDbDatastore, RocksDbConfig};
	use crate::kvs::{Direction, NORMAL_BATCH_SIZE, ScanLimit};

	const PREFIX_COUNT: usize = 6;
	const KEYS_PER_PREFIX: usize = 300;

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	// Seed in a first transaction so the snapshot the second transaction
	// captures already contains the data.
	{
		let tx = ds.transaction(true, true).await.unwrap();
		for p in 0..PREFIX_COUNT {
			for k in 0..KEYS_PER_PREFIX {
				let key = format!("wp_{p:02}/key_{k:04}").into_bytes();
				let value = vec![p as u8; 8];
				tx.set(key, value).await.unwrap();
			}
		}
		tx.commit().await.unwrap();
	}

	let tx = ds.transaction(true, true).await.unwrap();
	let tx_ref = tx.as_ref();

	let mut cursors = Vec::with_capacity(PREFIX_COUNT);
	for p in 0..PREFIX_COUNT {
		let rng = prefix_byte_range(&format!("wp_{p:02}/"));
		let cursor =
			Transactable::open_keys_cursor(tx_ref, rng, Direction::Forward, 0, None).await.unwrap();
		cursors.push(cursor);
	}

	let batch_limit = ScanLimit::Count(NORMAL_BATCH_SIZE.min(64));
	let mut totals = [0usize; PREFIX_COUNT];
	let mut active = [true; PREFIX_COUNT];
	while active.iter().any(|a| *a) {
		for (p, cursor) in cursors.iter_mut().enumerate() {
			if !active[p] {
				continue;
			}
			let batch = cursor.next_batch(batch_limit).await.unwrap();
			let len = batch.len();
			totals[p] += len;
			// Two termination signals, both correct: a short batch
			// (`len < c`) or an empty batch (`len == 0`). Either alone
			// would suffice for the current `KEYS_PER_PREFIX` (300, not
			// divisible by 64), but pairing them keeps the test robust
			// if `KEYS_PER_PREFIX` is later changed to a multiple of the
			// batch size — without the `len == 0` guard the cursor would
			// pump one extra empty batch before the short-batch heuristic
			// fires, which is correct but loops one round longer than
			// intended. See [DefaultKeysCursor] for the heuristic.
			if len == 0
				|| len
					< match batch_limit {
						ScanLimit::Count(c) => c as usize,
						_ => unreachable!(),
					} {
				active[p] = false;
			}
		}
	}

	for (p, n) in totals.iter().enumerate() {
		assert_eq!(*n, KEYS_PER_PREFIX, "prefix {p}: expected {KEYS_PER_PREFIX}, got {n}");
	}

	drop(cursors);
	tx.cancel().await.unwrap();
}

/// Verifies that opening + dropping many cursors in a tight loop leaves
/// the transaction in a clean state: after a long sequence of open/drop
/// cycles, follow-up operations (another cursor, then a cancel) must
/// succeed without observable interference.
///
/// Each cursor's `_alive_guard` drops last (after the iterator's
/// `ScanState`) and decrements `Transaction::cursors_alive`. This test
/// exercises that path repeatedly under no contention; the
/// concurrent-cursor tests above are what would fail with wrong results
/// if state leaked between cursors.
#[tokio::test(flavor = "multi_thread")]
async fn cursor_drop_releases_slot() {
	use crate::kvs::api::Transactable;
	use crate::kvs::rocksdb::{Datastore as RocksDbDatastore, RocksDbConfig};
	use crate::kvs::{Direction, ScanLimit};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	// Seed a tiny dataset so the cursor has something to point at.
	{
		let tx = ds.transaction(true, true).await.unwrap();
		for k in 0..10 {
			let key = format!("drop_test/{k:02}").into_bytes();
			tx.set(key, vec![0u8]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	let tx = ds.transaction(false, true).await.unwrap();
	let tx_ref = tx.as_ref();

	// Open and immediately drop several cursors. Each Drop should remove
	// its slot.
	for _ in 0..16 {
		let rng = prefix_byte_range("drop_test/");
		let cursor =
			Transactable::open_keys_cursor(tx_ref, rng, Direction::Forward, 0, None).await.unwrap();
		drop(cursor);
	}

	// After the drop storm a fresh cursor must still work end-to-end.
	let rng = prefix_byte_range("drop_test/");
	let mut cursor =
		Transactable::open_keys_cursor(tx_ref, rng, Direction::Forward, 0, None).await.unwrap();
	let batch = cursor.next_batch(ScanLimit::Count(100)).await.unwrap();
	assert_eq!(batch.len(), 10, "fresh cursor should observe all seeded keys");
	drop(cursor);

	tx.cancel().await.unwrap();
}

/// Verifies `next_batch` returns borrowed slices that match the bytes
/// inserted, in order, across multiple batches. Confirms that reusing the
/// cursor's internal buffer doesn't bleed bytes between batches.
#[tokio::test(flavor = "multi_thread")]
async fn next_batch_borrowed_slices_match_owned_scan() {
	use crate::kvs::api::Transactable;
	use crate::kvs::rocksdb::{Datastore as RocksDbDatastore, RocksDbConfig};
	use crate::kvs::{Direction, ScanLimit};

	const N: usize = 1500;

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	{
		let tx = ds.transaction(true, true).await.unwrap();
		for k in 0..N {
			let key = format!("fe_key/{k:06}").into_bytes();
			tx.set(key, vec![0u8; 4]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Drive the cursor; copy each borrowed slice into an owned Vec so we
	// can hold across the next `next_batch` (which would invalidate the
	// borrow). Compare against the expected keys generated locally.
	let tx = ds.transaction(false, true).await.unwrap();
	let tx_ref = tx.as_ref();
	let rng = prefix_byte_range("fe_key/");
	let mut cursor =
		Transactable::open_keys_cursor(tx_ref, rng, Direction::Forward, 0, None).await.unwrap();
	let mut collected: Vec<Vec<u8>> = Vec::with_capacity(N);
	loop {
		let batch = cursor.next_batch(ScanLimit::Count(200)).await.unwrap();
		let batch_len = batch.len();
		collected.extend(batch.iter().map(|k| k.to_vec()));
		if batch_len < 200 {
			break;
		}
	}
	drop(cursor);
	tx.cancel().await.unwrap();

	let expected: Vec<Vec<u8>> = (0..N).map(|k| format!("fe_key/{k:06}").into_bytes()).collect();
	assert_eq!(collected.len(), N, "cursor should observe all seeded keys");
	assert_eq!(collected, expected, "borrowed slices must reproduce the inserted bytes");
}

/// Verifies the cursor/commit SeqCst protocol directly: `commit()` must
/// not complete while a caller-owned cursor handle is alive on the same
/// transaction, and must complete promptly once the cursor is dropped.
///
/// This is the subtle case the protocol exists for. If `drain_cursors`
/// did not actually wait, the boxed `rocksdb::Transaction` would be
/// consumed by `(*inner).commit()` while the cursor's iterator still
/// referenced it (use-after-free).
#[tokio::test(flavor = "multi_thread")]
async fn commit_blocks_until_live_cursor_drops() {
	use std::time::Duration;

	use futures::FutureExt;

	use crate::kvs::api::Transactable;
	use crate::kvs::rocksdb::{Datastore as RocksDbDatastore, RocksDbConfig};
	use crate::kvs::{Direction, ScanLimit};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	// Seed something so the cursor has data to read. The actual contents
	// don't matter; what matters is that a cursor can be opened.
	{
		let tx = ds.transaction(true, true).await.unwrap();
		for k in 0..16 {
			let key = format!("race_key/{k:02}").into_bytes();
			tx.set(key, vec![0u8]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Writable tx so we exercise the commit (not cancel) path.
	let tx = ds.transaction(true, true).await.unwrap();
	let tx_ref = tx.as_ref();

	// Open a cursor. After this point `cursors_alive == 1` and any
	// concurrent `commit()` must block in `drain_cursors` until the
	// cursor handle is dropped.
	let rng = prefix_byte_range("race_key/");
	let mut cursor =
		Transactable::open_keys_cursor(tx_ref, rng, Direction::Forward, 0, None).await.unwrap();

	// Pump a batch so the iterator is actually built (`started = true`),
	// not just allocated. This is the worst case for the race: the
	// iterator is fully initialised and references `inner.tx` /
	// `self.db`, so a premature commit would be a use-after-free, not a
	// no-op on an unallocated iterator.
	let _ = cursor.next_batch(ScanLimit::Count(4)).await.unwrap();

	// Kick off the commit. The first poll runs synchronously until the
	// first `.await` point, which is `drain_cursors`. After that point
	// `done == true` (so no new cursor can open) and the future is
	// parked yielding on `cursors_alive > 0`.
	// `commit()` already returns a `Pin<Box<dyn Future>>` (`BoxFut`),
	// so no extra `Box::pin` is needed. `as_mut()` below gives the
	// `Pin<&mut dyn Future>` that `now_or_never` / `await` operate on.
	let mut commit_fut = tx_ref.commit();

	// Poll commit several times. It must remain `Pending` for every poll
	// — anything else means `drain_cursors` returned early and the
	// SeqCst protocol is broken.
	//
	// 32 yields is more than enough for any reasonable scheduler to
	// drive the commit task through `drain_cursors`'s yield-loop a
	// large number of times. If it ever returns `Ready` here, the bug
	// is in the protocol, not in scheduling.
	for round in 0..32 {
		let snapshot = commit_fut.as_mut().now_or_never();
		assert!(
			snapshot.is_none(),
			"commit_fut completed at round {round} while a cursor was still alive — \
			drain_cursors did not actually drain (SeqCst protocol broken)"
		);
		tokio::task::yield_now().await;
	}

	// Drop the cursor. `AliveGuard::drop` decrements `cursors_alive`
	// AFTER the iterator's destructor has run (field-declaration order
	// on `RocksDbKeysCursor`), so by the time the drain-loop observes
	// the new value the parent's refcount is already decremented.
	drop(cursor);

	// Commit should now complete. A generous timeout guards against a
	// silent deadlock — without it a regression here would hang the
	// test runner instead of failing.
	tokio::time::timeout(Duration::from_secs(5), commit_fut)
		.await
		.expect("commit deadlocked after cursor was dropped — drain_cursors did not wake up")
		.expect("commit failed after cursor drop");
}

/// Same as `commit_blocks_until_live_cursor_drops` but for the cancel
/// path. Both `commit` and `cancel` use the same drain protocol; testing
/// both confirms the wait isn't accidentally elided on one branch.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_blocks_until_live_cursor_drops() {
	use std::time::Duration;

	use futures::FutureExt;

	use crate::kvs::api::Transactable;
	use crate::kvs::rocksdb::{Datastore as RocksDbDatastore, RocksDbConfig};
	use crate::kvs::{Direction, ScanLimit};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	{
		let tx = ds.transaction(true, true).await.unwrap();
		for k in 0..16 {
			let key = format!("cancel_race/{k:02}").into_bytes();
			tx.set(key, vec![0u8]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Read-only tx so the iterator goes through the `ScanIter::Db`
	// branch (borrow into `self.db`) rather than `ScanIter::Tx`.
	// Combined with the writable variant in the commit test above, both
	// `ScanIter` arms get coverage.
	let tx = ds.transaction(false, true).await.unwrap();
	let tx_ref = tx.as_ref();

	let rng = prefix_byte_range("cancel_race/");
	let mut cursor =
		Transactable::open_keys_cursor(tx_ref, rng, Direction::Forward, 0, None).await.unwrap();
	let _ = cursor.next_batch(ScanLimit::Count(4)).await.unwrap();

	let mut cancel_fut = tx_ref.cancel();
	for round in 0..32 {
		let snapshot = cancel_fut.as_mut().now_or_never();
		assert!(
			snapshot.is_none(),
			"cancel_fut completed at round {round} while a cursor was still alive — \
			drain_cursors did not actually drain on the cancel path"
		);
		tokio::task::yield_now().await;
	}

	drop(cursor);

	tokio::time::timeout(Duration::from_secs(5), cancel_fut)
		.await
		.expect("cancel deadlocked after cursor was dropped")
		.expect("cancel failed after cursor drop");
}

/// Verifies the open-side of the SeqCst protocol (invariant I3 in
/// `build_scan_iter`'s SAFETY comment): a cursor that tries to open
/// after `commit`/`cancel` has set `done = true` must fail immediately
/// with `TransactionFinished`, not race past the check and build an
/// iterator against a transaction that's about to drop its snapshot.
///
/// Test shape: hold one "blocker" cursor so commit's `drain_cursors`
/// stays parked. The blocker pins the transaction in a known state
/// (done == true, cursors_alive == 1) for the duration of the assertion,
/// which removes a timing dependency on the commit coordinator's
/// `wait_for_sync` path that fires once drain returns.
#[tokio::test(flavor = "multi_thread")]
async fn open_cursor_after_commit_starts_fails() {
	use futures::FutureExt;

	use crate::kvs::Direction;
	use crate::kvs::api::Transactable;
	use crate::kvs::rocksdb::{Datastore as RocksDbDatastore, RocksDbConfig};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	{
		let tx = ds.transaction(true, true).await.unwrap();
		tx.set(b"after_commit/key".to_vec(), vec![0u8]).await.unwrap();
		tx.commit().await.unwrap();
	}

	let tx = ds.transaction(true, true).await.unwrap();
	let tx_ref = tx.as_ref();

	// Open the blocker cursor first. `cursors_alive` is now 1, so any
	// concurrent commit will park in `drain_cursors` until we drop it.
	let blocker_rng = prefix_byte_range("after_commit/");
	let blocker = Transactable::open_keys_cursor(tx_ref, blocker_rng, Direction::Forward, 0, None)
		.await
		.unwrap();

	// Kick off commit. Synchronous prefix runs first:
	// `done.swap(true, SeqCst)` → writeable/restricted checks → enters
	// `drain_cursors`, which yields because `cursors_alive == 1`.
	// The poll returns Pending. After this point any new cursor open
	// must observe `done == true` and fail.
	// `commit()` already returns a `Pin<Box<dyn Future>>` (`BoxFut`),
	// so no extra `Box::pin` is needed. `as_mut()` below gives the
	// `Pin<&mut dyn Future>` that `now_or_never` / `await` operate on.
	let mut commit_fut = tx_ref.commit();
	let first_poll = commit_fut.as_mut().now_or_never();
	assert!(
		first_poll.is_none(),
		"commit completed despite the blocker cursor still being alive; drain_cursors must \
		park while cursors_alive > 0"
	);

	// With the blocker cursor still held, `done` is now true (commit's
	// swap landed in the same poll above). Attempt to open another
	// cursor — `open_*_cursor` does `fetch_add(SeqCst)` then
	// `load(done, SeqCst)`. The SeqCst total order forces the load to
	// observe `done == true` and abort with TransactionFinished.
	let rng = prefix_byte_range("after_commit/");
	let result = Transactable::open_keys_cursor(tx_ref, rng, Direction::Forward, 0, None).await;
	assert!(
		result.is_err(),
		"open_keys_cursor must fail after commit set done=true; got Ok — the open \
		protocol's done re-check raced past the swap"
	);

	// Drop blocker, let commit complete.
	drop(blocker);
	commit_fut.await.unwrap();
}

/// Regression test for the cursor-open cancellation leak.
///
/// `open_*_cursor` does `cursors_alive.fetch_add(SeqCst)` and *then*
/// awaits `self.inner.lock()`. If the calling future is dropped at that
/// await — query cancellation, timeout, `tokio::select!` losing arm,
/// etc. — the increment must not leak: a leaked slot stalls
/// `drain_cursors` forever in the next `commit`/`cancel`.
///
/// The fix wraps the increment in an `AliveGuard` bound before the
/// `lock().await`, so the future-drop runs the guard's `Drop`.
///
/// Test shape (behavioural, no atomic introspection):
///
/// 1. Seed enough keys that a writable `count()` takes long enough to reliably hold `inner` across
///    the polls below.
/// 2. Drive `count()` to its parked-at-affinitypool state. Its closure now holds the `inner` lock
///    guard.
/// 3. Drive `open_keys_cursor` to its parked-at-`inner.lock()` state. The
///    `fetch_add(cursors_alive)` has executed; the future would leak it pre-fix.
/// 4. Drop the parked future. Pre-fix, the slot stays incremented.
/// 5. Let `count()` finish to release `inner`.
/// 6. Call `commit()`. Pre-fix, `drain_cursors` loops forever on the leaked slot — the timeout
///    below would fail the test. With the fix, `AliveGuard::drop` ran at step 4 and commit
///    completes.
#[tokio::test(flavor = "multi_thread")]
async fn open_cursor_cancellation_releases_cursors_alive_slot() {
	use std::time::Duration;

	use futures::FutureExt;

	use crate::kvs::Direction;
	use crate::kvs::api::Transactable;
	use crate::kvs::rocksdb::{Datastore as RocksDbDatastore, RocksDbConfig};

	// `affinitypool::spawn_local` falls back to synchronous execution
	// when no global threadpool exists. Production paths init it via
	// `kvs::Datastore::new`; the rocksdb-only test harness does not.
	// Without an actual worker thread, `count()` never yields while
	// holding `inner`, so we can't drive `open_keys_cursor` into the
	// parked state that exposes the leak. Init is idempotent.
	crate::kvs::threadpool::initialise();

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	// Seed enough keys that a writable `count()` reliably holds the
	// inner lock across the polls below. 50K small keys runs in tens
	// of milliseconds — far more than the microsecond budget of the
	// two `now_or_never` polls plus the future-drop.
	{
		let tx = ds.transaction(true, true).await.unwrap();
		for k in 0..50_000u32 {
			let key = format!("cancel_open/{k:08}").into_bytes();
			tx.set(key, vec![0u8]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Writable tx so `count()` takes the `inner` lock and holds it
	// across the affinitypool-offloaded scan: that's our contention
	// source for forcing `open_keys_cursor` into the parked state.
	let tx = ds.transaction(true, true).await.unwrap();
	let tx_ref = tx.as_ref();

	let rng = prefix_byte_range("cancel_open/");

	// Drive `count()` to the affinitypool await. After this poll the
	// `inner.lock()` has been acquired and handed to `count_blocking`,
	// which is now running on a worker thread holding the guard.
	let mut count_fut = tx_ref.count(rng.clone(), None);
	let count_first_poll = count_fut.as_mut().now_or_never();
	assert!(
		count_first_poll.is_none(),
		"count() must be pending — count_blocking should still be running on the affinitypool \
		worker and holding the inner lock guard"
	);

	// Drive `open_keys_cursor` to its parked state. Its first poll
	// executes `cursors_alive.fetch_add(SeqCst)` then parks on
	// `inner.lock().await` because count's closure holds the lock.
	let mut open_fut =
		Transactable::open_keys_cursor(tx_ref, rng.clone(), Direction::Forward, 0, None);
	let open_first_poll = open_fut.as_mut().now_or_never();
	assert!(
		open_first_poll.is_none(),
		"open_keys_cursor must park on inner.lock() while count() holds it; if this fires Ok \
		the test set-up didn't produce contention and the regression isn't being exercised"
	);

	// Cancel: drop the parked future. The fix's `AliveGuard` runs Drop
	// here and decrements the slot claimed by the `fetch_add`. Pre-fix
	// the slot leaked and `drain_cursors` below would hang.
	drop(open_fut);

	// Let count complete so it releases `inner`.
	count_fut.await.unwrap();

	// commit must complete promptly. Pre-fix the leaked slot makes
	// `drain_cursors` loop forever on `cursors_alive > 0`, tripping
	// this timeout.
	tokio::time::timeout(Duration::from_secs(5), tx_ref.commit())
		.await
		.expect(
			"commit deadlocked — cancelled open_keys_cursor leaked cursors_alive (the \
			fetch_add must be wrapped in AliveGuard so the future-drop decrements it)",
		)
		.expect("commit failed");
}
