//! Tests for RocksDB SST file manager feature
//!
//! This module tests the SST file manager space monitoring feature that:
//! - Limits disk space usage for SST files via the `SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE`
//!   environment variable
//! - Transitions to read-and-deletion-only mode when the space limit is reached
//! - Allows read and delete operations during read-and-deletion-only mode (but blocks writes)
//! - Automatically recovers to normal mode when space drops below the limit after deletions and
//!   compaction

use surrealdb_kvs::{KeyRange, TransactionType};
use temp_dir::TempDir;

/// Build a half-open byte range that covers every key starting with
/// `prefix`. `end` is `prefix` with a trailing `0xff` byte so RocksDB's
/// iterate bounds match keys `[prefix, prefix\xff)` — used throughout the
/// cursor tests below to compose disjoint ranges.
fn prefix_byte_range(prefix: &str) -> KeyRange<'static> {
	let start = prefix.as_bytes().to_vec();
	let end = start.iter().copied().chain(std::iter::once(0xff)).collect::<Vec<u8>>();
	KeyRange {
		start: start.into(),
		end: end.into(),
	}
}

/// Build a range over `prefix` whose bounds differ in a byte that
/// `shard_range` can interpolate across, so a read-only `count()` takes the
/// parallel shard path rather than falling back to a single serial scan.
///
/// [`prefix_byte_range`] cannot be used for that: its bounds are `prefix` and
/// `prefix\xff`, so one is a prefix of the other and `shard_range` returns a
/// single shard. Padding the lower bound with `0x00` gives a first differing
/// byte of `0x00` vs `0xff`, which splits.
fn splittable_prefix_range(prefix: &str) -> KeyRange<'static> {
	let start = prefix.as_bytes().iter().copied().chain(std::iter::once(0x00)).collect::<Vec<u8>>();
	let end = prefix.as_bytes().iter().copied().chain(std::iter::once(0xff)).collect::<Vec<u8>>();
	KeyRange {
		start: start.into(),
		end: end.into(),
	}
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
	use surrealdb_kvs::api::Transactable;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

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
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for i in 0..32u32 {
			let key: Vec<u8> = format!("bottommost_test_{batch:04}_{i:04}").into_bytes();
			let mut value = vec![0u8; 256];
			rng.fill_bytes(&mut value);
			tx.set(key.into(), value).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Run the manual compaction. `None` range covers the full keyspace.
	// `compact` is on the `Transactable` impl for `Transaction`, so spin up
	// a short-lived transaction just to dispatch it.
	{
		let tx = ds.transaction(TransactionType::Read).await.unwrap();
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

/// Verify the default-shutdown path: flush memtables, drain in-flight
/// compactions, cancel background work, shut down the memory manager.
/// Asserts the path returns `Ok` and that any auto-scheduled compaction
/// from the post-flush L0 file has either drained or been cancelled
/// before the bg workers are stopped.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drains_cleanly_with_defaults() {
	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

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
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for i in 0..256u32 {
			let key: Vec<u8> = format!("shutdown_default_{i:04}").into_bytes();
			let value = vec![0u8; 256];
			tx.set(key.into(), value).await.unwrap();
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

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

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
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for i in 0..32u32 {
			let key: Vec<u8> = format!("shutdown_compact_{batch:04}_{i:04}").into_bytes();
			let mut value = vec![0u8; 256];
			rng.fill_bytes(&mut value);
			tx.set(key.into(), value).await.unwrap();
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
/// This guards against LRU thrash in a bounded iterator cache. A cache of
/// capacity 4 evicts the outer iterator when 5 or more inner prefixes are
/// walked concurrently, e.g. `SELECT ->knows FROM person` with more than
/// four outer rows. The caller owns each iterator for the duration of its
/// logical scan, so N concurrent prefixes give N live cursors and none is
/// evicted.
/// Every cursor must still return its full prefix.
///
/// The test opens 8 cursors over disjoint key prefixes, advances each
/// in a strided interleave (round-robin one batch per cursor) so that no
/// cursor is the "most recently used" for very long, and asserts every
/// cursor returns exactly the keys in its prefix.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cursors_do_not_evict() {
	use surrealdb_kvs::Direction;
	use surrealdb_kvs::api::Transactable;
	use surrealdb_kvs::consts::NORMAL_BATCH_SIZE;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	const PREFIX_COUNT: usize = 8;
	const KEYS_PER_PREFIX: usize = 1500;

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	// Seed: PREFIX_COUNT prefixes, each with KEYS_PER_PREFIX keys.
	// Keys are `prefix_NN/key_MMMM` so each prefix is a contiguous range
	// `prefix_NN/` ..= `prefix_NN/\xff`.
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for p in 0..PREFIX_COUNT {
			for k in 0..KEYS_PER_PREFIX {
				let key = format!("prefix_{p:02}/key_{k:04}").into_bytes();
				let value = format!("v_{p}_{k}").into_bytes();
				tx.set(key.into(), value).await.unwrap();
			}
		}
		tx.commit().await.unwrap();
	}

	// Open one cursor per prefix on a single read-only transaction.
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
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
	let batch_limit = NORMAL_BATCH_SIZE.min(200);
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
				if batch_len < batch_limit as usize {
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
	use surrealdb_kvs::Direction;
	use surrealdb_kvs::api::Transactable;
	use surrealdb_kvs::consts::NORMAL_BATCH_SIZE;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	const PREFIX_COUNT: usize = 6;
	const KEYS_PER_PREFIX: usize = 300;

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	// Seed in a first transaction so the snapshot the second transaction
	// captures already contains the data.
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for p in 0..PREFIX_COUNT {
			for k in 0..KEYS_PER_PREFIX {
				let key = format!("wp_{p:02}/key_{k:04}").into_bytes();
				let value = vec![p as u8; 8];
				tx.set(key.into(), value).await.unwrap();
			}
		}
		tx.commit().await.unwrap();
	}

	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	let tx_ref = tx.as_ref();

	let mut cursors = Vec::with_capacity(PREFIX_COUNT);
	for p in 0..PREFIX_COUNT {
		let rng = prefix_byte_range(&format!("wp_{p:02}/"));
		let cursor =
			Transactable::open_keys_cursor(tx_ref, rng, Direction::Forward, 0, None).await.unwrap();
		cursors.push(cursor);
	}

	let batch_limit = NORMAL_BATCH_SIZE.min(64);
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
			if len == 0 || len < batch_limit as usize {
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
	use surrealdb_kvs::Direction;
	use surrealdb_kvs::api::Transactable;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	// Seed a tiny dataset so the cursor has something to point at.
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for k in 0..10 {
			let key = format!("drop_test/{k:02}").into_bytes();
			tx.set(key.into(), vec![0u8]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	let tx = ds.transaction(TransactionType::Read).await.unwrap();
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
	let batch = cursor.next_batch(100).await.unwrap();
	assert_eq!(batch.len(), 10, "fresh cursor should observe all seeded keys");
	drop(cursor);

	tx.cancel().await.unwrap();
}

/// Verifies `next_batch` returns borrowed slices that match the bytes
/// inserted, in order, across multiple batches. Confirms that reusing the
/// cursor's internal buffer doesn't bleed bytes between batches.
#[tokio::test(flavor = "multi_thread")]
async fn next_batch_borrowed_slices_match_owned_scan() {
	use surrealdb_kvs::Direction;
	use surrealdb_kvs::api::Transactable;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	const N: usize = 1500;

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for k in 0..N {
			let key = format!("fe_key/{k:06}").into_bytes();
			tx.set(key.into(), vec![0u8; 4]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Drive the cursor; copy each borrowed slice into an owned Vec so we
	// can hold across the next `next_batch` (which would invalidate the
	// borrow). Compare against the expected keys generated locally.
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let tx_ref = tx.as_ref();
	let rng = prefix_byte_range("fe_key/");
	let mut cursor =
		Transactable::open_keys_cursor(tx_ref, rng, Direction::Forward, 0, None).await.unwrap();
	let mut collected: Vec<Vec<u8>> = Vec::with_capacity(N);
	loop {
		let batch = cursor.next_batch(200).await.unwrap();
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
	use surrealdb_kvs::Direction;
	use surrealdb_kvs::api::Transactable;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	// Seed something so the cursor has data to read. The actual contents
	// don't matter; what matters is that a cursor can be opened.
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for k in 0..16 {
			let key = format!("race_key/{k:02}").into_bytes();
			tx.set(key.into(), vec![0u8]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Writable tx so we exercise the commit (not cancel) path.
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
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
	let _ = cursor.next_batch(4).await.unwrap();

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
	use surrealdb_kvs::Direction;
	use surrealdb_kvs::api::Transactable;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for k in 0..16 {
			let key = format!("cancel_race/{k:02}").into_bytes();
			tx.set(key.into(), vec![0u8]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let tx_ref = tx.as_ref();

	let rng = prefix_byte_range("cancel_race/");
	let mut cursor =
		Transactable::open_keys_cursor(tx_ref, rng, Direction::Forward, 0, None).await.unwrap();
	let _ = cursor.next_batch(4).await.unwrap();

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

/// Verifies the open side of the SeqCst protocol described in the SAFETY
/// comment on `build_scan_iter`: a cursor that tries to open
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
	use surrealdb_kvs::Direction;
	use surrealdb_kvs::api::Transactable;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		tx.set(b"after_commit/key".as_slice().into(), vec![0u8]).await.unwrap();
		tx.commit().await.unwrap();
	}

	let tx = ds.transaction(TransactionType::Write).await.unwrap();
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

/// Dropping an open-cursor future must not leak its `cursors_alive` slot.
///
/// `open_*_cursor` increments `cursors_alive` with `fetch_add(SeqCst)` and then
/// awaits `self.inner.lock()`. If the calling future is dropped at that
/// await (query cancellation, timeout, a `tokio::select!` losing arm, and
/// so on), a leaked slot would stall `drain_cursors` forever in the next
/// `commit`/`cancel`.
///
/// The increment is owned by an `AliveGuard` bound before the
/// `lock().await`, so dropping the future runs the guard's `Drop`.
///
/// Test shape (behavioural, no atomic introspection):
///
/// 1. Seed enough keys that a writable `count()` takes long enough to reliably hold `inner` across
///    the polls below.
/// 2. Drive `count()` to its parked-at-affinitypool state. Its closure now holds the `inner` lock
///    guard.
/// 3. Drive `open_keys_cursor` to its parked-at-`inner.lock()` state. The
///    `fetch_add(cursors_alive)` has executed, so the slot is claimed.
/// 4. Drop the parked future. The guard releases the claimed slot.
/// 5. Let `count()` finish to release `inner`.
/// 6. Call `commit()`. A leaked slot would make `drain_cursors` loop forever, and the timeout below
///    would fail the test. `AliveGuard::drop` ran at step 4, so commit completes.
#[tokio::test(flavor = "multi_thread")]
async fn open_cursor_cancellation_releases_cursors_alive_slot() {
	use std::time::Duration;

	use futures::FutureExt;
	use surrealdb_kvs::Direction;
	use surrealdb_kvs::api::Transactable;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	// `affinitypool::spawn_local` falls back to synchronous execution
	// when no global threadpool exists. Production paths init it via
	// `kvs::Datastore::new`; the rocksdb-only test harness does not.
	// Without an actual worker thread, `count()` never yields while
	// holding `inner`, so we can't drive `open_keys_cursor` into the
	// parked state that exposes the leak. Init is idempotent.
	surrealdb_kvs::threadpool::initialise();

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	// Seed enough keys that a writable `count()` reliably holds the
	// inner lock across the polls below. 50K small keys runs in tens
	// of milliseconds — far more than the microsecond budget of the
	// two `now_or_never` polls plus the future-drop.
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for k in 0..50_000u32 {
			let key = format!("cancel_open/{k:08}").into_bytes();
			tx.set(key.into(), vec![0u8]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Writable tx so `count()` takes the `inner` lock and holds it
	// across the affinitypool-offloaded scan: that's our contention
	// source for forcing `open_keys_cursor` into the parked state.
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
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

	// Cancel: drop the parked future. The `AliveGuard` runs its `Drop` here
	// and decrements the slot claimed by the `fetch_add`, so `drain_cursors`
	// below does not wait on it.
	drop(open_fut);

	// Let count complete so it releases `inner`.
	count_fut.await.unwrap();

	// commit must complete promptly. A leaked slot would keep
	// `drain_cursors` looping on `cursors_alive > 0`, tripping
	// this timeout.
	tokio::time::timeout(Duration::from_secs(5), tx_ref.commit())
		.await
		.expect(
			"commit deadlocked — cancelled open_keys_cursor leaked cursors_alive (the \
			fetch_add must be wrapped in AliveGuard so the future-drop decrements it)",
		)
		.expect("commit failed");
}

/// A commit attempted after the coordinator has begun shutting down must be
/// refused before anything is applied.
///
/// Group commit applies the transaction on the caller thread and then waits
/// for the coordinator's batched fsync. Once shutdown has begun the fsync can
/// cannot confirm durability, so the pre-apply gate fails the commit with
/// `Shutdown` — nothing is applied and the caller can safely retry after
/// reconnecting. Shutdown is treated as a controlled crash: no commit runs
/// past this point, which the datastore must already stay consistent against.
#[tokio::test(flavor = "multi_thread")]
async fn commit_after_coordinator_shutdown_is_refused_before_apply() {
	use surrealdb_kvs::err::Error;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	// The default configuration uses `sync=every`, which runs the coordinator.
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();

	// Stage a write, then shut the coordinator down before committing, as a
	// datastore shutdown racing an in-flight writer would.
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	tx.set(b"refused/key".as_slice().into(), vec![1u8]).await.unwrap();
	ds.commit_coordinator
		.as_ref()
		.expect("the default sync mode must run the commit coordinator")
		.shutdown()
		.unwrap();
	let err = tx.commit().await.expect_err("a commit after coordinator shutdown must be refused");
	assert!(matches!(err, Error::Shutdown), "expected Shutdown, got {err:?}");

	// Nothing was applied: a later reader must not see the write.
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let val = tx.get(b"refused/key".as_slice().into(), None).await.unwrap();
	assert_eq!(val, None, "a refused commit must not have applied anything");
	tx.cancel().await.unwrap();
}

/// The memory reporter must see memtable memory.
///
/// The write buffer manager is constructed against the block cache
/// (cost-to-cache), so RocksDB charges memtable arena bytes into that cache as
/// reservation entries and `Cache::get_usage` covers them. Two properties
/// depend on that and are asserted here:
///
/// - the reporter counts memtables exactly once, by reading the cache alone; adding the write
///   buffer manager's own usage on top would double count.
/// - the charging survives `configure` dropping its local handle on the write buffer manager.
///   `Options` clones it into `OptionsMustOutliveDB` and the opened database stores a clone of
///   that, so the manager outlives the database without this crate holding a handle.
///
/// Writes are never read back, so no data block enters the cache: with a large
/// write buffer nothing flushes, and the growth observed here can only be the
/// memtable reservation.
///
/// Growth is measured on this datastore's own manager rather than through
/// `memory_reporters_allocated_by_name`. Every datastore registers under the
/// name `rocksdb`, so in a shared test binary that map resolves to an
/// arbitrary one of them — including a dropped one, which reports `0`.
#[tokio::test(flavor = "multi_thread")]
async fn memory_reporter_observes_memtable_charge() {
	use common::mem_registry::{MemoryReporter, memory_reporters_allocated_by_name};

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	// A write buffer far larger than the data below so no flush happens and
	// the arena keeps growing, and a block cache with room to hold the
	// resulting reservation.
	let config = RocksDbConfig {
		write_buffer_size: 64 * 1024 * 1024,
		max_write_buffer_number: 4,
		block_cache_size: 64 * 1024 * 1024,
		..RocksDbConfig::default()
	};

	let ds = RocksDbDatastore::new(&path, config).await.unwrap();

	// The datastore must register itself with the global tracker: without it
	// RocksDB memory is invisible to the memory threshold, which no other
	// test would notice. Only the key is asserted, since the value may belong
	// to another datastore in this binary.
	assert!(
		memory_reporters_allocated_by_name().contains_key("rocksdb"),
		"the rocksdb memory reporter must be registered with the global tracker"
	);

	let before = ds.memory_manager.memory_allocated();

	// Write enough to move the arena well past the 256 KiB granularity that
	// RocksDB charges cache reservations in.
	for batch in 0..16u32 {
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for i in 0..256u32 {
			let key: Vec<u8> = format!("memtable_charge_{batch:04}_{i:04}").into_bytes();
			tx.set(key.into(), vec![0u8; 1024]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	let after = ds.memory_manager.memory_allocated();
	assert!(
		after > before,
		"reported RocksDB memory did not grow while memtables filled: {before} -> {after}. \
		 Memtable arena bytes are charged into the block cache, so the cache usage the reporter \
		 reads must rise."
	);

	ds.shutdown().await.unwrap();
}

/// Commit each `(step, mib)` in order, as one transaction of `mib` MiB of
/// 1 KiB values, on a datastore whose memtable history can fill its write
/// buffer budget, and fail if the writers park.
///
/// `MemoryManager::configure` sizes the write buffer manager at
/// `max_write_buffer_number * write_buffer_size + block_cache_size`. An
/// `OptimisticTransactionDB` keeps flushed memtables resident as history for
/// commit-time conflict checking, sized to `max(2, max_write_buffer_number) *
/// write_buffer_size`. A flush moves its memtable into that history rather
/// than freeing it, and the history is trimmed only as later writes grow the
/// live memtable. A manager that stalls writers once memtable memory reaches
/// its budget can therefore park them with nothing left to release the
/// stall: the flush has finished, and only a write can free memory.
///
/// The configuration is small but real: a 16 MiB write buffer, one memtable
/// (which RocksDB raises to two) and a 16 MiB block cache, so the history
/// limit alone matches the 32 MiB budget.
///
/// A parked writer never returns from `commit`, and it may be the tokio
/// worker that drives the runtime's timers. The steps therefore run on their
/// own thread and runtime and report progress over a channel, and the calling
/// thread judges it with a plain `recv_timeout`.
fn assert_commits_are_not_parked(steps: Vec<(String, usize)>) {
	use std::sync::mpsc::{self, RecvTimeoutError};
	use std::time::Duration;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	const MIB: usize = 1024 * 1024;
	/// Orders of magnitude longer than any step takes. A parked writer never
	/// returns, so this bounds only how long a regression takes to report.
	const STALL_TIMEOUT: Duration = Duration::from_secs(60);

	let dir = TempDir::new().unwrap();
	let path = dir.path().to_string_lossy().to_string();
	let config = RocksDbConfig {
		write_buffer_size: 16 * MIB,
		max_write_buffer_number: 1,
		min_write_buffer_number_to_merge: 1,
		block_cache_size: 16 * MIB,
		..RocksDbConfig::default()
	};

	let (progress, observed) = mpsc::channel();
	let workload = std::thread::spawn(move || {
		let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
		runtime.block_on(async move {
			let ds = RocksDbDatastore::new(&path, config).await.unwrap();
			for (step, mib) in steps {
				let tx = ds.transaction(TransactionType::Write).await.unwrap();
				for m in 0..mib {
					for i in 0..1024 {
						let key = format!("stall/{step}/{m:03}/{i:04}").into_bytes();
						tx.set(key.into(), vec![0u8; 1024]).await.unwrap();
					}
					progress.send(format!("staged {} MiB of {step}", m + 1)).unwrap();
				}
				tx.commit().await.unwrap();
				progress.send(format!("committed {step}")).unwrap();
			}
			ds.shutdown().await.unwrap();
		});
	});

	// The channel disconnects once the workload returns or panics.
	let mut last = String::from("opening the datastore");
	loop {
		match observed.recv_timeout(STALL_TIMEOUT) {
			Ok(step) => last = step,
			Err(RecvTimeoutError::Disconnected) => break,
			Err(RecvTimeoutError::Timeout) => {
				panic!("writers are parked: no progress within {STALL_TIMEOUT:?} after {last}")
			}
		}
	}
	if let Err(panic) = workload.join() {
		std::panic::resume_unwind(panic);
	}
}

/// Steady 1 MiB commits, twice the budget in all, fill the history until it
/// and the live memtable reach the budget.
#[test]
fn steady_commits_are_not_parked_by_memtable_history() {
	assert_commits_are_not_parked((0..64).map(|c| (format!("steady-{c:02}"), 1)).collect());
}

/// A commit twice the budget lands in a single memtable, which exceeds the
/// budget on its own. The commits behind it outlast that memtable's flush and
/// its stay in the history, and every one of them must land.
#[test]
fn commits_behind_one_larger_than_the_budget_are_not_parked() {
	let large = std::iter::once(("large".to_string(), 64));
	let behind = (0..48).map(|c| (format!("behind-large-{c:02}"), 1));
	assert_commits_are_not_parked(large.chain(behind).collect());
}

/// Pins the snapshot-lifetime contract documented on `attach_snapshot`: the
/// pointer a `ReadOptions` retains is the transaction-owned snapshot, not the
/// short-lived wrapper that `tx.snapshot()` hands over.
///
/// The distinction is invisible in normal operation, so a `rocksdb` bump that
/// made `ReadOptions` retain the wrapper instead would turn every read in
/// this backend into a use-after-free without failing a single existing test.
/// This test forces the path that reads the pointer back explicitly:
/// `MemTableListVersion::AddIterators` dereferences `ReadOptions::snapshot`
/// for a sequence number while building range-tombstone iterators over
/// *immutable* memtables. The config keeps several of those resident, and the
/// write loop republishes every seeded key so the reader has something to be
/// isolated *from*.
///
/// Reading stale-but-correct data proves the retained pointer still resolves
/// to the transaction's snapshot; a freed one would fault, or yield a garbage
/// sequence number and leak post-snapshot writes into the results.
#[tokio::test(flavor = "multi_thread")]
async fn read_options_snapshot_outlives_the_temporary() {
	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	const KEYS: usize = 512;
	const OVERWRITE_ROUNDS: usize = 4;

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	// Tiny memtables that are *not* eagerly flushed: RocksDB switches a
	// memtable at most once per write batch and only starts a flush once
	// `min_write_buffer_number_to_merge` immutable ones have accumulated. Each
	// round below writes comfortably more than `write_buffer_size`, so
	// `OVERWRITE_ROUNDS` rounds produce roughly that many switches — kept well
	// under the merge threshold so no flush is ever triggered and the resident
	// count cannot land on a boundary. The reader then iterates with several
	// immutable memtables sitting in `MemTableListVersion::memlist_`.
	let config = RocksDbConfig {
		write_buffer_size: 64 * 1024,
		max_write_buffer_number: 16,
		min_write_buffer_number_to_merge: 12,
		..RocksDbConfig::default()
	};
	let ds = RocksDbDatastore::new(&path, config).await.unwrap();

	// Seed the values the reader must keep seeing.
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for k in 0..KEYS {
			let key = format!("snap/key_{k:04}").into_bytes();
			tx.set(key.into(), b"seed".to_vec()).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Open the reader. This is the call that captures the snapshot and stores
	// the resulting `ReadOptions` on the transaction for the rest of its life.
	let reader = ds.transaction(TransactionType::Read).await.unwrap();

	// Churn the same keys behind the reader's back. Every round commits a
	// new version of all `KEYS` keys plus a batch of padding, which switches
	// memtables repeatedly and leaves immutable memtables holding versions
	// newer than the reader's snapshot.
	for round in 0..OVERWRITE_ROUNDS {
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for k in 0..KEYS {
			let key = format!("snap/key_{k:04}").into_bytes();
			let val = format!("overwrite_{round}").into_bytes();
			tx.set(key.into(), val).await.unwrap();
		}
		for k in 0..KEYS {
			let key = format!("pad/{round:02}/key_{k:04}").into_bytes();
			tx.set(key.into(), vec![0u8; 128]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// The assertions below only exercise the `options.snapshot` dereference
	// if immutable memtables are actually resident, so make that a
	// precondition of the test rather than an assumption about the config.
	let immutable = ds
		.db
		.property_int_value("rocksdb.num-immutable-mem-table")
		.expect("rocksdb rejected the num-immutable-mem-table property")
		.expect("num-immutable-mem-table is not reported for this column family");
	assert!(
		immutable > 0,
		"expected resident immutable memtables so the scan reaches \
		 MemTableListVersion::AddIterators, got {immutable}"
	);

	// Point read through the stored `read_options`.
	let one = reader.get(b"snap/key_0000".as_slice().into(), None).await.unwrap();
	assert_eq!(
		one.as_deref(),
		Some(b"seed".as_slice()),
		"point get must observe the transaction's snapshot, not the latest write"
	);

	// Range scan through `scan_read_options` — this is the path that reaches
	// `MemTableListVersion::AddIterators` and dereferences `options.snapshot`.
	let scanned = reader.scan(prefix_byte_range("snap/"), u32::MAX, 0, None).await.unwrap();
	assert_eq!(scanned.values.len(), KEYS, "scan must return every seeded key");
	for (key, val) in &scanned.values {
		assert_eq!(
			val.as_slice(),
			b"seed".as_slice(),
			"scan returned a post-snapshot value for {}",
			String::from_utf8_lossy(key)
		);
	}

	// The padding written after the snapshot must be invisible to the
	// reader, and `count_read_options` must agree with the scan. The ranges
	// are splittable so this goes through the parallel shard path, where each
	// shard iterator carries its own snapshot-bearing `ReadOptions`.
	let padding = reader.count(splittable_prefix_range("pad/"), None).await.unwrap();
	assert_eq!(padding, 0, "count observed keys committed after the snapshot");
	let counted = reader.count(splittable_prefix_range("snap/"), None).await.unwrap();
	assert_eq!(counted, KEYS, "count must agree with the scan under the same snapshot");

	reader.cancel().await.unwrap();
	ds.shutdown().await.unwrap();
}

/// Every read-only iterator in this backend is built from a `ReadOptions`
/// holding the transaction's snapshot, which rocksdb frees when the
/// transaction is destroyed. `cancel` takes the inner transaction out from
/// behind a shared `&self` regardless of `self.write`, so a caller holding the
/// transaction in an `Arc` can finish it while a read is still setting up.
///
/// Each read therefore builds its iterator through `OwnedTx::handle` while the
/// inner lock is held, so the iterator owns an `OwnedTx` borrow and
/// `OwnedTx::into_inner` waits for it rather than freeing the snapshot
/// underneath it. This covers every shape that builds a detached iterator: the
/// sharded `count` fan-out and the serial `count_blocking`, `scan_blocking` and
/// `keys_blocking` helpers.
///
/// The two properties are checked separately, because the interleavings that
/// prove them are opposites.
///
/// *Liveness* — each shape, run with nothing racing it, returns the exact
/// range. This is what shows the path builds a working iterator at all, and it
/// does not depend on scheduling.
///
/// *Safety* — each shape raced against a concurrent `cancel`, which must
/// either lose (exact result) or win (clean `TransactionFinished`), never
/// fault. The race gets no head start on purpose: `ReadOptions::snapshot` is
/// dereferenced only while the iterator is being **constructed**, so the
/// vulnerable window is the gap between releasing the guard and creating the
/// iterator. A `cancel` landing later, mid-walk, is harmless — the iterator
/// already holds its own SuperVersion reference — so delaying the cancel to
/// make the read win would test nothing.
///
/// That window is wide for the fan-out, where the build happens on a worker,
/// and only a few instructions for the serial helpers. Detection is therefore
/// probabilistic while the pass is not: removing a keep-alive is caught on
/// most runs, and correct code passes every run. Lowering `ROUNDS` or `KEYS`
/// weakens the test without making it fail.
#[tokio::test(flavor = "multi_thread")]
async fn reads_survive_a_concurrent_cancel() {
	use std::sync::Arc;

	use surrealdb_kvs::err::Error as KvsError;

	use crate::range_shard::{COUNT_PARALLEL_MAX_SHARDS, shard_range};
	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	const KEYS: usize = 200_000;
	const ROUNDS: usize = 60;

	/// The three read shapes that build a detached iterator in their read-only
	/// branch: `count` (sharded fan-out, or its serial `count_blocking`
	/// fallback), `scan` (`scan_blocking`) and `keys` (`keys_blocking`).
	async fn read(
		tx: &dyn surrealdb_kvs::api::Transactable,
		shape: usize,
	) -> Result<usize, KvsError> {
		let rng = splittable_prefix_range("race/");
		match shape {
			0 => tx.count(rng, None).await,
			1 => tx.scan(rng, u32::MAX, 0, None).await.map(|r| r.values.len()),
			_ => tx.keys(rng, u32::MAX, 0, None).await.map(|r| r.keys.len()),
		}
	}

	const SHAPES: [&str; 3] = ["count", "scan", "keys"];

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = Arc::new(RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap());

	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for k in 0..KEYS {
			let key = format!("race/key_{k:06}").into_bytes();
			tx.set(key.into(), b"v".to_vec()).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Report which `count` path this machine exercises, derived the way
	// `count()` derives it rather than hardcoded: on a single-CPU runner the
	// range yields one shard and `count` takes the serial fallback. Both carry
	// the same keep-alive, so either is meaningful — but they are different
	// code, so a failure should say which ran.
	let rng = splittable_prefix_range("race/");
	let desired = std::thread::available_parallelism()
		.map(|n| n.get())
		.unwrap_or(8)
		.min(COUNT_PARALLEL_MAX_SHARDS);
	let count_shards = shard_range(&rng.start, &rng.end, desired).len();
	let path_note = format!("count uses {count_shards} shard(s) from {desired} desired");

	// Liveness: unraced, every shape must build a working iterator and return
	// the whole range.
	for (shape, name) in SHAPES.iter().enumerate() {
		let tx = ds.transaction(TransactionType::Read).await.unwrap();
		let n = read(tx.as_ref(), shape)
			.await
			.unwrap_or_else(|e| panic!("unraced {name} failed ({path_note}): {e}"));
		assert_eq!(n, KEYS, "unraced {name} returned a partial result ({path_note})");
		tx.cancel().await.unwrap();
	}

	// Safety: race a `cancel` against each shape with no head start.
	for round in 0..ROUNDS {
		let shape = round % SHAPES.len();
		let name = SHAPES[shape];
		let reader = Arc::new(ds.transaction(TransactionType::Read).await.unwrap());
		let worker = Arc::clone(&reader);
		let canceller = Arc::clone(&reader);

		let read_task = tokio::spawn(async move { read(&**worker, shape).await });
		let cancel_task = tokio::spawn(async move { canceller.cancel().await });

		let observed = read_task.await.unwrap();
		let cancelled = cancel_task.await.unwrap();

		match observed {
			// The read won: it must be exact. An iterator built over a released
			// snapshot would fault, or read a garbage sequence number and
			// return the wrong rows.
			Ok(n) => assert_eq!(n, KEYS, "{name} returned a partial result in round {round}"),
			// The cancel won: refusing is correct, faulting is not.
			Err(e) => assert!(
				matches!(e, KvsError::TransactionFinished),
				"{name} failed with an unexpected error in round {round}: {e}"
			),
		}
		assert!(cancelled.is_ok(), "cancel failed in round {round}: {cancelled:?}");
	}

	ds.shutdown().await.unwrap();
}

/// A savepoint rollback while a writable-transaction scan cursor is open must
/// neither panic nor disturb the scan: the cursor carries on from the first
/// entry it had not yielded, in the view left by the rollback.
///
/// Covers both directions and both cursor kinds, since each drives its own
/// fill path, and a write kept from before the savepoint alongside one
/// discarded by it.
#[tokio::test(flavor = "multi_thread")]
async fn cursor_survives_savepoint_rollback() {
	use surrealdb_kvs::Direction;
	use surrealdb_kvs::api::Transactable;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();
	let key = |i: usize| format!("sp/key_{i:04}").into_bytes();
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for i in (0..20).step_by(2) {
			tx.set(key(i).into(), b"v".to_vec()).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	for dir in [Direction::Forward, Direction::Backward] {
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		let tx_ref = tx.as_ref();
		// Kept: written before the savepoint, ahead of the cursor.
		tx.set(key(5).into(), b"v".to_vec()).await.unwrap();
		let mut keys_cursor =
			Transactable::open_keys_cursor(tx_ref, prefix_byte_range("sp/"), dir, 0, None)
				.await
				.unwrap();
		let mut vals_cursor =
			Transactable::open_vals_cursor(tx_ref, prefix_byte_range("sp/"), dir, 0, None)
				.await
				.unwrap();
		let mut seen_keys: Vec<Vec<u8>> = Vec::new();
		let mut seen_vals: Vec<Vec<u8>> = Vec::new();
		for batch in [3, 3] {
			let b = keys_cursor.next_batch(batch).await.unwrap();
			seen_keys.extend(b.iter().map(|k| k.to_vec()));
			let b = vals_cursor.next_batch(batch).await.unwrap();
			seen_vals.extend(b.iter().map(|(k, _)| k.to_vec()));
			if seen_keys.len() == 3 {
				// Discarded: written after the savepoint, ahead of the cursor.
				tx.new_save_point().await.unwrap();
				tx.set(key(7).into(), b"v".to_vec()).await.unwrap();
				tx.set(key(13).into(), b"v".to_vec()).await.unwrap();
				tx.rollback_to_save_point().await.unwrap();
			}
		}
		loop {
			let b = keys_cursor.next_batch(4).await.unwrap();
			if b.is_empty() {
				break;
			}
			seen_keys.extend(b.iter().map(|k| k.to_vec()));
		}
		loop {
			let b = vals_cursor.next_batch(4).await.unwrap();
			if b.is_empty() {
				break;
			}
			seen_vals.extend(b.iter().map(|(k, _)| k.to_vec()));
		}
		let mut expected: Vec<Vec<u8>> =
			(0..20).step_by(2).chain(std::iter::once(5)).map(key).collect();
		expected.sort();
		if dir == Direction::Backward {
			expected.reverse();
		}
		assert_eq!(seen_keys, expected, "keys cursor, {dir:?}");
		assert_eq!(seen_vals, expected, "vals cursor, {dir:?}");
		drop(keys_cursor);
		drop(vals_cursor);
		tx.cancel().await.unwrap();
	}
}

/// A view that opens through another and overrides nothing else, so every other method of
/// [`surrealdb_kvs::Snapshot`] answers by its default.
struct OpenOnly(std::sync::Arc<dyn surrealdb_kvs::Snapshot>);

impl surrealdb_kvs::Snapshot for OpenOnly {
	fn open<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: surrealdb_kvs::Direction,
		version: Option<u64>,
	) -> surrealdb_kvs::api::BoxFut<
		'a,
		surrealdb_kvs::Result<Box<dyn surrealdb_kvs::RangeCursor + 'a>>,
	> {
		self.0.open(rng, dir, version)
	}
}

/// `get_many` of a read view, native and default alike, answers what the transaction's `getm`
/// answers: hits, misses, a key repeated, and the input order, with the counts of `getm`.
///
/// The probes include a key whose successor-by-`0x00` is stored and a key that is the strict
/// prefix of a stored one, which a point read over `[k, k + 0x00)` must not see.
#[tokio::test(flavor = "multi_thread")]
async fn get_many_equals_getm_at_the_views_moment() {
	use surrealdb_kvs::{Key, Snapshot};

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();
	let stored: Vec<(Vec<u8>, Vec<u8>)> = vec![
		(b"gm/k0".to_vec(), b"zero".to_vec()),
		(b"gm/k0\x00".to_vec(), b"zero-successor".to_vec()),
		(b"gm/k1".to_vec(), b"one".to_vec()),
		(b"gm/k1/child".to_vec(), b"one-child".to_vec()),
		(b"gm/k3".to_vec(), Vec::new()),
	];
	{
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for (k, v) in &stored {
			tx.set(Key::from(k.clone()), v.clone()).await.unwrap();
		}
		tx.commit().await.unwrap();
	}
	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let view = tx.snapshot().expect("a read-only transaction gives a view");
	// Committed after the view was taken, so invisible to it and to `getm` alike.
	{
		let writer = ds.transaction(TransactionType::Write).await.unwrap();
		writer.set(Key::from(b"gm/k2".to_vec()), b"late".to_vec()).await.unwrap();
		writer.set(Key::from(b"gm/k1".to_vec()), b"changed".to_vec()).await.unwrap();
		writer.commit().await.unwrap();
	}
	let keys: Vec<Key<'static>> = [
		&b"gm/k3"[..],
		b"gm/k1",
		b"gm/missing",
		b"gm/k0",
		b"gm/k1",
		b"gm/k2",
		b"gm/k1/child",
		b"gm/k0\x00",
		b"gm/k",
	]
	.into_iter()
	.map(|k| Key::from(k.to_vec()))
	.collect();
	let expected = tx.getm(&keys, None).await.unwrap();
	assert_eq!(expected.records, 6);
	let native = view.get_many(&keys, None).await.unwrap();
	let default = OpenOnly(view).get_many(&keys, None).await.unwrap();
	for (name, got) in [("native", native), ("default", default)] {
		assert_eq!(got.values, expected.values, "{name} values");
		assert_eq!(got.records, expected.records, "{name} records");
		assert_eq!(got.value_bytes, expected.value_bytes, "{name} value bytes");
	}
	let none = tx.snapshot().expect("a view").get_many(&[], None).await.unwrap();
	assert!(none.values.is_empty() && none.records == 0 && none.value_bytes == 0);
	tx.cancel().await.unwrap();
}

/// Reads held inside a view's `get_many`, at the hook immediately before the batched read, and
/// how many were held there at once.
mod parked_reads {
	use std::sync::{Arc, Condvar, Mutex};
	use std::time::Duration;

	use futures::FutureExt;
	use surrealdb_kvs::{Error, Key, Snapshot, TransactionType};
	use temp_dir::TempDir;

	use crate::snapshot::hooks;
	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	/// How many reads are parked at once.
	const READERS: usize = 4;

	/// How long a parked read waits to be released before it gives up and reads anyway. It bounds
	/// a run in which fewer than [`READERS`] reads can ever be parked together; a run in which
	/// they can releases them as soon as the last arrives.
	const PARK_LIMIT: Duration = Duration::from_secs(5);

	/// How long the test waits for the reads it started to be parked.
	const ARRIVAL_LIMIT: Duration = Duration::from_secs(20);

	#[derive(Default)]
	struct State {
		/// Reads parked now.
		parked: usize,
		/// The most reads parked at once.
		most: usize,
		/// Whether parked reads may continue.
		released: bool,
		/// Release every read once this many are parked at once.
		release_at: Option<usize>,
	}

	#[derive(Default)]
	struct Parking {
		state: Mutex<State>,
		changed: Condvar,
	}

	impl Parking {
		/// Releasing once `n` reads are parked at once.
		fn releasing_at(n: usize) -> Arc<Self> {
			let parking = Self::default();
			parking.state.lock().unwrap().release_at = Some(n);
			Arc::new(parking)
		}

		/// The hook: park until released, or until [`PARK_LIMIT`] has passed.
		fn park(&self) {
			let mut state = self.state.lock().unwrap();
			state.parked += 1;
			state.most = state.most.max(state.parked);
			if state.release_at == Some(state.parked) {
				state.released = true;
			}
			self.changed.notify_all();
			let (mut state, _) =
				self.changed.wait_timeout_while(state, PARK_LIMIT, |s| !s.released).unwrap();
			state.parked -= 1;
			self.changed.notify_all();
		}

		fn release(&self) {
			self.state.lock().unwrap().released = true;
			self.changed.notify_all();
		}

		/// Whether `n` reads came to be parked at once within [`ARRIVAL_LIMIT`].
		fn wait_parked(&self, n: usize) -> bool {
			let state = self.state.lock().unwrap();
			let (state, _) =
				self.changed.wait_timeout_while(state, ARRIVAL_LIMIT, |s| s.parked < n).unwrap();
			state.parked >= n
		}

		fn most(&self) -> usize {
			self.state.lock().unwrap().most
		}
	}

	fn keys(prefix: &str) -> Vec<Key<'static>> {
		(0..8).map(|i| Key::from(format!("{prefix}{i:02}").into_bytes())).collect()
	}

	async fn seeded(prefix: &str) -> (TempDir, RocksDbDatastore) {
		let dir = TempDir::new().unwrap();
		let path = dir.path().to_string_lossy().to_string();
		let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		// Every other key, so the reads mix hits and misses.
		for key in keys(prefix).into_iter().step_by(2) {
			let val = key.as_slice().to_vec();
			tx.set(key, val).await.unwrap();
		}
		tx.commit().await.unwrap();
		(dir, ds)
	}

	type Read = tokio::task::JoinHandle<surrealdb_kvs::Result<Vec<Option<Vec<u8>>>>>;

	/// Starts [`READERS`] reads of `keys` through `view`, each on a task of its own.
	fn start_reads(view: &Arc<dyn Snapshot>, keys: &[Key<'static>]) -> Vec<Read> {
		(0..READERS)
			.map(|_| {
				let (view, keys) = (Arc::clone(view), keys.to_vec());
				tokio::spawn(async move { Ok(view.get_many(&keys, None).await?.values) })
			})
			.collect()
	}

	/// Point reads through one view do not wait for each other: [`READERS`] callers are all
	/// inside the batched read at once, and each answers what the transaction's `getm` does.
	///
	/// Each parked read blocks the thread it reads on, which is a tokio worker when the read runs
	/// inline, so the runtime has a worker for each of them and for the test.
	#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
	async fn get_many_reads_through_one_view_at_once() {
		let prefix = "parked/at-once/";
		let (_dir, ds) = seeded(prefix).await;
		let tx = ds.transaction(TransactionType::Read).await.unwrap();
		let view = tx.snapshot().expect("a read-only transaction gives a view");
		let keys = keys(prefix);
		let expected = tx.getm(&keys, None).await.unwrap().values;
		let parking = Parking::releasing_at(READERS);
		let hook = Arc::clone(&parking);
		let _registered =
			hooks::before_get_many_with(prefix.as_bytes(), Arc::new(move || hook.park()));
		for read in start_reads(&view, &keys) {
			assert_eq!(read.await.unwrap().unwrap(), expected);
		}
		assert_eq!(parking.most(), READERS, "reads in flight at once through one view");
		tx.cancel().await.unwrap();
	}

	/// A point read through a view runs on the calling task when the runtime has room for it, as
	/// the transaction's own point read does, rather than always hopping to the affinity pool.
	#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
	async fn view_point_reads_run_inline_when_the_runtime_has_room() {
		let prefix = "inline/view/";
		let (_dir, ds) = seeded(prefix).await;
		let tx = ds.transaction(TransactionType::Read).await.unwrap();
		let view = tx.snapshot().expect("a read-only transaction gives a view");
		let keys = keys(prefix);
		let granted = || ds.collect_u64_metric(RocksDbDatastore::INLINE_BLOCKING_GRANTED).unwrap();
		let before = granted();
		for _ in 0..2 {
			view.get_many(&keys, None).await.unwrap();
		}
		assert_eq!(granted() - before, 2, "view point reads granted an inline permit");
		tx.cancel().await.unwrap();
	}

	/// Readers that keep reading through one view while its transaction is cancelled never see a
	/// wrong answer, never keep the cancel waiting, and are all refused once it has completed.
	///
	/// Each round races [`READERS`] tasks, each reading in a loop until refused, against a cancel
	/// issued once every reader has read at least once, so reads start before, during and after
	/// the cancel begins to wait for the transaction. A wake-up lost between a read releasing its
	/// keep-alive and the cancel waiting for it would leave the cancel waiting for ever, which
	/// the time limit turns into a failure.
	#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
	async fn readers_racing_a_cancel_read_correctly_and_let_it_finish() {
		let prefix = "race/cancel/";
		let (_dir, ds) = seeded(prefix).await;
		let keys = keys(prefix);
		for round in 0..32 {
			let tx = ds.transaction(TransactionType::Read).await.unwrap();
			let view = tx.snapshot().expect("a read-only transaction gives a view");
			let expected = tx.getm(&keys, None).await.unwrap().values;
			let started = Arc::new(tokio::sync::Barrier::new(READERS + 1));
			let readers: Vec<_> = (0..READERS)
				.map(|_| {
					let (view, keys, expected, started) =
						(Arc::clone(&view), keys.clone(), expected.clone(), Arc::clone(&started));
					tokio::spawn(async move {
						let mut reads = 0usize;
						loop {
							match view.get_many(&keys, None).await {
								Ok(got) => {
									assert_eq!(got.values, expected, "round {round}");
									reads += 1;
									if reads == 1 {
										started.wait().await;
									}
								}
								Err(Error::TransactionFinished) if reads > 0 => return reads,
								Err(e) => panic!("round {round}: a read answered {e:?}"),
							}
							tokio::task::yield_now().await;
						}
					})
				})
				.collect();
			started.wait().await;
			tokio::time::timeout(ARRIVAL_LIMIT, tx.cancel())
				.await
				.expect("the cancel waited on readers for ever")
				.unwrap();
			let later = view.get_many(&keys, None).await;
			assert!(
				matches!(later, Err(Error::TransactionFinished)),
				"round {round}: a read after the cancel answered {later:?}"
			);
			for reader in readers {
				let reads = tokio::time::timeout(ARRIVAL_LIMIT, reader)
					.await
					.expect("a reader was never refused")
					.unwrap();
				assert!(reads > 0, "round {round}");
			}
		}
	}

	/// Finishing the transaction waits for the reads already inside a view's `get_many`, and a
	/// read started after the finish fails with `TransactionFinished` without waiting for them.
	#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
	async fn finishing_waits_for_reads_in_flight_and_refuses_later_ones() {
		for commit in [false, true] {
			let prefix = if commit {
				"parked/commit/"
			} else {
				"parked/cancel/"
			};
			let (_dir, ds) = seeded(prefix).await;
			let tt = if commit {
				TransactionType::Write
			} else {
				TransactionType::Read
			};
			let tx = ds.transaction(tt).await.unwrap();
			let view = tx.snapshot().expect("a transaction with no buffered write gives a view");
			let keys = keys(prefix);
			let expected = tx.getm(&keys, None).await.unwrap().values;
			let parking = Arc::new(Parking::default());
			let hook = Arc::clone(&parking);
			let registered =
				hooks::before_get_many_with(prefix.as_bytes(), Arc::new(move || hook.park()));
			let reads = start_reads(&view, &keys);
			let all_parked = tokio::task::block_in_place(|| parking.wait_parked(READERS));
			assert!(all_parked, "commit = {commit}: {} reads parked at once", parking.most());

			let mut finish = if commit {
				tx.commit()
			} else {
				tx.cancel()
			};
			for round in 0..32 {
				assert!(
					finish.as_mut().now_or_never().is_none(),
					"commit = {commit}: finished at round {round} with reads in flight"
				);
				tokio::task::yield_now().await;
			}
			let later = tokio::time::timeout(ARRIVAL_LIMIT, view.get_many(&keys, None))
				.await
				.expect("a read after the finish waited for the reads in flight");
			assert!(
				matches!(later, Err(Error::TransactionFinished)),
				"commit = {commit}: a read after the finish answered {later:?}"
			);

			parking.release();
			drop(registered);
			for read in reads {
				assert_eq!(read.await.unwrap().unwrap(), expected, "commit = {commit}");
			}
			tokio::time::timeout(ARRIVAL_LIMIT, finish)
				.await
				.expect("the finish did not complete once the reads had")
				.unwrap();
		}
	}
}

/// Conformance of the read view a writable transaction gives while it has buffered no put or
/// delete. The oracle is the transaction's own `get`, `getm`, `scan` and `scanr` over the test
/// range, taken immediately before `snapshot()`.
mod write_view {
	use std::ops::ControlFlow;
	use std::sync::Arc;
	use std::time::Duration;

	use rand::rngs::StdRng;
	use rand::{RngCore, SeedableRng};
	use surrealdb_kvs::api::Transactable;
	use surrealdb_kvs::err::{Error, Result};
	use surrealdb_kvs::snapshot::ViewCapabilities;
	use surrealdb_kvs::{Direction, Key, KeyRange, Page, Snapshot, TransactionType};
	use temp_dir::TempDir;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	type Entries = Vec<(Vec<u8>, Vec<u8>)>;

	fn key(i: usize) -> Vec<u8> {
		format!("wv/{i:05}").into_bytes()
	}

	fn val(i: usize, tag: &str) -> Vec<u8> {
		format!("{tag}-{i:05}").into_bytes()
	}

	fn range(lo: &[u8], hi: &[u8]) -> KeyRange<'static> {
		KeyRange {
			start: lo.to_vec().into(),
			end: hi.to_vec().into(),
		}
	}

	/// Every key under `wv/`.
	fn whole() -> KeyRange<'static> {
		super::prefix_byte_range("wv/")
	}

	async fn open_ds(config: RocksDbConfig) -> RocksDbDatastore {
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		RocksDbDatastore::new(&path, config).await.unwrap()
	}

	/// Commit `n` keys `wv/00000..` with the value `seed-<i>`.
	async fn seed(ds: &RocksDbDatastore, n: usize) {
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for i in 0..n {
			tx.set(key(i).into(), val(i, "seed")).await.unwrap();
		}
		tx.commit().await.unwrap();
	}

	/// What the transaction itself reads over `rng` and `keys`.
	struct Record {
		fwd: Entries,
		bwd: Entries,
		many: Vec<Option<Vec<u8>>>,
	}

	async fn record(
		tx: &dyn Transactable,
		rng: &KeyRange<'static>,
		keys: &[Key<'static>],
	) -> Record {
		let fwd = tx.scan(rng.clone(), u32::MAX, 0, None).await.unwrap().values;
		let bwd = tx.scanr(rng.clone(), u32::MAX, 0, None).await.unwrap().values;
		let many = tx.getm(keys, None).await.unwrap().values;
		for (k, v) in keys.iter().zip(&many) {
			assert_eq!(&tx.get(k.clone(), None).await.unwrap(), v, "get equals getm");
		}
		Record {
			fwd,
			bwd,
			many,
		}
	}

	/// Probe keys over `0..n` plus misses, shuffled by `seed`.
	fn probes(n: usize, seed: u64) -> Vec<Key<'static>> {
		let mut keys: Vec<Vec<u8>> = (0..n).map(key).collect();
		keys.push(b"wv/missing".to_vec());
		keys.push(b"wv/".to_vec());
		keys.push(key(0).into_iter().chain([0u8]).collect());
		keys.push(key(n / 2));
		let mut rng = StdRng::seed_from_u64(seed);
		for i in (1..keys.len()).rev() {
			let j = (rng.next_u32() as usize) % (i + 1);
			keys.swap(i, j);
		}
		keys.into_iter().map(Key::from).collect()
	}

	async fn drain_visit(
		view: &dyn Snapshot,
		rng: KeyRange<'static>,
		dir: Direction,
		limit: u32,
	) -> Entries {
		let mut out = Entries::new();
		let mut cursor = view.open(rng, dir, None).await.unwrap();
		let mut f = |k: &[u8], v: &[u8]| -> Result<ControlFlow<()>> {
			out.push((k.to_vec(), v.to_vec()));
			Ok(ControlFlow::Continue(()))
		};
		loop {
			let stats = cursor.for_each(limit, &mut f).await.unwrap();
			if stats.rows == 0 {
				break;
			}
		}
		drop(cursor);
		out
	}

	async fn drain_fill(view: &dyn Snapshot, rng: KeyRange<'static>, dir: Direction) -> Entries {
		let mut out = Entries::new();
		let mut cursor = view.open(rng, dir, None).await.unwrap();
		let mut page = Page::with_capacity(64);
		loop {
			cursor.fill(&mut page, u32::MAX).await.unwrap();
			if page.is_empty() {
				break;
			}
			let batch = page.batch();
			for i in 0..page.len() {
				let (k, v) = batch.get(i).unwrap();
				out.push((k.to_vec(), v.to_vec()));
			}
		}
		out
	}

	/// The view answers exactly what `rec` recorded: `get_many`, and cursors in both directions
	/// with limits 1, 7 and `u32::MAX`, and `fill` with a 64-byte page.
	async fn assert_view(
		view: &dyn Snapshot,
		rng: &KeyRange<'static>,
		keys: &[Key<'static>],
		rec: &Record,
		what: &str,
	) {
		let got = view.get_many(keys, None).await.unwrap();
		assert_eq!(got.values, rec.many, "{what}: get_many values");
		let hits = rec.many.iter().flatten().collect::<Vec<_>>();
		assert_eq!(got.records, hits.len() as u64, "{what}: get_many records");
		assert_eq!(
			got.value_bytes,
			hits.iter().map(|v| v.len() as u64).sum::<u64>(),
			"{what}: get_many value bytes"
		);
		for (dir, expected) in [(Direction::Forward, &rec.fwd), (Direction::Backward, &rec.bwd)] {
			for limit in [1, 7, u32::MAX] {
				let got = drain_visit(view, rng.clone(), dir, limit).await;
				assert_eq!(&got, expected, "{what}: {dir:?} cursor, limit {limit}");
			}
			let got = drain_fill(view, rng.clone(), dir).await;
			assert_eq!(&got, expected, "{what}: {dir:?} fill");
		}
	}

	/// Overwrite, delete and insert keys inside and outside the range of `whole()`.
	async fn scribble(tx: &dyn Transactable, round: usize) {
		for i in (0..200).step_by(3) {
			tx.set(key(i).into(), val(i, &format!("over{round}"))).await.unwrap();
		}
		for i in (1..200).step_by(5) {
			tx.del(key(i).into()).await.unwrap();
		}
		for i in 0..20 {
			tx.set(format!("wv/{:05}x", i * 10).into_bytes().into(), b"new".to_vec())
				.await
				.unwrap();
		}
		tx.set(b"aa/outside".to_vec().into(), b"o".to_vec()).await.unwrap();
		tx.set(b"zz/outside".to_vec().into(), b"o".to_vec()).await.unwrap();
		tx.del(b"zz/gone".to_vec().into()).await.unwrap();
	}

	fn assert_finished<T>(res: &Result<T>, what: &str) {
		assert!(
			matches!(res, Err(Error::TransactionFinished)),
			"{what}: expected TransactionFinished"
		);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn wv9_a_writable_transaction_with_no_write_gives_a_view() {
		let ds = open_ds(RocksDbConfig::default()).await;
		seed(&ds, 10).await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		let view = tx.snapshot().expect("a writable transaction with no write gives a view");
		assert_eq!(view.held_bytes(), 0);
		tx.cancel().await.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn wv2_later_writes_are_invisible_to_a_tier1_view() {
		let ds = open_ds(RocksDbConfig::default()).await;
		seed(&ds, 200).await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		let keys = probes(200, 2);
		let rec = record(&*tx, &whole(), &keys).await;
		let view = tx.snapshot().expect("a view before the first write");
		scribble(&*tx, 0).await;
		assert!(tx.snapshot().is_none(), "a transaction with buffered writes gives no view");
		assert_view(&*view, &whole(), &keys, &rec, "after the owner wrote").await;
		scribble(&*tx, 1).await;
		assert_view(&*view, &whole(), &keys, &rec, "after the owner wrote again").await;
		drop(view);
		tx.cancel().await.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn wv3_savepoint_rollback_is_invisible() {
		let ds = open_ds(RocksDbConfig::default()).await;
		seed(&ds, 200).await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		let keys = probes(200, 3);
		let rec = record(&*tx, &whole(), &keys).await;
		let view = tx.snapshot().expect("a view before the first write");
		tx.new_save_point().await.unwrap();
		scribble(&*tx, 0).await;
		assert_view(&*view, &whole(), &keys, &rec, "inside the savepoint").await;
		tx.rollback_to_save_point().await.unwrap();
		assert_view(&*view, &whole(), &keys, &rec, "after the rollback").await;
		scribble(&*tx, 1).await;
		assert_view(&*view, &whole(), &keys, &rec, "after writing again").await;
		drop(view);
		tx.cancel().await.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn wv4_other_commits_are_invisible() {
		let ds = open_ds(RocksDbConfig::default()).await;
		seed(&ds, 200).await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		let keys = probes(200, 4);
		let rec = record(&*tx, &whole(), &keys).await;
		let view = tx.snapshot().expect("a view before the first write");
		let other = ds.transaction(TransactionType::Write).await.unwrap();
		scribble(&*other, 0).await;
		other.commit().await.unwrap();
		assert_view(&*view, &whole(), &keys, &rec, "after another transaction committed").await;
		drop(view);
		tx.cancel().await.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn wv5_concurrent_with_the_owner() {
		let ds = open_ds(RocksDbConfig::default()).await;
		seed(&ds, 400).await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		let mut tasks = Vec::new();
		let mut views: Vec<Arc<dyn Snapshot>> = Vec::new();
		for q in 0..4 {
			let rng = range(&key(q * 100), &key((q + 1) * 100));
			let keys: Vec<Key<'static>> = probes(400, 50 + q as u64)
				.into_iter()
				.filter(|k| {
					rng.start.as_slice() <= k.as_slice() && k.as_slice() < rng.end.as_slice()
				})
				.collect();
			let rec = record(&*tx, &rng, &keys).await;
			views.push(tx.snapshot().expect("a view before the first write"));
			let view = Arc::clone(&views[q]);
			tasks.push(tokio::spawn(async move {
				for round in 0..5 {
					assert_view(&*view, &rng, &keys, &rec, &format!("quarter {q}, round {round}"))
						.await;
					tokio::task::yield_now().await;
				}
			}));
		}
		for chunk in 0..10usize {
			tx.new_save_point().await.unwrap();
			for i in 0..1000usize {
				let n = chunk * 1000 + i;
				tx.set(format!("wv/{:05}w{n}", n % 400).into_bytes().into(), b"w".to_vec())
					.await
					.unwrap();
				if i % 100 == 0 {
					tokio::task::yield_now().await;
				}
			}
			if chunk % 2 == 1 {
				tx.rollback_to_save_point().await.unwrap();
			}
		}
		for task in tasks {
			task.await.unwrap();
		}
		drop(views);
		tx.cancel().await.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn wv6_keep_alive() {
		for commit in [true, false] {
			let ds = open_ds(RocksDbConfig::default()).await;
			seed(&ds, 20).await;
			let tx: Arc<dyn Transactable> =
				Arc::from(ds.transaction(TransactionType::Write).await.unwrap());
			let view = tx.snapshot().expect("a view before the first write");
			let mut cursor = view.open(whole(), Direction::Forward, None).await.unwrap();
			let mut page = Page::with_capacity(64);
			cursor.fill(&mut page, 3).await.unwrap();
			assert_eq!(page.len(), 3);
			let finishing = {
				let tx = Arc::clone(&tx);
				tokio::spawn(async move {
					if commit {
						tx.commit().await
					} else {
						tx.cancel().await
					}
				})
			};
			for _ in 0..20 {
				tokio::time::sleep(Duration::from_millis(5)).await;
				assert!(!finishing.is_finished(), "finish completed while a cursor was alive");
			}
			// The cursor stays readable while the finish waits.
			// Each fill clears the page and stops before the first entry it cannot hold, so the
			// remaining entries arrive over several fills.
			let mut drained = 0;
			loop {
				cursor.fill(&mut page, u32::MAX).await.unwrap();
				if page.is_empty() {
					break;
				}
				drained += page.len();
			}
			assert_eq!(drained, 17);
			drop(cursor);
			tokio::time::timeout(Duration::from_secs(5), finishing)
				.await
				.expect("finish did not complete after the cursor was dropped")
				.unwrap()
				.unwrap();
			assert_finished(&view.open(whole(), Direction::Forward, None).await, "open");
			assert_finished(&view.get_many(&[Key::from(key(0))], None).await, "get_many");
			assert_finished(&view.get_many(&[], None).await, "get_many of no keys");
		}
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn wv8_versioned() {
		let ds = open_ds(RocksDbConfig {
			versioned: true,
			..RocksDbConfig::default()
		})
		.await;
		seed(&ds, 10).await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		assert!(tx.snapshot().is_some(), "a versioned transaction with no write gives a view");
		tx.set(key(3).into(), b"changed".to_vec()).await.unwrap();
		assert!(tx.snapshot().is_none(), "a versioned transaction with a write gives no view");
		tx.cancel().await.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn wv15_delete_only_is_not_tier1() {
		let ds = open_ds(RocksDbConfig::default()).await;
		seed(&ds, 10).await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		assert!(tx.snapshot().is_some());
		for i in 0..10 {
			tx.del(key(i).into()).await.unwrap();
		}
		assert!(tx.snapshot().is_none(), "a delete-only transaction would expose what it deleted");
		tx.cancel().await.unwrap();
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn wv13_view_reads_are_untracked() {
		let ds = open_ds(RocksDbConfig::default()).await;
		seed(&ds, 10).await;
		let reader = ds.transaction(TransactionType::Write).await.unwrap();
		assert!(!reader.plain_reads_tracked());
		let view = reader.snapshot().expect("a view before the first write");
		let a = Key::from(key(1));
		let got = view.get_many(std::slice::from_ref(&a), None).await.unwrap();
		assert_eq!(got.values, vec![Some(val(1, "seed"))]);
		reader.set(key(5).into(), b"b".to_vec()).await.unwrap();
		let writer = ds.transaction(TransactionType::Write).await.unwrap();
		writer.set(key(1).into(), b"a".to_vec()).await.unwrap();
		writer.commit().await.unwrap();
		drop(view);
		reader.commit().await.expect("a read through the view joins no conflict set");
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn rocksdb_view_capabilities() {
		let ds = open_ds(RocksDbConfig::default()).await;
		seed(&ds, 10).await;
		let both = ViewCapabilities {
			read_only: true,
			writable: true,
		};
		let read = ds.transaction(TransactionType::Read).await.unwrap();
		assert_eq!(read.view_capabilities(), both);
		assert!(read.snapshot().is_some());
		read.cancel().await.unwrap();
		let write = ds.transaction(TransactionType::Write).await.unwrap();
		assert_eq!(write.view_capabilities(), both);
		assert!(write.snapshot().is_some(), "a writable transaction with no write gives a view");
		write.cancel().await.unwrap();
	}
}

/// Open a cursor over `wr/` on a writable transaction whose entries in the
/// range are all pending writes, and write outside the range between every
/// batch. Returns what the cursor yielded.
///
/// Because every entry is pending, after each batch the iterator is parked on
/// an entry that lives in the transaction's write batch. That batch is one
/// growing buffer; the writes between batches reallocate it under the parked
/// iterator.
async fn scan_with_writes_between_batches(
	vals: bool,
	dir: surrealdb_kvs::Direction,
) -> Vec<(Vec<u8>, Vec<u8>)> {
	use surrealdb_kvs::api::Transactable;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	const COUNT: usize = 40;
	let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
	let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	let tx_ref = tx.as_ref();
	for i in 0..COUNT {
		let value = format!("{i:04}").repeat(16).into_bytes();
		tx.set(format!("wr/key_{i:04}").into_bytes().into(), value).await.unwrap();
	}
	let mut seen = Vec::new();
	let mut keys_cursor = None;
	let mut vals_cursor = None;
	if vals {
		vals_cursor = Some(
			Transactable::open_vals_cursor(tx_ref, prefix_byte_range("wr/"), dir, 0, None)
				.await
				.unwrap(),
		);
	} else {
		keys_cursor = Some(
			Transactable::open_keys_cursor(tx_ref, prefix_byte_range("wr/"), dir, 0, None)
				.await
				.unwrap(),
		);
	}
	for round in 0.. {
		let before = seen.len();
		if let Some(c) = vals_cursor.as_mut() {
			let b = c.next_batch(5).await.unwrap();
			seen.extend(b.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
		}
		if let Some(c) = keys_cursor.as_mut() {
			let b = c.next_batch(5).await.unwrap();
			seen.extend(b.iter().map(|k| (k.to_vec(), Vec::new())));
		}
		if seen.len() == before {
			break;
		}
		for j in 0..50 {
			let key = format!("other/{round:03}_{j:03}").into_bytes();
			tx.set(key.into(), vec![b'x'; 64]).await.unwrap();
		}
	}
	drop(keys_cursor);
	drop(vals_cursor);
	tx.cancel().await.unwrap();
	seen
}

/// Writes made between batches of an open writable-transaction scan cursor
/// must not disturb what a key+value cursor goes on to yield, in either
/// direction.
#[tokio::test(flavor = "multi_thread")]
async fn vals_cursor_survives_writes_between_batches() {
	use surrealdb_kvs::Direction;

	for dir in [Direction::Forward, Direction::Backward] {
		let mut expected: Vec<usize> = (0..40).collect();
		if dir == Direction::Backward {
			expected.reverse();
		}
		let expected: Vec<(Vec<u8>, Vec<u8>)> = expected
			.into_iter()
			.map(|i| {
				(format!("wr/key_{i:04}").into_bytes(), format!("{i:04}").repeat(16).into_bytes())
			})
			.collect();
		assert_eq!(scan_with_writes_between_batches(true, dir).await, expected, "{dir:?}");
	}
}

/// As [`vals_cursor_survives_writes_between_batches`], for a keys-only
/// cursor.
#[tokio::test(flavor = "multi_thread")]
async fn keys_cursor_survives_writes_between_batches() {
	use surrealdb_kvs::Direction;

	for dir in [Direction::Forward, Direction::Backward] {
		let mut expected: Vec<usize> = (0..40).collect();
		if dir == Direction::Backward {
			expected.reverse();
		}
		let expected: Vec<(Vec<u8>, Vec<u8>)> = expected
			.into_iter()
			.map(|i| (format!("wr/key_{i:04}").into_bytes(), Vec::new()))
			.collect();
		assert_eq!(scan_with_writes_between_batches(false, dir).await, expected, "{dir:?}");
	}
}

/// Between every batch of an open writable-transaction vals cursor, overwrite
/// the entry the iterator is parked on with a longer value and delete the
/// entry after it, in either direction. The cursor must yield the parked entry
/// with the value written while it was parked, and never the deleted entry.
///
/// Every write lands at or beyond the parked entry, so nothing the cursor has
/// already yielded changes afterwards: what it yields is the range as the
/// transaction finally holds it.
#[tokio::test(flavor = "multi_thread")]
async fn vals_cursor_rereads_the_entry_written_under_it() {
	use std::collections::BTreeMap;

	use surrealdb_kvs::Direction;
	use surrealdb_kvs::api::Transactable;

	use crate::{Datastore as RocksDbDatastore, RocksDbConfig};

	for dir in [Direction::Forward, Direction::Backward] {
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		let ds = RocksDbDatastore::new(&path, RocksDbConfig::default()).await.unwrap();
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		let mut model = BTreeMap::new();
		for i in 0..40 {
			let key = format!("wr/key_{i:04}").into_bytes();
			let value = format!("{i:04}").repeat(16).into_bytes();
			tx.set(key.clone().into(), value.clone()).await.unwrap();
			model.insert(key, value);
		}
		let mut cursor =
			Transactable::open_vals_cursor(tx.as_ref(), prefix_byte_range("wr/"), dir, 0, None)
				.await
				.unwrap();
		let mut seen: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		loop {
			let batch = cursor.next_batch(5).await.unwrap();
			let before = seen.len();
			seen.extend(batch.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
			let Some((last, _)) = seen.last().filter(|_| seen.len() > before) else {
				break;
			};
			// The entries after `last` in scan order: the parked entry first.
			let ahead: Vec<Vec<u8>> = match dir {
				Direction::Forward => model
					.range::<Vec<u8>, _>((
						std::ops::Bound::Excluded(last),
						std::ops::Bound::Unbounded,
					))
					.take(2)
					.map(|(k, _)| k.clone())
					.collect(),
				Direction::Backward => model
					.range::<Vec<u8>, _>(..last)
					.rev()
					.take(2)
					.map(|(k, _)| k.clone())
					.collect(),
			};
			if let Some(parked) = ahead.first() {
				let value = b"rewritten".repeat(64);
				tx.set(parked.clone().into(), value.clone()).await.unwrap();
				model.insert(parked.clone(), value);
			}
			if let Some(after) = ahead.get(1) {
				tx.del(after.clone().into()).await.unwrap();
				model.remove(after);
			}
		}
		drop(cursor);
		tx.cancel().await.unwrap();
		let mut expected: Vec<(Vec<u8>, Vec<u8>)> = model.into_iter().collect();
		if dir == Direction::Backward {
			expected.reverse();
		}
		assert_eq!(seen, expected, "{dir:?}");
	}
}
