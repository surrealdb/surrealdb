//! Read views, range cursors, estimate and split.
//!
//! `split` and `estimate` are answered by every backend, natively or from the defaults, and the
//! tests of them run against all of them. A view is offered only by a backend with a native one,
//! and only for a read-only transaction; the tests of the cursors it opens run against RocksDB.

use std::ops::ControlFlow;

use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::{
	Direction, Estimate, Key, KeyRange, Page, RangeCursor, SplitTarget, Transactable,
};

use crate::{TestBackend, TestDs, kvs_test};

/// The width of an id: four big-endian bytes.
const ID_BYTES: usize = 4;

/// The key of the `i`th of `of` ids spread evenly over the whole id space, under `prefix`.
///
/// Evenly spread ids are what a generated record id looks like to a splitter, so the keys divide
/// and extrapolate as a table's would.
fn id_key(prefix: &[u8], i: u32, of: u32) -> Vec<u8> {
	let step = u32::MAX / of.max(1);
	let mut key = prefix.to_vec();
	key.extend_from_slice(&(i * step).to_be_bytes());
	debug_assert_eq!(key.len(), prefix.len() + ID_BYTES);
	key
}

/// The value stored under every key: sixteen bytes.
fn value() -> Vec<u8> {
	vec![0xAB; 16]
}

/// The range of every key under `prefix`.
fn range_of(prefix: &[u8]) -> KeyRange<'static> {
	Key::from(prefix.to_vec()).prefix_expect()
}

/// Commit `count` evenly spread keys under `prefix`.
async fn seed(ds: &TestDs, prefix: &[u8], count: u32) {
	let tx = ds.transaction(Write).await.unwrap();
	for i in 0..count {
		tx.set(id_key(prefix, i, count).into(), value()).await.unwrap();
	}
	tx.commit().await.unwrap();
}

/// The keys `cursor` visits, up to `limit`.
async fn keys_of<C: RangeCursor + ?Sized>(cursor: &mut C, limit: u32) -> Vec<Vec<u8>> {
	let mut keys = Vec::new();
	cursor
		.for_each(limit, &mut |key, _| {
			keys.push(key.to_vec());
			Ok(ControlFlow::Continue(()))
		})
		.await
		.unwrap();
	keys
}

/// Every entry of `rng` in `dir`, read through the transaction's view when it has one and through
/// its own reads when it has not.
async fn read_all(
	tx: &dyn Transactable,
	rng: &KeyRange<'_>,
	dir: Direction,
) -> Vec<(Vec<u8>, Vec<u8>)> {
	match tx.snapshot() {
		Some(view) => {
			let mut cursor = view.open(rng.as_borrowed(), dir, None).await.unwrap();
			let mut entries = Vec::new();
			cursor
				.for_each(u32::MAX, &mut |key, val| {
					entries.push((key.to_vec(), val.to_vec()));
					Ok(ControlFlow::Continue(()))
				})
				.await
				.unwrap();
			entries
		}
		None => match dir {
			Direction::Forward => {
				tx.scan(rng.as_borrowed(), u32::MAX, 0, None).await.unwrap().values
			}
			Direction::Backward => {
				tx.scanr(rng.as_borrowed(), u32::MAX, 0, None).await.unwrap().values
			}
		},
	}
}

/// The parts `bounds` cut `rng` into, in ascending order.
fn parts_of(rng: &KeyRange<'_>, bounds: &[Key<'static>]) -> Vec<KeyRange<'static>> {
	let mut edges = vec![rng.start.clone().into_static()];
	edges.extend(bounds.iter().cloned());
	edges.push(rng.end.clone().into_static());
	edges
		.windows(2)
		.map(|pair| KeyRange {
			start: pair[0].clone(),
			end: pair[1].clone(),
		})
		.collect()
}

/// Boundaries are strictly inside the range and strictly ascending, whatever the range holds and
/// whatever target is asked for.
async fn split_answers_boundaries_strictly_inside_and_ascending(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"one/", 1).await;
	seed(&ds, b"many/", 1000).await;
	let tx = ds.transaction(Read).await.unwrap();
	for (prefix, keys) in [(&b"empty/"[..], 0), (&b"one/"[..], 1), (&b"many/"[..], 1000)] {
		let rng = range_of(prefix);
		for target in [SplitTarget::Parts(1), SplitTarget::Parts(8), SplitTarget::Bytes(1)] {
			let bounds = tx.split(rng.as_borrowed(), target, None).await.unwrap();
			let ctx = format!("{keys} key(s), {target:?}");
			for key in &bounds {
				assert!(rng.start < *key && *key < rng.end, "{key} is outside the range: {ctx}");
			}
			for pair in bounds.windows(2) {
				assert!(pair[0] < pair[1], "boundaries do not ascend: {ctx}");
			}
			if keys <= 1 || target == SplitTarget::Parts(1) {
				assert!(bounds.is_empty(), "nothing to divide, yet cut at {bounds:?}: {ctx}");
			}
			if target == SplitTarget::Parts(8) {
				assert!(bounds.len() < 8, "{} boundaries make more than 8 parts", bounds.len());
			}
		}
	}
	// A range with no extent divides into nothing.
	let inverted = KeyRange {
		start: Key::from(&b"z"[..]),
		end: Key::from(&b"a"[..]),
	};
	assert!(tx.split(inverted, SplitTarget::Parts(8), None).await.unwrap().is_empty());
	tx.cancel().await.unwrap();
}

kvs_test!(split_answers_boundaries_strictly_inside_and_ascending);

/// The parts a split cuts hold every key of the range once, in either direction.
async fn parts_of_a_split_tile_the_range_exactly(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"tile/", 1000).await;
	let tx = ds.transaction(Read).await.unwrap();
	let rng = range_of(b"tile/");
	let bounds = tx.split(rng.as_borrowed(), SplitTarget::Parts(8), None).await.unwrap();
	assert!(!bounds.is_empty(), "a thousand evenly spread keys divide");
	let parts = parts_of(&rng, &bounds);

	let whole = read_all(&*tx, &rng, Direction::Forward).await;
	assert_eq!(whole.len(), 1000);
	let mut joined = Vec::new();
	for part in &parts {
		joined.extend(read_all(&*tx, part, Direction::Forward).await);
	}
	assert_eq!(joined, whole, "forward parts do not concatenate to the whole scan");

	let whole = read_all(&*tx, &rng, Direction::Backward).await;
	let mut joined = Vec::new();
	for part in parts.iter().rev() {
		joined.extend(read_all(&*tx, part, Direction::Backward).await);
	}
	assert_eq!(joined, whole, "backward parts do not concatenate to the whole scan");
	tx.cancel().await.unwrap();
}

kvs_test!(parts_of_a_split_tile_the_range_exactly);

/// A range within the budget is counted; a larger one is extrapolated from the budget's worth.
async fn estimate_is_exact_within_the_budget(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"small/", 100).await;
	seed(&ds, b"large/", 5000).await;
	let tx = ds.transaction(Read).await.unwrap();

	let small = tx.estimate(range_of(b"small/"), None).await.unwrap();
	let key_bytes = (b"small/".len() + ID_BYTES) as u64;
	assert_eq!(
		small,
		Estimate {
			keys: 100,
			bytes: 100 * (key_bytes + value().len() as u64),
			exact: true,
		}
	);

	let large = tx.estimate(range_of(b"large/"), None).await.unwrap();
	assert!(!large.exact);
	assert!((2500..=10_000).contains(&large.keys), "{} keys estimated for 5000 held", large.keys);
	tx.cancel().await.unwrap();
}

kvs_test!(estimate_is_exact_within_the_budget);

/// A cursor reads what its transaction began with, whatever commits after.
async fn a_view_does_not_see_later_commits(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"later/", 100).await;
	let rng = range_of(b"later/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().expect("a read-only transaction gives a view");

	let writer = ds.transaction(Write).await.unwrap();
	for i in 0..10u32 {
		// Between the seeded ids, so the new keys fall inside the range being read.
		let mut key = id_key(b"later/", i, 100);
		key.push(1);
		writer.set(key.into(), value()).await.unwrap();
	}
	writer.commit().await.unwrap();

	let mut cursor = view.open(rng.as_borrowed(), Direction::Forward, None).await.unwrap();
	assert_eq!(keys_of(&mut *cursor, u32::MAX).await.len(), 100);
	drop(cursor);
	tx.cancel().await.unwrap();

	let fresh = ds.transaction(Read).await.unwrap();
	assert_eq!(fresh.scan(rng, u32::MAX, 0, None).await.unwrap().values.len(), 110);
	fresh.cancel().await.unwrap();
}

kvs_test!(a_view_does_not_see_later_commits, only = [rocksdb]);

/// A cursor stopped part-way, re-aimed at what it says remains, reads the rest and nothing twice.
async fn a_cursor_resumes_where_it_stopped(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"resume/", 50).await;
	let rng = range_of(b"resume/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	for dir in [Direction::Forward, Direction::Backward] {
		let mut whole = view.open(rng.as_borrowed(), dir, None).await.unwrap();
		let expected = keys_of(&mut *whole, u32::MAX).await;
		assert_eq!(expected.len(), 50);

		let mut cursor = view.open(rng.as_borrowed(), dir, None).await.unwrap();
		let mut seen = Vec::new();
		cursor
			.for_each(u32::MAX, &mut |key, _| {
				seen.push(key.to_vec());
				Ok(if seen.len() == 7 {
					ControlFlow::Break(())
				} else {
					ControlFlow::Continue(())
				})
			})
			.await
			.unwrap();
		assert_eq!(seen.len(), 7, "{dir:?}");
		let resume = cursor.resume().expect("the range is not exhausted");
		cursor.aim(resume.remainder(&rng, dir), dir).unwrap();
		seen.extend(keys_of(&mut *cursor, u32::MAX).await);
		assert_eq!(seen, expected, "{dir:?}: the stopped and resumed reads do not make the whole");
		assert!(cursor.resume().is_none(), "{dir:?}: an exhausted cursor has nowhere to resume");
	}
	tx.cancel().await.unwrap();
}

kvs_test!(a_cursor_resumes_where_it_stopped, only = [rocksdb]);

/// The entry a visitor answers `Break` for is spent: the cursor goes on after it.
async fn a_break_consumes_the_entry_it_stops_on(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"brk/", 20).await;
	let rng = range_of(b"brk/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	for dir in [Direction::Forward, Direction::Backward] {
		let mut whole = view.open(rng.as_borrowed(), dir, None).await.unwrap();
		let expected = keys_of(&mut *whole, u32::MAX).await;

		let mut cursor = view.open(rng.as_borrowed(), dir, None).await.unwrap();
		let mut seen = 0;
		let stats = cursor
			.for_each(u32::MAX, &mut |_, _| {
				seen += 1;
				Ok(if seen == 5 {
					ControlFlow::Break(())
				} else {
					ControlFlow::Continue(())
				})
			})
			.await
			.unwrap();
		assert_eq!(stats.rows, 5, "{dir:?}: the entry broken on is counted");

		// Reading the remainder starts at the sixth entry.
		let resume = cursor.resume().unwrap();
		let mut rest = view.open(resume.remainder(&rng, dir), dir, None).await.unwrap();
		assert_eq!(keys_of(&mut *rest, 1).await, vec![expected[5].clone()], "{dir:?}");
		// So does the cursor itself.
		assert_eq!(keys_of(&mut *cursor, 1).await, vec![expected[5].clone()], "{dir:?}");
	}
	tx.cancel().await.unwrap();
}

kvs_test!(a_break_consumes_the_entry_it_stops_on, only = [rocksdb]);

/// Narrowing a cursor ends its read at the new bound, without a seek, and only inside what is left.
async fn narrow_stops_a_cursor_without_reseeking(b: &TestBackend) {
	let ds = b.create_ds().await;
	// Consecutive ids, so "20 entries further" is an exact key.
	let tx = ds.transaction(Write).await.unwrap();
	let id = |i: u32| {
		let mut key = b"narrow/".to_vec();
		key.extend_from_slice(&i.to_be_bytes());
		key
	};
	for i in 0..100u32 {
		tx.set(id(i).into(), value()).await.unwrap();
	}
	tx.commit().await.unwrap();
	let rng = range_of(b"narrow/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();

	// Forward: read ids 0..10, then end the read at id 30.
	let mut cursor = view.open(rng.as_borrowed(), Direction::Forward, None).await.unwrap();
	assert_eq!(keys_of(&mut *cursor, 10).await.len(), 10);
	assert!(cursor.narrow(Key::from(id(5))).is_err(), "before the entries still to read");
	assert!(cursor.narrow(Key::from(&b"narrow1"[..])).is_err(), "past the end");
	cursor.narrow(Key::from(id(30))).unwrap();
	let rest = keys_of(&mut *cursor, u32::MAX).await;
	assert_eq!(rest, (10..30).map(id).collect::<Vec<_>>());
	drop(cursor);

	// Backward: read ids 99..90, then end the read at id 70, inclusive.
	let mut cursor = view.open(rng.as_borrowed(), Direction::Backward, None).await.unwrap();
	assert_eq!(keys_of(&mut *cursor, 10).await.len(), 10);
	assert!(cursor.narrow(Key::from(id(95))).is_err(), "inside the entries already read");
	assert!(cursor.narrow(Key::from(&b"narrow"[..])).is_err(), "before the start");
	cursor.narrow(Key::from(id(70))).unwrap();
	let rest = keys_of(&mut *cursor, u32::MAX).await;
	assert_eq!(rest, (70..90).rev().map(id).collect::<Vec<_>>());
	drop(cursor);
	tx.cancel().await.unwrap();
}

kvs_test!(narrow_stops_a_cursor_without_reseeking, only = [rocksdb]);

/// A page takes what it can hold; an entry too large for any page arrives alone.
async fn fill_copies_into_a_caller_page(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..10u8)
		.map(|i| {
			let len = if i == 5 {
				200
			} else {
				6
			};
			(format!("fl/{i}").into_bytes(), vec![i; len])
		})
		.collect();
	for (key, val) in &entries {
		tx.set(key.clone().into(), val.clone()).await.unwrap();
	}
	tx.commit().await.unwrap();
	let rng = range_of(b"fl/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	let mut cursor = view.open(rng.as_borrowed(), Direction::Forward, None).await.unwrap();
	let mut page = Page::with_capacity(64);
	let held = |page: &Page| -> Vec<(Vec<u8>, Vec<u8>)> {
		page.batch().iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
	};

	// Entries 0..5 are 10 bytes each; the sixth would take the page past 64.
	let stats = cursor.fill(&mut page, u32::MAX).await.unwrap();
	assert_eq!(stats.rows, 5);
	assert_eq!(held(&page), entries[..5]);
	// The entry the page could not hold is read next, alone, whatever the page's size.
	let stats = cursor.fill(&mut page, u32::MAX).await.unwrap();
	assert_eq!(stats.rows, 1);
	assert_eq!(held(&page), entries[5..6]);
	assert!(page.bytes() > page.capacity(), "the page grew for the large entry");
	// The rest fit, and `limit` is honoured.
	let stats = cursor.fill(&mut page, 3).await.unwrap();
	assert_eq!(stats.rows, 3);
	assert_eq!(held(&page), entries[6..9]);
	assert_eq!(
		stats.key_bytes + stats.value_bytes,
		page.bytes() as u64,
		"the chunk's stats are the page's bytes"
	);
	let stats = cursor.fill(&mut page, u32::MAX).await.unwrap();
	assert_eq!(held(&page), entries[9..]);
	assert_eq!(stats.rows, 1);
	assert_eq!(cursor.fill(&mut page, u32::MAX).await.unwrap().rows, 0);
	assert!(page.is_empty(), "a fill clears the page first");
	assert!(cursor.resume().is_none());
	drop(cursor);
	tx.cancel().await.unwrap();
}

kvs_test!(fill_copies_into_a_caller_page, only = [rocksdb]);

/// A read-only transaction refuses commit where a writable one would wait for open cursors.
/// The refusal leaves any open cursor alive and readable.
#[cfg(not(target_family = "wasm"))]
async fn commit_waits_for_an_open_cursor(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"commit/", 20).await;
	let rng = range_of(b"commit/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	let mut cursor = view.open(rng.as_borrowed(), Direction::Forward, None).await.unwrap();
	assert_eq!(keys_of(&mut *cursor, 5).await.len(), 5);
	let refused = tx.commit().await;
	assert!(
		matches!(refused, Err(surrealdb_kvs::Error::TransactionReadonly)),
		"a read-only commit is refused: {refused:?}"
	);
	assert_eq!(keys_of(&mut *cursor, u32::MAX).await.len(), 15, "the cursor reads on");
}

#[cfg(not(target_family = "wasm"))]
kvs_test!(commit_waits_for_an_open_cursor, only = [rocksdb]);

/// Cancelling does not finish while a cursor is alive, and finishes once it is dropped.
#[cfg(not(target_family = "wasm"))]
async fn cancel_waits_for_an_open_cursor(b: &TestBackend) {
	use std::time::Duration;

	use tokio::time::timeout;

	let ds = b.create_ds().await;
	seed(&ds, b"cancel/", 20).await;
	let rng = range_of(b"cancel/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	let mut cursor = view.open(rng.as_borrowed(), Direction::Forward, None).await.unwrap();
	assert_eq!(keys_of(&mut *cursor, 5).await.len(), 5);

	// The finisher is polled by reference: dropping it once it has started would leave the
	// transaction unfinishable.
	let mut cancel = tx.cancel();
	assert!(
		timeout(Duration::from_millis(50), &mut cancel).await.is_err(),
		"cancel finished with a cursor alive"
	);
	assert_eq!(keys_of(&mut *cursor, u32::MAX).await.len(), 15, "the cursor reads on");
	drop(cursor);
	timeout(Duration::from_secs(10), cancel)
		.await
		.expect("cancel did not finish after the cursor was dropped")
		.unwrap();
}

#[cfg(not(target_family = "wasm"))]
kvs_test!(cancel_waits_for_an_open_cursor, only = [rocksdb]);

/// Once the transaction has finished, its view opens nothing.
async fn a_finished_transaction_opens_no_cursor(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"done/", 5).await;
	let rng = range_of(b"done/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	tx.cancel().await.unwrap();
	let opened = view.open(rng.as_borrowed(), Direction::Forward, None).await;
	assert!(
		matches!(opened, Err(surrealdb_kvs::Error::TransactionFinished)),
		"a finished transaction opened a cursor"
	);
	assert!(tx.snapshot().is_none(), "a finished transaction gives no view");
}

kvs_test!(a_finished_transaction_opens_no_cursor, only = [rocksdb]);

/// A writable transaction reads its own buffered writes, which a view over the snapshot would not.
async fn a_writable_rocksdb_transaction_gives_no_view(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	assert!(tx.snapshot().is_none());
	tx.cancel().await.unwrap();
}

kvs_test!(a_writable_rocksdb_transaction_gives_no_view, only = [rocksdb]);

/// A read-only transaction has nothing buffered, so its reads are the snapshot's.
async fn a_read_only_rocksdb_transaction_gives_a_view(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.snapshot().is_some());
	tx.cancel().await.unwrap();
}

kvs_test!(a_read_only_rocksdb_transaction_gives_a_view, only = [rocksdb]);

/// A backend with no native view answers none, and still divides a range.
async fn memory_backends_give_no_view_and_split_by_default(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"mem/", 1000).await;
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.snapshot().is_none());
	let rng = range_of(b"mem/");
	let bounds = tx.split(rng.as_borrowed(), SplitTarget::Parts(8), None).await.unwrap();
	assert!(!bounds.is_empty() && bounds.len() < 8, "{} boundaries", bounds.len());
	for pair in bounds.windows(2) {
		assert!(pair[0] < pair[1]);
	}
	assert!(bounds.iter().all(|key| rng.start < *key && *key < rng.end));
	tx.cancel().await.unwrap();
}

kvs_test!(memory_backends_give_no_view_and_split_by_default, only = [mem]);
