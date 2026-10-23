//! Read views, range cursors, estimate and split.
//!
//! `split` and `estimate` are answered by every backend, natively or from the defaults, and the
//! tests of them run against all of them. A view is offered only by a backend with a native one,
//! and only for a read-only transaction. The tests of the cursors a view opens run against the
//! memory backend and SurrealKV. The test of batched point reads runs against every backend:
//! through the view where there is one, and through the transaction, as a caller falls back to,
//! where there is none.

#[cfg(not(target_family = "wasm"))]
use std::collections::BTreeMap;
use std::ops::ControlFlow;

#[cfg(not(target_family = "wasm"))]
use surrealdb_kvs::Snapshot;
use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::snapshot::ViewCapabilities;
use surrealdb_kvs::{
	Direction, Error, Estimate, Key, KeyRange, Page, RangeCursor, SplitTarget, Transactable,
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
			keys: Some(100),
			bytes: 100 * (key_bytes + value().len() as u64),
			exact: true,
		}
	);

	let large = tx.estimate(range_of(b"large/"), None).await.unwrap();
	assert!(!large.exact);
	assert!(
		large.keys.is_some_and(|keys| (2500..=10_000).contains(&keys)),
		"{:?} keys estimated for 5000 held",
		large.keys
	);
	if let Some(view) = tx.snapshot() {
		assert_eq!(view.estimate(range_of(b"small/"), None).await.unwrap(), small);
		assert_eq!(view.estimate(range_of(b"large/"), None).await.unwrap(), large);
	}
	tx.cancel().await.unwrap();
}

kvs_test!(estimate_is_exact_within_the_budget);

/// Keys that cluster at the bottom of their bounds, as integer ids under a table's prefix do, are
/// estimated from the share of the keys held that the budget's read spans, not the share of the
/// bounds: within a small factor of the count, as keys spread over their bounds are.
async fn estimate_of_clustered_keys_is_near_the_count(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in 0..3000u32 {
		let mut key = b"clustered/".to_vec();
		key.extend_from_slice(&i.to_be_bytes());
		tx.set(key.into(), value()).await.unwrap();
	}
	tx.commit().await.unwrap();
	seed(&ds, b"sparse/", 3000).await;
	let tx = ds.transaction(Read).await.unwrap();
	for prefix in [&b"clustered/"[..], b"sparse/"] {
		let est = tx.estimate(range_of(prefix), None).await.unwrap();
		assert!(!est.exact);
		assert!(
			est.keys.is_some_and(|keys| (1500..=6000).contains(&keys)),
			"{:?} keys estimated for 3000 held under {}",
			est.keys,
			String::from_utf8_lossy(prefix)
		);
		if let Some(view) = tx.snapshot() {
			assert_eq!(view.estimate(range_of(prefix), None).await.unwrap(), est);
		}
	}
	tx.cancel().await.unwrap();
}

kvs_test!(estimate_of_clustered_keys_is_near_the_count);

/// Many ranges are estimated together as each is alone, in the order asked: an empty range holds
/// nothing, exactly, and a range past the budget is extrapolated. No ranges get no answers.
async fn estimate_many_answers_each_range_as_estimate_does(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"small/", 100).await;
	seed(&ds, b"large/", 5000).await;
	let tx = ds.transaction(Read).await.unwrap();

	let ranges = vec![range_of(b"large/"), range_of(b"empty/"), range_of(b"small/")];
	let many = tx.estimate_many(ranges.clone(), None).await.unwrap();
	assert_eq!(many.len(), 3);
	for (rng, got) in ranges.into_iter().zip(&many) {
		assert_eq!(*got, tx.estimate(rng, None).await.unwrap());
	}
	assert!(!many[0].exact);
	assert!(
		many[0].keys.is_some_and(|keys| keys > u64::from(surrealdb_kvs::split::ESTIMATE_BUDGET)),
		"{:?}",
		many[0]
	);
	assert_eq!(
		many[1],
		Estimate {
			keys: Some(0),
			bytes: 0,
			exact: true,
		}
	);
	assert_eq!(many[2].keys, Some(100));
	assert!(many[2].exact);
	assert!(tx.estimate_many(Vec::new(), None).await.unwrap().is_empty());
	tx.cancel().await.unwrap();
}

kvs_test!(estimate_many_answers_each_range_as_estimate_does);

/// A metadata answer is never exact: a backend with no metadata answers none, and one whose
/// metadata has not yet seen the writes answers none of an unflushed range.
async fn approximate_is_never_exact(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"large/", 5000).await;
	let tx = ds.transaction(Read).await.unwrap();
	for rng in [range_of(b"large/"), range_of(b"empty/")] {
		let got = tx.approximate(rng, None).await.unwrap();
		assert!(got.is_none_or(|got| !got.exact), "{got:?}");
	}
	tx.cancel().await.unwrap();
}

kvs_test!(approximate_is_never_exact);

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

kvs_test!(a_view_does_not_see_later_commits, only = [mem, surrealkv, surrealkv_versioned]);

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

kvs_test!(a_cursor_resumes_where_it_stopped, only = [mem, surrealkv, surrealkv_versioned]);

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

kvs_test!(a_break_consumes_the_entry_it_stops_on, only = [mem, surrealkv, surrealkv_versioned]);

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

kvs_test!(narrow_stops_a_cursor_without_reseeking, only = [mem, surrealkv, surrealkv_versioned]);

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

kvs_test!(fill_copies_into_a_caller_page, only = [mem, surrealkv, surrealkv_versioned]);

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
kvs_test!(commit_waits_for_an_open_cursor, only = [mem, surrealkv, surrealkv_versioned]);

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
kvs_test!(cancel_waits_for_an_open_cursor, only = [mem, surrealkv, surrealkv_versioned]);

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

kvs_test!(a_finished_transaction_opens_no_cursor, only = [mem, surrealkv, surrealkv_versioned]);

/// A read-only transaction has nothing buffered, so its reads are the snapshot's.
async fn a_read_only_transaction_gives_a_view(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.snapshot().is_some());
	tx.cancel().await.unwrap();
}

kvs_test!(a_read_only_transaction_gives_a_view, only = [surrealkv, surrealkv_versioned]);

/// Batched point reads at the moment a read-only transaction's view is taken: through the view
/// where the backend gives one, and through the transaction itself, a caller's fallback, where it
/// gives none. Either answers what the transaction's `getm` answered then, hits and misses in
/// input order with the same counts, whatever commits afterwards. A backend that declares no
/// view for a read-only transaction gives none.
async fn get_many_at_the_views_moment_with_or_without_a_view(b: &TestBackend) {
	let ds = b.create_ds().await;
	let prefix = b"getm/";
	seed(&ds, prefix, 16).await;
	// Every seeded key, interleaved with keys between them that were never written.
	let keys: Vec<Key<'static>> = (0..16)
		.flat_map(|i| [id_key(prefix, i, 16), id_key(prefix, 2 * i + 1, 32)])
		.map(Key::from)
		.collect();
	let tx = ds.transaction(Read).await.unwrap();
	let expected = tx.getm(&keys, None).await.unwrap();
	assert_eq!(expected.records, 16);
	let view = tx.snapshot();
	if !tx.view_capabilities().read_only {
		assert!(view.is_none(), "a backend that declares no read-only view gave one");
	}

	// Committed after the view was taken: one key overwritten and one key that was missing.
	let writer = ds.transaction(Write).await.unwrap();
	writer.set(Key::from(id_key(prefix, 0, 16)), b"changed".to_vec()).await.unwrap();
	writer.set(Key::from(id_key(prefix, 1, 32)), value()).await.unwrap();
	writer.commit().await.unwrap();

	let got = match &view {
		Some(view) => view.get_many(&keys, None).await.unwrap(),
		None => tx.getm(&keys, None).await.unwrap(),
	};
	let through = if view.is_some() {
		"the view"
	} else {
		"the transaction"
	};
	assert_eq!(got.values, expected.values, "values read through {through}");
	assert_eq!(got.records, expected.records, "records read through {through}");
	assert_eq!(got.value_bytes, expected.value_bytes, "value bytes read through {through}");
	drop(view);
	tx.cancel().await.unwrap();
}

kvs_test!(get_many_at_the_views_moment_with_or_without_a_view);

/// A backend with no native split still divides a range, through the defaults.
async fn the_memory_backend_splits_a_range_by_default(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"mem/", 1000).await;
	let tx = ds.transaction(Read).await.unwrap();
	let rng = range_of(b"mem/");
	let bounds = tx.split(rng.as_borrowed(), SplitTarget::Parts(8), None).await.unwrap();
	assert!(!bounds.is_empty() && bounds.len() < 8, "{} boundaries", bounds.len());
	for pair in bounds.windows(2) {
		assert!(pair[0] < pair[1]);
	}
	assert!(bounds.iter().all(|key| rng.start < *key && *key < rng.end));
	tx.cancel().await.unwrap();
}

kvs_test!(the_memory_backend_splits_a_range_by_default, only = [mem]);

/// A generator whose draws are the same on every run, so the ranges and chunk sizes a test reads
/// are reproducible.
#[cfg(not(target_family = "wasm"))]
struct Draw(u64);

#[cfg(not(target_family = "wasm"))]
impl Draw {
	fn next(&mut self) -> u64 {
		self.0 =
			self.0.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
		self.0 >> 33
	}

	/// A draw in `0..n`.
	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}
}

/// Key-value pairs, in the order a read returns them.
#[cfg(not(target_family = "wasm"))]
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// The versions of one key in commit order: the commit, and the value or `None` for a delete.
#[cfg(not(target_family = "wasm"))]
type Versions = Vec<(usize, Option<Vec<u8>>)>;

/// What reads of the keys a test wrote must return, worked out from the writes the test made and
/// not from the code under test.
///
/// A key keeps its versions in commit order, as `(commit, value)` with `None` for a delete. A read
/// as of the first `at` commits is answered by the rule of the engine's history iterator, one rule
/// per direction because the iterator yields a key's versions in opposite orders. The newest
/// version at or before `at` is the state of the key as of `at`: a value, or a delete, in which
/// case the key is absent. A key with no version at or before `at` is absent too.
#[cfg(not(target_family = "wasm"))]
#[derive(Default)]
struct Model {
	versions: BTreeMap<Vec<u8>, Versions>,
}

#[cfg(not(target_family = "wasm"))]
impl Model {
	fn write(&mut self, commit: usize, key: &[u8], val: Option<Vec<u8>>) {
		self.versions.entry(key.to_vec()).or_default().push((commit, val));
	}

	/// Forward, versions arrive newest first and the first one at or before `at` is the state.
	fn forward_pick(versions: &[(usize, Option<Vec<u8>>)], at: usize) -> Option<Vec<u8>> {
		versions.iter().rev().find(|(commit, _)| *commit <= at).and_then(|(_, val)| val.clone())
	}

	/// Backward, versions arrive oldest first and the last one at or before `at` is the state.
	fn backward_pick(versions: &[(usize, Option<Vec<u8>>)], at: usize) -> Option<Vec<u8>> {
		let mut kept = None;
		for (commit, val) in versions {
			if *commit <= at {
				kept.clone_from(val);
			}
		}
		kept
	}

	/// The entries of `rng` in `dir`: as of the first `at` commits, or, with `None`, as of now,
	/// where a key's newest version is its state and a delete removes it.
	fn read(&self, rng: &KeyRange<'_>, dir: Direction, at: Option<usize>) -> Entries {
		let mut entries: Entries = self
			.versions
			.iter()
			.filter(|(key, _)| {
				rng.start.as_slice() <= key.as_slice() && key.as_slice() < rng.end.as_slice()
			})
			.filter_map(|(key, versions)| {
				let val = match (at, dir) {
					(None, _) => versions.last().and_then(|(_, val)| val.clone()),
					(Some(at), Direction::Forward) => Self::forward_pick(versions, at),
					(Some(at), Direction::Backward) => Self::backward_pick(versions, at),
				};
				val.map(|val| (key.clone(), val))
			})
			.collect();
		if matches!(dir, Direction::Backward) {
			entries.reverse();
		}
		entries
	}
}

/// How many keys the bulk of a fixture holds.
#[cfg(not(target_family = "wasm"))]
const BULK: u32 = 2000;

/// The key of the `i`th key of a fixture.
#[cfg(not(target_family = "wasm"))]
fn bulk_key(i: u32) -> Vec<u8> {
	let mut key = b"vw/".to_vec();
	key.extend_from_slice(&i.to_be_bytes());
	key
}

/// Commits of a fixture, and the versions the datastore stamped them with.
///
/// `stamps[at]` is a version after the first `at` commits and before commit `at + 1`, so a read
/// as of it sees exactly those commits.
#[cfg(not(target_family = "wasm"))]
struct Fixture {
	model: Model,
	stamps: Vec<u64>,
}

/// Wait for the datastore's clock to move past every earlier commit, take the version it reads,
/// and wait again so that the next commit is stamped after it.
#[cfg(not(target_family = "wasm"))]
async fn stamp(ds: &TestDs) -> u64 {
	use std::time::Duration;

	use tokio::time::sleep;

	let tx = ds.transaction(Read).await.unwrap();
	sleep(Duration::from_millis(20)).await;
	let at = tx.version_impl().create_from_datetime(chrono::Utc::now()).unwrap().as_versionstamp();
	sleep(Duration::from_millis(20)).await;
	tx.cancel().await.unwrap();
	u64::try_from(at).unwrap()
}

/// Commit three generations of keys under `vw/`, in commits of their own.
///
/// 1. `BULK` keys.
/// 2. A third of them overwritten and a third deleted; a key between every fiftieth pair; a tail of
///    keys past the last.
/// 3. Some deleted keys written again, some overwritten ones deleted, some overwritten; a second
///    tail.
///
/// A read as of an earlier commit therefore passes keys that exist only later, between keys and at
/// both ends of the range.
#[cfg(not(target_family = "wasm"))]
async fn commit_generations(ds: &TestDs) -> Fixture {
	let mut model = Model::default();
	let mut stamps = vec![stamp(ds).await];
	for commit in 1..=3usize {
		let tx = ds.transaction(Write).await.unwrap();
		let mut put = |key: Vec<u8>, val: String| {
			model.write(commit, &key, Some(val.clone().into_bytes()));
			(key, val.into_bytes())
		};
		let mut sets = Vec::new();
		let mut dels = Vec::new();
		match commit {
			1 => {
				for i in 0..BULK {
					sets.push(put(bulk_key(i), format!("g1-{i}")));
				}
			}
			2 => {
				for i in 0..BULK {
					match i % 3 {
						0 => sets.push(put(bulk_key(i), format!("g2-{i}"))),
						1 => dels.push(bulk_key(i)),
						_ => {}
					}
					if i % 50 == 0 {
						let mut between = bulk_key(i);
						between.push(1);
						sets.push(put(between, format!("g2-between-{i}")));
					}
				}
				for i in BULK..BULK + BULK / 6 {
					sets.push(put(bulk_key(i), format!("g2-tail-{i}")));
				}
			}
			_ => {
				for i in 0..BULK {
					match i % 6 {
						1 => sets.push(put(bulk_key(i), format!("g3-{i}"))),
						0 => dels.push(bulk_key(i)),
						3 => sets.push(put(bulk_key(i), format!("g3-{i}"))),
						_ => {}
					}
				}
				for i in BULK + BULK / 6..BULK + BULK / 3 {
					sets.push(put(bulk_key(i), format!("g3-tail-{i}")));
				}
			}
		}
		for key in &dels {
			model.write(commit, key, None);
		}
		for (key, val) in sets {
			tx.set(key.into(), val).await.unwrap();
		}
		for key in dels {
			tx.del(key.into()).await.unwrap();
		}
		tx.commit().await.unwrap();
		stamps.push(stamp(ds).await);
	}
	Fixture {
		model,
		stamps,
	}
}

/// A range over a fixture's keys, whose bounds may fall between two keys.
#[cfg(not(target_family = "wasm"))]
fn random_range(draw: &mut Draw) -> KeyRange<'static> {
	let span = u64::from(BULK + BULK / 3);
	let mut ends = [draw.below(span) as u32, draw.below(span) as u32];
	ends.sort_unstable();
	let mut start = bulk_key(ends[0]);
	let mut end = bulk_key(ends[1]);
	if draw.below(4) == 0 {
		start.push(1);
	}
	if draw.below(4) == 0 {
		end.push(1);
	}
	if start >= end {
		end.clone_from(&start);
		end.push(0);
	}
	KeyRange {
		start: Key::from(start),
		end: Key::from(end),
	}
}

/// What the transaction's own `scan` or `scanr` returns for `rng`.
#[cfg(not(target_family = "wasm"))]
async fn scan_in(
	tx: &dyn Transactable,
	rng: &KeyRange<'_>,
	dir: Direction,
	version: Option<u64>,
) -> Entries {
	match dir {
		Direction::Forward => {
			tx.scan(rng.as_borrowed(), u32::MAX, 0, version).await.unwrap().values
		}
		Direction::Backward => {
			tx.scanr(rng.as_borrowed(), u32::MAX, 0, version).await.unwrap().values
		}
	}
}

/// What the transaction's own `keys` or `keysr` returns for `rng`.
#[cfg(not(target_family = "wasm"))]
async fn keys_in(
	tx: &dyn Transactable,
	rng: &KeyRange<'_>,
	dir: Direction,
	version: Option<u64>,
) -> Vec<Vec<u8>> {
	match dir {
		Direction::Forward => tx.keys(rng.as_borrowed(), u32::MAX, 0, version).await.unwrap().keys,
		Direction::Backward => {
			tx.keysr(rng.as_borrowed(), u32::MAX, 0, version).await.unwrap().keys
		}
	}
}

/// The transaction's own reads of a version agree with the model, in both directions, for the keys
/// and for the entries, over whole and partial ranges.
#[cfg(not(target_family = "wasm"))]
async fn versioned_reads_match_the_model(b: &TestBackend) {
	let ds = b.create_ds().await;
	let fixture = commit_generations(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	let mut draw = Draw(1);
	for (at, &stamp) in fixture.stamps.iter().enumerate() {
		let mut ranges = vec![range_of(b"vw/")];
		ranges.extend((0..20).map(|_| random_range(&mut draw)));
		for rng in ranges {
			for dir in [Direction::Forward, Direction::Backward] {
				let expected = fixture.model.read(&rng, dir, Some(at));
				let ctx = format!("{rng} as of commit {at}, {dir:?}");
				assert_eq!(scan_in(&*tx, &rng, dir, Some(stamp)).await, expected, "scan of {ctx}");
				let keys: Vec<_> = expected.iter().map(|(key, _)| key.clone()).collect();
				assert_eq!(keys_in(&*tx, &rng, dir, Some(stamp)).await, keys, "keys of {ctx}");
			}
		}
	}
	tx.cancel().await.unwrap();
}

#[cfg(not(target_family = "wasm"))]
kvs_test!(versioned_reads_match_the_model, only = [surrealkv_versioned]);

/// Every entry a view's cursor reads over `rng` in `dir`, as of `version`.
#[cfg(not(target_family = "wasm"))]
async fn view_entries(
	view: &dyn Snapshot,
	rng: &KeyRange<'_>,
	dir: Direction,
	version: Option<u64>,
) -> Entries {
	let mut cursor = view.open(rng.as_borrowed(), dir, version).await.unwrap();
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

/// What a fixture's reads are checked at: the current state, and for a datastore that keeps
/// versions, the state after each commit, as `(commits, version)`.
#[cfg(not(target_family = "wasm"))]
fn reads_of(b: &TestBackend, fixture: &Fixture) -> Vec<(Option<usize>, Option<u64>)> {
	let mut reads = vec![(None, None)];
	if b.name() == "surrealkv_versioned" {
		reads.extend(fixture.stamps.iter().enumerate().map(|(at, &stamp)| (Some(at), Some(stamp))));
	}
	reads
}

/// A view answers every range as the transaction's own reads do, and as the writes made say,
/// whole and in part, forward and backward, now and as of each version.
#[cfg(not(target_family = "wasm"))]
async fn view_equals_transaction_reads(b: &TestBackend) {
	let ds = b.create_ds().await;
	let fixture = commit_generations(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().expect("a read-only transaction gives a view");
	let mut draw = Draw(2);
	for (at, version) in reads_of(b, &fixture) {
		let mut ranges = vec![range_of(b"vw/")];
		ranges.extend((0..20).map(|_| random_range(&mut draw)));
		for rng in ranges {
			for dir in [Direction::Forward, Direction::Backward] {
				let expected = fixture.model.read(&rng, dir, at);
				let ctx = format!("{rng} as of {at:?}, {dir:?}");
				assert_eq!(
					view_entries(&*view, &rng, dir, version).await,
					expected,
					"view of {ctx}"
				);
				assert_eq!(
					scan_in(&*tx, &rng, dir, version).await,
					expected,
					"transaction scan of {ctx}"
				);
			}
		}
	}
	tx.cancel().await.unwrap();
}

#[cfg(not(target_family = "wasm"))]
kvs_test!(view_equals_transaction_reads, only = [mem, surrealkv, surrealkv_versioned]);

/// A range read in chunks of any size, by visitor or into a page, with a visitor that stops early,
/// makes the entries one scan returns, and its cursor has nowhere to resume exactly at the end.
#[cfg(not(target_family = "wasm"))]
async fn chunked_view_reads_equal_one_scan(b: &TestBackend) {
	let ds = b.create_ds().await;
	let fixture = commit_generations(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	let rng = range_of(b"vw/");
	let mut draw = Draw(3);
	for (at, version) in reads_of(b, &fixture) {
		for dir in [Direction::Forward, Direction::Backward] {
			let expected = fixture.model.read(&rng, dir, at);
			assert_eq!(scan_in(&*tx, &rng, dir, version).await, expected, "scan, {at:?}, {dir:?}");

			let mut cursor = view.open(rng.as_borrowed(), dir, version).await.unwrap();
			let mut read = Entries::new();
			while cursor.resume().is_some() {
				let before = read.len();
				let limit = 1 + draw.below(300) as u32;
				match draw.below(3) {
					0 => {
						cursor
							.for_each(limit, &mut |key, val| {
								read.push((key.to_vec(), val.to_vec()));
								Ok(ControlFlow::Continue(()))
							})
							.await
							.unwrap();
					}
					1 => {
						let stop = 1 + draw.below(u64::from(limit)) as usize;
						let mut taken = 0;
						cursor
							.for_each(limit, &mut |key, val| {
								read.push((key.to_vec(), val.to_vec()));
								taken += 1;
								Ok(if taken == stop {
									ControlFlow::Break(())
								} else {
									ControlFlow::Continue(())
								})
							})
							.await
							.unwrap();
					}
					_ => {
						let mut page = Page::with_capacity(32 + draw.below(1000) as usize);
						cursor.fill(&mut page, limit).await.unwrap();
						read.extend(page.batch().iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
					}
				}
				let done = read.len() == expected.len();
				assert_eq!(
					cursor.resume().is_none(),
					done,
					"{at:?}, {dir:?}: resume after {} of {} entries",
					read.len(),
					expected.len()
				);
				assert!(read.len() > before || done, "{at:?}, {dir:?}: a chunk read nothing");
			}
			assert_eq!(read, expected, "{at:?}, {dir:?}: chunks do not make the scan");
		}
	}
	tx.cancel().await.unwrap();
}

#[cfg(not(target_family = "wasm"))]
kvs_test!(chunked_view_reads_equal_one_scan, only = [mem, surrealkv, surrealkv_versioned]);

/// The range a cursor as of `version` has left to read once it has read `count` entries, or
/// `None` when it has nowhere to resume.
#[cfg(not(target_family = "wasm"))]
async fn left_after(
	view: &dyn Snapshot,
	rng: &KeyRange<'_>,
	dir: Direction,
	version: Option<u64>,
	count: usize,
) -> Option<KeyRange<'static>> {
	let mut cursor = view.open(rng.as_borrowed(), dir, version).await.unwrap();
	assert_eq!(keys_of(&mut *cursor, count as u32).await.len(), count);
	cursor.resume().map(|at| {
		let left = at.remainder(rng, dir);
		KeyRange {
			start: left.start.into_static(),
			end: left.end.into_static(),
		}
	})
}

/// A cursor as of a version rests on the next key that version holds, passing keys written only
/// later, and has nowhere to resume once only such keys remain.
#[cfg(not(target_family = "wasm"))]
async fn resume_is_none_when_only_invisible_versions_remain(b: &TestBackend) {
	let ds = b.create_ds().await;
	let fixture = commit_generations(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	let rng = range_of(b"vw/");
	// After the first commit only the bulk exists. The keys past it, and a key after every fiftieth
	// of it, are written later.
	let version = Some(fixture.stamps[1]);
	let mut written_later = bulk_key(50);
	written_later.push(1);
	assert!(fixture.model.versions.contains_key(&written_later));

	for dir in [Direction::Forward, Direction::Backward] {
		let keys: Vec<_> =
			fixture.model.read(&rng, dir, Some(1)).into_iter().map(|(key, _)| key).collect();
		assert_eq!(keys.len(), BULK as usize);
		// Entries read up to the one on either side of the key written later after key 50.
		let beside = keys
			.iter()
			.position(|key| {
				*key == bulk_key(if dir == Direction::Forward {
					50
				} else {
					51
				})
			})
			.unwrap();
		for read in [1, beside + 1, keys.len() / 2, keys.len() - 1] {
			let left = left_after(&*view, &rng, dir, version, read).await.expect("entries remain");
			// The cursor rests on the next key the version holds.
			let next = &keys[read];
			match dir {
				Direction::Forward => {
					assert_eq!(left.start.as_slice(), next, "{dir:?} after {read}")
				}
				Direction::Backward => {
					let mut bound = next.clone();
					bound.push(0);
					assert_eq!(left.end.as_slice(), bound, "{dir:?} after {read}");
				}
			}
		}
		let left = left_after(&*view, &rng, dir, version, keys.len()).await;
		assert!(left.is_none(), "{dir:?}: only keys written later remain");
	}
	tx.cancel().await.unwrap();
}

#[cfg(not(target_family = "wasm"))]
kvs_test!(resume_is_none_when_only_invisible_versions_remain, only = [surrealkv_versioned]);

/// A view's batched read answers what the transaction's own batched read does: hits, misses,
/// repeated keys, an empty list, the current state and every version.
#[cfg(not(target_family = "wasm"))]
async fn view_get_many_equals_getm(b: &TestBackend) {
	let ds = b.create_ds().await;
	let fixture = commit_generations(&ds).await;
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	let mut between = bulk_key(100);
	between.push(1);
	let mut missing = bulk_key(7);
	missing.push(2);
	// Present, overwritten, deleted, written late, missing and repeated.
	let keys: Vec<Key<'static>> = [
		bulk_key(0),
		bulk_key(1),
		bulk_key(2),
		bulk_key(3),
		bulk_key(4),
		bulk_key(5),
		between,
		bulk_key(BULK + 1),
		bulk_key(BULK + BULK / 6 + 1),
		missing,
		b"vw/".to_vec(),
		bulk_key(1),
		bulk_key(0),
	]
	.into_iter()
	.map(Key::from)
	.collect();
	for (at, version) in reads_of(b, &fixture) {
		let got = view.get_many(&keys, version).await.unwrap();
		let expected = tx.getm(&keys, version).await.unwrap();
		assert_eq!(got.values, expected.values, "values as of {at:?}");
		assert_eq!(got.records, expected.records, "records as of {at:?}");
		assert_eq!(got.value_bytes, expected.value_bytes, "value bytes as of {at:?}");
		assert_eq!(got.values.len(), keys.len());
		let none = view.get_many(&[], version).await.unwrap();
		assert!(none.values.is_empty() && none.records == 0 && none.value_bytes == 0);
	}
	tx.cancel().await.unwrap();
	assert!(
		matches!(view.get_many(&[], None).await, Err(Error::TransactionFinished)),
		"a finished transaction answers no batched read, an empty one included"
	);
}

#[cfg(not(target_family = "wasm"))]
kvs_test!(view_get_many_equals_getm, only = [mem, surrealkv, surrealkv_versioned]);

/// The owner reads while cursors of its view are open, and the cursors read on.
#[cfg(not(target_family = "wasm"))]
async fn owner_reads_while_a_cursor_is_open(b: &TestBackend) {
	use std::time::Duration;

	use tokio::time::timeout;

	let ds = b.create_ds().await;
	seed(&ds, b"owner/", 50).await;
	let rng = range_of(b"owner/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	let mut cursor = view.open(rng.as_borrowed(), Direction::Forward, None).await.unwrap();
	assert_eq!(keys_of(&mut *cursor, 5).await.len(), 5);

	let wait = Duration::from_secs(5);
	let got = timeout(wait, tx.get(id_key(b"owner/", 3, 50).into(), None)).await;
	assert!(got.expect("get waited on the cursor").unwrap().is_some());
	let scanned = timeout(wait, tx.scan(rng.as_borrowed(), u32::MAX, 0, None)).await;
	assert_eq!(scanned.expect("scan waited on the cursor").unwrap().values.len(), 50);
	let counted = timeout(wait, tx.count(rng.as_borrowed(), None)).await;
	assert_eq!(counted.expect("count waited on the cursor").unwrap(), 50);

	assert_eq!(keys_of(&mut *cursor, u32::MAX).await.len(), 45, "the cursor reads on");
	drop(cursor);
	tx.cancel().await.unwrap();
}

#[cfg(not(target_family = "wasm"))]
kvs_test!(owner_reads_while_a_cursor_is_open, only = [mem, surrealkv, surrealkv_versioned]);

/// A savepoint on a read-only transaction is refused at once, whatever cursors are open.
#[cfg(not(target_family = "wasm"))]
async fn savepoint_on_a_read_only_transaction_does_not_wait(b: &TestBackend) {
	use std::time::Duration;

	use tokio::time::timeout;

	let ds = b.create_ds().await;
	seed(&ds, b"save/", 20).await;
	let rng = range_of(b"save/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	let mut cursor = view.open(rng.as_borrowed(), Direction::Forward, None).await.unwrap();
	assert_eq!(keys_of(&mut *cursor, 5).await.len(), 5);

	let refused = timeout(Duration::from_secs(5), tx.new_save_point()).await;
	assert!(
		matches!(
			refused.expect("a savepoint waited on the cursor"),
			Err(Error::TransactionReadonly)
		),
		"a read-only transaction takes no savepoint"
	);
	assert_eq!(keys_of(&mut *cursor, u32::MAX).await.len(), 15, "the cursor reads on");
	drop(cursor);
	tx.cancel().await.unwrap();
}

#[cfg(not(target_family = "wasm"))]
kvs_test!(
	savepoint_on_a_read_only_transaction_does_not_wait,
	only = [mem, surrealkv, surrealkv_versioned]
);

/// While `cancel` waits for a cursor, the view opens no further cursor and does not wait for the
/// cancel either.
#[cfg(not(target_family = "wasm"))]
async fn open_during_cancel_is_refused_not_blocked(b: &TestBackend) {
	use std::time::Duration;

	use tokio::time::timeout;

	let ds = b.create_ds().await;
	seed(&ds, b"during/", 20).await;
	let rng = range_of(b"during/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	let mut cursor = view.open(rng.as_borrowed(), Direction::Forward, None).await.unwrap();
	assert_eq!(keys_of(&mut *cursor, 5).await.len(), 5);

	// Polled by reference: dropping a cancel that has started would leave the transaction
	// unfinishable.
	let mut cancel = tx.cancel();
	assert!(
		timeout(Duration::from_millis(50), &mut cancel).await.is_err(),
		"cancel finished with a cursor alive"
	);
	let opened =
		timeout(Duration::from_secs(5), view.open(rng.as_borrowed(), Direction::Forward, None))
			.await;
	assert!(
		matches!(opened.expect("open waited on the cancel"), Err(Error::TransactionFinished)),
		"a cancelling transaction opened a cursor"
	);
	let many = timeout(Duration::from_secs(5), view.get_many(&[], None)).await;
	assert!(
		matches!(many.expect("get_many waited on the cancel"), Err(Error::TransactionFinished)),
		"a cancelling transaction answered a batched read"
	);
	drop(cursor);
	timeout(Duration::from_secs(10), cancel)
		.await
		.expect("cancel did not finish after the cursor was dropped")
		.unwrap();
}

#[cfg(not(target_family = "wasm"))]
kvs_test!(open_during_cancel_is_refused_not_blocked, only = [mem, surrealkv, surrealkv_versioned]);

/// A view taken before the transaction cancelled opens nothing and answers nothing afterwards.
async fn open_after_cancel_completes_is_refused(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"after/", 5).await;
	let rng = range_of(b"after/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	tx.cancel().await.unwrap();
	let opened = view.open(rng.as_borrowed(), Direction::Forward, None).await;
	assert!(matches!(opened, Err(Error::TransactionFinished)), "a cursor opened after the cancel");
	let many = view.get_many(&[], None).await;
	assert!(matches!(many, Err(Error::TransactionFinished)), "a batched read after the cancel");
}

kvs_test!(open_after_cancel_completes_is_refused, only = [mem, surrealkv, surrealkv_versioned]);

/// A read at a version is refused by a datastore that keeps none, before anything is read.
async fn unsupported_version_is_refused_by_open(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"unsupported/", 5).await;
	let rng = range_of(b"unsupported/");
	let tx = ds.transaction(Read).await.unwrap();
	let view = tx.snapshot().unwrap();
	let opened = view.open(rng.as_borrowed(), Direction::Forward, Some(1)).await;
	assert!(matches!(opened, Err(Error::UnsupportedVersionedQueries)));
	let many = view.get_many(&[], Some(1)).await;
	assert!(matches!(many, Err(Error::UnsupportedVersionedQueries)));
	tx.cancel().await.unwrap();
}

kvs_test!(unsupported_version_is_refused_by_open, only = [mem, surrealkv]);

/// Reads through a view put nothing in anyone's conflict set, and a writable transaction, which
/// gives no view, reads without joining its own.
async fn view_reads_add_no_conflicts(b: &TestBackend) {
	let ds = b.create_ds().await;
	seed(&ds, b"conflict/", 10).await;
	let rng = range_of(b"conflict/");
	let key = id_key(b"conflict/", 3, 10);

	// A view reads the key, a writer commits it, and the owner finishes cleanly.
	let reader = ds.transaction(Read).await.unwrap();
	let view = reader.snapshot().unwrap();
	let mut cursor = view.open(rng.as_borrowed(), Direction::Forward, None).await.unwrap();
	assert_eq!(keys_of(&mut *cursor, u32::MAX).await.len(), 10);
	let found = view.get_many(&[Key::from(key.clone())], None).await.unwrap();
	assert_eq!(found.records, 1);
	let writer = ds.transaction(Write).await.unwrap();
	writer.set(key.clone().into(), b"changed".to_vec()).await.unwrap();
	writer.commit().await.unwrap();
	drop(cursor);
	reader.cancel().await.unwrap();

	// A writable transaction gives no view, and its plain read of the key does not make its own
	// commit conflict with another's of that key.
	let tx = ds.transaction(Write).await.unwrap();
	assert!(tx.snapshot().is_none());
	assert_eq!(tx.get(key.clone().into(), None).await.unwrap().unwrap(), b"changed");
	let other = ds.transaction(Write).await.unwrap();
	other.set(key.clone().into(), b"again".to_vec()).await.unwrap();
	other.commit().await.unwrap();
	tx.set(id_key(b"conflict/", 4, 10).into(), b"mine".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
}

kvs_test!(view_reads_add_no_conflicts, only = [mem, surrealkv, surrealkv_versioned]);

/// A writable transaction gives no view, before or after it writes, and says so up front.
async fn a_writable_transaction_gives_no_view_where_only_read_only_ones_do(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	assert!(tx.snapshot().is_none());
	tx.set(b"writable".to_vec().into(), value()).await.unwrap();
	assert!(tx.snapshot().is_none(), "a view would not see the write the owner makes next");
	assert_eq!(
		tx.view_capabilities(),
		ViewCapabilities {
			read_only: true,
			writable: false
		}
	);
	tx.cancel().await.unwrap();
}

kvs_test!(
	a_writable_transaction_gives_no_view_where_only_read_only_ones_do,
	only = [mem, surrealkv, surrealkv_versioned]
);

/// A read-only transaction gives a view of its snapshot, and says so up front.
async fn a_read_only_transaction_gives_a_view_where_only_read_only_ones_do(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.snapshot().is_some());
	assert_eq!(
		tx.view_capabilities(),
		ViewCapabilities {
			read_only: true,
			writable: false
		}
	);
	tx.cancel().await.unwrap();
}

kvs_test!(
	a_read_only_transaction_gives_a_view_where_only_read_only_ones_do,
	only = [mem, surrealkv, surrealkv_versioned]
);
