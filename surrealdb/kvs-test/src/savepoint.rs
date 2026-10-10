//! Savepoints: `new_save_point` / `rollback_to_save_point` /
//! `release_last_save_point` are required trait methods on every backend.

use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::{Direction, Error, KeyRange};

use crate::{TestBackend, kvs_test};

/// Writes made after a savepoint are reverted by rollback; writes made
/// before it — and previously committed data — survive.
async fn rollback_reverts_writes(b: &TestBackend) {
	let ds = b.create_ds().await;
	// Committed baseline
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"committed".into(), b"before".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// In-tx: pre-savepoint write, savepoint, post-savepoint mutations
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"pre".into(), b"kept".to_vec()).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"post".into(), b"discarded".to_vec()).await.unwrap();
	tx.set(b"committed".into(), b"overwritten".to_vec()).await.unwrap();
	tx.del(b"pre".into()).await.unwrap();
	tx.rollback_to_save_point().await.unwrap();
	// In-tx visibility after rollback
	let val = tx.get(b"pre".into(), None).await.unwrap();
	assert_eq!(val.as_deref(), Some(&b"kept"[..]), "pre-savepoint write must survive rollback");
	let val = tx.get(b"post".into(), None).await.unwrap();
	assert!(val.is_none(), "post-savepoint write must be reverted");
	let val = tx.get(b"committed".into(), None).await.unwrap();
	assert_eq!(val.as_deref(), Some(&b"before"[..]), "committed value must be restored");
	tx.commit().await.unwrap();
	// Post-commit state matches the in-tx view
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.get(b"post".into(), None).await.unwrap().is_none());
	assert_eq!(tx.get(b"pre".into(), None).await.unwrap().as_deref(), Some(&b"kept"[..]));
	assert_eq!(tx.get(b"committed".into(), None).await.unwrap().as_deref(), Some(&b"before"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(rollback_reverts_writes);

/// A value written in the transaction before the savepoint — not just a
/// committed one — is what rollback restores.
async fn rollback_restores_uncommitted_previous_value(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"uncommitted".to_vec()).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"test".into(), b"overwritten".to_vec()).await.unwrap();
	tx.rollback_to_save_point().await.unwrap();
	let val = tx.get(b"test".into(), None).await.unwrap();
	assert_eq!(val.as_deref(), Some(&b"uncommitted"[..]));
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"uncommitted"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(rollback_restores_uncommitted_previous_value);

/// Savepoints nest: each rollback reverts to the most recent savepoint.
async fn nested(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"outer".into(), b"a".to_vec()).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"inner".into(), b"b".to_vec()).await.unwrap();
	// Rolling back the inner savepoint reverts only the inner write
	tx.rollback_to_save_point().await.unwrap();
	assert!(tx.get(b"inner".into(), None).await.unwrap().is_none());
	assert_eq!(tx.get(b"outer".into(), None).await.unwrap().as_deref(), Some(&b"a"[..]));
	// Rolling back the outer savepoint reverts the rest
	tx.rollback_to_save_point().await.unwrap();
	assert!(tx.get(b"outer".into(), None).await.unwrap().is_none());
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.get(b"outer".into(), None).await.unwrap().is_none());
	assert!(tx.get(b"inner".into(), None).await.unwrap().is_none());
	tx.cancel().await.unwrap();
}

kvs_test!(nested);

/// Releasing a savepoint keeps its writes, which then commit normally.
async fn release_keeps_writes(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"test".into(), b"kept".to_vec()).await.unwrap();
	tx.release_last_save_point().await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"kept"[..]));
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"kept"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(release_keeps_writes);

/// Releasing a nested savepoint keeps its writes undoable by the savepoint
/// that encloses it: rolling that one back reverts both scopes.
///
/// Core reaches this through a synchronous `DEFINE EVENT`. The per-document
/// savepoint taken by an INSERT or UPSERT is still open while the event's own
/// statement takes and releases one of its own, so a create attempt that fails
/// after the event has run must still undo everything it wrote.
async fn release_nested_then_rollback_enclosing(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"outer".into(), b"a".to_vec()).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"inner".into(), b"b".to_vec()).await.unwrap();
	// Release keeps the inner write visible
	tx.release_last_save_point().await.unwrap();
	assert_eq!(tx.get(b"inner".into(), None).await.unwrap().as_deref(), Some(&b"b"[..]));
	// Rolling back the enclosing savepoint reverts both scopes' writes
	tx.rollback_to_save_point().await.unwrap();
	assert!(
		tx.get(b"inner".into(), None).await.unwrap().is_none(),
		"a released savepoint's writes must still be undone by the enclosing rollback"
	);
	assert!(
		tx.get(b"outer".into(), None).await.unwrap().is_none(),
		"the enclosing savepoint's own writes must be undone"
	);
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.get(b"inner".into(), None).await.unwrap().is_none());
	assert!(tx.get(b"outer".into(), None).await.unwrap().is_none());
	tx.cancel().await.unwrap();
}

kvs_test!(release_nested_then_rollback_enclosing);

/// Releasing the outermost savepoint closes the scope, leaving nothing to roll
/// back to and no savepoint to reach past.
async fn release_outermost_then_rollback_errors(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"test".into(), b"kept".to_vec()).await.unwrap();
	tx.release_last_save_point().await.unwrap();
	assert!(
		matches!(tx.rollback_to_save_point().await, Err(Error::NoSavepoint)),
		"rollback after releasing the only savepoint must report NoSavepoint"
	);
	// The released write is untouched by the refused rollback
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"kept"[..]));
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"kept"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(release_outermost_then_rollback_errors);

/// A transaction stays usable after a rollback: later writes commit.
async fn rollback_then_continue(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"test".into(), b"discarded".to_vec()).await.unwrap();
	tx.rollback_to_save_point().await.unwrap();
	tx.set(b"test".into(), b"final".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"final"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(rollback_then_continue);

/// Rolling back with no savepoint on the stack is an error, while releasing
/// with none is accepted.
async fn underflow_errors(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	assert!(
		matches!(tx.rollback_to_save_point().await, Err(Error::NoSavepoint)),
		"rollback with no savepoint must report NoSavepoint"
	);
	assert!(
		tx.release_last_save_point().await.is_ok(),
		"release with no savepoint must be accepted"
	);
	tx.cancel().await.unwrap();
}

kvs_test!(underflow_errors);

/// Savepoint writes and the savepoint itself are discarded by cancel.
async fn discarded_by_cancel(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.new_save_point().await.unwrap();
	tx.set(b"test".into(), b"discarded".to_vec()).await.unwrap();
	tx.cancel().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	assert!(tx.get(b"test".into(), None).await.unwrap().is_none());
	tx.cancel().await.unwrap();
}

kvs_test!(discarded_by_cancel);

/// A scan cursor that is open when a savepoint rolls back carries on over the
/// transaction's post-rollback view of the rest of its range.
///
/// Core reaches this whenever a write statement scans a table while an event,
/// or a function it calls, takes a savepoint and rolls it back: an `UPSERT`
/// whose create attempt hits an existing record does exactly that, and the
/// scan's cursor is still open across it. The rollback must neither fail nor
/// disturb the cursor: entries written after the savepoint are not yielded,
/// and entries it overwrote or deleted are yielded as they were before it.
///
/// Both directions and both cursor kinds are driven, since each has its own
/// read path, and two cursors are open across the same rollback.
async fn cursor_survives_rollback(b: &TestBackend) {
	let key = |i: usize| format!("cur/key_{i:04}").into_bytes();
	let ds = b.create_ds().await;
	// Committed baseline: the even keys.
	let tx = ds.transaction(Write).await.unwrap();
	for i in (0..20).step_by(2) {
		tx.set(key(i).into(), b"v".to_vec()).await.unwrap();
	}
	tx.commit().await.unwrap();

	for dir in [Direction::Forward, Direction::Backward] {
		let range = || KeyRange::from(b"cur/".as_slice()..b"cur0".as_slice());
		let tx = ds.transaction(Write).await.unwrap();
		// Written before the savepoint, ahead of both cursors: survives it.
		tx.set(key(5).into(), b"v".to_vec()).await.unwrap();
		let mut keys_cursor = tx.open_keys_cursor(range(), dir, 0, None).await.unwrap();
		let mut vals_cursor = tx.open_vals_cursor(range(), dir, 0, None).await.unwrap();
		// Read a first batch from each so both cursors are mid-scan.
		let mut seen_keys: Vec<Vec<u8>> = Vec::new();
		let mut seen_vals: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		seen_keys.extend(keys_cursor.next_batch(3).await.unwrap().iter().map(|k| k.to_vec()));
		seen_vals.extend(
			vals_cursor.next_batch(3).await.unwrap().iter().map(|(k, v)| (k.to_vec(), v.to_vec())),
		);
		// After the savepoint, ahead of both cursors: two inserts, an
		// overwrite and a delete, all undone by the rollback.
		tx.new_save_point().await.unwrap();
		tx.set(key(7).into(), b"v".to_vec()).await.unwrap();
		tx.set(key(13).into(), b"v".to_vec()).await.unwrap();
		tx.set(key(8).into(), b"overwritten".to_vec()).await.unwrap();
		tx.del(key(10).into()).await.unwrap();
		tx.rollback_to_save_point().await.unwrap();
		// Drain both cursors.
		loop {
			let batch = keys_cursor.next_batch(4).await.unwrap();
			if batch.is_empty() {
				break;
			}
			seen_keys.extend(batch.iter().map(|k| k.to_vec()));
		}
		loop {
			let batch = vals_cursor.next_batch(4).await.unwrap();
			if batch.is_empty() {
				break;
			}
			seen_vals.extend(batch.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
		}
		// Drop the cursors before finishing: backends that drain live cursors
		// during cancel wait for them to be released.
		drop(keys_cursor);
		drop(vals_cursor);

		let mut expected: Vec<Vec<u8>> =
			(0..20).step_by(2).chain(std::iter::once(5)).map(key).collect();
		expected.sort();
		if dir == Direction::Backward {
			expected.reverse();
		}
		assert_eq!(seen_keys, expected, "keys cursor, {dir:?}");
		assert_eq!(
			seen_vals,
			expected.iter().map(|k| (k.clone(), b"v".to_vec())).collect::<Vec<_>>(),
			"vals cursor, {dir:?}"
		);
		// The transaction itself holds the same view the cursors saw.
		assert!(tx.get(key(7).into(), None).await.unwrap().is_none());
		assert_eq!(tx.get(key(10).into(), None).await.unwrap().as_deref(), Some(&b"v"[..]));
		tx.cancel().await.unwrap();
	}
}

kvs_test!(cursor_survives_rollback);

/// A scan cursor resumes strictly after the last entry it consumed, so an
/// entry a rollback restores ahead of that point is yielded even though the
/// cursor had stepped over its absence.
///
/// The entries deleted inside the savepoint are hidden from the cursor when it
/// reads its first batch. The rollback then brings them back: either one lies
/// between the last consumed entry and the entry the cursor was next due to
/// yield, or all of them lie beyond the point where the cursor had run out.
///
/// Both cursor kinds are driven both ways, by `next_batch` and by `for_each`,
/// since each has its own read path.
async fn cursor_yields_entries_a_rollback_restores(b: &TestBackend) {
	let key = |i: usize| format!("res/key_{i:04}").into_bytes();
	let continue_ = || Ok(std::ops::ControlFlow::Continue(()));
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in (0..10).step_by(2) {
		tx.set(key(i).into(), b"v".to_vec()).await.unwrap();
	}
	tx.commit().await.unwrap();
	for dir in [Direction::Forward, Direction::Backward] {
		for exhausted in [false, true] {
			for visitor in [false, true] {
				let tx = ds.transaction(Write).await.unwrap();
				tx.new_save_point().await.unwrap();
				// The first batch of two takes entries 0 and 2 going forward, 8
				// and 6 going backward. What is hidden sits just past them, or
				// is all that remains past them.
				let hidden: &[usize] = match (dir, exhausted) {
					(_, false) => &[4],
					(Direction::Forward, true) => &[4, 6, 8],
					(Direction::Backward, true) => &[0, 2, 4],
				};
				for i in hidden {
					tx.del(key(*i).into()).await.unwrap();
				}
				let range = || KeyRange::from(b"res/".as_slice()..b"res0".as_slice());
				let mut keys_cursor = tx.open_keys_cursor(range(), dir, 0, None).await.unwrap();
				let mut vals_cursor = tx.open_vals_cursor(range(), dir, 0, None).await.unwrap();
				let mut seen_keys: Vec<Vec<u8>> = Vec::new();
				let mut seen_vals: Vec<Vec<u8>> = Vec::new();

				// One read of `limit` rows from each cursor; returns the rows read.
				macro_rules! read {
					($limit:expr) => {{
						let before = (seen_keys.len(), seen_vals.len());
						if visitor {
							let mut fk = |k: &[u8]| {
								seen_keys.push(k.to_vec());
								continue_()
							};
							keys_cursor.for_each($limit, &mut fk).await.unwrap();
							let mut fv = |k: &[u8], _: &[u8]| {
								seen_vals.push(k.to_vec());
								continue_()
							};
							vals_cursor.for_each($limit, &mut fv).await.unwrap();
						} else {
							let batch = keys_cursor.next_batch($limit).await.unwrap();
							seen_keys.extend(batch.iter().map(|k| k.to_vec()));
							let batch = vals_cursor.next_batch($limit).await.unwrap();
							seen_vals.extend(batch.iter().map(|(k, _)| k.to_vec()));
						}
						(seen_keys.len() - before.0, seen_vals.len() - before.1)
					}};
				}

				read!(2);
				tx.rollback_to_save_point().await.unwrap();
				while read!(4) != (0, 0) {}
				// Drop the cursors before finishing: backends that drain live
				// cursors during cancel wait for them to be released.
				drop(keys_cursor);
				drop(vals_cursor);

				let mut expected: Vec<Vec<u8>> = (0..10).step_by(2).map(key).collect();
				if dir == Direction::Backward {
					expected.reverse();
				}
				let case = format!("{dir:?}, exhausted={exhausted}, visitor={visitor}");
				assert_eq!(seen_keys, expected, "keys cursor, {case}");
				assert_eq!(seen_vals, expected, "vals cursor, {case}");
				tx.cancel().await.unwrap();
			}
		}
	}
}

kvs_test!(cursor_yields_entries_a_rollback_restores);

/// A scan cursor that opened with a `skip` resumes after the entries it
/// yielded, not from the start of the range: the entries it skipped are not
/// yielded again, and an entry a rollback restores ahead of it is.
async fn cursor_skip_survives_rollback(b: &TestBackend) {
	let key = |i: usize| format!("skp/key_{i:04}").into_bytes();
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in (0..10).step_by(2) {
		tx.set(key(i).into(), b"v".to_vec()).await.unwrap();
	}
	tx.commit().await.unwrap();
	for dir in [Direction::Forward, Direction::Backward] {
		let tx = ds.transaction(Write).await.unwrap();
		tx.new_save_point().await.unwrap();
		// Skipping two entries leaves 4 as the first one yielded in either
		// direction. The entry after it is hidden until the rollback.
		let hidden = match dir {
			Direction::Forward => 6,
			Direction::Backward => 2,
		};
		tx.del(key(hidden).into()).await.unwrap();
		let range = || KeyRange::from(b"skp/".as_slice()..b"skp0".as_slice());
		let mut keys_cursor = tx.open_keys_cursor(range(), dir, 2, None).await.unwrap();
		let mut vals_cursor = tx.open_vals_cursor(range(), dir, 2, None).await.unwrap();
		let mut seen_keys: Vec<Vec<u8>> =
			keys_cursor.next_batch(1).await.unwrap().iter().map(|k| k.to_vec()).collect();
		let mut seen_vals: Vec<Vec<u8>> =
			vals_cursor.next_batch(1).await.unwrap().iter().map(|(k, _)| k.to_vec()).collect();
		tx.rollback_to_save_point().await.unwrap();
		loop {
			let batch = keys_cursor.next_batch(4).await.unwrap();
			if batch.is_empty() {
				break;
			}
			seen_keys.extend(batch.iter().map(|k| k.to_vec()));
		}
		loop {
			let batch = vals_cursor.next_batch(4).await.unwrap();
			if batch.is_empty() {
				break;
			}
			seen_vals.extend(batch.iter().map(|(k, _)| k.to_vec()));
		}
		drop(keys_cursor);
		drop(vals_cursor);
		let expected: Vec<Vec<u8>> = match dir {
			Direction::Forward => vec![key(4), key(6), key(8)],
			Direction::Backward => vec![key(4), key(2), key(0)],
		};
		assert_eq!(seen_keys, expected, "keys cursor, {dir:?}");
		assert_eq!(seen_vals, expected, "vals cursor, {dir:?}");
		tx.cancel().await.unwrap();
	}
}

kvs_test!(cursor_skip_survives_rollback);

/// A zero-limit call leaves a cursor's pending `skip` unspent, so the skip is
/// applied on the first call that reads, in the view the transaction holds
/// then, even if a rollback has changed that view in between.
///
/// The first entry in scan order is hidden inside the savepoint, so skipping
/// two entries before the rollback would burn different entries than skipping
/// two after it.
async fn cursor_zero_limit_leaves_skip_pending(b: &TestBackend) {
	let key = |i: usize| format!("zlm/key_{i:04}").into_bytes();
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in (0..10).step_by(2) {
		tx.set(key(i).into(), b"v".to_vec()).await.unwrap();
	}
	tx.commit().await.unwrap();
	for dir in [Direction::Forward, Direction::Backward] {
		let tx = ds.transaction(Write).await.unwrap();
		tx.new_save_point().await.unwrap();
		let hidden = match dir {
			Direction::Forward => 0,
			Direction::Backward => 8,
		};
		tx.del(key(hidden).into()).await.unwrap();
		let range = || KeyRange::from(b"zlm/".as_slice()..b"zlm0".as_slice());
		let mut keys_cursor = tx.open_keys_cursor(range(), dir, 2, None).await.unwrap();
		let mut vals_cursor = tx.open_vals_cursor(range(), dir, 2, None).await.unwrap();
		// Zero-limit calls through both read paths visit and yield nothing.
		assert!(keys_cursor.next_batch(0).await.unwrap().is_empty(), "keys cursor, {dir:?}");
		assert!(vals_cursor.next_batch(0).await.unwrap().is_empty(), "vals cursor, {dir:?}");
		let mut visited = 0;
		let mut fk = |_: &[u8]| {
			visited += 1;
			Ok(std::ops::ControlFlow::Continue(()))
		};
		keys_cursor.for_each(0, &mut fk).await.unwrap();
		let mut fv = |_: &[u8], _: &[u8]| {
			visited += 1;
			Ok(std::ops::ControlFlow::Continue(()))
		};
		vals_cursor.for_each(0, &mut fv).await.unwrap();
		assert_eq!(visited, 0, "{dir:?}");
		tx.rollback_to_save_point().await.unwrap();
		let keys: Vec<Vec<u8>> =
			keys_cursor.next_batch(4).await.unwrap().iter().map(|k| k.to_vec()).collect();
		let vals: Vec<Vec<u8>> =
			vals_cursor.next_batch(4).await.unwrap().iter().map(|(k, _)| k.to_vec()).collect();
		drop(keys_cursor);
		drop(vals_cursor);
		let expected: Vec<Vec<u8>> = match dir {
			Direction::Forward => vec![key(4), key(6), key(8)],
			Direction::Backward => vec![key(4), key(2), key(0)],
		};
		assert_eq!(keys, expected, "keys cursor, {dir:?}");
		assert_eq!(vals, expected, "vals cursor, {dir:?}");
		tx.cancel().await.unwrap();
	}
}

kvs_test!(cursor_zero_limit_leaves_skip_pending);

/// Entries a cursor skipped are never yielded afterwards, even when the skip
/// used up everything the cursor could see and a rollback then restores an
/// entry beyond them.
///
/// The last entry in scan order is hidden inside the savepoint, so skipping
/// two entries consumes everything visible and the first read comes back
/// empty. Whether a backend then yields the restored entry is unspecified,
/// since the empty batch already signalled the end of the range. What it must
/// not do is hand back an entry the skip consumed.
async fn cursor_skipped_entries_stay_skipped(b: &TestBackend) {
	let key = |i: usize| format!("skx/key_{i:04}").into_bytes();
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in (0..6).step_by(2) {
		tx.set(key(i).into(), b"v".to_vec()).await.unwrap();
	}
	tx.commit().await.unwrap();
	for dir in [Direction::Forward, Direction::Backward] {
		let tx = ds.transaction(Write).await.unwrap();
		tx.new_save_point().await.unwrap();
		let (hidden, skipped) = match dir {
			Direction::Forward => (4, [key(0), key(2)]),
			Direction::Backward => (0, [key(4), key(2)]),
		};
		tx.del(key(hidden).into()).await.unwrap();
		let range = || KeyRange::from(b"skx/".as_slice()..b"skx0".as_slice());
		let mut keys_cursor = tx.open_keys_cursor(range(), dir, 2, None).await.unwrap();
		let mut vals_cursor = tx.open_vals_cursor(range(), dir, 2, None).await.unwrap();
		assert!(keys_cursor.next_batch(1).await.unwrap().is_empty(), "keys cursor, {dir:?}");
		assert!(vals_cursor.next_batch(1).await.unwrap().is_empty(), "vals cursor, {dir:?}");
		tx.rollback_to_save_point().await.unwrap();
		let keys: Vec<Vec<u8>> =
			keys_cursor.next_batch(4).await.unwrap().iter().map(|k| k.to_vec()).collect();
		let vals: Vec<Vec<u8>> =
			vals_cursor.next_batch(4).await.unwrap().iter().map(|(k, _)| k.to_vec()).collect();
		drop(keys_cursor);
		drop(vals_cursor);
		for s in &skipped {
			assert!(!keys.contains(s), "keys cursor yielded a skipped entry, {dir:?}");
			assert!(!vals.contains(s), "vals cursor yielded a skipped entry, {dir:?}");
		}
		tx.cancel().await.unwrap();
	}
}

kvs_test!(cursor_skipped_entries_stay_skipped);
