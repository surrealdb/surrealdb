//! The save-point frame stack a per-transaction write buffer keeps.
//!
//! A buffer that describes writes a transaction has made cannot be one flat map:
//! a rollback that undoes those writes has to reach the description of them too.
//! So each such buffer holds a stack of frames mirroring the storage save-point
//! stack — [`Transaction::new_save_point`](crate::tx::Transaction::new_save_point)
//! pushes one, [`release_last_save_point`](crate::tx::Transaction::release_last_save_point)
//! folds it into its parent, and
//! [`rollback_to_save_point`](crate::tx::Transaction::rollback_to_save_point)
//! discards it.
//!
//! Every write goes to the top frame, so frames below it hold strictly older
//! writes. That is what makes folding the stack downward preserve the order the
//! writes were made in, which an ordered buffer — the change feed's mutations,
//! the live-query events — depends on.
//!
//! The bottom frame is the transaction's own and is never popped: the storage
//! layer has no save point matching it, and every operation assumes it is there.

use std::collections::HashMap;

/// Pops a save-point frame off `frames`, or `None` when only the transaction's
/// own frame remains.
pub(crate) fn pop_save_point<K, V>(frames: &mut Vec<HashMap<K, V>>) -> Option<HashMap<K, V>> {
	(frames.len() > 1).then(|| frames.pop()).flatten()
}

/// Drops every save-point frame and empties the transaction's own, leaving the
/// stack at the floor depth every other operation assumes.
pub(crate) fn reset_frames<K, V>(frames: &mut Vec<HashMap<K, V>>) {
	frames.truncate(1);
	match frames.first_mut() {
		Some(frame) => frame.clear(),
		None => frames.push(HashMap::new()),
	}
}

/// Folds every open save-point frame into the transaction's own, `fold` merging
/// one entry into the entry already there for that key.
///
/// Frames fold from the bottom up, so entries keep the order they were buffered
/// in. A caller flushing the buffer uses this to read one frame rather than the
/// stack: a scope still open at the commit belongs to the transaction, since the
/// writes it describes are the ones about to be committed.
pub(crate) fn fold_frames<K, V>(frames: &mut Vec<HashMap<K, V>>, fold: impl Fn(&mut V, V))
where
	K: std::hash::Hash + Eq,
{
	if frames.len() < 2 {
		return;
	}
	let (bottom, open) = frames.split_at_mut(1);
	let parent = bottom.first_mut().expect("the transaction frame is always present");
	for frame in open {
		for (key, value) in std::mem::take(frame) {
			match parent.get_mut(&key) {
				Some(entry) => fold(entry, value),
				None => {
					parent.insert(key, value);
				}
			}
		}
	}
	frames.truncate(1);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;

	fn frames(depth: usize) -> Vec<HashMap<&'static str, Vec<u8>>> {
		(0..depth).map(|_| HashMap::new()).collect()
	}

	#[test]
	fn the_transaction_frame_is_never_popped() {
		let mut frames = frames(1);
		assert!(pop_save_point(&mut frames).is_none());
		assert_eq!(frames.len(), 1, "the floor every other operation assumes has to stay");
	}

	#[test]
	fn a_reset_leaves_one_empty_frame() {
		let mut frames = frames(3);
		frames[0].insert("a", vec![1]);
		frames[2].insert("b", vec![2]);
		reset_frames(&mut frames);
		assert_eq!(frames.len(), 1);
		assert!(frames[0].is_empty());
	}

	/// An entry buffered in an outer scope is older than one buffered in a scope
	/// opened after it, so folding has to put it first.
	#[test]
	fn folding_keeps_the_order_the_entries_were_buffered_in() {
		let mut frames = frames(3);
		frames[0].insert("t", vec![1, 2]);
		frames[1].insert("t", vec![3]);
		frames[2].insert("t", vec![4, 5]);
		frames[1].insert("u", vec![9]);

		fold_frames(&mut frames, |entry: &mut Vec<u8>, value| entry.extend(value));

		assert_eq!(frames.len(), 1);
		assert_eq!(frames[0]["t"], vec![1, 2, 3, 4, 5]);
		assert_eq!(frames[0]["u"], vec![9], "a key only an open scope holds still reaches it");
	}
}
