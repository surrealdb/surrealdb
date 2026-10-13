//! The apply half of a morsel write: the computed images are written in the order of the serial
//! run.
//!
//! Applying takes the morsels' results front morsel first and writes them on the statement's
//! transaction, so every write, event and index update lands in the order the serial run gives
//! it, and a failure surfaces at the row where the serial run would have failed.
//!
//! Candidates ([`Candidate`](super::write_compute::Candidate)) reach the coordinator in any
//! order, because workers finish their morsels in any order. [`ApplyWindow`] holds the ones that
//! arrive before their turn and hands the coordinator the candidate at the next position, and only
//! that one, so delivery order is the serial order whatever the arrival order. The bytes the
//! window holds are bounded: a candidate beyond the bound is handed back to its worker, except the
//! one at the next position, which is always admitted so the coordinator always progresses. When
//! the coordinator gives up on the computed candidates, [`ApplyWindow::revoke`] hands back every
//! held position for the coordinator to compute itself.
#![allow(dead_code, reason = "the morsel runtime is the first caller")]

use std::collections::BTreeMap;

use super::write_compute::{Candidate, Pos};

/// Candidates computed ahead of the coordinator, held until their position is next.
///
/// The coordinator applies position `p` only after every earlier position is finished, so a
/// candidate that arrives early waits here. The window admits the candidate at the next position
/// whatever its size, so the loop always progresses; any other candidate is admitted only while
/// the held bytes stay within the bound, and is otherwise handed back for its worker to park on.
///
/// The window never touches the memory account's `Lease`. The caller reserves `candidate.bytes`
/// before `offer`, and releases the bytes of what `take_next` returns, of what `offer` hands back
/// as `Full` or `Stale`, and of every position `revoke` drops.
pub(crate) struct ApplyWindow {
	/// The position the coordinator applies next.
	next: Pos,
	/// The admitted candidates, by position; every key is at or after `next`.
	held: BTreeMap<Pos, Candidate>,
	/// The sum of `bytes` over `held`, saturating.
	held_bytes: usize,
	/// The bound on `held_bytes` for a candidate that is not at the next position.
	max_bytes: usize,
}

/// The window's answer to an offered candidate.
pub(crate) enum Offer {
	/// The window holds the candidate.
	Held,
	/// The window is full: the worker parks and offers it again after the coordinator takes one.
	Full(Candidate),
	/// The position was already taken or is already held: a caller bug, which the coordinator
	/// raises as an internal error.
	Stale(Candidate),
}

impl ApplyWindow {
	/// An empty window whose next position is `first`, holding at most `max_bytes` beyond the
	/// candidate at the next position.
	pub(crate) fn new(first: Pos, max_bytes: usize) -> ApplyWindow {
		ApplyWindow {
			next: first,
			held: BTreeMap::new(),
			held_bytes: 0,
			max_bytes,
		}
	}

	/// The position the coordinator applies next.
	pub(crate) fn next(&self) -> Pos {
		self.next
	}

	/// The bytes of the candidates the window holds.
	pub(crate) fn held_bytes(&self) -> usize {
		self.held_bytes
	}

	/// Whether the window holds no candidate.
	pub(crate) fn is_empty(&self) -> bool {
		self.held.is_empty()
	}

	/// Admits `candidate` when it is at the next position, or when the held bytes plus its own
	/// stay within the bound; otherwise hands it back. A position before the next one, or one
	/// already held, is `Stale`.
	pub(crate) fn offer(&mut self, candidate: Candidate) -> Offer {
		if candidate.pos < self.next || self.held.contains_key(&candidate.pos) {
			return Offer::Stale(candidate);
		}
		let total = self.held_bytes.saturating_add(candidate.bytes);
		if candidate.pos != self.next && total > self.max_bytes {
			return Offer::Full(candidate);
		}
		self.held_bytes = total;
		self.held.insert(candidate.pos, candidate);
		Offer::Held
	}

	/// The candidate at the next position, if it has arrived; the next position advances past
	/// it and its bytes leave the window. The caller releases them from the memory account.
	pub(crate) fn take_next(&mut self) -> Option<Candidate> {
		let candidate = self.held.remove(&self.next)?;
		self.held_bytes = self.held_bytes.saturating_sub(candidate.bytes);
		self.next = self.next.next();
		Some(candidate)
	}

	/// Gives up every held candidate: the positions, ascending, that the coordinator must now
	/// compute itself. The next position is unchanged and the window holds nothing.
	pub(crate) fn revoke(&mut self) -> Vec<Pos> {
		self.held_bytes = 0;
		std::mem::take(&mut self.held).into_keys().collect()
	}
}

#[cfg(test)]
mod tests {
	use smallvec::SmallVec;

	use super::*;
	use crate::exec::vm::morsel::write_compute::Stop;

	fn candidate(pos: u64) -> Candidate {
		Candidate::new(Pos(pos), None, Stop::OffPath, SmallVec::new()).expect("consistent")
	}

	fn size() -> usize {
		candidate(0).bytes
	}

	#[test]
	fn takes_in_delivery_order_whatever_the_arrival_order() {
		let mut window = ApplyWindow::new(Pos(1), usize::MAX);
		for pos in [3, 1, 2] {
			assert!(matches!(window.offer(candidate(pos)), Offer::Held));
		}
		let mut taken = Vec::new();
		while let Some(candidate) = window.take_next() {
			taken.push(candidate.pos);
		}
		assert_eq!(taken, vec![Pos(1), Pos(2), Pos(3)]);
		assert!(window.take_next().is_none());
		assert_eq!(window.next(), Pos(4));
	}

	#[test]
	fn admits_the_next_position_over_the_bound() {
		let mut window = ApplyWindow::new(Pos(5), 0);
		assert!(matches!(window.offer(candidate(6)), Offer::Full(_)));
		assert!(matches!(window.offer(candidate(5)), Offer::Held));
		assert!(matches!(window.offer(candidate(7)), Offer::Full(_)));
		assert_eq!(window.held_bytes(), size());
	}

	#[test]
	fn full_hands_the_candidate_back() {
		let mut window = ApplyWindow::new(Pos(0), 0);
		match window.offer(candidate(3)) {
			Offer::Full(back) => assert_eq!(back.pos, Pos(3)),
			_ => panic!("a candidate beyond the bound is handed back as Full"),
		}
		assert!(window.is_empty());
		assert_eq!(window.held_bytes(), 0);
	}

	#[test]
	fn stale_on_taken_or_duplicate() {
		let mut window = ApplyWindow::new(Pos(0), usize::MAX);
		assert!(matches!(window.offer(candidate(0)), Offer::Held));
		assert!(matches!(window.offer(candidate(0)), Offer::Stale(_)));
		assert!(window.take_next().is_some());
		match window.offer(candidate(0)) {
			Offer::Stale(back) => assert_eq!(back.pos, Pos(0)),
			_ => panic!("a taken position is Stale"),
		}
		assert!(matches!(window.offer(candidate(2)), Offer::Held));
		assert!(matches!(window.offer(candidate(2)), Offer::Stale(_)));
	}

	#[test]
	fn revoke_returns_held_ascending_and_empties() {
		let mut window = ApplyWindow::new(Pos(2), usize::MAX);
		for pos in [6, 3, 5] {
			assert!(matches!(window.offer(candidate(pos)), Offer::Held));
		}
		assert_eq!(window.revoke(), vec![Pos(3), Pos(5), Pos(6)]);
		assert_eq!(window.held_bytes(), 0);
		assert!(window.is_empty());
		assert_eq!(window.next(), Pos(2));
		assert!(window.revoke().is_empty());
	}

	#[test]
	fn bytes_leave_on_take() {
		let mut window = ApplyWindow::new(Pos(0), usize::MAX);
		assert!(matches!(window.offer(candidate(1)), Offer::Held));
		assert!(matches!(window.offer(candidate(0)), Offer::Held));
		assert_eq!(window.held_bytes(), 2 * size());
		assert!(window.take_next().is_some());
		assert_eq!(window.held_bytes(), size());
		assert!(window.take_next().is_some());
		assert_eq!(window.held_bytes(), 0);
		assert!(window.is_empty());
	}
}
