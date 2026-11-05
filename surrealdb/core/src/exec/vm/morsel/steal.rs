//! Cooperative stealing and re-splitting: how a running morsel gives part of what it has left to
//! the morsels that wait.
//!
//! A morsel reads its range in chunks and a cursor is only ever touched by the thread that runs its
//! morsel, so a morsel is never taken from; it is asked, and gives. Each running morsel publishes a
//! [`Slot`]: an estimate of what it has left, and a flag a thief sets. At the end of a chunk the
//! owner reads where its cursor would continue, asks the datastore to split the remainder and
//! keeps the first part in scan order, narrowing its own range to it; the other parts are queued
//! with their order keys and merge exactly where the serial run reads them, between the owner's
//! narrowed range and whatever followed it. An owner whose remainder has no boundary declines, and
//! the thief asks another.
//!
//! Who asks, and what an owner donates, is the policy's: [`Policy::steal`], [`Policy::resplit`],
//! [`Policy::yield_place`]. This module acts on the answers.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use surrealdb_kvs::{Estimate, Key, RangeCursor, Snapshot, SplitTarget};

use super::places::Statement;
use super::set::{OrderKey, Shared};
use crate::exec::FlowResult;
use crate::exec::planner::placement::{Policy, Signals};
use crate::key::{AnyRange, RawRange};
use crate::kvs::Direction;

/// How a queued morsel came to be, for the counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Origin {
	/// Cut off a range by the policy's first split.
	Split,
	/// Donated at a chunk boundary: to an idle worker, or by a morsel that donates at every one.
	Stolen,
	/// Cut off by a morsel that would outlast its siblings.
	Resplit,
	/// The unread rest of a morsel that stopped at a chunk boundary.
	Parked,
}

/// What a running morsel publishes, and what a thief says to it.
#[derive(Debug, Default)]
pub(super) struct Slot {
	/// A thief asks the owner to donate at its next chunk boundary.
	requested: AtomicBool,
	/// Asks the owner has answered, by donating or declining.
	answers: AtomicU64,
	declines: AtomicU64,
	/// What the owner estimated it has left at its last chunk boundary. `left_keys` holds
	/// [`UNCOUNTED`] when the estimate counted no keys.
	left_keys: AtomicU64,
	left_bytes: AtomicU64,
	/// The owner's morsel has ended.
	ended: AtomicBool,
}

/// What [`Slot::left_keys`] holds for an estimate that counted no keys. No count reaches it: a
/// range holds fewer keys than there are values of a `u64`.
const UNCOUNTED: u64 = u64::MAX;

impl Slot {
	/// A thief asks the owner to donate.
	pub(super) fn request(&self) {
		self.requested.store(true, Ordering::Release);
	}

	/// Whether a thief has asked, clearing the ask.
	pub(super) fn take_request(&self) -> bool {
		self.requested.swap(false, Ordering::AcqRel)
	}

	/// The owner donated in answer to an ask, or by its own policy.
	pub(super) fn donated(&self) {
		self.answers.fetch_add(1, Ordering::Release);
	}

	/// The owner has no boundary to donate at.
	pub(super) fn declined(&self) {
		self.declines.fetch_add(1, Ordering::Release);
		self.answers.fetch_add(1, Ordering::Release);
	}

	pub(super) fn answers(&self) -> u64 {
		self.answers.load(Ordering::Acquire)
	}

	pub(super) fn declines(&self) -> u64 {
		self.declines.load(Ordering::Acquire)
	}

	/// The owner's estimate of what it has left, at its last chunk boundary.
	pub(super) fn publish(&self, left: Estimate) {
		self.left_keys.store(left.keys.unwrap_or(UNCOUNTED), Ordering::Relaxed);
		self.left_bytes.store(left.bytes, Ordering::Relaxed);
	}

	pub(super) fn left(&self) -> Estimate {
		Estimate {
			keys: Some(self.left_keys.load(Ordering::Relaxed)).filter(|&keys| keys != UNCOUNTED),
			bytes: self.left_bytes.load(Ordering::Relaxed),
			exact: false,
		}
	}

	pub(super) fn has_ended(&self) -> bool {
		self.ended.load(Ordering::Acquire)
	}

	/// The owner's morsel ended.
	pub(super) fn end(&self) {
		self.ended.store(true, Ordering::Release);
	}
}

/// A running morsel's registration in the scan's table, undone when it ends.
pub(super) struct Registered<'s> {
	shared: &'s Shared,
	slot: Arc<Slot>,
}

impl<'s> Registered<'s> {
	/// Register a running morsel with `shared`.
	pub(super) fn new(shared: &'s Shared) -> Self {
		let slot = Arc::new(Slot::default());
		shared.table().slots.push(Arc::clone(&slot));
		Self {
			shared,
			slot,
		}
	}

	pub(super) fn slot(&self) -> &Arc<Slot> {
		&self.slot
	}
}

impl Drop for Registered<'_> {
	fn drop(&mut self) {
		self.slot.end();
		self.shared.table().slots.retain(|slot| !Arc::ptr_eq(slot, &self.slot));
		self.shared.arrived.notify_waiters();
	}
}

/// How a remainder is cut: the edge up to which the owner keeps reading, and the parts it gives
/// away.
pub(super) struct Cut {
	/// Where the owner's range now ends in the direction of its scan: the exclusive end for a
	/// forward scan, the inclusive start for a backward one.
	pub(super) edge: Vec<u8>,
	/// The parts cut off, which lie after the owner's in the scan.
	pub(super) donated: Vec<RawRange>,
}

/// Cut `rest`, the unread remainder of a morsel scanning in `dir`, at `bounds`: strictly
/// ascending keys strictly inside it. The owner keeps the part that comes first in the scan.
pub(super) fn cut(rest: &RawRange, dir: Direction, bounds: Vec<Vec<u8>>) -> Cut {
	let (lo, hi) = (rest.bounds().0.to_vec(), rest.bounds().1.to_vec());
	let mut edges = Vec::with_capacity(bounds.len() + 2);
	edges.push(lo);
	edges.extend(bounds);
	edges.push(hi);
	let mut parts: Vec<RawRange> =
		edges.windows(2).map(|edge| rest.part(&edge[0], &edge[1])).collect();
	if dir == Direction::Backward {
		parts.reverse();
	}
	let mine = parts.remove(0);
	let edge = match dir {
		Direction::Forward => mine.bounds().1.to_vec(),
		Direction::Backward => mine.bounds().0.to_vec(),
	};
	Cut {
		edge,
		donated: parts,
	}
}

/// The remainder of a range whose metadata sized it before its first split. A known key count
/// removes the visited entries and scales bytes by their share, independent of key encoding.
/// Without a key count, the metadata's byte size bounds the remainder without subtracting the
/// scan's uncompressed bytes from a possibly compressed storage size.
pub(super) fn remaining_from_size(size: Estimate, visited: u64) -> Estimate {
	let keys = size.keys.map(|keys| keys.saturating_sub(visited));
	let bytes = match (size.keys, keys) {
		(Some(total), Some(left)) if total > 0 => {
			(u128::from(size.bytes) * u128::from(left) / u128::from(total)) as u64
		}
		(Some(0), _) => 0,
		_ => size.bytes,
	};
	Estimate {
		keys,
		bytes,
		exact: false,
	}
}

/// What a morsel has left, estimated from what it has read: the entries `visited` and their
/// `bytes`, the first key it read, `first`, and where reading continues, `at`, within `span`, the
/// range the morsel is reading. See [`remaining_between`]; the far end of the range is taken to be
/// the end of `span`, which holds keys of the whole key space when `span` is a table's.
pub(super) fn remaining(
	span: &RawRange,
	dir: Direction,
	first: &[u8],
	at: &[u8],
	visited: u64,
	bytes: u64,
) -> Estimate {
	let (lo, hi) = span.bounds();
	remaining_between(lo, hi, dir, first, at, visited, bytes)
}

/// What a scan has left, measured from what it has read and the keys that bound where it lies:
/// the entries `visited` and their `bytes`, the first key read, `first`, where reading continues,
/// `at`, and the bounds `lo` and `hi` of the keys the scan can still reach (for a forward scan
/// only `hi` is read, for a backward one only `lo`). The share of the keys read is measured from
/// `first` to the far bound on the key bytes the datastore splits on, and the entries read are
/// scaled by what remains of it. A share too small to measure leaves the scan as much as it has
/// read, and no scan is credited more than a thousand times what it has read. Bounds that are the
/// first and last keys the scan holds measure clustered keys, which the end of the key space does
/// not.
pub(super) fn remaining_between(
	lo: &[u8],
	hi: &[u8],
	dir: Direction,
	first: &[u8],
	at: &[u8],
	visited: u64,
	bytes: u64,
) -> Estimate {
	let read = if first.is_empty() {
		0.0
	} else {
		match dir {
			Direction::Forward => surrealdb_kvs::split::position_fraction(first, hi, at),
			Direction::Backward => {
				// The entries read lie between `at` and `first`, the first of them.
				let mut top = first.to_vec();
				top.push(0);
				1.0 - surrealdb_kvs::split::position_fraction(lo, &top, at)
			}
		}
	};
	let left = if read < 1.0 / 1024.0 {
		1.0
	} else {
		((1.0 - read) / read).clamp(0.0, 1024.0)
	};
	Estimate {
		keys: Some((visited as f64 * left) as u64),
		bytes: (bytes as f64 * left) as u64,
		exact: false,
	}
}

/// What a scan has left once the key at the far end of what it still reaches is known, `far`: the
/// last key a forward scan reaches, the first a backward one does. See [`remaining_between`].
pub(super) fn remaining_to(
	far: &[u8],
	dir: Direction,
	first: &[u8],
	at: &[u8],
	visited: u64,
	bytes: u64,
) -> Estimate {
	match dir {
		Direction::Forward => {
			let mut hi = far.to_vec();
			hi.push(0);
			remaining_between(&[], &hi, dir, first, at, visited, bytes)
		}
		Direction::Backward => remaining_between(far, &[], dir, first, at, visited, bytes),
	}
}

/// What a morsel read since it began, which its owner reports at a chunk boundary.
#[derive(Clone, Debug, Default)]
pub(super) struct Progress {
	/// The first key the morsel read; empty until it has read one.
	pub(super) first: Vec<u8>,
	pub(super) visited: u64,
	pub(super) bytes: u64,
	/// The entries of `visited` the pipeline's filters passed, as far as the coordinator counts
	/// them; a worker leaves it unread.
	pub(super) passed: u64,
	/// The entries read since the morsel was last cut, and every entry when it never was.
	pub(super) since_cut: u64,
	/// The times the morsel has been cut.
	pub(super) cuts: u32,
	/// The chunk boundaries the place has been held for. The pool is asked once this reaches
	/// the policy's quantum.
	pub(super) quanta: u32,
}

/// What a running morsel does with its remainder at a chunk boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Boundary {
	/// Keep reading the range.
	Go,
	/// Part of the range was given away; keep reading what remains.
	Donated,
	/// The morsel returns its place: stop here and queue what is left.
	Yield,
}

/// The decision of a morsel's owner at a chunk boundary, from the policy.
pub(super) struct Gate<'a> {
	pub(super) shared: &'a Shared,
	pub(super) slot: &'a Slot,
	pub(super) policy: Policy,
}

impl Gate<'_> {
	/// The part of the owner's remainder to cut off, if any, with its origin and whether a thief
	/// asked for it. A cut happens when a thief asks, when the policy makes every morsel donate,
	/// or when the policy asks for a re-split.
	pub(super) fn target(&self, signals: &Signals) -> Option<(SplitTarget, Origin, bool)> {
		let asked = self.slot.take_request();
		if asked || self.policy.steal_always() {
			return Some((SplitTarget::Parts(2), Origin::Stolen, asked));
		}
		self.policy.resplit(signals).map(|target| (target, Origin::Resplit, false))
	}

	/// The owner gave `parts` away, ordered by their keys for a scan in `dir`.
	pub(super) fn gave(&self, parts: Vec<RawRange>, dir: Direction, origin: Origin, asked: bool) {
		for part in parts {
			self.shared.queue(OrderKey::of(&part, dir), part, origin);
		}
		if asked {
			self.slot.donated();
		}
		self.shared.arrived.notify_waiters();
	}

	/// The owner found no boundary in its remainder.
	pub(super) fn found_none(&self, asked: bool) {
		if asked {
			self.slot.declined();
			self.shared.metrics().declined += 1;
			self.shared.arrived.notify_waiters();
		}
	}
}

/// A morsel a worker runs through a cursor on the transaction's read view, and its way to give
/// part of its range away.
pub(super) struct Donor<'a> {
	pub(super) gate: Gate<'a>,
	pub(super) view: &'a dyn Snapshot,
	pub(super) version: Option<u64>,
	pub(super) statement: &'a Statement,
	/// The context the morsel runs under, for the reads a test makes fail.
	#[cfg(test)]
	pub(super) ctx: &'a crate::exec::ExecutionContext,
}

impl Donor<'_> {
	/// At the end of a chunk of the morsel whose range is `own` and whose cursor is `cursor`:
	/// publish what is left, and either return the place, give part of the remainder away, or
	/// read on. A donation narrows `cursor` and `own` to the part kept, without a seek.
	pub(super) async fn boundary(
		&self,
		cursor: &mut dyn RangeCursor,
		own: &mut RawRange,
		dir: Direction,
		progress: &Progress,
	) -> FlowResult<Boundary> {
		let Some(resume) = cursor.resume() else {
			return Ok(Boundary::Go);
		};
		let current = own.clone().into_key_range();
		let ahead = resume.remainder(&current, dir);
		let rest = own.part(ahead.start.as_slice(), ahead.end.as_slice());
		let at = match dir {
			Direction::Forward => rest.bounds().0,
			Direction::Backward => rest.bounds().1,
		};
		let left = remaining(own, dir, &progress.first, at, progress.visited, progress.bytes);
		self.gate.slot.publish(left);
		// The pool is asked only once the quantum is spent.
		if self.gate.policy.yield_place(progress.quanta, true) && self.statement.others_wait() {
			return Ok(Boundary::Yield);
		}
		let signals = Signals {
			remaining_estimate: Some(left),
			queued: self.gate.shared.queued_len(),
			since_cut: progress.since_cut,
			cuts: progress.cuts,
			visited: progress.visited,
			visited_bytes: progress.bytes,
			..Signals::default()
		};
		let Some((target, origin, asked)) = self.gate.target(&signals) else {
			return Ok(Boundary::Go);
		};
		if rest.bounds().0 >= rest.bounds().1 {
			self.gate.found_none(asked);
			return Ok(Boundary::Go);
		}
		// A failed split finds no boundary. A cursor that refuses to narrow keeps the range it
		// has. In either case the owner reads on and gives nothing away.
		let split = self
			.view
			.split(rest.clone().into_key_range(), target, self.version)
			.await
			.map_err(|e| e.to_string());
		#[cfg(test)]
		let split = if super::test_hooks::split_fails(self.ctx) {
			Err("a split the test fails".to_owned())
		} else {
			split
		};
		let bounds = split.unwrap_or_else(|failure| {
			tracing::debug!(%failure, "a scan could not be split; its morsel reads on");
			Vec::new()
		});
		if bounds.is_empty() {
			self.gate.found_none(asked);
			return Ok(Boundary::Go);
		}
		let cut = cut(&rest, dir, bounds.iter().map(|key| key.as_slice().to_vec()).collect());
		#[cfg(test)]
		let refused = super::test_hooks::narrow_fails(self.ctx);
		#[cfg(not(test))]
		let refused = false;
		let narrowed = if refused {
			Err("a narrowing the test fails".to_owned())
		} else {
			cursor.narrow(Key::from(cut.edge.clone())).map_err(|e| e.to_string())
		};
		if let Err(failure) = narrowed {
			tracing::debug!(%failure, "a cursor could not be narrowed; its morsel reads on");
			self.gate.found_none(asked);
			return Ok(Boundary::Go);
		}
		*own = match dir {
			Direction::Forward => own.part(own.bounds().0, &cut.edge),
			Direction::Backward => own.part(&cut.edge, own.bounds().1),
		};
		self.gate.gave(cut.donated, dir, origin, asked);
		Ok(Boundary::Donated)
	}
}

impl Shared {
	/// The slot of the running morsel with the most left that `tried` does not hold.
	fn victim(&self, tried: &[Arc<Slot>]) -> Option<Arc<Slot>> {
		self.table()
			.slots
			.iter()
			.filter(|slot| !tried.iter().any(|seen| Arc::ptr_eq(seen, slot)))
			.max_by_key(|slot| slot.left().bytes)
			.cloned()
	}

	/// An idle worker holding a place asks the running morsel with the most left to donate, and
	/// waits for its answer. `true` when a morsel waits in the queue, which the worker claims;
	/// `false` when the worker returns its place: the scan is done, no morsel is running that has
	/// not declined, or another statement waits for a place. While every running morsel has too
	/// little left for the policy to ask it, the worker keeps its place and looks again.
	///
	/// A morsel answers at its next chunk boundary or when it ends, so the wait is bounded by a
	/// chunk. The worker looks again for a statement waiting for its place every few milliseconds,
	/// which no morsel's progress announces.
	#[cfg(not(target_family = "wasm"))]
	pub(super) async fn steal(
		&self,
		policy: Policy,
		statement: &Statement,
		tried: &mut Vec<Arc<Slot>>,
	) -> bool {
		loop {
			if self.stopped() || statement.others_wait() {
				return false;
			}
			if !self.table().queued.is_empty() {
				return true;
			}
			let Some(victim) = self.victim(tried) else {
				return false;
			};
			let signals = Signals {
				remaining_estimate: Some(victim.left()),
				..Signals::default()
			};
			if !policy.steal(&signals) {
				// Nothing running has enough left yet; one may by its next chunk boundary.
				let notified = self.arrived.notified();
				tokio::pin!(notified);
				notified.as_mut().enable();
				if self.stopped() || statement.others_wait() || !self.table().queued.is_empty() {
					continue;
				}
				let _ = tokio::time::timeout(WAITING_LOOKS, notified).await;
				continue;
			}
			let (answers, declines) = (victim.answers(), victim.declines());
			victim.request();
			self.metrics().steal_asks += 1;
			loop {
				let notified = self.arrived.notified();
				tokio::pin!(notified);
				notified.as_mut().enable();
				if self.stopped() || statement.others_wait() {
					return false;
				}
				if !self.table().queued.is_empty() {
					return true;
				}
				if victim.has_ended() || victim.answers() > answers {
					if victim.declines() > declines {
						tried.push(Arc::clone(&victim));
					}
					break;
				}
				let _ = tokio::time::timeout(WAITING_LOOKS, notified).await;
			}
		}
	}
}

/// How long an idle worker waits for a morsel's answer before it looks again for a statement that
/// waits for its place.
#[cfg(not(target_family = "wasm"))]
const WAITING_LOOKS: std::time::Duration = std::time::Duration::from_millis(2);

#[cfg(test)]
mod tests {
	use super::*;

	fn range(lo: &[u8], hi: &[u8]) -> RawRange {
		RawRange::of_bytes(lo, hi)
	}

	/// A forward owner keeps the first part, which the donated parts follow.
	#[test]
	fn a_forward_cut_keeps_the_first_part() {
		let cut = cut(&range(b"a", b"z"), Direction::Forward, vec![b"f".to_vec(), b"p".to_vec()]);
		assert_eq!(cut.edge, b"f".to_vec());
		let bounds: Vec<_> = cut.donated.iter().map(|part| part.bounds()).collect();
		assert_eq!(bounds, vec![(b"f".as_slice(), b"p".as_slice()), (b"p".as_slice(), b"z")]);
	}

	/// A backward owner keeps the part with the highest keys, and the parts donated lie below it.
	#[test]
	fn a_backward_cut_keeps_the_upper_part() {
		let cut = cut(&range(b"a", b"z"), Direction::Backward, vec![b"f".to_vec(), b"p".to_vec()]);
		assert_eq!(cut.edge, b"p".to_vec());
		let mut bounds: Vec<_> = cut.donated.iter().map(|part| part.bounds()).collect();
		bounds.sort();
		assert_eq!(bounds, vec![(b"a".as_slice(), b"f".as_slice()), (b"f", b"p")]);
	}

	/// A donated part's order key lies between its owner's and the one that followed the owner,
	/// forward and backward.
	#[test]
	fn a_donated_part_orders_between_its_owner_and_the_successor() {
		let owner = range(b"c", b"m");
		let successor = range(b"m", b"z");
		let forward = cut(&owner, Direction::Forward, vec![b"g".to_vec()]);
		let donated = OrderKey::of(&forward.donated[0], Direction::Forward);
		assert!(OrderKey::of(&owner, Direction::Forward) < donated);
		assert!(donated < OrderKey::of(&successor, Direction::Forward));
		// Scanning backward the successor is the range below the owner.
		let (owner, successor) = (range(b"m", b"z"), range(b"c", b"m"));
		let backward = cut(&owner, Direction::Backward, vec![b"t".to_vec()]);
		let donated = OrderKey::of(&backward.donated[0], Direction::Backward);
		assert!(OrderKey::of(&owner, Direction::Backward) < donated);
		assert!(donated < OrderKey::of(&successor, Direction::Backward));
	}

	/// What a morsel has left is scaled from what it has read; a position too small to measure
	/// leaves it as much as it has read.
	#[test]
	fn the_estimate_scales_and_is_never_unbounded() {
		let span = range(&[0, 0, 0, 0], &[0, 0, 1, 0]);
		let first = [0u8, 0, 0, 0];
		let half = remaining(&span, Direction::Forward, &first, &[0, 0, 0, 128], 100, 4_000);
		assert_eq!((half.keys, half.bytes), (Some(100), 4_000));
		let quarter = remaining(&span, Direction::Forward, &first, &[0, 0, 0, 64], 100, 4_000);
		assert_eq!(quarter.keys, Some(300));
		let unmeasurable = remaining(&range(b"a", b"a"), Direction::Forward, b"a", b"a", 10, 50);
		assert_eq!((unmeasurable.keys, unmeasurable.bytes), (Some(10), 50));
		let tiny = remaining(&span, Direction::Forward, &first, &[0, 0, 0, 0], 10, 50);
		assert_eq!(tiny.keys, Some(10));
		let unread = remaining(&span, Direction::Forward, &[], &[0, 0, 0, 0], 0, 0);
		assert_eq!(unread.keys, Some(0));
		// Scanning backward from the top of the range, a quarter read leaves three times that.
		let top = [0u8, 0, 0, 255];
		let backward = remaining(&span, Direction::Backward, &top, &[0, 0, 0, 192], 100, 4_000);
		assert!(backward.keys.is_some_and(|keys| (250..=350).contains(&keys)), "{backward:?}");
		// A key range whose lower bound is shorter than the keys read, as a table's is, does not
		// read as finished: the share is measured from the first key read.
		let table = range(&[1], &[2]);
		let hot = remaining(&table, Direction::Forward, &[1, 7, 0, 0], &[1, 7, 0, 5], 100, 4_000);
		assert!(hot.keys.is_some_and(|keys| keys >= 100), "{hot:?}");
	}

	/// A published estimate reads back as published, an uncounted one with no keys.
	#[test]
	fn a_published_estimate_keeps_whether_it_counted_keys() {
		let slot = Slot::default();
		for keys in [Some(0), Some(12), None] {
			slot.publish(Estimate {
				keys,
				bytes: 4_096,
				exact: false,
			});
			assert_eq!(slot.left().keys, keys);
			assert_eq!(slot.left().bytes, 4_096);
		}
	}

	/// An owner that declines is skipped by the thief, which asks the next largest.
	#[test]
	fn a_morsel_with_no_boundary_declines() {
		let shared = Shared::new();
		let first = Registered::new(&shared);
		let second = Registered::new(&shared);
		first.slot().publish(Estimate {
			keys: Some(100),
			bytes: 10_000,
			exact: false,
		});
		second.slot().publish(Estimate {
			keys: Some(10),
			bytes: 1_000,
			exact: false,
		});
		let mut tried = Vec::new();
		let victim = shared.victim(&tried).expect("a morsel runs");
		assert!(Arc::ptr_eq(&victim, first.slot()), "the largest remainder is asked first");
		// Its owner finds no boundary: it declines, and the thief skips it.
		victim.request();
		let gate = Gate {
			shared: &shared,
			slot: &victim,
			policy: Policy::from_config(&crate::exec::config::ExecConfig::default()),
		};
		let signals = Signals::default();
		let (_, origin, asked) = gate.target(&signals).expect("asked to donate");
		assert_eq!((origin, asked), (Origin::Stolen, true));
		gate.found_none(asked);
		assert_eq!((victim.answers(), victim.declines()), (1, 1));
		tried.push(victim);
		let next = shared.victim(&tried).expect("another morsel runs");
		assert!(Arc::ptr_eq(&next, second.slot()), "the thief takes another");
		tried.push(next);
		assert!(shared.victim(&tried).is_none(), "every morsel has declined");
		// A morsel that has ended is not offered.
		drop(second);
		assert_eq!(shared.table().slots.len(), 1);
	}

	/// A remainder of one entry has no boundary: the owner declines the thief's ask and reads on,
	/// and a remainder of several is cut with the first part kept.
	#[cfg(feature = "kv-surrealkv")]
	#[tokio::test(flavor = "multi_thread")]
	async fn a_one_row_remainder_declines_a_steal() {
		use std::borrow::Cow;

		use super::super::places;
		use crate::exec::operators::test_util::TestDb;
		use crate::key::schema::RecordPrefix;

		let dir = tempfile::tempdir().expect("temporary directory");
		let setup =
			"DEFINE TABLE t; CREATE t:1 SET n = 1; CREATE t:2 SET n = 2; CREATE t:3 SET n = 3;";
		let db =
			TestDb::new_on_surrealkv(setup, dir.path(), surrealdb_cnf::ConfigMap::empty()).await;
		let ctx = db.exec_ctx().await;
		let view = ctx.txn().snapshot().expect("a read-only transaction on SurrealKV gives a view");
		let db_ctx = ctx.database().expect("a database context");
		let table = crate::val::TableName::from("t".to_owned());
		let span = RecordPrefix {
			ns: db_ctx.ns_ctx.ns.namespace_id,
			db: db_ctx.db.database_id,
			tb: Cow::Borrowed(&table),
		}
		.range()
		.expect("the table's range");
		let shared = Shared::new();
		let running = Registered::new(&shared);
		let statement = places::register();
		let donor = Donor {
			gate: Gate {
				shared: &shared,
				slot: running.slot(),
				policy: Policy::from_config(&crate::exec::config::ExecConfig::default()),
			},
			view: &*view,
			version: None,
			statement: &statement,
			ctx: &ctx,
		};
		let mut cursor = view
			.open(span.clone().into_key_range(), Direction::Forward, None)
			.await
			.expect("opens");
		let mut visitor = |_: &[u8], _: &[u8]| Ok(std::ops::ControlFlow::Continue(()));
		// One entry read: two remain, which a cut divides.
		cursor.for_each(1, &mut visitor).await.expect("reads");
		let mut own = span.clone();
		let progress = Progress {
			first: span.bounds().0.to_vec(),
			visited: 1,
			bytes: 10,
			passed: 0,
			since_cut: 1,
			cuts: 0,
			quanta: 1,
		};
		running.slot().request();
		let outcome = donor.boundary(&mut *cursor, &mut own, Direction::Forward, &progress).await;
		assert_eq!(outcome.ok(), Some(Boundary::Donated));
		assert_eq!(running.slot().answers(), 1);
		assert_eq!(running.slot().declines(), 0);
		assert_eq!(shared.table().queued.len(), 1);
		// The owner holds one entry, which has no boundary to give.
		running.slot().request();
		let outcome = donor.boundary(&mut *cursor, &mut own, Direction::Forward, &progress).await;
		assert_eq!(outcome.ok(), Some(Boundary::Go));
		assert_eq!((running.slot().answers(), running.slot().declines()), (2, 1));
		assert_eq!(shared.table().queued.len(), 1, "nothing more was queued");
	}

	/// A morsel cuts its own remainder when nobody asked, while no morsel waits and the remainder
	/// and what it has read since it was last cut are large enough; a morsel that waits, or a read
	/// too short, leaves the remainder whole.
	#[cfg(feature = "kv-surrealkv")]
	#[tokio::test(flavor = "multi_thread")]
	async fn a_hot_morsel_cuts_its_own_remainder_while_none_waits() {
		use std::borrow::Cow;

		use super::super::places;
		use crate::exec::operators::test_util::TestDb;
		use crate::key::schema::RecordPrefix;

		let dir = tempfile::tempdir().expect("temporary directory");
		let setup = "DEFINE TABLE t; FOR $i IN 0..300 { CREATE type::record('t', $i) SET n = $i };";
		let db =
			TestDb::new_on_surrealkv(setup, dir.path(), surrealdb_cnf::ConfigMap::empty()).await;
		let ctx = db.exec_ctx().await;
		let view = ctx.txn().snapshot().expect("a read-only transaction on SurrealKV gives a view");
		let db_ctx = ctx.database().expect("a database context");
		let table = crate::val::TableName::from("t".to_owned());
		let span = RecordPrefix {
			ns: db_ctx.ns_ctx.ns.namespace_id,
			db: db_ctx.db.database_id,
			tb: Cow::Borrowed(&table),
		}
		.range()
		.expect("the table's range");
		let config = crate::exec::config::ExecConfig {
			morsel_target_bytes: 1_000,
			..Default::default()
		};
		let shared = Shared::new();
		let running = Registered::new(&shared);
		let statement = places::register();
		let donor = Donor {
			gate: Gate {
				shared: &shared,
				slot: running.slot(),
				policy: Policy::from_config(&config),
			},
			view: &*view,
			version: None,
			statement: &statement,
			ctx: &ctx,
		};
		let mut cursor = view
			.open(span.clone().into_key_range(), Direction::Forward, None)
			.await
			.expect("opens");
		let mut visitor = |_: &[u8], _: &[u8]| Ok(std::ops::ControlFlow::Continue(()));
		cursor.for_each(1, &mut visitor).await.expect("reads");
		let mut own = span.clone();
		// A read that holds three morsels' bytes and has run past the first chunks since its last
		// cut.
		let read_since = |since_cut, cuts| Progress {
			first: span.bounds().0.to_vec(),
			visited: 100,
			bytes: 3_000,
			passed: 0,
			since_cut,
			cuts,
			quanta: 0,
		};
		// Too few entries since the last cut: the remainder stays whole.
		let outcome =
			donor.boundary(&mut *cursor, &mut own, Direction::Forward, &read_since(10, 0)).await;
		assert_eq!(outcome.ok(), Some(Boundary::Go));
		assert_eq!(shared.metrics().resplit, 0);
		assert!(shared.table().queued.is_empty());
		// Enough read, and nobody waits: the morsel cuts its remainder without being asked.
		let since = 2 * u64::from(Policy::from_config(&config).scan_chunk());
		let outcome =
			donor.boundary(&mut *cursor, &mut own, Direction::Forward, &read_since(since, 0)).await;
		assert_eq!(outcome.ok(), Some(Boundary::Donated));
		assert!(shared.metrics().resplit > 0);
		assert_eq!(shared.metrics().stolen, 0);
		assert!(!shared.table().queued.is_empty());
		// The parts it gave wait for a place: it cuts nothing more, however much it has read.
		let waiting = shared.table().queued.len();
		let outcome = donor
			.boundary(&mut *cursor, &mut own, Direction::Forward, &read_since(since << 1, 1))
			.await;
		assert_eq!(outcome.ok(), Some(Boundary::Go));
		assert_eq!(shared.table().queued.len(), waiting);
	}

	/// A cursor that refuses to narrow leaves the owner with the whole of its remainder: nothing is
	/// queued, the owner's range is as it was, the thief's ask is declined, and a later boundary
	/// that narrows donates as it would have.
	#[cfg(feature = "kv-surrealkv")]
	#[tokio::test(flavor = "multi_thread")]
	async fn a_failed_narrow_gives_nothing_away() {
		use std::borrow::Cow;

		use super::super::places;
		use super::super::test_hooks::fail_narrows;
		use crate::exec::operators::test_util::TestDb;
		use crate::key::schema::RecordPrefix;

		let dir = tempfile::tempdir().expect("temporary directory");
		let setup =
			"DEFINE TABLE t; CREATE t:1 SET n = 1; CREATE t:2 SET n = 2; CREATE t:3 SET n = 3;";
		let db =
			TestDb::new_on_surrealkv(setup, dir.path(), surrealdb_cnf::ConfigMap::empty()).await;
		let ctx = db.exec_ctx().await;
		let view = ctx.txn().snapshot().expect("a read-only transaction on SurrealKV gives a view");
		let db_ctx = ctx.database().expect("a database context");
		let table = crate::val::TableName::from("t".to_owned());
		let span = RecordPrefix {
			ns: db_ctx.ns_ctx.ns.namespace_id,
			db: db_ctx.db.database_id,
			tb: Cow::Borrowed(&table),
		}
		.range()
		.expect("the table's range");
		let shared = Shared::new();
		let running = Registered::new(&shared);
		let statement = places::register();
		let donor = Donor {
			gate: Gate {
				shared: &shared,
				slot: running.slot(),
				policy: Policy::from_config(&crate::exec::config::ExecConfig::default()),
			},
			view: &*view,
			version: None,
			statement: &statement,
			ctx: &ctx,
		};
		let mut cursor = view
			.open(span.clone().into_key_range(), Direction::Forward, None)
			.await
			.expect("opens");
		let mut visitor = |_: &[u8], _: &[u8]| Ok(std::ops::ControlFlow::Continue(()));
		cursor.for_each(1, &mut visitor).await.expect("reads");
		let mut own = span.clone();
		let progress = Progress {
			first: span.bounds().0.to_vec(),
			visited: 1,
			bytes: 10,
			passed: 0,
			since_cut: 1,
			cuts: 0,
			quanta: 1,
		};
		let failing = fail_narrows(&ctx);
		running.slot().request();
		let outcome = donor.boundary(&mut *cursor, &mut own, Direction::Forward, &progress).await;
		assert_eq!(outcome.ok(), Some(Boundary::Go));
		assert_eq!(failing.refused(), 1);
		assert_eq!(own.bounds(), span.bounds(), "the owner's range is as it was");
		assert!(shared.table().queued.is_empty(), "nothing was queued");
		assert_eq!(shared.metrics().stolen, 0);
		assert_eq!((running.slot().answers(), running.slot().declines()), (1, 1));
		drop(failing);
		// The cursor still reads what it had left, so the next boundary donates part of it.
		running.slot().request();
		let outcome = donor.boundary(&mut *cursor, &mut own, Direction::Forward, &progress).await;
		assert_eq!(outcome.ok(), Some(Boundary::Donated));
		assert_eq!(shared.table().queued.len(), 1);
		assert_eq!(shared.metrics().stolen, 1);
	}
}
