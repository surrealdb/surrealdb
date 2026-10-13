//! An owned, shareable read view of one transaction, and the cursors that read through it.
//!
//! A [`Snapshot`] is what [`Transactable::snapshot`](crate::Transactable::snapshot) returns when
//! the backend can give a transaction's reads, read-only or writable, to code that does not hold
//! the transaction, such as another thread or many threads at once. [`RangeCursor`]s opened from a
//! view read in place, copy into a caller-owned [`Page`], and can be re-aimed at new ranges without
//! losing their iterator.
//!
//! # Contract
//!
//! - **Same view.** The view answers every key and range exactly as the transaction's own `get`,
//!   `getm`, `scan` and cursor reads did when `snapshot()` returned. That is the committed data at
//!   the transaction's snapshot, plus every put and delete the transaction had buffered at that
//!   point. This holds for writable transactions too.
//! - **Immutable.** Writes, deletes, savepoint pushes, releases and rollbacks made by the owner
//!   after `snapshot()` returned are not visible to the view or its cursors. Commits by other
//!   transactions are not visible either.
//! - **Concurrent with the owner.** The owner may keep writing, rolling back savepoints and reading
//!   while cursors from the view run on other threads. Neither side waits for the other, except as
//!   described under keep-alive, and except where a backend serialises individual reads on an
//!   internal lock.
//! - **Refusal.** `Transactable::snapshot` returns `None` whenever the backend cannot provide the
//!   view above at that moment. That covers: no view support, buffered writes that cannot be
//!   frozen, a capture larger than the backend's bound, a contended internal lock, or a finished
//!   transaction. `None` is always correct; the caller then reads through the transaction.
//! - **Static capability.**
//!   [`Transactable::view_capabilities`](crate::Transactable::view_capabilities) is a fact of the
//!   datastore and an upper bound on what `snapshot()` returns. When `writable` is `false`,
//!   `snapshot()` returns `None` for every writable transaction. When `read_only` is `false`, it
//!   returns `None` for every read-only transaction. A `true` flag means `snapshot()` may return
//!   `Some` for a live transaction of that kind, but `None` is still possible at run time. A caller
//!   therefore treats the flag as eligibility and reads through the transaction when no view comes.
//!   A backend that promises more says so in its own docs.
//! - **Versions.** A read at `version = Some(v)` returns what the transaction's read at `v`
//!   returned when the view was taken. A backend that cannot provide that over buffered writes
//!   returns `None` from `snapshot()` whenever any writes are buffered.
//! - **Conflict tracking.** View reads join the owner's conflict set if and only if
//!   [`Transactable::plain_reads_tracked`](crate::Transactable::plain_reads_tracked) is `true`. A
//!   backend that tracks plain reads adds each view read to the owner's read set from any thread,
//!   or returns `None` from `snapshot()` for writable transactions.
//! - **No locks.** Nothing read through a view takes a lock the way
//!   [`Transactable::getu`](crate::Transactable::getu) does, and nothing read through a view
//!   writes.
//! - **Held bytes.** [`Snapshot::held_bytes`] is the number of bytes the view holds only because it
//!   was taken. It is constant for the life of the view, and `0` for a view over the backend's own
//!   snapshot.
//! - **Batched reads.** [`Snapshot::get_many`] returns what the transaction's
//!   [`getm`](crate::Transactable::getm) returned at capture, with one entry per input key, in
//!   input order.
//! - **Independent cursors.** Cursors opened from one view advance on different threads with no
//!   lock shared between them on the read path.
//! - **Keep-alive.** The transaction's commit and cancel wait until every cursor opened from the
//!   view is dropped. Cursors that are already open stay readable until they are dropped.
//! - **Finished transaction.** Once the transaction has committed or cancelled, [`Snapshot::open`]
//!   and [`Snapshot::get_many`] fail with
//!   [`Error::TransactionFinished`](crate::Error::TransactionFinished), for any keys, including an
//!   empty list.
//! - **Consumed entries.** An entry passed to a visitor is consumed whatever the visitor returns,
//!   as with [`ScanCursorVals::for_each`](crate::api::ScanCursorVals::for_each). A `Break` stops
//!   the chunk after that entry, and [`RangeCursor::resume`] names the entry after it.
//!
//! A backend that implements none of this returns `None` from `snapshot()`, declares the default
//! [`ViewCapabilities`], and inherits valid [`Estimate`]s and split boundaries from
//! [`Transactable`](crate::Transactable)'s defaults.

use std::ops::ControlFlow;

use common::future::BoxFut;

use crate::Direction;
use crate::api::{GetMultiResult, ScanChunkStats, ValVisitor};
use crate::err::Result;
use crate::page::Page;
use crate::split::{
	ESTIMATE_BUDGET, Estimate, SplitTarget, default_split, estimate_from_prefix, parts_for,
};
use crate::types::{Key, KeyRange};

/// What read views a backend can hand out, the same for every transaction of one datastore and
/// its configuration, so a planner may read it once as a fact of the datastore.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ViewCapabilities {
	/// `Transactable::snapshot` may return `Some` for a read-only transaction. `false` means it
	/// always returns `None` for one.
	pub read_only: bool,
	/// `Transactable::snapshot` may return `Some` for a writable transaction. `false` means it
	/// always returns `None` for one.
	pub writable: bool,
}

/// An owned, shareable read view of one transaction, taken at one moment. See the
/// [module docs](self) for the contract every implementation upholds.
pub trait Snapshot: Send + Sync + 'static {
	/// The bytes this view holds only because it was taken, constant for its life: `0` for a view
	/// over the backend's own snapshot. A caller charges it to its memory account when it takes
	/// the view and releases it when the view is dropped.
	fn held_bytes(&self) -> u64 {
		0
	}

	/// A cursor over `rng` in `dir`, as of `version` when given. Reads nothing until it is driven.
	fn open<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn RangeCursor + 'a>>>;

	/// Fetch many keys as of `version` when given, in the manner of
	/// [`Transactable::getm`](crate::Transactable::getm): one entry per input key in input order,
	/// a miss as `None`, with `records` the number of hits and `value_bytes` their total length.
	///
	/// Conformance contract: the answer equals the transaction's `getm` at the moment the view was
	/// taken. The default opens one cursor per key over `[k, k⁺)`, where `k⁺` is `k` with a `0x00`
	/// byte appended, and takes its first entry; a backend where each cursor is expensive
	/// overrides it with a native batched read.
	fn get_many<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, Result<GetMultiResult>> {
		Box::pin(async move {
			let mut values = Vec::with_capacity(keys.len());
			let mut records = 0u64;
			let mut value_bytes = 0u64;
			for key in keys {
				let rng = KeyRange {
					start: key.as_borrowed(),
					end: key.as_borrowed().next(),
				};
				let mut cursor = self.open(rng, Direction::Forward, version).await?;
				let mut found = None;
				cursor
					.for_each(1, &mut |_, val| {
						found = Some(val.to_vec());
						Ok(ControlFlow::Break(()))
					})
					.await?;
				if let Some(val) = &found {
					records += 1;
					value_bytes += val.len() as u64;
				}
				values.push(found);
			}
			Ok(GetMultiResult {
				values,
				records,
				value_bytes,
			})
		})
	}

	/// What `rng` holds. The default reads up to [`ESTIMATE_BUDGET`] entries through
	/// [`open`](Self::open): exact when the range ends within the budget, otherwise extrapolated
	/// with [`position_fraction`](crate::split::position_fraction) and not exact.
	fn estimate<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Estimate>> {
		Box::pin(async move {
			let mut cursor = self.open(rng.as_borrowed(), Direction::Forward, version).await?;
			let mut last = Vec::new();
			let stats = cursor
				.for_each(ESTIMATE_BUDGET, &mut |key, _| {
					last.clear();
					last.extend_from_slice(key);
					Ok(ControlFlow::Continue(()))
				})
				.await?;
			let last = (stats.rows > 0).then_some(last.as_slice());
			Ok(estimate_from_prefix(&rng, stats.rows, stats.key_bytes + stats.value_bytes, last))
		})
	}

	/// Boundary keys that divide `rng` into parts, strictly inside it and strictly ascending; empty
	/// when the range holds no more than one key. The default finds the first and last key with one
	/// seek each, then cuts between them with [`split_range`](crate::split::split_range). For
	/// [`SplitTarget::Bytes`] the part count is `ceil(estimate.bytes / max(b, 1))`, and it is
	/// clamped to `[1, MAX_SPLIT_PARTS]`.
	fn split<'a>(
		&'a self,
		rng: KeyRange<'a>,
		target: SplitTarget,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Vec<Key<'static>>>> {
		Box::pin(async move {
			if rng.is_empty() {
				return Ok(Vec::new());
			}
			let mut cursor = self.open(rng.as_borrowed(), Direction::Forward, version).await?;
			let first = first_key(&mut *cursor).await?;
			cursor.aim(rng.as_borrowed(), Direction::Backward)?;
			let last = first_key(&mut *cursor).await?;
			drop(cursor);
			let (Some(first), Some(last)) = (first, last) else {
				return Ok(Vec::new());
			};
			let parts = match target {
				SplitTarget::Parts(_) => parts_for(target, || 0),
				SplitTarget::Bytes(_) => {
					let bytes = self.estimate(rng.as_borrowed(), version).await?.bytes;
					parts_for(target, || bytes)
				}
			};
			Ok(default_split(&first, &last, parts, &rng))
		})
	}

	/// As [`Transactable::count_bounded`](crate::Transactable::count_bounded), against this view.
	/// The default reads up to `budget` keys through [`open`](Self::open).
	fn count_bounded<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
		budget: u32,
	) -> BoxFut<'a, Result<(usize, Option<Key<'static>>)>> {
		Box::pin(async move {
			let budget = budget.max(1);
			let mut cursor = self.open(rng.as_borrowed(), Direction::Forward, version).await?;
			let stats = cursor.for_each(budget, &mut |_, _| Ok(ControlFlow::Continue(()))).await?;
			let counted = stats.rows as usize;
			let resume = match cursor.resume() {
				Some(at) if stats.rows == u64::from(budget) => {
					Some(at.remainder(&rng, Direction::Forward).start.into_static())
				}
				_ => None,
			};
			Ok((counted, resume))
		})
	}
}

/// The key of the first entry `cursor` reads, or `None` when it reads none.
async fn first_key(cursor: &mut dyn RangeCursor) -> Result<Option<Vec<u8>>> {
	let mut key = Vec::new();
	let stats = cursor
		.for_each(1, &mut |entry, _| {
			key.extend_from_slice(entry);
			Ok(ControlFlow::Break(()))
		})
		.await?;
	Ok((stats.rows > 0).then_some(key))
}

/// A re-aimable cursor over one [`Snapshot`].
pub trait RangeCursor: Send {
	/// Point the cursor at `rng` in `dir`, keeping its iterator and buffers. Never reads. The
	/// cursor keeps the version it was opened at.
	fn aim(&mut self, rng: KeyRange<'_>, dir: Direction) -> Result<()>;

	/// Shrink the range still to be read without re-seeking: forward, `end` becomes the exclusive
	/// end; backward, `end` becomes the inclusive start. A bound outside the range still to be read
	/// is an error. An error leaves the range still to be read unchanged, so a caller that gives
	/// nothing away on a failure reads the whole of it. Used when a running range gives up its tail
	/// or is split.
	fn narrow(&mut self, end: Key<'_>) -> Result<()>;

	/// Visit up to `limit` entries in place; `limit == 0` visits nothing. The visitor contract is
	/// that of [`ScanCursorVals::for_each`](crate::api::ScanCursorVals::for_each): an entry handed
	/// to the visitor is consumed whatever it answers, so a visitor `Break` leaves the cursor
	/// resumable at the entry after it. After a visitor error the resume position is unspecified.
	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>>;

	/// Copy up to `limit` entries into `page`, after clearing it, stopping before the first entry
	/// the page cannot hold; the first entry is always taken, the page growing for it if need be.
	/// An entry the page cannot hold stays unread. Only the backend's own iterator can guarantee
	/// this, so there is no default.
	fn fill<'s>(&'s mut self, page: &'s mut Page, limit: u32)
	-> BoxFut<'s, Result<ScanChunkStats>>;

	/// Where the cursor continues, or `None` once the range is exhausted.
	fn resume(&self) -> Option<Resume>;
}

/// An opaque resume position: reading [`remainder`](Self::remainder) next visits exactly the
/// entries a cursor has not yet visited.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resume(Key<'static>);

impl Resume {
	/// The position at `key`: forward, the first entry still to read is at or after it; backward,
	/// the entries still to read are before it.
	pub fn new(key: Key<'static>) -> Self {
		Self(key)
	}

	/// The range left to read of `of`. Forward: `[resume, of.end)`. Backward: `[of.start, resume)`.
	pub fn remainder<'a>(&'a self, of: &KeyRange<'a>, dir: Direction) -> KeyRange<'a> {
		match dir {
			Direction::Forward => KeyRange {
				start: self.0.as_borrowed(),
				end: of.end.clone(),
			},
			Direction::Backward => KeyRange {
				start: of.start.clone(),
				end: self.0.as_borrowed(),
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A view that opens nothing: only the defaulted methods are called on it.
	struct Finished;

	impl Snapshot for Finished {
		fn open<'a>(
			&'a self,
			_rng: KeyRange<'a>,
			_dir: Direction,
			_version: Option<u64>,
		) -> BoxFut<'a, Result<Box<dyn RangeCursor + 'a>>> {
			Box::pin(async { Err(crate::err::Error::TransactionFinished) })
		}
	}

	#[test]
	fn view_capabilities_default_is_no_view() {
		assert_eq!(
			ViewCapabilities::default(),
			ViewCapabilities {
				read_only: false,
				writable: false
			}
		);
	}

	#[test]
	fn held_bytes_defaults_to_zero() {
		assert_eq!(Finished.held_bytes(), 0);
	}
}
