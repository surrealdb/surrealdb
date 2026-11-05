//! The read view of a read-only memory transaction, and the cursors it opens.
//!
//! A read-only transaction holds one fixed engine snapshot and an empty write set, so every read
//! through its `Tx` is the view, from any thread. A [`MemSnapshot`] shares the transaction's `Tx`
//! and its `done` flag and copies nothing, so [`Snapshot::held_bytes`] stays at its default of `0`.
//! Writable transactions give no view: the engine's reads always merge the transaction's write set,
//! so a later write by the owner would show through.
//!
//! Each cursor holds an owned read guard on the `Tx`, which is its keep-alive. `cancel` is the only
//! writer of a read-only transaction's lock, and it sets `done` before it queues for the lock, so
//! it waits for every cursor to drop while [`Snapshot::open`] and [`Snapshot::get_many`] refuse
//! with [`Error::TransactionFinished`]. The view itself holds no guard and never delays `cancel`.
//!
//! The engine's iterators borrow the `Tx`, so a cursor keeps none between chunks. Every chunk
//! builds an iterator over what is left of its range, reads from it, and drops it before
//! returning. The transaction's snapshot is fixed, so the entries read across chunks are those of
//! one continuous iteration. The memory backend keeps no versions, so a read as of one is refused.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use surrealdb_kvs::api::{BoxFut, GetMultiResult, ScanChunkStats, ValVisitor};
use surrealdb_kvs::err::{Error, Result};
use surrealdb_kvs::{Direction, Key, KeyRange, Page, RangeCursor, Resume, Snapshot};
use surrealmx::Transaction as Tx;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

use crate::kvs_error;

const _: fn() = || {
	fn send_sync<T: Send + Sync>() {}
	send_sync::<Tx>();
	send_sync::<OwnedRwLockReadGuard<Tx>>();
};

/// The read view of a read-only transaction. See the [module docs](self).
pub(super) struct MemSnapshot {
	/// The transaction's engine handle, shared with the owner.
	pub(super) inner: Arc<RwLock<Tx>>,
	/// Whether the transaction has committed or cancelled.
	pub(super) done: Arc<AtomicBool>,
}

impl MemSnapshot {
	/// The keep-alive a read takes on the transaction.
	///
	/// A lock that cannot be taken without waiting means `cancel` is queued for it, so the
	/// transaction is finishing. Waiting would let a worker that holds one cursor deadlock against
	/// a `cancel` that waits for that cursor. `done` is read again once the guard is held:
	/// `cancel` sets it before it queues, so a guard taken while it is still clear is ahead of
	/// `cancel` and holds the engine snapshot `cancel` has yet to take away.
	fn hold(&self, version: Option<u64>) -> Result<OwnedRwLockReadGuard<Tx>> {
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		if self.done.load(Ordering::Acquire) {
			return Err(Error::TransactionFinished);
		}
		let guard =
			Arc::clone(&self.inner).try_read_owned().map_err(|_| Error::TransactionFinished)?;
		if self.done.load(Ordering::Acquire) {
			return Err(Error::TransactionFinished);
		}
		Ok(guard)
	}
}

impl Snapshot for MemSnapshot {
	fn open<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn RangeCursor + 'a>>> {
		Box::pin(async move {
			let guard = self.hold(version)?;
			Ok(Box::new(MemRangeCursor::new(guard, &rng, dir)) as Box<dyn RangeCursor + 'a>)
		})
	}

	/// One guard and one batched read, the read the transaction's own `getm` makes.
	fn get_many<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, Result<GetMultiResult>> {
		Box::pin(async move {
			let guard = self.hold(version)?;
			let found =
				guard.getm(keys.iter().map(|key| key.as_slice()).collect()).map_err(kvs_error)?;
			let mut records = 0u64;
			let mut value_bytes = 0u64;
			let values = found
				.into_iter()
				.map(|val| {
					val.map(|val| {
						records += 1;
						value_bytes += val.len() as u64;
						val.to_vec()
					})
				})
				.collect();
			Ok(GetMultiResult {
				values,
				records,
				value_bytes,
			})
		})
	}
}

/// What a chunk hands each entry to.
enum Sink<'s> {
	/// A visitor, which consumes every entry it is handed.
	Visit(&'s mut dyn ValVisitor),
	/// A page, which refuses the entry it cannot hold.
	Fill(&'s mut Page),
}

/// What a [`Sink`] did with an entry.
enum Taken {
	/// The entry is consumed, and the visitor's answer says whether to go on.
	Consumed(ControlFlow<()>),
	/// The entry is not consumed and stays to be read.
	Refused,
}

impl Sink<'_> {
	fn take(&mut self, key: &[u8], val: &[u8]) -> Result<Taken> {
		Ok(match self {
			Sink::Visit(f) => Taken::Consumed(f(key, val)?),
			Sink::Fill(page) => {
				if page.push(key, val) {
					Taken::Consumed(ControlFlow::Continue(()))
				} else {
					Taken::Refused
				}
			}
		})
	}
}

/// A re-aimable cursor over a [`MemSnapshot`].
///
/// `lo` and `hi` bound the range still to be read, half-open. `resume` is where reading continues
/// once something has been read: forward, the key of the first entry not yet consumed; backward,
/// the exclusive upper bound of the entries not yet consumed. Every buffer is allocated once and
/// refilled in place.
pub(super) struct MemRangeCursor {
	/// The keep-alive, and the engine handle every chunk reads through.
	guard: OwnedRwLockReadGuard<Tx>,
	/// Inclusive lower bound of the range still to be read.
	lo: Vec<u8>,
	/// Exclusive upper bound of the range still to be read.
	hi: Vec<u8>,
	/// Direction the range is read in.
	dir: Direction,
	/// Where reading continues once a chunk has been read; meaningful only when `resumed`.
	resume: Vec<u8>,
	/// Whether a chunk has been read since the cursor was aimed.
	resumed: bool,
	/// Whether nothing is left to read.
	exhausted: bool,
}

impl MemRangeCursor {
	fn new(guard: OwnedRwLockReadGuard<Tx>, rng: &KeyRange<'_>, dir: Direction) -> Self {
		Self {
			guard,
			lo: rng.start.to_vec(),
			hi: rng.end.to_vec(),
			dir,
			resume: Vec::new(),
			resumed: false,
			exhausted: rng.is_empty(),
		}
	}

	/// Read up to `limit` entries into `sink`, then record where reading continues.
	fn read(&mut self, limit: u32, sink: &mut Sink<'_>) -> Result<ScanChunkStats> {
		let mut stats = ScanChunkStats::default();
		if limit == 0 || self.exhausted {
			return Ok(stats);
		}
		let tx: &Tx = &self.guard;
		let Self {
			lo,
			hi,
			dir,
			resume,
			resumed,
			exhausted,
			..
		} = self;
		let dir = *dir;
		// A chunk reads what is left: past the resume point once there is one.
		let (start, end): (&[u8], &[u8]) = match (*resumed, dir) {
			(false, _) => (lo, hi),
			(true, Direction::Forward) => (resume, hi),
			(true, Direction::Backward) => (lo, resume),
		};
		if start >= end {
			*exhausted = true;
			return Ok(stats);
		}
		let mut iter = match dir {
			Direction::Forward => tx.scan_iter(start..end),
			Direction::Backward => tx.scan_iter_reverse(start..end),
		}
		.map_err(kvs_error)?;
		// The key of the first entry the chunk leaves unread, when one is left.
		let mut left = None;
		while let Some((key, val)) = iter.next() {
			if stats.rows >= u64::from(limit) {
				left = Some(key);
				break;
			}
			match sink.take(&key, &val)? {
				Taken::Refused => {
					left = Some(key);
					break;
				}
				Taken::Consumed(flow) => {
					stats.rows += 1;
					stats.key_bytes += key.len() as u64;
					stats.value_bytes += val.len() as u64;
					if flow.is_break() {
						left = iter.next().map(|(key, _)| key);
						break;
					}
				}
			}
		}
		drop(iter);
		match left {
			Some(key) => {
				resume.clear();
				resume.extend_from_slice(&key);
				if matches!(dir, Direction::Backward) {
					// The entry is still to be read, and backward the resume point is an exclusive
					// upper bound. The smallest key after it is the key with a zero byte appended.
					resume.push(0);
				}
				*resumed = true;
			}
			None => *exhausted = true,
		}
		Ok(stats)
	}
}

impl RangeCursor for MemRangeCursor {
	fn aim(&mut self, rng: KeyRange<'_>, dir: Direction) -> Result<()> {
		self.lo.clear();
		self.lo.extend_from_slice(&rng.start);
		self.hi.clear();
		self.hi.extend_from_slice(&rng.end);
		self.dir = dir;
		self.resumed = false;
		self.exhausted = rng.is_empty();
		Ok(())
	}

	fn narrow(&mut self, end: Key<'_>) -> Result<()> {
		let end = end.as_slice();
		if self.exhausted {
			return Err(Error::Internal("cannot narrow a cursor with nothing left to read".into()));
		}
		match self.dir {
			Direction::Forward => {
				let from = if self.resumed {
					self.resume.as_slice()
				} else {
					self.lo.as_slice()
				};
				if end < from || end > self.hi.as_slice() {
					return Err(Error::Internal(
						"the narrowed end lies outside the range left to read".into(),
					));
				}
				self.exhausted = end == from;
				self.hi.clear();
				self.hi.extend_from_slice(end);
			}
			Direction::Backward => {
				let upto = if self.resumed {
					self.resume.as_slice()
				} else {
					self.hi.as_slice()
				};
				if end < self.lo.as_slice() || end > upto {
					return Err(Error::Internal(
						"the narrowed start lies outside the range left to read".into(),
					));
				}
				self.exhausted = end == upto;
				self.lo.clear();
				self.lo.extend_from_slice(end);
			}
		}
		Ok(())
	}

	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move { self.read(limit, &mut Sink::Visit(f)) })
	}

	fn fill<'s>(
		&'s mut self,
		page: &'s mut Page,
		limit: u32,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move {
			page.clear();
			self.read(limit, &mut Sink::Fill(page))
		})
	}

	fn resume(&self) -> Option<Resume> {
		if self.exhausted {
			return None;
		}
		let at = match (self.resumed, self.dir) {
			(true, _) => &self.resume,
			(false, Direction::Forward) => &self.lo,
			(false, Direction::Backward) => &self.hi,
		};
		Some(Resume::new(Key::from(at.clone())))
	}
}
