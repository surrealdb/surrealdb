//! The read view of a read-only SurrealKV transaction, and the cursors it opens.
//!
//! A read-only transaction holds one fixed engine snapshot and an empty write set, so every read
//! through its `Tx` is the view, from any thread. A [`SurrealKvSnapshot`] shares the transaction's
//! `Tx` and its `done` flag and copies nothing, so [`Snapshot::held_bytes`] stays at its default of
//! `0`. Writable transactions give no view: the engine's iterators always merge the transaction's
//! write set, so a later write by the owner would show through.
//!
//! Each cursor holds an owned read guard on the `Tx`, which is its keep-alive. `cancel` is the only
//! writer of a read-only transaction's lock, and it sets `done` before it queues for the lock, so
//! it waits for every cursor to drop while [`Snapshot::open`] and [`Snapshot::get_many`] refuse
//! with [`Error::TransactionFinished`]. The view itself holds no guard and never delays `cancel`.
//!
//! The engine's iterators are not `Send`, so a cursor keeps none between chunks. Every chunk builds
//! an iterator at the position the previous chunk stopped at, reads from it on the caller's thread,
//! and drops it before returning. The transaction's snapshot is fixed, so the entries read across
//! chunks are those of one continuous iteration.
//!
//! A chunk is not handed to the affinity pool: the caller is already a task of its own (a morsel's
//! worker, or a scan's read-ahead producer), and a hop to the pool for every chunk costs a wake-up
//! and a park of a pool worker, whose handshake serialises on one lock when many scans run at once.
//! The transaction's own cursor reads in place for the same reason, as `scan` always has.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use surrealdb_kvs::api::{BoxFut, GetMultiResult, ScanChunkStats, ValVisitor};
use surrealdb_kvs::err::{Error, Result};
use surrealdb_kvs::{Direction, Key, KeyRange, Page, RangeCursor, Resume, Snapshot};
use surrealkv::{LSMIterator, Transaction as Tx};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

use crate::{history_backward, history_forward, history_iter, kvs_error};

const _: fn() = || {
	fn send_sync<T: Send + Sync>() {}
	send_sync::<Tx>();
	send_sync::<OwnedRwLockReadGuard<Tx>>();
};

/// The read view of a read-only transaction. See the [module docs](self).
pub(super) struct SurrealKvSnapshot {
	/// The transaction's engine handle, shared with the owner.
	pub(super) inner: Arc<RwLock<Tx>>,
	/// Whether the transaction has committed or cancelled.
	pub(super) done: Arc<AtomicBool>,
	/// Whether the datastore keeps versions, and so accepts a read as of one.
	pub(super) versioned: bool,
}

impl SurrealKvSnapshot {
	/// The keep-alive a read takes on the transaction.
	///
	/// A lock that cannot be taken without waiting means `cancel` is queued for it, so the
	/// transaction is finishing. Waiting would let a worker that holds one cursor deadlock against
	/// a `cancel` that waits for that cursor. `done` is read again once the guard is held:
	/// `cancel` sets it before it queues, so a guard taken while it is still clear is ahead of
	/// `cancel` and holds the engine snapshot `cancel` has yet to take away.
	fn hold(&self, version: Option<u64>) -> Result<OwnedRwLockReadGuard<Tx>> {
		if !self.versioned && version.is_some() {
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

impl Snapshot for SurrealKvSnapshot {
	fn open<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn RangeCursor + 'a>>> {
		Box::pin(async move {
			let guard = self.hold(version)?;
			Ok(Box::new(SurrealKvRangeCursor::new(guard, &rng, dir, version))
				as Box<dyn RangeCursor + 'a>)
		})
	}

	/// One guard and one pass over the keys, through the reads the transaction's own `get` makes.
	fn get_many<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, Result<GetMultiResult>> {
		Box::pin(async move {
			let guard = self.hold(version)?;
			// SAFETY: the closure borrows the keys and holds the read guard, so the returned future
			// must not be leaked while those borrows are live. It is awaited inline here and never
			// stored, so its destructor -- which blocks until the worker has stopped touching the
			// borrows -- always runs before they expire.
			unsafe {
				affinitypool::spawn_local(move || {
					let tx: &Tx = &guard;
					let mut values = Vec::with_capacity(keys.len());
					let mut records = 0u64;
					let mut value_bytes = 0u64;
					for key in keys {
						let val = match version {
							Some(ts) => tx.get_at(key.as_slice(), ts),
							None => tx.get(key.as_slice()),
						}
						.map_err(kvs_error)?;
						if let Some(val) = &val {
							records += 1;
							value_bytes += val.len() as u64;
						}
						values.push(val);
					}
					Ok(GetMultiResult {
						values,
						records,
						value_bytes,
					})
				})
			}
			.await
		})
	}
}

/// What a chunk hands each entry to.
pub(super) enum Sink<'s> {
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

/// Where a read of a range stands, shared by the cursors that read through a transaction.
///
/// `lo` and `hi` bound the range still to be read, half-open. `resume` is where reading continues
/// once something has been read: forward, the key of the first entry not yet consumed; backward,
/// the exclusive upper bound of the entries not yet consumed. Every buffer is allocated once and
/// refilled in place. A position holds no engine iterator: each chunk builds one over what is left
/// and drops it before returning.
pub(super) struct Position {
	/// Inclusive lower bound of the range still to be read.
	lo: Vec<u8>,
	/// Exclusive upper bound of the range still to be read.
	hi: Vec<u8>,
	/// Direction the range is read in.
	dir: Direction,
	/// The version read, when given.
	version: Option<u64>,
	/// Where reading continues once a chunk has been read; meaningful only when `resumed`.
	resume: Vec<u8>,
	/// Whether a chunk has been read since the position was aimed.
	resumed: bool,
	/// Whether nothing is left to read.
	exhausted: bool,
	/// The key of the entry a chunk is working on, and of the entry it stops before.
	key: Vec<u8>,
	/// The value of the entry a history chunk has chosen.
	val: Vec<u8>,
}

impl Position {
	pub(super) fn new(rng: &KeyRange<'_>, dir: Direction, version: Option<u64>) -> Self {
		Self {
			lo: rng.start.to_vec(),
			hi: rng.end.to_vec(),
			dir,
			version,
			resume: Vec::new(),
			resumed: false,
			exhausted: rng.is_empty(),
			key: Vec::new(),
			val: Vec::new(),
		}
	}

	/// Read up to `limit` entries of `tx` into `sink`, then record where reading continues.
	pub(super) fn read(
		&mut self,
		tx: &Tx,
		limit: u32,
		sink: &mut Sink<'_>,
	) -> Result<ScanChunkStats> {
		let mut stats = ScanChunkStats::default();
		if limit == 0 || self.exhausted {
			return Ok(stats);
		}
		let Self {
			lo,
			hi,
			dir,
			version,
			resume,
			resumed,
			exhausted,
			key,
			val,
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
		let found = match *version {
			None => {
				let mut iter = tx.range(start, end).map_err(kvs_error)?;
				read_range(&mut iter, dir, limit, sink, &mut stats, key)?
			}
			Some(ts) => {
				let mut iter = history_iter(tx, start, end)?;
				read_history(&mut iter, dir, ts, limit, sink, &mut stats, key, val)?
			}
		};
		if found {
			resume.clear();
			resume.extend_from_slice(key);
			if matches!(dir, Direction::Backward) {
				// A backward read must not revisit the entry. The smallest key after it is the key
				// with a zero byte appended, so that is the exclusive bound.
				resume.push(0);
			}
			*resumed = true;
		} else {
			*exhausted = true;
		}
		Ok(stats)
	}

	fn aim(&mut self, rng: &KeyRange<'_>, dir: Direction) {
		self.lo.clear();
		self.lo.extend_from_slice(&rng.start);
		self.hi.clear();
		self.hi.extend_from_slice(&rng.end);
		self.dir = dir;
		self.resumed = false;
		self.exhausted = rng.is_empty();
	}

	fn narrow(&mut self, end: &[u8]) -> Result<()> {
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

/// A re-aimable cursor over a [`SurrealKvSnapshot`].
pub(super) struct SurrealKvRangeCursor {
	/// The keep-alive, and the engine handle every chunk reads through.
	guard: OwnedRwLockReadGuard<Tx>,
	/// Where the read stands.
	pos: Position,
}

impl SurrealKvRangeCursor {
	fn new(
		guard: OwnedRwLockReadGuard<Tx>,
		rng: &KeyRange<'_>,
		dir: Direction,
		version: Option<u64>,
	) -> Self {
		Self {
			guard,
			pos: Position::new(rng, dir, version),
		}
	}

	/// Read up to `limit` entries into `sink`, then record where reading continues.
	fn read(&mut self, limit: u32, sink: &mut Sink<'_>) -> Result<ScanChunkStats> {
		self.pos.read(&self.guard, limit, sink)
	}
}

/// Position `iter` on the first entry of its range in `dir`.
fn seek<I: LSMIterator>(iter: &mut I, dir: Direction) -> Result<()> {
	match dir {
		Direction::Forward => iter.seek_first(),
		Direction::Backward => iter.seek_last(),
	}
	.map_err(kvs_error)?;
	Ok(())
}

/// Step `iter` past the entry it stands on.
fn step<I: LSMIterator>(iter: &mut I, dir: Direction) -> Result<()> {
	match dir {
		Direction::Forward => iter.next(),
		Direction::Backward => iter.prev(),
	}
	.map_err(kvs_error)?;
	Ok(())
}

/// Read the current state of a range. Returns whether an entry is left to read, and when one is,
/// leaves its key in `key`.
pub(super) fn read_range<I: LSMIterator>(
	iter: &mut I,
	dir: Direction,
	limit: u32,
	sink: &mut Sink<'_>,
	stats: &mut ScanChunkStats,
	key: &mut Vec<u8>,
) -> Result<bool> {
	seek(iter, dir)?;
	while stats.rows < u64::from(limit) && iter.valid() {
		// The value is borrowed from the engine's block where it is stored inline, and resolved
		// into a buffer of its own only from the value log; either way it lives until the step.
		let taken = {
			let value = iter.value_ref().map_err(kvs_error)?;
			let user_key = iter.key().user_key();
			let taken = sink.take(user_key, &value)?;
			if let Taken::Consumed(_) = taken {
				stats.rows += 1;
				stats.key_bytes += user_key.len() as u64;
				stats.value_bytes += value.len() as u64;
			}
			taken
		};
		match taken {
			Taken::Refused => break,
			Taken::Consumed(flow) => {
				step(iter, dir)?;
				if flow.is_break() {
					break;
				}
			}
		}
	}
	if iter.valid() {
		key.clear();
		key.extend_from_slice(iter.key().user_key());
		return Ok(true);
	}
	Ok(false)
}

/// Read a range as of version `ts`, one visible entry per user key. Returns and leaves `key` as
/// [`read_range`] does. An entry the sink refuses is the one left to read.
#[expect(clippy::too_many_arguments, reason = "the buffers are the cursor's, lent for one chunk")]
pub(super) fn read_history<I: LSMIterator>(
	iter: &mut I,
	dir: Direction,
	ts: u64,
	limit: u32,
	sink: &mut Sink<'_>,
	stats: &mut ScanChunkStats,
	key: &mut Vec<u8>,
	val: &mut Vec<u8>,
) -> Result<bool> {
	seek(iter, dir)?;
	while stats.rows < u64::from(limit) {
		let chosen = match dir {
			Direction::Forward => history_forward(iter, ts, key, Some(val))?,
			Direction::Backward => history_backward(iter, ts, key, Some(val))?,
		};
		if !chosen {
			return Ok(false);
		}
		match sink.take(key, val)? {
			// The stepper has moved past the entry, so the entry itself is where reading resumes.
			Taken::Refused => return Ok(true),
			Taken::Consumed(flow) => {
				stats.rows += 1;
				stats.key_bytes += key.len() as u64;
				stats.value_bytes += val.len() as u64;
				if flow.is_break() {
					break;
				}
			}
		}
	}
	// The next visible key is where reading resumes. The stepper that chooses entries finds it, so
	// the key a chunk stops before is chosen by the same rule as the entries it read.
	match dir {
		Direction::Forward => history_forward(iter, ts, key, None),
		Direction::Backward => history_backward(iter, ts, key, None),
	}
}

impl RangeCursor for SurrealKvRangeCursor {
	fn aim(&mut self, rng: KeyRange<'_>, dir: Direction) -> Result<()> {
		self.pos.aim(&rng, dir);
		Ok(())
	}

	fn narrow(&mut self, end: Key<'_>) -> Result<()> {
		self.pos.narrow(end.as_slice())
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
		self.pos.resume()
	}
}
