//! The read view of a RocksDB transaction that has buffered no put or delete, and the cursors it
//! opens.
//!
//! A [`RocksSnapshot`] holds the transaction's `inner` slot and nothing else. The transaction's
//! own snapshot is the only state the view relies on and nothing is copied for it, so
//! [`Snapshot::held_bytes`] stays at its default of `0`. Each cursor takes its
//! own keep-alive on the transaction when it is opened, so `commit` and `cancel` wait for the
//! cursors that are alive and the view itself never delays them: once the slot has been emptied,
//! [`Snapshot::open`] reports [`Error::TransactionFinished`].
//!
//! A cursor reads the database directly through the transaction's snapshot, so it sees what the
//! transaction began with and nothing committed since. Every chunk it reads runs on the affinity
//! pool as one unit, so the iterator work, and the visitor that decodes what it yields, stay off
//! the async runtime and cursors on different threads share no lock.
//!
//! The iterator is opened without bounds and checks its own: the cursor is re-aimed across ranges
//! (and across prefixes), which a bound baked into the iterator's options would forbid.

use std::ops::ControlFlow;
use std::sync::Arc;

use rocksdb::ReadOptions;
use surrealdb_kvs::api::{BoxFut, GetMultiResult, ScanChunkStats, ValVisitor};
use surrealdb_kvs::err::{Error, Result};
use surrealdb_kvs::{Direction, Key, KeyRange, Page, RangeCursor, Resume, Snapshot};
use tokio::sync::Mutex;

use crate::owned_tx::{OwnedRawDbIter, OwnedTx};
use crate::{attach_snapshot, kvs_error};

/// The read view of a transaction that has buffered no put or delete: every read-only
/// transaction, and a writable one until its first put or delete. It reads the database under the
/// transaction's begin snapshot, never the transaction's write batch, and holds no bytes of its
/// own (`held_bytes` is `0`). See the [module docs](self).
pub(super) struct RocksSnapshot {
	/// The transaction's slot, emptied by `commit` and `cancel`.
	pub(super) inner: Arc<Mutex<Option<OwnedTx>>>,
	/// Whether the datastore keeps versions, and so accepts a read as of one.
	pub(super) versioned: bool,
	/// Whether cursors verify block checksums, as scans do.
	pub(super) verify_checksums: bool,
}

impl Snapshot for RocksSnapshot {
	fn open<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn RangeCursor + 'a>>> {
		Box::pin(async move {
			if !self.versioned && version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// The lock is held only to read the snapshot pointer into the options and to take the
			// keep-alive the iterator will own. With the keep-alive taken, the options stay valid
			// after the lock is released: `into_inner` waits for the iterator before the
			// transaction, and with it the snapshot, is destroyed. See `attach_snapshot`.
			let (ro, handle) = {
				let guard = self.inner.lock().await;
				let inner = guard.as_ref().ok_or(Error::TransactionFinished)?;
				let mut ro = ReadOptions::default();
				attach_snapshot(&mut ro, inner);
				ro.set_auto_readahead_size(true);
				ro.set_async_io(true);
				ro.fill_cache(false);
				ro.set_verify_checksums(self.verify_checksums);
				// A cursor is re-aimed across ranges, so no prefix seek mode applies.
				ro.set_total_order_seek(true);
				if self.versioned {
					ro.set_timestamp(version.unwrap_or(u64::MAX).to_le_bytes().to_vec());
				}
				(ro, inner.handle())
			};
			let iter = handle.raw_iter(ro);
			Ok(Box::new(RocksRangeCursor::new(iter, &rng, dir)) as Box<dyn RangeCursor + 'a>)
		})
	}

	/// RocksDB's native `multi_get` through the transaction's snapshot. The lock is held for the
	/// read, which runs on the affinity pool, so `commit` and `cancel` wait for it.
	fn get_many<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, Result<GetMultiResult>> {
		Box::pin(async move {
			if !self.versioned && version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			let guard = self.inner.lock().await;
			// SAFETY: the closure borrows `self`, the keys and the held `MutexGuard`, so the
			// returned future must not be leaked while those borrows are live. It is awaited
			// inline here and never stored, so its destructor -- which blocks until the worker has
			// stopped touching the borrows -- always runs before they expire.
			unsafe {
				affinitypool::spawn_local(move || {
					let inner = guard.as_ref().ok_or(Error::TransactionFinished)?;
					let mut ro = ReadOptions::default();
					attach_snapshot(&mut ro, inner);
					ro.set_async_io(true);
					ro.fill_cache(true);
					ro.set_verify_checksums(self.verify_checksums);
					if self.versioned {
						ro.set_timestamp(version.unwrap_or(u64::MAX).to_le_bytes().to_vec());
					}
					let mut records = 0u64;
					let mut value_bytes = 0u64;
					let values = inner
						.db()
						.multi_get_opt(keys, &ro)
						.into_iter()
						.map(|res| match res {
							Ok(Some(val)) => {
								records += 1;
								value_bytes += val.len() as u64;
								Ok(Some(val))
							}
							Ok(None) => Ok(None),
							Err(e) => Err(kvs_error(e)),
						})
						.collect::<Result<Vec<_>>>()?;
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

/// A re-aimable cursor over a [`RocksSnapshot`].
///
/// `lo` and `hi` bound the range still to be read, half-open. `resume` is where reading continues
/// once something has been read: forward, the key of the first entry not yet visited; backward, the
/// exclusive upper bound of the entries not yet visited. Both are taken from where the iterator
/// stands after a chunk, so reading from them next visits exactly the entries the chunk did not.
pub(super) struct RocksRangeCursor {
	/// The iterator, which also keeps the transaction alive. Not seeked until first read.
	iter: OwnedRawDbIter,
	/// Inclusive lower bound of the range still to be read.
	lo: Vec<u8>,
	/// Exclusive upper bound of the range still to be read.
	hi: Vec<u8>,
	/// Direction the range is read in.
	dir: Direction,
	/// Whether the iterator has been positioned for the current aim.
	sought: bool,
	/// Where reading continues once a chunk has been read; `None` before the first.
	resume: Option<Vec<u8>>,
	/// Whether nothing is left to read.
	exhausted: bool,
}

/// Whether `key` lies inside `[lo, hi)`: what a forward read still has to cross the upper bound
/// for, and a backward read the lower.
fn within(lo: &[u8], hi: &[u8], dir: Direction, key: &[u8]) -> bool {
	match dir {
		Direction::Forward => key < hi,
		Direction::Backward => key >= lo,
	}
}

impl RocksRangeCursor {
	fn new(iter: OwnedRawDbIter, rng: &KeyRange<'_>, dir: Direction) -> Self {
		Self {
			iter,
			lo: rng.start.to_vec(),
			hi: rng.end.to_vec(),
			dir,
			sought: false,
			resume: None,
			exhausted: rng.is_empty(),
		}
	}

	/// Position the iterator on the first entry of the range in the cursor's direction.
	fn seek(&mut self) {
		match self.dir {
			Direction::Forward => self.iter.seek(&self.lo),
			Direction::Backward => {
				// The last key at or before `hi`; `hi` itself is outside the range.
				self.iter.seek_for_prev(&self.hi);
				if self.iter.key() == Some(self.hi.as_slice()) {
					self.iter.prev();
				}
			}
		}
		self.sought = true;
	}

	/// Step the iterator past the entry it stands on.
	fn step(&mut self) {
		match self.dir {
			Direction::Forward => self.iter.next(),
			Direction::Backward => self.iter.prev(),
		}
	}

	/// Record where the iterator stands as where reading continues, or that nothing is left.
	fn settle(&mut self) {
		let Self {
			iter,
			lo,
			hi,
			dir,
			resume,
			exhausted,
			..
		} = self;
		match iter.key() {
			Some(key) if within(lo, hi, *dir, key) => {
				let at = resume.get_or_insert_with(Vec::new);
				at.clear();
				at.extend_from_slice(key);
				if matches!(dir, Direction::Backward) {
					// A backward read must not revisit `key`. The smallest key after `key` is
					// `key` with a zero byte appended, so that is the exclusive bound.
					at.push(0);
				}
			}
			_ => *exhausted = true,
		}
	}

	/// Visit up to `limit` entries in place. See [`RangeCursor::for_each`].
	fn visit(&mut self, limit: u32, f: &mut dyn ValVisitor) -> Result<ScanChunkStats> {
		let mut stats = ScanChunkStats::default();
		if limit == 0 || self.exhausted {
			return Ok(stats);
		}
		if !self.sought {
			self.seek();
		}
		while stats.rows < u64::from(limit) {
			let Some((key, val)) = self.iter.item() else {
				break;
			};
			if !within(&self.lo, &self.hi, self.dir, key) {
				break;
			}
			// The entry is consumed whatever the visitor answers. A visitor error returns before
			// the iterator moves, leaving the resume position unspecified.
			let flow = f(key, val)?;
			stats.rows += 1;
			stats.key_bytes += key.len() as u64;
			stats.value_bytes += val.len() as u64;
			self.step();
			if let ControlFlow::Break(()) = flow {
				break;
			}
		}
		self.iter.status().map_err(kvs_error)?;
		self.settle();
		Ok(stats)
	}

	/// Copy up to `limit` entries into `page`. See [`RangeCursor::fill`].
	fn copy(&mut self, page: &mut Page, limit: u32) -> Result<ScanChunkStats> {
		page.clear();
		let mut stats = ScanChunkStats::default();
		if limit == 0 || self.exhausted {
			return Ok(stats);
		}
		if !self.sought {
			self.seek();
		}
		while stats.rows < u64::from(limit) {
			let Some((key, val)) = self.iter.item() else {
				break;
			};
			if !within(&self.lo, &self.hi, self.dir, key) {
				break;
			}
			// An entry the page cannot hold is left under the iterator, unread.
			if !page.push(key, val) {
				break;
			}
			stats.rows += 1;
			stats.key_bytes += key.len() as u64;
			stats.value_bytes += val.len() as u64;
			self.step();
		}
		self.iter.status().map_err(kvs_error)?;
		self.settle();
		Ok(stats)
	}
}

impl RangeCursor for RocksRangeCursor {
	fn aim(&mut self, rng: KeyRange<'_>, dir: Direction) -> Result<()> {
		self.lo.clear();
		self.lo.extend_from_slice(&rng.start);
		self.hi.clear();
		self.hi.extend_from_slice(&rng.end);
		self.dir = dir;
		self.sought = false;
		self.resume = None;
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
				let from = self.resume.as_deref().unwrap_or(&self.lo);
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
				let upto = self.resume.as_deref().unwrap_or(&self.hi);
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
		Box::pin(async move {
			// SAFETY: the closure borrows the cursor and the visitor, so the returned future must
			// not be leaked while those borrows are live. It is awaited inline here and never
			// stored, so its destructor -- which blocks until the worker has stopped touching the
			// borrows -- always runs before they expire.
			unsafe { affinitypool::spawn_local(move || self.visit(limit, f)) }.await
		})
	}

	fn fill<'s>(
		&'s mut self,
		page: &'s mut Page,
		limit: u32,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move {
			// SAFETY: as for `for_each`: the future is awaited inline and never stored.
			unsafe { affinitypool::spawn_local(move || self.copy(page, limit)) }.await
		})
	}

	fn resume(&self) -> Option<Resume> {
		if self.exhausted {
			return None;
		}
		let at = match (&self.resume, self.dir) {
			(Some(at), _) => at.clone(),
			(None, Direction::Forward) => self.lo.clone(),
			(None, Direction::Backward) => self.hi.clone(),
		};
		Some(Resume::new(Key::from(at)))
	}
}
