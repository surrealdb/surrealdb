//! The key and value scan cursor of a SurrealKV transaction.
//!
//! The default cursor of the `kvs` crate re-issues `scan` for every page: the engine's resolved
//! value of each entry is copied into a `Vec` of its own, the page of those `Vec`s is held while
//! the visitor drains it, and the page is then dropped. This cursor reads each chunk straight from
//! the engine's iterator instead, and hands the visitor the key and the value where the engine
//! holds them: a value stored inline is borrowed from its block, and only a value-log value is
//! resolved into a buffer. `next_batch` copies a chunk once, into a page the cursor reuses.
//!
//! The engine's iterators are not `Send`, so the cursor keeps none between chunks: every chunk
//! takes the transaction's lock, builds an iterator over what is left of the range, reads, and
//! drops both before returning, as `scan` does for each of its pages, and on the caller's thread as
//! `scan` does. The cursor keeps the default cursor's resume rule: the range left to read starts
//! strictly after the last entry handed on, and only a chunk that ends short ends the range. So a
//! writable transaction's later chunk sees what the transaction wrote, or a rollback restored,
//! ahead of it.

use std::ops::ControlFlow;

use surrealdb_kvs::api::{
	BoxFut, ScanChunkStats, ScanCursorVals, Transactable, ValVisitor, ValsBatch,
};
use surrealdb_kvs::cursor::update_range;
use surrealdb_kvs::err::{Error, Result};
use surrealdb_kvs::{Direction, Key, KeyRange, Page};
use surrealkv::Transaction as Tx;

use crate::snapshot::{Sink, read_history, read_range};
use crate::{Transaction, history_iter, kvs_error};

/// A key and value cursor over a range of a [`Transaction`]. See the [module docs](self).
pub(super) struct SurrealKvValsCursor<'a> {
	scan: Scan<'a>,
	/// The entries `next_batch` returns, copied once and reused across chunks. It stays in the
	/// cursor while a batch is read, so a batch dropped before it is filled leaves it as it was.
	page: Page,
}

/// What a [`SurrealKvValsCursor`] reads with, apart from its page.
struct Scan<'a> {
	tx: &'a Transaction,
	/// The range still to be read; empty once a chunk has ended short.
	rng: KeyRange<'a>,
	dir: Direction,
	version: Option<u64>,
	/// The entries still to pass over before the first one handed on.
	skip: u32,
	/// The key of the last entry a chunk handed on.
	last: Vec<u8>,
	/// The readers' scratch: the key a chunk stops before, and a history chunk's chosen value.
	next: Vec<u8>,
	val: Vec<u8>,
}

impl<'a> SurrealKvValsCursor<'a> {
	pub(super) fn new(
		tx: &'a Transaction,
		rng: KeyRange<'a>,
		dir: Direction,
		version: Option<u64>,
		skip: u32,
	) -> Self {
		Self {
			scan: Scan {
				tx,
				rng,
				dir,
				version,
				skip,
				last: Vec::new(),
				next: Vec::new(),
				val: Vec::new(),
			},
			// The capacity is a byte budget, not an allocation: the page grows with what it holds,
			// and with no budget to reach it takes every entry a batch reads, so `limit` alone
			// bounds a batch.
			page: Page::with_capacity(usize::MAX),
		}
	}
}

impl Scan<'_> {
	/// Visit up to `limit` entries under the transaction's lock, past the entries still to be
	/// skipped.
	async fn read(&mut self, limit: u32, f: &mut dyn ValVisitor) -> Result<ScanChunkStats> {
		// A zero-limit chunk is a pure no-op: nothing read, nothing skipped.
		if limit == 0 || self.rng.is_empty() {
			return Ok(ScanChunkStats::default());
		}
		if self.tx.closed() {
			return Err(Error::TransactionFinished);
		}
		let inner = self.tx.inner.read().await;
		if self.skip > 0 {
			// The skipped entries are read in the view the first reading chunk sees, and are
			// passed over as consumed ones: the range left starts after them.
			let skip = std::mem::take(&mut self.skip);
			self.chunk(&inner, skip, &mut |_, _| Ok(ControlFlow::Continue(())))?;
			if self.rng.is_empty() {
				return Ok(ScanChunkStats::default());
			}
		}
		self.chunk(&inner, limit, f)
	}

	/// Visit up to `limit` entries of what is left of the range, then move the range past the
	/// last entry visited, or end it when the chunk ended short.
	fn chunk(&mut self, tx: &Tx, limit: u32, f: &mut dyn ValVisitor) -> Result<ScanChunkStats> {
		let Self {
			rng,
			dir,
			version,
			last,
			next,
			val,
			..
		} = self;
		let dir = *dir;
		let mut stats = ScanChunkStats::default();
		// Only the last entry handed on is resumed after: the `limit`th, or the one the visitor
		// stopped at. A chunk that ends short of both has ended its range, so its last key is not
		// needed, and no other entry's key is copied.
		let mut visited = 0u32;
		let mut visit = |key: &[u8], value: &[u8]| {
			let flow = f(key, value)?;
			visited += 1;
			if visited == limit || flow.is_break() {
				last.clear();
				last.extend_from_slice(key);
			}
			Ok(flow)
		};
		let mut sink = Sink::Visit(&mut visit);
		let (start, end) = (rng.start.as_slice(), rng.end.as_slice());
		let more = match *version {
			None => {
				let mut iter = tx.range(start, end).map_err(kvs_error)?;
				read_range(&mut iter, dir, limit, &mut sink, &mut stats, next)?
			}
			Some(ts) => {
				let mut iter = history_iter(tx, start, end)?;
				read_history(&mut iter, dir, ts, limit, &mut sink, &mut stats, next, val)?
			}
		};
		if !more && stats.rows < u64::from(limit) {
			rng.end = Key::empty();
		} else if stats.rows > 0 {
			update_range(rng, dir, last);
		}
		Ok(stats)
	}
}

impl ScanCursorVals for SurrealKvValsCursor<'_> {
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<ValsBatch<'s>>> {
		Box::pin(async move {
			let Self {
				scan,
				page,
			} = self;
			page.clear();
			scan.read(limit, &mut |key, value| {
				// The page has no budget to reach, so it takes every entry.
				let taken = page.push(key, value);
				debug_assert!(taken, "a batch page refused an entry");
				Ok(ControlFlow::Continue(()))
			})
			.await?;
			Ok(page.batch())
		})
	}

	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move { self.scan.read(limit, f).await })
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::sync::atomic::AtomicBool;

	use surrealdb_kvs::{SavepointStack, TransactionType};
	use surrealkv::Mode;
	use tokio::sync::RwLock;

	use super::*;
	use crate::{Datastore, SurrealKvConfig};

	/// A batch dropped while it waits for the transaction's lock leaves the cursor's page as it
	/// was, so the next batch still takes every entry it reads.
	#[tokio::test]
	async fn a_dropped_batch_leaves_the_page_whole() {
		let dir = std::env::temp_dir().join(format!(
			"surrealkv_dropped_batch_{}_{}",
			std::process::id(),
			std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
		));
		let path = dir.to_string_lossy().into_owned();
		let ds = Datastore::new(&path, SurrealKvConfig::default()).await.unwrap();
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		for i in 0..8u8 {
			tx.set(Key::from(vec![b'k', i]), vec![i; 16]).await.unwrap();
		}
		tx.commit().await.unwrap();

		let tx = Transaction {
			done: Arc::new(AtomicBool::new(false)),
			write: false,
			versioned: false,
			db: Arc::clone(&ds.db),
			inner: Arc::new(RwLock::new(ds.db.begin_with_mode(Mode::ReadOnly).unwrap())),
			savepoints: RwLock::new(SavepointStack::default()),
			sync: false,
			shutting_down: Arc::new(AtomicBool::new(false)),
		};
		let rng = KeyRange::from(Key::from(b"k".as_slice())..Key::from(b"l".as_slice()));
		let mut cursor = SurrealKvValsCursor::new(&tx, rng, Direction::Forward, None, 0);
		{
			// The batch waits for the lock, and is dropped while it waits.
			let held = tx.inner.write().await;
			let mut batch = cursor.next_batch(4);
			let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
			assert!(batch.as_mut().poll(&mut cx).is_pending());
			drop(batch);
			drop(held);
		}
		let batch = cursor.next_batch(4).await.unwrap();
		let keys: Vec<Vec<u8>> = batch.iter().map(|(k, _)| k.to_vec()).collect();
		assert_eq!(keys, (0..4u8).map(|i| vec![b'k', i]).collect::<Vec<_>>());
		let batch = cursor.next_batch(8).await.unwrap();
		assert_eq!(batch.len(), 4);
		drop(cursor);
		drop(tx);
		drop(ds);
		let _ = std::fs::remove_dir_all(&dir);
	}
}
