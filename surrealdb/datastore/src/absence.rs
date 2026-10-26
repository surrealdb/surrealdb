//! Creates whose check that the key is absent is made for many keys at once.
//!
//! A create writes its key only where nothing is stored there. Made one at a time, that check is
//! one read of the store per key, each awaited before the next record is made. A held create
//! ([`crate::Transactor::put_held`]) instead joins a list kept on the transaction, unwritten; the
//! list is checked with one [`Transactable::assert_absent`] and, if every key is absent, written,
//! when it is flushed.
//!
//! # Contract
//!
//! * **Nothing reads past a held key.** Every read or write of the transaction whose key or range
//!   may hold a held key flushes the list first, so it sees the state an immediate create would
//!   have left. Point operations test the key's fingerprint and ranges the list's bounds; both may
//!   answer "may hold" for a key the list does not hold, which only flushes early. A savepoint
//!   push, release or rollback and a commit flush too; a cancel drops the list.
//! * **The check is the one an immediate create makes.** The list is checked against the
//!   transaction's current view, which holds the transaction's own writes and deletes and none of
//!   the held keys, so a held key is judged against exactly the state its create would have met,
//!   with one exception the holder rules out: a held key written by a later held create of the same
//!   key. A create of a key the list may hold flushes the list and is made immediately.
//! * **First failure in order.** A flush that finds a held key present writes the creates before
//!   it, drops it and those after it, and keeps the key as the transaction's violation. Every
//!   operation after that fails with [`Error::DeferredKeyExists`] naming it until the holder takes
//!   it ([`Absence::take_violation`]), so the failure is the one the earliest failing create would
//!   have raised, whatever the operation that found it.
//! * **Bounded.** The list holds at most [`HELD_KEYS`] creates or [`HELD_BYTES`] bytes of keys and
//!   values; a create past either flushes it.
//! * **No live cursor over the key.** A create is made immediately while a cursor of the
//!   transaction whose range may hold its key is open: the cursor reads the store directly, past
//!   the list. The open cursors are tracked as the span of their ranges.

use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};

use surrealdb_kvs::api::Transactable;
use surrealdb_kvs::{Key, KeyRange};
use tokio::sync::Mutex;

use crate::record_writes::{Reached, RecordWrites, record_prefix};
use crate::{Error, Result};

/// Whether creates are held at all: `SURREAL_HOLD_CREATES=false` makes every create check its key
/// immediately.
pub fn enabled() -> bool {
	static ENABLED: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
		!matches!(std::env::var("SURREAL_HOLD_CREATES").as_deref(), Ok("false" | "0"))
	});
	*ENABLED
}

/// The most creates the list holds before it is flushed.
pub const HELD_KEYS: usize = 65_536;
/// The most bytes of keys and values the list holds before it is flushed.
pub const HELD_BYTES: usize = 16 * 1024 * 1024;

/// The creates a transaction holds, and the cursors it has open.
#[derive(Default)]
pub(crate) struct Absence {
	/// Whether the list holds a create or a violation: the one read an operation makes when it
	/// holds neither.
	held: AtomicBool,
	/// The cursors of the transaction that are open.
	cursors: parking_lot::Mutex<Cursors>,
	list: Mutex<List>,
}

/// How many cursors are open, and the span of their ranges: from the least start to the greatest
/// end of the ranges opened since none was open.
#[derive(Default)]
struct Cursors {
	live: usize,
	lo: Vec<u8>,
	hi: Vec<u8>,
}

#[derive(Default)]
struct List {
	/// The held creates, in the order they were made.
	entries: Vec<(Vec<u8>, Vec<u8>)>,
	/// The fingerprints of the held keys.
	prints: HashSet<u64, BuildHasherDefault<Prints>>,
	/// The least and greatest held key.
	lo: Vec<u8>,
	hi: Vec<u8>,
	/// The bytes of keys and values held.
	bytes: usize,
	/// The held key a flush found present.
	violation: Option<Vec<u8>>,
}

/// The hasher of the fingerprint set: a fingerprint is a hash already, so it is its own.
#[derive(Default)]
struct Prints(u64);

impl Hasher for Prints {
	fn finish(&self) -> u64 {
		self.0
	}

	fn write(&mut self, bytes: &[u8]) {
		self.0 = print(bytes);
	}

	fn write_u64(&mut self, n: u64) {
		self.0 = n;
	}
}

/// A key's fingerprint: a fast multiplicative hash of its 8-byte words, compared only with
/// another of the same transaction, so a collision only flushes the list early.
///
/// The fingerprint set hashes by the fingerprint itself and places it by its low bits, so the
/// words' hash is finished with murmur3's 64-bit mix, which makes every bit depend on every other:
/// a wrapping multiply leaves the low bits a function of the low bits of the last word, which for
/// a key ending in a big-endian integer, a record id's, barely vary from one key to the next.
fn print(key: &[u8]) -> u64 {
	const K: u64 = 0x517c_c1b7_2722_0a95;
	let mut hash = key.len() as u64;
	let mut words = key.chunks_exact(8);
	for word in words.by_ref() {
		let word = u64::from_le_bytes(word.try_into().unwrap_or([0; 8]));
		hash = (hash.rotate_left(5) ^ word).wrapping_mul(K);
	}
	let mut tail = [0u8; 8];
	tail[..words.remainder().len()].copy_from_slice(words.remainder());
	let mut hash = (hash.rotate_left(5) ^ u64::from_le_bytes(tail)).wrapping_mul(K);
	hash ^= hash >> 33;
	hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
	hash ^= hash >> 33;
	hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
	hash ^ (hash >> 33)
}

impl List {
	fn may_hold(&self, key: &[u8]) -> bool {
		self.violation.is_some()
			|| (!self.entries.is_empty()
				&& self.lo.as_slice() <= key
				&& key <= self.hi.as_slice()
				&& self.prints.contains(&print(key)))
	}

	fn may_meet(&self, start: &[u8], end: &[u8]) -> bool {
		self.violation.is_some()
			|| (!self.entries.is_empty() && start <= self.hi.as_slice() && self.lo.as_slice() < end)
	}

	fn clear(&mut self) {
		self.entries.clear();
		self.prints.clear();
		self.lo.clear();
		self.hi.clear();
		self.bytes = 0;
	}

	fn push(&mut self, key: Vec<u8>, val: Vec<u8>) {
		if self.entries.is_empty() || key < self.lo {
			self.lo.clone_from(&key);
		}
		if self.entries.is_empty() || key > self.hi {
			self.hi.clone_from(&key);
		}
		self.prints.insert(print(&key));
		self.bytes += key.len() + val.len();
		self.entries.push((key, val));
	}

	fn full(&self) -> bool {
		self.entries.len() >= HELD_KEYS || self.bytes >= HELD_BYTES
	}
}

impl Absence {
	/// Whether the list holds a create or a violation.
	#[inline]
	pub(crate) fn holds(&self) -> bool {
		self.held.load(Ordering::Acquire)
	}

	/// Counts a cursor over `start..end` opened; [`Absence::cursor_closed`] counts it closed.
	pub(crate) fn cursor_opened(&self, start: &[u8], end: &[u8]) {
		let mut cursors = self.cursors.lock();
		if cursors.live == 0 || start < cursors.lo.as_slice() {
			cursors.lo.clear();
			cursors.lo.extend_from_slice(start);
		}
		if cursors.live == 0 || end > cursors.hi.as_slice() {
			cursors.hi.clear();
			cursors.hi.extend_from_slice(end);
		}
		cursors.live += 1;
	}

	/// Counts a cursor closed.
	pub(crate) fn cursor_closed(&self) {
		let mut cursors = self.cursors.lock();
		cursors.live = cursors.live.saturating_sub(1);
	}

	/// Whether an open cursor's range may hold `key`.
	fn cursor_may_meet(&self, key: &[u8]) -> bool {
		let cursors = self.cursors.lock();
		cursors.live > 0 && cursors.lo.as_slice() <= key && key < cursors.hi.as_slice()
	}

	/// Holds the create of `key`, answering `None`, or hands it back for the caller to make
	/// immediately: while an open cursor's range may hold the key, and when the list may already
	/// hold the key, which is flushed first. A full list is flushed once the create joins it.
	pub(crate) async fn hold(
		&self,
		inner: &dyn Transactable,
		record_writes: Option<&parking_lot::Mutex<RecordWrites>>,
		key: Vec<u8>,
		val: Vec<u8>,
	) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		let mut list = self.list.lock().await;
		if let Some(key) = &list.violation {
			return Err(violation(key));
		}
		if self.cursor_may_meet(&key) || list.may_hold(&key) {
			self.flush(inner, record_writes, &mut list).await?;
			return Ok(Some((key, val)));
		}
		list.push(key, val);
		self.held.store(true, Ordering::Release);
		if list.full() {
			self.flush(inner, record_writes, &mut list).await?;
		}
		Ok(None)
	}

	/// Flushes the list when it may hold `key`.
	pub(crate) async fn guard_key(
		&self,
		inner: &dyn Transactable,
		record_writes: Option<&parking_lot::Mutex<RecordWrites>>,
		key: &[u8],
	) -> Result<()> {
		let mut list = self.list.lock().await;
		if list.may_hold(key) {
			self.flush(inner, record_writes, &mut list).await?;
		}
		Ok(())
	}

	/// Flushes the list when it may hold any of `keys`.
	pub(crate) async fn guard_keys(
		&self,
		inner: &dyn Transactable,
		record_writes: Option<&parking_lot::Mutex<RecordWrites>>,
		keys: &[Key<'_>],
	) -> Result<()> {
		let mut list = self.list.lock().await;
		if keys.iter().any(|key| list.may_hold(key.as_slice())) {
			self.flush(inner, record_writes, &mut list).await?;
		}
		Ok(())
	}

	/// Flushes the list when it may hold a key of `rng`.
	pub(crate) async fn guard_range(
		&self,
		inner: &dyn Transactable,
		record_writes: Option<&parking_lot::Mutex<RecordWrites>>,
		rng: &KeyRange<'_>,
	) -> Result<()> {
		let mut list = self.list.lock().await;
		if list.may_meet(rng.start.as_slice(), rng.end.as_slice()) {
			self.flush(inner, record_writes, &mut list).await?;
		}
		Ok(())
	}

	/// Flushes the list whatever it holds.
	pub(crate) async fn flush_all(
		&self,
		inner: &dyn Transactable,
		record_writes: Option<&parking_lot::Mutex<RecordWrites>>,
	) -> Result<()> {
		let mut list = self.list.lock().await;
		self.flush(inner, record_writes, &mut list).await
	}

	/// Flushes the list and takes the violation, if any: the key of the earliest held create that
	/// found its key present. The transaction answers every operation as usual afterwards.
	pub(crate) async fn take_violation(
		&self,
		inner: &dyn Transactable,
		record_writes: Option<&parking_lot::Mutex<RecordWrites>>,
	) -> Result<Option<Vec<u8>>> {
		let mut list = self.list.lock().await;
		if list.violation.is_none() {
			match self.flush(inner, record_writes, &mut list).await {
				Ok(()) => return Ok(None),
				Err(Error::DeferredKeyExists {
					..
				}) => {}
				Err(e) => return Err(e),
			}
		}
		let taken = list.violation.take();
		self.held.store(!list.entries.is_empty(), Ordering::Release);
		Ok(taken)
	}

	/// Drops the list and any violation, for a transaction that is cancelled.
	pub(crate) async fn discard(&self) {
		let mut list = self.list.lock().await;
		list.clear();
		list.violation = None;
		self.held.store(false, Ordering::Release);
	}

	async fn flush(
		&self,
		inner: &dyn Transactable,
		record_writes: Option<&parking_lot::Mutex<RecordWrites>>,
		list: &mut List,
	) -> Result<()> {
		if let Some(key) = &list.violation {
			return Err(violation(key));
		}
		if list.entries.is_empty() {
			return Ok(());
		}
		let first = {
			let keys: Vec<Key<'_>> =
				list.entries.iter().map(|(key, _)| Key::from(key.as_slice())).collect();
			inner.assert_absent(&keys).await?
		};
		let mut entries = std::mem::take(&mut list.entries);
		list.clear();
		let failed = match first {
			Some(at) if at < entries.len() => {
				let failed = std::mem::take(&mut entries[at].0);
				entries.truncate(at);
				Some(failed)
			}
			Some(_) => {
				return Err(Error::Internal("a present held key is one the list holds".into()));
			}
			None => None,
		};
		let tables = match record_writes {
			Some(writes) => {
				let writes = writes.lock();
				entries
					.iter()
					.filter_map(|(key, _)| record_prefix(key))
					.filter(|records| !writes.noted(records))
					.map(<[u8]>::to_vec)
					.collect()
			}
			None => Vec::new(),
		};
		inner.set_many(entries).await?;
		if let Some(writes) = record_writes
			&& !tables.is_empty()
		{
			let mut writes = writes.lock();
			for records in tables {
				writes.note(Reached::Table(records));
			}
		}
		match failed {
			None => {
				self.held.store(false, Ordering::Release);
				Ok(())
			}
			Some(key) => {
				let err = violation(&key);
				list.violation = Some(key);
				Err(err)
			}
		}
	}
}

fn violation(key: &[u8]) -> Error {
	Error::DeferredKeyExists {
		key: key.to_vec(),
	}
}

#[cfg(test)]
mod tests {
	use super::print;

	/// Keys that differ only in a trailing big-endian integer, as consecutive record ids do,
	/// spread over the low bits the fingerprint set places them by.
	#[test]
	fn consecutive_record_keys_spread_over_the_low_bits() {
		let buckets: std::collections::HashSet<u64> = (1u64..=65_536)
			.map(|id| {
				let mut key = b"/*ns\0*db\0*record\0*".to_vec();
				key.push(0x03);
				key.extend_from_slice(&(id ^ (1 << 63)).to_be_bytes());
				print(&key) & 0xffff
			})
			.collect();
		assert!(buckets.len() > 40_000, "{} of 65536 low-bit buckets used", buckets.len());
	}
}
