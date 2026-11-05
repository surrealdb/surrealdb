//! A transaction's writes held apart from its storage, for planning a
//! program against what its own schema statements will do before they run.
//!
//! While a transaction speculates, every write it takes lands in the
//! speculation's overlay instead of the storage beneath, and every read of the
//! current state answers from the overlay where it holds the key and from the
//! storage elsewhere, so the reads see the writes as they would once taken.
//! Reads at a past version answer from the storage alone: the overlay holds
//! nothing that was ever committed. Nothing a speculation writes reaches the
//! storage, the transaction's cache, or its count of catalog stamps; ending
//! the speculation hands back the overlay and the transaction reads and
//! writes its storage again.
//!
//! A speculation is a view of the transaction, a transaction of its own
//! ([`crate::Transaction::speculate`]): only code handed the view reads
//! through the overlay, and every other holder of the transaction reads and
//! writes the storage as before, so a speculation needs no one else to stop
//! using the transaction while it lasts. The view reads the transaction it
//! speculates on through [`Base`], which takes no write; once the speculation
//! ends, the view refuses every write, so a task still holding it reaches
//! neither the overlay nor the storage.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use surrealdb_kvs::api::{
	BoxFut, GetMultiResult, KeySpan, KeyValSpan, KeyVisitor, KeysBatch, KeysResult, ScanChunkStats,
	ScanCursorKeys, ScanCursorVals, ScanResult, Transactable, ValVisitor, ValsBatch,
};
use surrealdb_kvs::snapshot::ViewCapabilities;
use surrealdb_kvs::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use surrealdb_kvs::{
	Direction, Error, Estimate, Key, KeyRange, Result, Snapshot, SplitTarget, Val,
};

/// What an [`Overlay`] touches: the keys it writes or deletes, `K`, and the
/// ranges `[start, end)` it clears.
pub type Footprint<'a, K> = (K, &'a [(Vec<u8>, Vec<u8>)]);

/// The writes a speculation has taken, in the order they were taken.
///
/// A key written last by a write holds the value written (`Some`) or its
/// deletion (`None`); a key written by no write but inside a range cleared
/// since is deleted; any other key reads from the storage.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Overlay {
	writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
	/// Ranges cleared, each `[start, end)`. A write taken after a clear sits
	/// in [`Self::writes`], which a read consults first.
	cleared: Vec<(Vec<u8>, Vec<u8>)>,
	/// What the speculation's caller asked to keep with its writes, in the
	/// order asked: what it drew that a later run takes — ids it leased
	/// outside the transaction, uuids it minted — each with the draw that
	/// took it.
	notes: Vec<(u16, u128)>,
	/// The catalog ids the speculation's caller held
	/// ([`crate::sequences::Sequences::hold_catalog_id`]), in the order held:
	/// each is confirmed by the definition that takes it or released by
	/// whoever owns the speculation's plan, or released by the speculation
	/// when it is dropped without being ended.
	held: Vec<(crate::sequences::CatalogSequence, u32)>,
	/// Whether the speculation's caller left an effect for the real run that
	/// the overlay does not hold.
	skipped: bool,
	/// Whether the speculation's caller raised a refusal its statement
	/// declares, which a real run over the same state raises too.
	refused: bool,
}

/// What the overlay answers for one key.
pub(crate) enum Held {
	/// The overlay holds the key's value.
	Value(Vec<u8>),
	/// The overlay deletes the key.
	Deleted,
	/// The storage answers.
	Storage,
}

impl Overlay {
	/// Whether the overlay holds no write.
	pub fn is_empty(&self) -> bool {
		self.writes.is_empty() && self.cleared.is_empty()
	}

	/// Whether the overlay holds the same writes as `other`, whatever each
	/// noted.
	pub fn writes_as(&self, other: &Overlay) -> bool {
		self.writes == other.writes && self.cleared == other.cleared
	}

	/// Take what was noted.
	pub fn take_notes(&mut self) -> Vec<(u16, u128)> {
		std::mem::take(&mut self.notes)
	}

	pub(crate) fn note(&mut self, note: (u16, u128)) {
		self.notes.push(note);
	}

	/// Take the catalog ids held.
	pub fn take_held(&mut self) -> Vec<(crate::sequences::CatalogSequence, u32)> {
		std::mem::take(&mut self.held)
	}

	pub(crate) fn hold(&mut self, held: (crate::sequences::CatalogSequence, u32)) {
		self.held.push(held);
	}

	/// Whether an effect was left for the real run, clearing the mark.
	pub fn take_skipped(&mut self) -> bool {
		std::mem::take(&mut self.skipped)
	}

	pub(crate) fn skip(&mut self) {
		self.skipped = true;
	}

	/// Whether a declared refusal was raised, clearing the mark.
	pub fn take_refused(&mut self) -> bool {
		std::mem::take(&mut self.refused)
	}

	pub(crate) fn refuse(&mut self) {
		self.refused = true;
	}

	/// The keys the overlay writes or deletes, and the ranges it clears.
	pub fn footprint(&self) -> Footprint<'_, impl Iterator<Item = &[u8]>> {
		(self.writes.keys().map(Vec::as_slice), self.cleared.as_slice())
	}

	pub(crate) fn get(&self, key: &[u8]) -> Held {
		match self.writes.get(key) {
			Some(Some(value)) => Held::Value(value.clone()),
			Some(None) => Held::Deleted,
			None if self.clears(key) => Held::Deleted,
			None => Held::Storage,
		}
	}

	fn clears(&self, key: &[u8]) -> bool {
		self.cleared.iter().any(|(start, end)| start.as_slice() <= key && key < end.as_slice())
	}

	/// Whether anything the overlay holds falls inside `rng`.
	pub(crate) fn touches(&self, rng: &KeyRange<'_>) -> bool {
		let (start, end) = (rng.start.as_slice(), rng.end.as_slice());
		self.writes
			.range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
			.next()
			.is_some()
			|| self.cleared.iter().any(|(s, e)| s.as_slice() < end && start < e.as_slice())
	}

	pub(crate) fn set(&mut self, key: &Key<'_>, value: Vec<u8>) {
		self.writes.insert(key.as_slice().to_vec(), Some(value));
	}

	pub(crate) fn delete(&mut self, key: &Key<'_>) {
		self.writes.insert(key.as_slice().to_vec(), None);
	}

	pub(crate) fn clear(&mut self, rng: &KeyRange<'_>) {
		let (start, end) = (rng.start.as_slice().to_vec(), rng.end.as_slice().to_vec());
		let held: Vec<Vec<u8>> = self
			.writes
			.range::<[u8], _>((Bound::Included(start.as_slice()), Bound::Excluded(end.as_slice())))
			.map(|(key, _)| key.clone())
			.collect();
		for key in held {
			self.writes.remove(&key);
		}
		self.cleared.push((start, end));
	}

	/// `stored`, the storage's pairs inside `rng` in key order, as the
	/// overlay leaves them: the pairs it deletes dropped, those it writes
	/// replaced or added, in key order.
	pub(crate) fn merge(
		&self,
		rng: &KeyRange<'_>,
		stored: Vec<(Vec<u8>, Vec<u8>)>,
	) -> Vec<(Vec<u8>, Vec<u8>)> {
		let (start, end) = (rng.start.as_slice(), rng.end.as_slice());
		let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = stored
			.into_iter()
			.filter(|(key, _)| !self.clears(key) && !self.writes.contains_key(key))
			.collect();
		for (key, value) in
			self.writes.range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
		{
			if let Some(value) = value {
				merged.insert(key.clone(), value.clone());
			}
		}
		merged.into_iter().collect()
	}
}

/// A cursor over pairs read in full: a speculative read of a range the
/// speculation writes into, served in the order the caller asked for.
pub(crate) struct HeldCursor {
	pairs: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
	key_buf: Vec<u8>,
	val_buf: Vec<u8>,
	key_spans: Vec<KeySpan>,
	val_spans: Vec<KeyValSpan>,
}

impl HeldCursor {
	pub(crate) fn new(pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
		Self {
			pairs: pairs.into_iter(),
			key_buf: Vec::new(),
			val_buf: Vec::new(),
			key_spans: Vec::new(),
			val_spans: Vec::new(),
		}
	}

	/// Load up to `limit` further pairs into the buffers.
	fn fill(&mut self, limit: u32) -> (u64, u64) {
		self.key_buf.clear();
		self.val_buf.clear();
		self.key_spans.clear();
		self.val_spans.clear();
		let (mut key_bytes, mut value_bytes) = (0, 0);
		for (key, value) in self.pairs.by_ref().take(limit as usize) {
			self.key_spans.push(KeySpan {
				offset: self.key_buf.len(),
				len: key.len(),
			});
			self.val_spans.push(KeyValSpan {
				key_offset: self.key_buf.len(),
				key_len: key.len(),
				val_offset: self.val_buf.len(),
				val_len: value.len(),
			});
			key_bytes += key.len() as u64;
			value_bytes += value.len() as u64;
			self.key_buf.extend_from_slice(&key);
			self.val_buf.extend_from_slice(&value);
		}
		(key_bytes, value_bytes)
	}
}

impl ScanCursorKeys for HeldCursor {
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<KeysBatch<'s>>> {
		Box::pin(async move {
			let (key_bytes, _) = self.fill(limit);
			Ok(KeysBatch::from_parts(&self.key_buf, &self.key_spans, key_bytes))
		})
	}

	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn KeyVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move {
			let mut stats = ScanChunkStats::default();
			for (key, _) in self.pairs.by_ref().take(limit as usize) {
				stats.rows += 1;
				stats.key_bytes += key.len() as u64;
				if f(&key)?.is_break() {
					break;
				}
			}
			Ok(stats)
		})
	}
}

impl ScanCursorVals for HeldCursor {
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<ValsBatch<'s>>> {
		Box::pin(async move {
			let (key_bytes, value_bytes) = self.fill(limit);
			Ok(ValsBatch::from_parts(
				&self.key_buf,
				&self.val_buf,
				&self.val_spans,
				key_bytes,
				value_bytes,
			))
		})
	}

	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move {
			let mut stats = ScanChunkStats::default();
			for (key, value) in self.pairs.by_ref().take(limit as usize) {
				stats.rows += 1;
				stats.key_bytes += key.len() as u64;
				stats.value_bytes += value.len() as u64;
				if f(&key, &value)?.is_break() {
					break;
				}
			}
			Ok(stats)
		})
	}
}

/// The storage a speculation's view reads: the transaction it speculates on,
/// as that transaction's own storage answers.
///
/// It takes no write: the view's transactor holds every write in the overlay
/// before one could reach here, and one that did would be a write past the
/// speculation into the transaction beneath, so it is refused. A save point
/// the view opens frames the overlay, not the storage, whose save points are
/// the transaction's own.
pub(crate) struct Base {
	txn: Arc<crate::Transaction>,
	overlay: Arc<Mutex<Option<Overlay>>>,
	/// The overlay as each open save point found it.
	saved: Mutex<Vec<Option<Overlay>>>,
}

impl Base {
	pub(crate) fn new(txn: Arc<crate::Transaction>, overlay: Arc<Mutex<Option<Overlay>>>) -> Self {
		Self {
			txn,
			overlay,
			saved: Mutex::new(Vec::new()),
		}
	}

	fn storage(&self) -> &dyn Transactable {
		self.txn.tr.inner.as_ref()
	}
}

fn refused<'a, T: Send + 'a>() -> BoxFut<'a, Result<T>> {
	Box::pin(async {
		Err(Error::Internal("A speculation's view wrote past its overlay".to_owned()))
	})
}

impl Transactable for Base {
	fn kind(&self) -> &'static str {
		self.storage().kind()
	}

	fn closed(&self) -> bool {
		self.storage().closed()
	}

	fn writeable(&self) -> bool {
		self.storage().writeable()
	}

	/// The view counts through the storage beneath.
	fn parallel_count(&self) -> bool {
		self.storage().parallel_count()
	}

	fn count_cost(&self) -> surrealdb_kvs::CountCost {
		self.storage().count_cost()
	}

	/// The view reads through the storage beneath, so its reads are tracked exactly as the
	/// storage's are.
	fn plain_reads_tracked(&self) -> bool {
		self.storage().plain_reads_tracked()
	}

	/// A speculation reads through the storage beneath, so it declares the storage's views as the
	/// upper bound; its own `snapshot()` answers `None`, which the trait permits under any
	/// declaration.
	fn view_capabilities(&self) -> ViewCapabilities {
		self.storage().view_capabilities()
	}

	/// The view holds nothing to cancel; the transaction beneath is not the
	/// view's to cancel.
	fn cancel<'a>(&'a self) -> BoxFut<'a, Result<()>> {
		Box::pin(async { Ok(()) })
	}

	fn commit<'a>(&'a self) -> BoxFut<'a, Result<()>> {
		refused()
	}

	fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<bool>> {
		self.storage().exists(key, version)
	}

	fn get<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<Option<Val>>> {
		self.storage().get(key, version)
	}

	fn getu<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<Option<Val>>> {
		self.storage().getu(key)
	}

	fn getm<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, Result<GetMultiResult>> {
		self.storage().getm(keys, version)
	}

	fn getr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		self.storage().getr(rng, version)
	}

	fn set<'a>(&'a self, _: Key<'a>, _: Val) -> BoxFut<'a, Result<()>> {
		refused()
	}

	fn put<'a>(&'a self, _: Key<'a>, _: Val) -> BoxFut<'a, Result<()>> {
		refused()
	}

	fn putc<'a>(&'a self, _: Key<'a>, _: Val, _: Option<Val>) -> BoxFut<'a, Result<()>> {
		refused()
	}

	fn del<'a>(&'a self, _: Key<'a>) -> BoxFut<'a, Result<()>> {
		refused()
	}

	fn delc<'a>(&'a self, _: Key<'a>, _: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		refused()
	}

	fn delr<'a>(&'a self, _: KeyRange<'a>) -> BoxFut<'a, Result<()>> {
		refused()
	}

	fn clrr<'a>(&'a self, _: KeyRange<'a>) -> BoxFut<'a, Result<()>> {
		refused()
	}

	fn replace<'a>(&'a self, _: Key<'a>, _: Val) -> BoxFut<'a, Result<()>> {
		refused()
	}

	fn clr<'a>(&'a self, _: Key<'a>) -> BoxFut<'a, Result<()>> {
		refused()
	}

	fn clrc<'a>(&'a self, _: Key<'a>, _: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		refused()
	}

	fn keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		self.storage().keys(rng, limit, skip, version)
	}

	fn keysr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		self.storage().keysr(rng, limit, skip, version)
	}

	fn scan<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		self.storage().scan(rng, limit, skip, version)
	}

	fn scanr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		self.storage().scanr(rng, limit, skip, version)
	}

	fn count<'a>(&'a self, rng: KeyRange<'a>, version: Option<u64>) -> BoxFut<'a, Result<usize>> {
		self.storage().count(rng, version)
	}

	fn count_bounded<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
		budget: u32,
	) -> BoxFut<'a, Result<(usize, Option<Key<'static>>)>> {
		self.storage().count_bounded(rng, version, budget)
	}

	fn snapshot(&self) -> Option<Arc<dyn Snapshot>> {
		None
	}

	fn estimate<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Estimate>> {
		self.storage().estimate(rng, version)
	}

	fn estimate_many<'a>(
		&'a self,
		ranges: Vec<KeyRange<'a>>,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Vec<Estimate>>> {
		self.storage().estimate_many(ranges, version)
	}

	fn approximate<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Option<Estimate>>> {
		self.storage().approximate(rng, version)
	}

	fn split<'a>(
		&'a self,
		rng: KeyRange<'a>,
		target: SplitTarget,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Vec<Key<'static>>>> {
		self.storage().split(rng, target, version)
	}
	fn open_keys_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorKeys + 'a>>> {
		self.storage().open_keys_cursor(rng, dir, skip, version)
	}

	fn open_vals_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorVals + 'a>>> {
		self.storage().open_vals_cursor(rng, dir, skip, version)
	}

	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
		let now = self.overlay.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
		self.saved.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push(now);
		Box::pin(async { Ok(()) })
	}

	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>> {
		let popped = self.saved.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).pop();
		Box::pin(async move { popped.map(|_| ()).ok_or(Error::NoSavepoint) })
	}

	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>> {
		let popped = self.saved.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).pop();
		let rolled = match popped {
			Some(saved) => {
				let mut overlay =
					self.overlay.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
				// A speculation that has ended stays ended.
				if overlay.is_some() {
					*overlay = saved;
				}
				Ok(())
			}
			None => Err(Error::NoSavepoint),
		};
		Box::pin(async move { rolled })
	}

	fn timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		self.storage().timestamp()
	}

	fn safe_timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		self.storage().safe_timestamp()
	}

	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		self.storage().timestamp_impl()
	}

	fn version_impl(&self) -> BoxTimeStampImpl {
		self.storage().version_impl()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn key(bytes: &[u8]) -> Key<'static> {
		Key::from(bytes.to_vec())
	}

	fn range(start: &[u8], end: &[u8]) -> KeyRange<'static> {
		KeyRange {
			start: key(start),
			end: key(end),
		}
	}

	#[test]
	fn a_write_after_a_clear_survives_it() {
		let mut overlay = Overlay::default();
		overlay.set(&key(b"a1"), b"x".to_vec());
		overlay.clear(&range(b"a", b"b"));
		overlay.set(&key(b"a2"), b"y".to_vec());
		assert!(matches!(overlay.get(b"a1"), Held::Deleted));
		assert!(matches!(overlay.get(b"a2"), Held::Value(v) if v == b"y"));
		assert!(matches!(overlay.get(b"a3"), Held::Deleted));
		assert!(matches!(overlay.get(b"b1"), Held::Storage));
	}

	#[test]
	fn a_merge_answers_the_range_as_the_writes_leave_it() {
		let mut overlay = Overlay::default();
		overlay.set(&key(b"a2"), b"new".to_vec());
		overlay.delete(&key(b"a3"));
		overlay.set(&key(b"c"), b"outside".to_vec());
		let stored = vec![
			(b"a1".to_vec(), b"1".to_vec()),
			(b"a2".to_vec(), b"2".to_vec()),
			(b"a3".to_vec(), b"3".to_vec()),
		];
		let merged = overlay.merge(&range(b"a", b"b"), stored);
		assert_eq!(
			merged,
			vec![(b"a1".to_vec(), b"1".to_vec()), (b"a2".to_vec(), b"new".to_vec())]
		);
		assert!(overlay.touches(&range(b"a", b"b")));
		assert!(!overlay.touches(&range(b"d", b"e")));
	}
}
