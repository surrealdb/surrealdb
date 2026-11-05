//! The frontier window: the inline-cache values and adjacency keys that one multi-get and one
//! multi-range read answered for a set of sources, held until the reads that would have made them
//! one by one consume them.
//!
//! A window holds bytes, never an answer. [`adjacency`](super::adjacency) and
//! [`references`](super::references) read a source exactly as they do without a window, through
//! the same decode, `reach`, fuel charges, metrics and fold observation, and each asks the window
//! before it asks storage: a probe of the window answers the cache value of a (source,
//! direction), a range of the window answers the keys of one adjacency range. What the window
//! does not hold is read from storage by the position itself, so a failed fill, a range cut at its
//! limit, a source the fill never reached and a folded or lightweight adjacency all read as they
//! do without a window.
//!
//! # Bounds
//!
//! A fill takes its sources [`SOURCES_PER_FILL`] at a time and reads each group's probes and ranges
//! before it builds the next group's, so it holds no more ranges than a group of sources has,
//! however many sources the caller hands it. It reads at most [`RANGES_PER_FILL`] ranges in one
//! call and at most [`KEYS_PER_RANGE`] keys from any one of them, and stops once what it holds
//! passes the budget the caller passes: the key bytes
//! and an estimate of the heap each decoded inline-cache probe holds both count. The probes it
//! has not read, the ranges it has not reached and the sources they stand for are unread, and the
//! range it stopped in is cut. A cut range serves the keys it holds and the position continues
//! from a cursor opened after the last of them. The inline caches are read [`PROBES_PER_CALL`]
//! at a time, so a call decodes no more than that many cache values before the budget is
//! checked. The key buffers, and the lists a group of sources builds (their facts, the caches and
//! ranges still wanted, the ranges to read), are kept with the window and reused from fill to fill,
//! so a warm window allocates nothing for a key and none of those lists. A fill takes its sources
//! as the slice its caller holds ([`Source`]) and builds no list of them. What it still allocates
//! is what the reads it makes allocate: the key ranges it builds, one vector for each call of a
//! multi-range read and of a multi-get, and what the decoded cache entries own.
//!
//! # Fuel
//!
//! A fill is work between two yield points like any other, so it draws on the thread's meter
//! ([`fuel`]) and keeps the bound `vm/fuel.rs` states. Each read it makes takes no more entries
//! than the fuel left, and is charged when it returns: a multi-range read counts the keys it
//! visited, a probe read the probes and the cache entries they decoded, and a group the sources it
//! resolves. A multi-range read seeks no more ranges than the fuel left and charges them before it
//! seeks them, so its keys are bounded by what remains. A read that would start on fewer than
//! [`MIN_READ`] units reaches a yield point first, so that a nearly spent meter does not cut a
//! range short. The
//! positions that consume a window charge their own reads as they do without one. An interruption
//! a yield raises ends the fill like any other failure to read: the window holds what it held, and
//! the next position's charge raises it again, the meter being spent.
//!
//! # Identity
//!
//! A probe is told by the encoded inline-cache key it read, and a range by the read it was built
//! for: the same encoded key of the source (or referenced record) and direction, the address of
//! the hop's key ranges (or reference subject), and the range's place among the hop's ranges.
//! The encoded keys lie in the window's byte buffer, so no id is held on the heap, and a
//! position names its read the same way: it is handed the range the fill built for it instead of
//! building it again, and builds one only for a read the window holds no range for. A range two
//! hops read, as two hops over the same source do, is read once and named for each of them. Hashes
//! find a candidate; the bytes decide. A hop window's fill reads nothing the window of the rows
//! holds, since the positions read it from there.
//!
//! # Lifetime
//!
//! The windows are three, each with a lifetime of its own: the hop window a lookup fills for the
//! sources of one hop and drops when the hop has read them, the origins window filled for one
//! group of the origins of an array and dropped when the group has been answered, and the rows
//! window filled once for a batch of rows and dropped when the next batch replaces it. A lookup
//! fills a window only when none of that kind stands: one that stands is an enclosing lookup's,
//! which it reads and never fills or drops, so a window never holds what a later fill of another
//! lifetime appended. A window is filled where nothing writes between the fill and the last
//! position it serves: the positions of one hop run no code but their own reads, and the plan
//! admits a rows window only for a statement that writes nothing.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::ops::Bound;

use anyhow::Context as _;
use surrealdb_datastore::values::inline_cache::{CacheValue, EdgeCacheEntry, RefCacheEntry};
use surrealdb_kvs::{Key, KeyRange as KvRange};

use super::{At, Facts, HopRead, KeyRanges, ReferenceSubject, facts, origin_id};
use crate::exec::operators::scan::graph_keys::{GraphRange, graph_ranges};
use crate::exec::vm::descriptor::TableMemo;
use crate::exec::vm::fuel;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::Dir;
use crate::idx::adjacency::VertexAdjacency;
use crate::key::schema::{EdgeCacheKey, RefCacheKey};
use crate::key::{KVKey, TypedRange};
use crate::kvs::NORMAL_BATCH_SIZE;
use crate::val::{RecordId, Value};

/// The most key ranges one fill reads.
pub(super) const RANGES_PER_FILL: usize = 2 * NORMAL_BATCH_SIZE as usize;

/// The most keys one fill reads from one range, which a cursor continues from where a range holds
/// more.
pub(super) const KEYS_PER_RANGE: u32 = NORMAL_BATCH_SIZE;

/// How many sources one fill of a hop covers.
pub(in crate::exec::vm) const SOURCES_PER_FILL: usize = NORMAL_BATCH_SIZE as usize;

/// The most inline caches one multi-get of a fill reads.
const PROBES_PER_CALL: usize = 64;

/// The fewest units of fuel a read of a fill starts on: with fewer left the fill reaches a yield
/// point first, since a read bounded by the fuel left would cut ranges short for the sake of a
/// meter about to be refilled.
const MIN_READ: u32 = PROBES_PER_CALL as u32;

/// The most bytes a window holds, whatever the scan batch bytes allow.
const WINDOW_BYTES: usize = 1 << 20;

/// A value a fill reads the adjacency of when it names a record: a record id, or an element of an
/// array of origins that is a record id or an object holding one as its `id`. A fill takes its
/// sources as the slice its caller holds, so it builds no list of them.
pub(in crate::exec::vm) trait Source {
	/// The record the value names, if it names one.
	fn record(&self) -> Option<&RecordId>;
}

impl Source for RecordId {
	fn record(&self) -> Option<&RecordId> {
		Some(self)
	}
}

impl Source for Value {
	fn record(&self) -> Option<&RecordId> {
		origin_id(self)
	}
}

/// The record a source the fill kept names.
fn record_of<S: Source>(source: &S) -> &RecordId {
	source.record().expect("a source the fill kept names a record")
}

/// The most bytes of keys one multi-range read appends, so that the read holds the thread it runs
/// on about as long as a chunk of a cursor does; a fill that is not done reads again.
const CALL_BYTES: usize = 256 << 10;

/// The bytes of keys one multi-range read appends.
fn call_bytes() -> usize {
	#[cfg(test)]
	if let Some(pinned) = counters::CALL_BYTES.with(std::cell::Cell::get) {
		return pinned;
	}
	CALL_BYTES
}

/// The units a read of a fill may take, which is no more than the fuel left: a yield point comes
/// first when fewer than [`MIN_READ`] are left. An interruption the yield raises is the fill's
/// failure.
async fn room(ctx: &ExecutionContext) -> FlowResult<u32> {
	let left = fuel::left();
	if left < MIN_READ {
		fuel::tick(ctx, left).await?;
	}
	Ok(fuel::batch(u32::MAX))
}

/// The bytes of keys a window may hold in the context `ctx`.
pub(super) fn budget(ctx: &ExecutionContext) -> usize {
	ctx.root().ctx.config.exec.scan_batch_bytes.min(WINDOW_BYTES)
}

/// What the fills and the positions of a test run did, for the tests that compare a run with
/// windows to one without.
#[cfg(test)]
pub(crate) mod counters {
	use std::cell::Cell;

	thread_local! {
		static FILLED: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
		static SERVED: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
		static ROWS: Cell<usize> = const { Cell::new(0) };
		static LEVELS: Cell<usize> = const { Cell::new(0) };
		static LEFT: Cell<usize> = const { Cell::new(0) };
		static ENDED_HOLDING: Cell<usize> = const { Cell::new(0) };
		static PEAK_GROUP: Cell<usize> = const { Cell::new(0) };
		static BUILT: Cell<usize> = const { Cell::new(0) };
		static CALL_KEYS: Cell<usize> = const { Cell::new(0) };
		static CALL_RANGES: Cell<usize> = const { Cell::new(0) };
		static SPAWNED: Cell<usize> = const { Cell::new(0) };
		/// Pins whether the sources of a hop are read side by side from the start, instead of
		/// after the first window of them was timed.
		pub(crate) static ENGAGED: Cell<Option<bool>> = const { Cell::new(None) };
		/// Pins the bytes one multi-range read appends, so that a test crosses calls with a few
		/// keys.
		pub(crate) static CALL_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
	}

	/// Forget what was counted.
	pub(crate) fn reset() {
		FILLED.with(|filled| filled.set((0, 0)));
		SERVED.with(|served| served.set((0, 0)));
		ROWS.with(|rows| rows.set(0));
		LEVELS.with(|levels| levels.set(0));
		LEFT.with(|left| left.set(0));
		ENDED_HOLDING.with(|ended| ended.set(0));
		PEAK_GROUP.with(|peak| peak.set(0));
		BUILT.with(|built| built.set(0));
		CALL_KEYS.with(|keys| keys.set(0));
		CALL_RANGES.with(|ranges| ranges.set(0));
		SPAWNED.with(|spawned| spawned.set(0));
	}

	/// The most keys one multi-range read visited.
	pub(crate) fn call_keys() -> usize {
		CALL_KEYS.with(Cell::get)
	}

	/// The reads of one source that were spawned as tasks of their own.
	pub(crate) fn spawned() -> usize {
		SPAWNED.with(Cell::get)
	}

	pub(in crate::exec::vm) fn spawn() {
		SPAWNED.with(|spawned| spawned.set(spawned.get() + 1));
	}

	/// The most ranges one multi-range read sought.
	pub(crate) fn call_ranges() -> usize {
		CALL_RANGES.with(Cell::get)
	}

	pub(super) fn call(keys: usize, ranges: usize) {
		CALL_KEYS.with(|most| most.set(most.get().max(keys)));
		CALL_RANGES.with(|most| most.set(most.get().max(ranges)));
	}

	/// The key ranges the positions built for themselves: a read that no window held a range for.
	pub(crate) fn built() -> usize {
		BUILT.with(Cell::get)
	}

	pub(in crate::exec::vm) fn build() {
		BUILT.with(|built| built.set(built.get() + 1));
	}

	/// The most ranges one group of a fill built.
	pub(crate) fn peak_group() -> usize {
		PEAK_GROUP.with(Cell::get)
	}

	pub(super) fn group(ranges: usize) {
		PEAK_GROUP.with(|peak| peak.set(peak.get().max(ranges)));
	}

	/// The scans that ended while the window of their batch of rows still held something.
	pub(crate) fn ended_holding() -> usize {
		ENDED_HOLDING.with(Cell::get)
	}

	pub(in crate::exec::vm) fn end_held() {
		ENDED_HOLDING.with(|ended| ended.set(ended.get() + 1));
	}

	/// The lookups that ended with a hop window they filled still held.
	pub(crate) fn left() -> usize {
		LEFT.with(Cell::get)
	}

	pub(in crate::exec::vm) fn left_behind() {
		LEFT.with(|left| left.set(left.get() + 1));
	}

	/// The levels of a path a rows window was filled for.
	pub(crate) fn levels() -> usize {
		LEVELS.with(Cell::get)
	}

	pub(in crate::exec::vm) fn level() {
		LEVELS.with(|levels| levels.set(levels.get() + 1));
	}

	/// The batches of rows whose window held something when it was filled.
	pub(crate) fn rows_filled() -> usize {
		ROWS.with(Cell::get)
	}

	pub(in crate::exec::vm) fn rows_fill() {
		ROWS.with(|rows| rows.set(rows.get() + 1));
	}

	/// The fills that held something, and the ranges and probes they held.
	pub(crate) fn filled() -> (usize, usize) {
		FILLED.with(Cell::get)
	}

	/// The ranges and the probes positions read from a window.
	pub(crate) fn served() -> (usize, usize) {
		SERVED.with(Cell::get)
	}

	pub(super) fn fill(held: usize) {
		if held > 0 {
			FILLED.with(|filled| {
				let (fills, held_before) = filled.get();
				filled.set((fills + 1, held_before + held));
			});
		}
	}

	pub(super) fn serve(range: bool) {
		SERVED.with(|served| {
			let (ranges, probes) = served.get();
			served.set(if range {
				(ranges + 1, probes)
			} else {
				(ranges, probes + 1)
			});
		});
	}
}

/// The adjacency keys of one range, as a fill read them: a span of [`Frontier::keys`].
struct RangeSlot {
	/// Where the range's start and end bytes lie in the buffer.
	start: (u32, u32),
	end: (u32, u32),
	/// The first of the range's keys in `keys`, and how many the fill read.
	first: u32,
	count: u32,
	/// The range may hold more keys than the fill read.
	cut: bool,
	/// The encoded inline-cache key of the read the range was first named for, once it is named:
	/// a range's bounds name one source and direction, or one referenced record, so every read
	/// named for it shares the key.
	ident: Option<(u32, u32)>,
}

/// The read a range answers: the key range of one hop, at one place among the hop's ranges, over
/// one source along one direction or one referenced record. A position names the read the same
/// way and is handed the range without building it. A range several hops read, as two hops over
/// the same source do, has an owner for each.
struct Owner {
	/// The range the read is answered by: its place in [`Frontier::ranges`].
	slot: u32,
	/// The encoded inline-cache key of the source and direction, or of the referenced record: a
	/// span of [`Frontier::bytes`] shared by every range of the source.
	ident: (u32, u32),
	/// The address of the hop's key ranges, or of its reference subject.
	hop: usize,
	/// The range's place among the hop's ranges.
	place: u16,
}

/// The inline adjacency cache of one (source, direction) as a fill read it: `None` where no cache
/// entry exists. The probe is told by the encoded key it read, a span of [`Frontier::bytes`].
struct EdgeProbe {
	ident: (u32, u32),
	value: Option<CacheValue<EdgeCacheEntry>>,
}

/// The inline reference cache of one referenced record as a fill read it: `None` where no cache
/// entry exists. The probe is told by the encoded key it read, a span of [`Frontier::bytes`].
struct RefProbe {
	ident: (u32, u32),
	value: Option<CacheValue<RefCacheEntry>>,
}

/// What one or more fills read, for the positions to consume.
#[derive(Default)]
pub(in crate::exec::vm) struct Frontier {
	/// The bytes of the ranges' bounds and of the keys read.
	bytes: Vec<u8>,
	/// Where each key lies in `bytes`.
	keys: Vec<(u32, u32)>,
	ranges: Vec<RangeSlot>,
	by_range: ByHash,
	/// The reads the ranges answer, and where the owner of a read is found by the hash of it.
	owners: Vec<Owner>,
	by_owner: ByHash,
	edges: Vec<EdgeProbe>,
	by_edge: ByHash,
	refs: Vec<RefProbe>,
	by_ref: ByHash,
	/// The heap the probes hold, as [`probe_bytes`] estimates it.
	probe_bytes: usize,
	/// What the fills of this window build for a group of sources, kept across fills and across
	/// [`Frontier::clear`].
	scratch: Scratch,
}

/// The ends of a window's buffers of keys at one moment.
#[derive(Clone, Copy)]
struct Mark {
	bytes: usize,
	keys: usize,
}

/// The keys of one range a window holds.
pub(super) struct Served<'f> {
	frontier: &'f Frontier,
	slot: &'f RangeSlot,
}

impl<'f> Served<'f> {
	/// The keys the fill read from the range, in key order.
	pub(super) fn keys(&self) -> impl Iterator<Item = &'f [u8]> + use<'f> {
		let frontier = self.frontier;
		let first = self.slot.first as usize;
		frontier.keys[first..first + self.slot.count as usize]
			.iter()
			.map(move |&(at, len)| &frontier.bytes[at as usize..(at + len) as usize])
	}

	/// Whether the range may hold keys after the last the fill read.
	pub(super) fn cut(&self) -> bool {
		self.slot.cut
	}

	/// The last key the fill read from the range, when it read any.
	pub(super) fn last(&self) -> Option<&'f [u8]> {
		let frontier = self.frontier;
		let (at, len) =
			*frontier.keys[self.slot.first as usize..][..self.slot.count as usize].last()?;
		Some(&frontier.bytes[at as usize..(at + len) as usize])
	}
}

/// An estimate of the heap an inline adjacency cache probe holds: the entries of its decoded
/// value.
fn edge_probe_bytes(value: Option<&CacheValue<EdgeCacheEntry>>) -> usize {
	let entries = match value {
		Some(CacheValue::Live(entries)) => entries
			.iter()
			.map(|entry| {
				std::mem::size_of::<EdgeCacheEntry>() + entry.edge.len() + entry.target.len()
			})
			.sum(),
		Some(CacheValue::Spilled) | None => 0,
	};
	std::mem::size_of::<EdgeProbe>() + entries
}

/// An estimate of the heap an inline reference cache probe holds.
fn ref_probe_bytes(value: Option<&CacheValue<RefCacheEntry>>) -> usize {
	let entries = match value {
		Some(CacheValue::Live(entries)) => entries
			.iter()
			.map(|entry| std::mem::size_of::<RefCacheEntry>() + entry.reference.len())
			.sum(),
		Some(CacheValue::Spilled) | None => 0,
	};
	std::mem::size_of::<RefProbe>() + entries
}

fn hash_of(parts: impl FnOnce(&mut DefaultHasher)) -> u64 {
	let mut hasher = DefaultHasher::new();
	parts(&mut hasher);
	hasher.finish()
}

fn range_hash(start: &[u8], end: &[u8]) -> u64 {
	hash_of(|hasher| {
		start.hash(hasher);
		end.hash(hasher);
	})
}

fn ref_hash(target: &RecordId) -> u64 {
	hash_of(|hasher| {
		target.table.hash(hasher);
		target.key.hash(hasher);
	})
}

/// The hasher of a map keyed by a hash the window computed itself ([`range_hash`], [`owner_hash`],
/// [`bytes_hash`]): the key is its own hash, so it is not hashed a second time. Two keys whose
/// hashes collide share an entry; the window compares the bytes behind an entry before it answers
/// one, so the second of them is not held and its read is made for itself.
#[derive(Default)]
struct Prehashed(u64);

impl Hasher for Prehashed {
	fn finish(&self) -> u64 {
		self.0
	}

	fn write(&mut self, bytes: &[u8]) {
		for &byte in bytes {
			self.0 = self.0.rotate_left(8) ^ u64::from(byte);
		}
	}

	fn write_u64(&mut self, hash: u64) {
		self.0 = hash;
	}
}

/// A map from a hash the window computed to the place of what it names.
type ByHash = HashMap<u64, u32, BuildHasherDefault<Prehashed>>;
/// A set of hashes the window computed.
type HashesSeen = HashSet<u64, BuildHasherDefault<Prehashed>>;

fn bytes_hash(bytes: &[u8]) -> u64 {
	hash_of(|hasher| bytes.hash(hasher))
}

fn owner_hash(ident: &[u8], hop: usize, place: u16) -> u64 {
	hash_of(|hasher| {
		ident.hash(hasher);
		hop.hash(hasher);
		place.hash(hasher);
	})
}

/// The encoded inline-cache key a window tells a probe or a read by: the bytes of `key`, in a
/// buffer the thread reuses, handed to `with` and not kept past it.
fn with_encoded<R>(key: &impl KVKey, with: impl FnOnce(&[u8]) -> R) -> Option<R> {
	thread_local! {
		static SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
	}
	SCRATCH.with(|scratch| {
		let mut scratch = scratch.borrow_mut();
		scratch.clear();
		key.encode_buffer(&mut scratch).ok()?;
		Some(with(&scratch))
	})
}

/// The inline adjacency cache key of `source` along `dir`.
fn edge_key<'a>(at: &At, source: &'a RecordId, dir: Dir) -> EdgeCacheKey<'a> {
	EdgeCacheKey {
		ns: at.ns,
		db: at.db,
		tb: std::borrow::Cow::Borrowed(&source.table),
		id: std::borrow::Cow::Borrowed(&source.key),
		dir,
	}
}

/// The inline reference cache key of `target`.
fn ref_key<'a>(at: &At, target: &'a RecordId) -> RefCacheKey<'a> {
	RefCacheKey {
		ns: at.ns,
		db: at.db,
		tb: std::borrow::Cow::Borrowed(&target.table),
		id: std::borrow::Cow::Borrowed(&target.key),
	}
}

impl Frontier {
	/// Forget everything held, keeping the buffers.
	pub(in crate::exec::vm) fn clear(&mut self) {
		self.bytes.clear();
		self.keys.clear();
		self.ranges.clear();
		self.by_range.clear();
		self.owners.clear();
		self.by_owner.clear();
		self.edges.clear();
		self.by_edge.clear();
		self.refs.clear();
		self.by_ref.clear();
		self.probe_bytes = 0;
	}

	/// The bytes the window holds: its keys and bounds, and what its probes hold.
	fn held(&self) -> usize {
		self.bytes.len().saturating_add(self.probe_bytes)
	}

	/// Where the buffers of keys end, for [`Frontier::rollback`] to cut back to.
	fn mark(&self) -> Mark {
		Mark {
			bytes: self.bytes.len(),
			keys: self.keys.len(),
		}
	}

	/// Cut the buffers of keys back to `mark`: what a read appended since is no range a slot names,
	/// so it holds no budget.
	fn rollback(&mut self, mark: Mark) {
		self.bytes.truncate(mark.bytes);
		self.keys.truncate(mark.keys);
	}

	/// Whether the window holds `budget` bytes.
	pub(in crate::exec::vm) fn is_full(&self, budget: usize) -> bool {
		self.held() >= budget
	}

	/// Whether the window holds nothing.
	pub(in crate::exec::vm) fn is_empty(&self) -> bool {
		self.ranges.is_empty() && self.edges.is_empty() && self.refs.is_empty()
	}

	fn slice(&self, (at, len): (u32, u32)) -> &[u8] {
		&self.bytes[at as usize..(at + len) as usize]
	}

	fn find_range(&self, start: &[u8], end: &[u8]) -> Option<Served<'_>> {
		let at = *self.by_range.get(&range_hash(start, end))?;
		let slot = &self.ranges[at as usize];
		(self.slice(slot.start) == start && self.slice(slot.end) == end).then_some(Served {
			frontier: self,
			slot,
		})
	}

	/// The keys of the range `[start, end)`, where the window holds them.
	pub(super) fn range(&self, start: &[u8], end: &[u8]) -> Option<Served<'_>> {
		let served = self.find_range(start, end);
		#[cfg(test)]
		if served.is_some() {
			counters::serve(true);
		}
		served
	}

	/// The keys of the range a fill built for the read of `ident` by the range at `place` of `hop`:
	/// the keys [`Frontier::range`] answers for the range's bounds, found without building them.
	/// `ident` is the encoded inline-cache key of the source and direction, or of the referenced
	/// record.
	fn find_owned(&self, ident: &[u8], hop: usize, place: u16) -> Option<Served<'_>> {
		let at = *self.by_owner.get(&owner_hash(ident, hop, place))?;
		let owner = &self.owners[at as usize];
		(owner.hop == hop && owner.place == place && self.slice(owner.ident) == ident).then(|| {
			Served {
				frontier: self,
				slot: &self.ranges[owner.slot as usize],
			}
		})
	}

	/// [`Frontier::find_owned`] for the read of `source` along `dir`.
	fn peek_owned(
		&self,
		at: &At,
		source: &RecordId,
		dir: Dir,
		(hop, place): (usize, u16),
	) -> Option<Served<'_>> {
		with_encoded(&edge_key(at, source, dir), |ident| self.find_owned(ident, hop, place))
			.flatten()
	}

	/// [`Frontier::find_owned`] for the read of the records referencing `target`.
	fn peek_owned_references(&self, at: &At, target: &RecordId, hop: usize) -> Option<Served<'_>> {
		with_encoded(&ref_key(at, target), |ident| self.find_owned(ident, hop, 0)).flatten()
	}

	/// [`Frontier::peek_owned`], for a position that reads what it is handed.
	fn owned_range(
		&self,
		at: &At,
		source: &RecordId,
		dir: Dir,
		hop: (usize, u16),
	) -> Option<Served<'_>> {
		let served = self.peek_owned(at, source, dir, hop);
		#[cfg(test)]
		if served.is_some() {
			counters::serve(true);
		}
		served
	}

	/// [`Frontier::peek_owned_references`], for a position that reads what it is handed.
	fn owned_references(&self, at: &At, target: &RecordId, hop: usize) -> Option<Served<'_>> {
		let served = self.peek_owned_references(at, target, hop);
		#[cfg(test)]
		if served.is_some() {
			counters::serve(true);
		}
		served
	}

	/// The inline adjacency cache of `source` along `dir`, where the window holds an answer: the
	/// outer `Some` says a fill read it, the inner `None` that it found no entry.
	pub(super) fn edge_probe(
		&self,
		at: &At,
		source: &RecordId,
		dir: Dir,
	) -> Option<Option<&CacheValue<EdgeCacheEntry>>> {
		let probe = self.find_edge(at, source, dir);
		#[cfg(test)]
		if probe.is_some() {
			counters::serve(false);
		}
		probe
	}

	fn find_edge(
		&self,
		at: &At,
		source: &RecordId,
		dir: Dir,
	) -> Option<Option<&CacheValue<EdgeCacheEntry>>> {
		with_encoded(&edge_key(at, source, dir), |key| self.find_edge_key(key)).flatten()
	}

	fn find_edge_key(&self, key: &[u8]) -> Option<Option<&CacheValue<EdgeCacheEntry>>> {
		let at = *self.by_edge.get(&bytes_hash(key))?;
		let probe = &self.edges[at as usize];
		(self.slice(probe.ident) == key).then_some(probe.value.as_ref())
	}

	/// The inline reference cache of `target`, where the window holds an answer: the outer `Some`
	/// says a fill read it, the inner `None` that it found no entry.
	pub(super) fn ref_probe(
		&self,
		at: &At,
		target: &RecordId,
	) -> Option<Option<&CacheValue<RefCacheEntry>>> {
		let probe = self.find_ref(at, target);
		#[cfg(test)]
		if probe.is_some() {
			counters::serve(false);
		}
		probe
	}

	fn find_ref(&self, at: &At, target: &RecordId) -> Option<Option<&CacheValue<RefCacheEntry>>> {
		with_encoded(&ref_key(at, target), |key| self.find_ref_key(key)).flatten()
	}

	fn find_ref_key(&self, key: &[u8]) -> Option<Option<&CacheValue<RefCacheEntry>>> {
		let at = *self.by_ref.get(&bytes_hash(key))?;
		let probe = &self.refs[at as usize];
		(self.slice(probe.ident) == key).then_some(probe.value.as_ref())
	}

	/// Hold what a read of the inline reference cache found for `target`. A probe the window
	/// already holds, or whose hash another's shares, is not held twice.
	fn push_ref_probe(
		&mut self,
		at: &At,
		target: &RecordId,
		value: Option<CacheValue<RefCacheEntry>>,
	) {
		let Some(ident) = self.push_ident(&ref_key(at, target)) else {
			return;
		};
		let hash = bytes_hash(self.slice(ident));
		if self.by_ref.contains_key(&hash) {
			self.bytes.truncate(ident.0 as usize);
			return;
		}
		self.by_ref.insert(hash, self.refs.len() as u32);
		self.probe_bytes = self.probe_bytes.saturating_add(ref_probe_bytes(value.as_ref()));
		self.refs.push(RefProbe {
			ident,
			value,
		});
	}

	/// Hold what a read of the inline adjacency cache found for `source` along `dir`. A probe the
	/// window already holds, or whose hash another's shares, is not held twice.
	fn push_edge_probe(
		&mut self,
		at: &At,
		source: &RecordId,
		dir: Dir,
		value: Option<CacheValue<EdgeCacheEntry>>,
	) {
		let Some(ident) = self.push_ident(&edge_key(at, source, dir)) else {
			return;
		};
		let hash = bytes_hash(self.slice(ident));
		if self.by_edge.contains_key(&hash) {
			self.bytes.truncate(ident.0 as usize);
			return;
		}
		self.by_edge.insert(hash, self.edges.len() as u32);
		self.probe_bytes = self.probe_bytes.saturating_add(edge_probe_bytes(value.as_ref()));
		self.edges.push(EdgeProbe {
			ident,
			value,
		});
	}

	/// Where the encoded inline-cache key of `source` along `dir` lies in the window's bytes: the
	/// span a probe of the cache holds, where the window holds one, and otherwise the key appended.
	fn edge_ident(&mut self, at: &At, source: &RecordId, dir: Dir) -> Option<(u32, u32)> {
		let ident = self.push_ident(&edge_key(at, source, dir))?;
		let held = self
			.by_edge
			.get(&bytes_hash(self.slice(ident)))
			.map(|&at| self.edges[at as usize].ident)
			.filter(|held| self.slice(*held) == self.slice(ident));
		Some(match held {
			Some(held) => {
				self.bytes.truncate(ident.0 as usize);
				held
			}
			None => ident,
		})
	}

	/// [`Frontier::edge_ident`] for the inline reference cache key of `target`.
	fn ref_ident(&mut self, at: &At, target: &RecordId) -> Option<(u32, u32)> {
		let ident = self.push_ident(&ref_key(at, target))?;
		let held = self
			.by_ref
			.get(&bytes_hash(self.slice(ident)))
			.map(|&at| self.refs[at as usize].ident)
			.filter(|held| self.slice(*held) == self.slice(ident));
		Some(match held {
			Some(held) => {
				self.bytes.truncate(ident.0 as usize);
				held
			}
			None => ident,
		})
	}

	/// Append the encoded `key` to the window's bytes and answer where it lies.
	fn push_ident(&mut self, key: &impl KVKey) -> Option<(u32, u32)> {
		let start = self.bytes.len();
		key.encode_buffer(&mut self.bytes).ok()?;
		Some((start as u32, (self.bytes.len() - start) as u32))
	}

	/// Name the read the range at slot `slot` answers: the range at `place` among the ranges of
	/// `hop`, over the source or referenced record whose encoded inline-cache key is `ident`, a
	/// span of the window's bytes. A read already named by another range, or whose hash another's
	/// shares, keeps the first.
	fn own(&mut self, slot: usize, ident: (u32, u32), hop: usize, place: u16) {
		if let Some(range) = self.ranges.get_mut(slot) {
			range.ident.get_or_insert(ident);
		}
		let hash = owner_hash(self.slice(ident), hop, place);
		if let std::collections::hash_map::Entry::Vacant(vacant) = self.by_owner.entry(hash) {
			vacant.insert(self.owners.len() as u32);
			self.owners.push(Owner {
				slot: slot as u32,
				ident,
				hop,
				place,
			});
		}
	}

	/// Name the range at `slot`, which an earlier read of the window holds, for the read of
	/// `source` along `dir` by the range at `place` of the hop `hop`, unless the window names the
	/// read already.
	fn name_held(
		&mut self,
		at: &At,
		source: &RecordId,
		dir: Dir,
		slot: usize,
		(hop, place): (usize, u16),
	) {
		if self.peek_owned(at, source, dir, (hop, place)).is_some() {
			return;
		}
		if let Some(ident) = self.ranges[slot].ident.or_else(|| self.edge_ident(at, source, dir)) {
			self.own(slot, ident, hop, place);
		}
	}

	/// [`Frontier::name_held`] for the read of the records referencing `target` by the subject
	/// `hop`.
	fn name_held_references(&mut self, at: &At, target: &RecordId, slot: usize, hop: usize) {
		if self.peek_owned_references(at, target, hop).is_some() {
			return;
		}
		if let Some(ident) = self.ranges[slot].ident.or_else(|| self.ref_ident(at, target)) {
			self.own(slot, ident, hop, 0);
		}
	}

	/// The place in [`Frontier::ranges`] of the range `[start, end)`, where the window holds it.
	fn slot_of(&self, start: &[u8], end: &[u8]) -> Option<usize> {
		let at = *self.by_range.get(&range_hash(start, end))?;
		let slot = &self.ranges[at as usize];
		(self.slice(slot.start) == start && self.slice(slot.end) == end).then_some(at as usize)
	}
}

/// The windows a position reads, the hop's, the origins' and the rows' in that order; any may be
/// absent.
#[derive(Clone, Copy, Default)]
pub(in crate::exec::vm) struct Windows<'a> {
	pub(in crate::exec::vm) hop: Option<&'a Frontier>,
	pub(in crate::exec::vm) origins: Option<&'a Frontier>,
	pub(in crate::exec::vm) rows: Option<&'a Frontier>,
}

impl<'a> Windows<'a> {
	/// No window: every read goes to storage.
	pub(super) const NONE: Windows<'static> = Windows {
		hop: None,
		origins: None,
		rows: None,
	};

	fn each(&self) -> impl Iterator<Item = &'a Frontier> {
		[self.hop, self.origins, self.rows]
			.into_iter()
			.flatten()
			.filter(|window| !window.is_empty())
	}

	pub(super) fn range(&self, start: &[u8], end: &[u8]) -> Option<Served<'a>> {
		self.each().find_map(|window| window.range(start, end))
	}

	pub(super) fn edge_probe(
		&self,
		at: &At,
		source: &RecordId,
		dir: Dir,
	) -> Option<Option<&'a CacheValue<EdgeCacheEntry>>> {
		self.each().find_map(|window| window.edge_probe(at, source, dir))
	}

	pub(super) fn ref_probe(
		&self,
		at: &At,
		target: &RecordId,
	) -> Option<Option<&'a CacheValue<RefCacheEntry>>> {
		self.each().find_map(|window| window.ref_probe(at, target))
	}

	/// Whether the windows hold what the read of `source`'s adjacency along `directions` over
	/// `ranges` takes from storage: a live inline cache for each direction, or the whole of each
	/// of the direction's ranges, a range a fill cut being held only in part. A read at a version,
	/// of an adjacency that has folded or is lightweight, or of a cache or range no window holds,
	/// is not. It counts nothing as served and is a routing decision only: a read it says the
	/// windows answer may still open a cursor for a vertex whose key names none, and a read it
	/// says they do not answer reads what they hold all the same.
	#[cfg(not(target_family = "wasm"))]
	pub(super) fn serves_adjacency(
		&self,
		at: &At,
		source: &RecordId,
		(kind, edges_cap): Facts,
		(directions, ranges): (&[Dir], &KeyRanges),
	) -> bool {
		if at.version.is_some()
			|| !matches!(
				kind,
				VertexAdjacency::Normal {
					folded: false
				}
			) {
			return false;
		}
		let hop = hop_id(ranges);
		directions.iter().all(|&dir| {
			if edges_cap.is_some() && ranges.whole {
				match self.each().find_map(|window| window.find_edge(at, source, dir)) {
					Some(Some(CacheValue::Live(_))) => return true,
					Some(_) => {}
					None => return false,
				}
			}
			(0..ranges.specs.len().max(1)).all(|place| {
				self.each()
					.find_map(|window| window.peek_owned(at, source, dir, (hop, place as u16)))
					.is_some_and(|served| !served.cut())
			})
		})
	}

	/// Whether the windows hold any part of what the read of `source`'s adjacency along
	/// `directions` over `ranges` takes from storage: a live inline cache of a direction, or a
	/// range of it, whole or cut. A read that takes part of its keys from the windows reads the
	/// rest after them, so it runs where the windows are, never on a task of its own. Like
	/// [`Self::serves_adjacency`] it counts nothing as served and is a routing decision only.
	#[cfg(not(target_family = "wasm"))]
	pub(super) fn holds_adjacency(
		&self,
		at: &At,
		source: &RecordId,
		(kind, edges_cap): Facts,
		(directions, ranges): (&[Dir], &KeyRanges),
	) -> bool {
		if at.version.is_some()
			|| !matches!(
				kind,
				VertexAdjacency::Normal {
					folded: false
				}
			) {
			return false;
		}
		let hop = hop_id(ranges);
		directions.iter().any(|&dir| {
			(edges_cap.is_some()
				&& ranges.whole
				&& matches!(
					self.each().find_map(|window| window.find_edge(at, source, dir)),
					Some(Some(CacheValue::Live(_)))
				)) || (0..ranges.specs.len().max(1)).any(|place| {
				self.each()
					.any(|window| window.peek_owned(at, source, dir, (hop, place as u16)).is_some())
			})
		})
	}

	/// The keys the read of `source` along `dir` by the range at `place` of the hop `hop` names
	/// (see [`hop_id`]) holds, where a fill built that range for the read.
	pub(super) fn owned_range(
		&self,
		at: &At,
		source: &RecordId,
		dir: Dir,
		hop: (usize, u16),
	) -> Option<Served<'a>> {
		self.each().find_map(|window| window.owned_range(at, source, dir, hop))
	}

	/// The keys the read of the records referencing `target` by the subject `hop` names holds,
	/// where a fill built that range for the read.
	pub(super) fn owned_references(
		&self,
		at: &At,
		target: &RecordId,
		hop: usize,
	) -> Option<Served<'a>> {
		self.each().find_map(|window| window.owned_references(at, target, hop))
	}
}

/// What names a hop to the window: the address of its key ranges or of its reference subject,
/// which the program holds for the whole run.
pub(super) fn hop_id<T>(hop: &T) -> usize {
	std::ptr::from_ref(hop) as usize
}

/// The inline adjacency cache of `source` along `dir` as `window` or the window of the rows holds
/// it.
fn held_edge<'f>(
	window: &'f Frontier,
	rows: Option<&'f Frontier>,
	at: &At,
	source: &RecordId,
	dir: Dir,
) -> Option<Option<&'f CacheValue<EdgeCacheEntry>>> {
	window.find_edge(at, source, dir).or_else(|| rows?.find_edge(at, source, dir))
}

/// The inline reference cache of `target` as `window` or the window of the rows holds it.
fn held_ref<'f>(
	window: &'f Frontier,
	rows: Option<&'f Frontier>,
	at: &At,
	target: &RecordId,
) -> Option<Option<&'f CacheValue<RefCacheEntry>>> {
	window.find_ref(at, target).or_else(|| rows?.find_ref(at, target))
}

/// Whether a source's adjacency keys can be read as a plain keys range: a vertex table that has
/// never folded, read for its keys alone.
fn plain_keys((kind, _): Facts) -> bool {
	matches!(
		kind,
		VertexAdjacency::Normal {
			folded: false
		}
	)
}

/// What a fill reads with: where it reads, and how many bytes of keys the window may hold.
#[derive(Clone, Copy)]
pub(super) struct Fill<'a> {
	pub(super) at: &'a At,
	pub(super) ctx: &'a ExecutionContext,
	pub(super) budget: usize,
	/// The window of a batch of rows, for a fill of a hop window: what it holds is read by the
	/// positions from it, so the fill reads none of it again. `None` for a fill of that window.
	pub(super) rows: Option<&'a Frontier>,
}

/// What the fills of a window build and discard for each group of sources, kept with the window
/// between fills so that a warm window allocates none of it. It holds no borrow of a source or of
/// a key: what refers to a source does so by its place in the group.
#[derive(Default)]
struct Scratch {
	graph: GraphGroup,
	references: ReferenceGroup,
	/// The keys each range of one multi-range read visited.
	counts: Vec<u32>,
}

/// Fill `window` for the sources `sources` of the graph hop `read` over the key ranges `ranges`:
/// the inline adjacency caches of those whose table keeps one, and the adjacency keys of those
/// whose cache does not answer and whose adjacency is a plain keys range.
///
/// `budget` bounds the bytes the window's keys take; the ranges past it are unread. A failure to
/// read leaves the window as it was when the failure came, and is the caller's to ignore: every
/// position reads what the window does not hold for itself.
pub(super) async fn fill_graph<S: Source>(
	window: &mut Frontier,
	read: &HopRead,
	ranges: &KeyRanges,
	sources: &[S],
	fill: Fill<'_>,
	tables: &mut TableMemo,
) -> FlowResult<()> {
	let HopRead::Graph {
		directions,
		fixed,
		..
	} = read
	else {
		return Ok(());
	};
	if fill.at.version.is_some() {
		return Ok(());
	}
	// The ranges a position is handed are named by the address of the hop's key ranges, which
	// only the ranges the program holds for the whole run keep to themselves.
	let hop = fixed
		.as_deref()
		.filter(|fixed| std::ptr::eq::<KeyRanges>(&***fixed, ranges))
		.map(|_| hop_id(ranges));
	#[cfg(test)]
	let before = window.ranges.len() + window.edges.len() + window.refs.len();
	// The sources are taken a group at a time, each group's probes and ranges read before the next
	// group's are built, so a fill builds no more ranges than a group has.
	let mut scratch = std::mem::take(&mut window.scratch);
	let mut filled = Ok(());
	for group in sources.chunks(SOURCES_PER_FILL) {
		// A full window reads nothing more, but the ranges it holds are still named for the reads
		// of this hop that they answer, so a later chain over the same ranges is handed them.
		if window.is_full(fill.budget) && (hop.is_none() || window.ranges.is_empty()) {
			break;
		}
		filled = fill_graph_group(
			window,
			(&directions[..], ranges, hop),
			group,
			&fill,
			tables,
			&mut scratch,
		)
		.await;
		if filled.is_err() {
			break;
		}
	}
	window.scratch = scratch;
	filled?;
	#[cfg(test)]
	counters::fill(window.ranges.len() + window.edges.len() + window.refs.len() - before);
	Ok(())
}

/// What [`fill_graph`] builds for one group of sources, reused from one group to the next.
#[derive(Default)]
struct GraphGroup {
	/// The facts of each source of the group, `None` for one that names no record.
	known: Vec<Option<Facts>>,
	/// The inline caches the window does not hold yet: the place of the source in the group and
	/// the direction.
	wanted: Vec<(u32, Dir)>,
	owned: Vec<GraphRange>,
	/// The read each of `owned` is for: the index of its source in the group, the direction and
	/// the range's place among the hop's.
	reads: Vec<(usize, Dir, u16)>,
	queued: HashesSeen,
}

/// One group of [`fill_graph`]: the probes of `group`'s inline caches, then the keys of the ranges
/// no live cache answers.
async fn fill_graph_group<S: Source>(
	window: &mut Frontier,
	(directions, ranges, hop): (&[Dir], &KeyRanges, Option<usize>),
	group: &[S],
	fill: &Fill<'_>,
	tables: &mut TableMemo,
	scratch: &mut Scratch,
) -> FlowResult<()> {
	let Fill {
		at,
		ctx,
		budget,
		rows,
	} = *fill;
	let Scratch {
		graph: GraphGroup {
			known,
			wanted,
			owned,
			reads,
			queued,
		},
		counts,
		..
	} = scratch;
	known.clear();
	wanted.clear();
	owned.clear();
	reads.clear();
	queued.clear();
	fuel::tick(ctx, fuel::units(group.len())).await?;
	for source in group {
		known.push(match source.record() {
			Some(source) => Some(facts(&source.table, ctx, tables).await?),
			None => None,
		});
	}
	// The caches first: a source whose cache is live reads no key.
	if ranges.whole {
		for (index, (source, facts)) in group.iter().zip(known.iter()).enumerate() {
			let (Some(source), Some(facts)) = (source.record(), facts) else {
				continue;
			};
			if facts.1.is_none() {
				continue;
			}
			for &dir in directions {
				if held_edge(window, rows, at, source, dir).is_none() {
					wanted.push((index as u32, dir));
				}
			}
		}
		let mut next = 0;
		while next < wanted.len() && !window.is_full(budget) {
			let take = (room(ctx).await? as usize).min(PROBES_PER_CALL);
			let probes = &wanted[next..(next + take).min(wanted.len())];
			next += probes.len();
			let keys: Vec<_> = probes
				.iter()
				.map(|&(index, dir)| edge_key(at, record_of(&group[index as usize]), dir))
				.collect();
			let found = at
				.txn
				.get_many_key(keys, None)
				.await
				.context("Failed to read the inline adjacency caches")
				.map_err(crate::expr::ControlFlow::Err)?;
			let decoded: usize = found
				.iter()
				.map(|value| match value {
					Some(CacheValue::Live(entries)) => entries.len(),
					_ => 0,
				})
				.sum();
			for (&(index, dir), value) in probes.iter().zip(found) {
				window.push_edge_probe(at, record_of(&group[index as usize]), dir, value);
			}
			fuel::tick(ctx, fuel::units(probes.len() + decoded)).await?;
		}
	}
	// The keys of the ranges no live cache answers.
	for (index, (source, facts)) in group.iter().zip(known.iter()).enumerate() {
		let (Some(source), Some(facts)) = (source.record(), facts) else {
			continue;
		};
		if !plain_keys(*facts) {
			continue;
		}
		for &dir in directions {
			let live =
				matches!(held_edge(window, rows, at, source, dir), Some(Some(CacheValue::Live(_))));
			if live && facts.1.is_some() && ranges.whole {
				continue;
			}
			// The window of the rows reads these ranges for the same read.
			if let (Some(rows), Some(hop)) = (rows, hop)
				&& (0..ranges.specs.len().max(1))
					.all(|place| rows.peek_owned(at, source, dir, (hop, place as u16)).is_some())
			{
				continue;
			}
			for (place, range) in
				graph_ranges(at.ns, at.db, source, dir, &ranges.specs)?.into_iter().enumerate()
			{
				let (start, end) = range.range.bounds();
				match window.slot_of(start, end) {
					// An earlier read holds the range, for another hop of the source: this hop's
					// read is handed it too.
					Some(slot) => {
						if let Some(hop) = hop {
							window.name_held(at, source, dir, slot, (hop, place as u16));
						}
					}
					// A range met twice, or whose hash another's shares, is read once: a source it
					// leaves out reads for itself.
					None => {
						if queued.insert(range_hash(start, end)) {
							owned.push(range);
							reads.push((index, dir, place as u16));
						}
					}
				}
			}
		}
	}
	#[cfg(test)]
	counters::group(owned.len());
	let first = window.ranges.len();
	let recorded =
		read_all(window, owned.len(), &|at| owned[at].range.bounds(), (at, ctx), budget, counts)
			.await?;
	// Each range read is named for the read it answers, so the position that makes the read is
	// handed it without building the range again. A source's ranges are consecutive.
	let mut named: Option<(usize, Dir, (u32, u32))> = None;
	for (slot, &(index, dir, place)) in reads.iter().take(recorded).enumerate() {
		let Some(hop) = hop else {
			break;
		};
		let ident = match named {
			Some((last, last_dir, ident)) if last == index && last_dir == dir => ident,
			_ => {
				let Some(ident) = window.edge_ident(at, record_of(&group[index]), dir) else {
					continue;
				};
				named = Some((index, dir, ident));
				ident
			}
		};
		window.own(first + slot, ident, hop, place);
	}
	Ok(())
}

/// Read the `count` ranges `bounds` names into `window`, a call of [`RANGES_PER_FILL`] ranges and
/// [`CALL_BYTES`] of keys at a time, until every range is read or the window holds `budget` bytes,
/// and answer how many it read: the window holds a slot for each, in order, after the slots it
/// held. The ranges past the last one read are unread. `bounds` answers the start and the end of
/// the range at an index.
async fn read_all<'b>(
	window: &mut Frontier,
	count: usize,
	bounds: &impl Fn(usize) -> (&'b [u8], &'b [u8]),
	(at, ctx): (&At, &ExecutionContext),
	budget: usize,
	counts: &mut Vec<u32>,
) -> FlowResult<usize> {
	let mut next = 0;
	while next < count && !window.is_full(budget) {
		let end = (next + RANGES_PER_FILL).min(count);
		next += read_ranges(window, next..end, bounds, (at, ctx), budget, counts).await?;
	}
	Ok(next)
}

/// Read the ranges `span` of `bounds` into `window` with one call, answering how many of them it
/// recorded: all of them, or the ones up to the range in which the call reached its byte limit or
/// the fuel left, which is cut. The keys the call visited are charged to the fuel once the window
/// holds them.
async fn read_ranges<'b>(
	window: &mut Frontier,
	span: std::ops::Range<usize>,
	bounds: &impl Fn(usize) -> (&'b [u8], &'b [u8]),
	(at, ctx): (&At, &ExecutionContext),
	budget: usize,
	counts: &mut Vec<u32>,
) -> FlowResult<usize> {
	// A range is a unit of the fuel, as a key is: no more ranges than the fuel left are sought by
	// one call, and they are charged before the call seeks them, so that a yield point comes
	// first when they spend the meter and the keys that follow are bounded by what is left.
	let span = span.start..span.start + span.len().min(room(ctx).await? as usize);
	fuel::tick(ctx, fuel::units(span.len())).await?;
	let ranges: Vec<KvRange<'_>> = span
		.clone()
		.map(|place| {
			let (start, end) = bounds(place);
			KvRange {
				start: Key::from(start),
				end: Key::from(end),
			}
		})
		.collect();
	counts.clear();
	counts.resize(ranges.len(), 0);
	let most = room(ctx).await? as usize;
	let mut visited = 0usize;
	let mark = window.mark();
	let first_key = mark.keys as u32;
	let mut stopped_in = None;
	let limit = budget.min(window.held().saturating_add(call_bytes()));
	// The keys are appended to the window's own buffers as they arrive.
	let held_before = window.probe_bytes;
	let Frontier {
		bytes,
		keys,
		..
	} = window;
	let result = at
		.txn
		.visit_key_ranges(&ranges, KEYS_PER_RANGE, at.version, &mut |range: usize, key: &[u8]| {
			let start = bytes.len() as u32;
			bytes.extend_from_slice(key);
			keys.push((start, key.len() as u32));
			counts[range] += 1;
			visited += 1;
			Ok(if visited >= most || bytes.len().saturating_add(held_before) >= limit {
				stopped_in = Some(range);
				std::ops::ControlFlow::Break(())
			} else {
				std::ops::ControlFlow::Continue(())
			})
		})
		.await
		.context("Failed to read the adjacency keys of a window")
		.map_err(crate::expr::ControlFlow::Err);
	if let Err(failure) = result {
		window.rollback(mark);
		return Err(failure);
	}
	let mut first = first_key;
	for (place, &count) in counts.iter().enumerate() {
		if stopped_in.is_some_and(|stopped| place > stopped) {
			break;
		}
		let (start, end) = bounds(span.start + place);
		let cut = count >= KEYS_PER_RANGE || stopped_in == Some(place);
		let start_at = window.bytes.len() as u32;
		window.bytes.extend_from_slice(start);
		let end_at = window.bytes.len() as u32;
		window.bytes.extend_from_slice(end);
		let slot = window.ranges.len() as u32;
		window.ranges.push(RangeSlot {
			start: (start_at, end_at - start_at),
			end: (end_at, window.bytes.len() as u32 - end_at),
			first,
			count,
			cut,
			ident: None,
		});
		window.by_range.entry(range_hash(start, end)).or_insert(slot);
		first += count;
	}
	#[cfg(test)]
	counters::call(visited, ranges.len());
	fuel::tick(ctx, fuel::units(visited).saturating_add(1)).await?;
	Ok(stopped_in.map_or(ranges.len(), |stopped| stopped + 1))
}

/// Whether `subject` reads the whole set of the records referencing a target, which is the one read
/// a window holds.
fn is_whole(subject: &ReferenceSubject) -> bool {
	matches!(subject.start, Bound::Unbounded) && matches!(subject.end, Bound::Unbounded)
}

/// Fill `window` for the referenced records `targets` of the reference subjects `subjects`: the
/// inline reference caches of those whose table keeps one, and the reference keys of those whose
/// cache does not answer. See [`fill_graph`] for the bounds and the failure contract.
///
/// Only a subject that reads the whole set of the records referencing a target is filled for: a
/// bound is the caller's to evaluate, so a subject whose records are narrowed by one is left to its
/// own reads.
pub(super) async fn fill_references<S: Source>(
	window: &mut Frontier,
	subjects: &[ReferenceSubject],
	targets: &[S],
	fill: Fill<'_>,
	tables: &mut TableMemo,
) -> FlowResult<()> {
	if fill.at.version.is_some() {
		return Ok(());
	}
	#[cfg(test)]
	let before = window.ranges.len() + window.edges.len() + window.refs.len();
	// The targets are taken a group at a time, as [`fill_graph`] takes its sources.
	let mut scratch = std::mem::take(&mut window.scratch);
	let mut filled = Ok(());
	for group in targets.chunks(SOURCES_PER_FILL) {
		if window.is_full(fill.budget) {
			break;
		}
		filled = fill_references_group(window, subjects, group, &fill, tables, &mut scratch).await;
		if filled.is_err() {
			break;
		}
	}
	window.scratch = scratch;
	filled?;
	#[cfg(test)]
	counters::fill(window.ranges.len() + window.edges.len() + window.refs.len() - before);
	Ok(())
}

/// What [`fill_references`] builds for one group of targets, reused from one group to the next.
#[derive(Default)]
struct ReferenceGroup {
	/// Whether the table of each target of the group keeps an inline reference cache.
	cached: Vec<bool>,
	/// The places in the group of the targets whose cache the window does not hold yet.
	wanted: Vec<u32>,
	owned: Vec<TypedRange<()>>,
	/// The read each of `owned` is for: the index of its target in the group and the address of
	/// its subject.
	reads: Vec<(usize, usize)>,
	queued: HashesSeen,
	/// Where the encoded inline-cache key of each target of the group lies in the window, once a
	/// range has been named for it.
	idents: Vec<Option<(u32, u32)>>,
}

/// One group of [`fill_references`]: the probes of `group`'s inline reference caches, then the
/// keys of the ranges no live cache answers.
async fn fill_references_group<S: Source>(
	window: &mut Frontier,
	subjects: &[ReferenceSubject],
	group: &[S],
	fill: &Fill<'_>,
	tables: &mut TableMemo,
	scratch: &mut Scratch,
) -> FlowResult<()> {
	let Fill {
		at,
		ctx,
		budget,
		rows,
	} = *fill;
	let Scratch {
		references:
			ReferenceGroup {
				cached,
				wanted,
				owned,
				reads,
				queued,
				idents,
			},
		counts,
		..
	} = scratch;
	cached.clear();
	wanted.clear();
	owned.clear();
	reads.clear();
	queued.clear();
	idents.clear();
	fuel::tick(ctx, fuel::units(group.len())).await?;
	for target in group {
		cached.push(match target.record() {
			Some(target) => {
				let tb = tables
					.current(ctx, &target.table)
					.await
					.context("Failed to resolve the referenced record's table")
					.map_err(crate::expr::ControlFlow::Err)?;
				crate::idx::inline_cache::effective_refs_cap(tb.definition.as_deref()).is_some()
			}
			None => false,
		});
	}
	if subjects.iter().any(is_whole) {
		for (index, (target, cache)) in group.iter().zip(cached.iter()).enumerate() {
			if let Some(target) = target.record()
				&& *cache && held_ref(window, rows, at, target).is_none()
			{
				wanted.push(index as u32);
			}
		}
		let mut next = 0;
		while next < wanted.len() && !window.is_full(budget) {
			let take = (room(ctx).await? as usize).min(PROBES_PER_CALL);
			let probes = &wanted[next..(next + take).min(wanted.len())];
			next += probes.len();
			let keys: Vec<_> = probes
				.iter()
				.map(|&index| ref_key(at, record_of(&group[index as usize])))
				.collect();
			let found = at
				.txn
				.get_many_key(keys, None)
				.await
				.context("Failed to read the inline reference caches")
				.map_err(crate::expr::ControlFlow::Err)?;
			let decoded: usize = found
				.iter()
				.map(|value| match value {
					Some(CacheValue::Live(entries)) => entries.len(),
					_ => 0,
				})
				.sum();
			for (&index, value) in probes.iter().zip(found) {
				window.push_ref_probe(at, record_of(&group[index as usize]), value);
			}
			fuel::tick(ctx, fuel::units(probes.len() + decoded)).await?;
		}
	}
	for subject in subjects.iter().filter(|subject| is_whole(subject)) {
		for (index, (target, cache)) in group.iter().zip(cached.iter()).enumerate() {
			let Some(target) = target.record() else {
				continue;
			};
			if *cache
				&& matches!(held_ref(window, rows, at, target), Some(Some(CacheValue::Live(_))))
			{
				continue;
			}
			// The window of the rows reads this range for the same read.
			if rows.is_some_and(|rows| {
				rows.peek_owned_references(at, target, hop_id(subject)).is_some()
			}) {
				continue;
			}
			let range = crate::exec::operators::scan::ref_key_range(
				at.ns,
				at.db,
				target,
				subject.table.as_ref(),
				subject.field.as_deref(),
				Bound::Unbounded,
				Bound::Unbounded,
			)?;
			let (low, high) = (range.start().as_slice(), range.end().as_slice());
			match window.slot_of(low, high) {
				// An earlier read holds the range, for another subject: this read is handed it
				// too.
				Some(slot) => window.name_held_references(at, target, slot, hop_id(subject)),
				None => {
					if queued.insert(range_hash(low, high)) {
						owned.push(range);
						reads.push((index, hop_id(subject)));
					}
				}
			}
		}
	}
	#[cfg(test)]
	counters::group(owned.len());
	let first = window.ranges.len();
	let recorded = read_all(
		window,
		owned.len(),
		&|at| (owned[at].start().as_slice(), owned[at].end().as_slice()),
		(at, ctx),
		budget,
		counts,
	)
	.await?;
	idents.resize(group.len(), None);
	for (slot, &(index, hop)) in reads.iter().take(recorded).enumerate() {
		let ident = match idents[index] {
			Some(ident) => ident,
			None => {
				let Some(ident) = window.ref_ident(at, record_of(&group[index])) else {
					continue;
				};
				idents[index] = Some(ident);
				ident
			}
		};
		window.own(first + slot, ident, hop, 0);
	}
	Ok(())
}

/// The levels of a path a rows window is filled for, one at a time: the records one level reaches
/// are the sources of the next. Kept in the state between fills so that a level allocates no list
/// of its own.
#[derive(Default)]
pub(in crate::exec::vm) struct Levels {
	/// The records the level last read reaches: the sources of the level after it.
	pub(in crate::exec::vm) current: Vec<RecordId>,
	/// The records [`reached_graph`] found for the level being filled.
	pub(in crate::exec::vm) next: Vec<RecordId>,
	/// The hashes of the records of `next`, which tell the records it holds from one another.
	pub(in crate::exec::vm) seen: HashSet<u64>,
}

impl Levels {
	/// Forget every record held, keeping the buffers.
	pub(in crate::exec::vm) fn clear(&mut self) {
		self.current.clear();
		self.next.clear();
		self.seen.clear();
	}

	/// Make the records the last read reached the sources of the next level.
	pub(in crate::exec::vm) fn advance(&mut self) {
		std::mem::swap(&mut self.current, &mut self.next);
	}
}

/// Collect into `next` the records the graph hop `read` reaches from `sources`, as far as `window`
/// holds what it reads: from a live inline cache, or the keys of a never-folded range, each as the
/// hop's own read reaches it. A source the window holds nothing for reaches nothing here, and a
/// source whose read the window cut reaches what the window holds of it. Answers whether it
/// reached any.
///
/// A hop through an edge to the vertex beyond it reaches the vertices its keys name; any other hop
/// reaches the edges. The records are not judged by the caller's permissions or by the existence of
/// the record, so they are a superset of the ones the lookup's next hop is handed, and each is
/// answered once: a record is told from the ones answered by a 64-bit hash of its id, and a record
/// whose hash another's shares is left out, which only leaves the source it would have been to read
/// for itself.
///
/// Each record is decoded from its key into one id, which the reads of the next level are built
/// from; the position of the hop decodes the same keys again when it reads them. The list `next`
/// and the set `seen` are the caller's and are reused from level to level and batch to batch, so
/// the level allocates for an id only what the id owns: nothing for a numeric, uuid or short
/// string key (a string of up to 23 bytes is stored inline), and the heap of the key otherwise.
pub(super) async fn reached_graph<S: Source>(
	window: &Frontier,
	read: &HopRead,
	ranges: &KeyRanges,
	sources: &[S],
	fill: Fill<'_>,
	tables: &mut TableMemo,
	(next, seen): (&mut Vec<RecordId>, &mut HashSet<u64>),
) -> FlowResult<bool> {
	let Fill {
		at,
		ctx,
		..
	} = fill;
	next.clear();
	seen.clear();
	let HopRead::Graph {
		directions,
		prefixes,
		targets,
		..
	} = read
	else {
		return Ok(false);
	};
	if at.version.is_some() {
		return Ok(false);
	}
	let hop = hop_id(ranges);
	let mut push = |id: RecordId| {
		if seen.insert(ref_hash(&id)) {
			next.push(id);
		}
	};
	let mut reach = |edge: RecordId, far: Option<RecordId>| match (targets.as_deref(), far) {
		(Some(targets), Some(vertex)) => {
			if targets.is_empty() || targets.contains(&vertex.table) {
				push(vertex);
			}
		}
		(Some(_), None) => {}
		(None, _) => push(edge),
	};
	for source in sources {
		let Some(source) = source.record() else {
			continue;
		};
		let facts = facts(&source.table, ctx, tables).await?;
		// The entries and keys decoded for the source, charged once it is done.
		let mut decoded = 1usize;
		for &dir in directions {
			if facts.1.is_some()
				&& ranges.whole
				&& let Some(Some(CacheValue::Live(entries))) = window.find_edge(at, source, dir)
			{
				decoded += entries.len();
				for entry in entries {
					if !prefixes.is_empty()
						&& !prefixes.iter().any(|prefix| entry.edge.starts_with(prefix))
					{
						continue;
					}
					if targets.is_none() {
						if let Ok(edge) = super::decode_cache_edge(entry) {
							reach(edge, None);
						}
					} else if let Ok((edge, vertex)) = super::decode_cache_entry(entry) {
						reach(edge, Some(vertex));
					}
				}
				continue;
			}
			if !plain_keys(facts) {
				continue;
			}
			let mut built: Option<Vec<GraphRange>> = None;
			for place in 0..ranges.specs.len().max(1) {
				let served = match window.peek_owned(at, source, dir, (hop, place as u16)) {
					Some(served) => Some(served),
					None => {
						let built = match &mut built {
							Some(built) => built,
							slot @ None => {
								slot.insert(graph_ranges(at.ns, at.db, source, dir, &ranges.specs)?)
							}
						};
						let (start, end) = built[place].range.bounds();
						window.find_range(start, end)
					}
				};
				let Some(served) = served else {
					continue;
				};
				for key in served.keys() {
					decoded += 1;
					// A hop that reaches edges needs the identity of the edge alone, which one
					// pass over either layout of the key answers.
					if targets.is_none() {
						if let Ok(edge) = super::DecodedGraph::decode_edge(key) {
							reach(edge, None);
						}
					} else if let Ok(graph) = super::DecodedGraph::decode(key) {
						reach(graph.edge, graph.target);
					}
				}
			}
		}
		fuel::tick(ctx, fuel::units(decoded)).await?;
	}
	Ok(!next.is_empty())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_rolled_back_read_leaves_the_window_holding_what_it_held() {
		let mut window = Frontier::default();
		window.bytes.extend_from_slice(b"held");
		window.keys.push((0, 4));
		let held = window.held();
		let mark = window.mark();
		window.bytes.extend_from_slice(b"appended by a read that failed");
		window.keys.push((4, 30));
		assert!(window.held() > held);
		window.rollback(mark);
		assert_eq!(window.held(), held, "the failed read's bytes hold no budget");
		assert_eq!(window.keys, vec![(0, 4)]);
		assert_eq!(window.bytes, b"held");
	}

	#[test]
	fn a_probe_is_told_by_the_bytes_of_its_key_and_not_by_their_hash() {
		let mut window = Frontier::default();
		window.bytes.extend_from_slice(b"first");
		window.edges.push(EdgeProbe {
			ident: (0, 5),
			value: None,
		});
		// A key whose hash is filed under the probe, whose bytes are not the probe's.
		window.by_edge.insert(bytes_hash(b"other"), 0);
		assert!(window.find_edge_key(b"other").is_none());
		window.by_edge.insert(bytes_hash(b"first"), 0);
		assert!(matches!(window.find_edge_key(b"first"), Some(None)));
	}

	#[test]
	fn a_range_is_handed_to_the_read_it_was_named_for_and_to_no_other() {
		let mut window = Frontier::default();
		window.bytes.extend_from_slice(b"ident");
		window.ranges.push(RangeSlot {
			start: (0, 0),
			end: (0, 0),
			first: 0,
			count: 0,
			cut: false,
			ident: None,
		});
		window.own(0, (0, 5), 7, 1);
		assert!(window.find_owned(b"ident", 7, 1).is_some());
		for (ident, hop, place) in
			[(&b"ident"[..], 8, 1), (&b"ident"[..], 7, 0), (&b"other"[..], 7, 1)]
		{
			assert!(window.find_owned(ident, hop, place).is_none(), "{ident:?} {hop} {place}");
		}
		// A read whose hash is filed under the range but whose identity differs is not handed it.
		window.by_owner.insert(owner_hash(b"other", 7, 1), 0);
		assert!(window.find_owned(b"other", 7, 1).is_none());
	}

	#[test]
	fn a_range_two_hops_read_is_handed_to_each_of_them() {
		let mut window = Frontier::default();
		window.bytes.extend_from_slice(b"ident");
		window.ranges.push(RangeSlot {
			start: (0, 0),
			end: (0, 0),
			first: 0,
			count: 0,
			cut: false,
			ident: None,
		});
		window.own(0, (0, 5), 7, 0);
		window.own(0, (0, 5), 9, 0);
		assert!(window.find_owned(b"ident", 7, 0).is_some());
		assert!(window.find_owned(b"ident", 9, 0).is_some());
		assert!(window.find_owned(b"ident", 8, 0).is_none());
		// A read named twice keeps the first name.
		window.own(0, (0, 5), 9, 0);
		assert_eq!(window.owners.len(), 2);
	}

	/// Pins the fan-out of a hop's sources on from its first window of them, for the length of a
	/// test.
	struct Engaged;

	impl Engaged {
		fn pin() -> Engaged {
			counters::ENGAGED.with(|pinned| pinned.set(Some(true)));
			Engaged
		}
	}

	impl Drop for Engaged {
		fn drop(&mut self) {
			counters::ENGAGED.with(|pinned| pinned.set(None));
		}
	}

	#[tokio::test]
	async fn the_sources_a_window_answers_are_read_in_turn_and_the_others_side_by_side() {
		use std::sync::Arc;

		use crate::exec::operators::test_util::TestDb;
		use crate::exec::vm::program::EdgeSpec;
		use crate::val::{RecordIdKey, TableName};

		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION;
			 FOR $i IN 1..=40 { CREATE type::record('person', $i); };
			 FOR $i IN 1..=40 {
				FOR $k IN 0..2 {
					LET $edge = type::record('knows', $i * 10 + $k);
					RELATE (type::record('person', $i))->$edge->(type::record('person', ($i + $k) % 40 + 1));
				};
			 };",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let at = super::super::at_of(&ctx).await.expect("a place to read");
		let table = TableName::from("knows");
		let prefixes = vec![storekey::encode_vec(&table).expect("a table name encodes")];
		let read = Arc::new(HopRead::graph(
			vec![Dir::Out],
			vec![EdgeSpec::whole(table)],
			prefixes,
			None,
			Vec::new(),
		));
		let HopRead::Graph {
			fixed: Some(fixed),
			..
		} = &*read
		else {
			panic!("a hop with constant ranges is fixed");
		};
		let ranges: Arc<KeyRanges> = Arc::clone(fixed);
		let sources: Vec<RecordId> = (1..=40)
			.map(|i| RecordId::new(TableName::from("person"), RecordIdKey::Number(i)))
			.collect();
		let mut tables = TableMemo::default();
		let _engaged = Engaged::pin();

		counters::reset();
		let alone = super::super::adjacencies(
			(&read, &ranges),
			&sources,
			&at,
			&ctx,
			&mut tables,
			usize::MAX,
			Windows::NONE,
		)
		.await
		.expect("the sources read");
		let alone_tasks = counters::spawned();
		assert!(
			alone_tasks >= sources.len(),
			"without a window every source is a task: {alone_tasks}"
		);
		let alone: Vec<_> = alone.into_iter().map(|entry| entry.id).collect();

		for (budget, whole) in [(WINDOW_BYTES, true), (600, false)] {
			let mut window = Frontier::default();
			let fill = Fill {
				at: &at,
				ctx: &ctx,
				budget,
				rows: None,
			};
			fill_graph(&mut window, &read, &ranges, &sources, fill, &mut tables)
				.await
				.expect("the window fills");
			counters::reset();
			let held = super::super::adjacencies(
				(&read, &ranges),
				&sources,
				&at,
				&ctx,
				&mut tables,
				usize::MAX,
				Windows {
					hop: Some(&window),
					origins: None,
					rows: None,
				},
			)
			.await
			.expect("the sources read");
			let held: Vec<_> = held.into_iter().map(|entry| entry.id).collect();
			assert_eq!(held, alone, "the entries of a run with a window, budget {budget}");
			let tasks = counters::spawned();
			if whole {
				assert_eq!(tasks, 0, "a window that answers every source leaves no read to a task");
			} else {
				assert!(
					0 < tasks && tasks < alone_tasks,
					"{tasks} tasks against {alone_tasks} without a window"
				);
			}
		}
	}

	/// A source whose range a fill cut holds the keys the window read and the read of the rest
	/// goes on from them: it is read in turn, and no task reads its range again from the start.
	#[tokio::test]
	async fn a_source_a_window_holds_part_of_is_read_in_turn() {
		use std::sync::Arc;

		use crate::exec::operators::test_util::TestDb;
		use crate::exec::vm::program::EdgeSpec;
		use crate::val::{RecordIdKey, TableName};

		// Three hubs of 600 edges each, more than a fill reads of one range.
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION;
			 FOR $i IN 1..=3 { CREATE type::record('person', $i); };
			 FOR $i IN 1..=3 {
				FOR $k IN 0..600 {
					LET $edge = type::record('knows', $i * 1000 + $k);
					RELATE (type::record('person', $i))->$edge->(type::record('person', 10 + $k));
				};
			 };",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let at = super::super::at_of(&ctx).await.expect("a place to read");
		let table = TableName::from("knows");
		let prefixes = vec![storekey::encode_vec(&table).expect("a table name encodes")];
		let read = Arc::new(HopRead::graph(
			vec![Dir::Out],
			vec![EdgeSpec::whole(table)],
			prefixes,
			None,
			Vec::new(),
		));
		let HopRead::Graph {
			fixed: Some(fixed),
			..
		} = &*read
		else {
			panic!("a hop with constant ranges is fixed");
		};
		let ranges: Arc<KeyRanges> = Arc::clone(fixed);
		let sources: Vec<RecordId> = (1..=3)
			.map(|i| RecordId::new(TableName::from("person"), RecordIdKey::Number(i)))
			.collect();
		let mut tables = TableMemo::default();
		let _engaged = Engaged::pin();
		counters::reset();
		let alone = super::super::adjacencies(
			(&read, &ranges),
			&sources,
			&at,
			&ctx,
			&mut tables,
			usize::MAX,
			Windows::NONE,
		)
		.await
		.expect("the sources read");
		assert_eq!(counters::spawned(), sources.len(), "without a window every source is a task");
		let alone: Vec<_> = alone.into_iter().map(|entry| entry.id).collect();
		assert_eq!(alone.len(), 1800);

		let mut window = Frontier::default();
		let fill = Fill {
			at: &at,
			ctx: &ctx,
			budget: WINDOW_BYTES,
			rows: None,
		};
		fill_graph(&mut window, &read, &ranges, &sources, fill, &mut tables)
			.await
			.expect("the window fills");
		let windows = Windows {
			hop: Some(&window),
			origins: None,
			rows: None,
		};
		let facts =
			super::super::facts(&sources[0].table, &ctx, &mut tables).await.expect("the facts");
		for source in &sources {
			assert!(!windows.serves_adjacency(&at, source, facts, (&[Dir::Out], &ranges)));
			assert!(windows.holds_adjacency(&at, source, facts, (&[Dir::Out], &ranges)));
		}
		counters::reset();
		let held = super::super::adjacencies(
			(&read, &ranges),
			&sources,
			&at,
			&ctx,
			&mut tables,
			usize::MAX,
			windows,
		)
		.await
		.expect("the sources read");
		let held: Vec<_> = held.into_iter().map(|entry| entry.id).collect();
		assert_eq!(held, alone);
		assert_eq!(counters::spawned(), 0, "a source the window holds part of is read in turn");
	}

	/// Replaces the inline adjacency cache of `source` toward its out edges with one entry whose
	/// edge and target are `edge` and `target`, the storekey bytes of the records they name.
	async fn corrupt_edge_cache(
		db: &crate::exec::operators::test_util::TestDb,
		source: RecordId,
		edge: Vec<u8>,
		target: Vec<u8>,
	) {
		use std::borrow::Cow;

		use crate::catalog::providers::DatabaseProvider;

		let write = db
			.datastore()
			.transaction(crate::kvs::TransactionType::Write)
			.await
			.expect("a write transaction opens");
		let def = write
			.get_db_by_name("test", "test", None)
			.await
			.expect("the database reads")
			.expect("the database exists");
		let key = EdgeCacheKey {
			ns: def.namespace_id,
			db: def.database_id,
			tb: Cow::Borrowed(&source.table),
			id: Cow::Borrowed(&source.key),
			dir: Dir::Out,
		};
		let value = CacheValue::Live(vec![EdgeCacheEntry {
			edge,
			target,
		}]);
		write.set_key(&key, &value).await.expect("the cache is written");
		write.commit().await.expect("the corruption commits");
	}

	/// Whichever of two failing sources comes first in source order raises its error, whether the
	/// window serves it and a task reads the other, or a task reads it and the window serves the
	/// other: the sources a window answers are read in turn, the others side by side, and the
	/// failures are taken in source order.
	#[tokio::test]
	async fn the_first_failing_source_in_order_raises_whichever_way_it_is_read() {
		use std::sync::Arc;

		use crate::exec::operators::test_util::TestDb;
		use crate::exec::vm::program::EdgeSpec;
		use crate::expr::ControlFlow;
		use crate::val::{RecordIdKey, TableName};

		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS INLINE EDGES 4; DEFINE TABLE knows TYPE RELATION;
			 FOR $i IN 1..=40 { CREATE type::record('person', $i); };
			 FOR $i IN 1..=40 {
				LET $edge = type::record('knows', $i);
				RELATE (type::record('person', $i))->$edge->(type::record('person', $i % 40 + 1));
			 };",
		)
		.await;
		let person = |i: i64| RecordId::new(TableName::from("person"), RecordIdKey::Number(i));
		let shown = |failed: ControlFlow| match failed {
			ControlFlow::Err(error) => format!("{error:#}"),
			other @ (ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_)) => {
				format!("{other}")
			}
		};
		// The inline cache of `person:3` names an edge of `knows` whose key decodes as none, and
		// the cache of `person:7` an edge that decodes and a target that does not: the two
		// failures are told apart.
		let mut broken = storekey::encode_vec(&TableName::from("knows")).expect("a table encodes");
		broken.push(0xFF);
		let edge = storekey::encode_vec(&(TableName::from("knows"), RecordIdKey::Number(7)))
			.expect("an edge encodes");
		corrupt_edge_cache(&db, person(3), broken, vec![0xFF]).await;
		corrupt_edge_cache(&db, person(7), edge, vec![0xFF]).await;
		let ctx = db.exec_ctx().await;
		let at = super::super::at_of(&ctx).await.expect("a place to read");
		let table = TableName::from("knows");
		let prefixes = vec![storekey::encode_vec(&table).expect("a table name encodes")];
		let read = Arc::new(HopRead::graph(
			vec![Dir::Out],
			vec![EdgeSpec::whole(table)],
			prefixes,
			None,
			Vec::new(),
		));
		let HopRead::Graph {
			fixed: Some(fixed),
			..
		} = &*read
		else {
			panic!("a hop with constant ranges is fixed");
		};
		let ranges: Arc<KeyRanges> = Arc::clone(fixed);
		let sources: Vec<RecordId> = (1..=40).map(person).collect();
		let mut tables = TableMemo::default();
		let _engaged = Engaged::pin();

		// The failure each source raises when it is read alone.
		let mut alone = Vec::new();
		for source in [&sources[2], &sources[6]] {
			let failed = super::super::adjacencies(
				(&read, &ranges),
				std::slice::from_ref(source),
				&at,
				&ctx,
				&mut tables,
				usize::MAX,
				Windows::NONE,
			)
			.await
			.err()
			.expect("a source whose adjacency holds a corrupt key fails");
			alone.push(shown(failed));
		}
		assert_ne!(alone[0], alone[1], "the two failures are told apart");

		// `person:3` is served by the window and `person:7` is read by a task, then the other way
		// round; `person:3` comes first in source order in both.
		for (held, tasks, expect) in [(0..5, 16 - 5, &alone[0]), (4..10, 16 - 6, &alone[0])] {
			let mut window = Frontier::default();
			let fill = Fill {
				at: &at,
				ctx: &ctx,
				budget: WINDOW_BYTES,
				rows: None,
			};
			fill_graph(&mut window, &read, &ranges, &sources[held.clone()], fill, &mut tables)
				.await
				.expect("the window fills");
			counters::reset();
			let failed = super::super::adjacencies(
				(&read, &ranges),
				&sources,
				&at,
				&ctx,
				&mut tables,
				usize::MAX,
				Windows {
					hop: Some(&window),
					origins: None,
					rows: None,
				},
			)
			.await
			.err()
			.expect("two of the sources fail");
			assert_eq!(&shown(failed), expect, "the sources a window holds: {held:?}");
			assert_eq!(
				counters::spawned(),
				tasks,
				"the sources the window does not hold: {held:?}"
			);
		}
	}
}
