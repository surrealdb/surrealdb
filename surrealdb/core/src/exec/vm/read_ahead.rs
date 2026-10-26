//! The entries of a key range, in scan order, read a page ahead of the caller.
//!
//! A scan that runs its body inside its read loop alternates storage work and compute on one task:
//! each chunk is read, and then the body runs over it. [`ChunkSource`] hands such a scan the
//! entries with the contract of [`ScanCursorVals::for_each`], and once the first chunk shows the
//! range holds more, reads the entries on a task of its own while the caller runs the chunk it
//! holds.
//!
//! # Contract
//!
//! [`ChunkSource::for_each`] visits at most `limit` entries, in scan order. An entry handed to the
//! visitor is consumed when the visitor answers `Ok`, and `Break` stops after that entry. A
//! visitor error consumes and counts nothing, records nothing, and leaves the source terminal.
//! The statistics count the entries visited, and a visit that ends before `limit` without
//! `Break` means the range is exhausted. Entries are visited before the error that followed them in
//! storage is raised, which is the order a cursor raises in; an error past an entry the caller
//! never asks for is never raised.
//!
//! # The producer
//!
//! The first entries of the range are always read through the caller's own cursor, a chunk's worth
//! of them ([`START_AFTER`]) whatever `limit` each call asks for, and the producer starts at the
//! next call only when the range holds more: the last call visited `limit` entries, or the visitor
//! stopped it. A range that fits one chunk spawns nothing. The producer reads the rest of
//! the range through its own cursor over the same transaction, in the same direction and at the
//! same version, into pages. It sees what the caller's cursor would, because it reads through the
//! same transaction, and a caller that admits it holds that nothing writes the range while the scan
//! runs.
//!
//! A producer only pays where a core is free to run it: it reads on a core of its own what the
//! caller's cursor would read on the caller's, and copies each entry into a page besides. So a
//! source starts one only while at most half as many sources are open in the process as the node
//! has cores ([`Open`]); past that, every core already runs a scan, a producer would take a core
//! from another scan, and its copies are pure cost. A source that may not start one reads on
//! through its own cursor, and asks again at its next call. A producer asks again before each page
//! it fills, so one started while cores were spare does not outlast them: once they are not, it
//! hands the page it holds to the consumer marked as the last it reads and ends, and the source
//! reads what is left of the range through a cursor of its own again, from just after the last
//! entry the producer read.
//!
//! Two pages exist: the one the consumer visits and the one being filled, so memory is bounded by
//! two pages of the byte budget and one entry each, plus one carried entry: the entry that did not
//! fit the page being filled is held in buffers the producer reuses for the next page. Once those
//! buffers have grown to the widest entry the producer allocates nothing per entry or per page.
//! It sends a filled page on a channel of capacity one and takes the next free page from the
//! consumer, so it runs at most one page ahead. It holds no transaction lock across a send or a
//! receive: a page is filled by one `for_each` call, which returns and releases what it holds
//! before the page is sent.
//!
//! The consumer waits on storage only when it has visited every entry of its page. A range cut
//! at a published edge ([`Cut`]) is not read past it: the producer ends after the first entry at
//! or past the edge (before it, scanning backward), and hands that entry to the consumer as a
//! cursor does, so that the consumer's check of the edge ends the scan at the same entry, with the
//! same entries counted, as it does over a cursor. An entry already read into a page in flight
//! past the edge is ended by the same check.
//!
//! # Ending
//!
//! [`ChunkSource::finish`] closes the producer's channels and waits for it to end: a producer
//! parked on a channel ends at once, and one in a chunk read ends when the read returns, so no read
//! of the scan is in flight once it returns. A source dropped without it aborts the producer. An
//! abort does not stop a chunk the storage engine is already reading on a thread of its own: that
//! chunk finishes, holding its `Arc<Transaction>`, so the transaction outlives it, and a writable
//! chunk can hold the transaction's lock for its own length, so a write or a rollback after
//! the scan waits for it.

#[cfg(not(target_family = "wasm"))]
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use surrealdb_kvs::Result;
use surrealdb_kvs::api::{BoxFut, ScanChunkStats, ScanCursorVals, ValVisitor};
#[cfg(not(target_family = "wasm"))]
use surrealdb_kvs::{Error, Page};

use crate::key::{AnyRange, RawRange};
#[cfg(not(target_family = "wasm"))]
use crate::kvs::NORMAL_BATCH_SIZE;
use crate::kvs::{Direction, Transaction};

/// What a scan reads its entries through: a transaction, or a test's stand-in for one.
pub(super) trait Reader: Send + Sync + 'static {
	/// A cursor over `range` in `dir` at `version`, which records nothing of what it reads.
	fn open<'a>(
		&'a self,
		range: RawRange,
		dir: Direction,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorVals + 'a>>>;

	/// Records the entries the caller visited in one chunk, as a metered cursor records its own.
	fn record(&self, stats: &ScanChunkStats);

	/// The entries a source visits through the caller's own cursor before it may start a
	/// producer ([`START_AFTER`]).
	fn start_after(&self) -> u32 {
		START_AFTER
	}

	/// Whether a core is free for a producer to read ahead on ([`Open::spare_core`]).
	#[cfg(not(target_family = "wasm"))]
	fn spare_core(&self) -> bool {
		Open::spare_core()
	}
}

impl Reader for Transaction {
	fn open<'a>(
		&'a self,
		range: RawRange,
		dir: Direction,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorVals + 'a>>> {
		Box::pin(async move {
			surrealdb_datastore::Transactor::open_vals_cursor(
				self,
				range.into_key_range(),
				dir,
				0,
				version,
			)
			.await
		})
	}

	fn record(&self, stats: &ScanChunkStats) {
		self.record_view_scan(stats.rows, stats.key_bytes, stats.value_bytes);
	}
}

/// The edge a range is cut at, published by the scan's owner and read by its producer: the first
/// key of the part the scan no longer reads (the last key it reads, scanning backward).
#[derive(Default)]
pub(super) struct Cut {
	/// How many edges have been published.
	published: AtomicU64,
	edge: std::sync::Mutex<Vec<u8>>,
}

impl Cut {
	/// Publishes `edge`, which replaces the one before it.
	pub(super) fn publish(&self, edge: &[u8]) {
		let mut held = self.edge.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		held.clear();
		held.extend_from_slice(edge);
		self.published.fetch_add(1, Ordering::Release);
	}
}

/// What a producer last read of a [`Cut`], so that an unchanged edge is not read under the lock
/// again.
#[cfg(not(target_family = "wasm"))]
#[derive(Default)]
struct Seen {
	published: u64,
	edge: Option<Vec<u8>>,
}

#[cfg(not(target_family = "wasm"))]
impl Seen {
	fn refresh(&mut self, cut: &Cut) {
		let published = cut.published.load(Ordering::Acquire);
		if published != self.published {
			let held = cut.edge.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
			let edge = self.edge.get_or_insert_with(Vec::new);
			edge.clear();
			edge.extend_from_slice(&held);
			self.published = published;
		}
	}

	/// Whether `key` lies in the part cut off: at or after the edge scanning forward, before it
	/// scanning backward.
	fn passes(&self, key: &[u8], dir: Direction) -> bool {
		self.edge.as_deref().is_some_and(|edge| match dir {
			Direction::Forward => key >= edge,
			Direction::Backward => key < edge,
		})
	}
}

/// The entries a source visits through the caller's own cursor before it may start a producer: a
/// chunk's worth, or the caller's own chunk where that is smaller
/// ([`ChunkSource::starting_after`]). A range that the caller's first chunks exhaust, however the
/// fuel left or a stop of the visitor cut those chunks short, spawns nothing.
#[cfg(not(target_family = "wasm"))]
const START_AFTER: u32 = NORMAL_BATCH_SIZE;
#[cfg(target_family = "wasm")]
const START_AFTER: u32 = u32::MAX;

/// What a scan of a range reads its entries through: its own cursor, or a producer.
pub(super) struct ChunkSource<'a> {
	reader: &'a Arc<dyn Reader>,
	/// The range, direction and version a producer reads the rest of the range at.
	#[cfg(not(target_family = "wasm"))]
	range: RawRange,
	#[cfg(not(target_family = "wasm"))]
	dir: Direction,
	#[cfg(not(target_family = "wasm"))]
	version: Option<u64>,
	/// The byte budget of a page, when the source may read ahead.
	budget: Option<usize>,
	/// The entries the source visits through the caller's cursor before it may start a producer.
	start_after: u64,
	cut: Arc<Cut>,
	mode: Mode<'a>,
	/// Counts the source among the open ones while it lives.
	#[cfg(not(target_family = "wasm"))]
	_open: Open,
}

/// One open [`ChunkSource`], counted among the open sources of the process while it lives.
#[cfg(not(target_family = "wasm"))]
struct Open;

/// How many [`ChunkSource`]s are open in the process.
#[cfg(not(target_family = "wasm"))]
static OPEN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(not(target_family = "wasm"))]
impl Open {
	fn new() -> Self {
		OPEN.fetch_add(1, Ordering::Relaxed);
		Self
	}

	/// Whether a source may start a producer: whether at most half as many sources are open as
	/// the node has cores, so that one more task reading ahead finds a core no scan is using.
	fn spare_core() -> bool {
		// Tests of the producer run beside each other's sources, which the count would take for
		// load; the rule itself is tested on its own (`spare`).
		if cfg!(test) {
			return true;
		}
		spare(OPEN.load(Ordering::Relaxed), crate::exec::planner::placement::node_cores())
	}
}

/// Whether, with `open` sources open on a node of `cores` cores, a core is free for a producer:
/// at most half as many sources are open as there are cores.
#[cfg(not(target_family = "wasm"))]
fn spare(open: usize, cores: usize) -> bool {
	open.saturating_mul(2) <= cores
}

#[cfg(not(target_family = "wasm"))]
impl Drop for Open {
	fn drop(&mut self) {
		OPEN.fetch_sub(1, Ordering::Relaxed);
	}
}

enum Mode<'a> {
	/// Read through the caller's own cursor. `last` holds the last key visited when `budget` is
	/// set and `more` is (the key of a call that left the range unread), `visited` counts the
	/// entries visited, and `more` says the range holds entries past the last call and `visited`
	/// has reached [`START_AFTER`].
	Direct {
		cursor: Box<dyn ScanCursorVals + 'a>,
		last: Vec<u8>,
		visited: u64,
		more: bool,
	},
	#[cfg(not(target_family = "wasm"))]
	Ahead(Ahead),
}

impl<'a> ChunkSource<'a> {
	/// Opens `range` in `dir` at `version` through `reader`. With a `budget`, the pages of a
	/// producer take that many bytes; without one the source never reads ahead.
	pub(super) async fn open(
		reader: &'a Arc<dyn Reader>,
		range: RawRange,
		dir: Direction,
		version: Option<u64>,
		budget: Option<usize>,
	) -> Result<Self> {
		let cursor = reader.open(range.clone(), dir, version).await?;
		Ok(Self {
			reader,
			#[cfg(not(target_family = "wasm"))]
			range,
			#[cfg(not(target_family = "wasm"))]
			dir,
			#[cfg(not(target_family = "wasm"))]
			version,
			budget: budget.filter(|_| cfg!(not(target_family = "wasm"))),
			start_after: u64::from(reader.start_after()),
			cut: Arc::new(Cut::default()),
			mode: Mode::Direct {
				cursor,
				last: Vec::new(),
				visited: 0,
				more: false,
			},
			#[cfg(not(target_family = "wasm"))]
			_open: Open::new(),
		})
	}

	/// Lets the source start a producer once it has visited `entries`, where that is fewer than
	/// it would wait for: the chunk a caller reads when nothing cuts it short, for a caller whose
	/// chunks are smaller than [`START_AFTER`].
	pub(super) fn starting_after(&mut self, entries: u32) {
		self.start_after = self.start_after.min(u64::from(entries.max(1)));
	}

	/// The edge the producer is told not to read past: publish it where the scan cuts its range.
	pub(super) fn cut(&self, edge: &[u8]) {
		#[cfg(all(test, not(target_family = "wasm")))]
		{
			probe::note_cut();
			if matches!(self.mode, Mode::Ahead(_)) {
				probe::note_cut_ahead();
			}
		}
		self.cut.publish(edge);
	}

	/// Visits up to `limit` entries; see the [module contract](self).
	pub(super) async fn for_each(
		&mut self,
		limit: u32,
		f: &mut dyn ValVisitor,
	) -> Result<ScanChunkStats> {
		#[cfg(not(target_family = "wasm"))]
		if let Mode::Ahead(ahead) = &mut self.mode {
			#[cfg(test)]
			probe::stalls().await;
			let (stats, stopped) = ahead.for_each(self.reader.as_ref(), limit, f).await?;
			if !ahead.handed_off() {
				return Ok(stats);
			}
			// The producer read its last page: what is left of the range is read through a
			// cursor of the source's own again, in this call if it has entries left to visit.
			self.resume_direct().await?;
			let rows = u32::try_from(stats.rows).unwrap_or(limit);
			if stopped || rows >= limit {
				return Ok(stats);
			}
			let rest = self.direct(limit - rows, f).await?;
			return Ok(ScanChunkStats {
				rows: stats.rows + rest.rows,
				key_bytes: stats.key_bytes + rest.key_bytes,
				value_bytes: stats.value_bytes + rest.value_bytes,
			});
		}
		self.direct(limit, f).await
	}

	/// Visits up to `limit` entries through the source's own cursor, after starting a producer
	/// instead when the last call left the range unread and a core is spare.
	async fn direct(&mut self, limit: u32, f: &mut dyn ValVisitor) -> Result<ScanChunkStats> {
		#[cfg(not(target_family = "wasm"))]
		if matches!(
			self.mode,
			Mode::Direct {
				more: true,
				..
			}
		) && self.reader.spare_core()
		{
			self.start();
		}
		let stats = match &mut self.mode {
			Mode::Direct {
				cursor,
				last,
				visited,
				more,
			} => {
				let track = self.budget.is_some();
				let mut stopped = false;
				// A producer starts after the last entry of a call that left the range unread: the
				// `limit`th, or the one the visitor stopped at. Only that entry's key is kept.
				let mut seen = 0u32;
				let stats = cursor
					.for_each(limit, &mut |key, value| {
						let flow = f(key, value)?;
						stopped = flow.is_break();
						seen += 1;
						if track && (stopped || seen == limit) {
							last.clear();
							last.extend_from_slice(key);
						}
						Ok(flow)
					})
					.await?;
				*visited = visited.saturating_add(stats.rows);
				// A call that visits all it was asked for, or that the visitor stops, leaves the
				// rest of the range unread; one that ends short without a stop has exhausted it.
				let unread = stopped || (limit > 0 && stats.rows == u64::from(limit));
				*more = track && unread && *visited >= self.start_after;
				stats
			}
			#[cfg(not(target_family = "wasm"))]
			Mode::Ahead(ahead) => {
				#[cfg(test)]
				probe::stalls().await;
				return Ok(ahead.for_each(self.reader.as_ref(), limit, f).await?.0);
			}
		};
		self.reader.record(&stats);
		Ok(stats)
	}

	/// Ends the producer, if one reads, and waits for it.
	pub(super) async fn finish(self) {
		#[cfg(not(target_family = "wasm"))]
		if let Mode::Ahead(ahead) = self.mode {
			ahead.finish().await;
		}
	}

	/// Replaces the caller's cursor with a producer over what the range holds after the last
	/// entry visited.
	#[cfg(not(target_family = "wasm"))]
	fn start(&mut self) {
		let (
			Mode::Direct {
				last,
				..
			},
			Some(budget),
		) = (&self.mode, self.budget)
		else {
			return;
		};
		let rest = self.rest_after(last);
		let ahead = Ahead::start(
			Arc::clone(self.reader),
			rest,
			self.dir,
			self.version,
			budget,
			Arc::clone(&self.cut),
			last.clone(),
		);
		self.mode = Mode::Ahead(ahead);
	}

	/// What the range holds after the entry of key `last`, in the source's direction.
	#[cfg(not(target_family = "wasm"))]
	fn rest_after(&self, last: &[u8]) -> RawRange {
		let (lo, hi) = self.range.bounds();
		match self.dir {
			Direction::Forward => {
				let mut start = last.to_vec();
				start.push(0);
				self.range.part(&start, hi)
			}
			Direction::Backward => self.range.part(lo, last),
		}
	}

	/// Replaces a producer that handed off with a cursor of the source's own over what the range
	/// holds after the last entry it read. The source may start a producer again once a call
	/// leaves the range unread while a core is spare.
	#[cfg(not(target_family = "wasm"))]
	async fn resume_direct(&mut self) -> Result<()> {
		let Mode::Ahead(ahead) = &self.mode else {
			return Ok(());
		};
		let last = ahead.last.clone();
		let reader: &'a Arc<dyn Reader> = self.reader;
		let cursor = reader.open(self.rest_after(&last), self.dir, self.version).await?;
		let direct = Mode::Direct {
			cursor,
			last,
			visited: self.start_after,
			more: false,
		};
		if let Mode::Ahead(ahead) = std::mem::replace(&mut self.mode, direct) {
			ahead.finish().await;
		}
		Ok(())
	}
}

/// A page the producer filled, and how its fill ended.
#[cfg(not(target_family = "wasm"))]
struct PageFill {
	page: Page,
	/// The failure that followed the entries of the page.
	error: Option<Error>,
	/// The range holds nothing after the entries of the page.
	ended: bool,
	/// The producer reads nothing after the entries of the page, though the range may hold more:
	/// no core was spare for it ([`Reader::spare_core`]).
	handoff: bool,
}

#[cfg(not(target_family = "wasm"))]
pub(super) struct Ahead {
	fills: async_channel::Receiver<PageFill>,
	free: async_channel::Sender<Page>,
	handle: Option<tokio::task::JoinHandle<()>>,
	/// The page the consumer visits, and the position of its next entry.
	current: Option<(Page, usize)>,
	/// The failure that follows the entries of `current`.
	error: Option<Error>,
	/// No fill follows `current`.
	ended: bool,
	/// The producer handed `current` off as the last page it reads.
	handoff: bool,
	/// The key of the last entry of the fills taken, and before the first, the key the producer
	/// starts after: where a cursor resumes once the producer hands off.
	last: Vec<u8>,
}

#[cfg(not(target_family = "wasm"))]
impl Ahead {
	/// Starts a producer over `range`. The producer is spawned here, not as a buffered stream: its
	/// owner awaits its handle in [`finish`](Self::finish) and hands pages back to it, and a stream
	/// wrapper offers neither. It runs under the running-program seal, and an owner that drops
	/// aborts it, as a buffered stream's task is. `last` is the key of the entry `range` starts
	/// after.
	fn start(
		reader: Arc<dyn Reader>,
		range: RawRange,
		dir: Direction,
		version: Option<u64>,
		budget: usize,
		cut: Arc<Cut>,
		last: Vec<u8>,
	) -> Self {
		let (fills_tx, fills) = async_channel::bounded(1);
		let (free, free_rx) = async_channel::bounded(2);
		for _ in 0..2 {
			// The channel holds two pages, so these cannot wait.
			let _ = free.try_send(Page::with_capacity(budget));
		}
		#[cfg(test)]
		probe::note_started();
		let handle = tokio::spawn(crate::exec::compiles::while_running_task(produce(
			reader, range, dir, version, fills_tx, free_rx, cut,
		)));
		Self {
			fills,
			free,
			handle: Some(handle),
			current: None,
			error: None,
			ended: false,
			handoff: false,
			last,
		}
	}

	/// Visits up to `limit` entries of the pages the producer fills, and says whether the visitor
	/// stopped. Visits fewer, without a stop, when the range ends or the producer has handed off
	/// ([`Self::handed_off`]).
	async fn for_each(
		&mut self,
		reader: &dyn Reader,
		limit: u32,
		f: &mut dyn ValVisitor,
	) -> Result<(ScanChunkStats, bool)> {
		let mut stats = ScanChunkStats::default();
		let mut left = limit;
		let mut stopped = false;
		'visit: while left > 0 {
			let Some((page, at)) = &mut self.current else {
				if self.handoff || !self.next().await? {
					break;
				}
				continue;
			};
			let batch = page.batch();
			while *at < batch.len() && left > 0 {
				let Some((key, value)) = batch.get(*at) else {
					break;
				};
				// An entry is consumed and counted only once the visitor accepts it, as a
				// cursor does; a visitor error records nothing.
				let flow = f(key, value)?;
				*at += 1;
				left -= 1;
				stats.rows += 1;
				stats.key_bytes += key.len() as u64;
				stats.value_bytes += value.len() as u64;
				stopped = flow.is_break();
				if stopped {
					break;
				}
			}
			let spent = *at >= batch.len();
			if spent && let Some((page, _)) = self.current.take() {
				// The channel holds both pages, so returning one cannot wait. A page is returned as
				// soon as its last entry is visited, a `Break` on that entry included, so the
				// producer fills it while the caller runs what follows the chunk.
				let _ = self.free.try_send(page);
			}
			if stopped {
				break 'visit;
			}
		}
		reader.record(&stats);
		Ok((stats, stopped))
	}

	/// Whether the producer handed off and its last page has been visited: the rest of the range
	/// is for a cursor to read, after [`Self::last`](Ahead::last).
	fn handed_off(&self) -> bool {
		self.handoff && self.current.is_none()
	}

	/// Takes the next fill as the page the consumer visits. `false` when the range holds
	/// nothing more; the failure that followed the entries before, if any, is raised once.
	async fn next(&mut self) -> Result<bool> {
		if let Some(e) = self.error.take() {
			self.ended = true;
			return Err(e);
		}
		if self.ended {
			return Ok(false);
		}
		let Ok(fill) = self.fills.recv().await else {
			self.ended = true;
			return Err(Error::Internal("a scan's read-ahead task ended without its range".into()));
		};
		self.ended = fill.ended;
		self.error = fill.error;
		self.handoff = fill.handoff;
		let batch = fill.page.batch();
		if let Some((key, _)) = batch.len().checked_sub(1).and_then(|at| batch.get(at)) {
			self.last.clear();
			self.last.extend_from_slice(key);
		}
		self.current = Some((fill.page, 0));
		Ok(true)
	}

	/// Ends the producer and waits for it, a chunk read it is in included: with both channels
	/// closed, its next send or receive fails and it returns.
	async fn finish(mut self) {
		self.fills.close();
		self.free.close();
		if let Some(handle) = self.handle.take() {
			// A producer that panicked has nothing left to read.
			let _ = handle.await;
			#[cfg(test)]
			probe::note_joined();
		}
	}
}

#[cfg(not(target_family = "wasm"))]
impl Drop for Ahead {
	fn drop(&mut self) {
		if let Some(handle) = &self.handle {
			handle.abort();
		}
		#[cfg(test)]
		probe::note_finished();
	}
}

/// Reads `range` into the pages `free` hands out and sends each filled page, until the range, a
/// cut or the consumer ends it.
#[cfg(not(target_family = "wasm"))]
async fn produce(
	reader: Arc<dyn Reader>,
	range: RawRange,
	dir: Direction,
	version: Option<u64>,
	fills: async_channel::Sender<PageFill>,
	free: async_channel::Receiver<Page>,
	cut: Arc<Cut>,
) {
	let mut cursor = match reader.open(range, dir, version).await {
		Ok(cursor) => cursor,
		Err(error) => {
			if let Ok(page) = free.recv().await {
				let _ = fills
					.send(PageFill {
						page,
						error: Some(error),
						ended: true,
						handoff: false,
					})
					.await;
			}
			return;
		}
	};
	let mut seen = Seen::default();
	// The entry that did not fit the page before, which opens the next one. Its buffers are kept
	// across pages and grow only to the widest entry carried.
	let mut carry = (Vec::new(), Vec::new());
	let mut carried = false;
	// The carried entry lies at or past the edge, so the page it opens is the last.
	let mut carried_past = false;
	loop {
		let Ok(mut page) = free.recv().await else {
			return;
		};
		// A free page may outlive the close of its channel; the consumer that closed it reads no
		// more.
		if fills.is_closed() {
			return;
		}
		page.clear();
		seen.refresh(&cut);
		let mut full = false;
		let mut past = std::mem::take(&mut carried_past);
		if carried {
			page.push(&carry.0, &carry.1);
			carried = false;
		}
		if !past && !reader.spare_core() {
			// No core is spare any more, so reading ahead now takes one from another scan: the
			// page goes out as the last this producer reads (the carried entry is in it), and the
			// source reads on through a cursor of its own from after it.
			let _ = fills
				.send(PageFill {
					page,
					error: None,
					ended: false,
					handoff: true,
				})
				.await;
			return;
		}
		let result = if past {
			Ok(ScanChunkStats::default())
		} else {
			cursor
				.for_each(NORMAL_BATCH_SIZE, &mut |key, value| {
					// A consumer that finished waits for this read, which then ends at the entry.
					if fills.is_closed() {
						return Ok(ControlFlow::Break(()));
					}
					seen.refresh(&cut);
					let beyond = seen.passes(key, dir);
					if page.push(key, value) {
						if beyond {
							past = true;
							return Ok(ControlFlow::Break(()));
						}
						if page.bytes() >= page.capacity() {
							full = true;
							return Ok(ControlFlow::Break(()));
						}
						return Ok(ControlFlow::Continue(()));
					}
					// The cursor has consumed the entry, so it opens the next page.
					carry.0.clear();
					carry.0.extend_from_slice(key);
					carry.1.clear();
					carry.1.extend_from_slice(value);
					carried = true;
					carried_past = beyond;
					full = true;
					Ok(ControlFlow::Break(()))
				})
				.await
		};
		let (error, rows) = match result {
			Ok(stats) => (None, stats.rows),
			Err(error) => (Some(error), 0),
		};
		let mut ended = error.is_some() || past || (!full && rows < u64::from(NORMAL_BATCH_SIZE));
		if !ended {
			// The last entry read lies past an edge published after it was read, so the consumer
			// ends at it or before it and nothing after it is read.
			let batch = page.batch();
			if let Some((last, _)) = batch.len().checked_sub(1).and_then(|at| batch.get(at)) {
				seen.refresh(&cut);
				ended = seen.passes(last, dir);
			}
		}
		if fills
			.send(PageFill {
				page,
				error,
				ended,
				handoff: false,
			})
			.await
			.is_err() || ended
		{
			return;
		}
	}
}

/// What tests observe of the sources of this thread's scans: how many producers started and how
/// many ended, finished or dropped. Both are counted by the consumer, so a producer on another
/// thread is counted on the thread of the scan.
#[cfg(all(test, not(target_family = "wasm")))]
pub(super) mod probe {
	use std::cell::Cell;

	thread_local! {
		static STARTED: Cell<usize> = const { Cell::new(0) };
		static FINISHED: Cell<usize> = const { Cell::new(0) };
		static JOINED: Cell<usize> = const { Cell::new(0) };
		static CUTS: Cell<usize> = const { Cell::new(0) };
		static CUTS_AHEAD: Cell<usize> = const { Cell::new(0) };
		static STALL: std::cell::RefCell<Option<(usize, std::sync::Arc<tokio::sync::Notify>)>> =
			const { std::cell::RefCell::new(None) };
	}

	pub(in crate::exec::vm) fn reset() {
		STARTED.with(|n| n.set(0));
		FINISHED.with(|n| n.set(0));
		JOINED.with(|n| n.set(0));
		CUTS.with(|n| n.set(0));
		CUTS_AHEAD.with(|n| n.set(0));
		STALL.with(|stall| *stall.borrow_mut() = None);
	}

	/// Makes the scan of this thread that reads ahead stop for good in its `calls`th call to the
	/// source after it started a producer, notifying `reached` first: the statement stays where a
	/// cancellation finds it.
	pub(in crate::exec::vm) fn stall_after(
		calls: usize,
		reached: std::sync::Arc<tokio::sync::Notify>,
	) {
		STALL.with(|stall| *stall.borrow_mut() = Some((calls, reached)));
	}

	pub(super) async fn stalls() {
		let reached = STALL.with(|stall| {
			let mut stall = stall.borrow_mut();
			match stall.as_mut() {
				Some((calls, reached)) => {
					*calls -= 1;
					(*calls == 0).then(|| std::sync::Arc::clone(reached))
				}
				None => None,
			}
		});
		if let Some(reached) = reached {
			reached.notify_one();
			std::future::pending::<()>().await;
		}
	}

	pub(in crate::exec::vm) fn started() -> usize {
		STARTED.with(Cell::get)
	}

	pub(in crate::exec::vm) fn finished() -> usize {
		FINISHED.with(Cell::get)
	}

	/// How many producers `finish` awaited to their end, which a dropped source does not.
	pub(in crate::exec::vm) fn joined() -> usize {
		JOINED.with(Cell::get)
	}

	/// How many edges were published to sources.
	pub(in crate::exec::vm) fn cuts() -> usize {
		CUTS.with(Cell::get)
	}

	/// How many of those edges were published while a producer read ahead.
	pub(in crate::exec::vm) fn cuts_ahead() -> usize {
		CUTS_AHEAD.with(Cell::get)
	}

	pub(super) fn note_cut_ahead() {
		CUTS_AHEAD.with(|n| n.set(n.get() + 1));
	}

	pub(super) fn note_joined() {
		JOINED.with(|n| n.set(n.get() + 1));
	}

	pub(super) fn note_cut() {
		CUTS.with(|n| n.set(n.get() + 1));
	}

	pub(super) fn note_started() {
		STARTED.with(|n| n.set(n.get() + 1));
	}

	pub(super) fn note_finished() {
		FINISHED.with(|n| n.set(n.get() + 1));
	}
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
	use std::sync::atomic::{AtomicBool, AtomicUsize};

	use tokio::sync::Semaphore;

	use super::*;

	/// A producer starts while at most half as many sources are open as there are cores, and not
	/// past that.
	#[test]
	fn a_producer_starts_only_on_a_spare_core() {
		assert!(spare(1, 64));
		assert!(spare(32, 64));
		assert!(!spare(33, 64));
		assert!(!spare(128, 64));
		assert!(!spare(2, 1));
		assert!(spare(1, 2));
	}

	/// A reader over sorted entries whose cursors count what they read, and fail on a key.
	struct Fake {
		entries: Vec<(Vec<u8>, Vec<u8>)>,
		fail_key: Option<Vec<u8>>,
		/// Entries the cursors have read.
		read: AtomicUsize,
		/// Each chunk a cursor reads takes one permit first, when set.
		gate: Option<Arc<Semaphore>>,
		recorded: AtomicUsize,
		/// The entries a source visits before it may start a producer: one, so that a producer
		/// starts after the first chunk of the small ranges these tests read.
		start_after: u32,
		/// Every core is taken, so no producer may read ahead.
		busy: AtomicBool,
	}

	struct FakeCursor<'a> {
		fake: &'a Fake,
		order: std::vec::IntoIter<usize>,
	}

	impl Reader for Fake {
		fn open<'a>(
			&'a self,
			range: RawRange,
			dir: Direction,
			_: Option<u64>,
		) -> BoxFut<'a, Result<Box<dyn ScanCursorVals + 'a>>> {
			Box::pin(async move {
				let (lo, hi) = range.bounds();
				let mut order: Vec<usize> = (0..self.entries.len())
					.filter(|at| {
						let key = self.entries[*at].0.as_slice();
						key >= lo && key < hi
					})
					.collect();
				if dir == Direction::Backward {
					order.reverse();
				}
				Ok(Box::new(FakeCursor {
					fake: self,
					order: order.into_iter(),
				}) as Box<dyn ScanCursorVals + 'a>)
			})
		}

		fn record(&self, stats: &ScanChunkStats) {
			self.recorded.fetch_add(stats.rows as usize, Ordering::Relaxed);
		}

		fn start_after(&self) -> u32 {
			self.start_after
		}

		fn spare_core(&self) -> bool {
			!self.busy.load(Ordering::Relaxed)
		}
	}

	impl ScanCursorVals for FakeCursor<'_> {
		fn next_batch<'s>(
			&'s mut self,
			_: u32,
		) -> BoxFut<'s, Result<surrealdb_kvs::api::ValsBatch<'s>>> {
			Box::pin(async { Err(Error::Internal("the fake is driven by for_each".into())) })
		}

		fn for_each<'s>(
			&'s mut self,
			limit: u32,
			f: &'s mut dyn ValVisitor,
		) -> BoxFut<'s, Result<ScanChunkStats>> {
			Box::pin(async move {
				if let Some(gate) = &self.fake.gate {
					gate.acquire().await.expect("the gate is open").forget();
				}
				let mut stats = ScanChunkStats::default();
				for _ in 0..limit {
					let Some(at) = self.order.next() else {
						break;
					};
					let (key, value) = &self.fake.entries[at];
					if self.fake.fail_key.as_ref() == Some(key) {
						return Err(Error::Internal("the fake fails here".into()));
					}
					self.fake.read.fetch_add(1, Ordering::Relaxed);
					stats.rows += 1;
					stats.key_bytes += key.len() as u64;
					stats.value_bytes += value.len() as u64;
					if f(key, value)?.is_break() {
						break;
					}
				}
				Ok(stats)
			})
		}
	}

	/// `n` entries with keys `k0000`.. and values of 15 bytes: 20 bytes an entry.
	fn fake(n: usize) -> Fake {
		Fake {
			entries: (0..n)
				.map(|at| (format!("k{at:04}").into_bytes(), format!("value-{at:08}").into_bytes()))
				.collect(),
			fail_key: None,
			read: AtomicUsize::new(0),
			gate: None,
			recorded: AtomicUsize::new(0),
			start_after: 1,
			busy: AtomicBool::new(false),
		}
	}

	fn whole() -> RawRange {
		RawRange::of_bytes(b"k", b"l")
	}

	fn key_of(at: usize) -> Vec<u8> {
		format!("k{at:04}").into_bytes()
	}

	/// What visiting a source in calls of `limits` in turn gives: the keys in the order visited,
	/// and the error that ended the scan. The scan ends on a call that visits fewer entries than
	/// it is asked for without breaking.
	async fn drive(
		source: &mut ChunkSource<'_>,
		limits: &[u32],
		break_after: Option<usize>,
	) -> (Vec<Vec<u8>>, Option<String>) {
		let mut seen = Vec::new();
		let mut calls = 0usize;
		loop {
			let limit = limits[calls % limits.len()];
			calls += 1;
			let mut broke = false;
			let result = source
				.for_each(limit, &mut |key, _| {
					seen.push(key.to_vec());
					if break_after.is_some_and(|every| seen.len() % every == 0) {
						broke = true;
						return Ok(ControlFlow::Break(()));
					}
					Ok(ControlFlow::Continue(()))
				})
				.await;
			match result {
				Err(e) => return (seen, Some(e.to_string())),
				Ok(stats) => {
					if stats.rows < u64::from(limit) && !broke {
						return (seen, None);
					}
				}
			}
		}
	}

	/// A page of ten entries.
	const TEN: Option<usize> = Some(200);

	#[tokio::test]
	async fn the_entries_are_those_of_a_direct_scan_across_page_boundaries() {
		for dir in [Direction::Forward, Direction::Backward] {
			for limit in [1u32, 7, 500, 501] {
				let reader: Arc<dyn Reader> = Arc::new(fake(1234));
				let mut direct = ChunkSource::open(&reader, whole(), dir, None, None)
					.await
					.expect("the source reads");
				let (expected, none) = drive(&mut direct, &[limit], None).await;
				direct.finish().await;
				assert_eq!(none, None);
				assert_eq!(expected.len(), 1234);
				for budget in [TEN, Some(1 << 20), Some(1)] {
					probe::reset();
					let reader: Arc<dyn Reader> = Arc::new(fake(1234));
					let mut ahead = ChunkSource::open(&reader, whole(), dir, None, budget)
						.await
						.expect("the source reads");
					let (seen, none) = drive(&mut ahead, &[limit], None).await;
					ahead.finish().await;
					assert_eq!(none, None);
					assert_eq!(seen, expected, "{dir:?} limit {limit} budget {budget:?}");
					assert_eq!(probe::started(), probe::finished());
					if limit < 1234 {
						assert_eq!(
							probe::started(),
							1,
							"a range of several chunks starts a producer"
						);
					}
				}
			}
		}
	}

	#[tokio::test]
	async fn a_break_consumes_the_entry_it_broke_on() {
		let reader: Arc<dyn Reader> = Arc::new(fake(300));
		let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, TEN)
			.await
			.expect("the source reads");
		let (seen, none) = drive(&mut source, &[50, 3], Some(7)).await;
		source.finish().await;
		assert_eq!(none, None);
		let expected: Vec<Vec<u8>> = (0..300).map(key_of).collect();
		assert_eq!(seen, expected, "no entry is visited twice or skipped");
	}

	#[tokio::test]
	async fn a_visitor_error_records_nothing_in_either_mode() {
		for budget in [None, TEN] {
			let fake = Arc::new(fake(300));
			let reader: Arc<dyn Reader> = Arc::clone(&fake) as _;
			let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, budget)
				.await
				.expect("the source reads");
			// The first chunk fills its limit, so a source that reads ahead starts its producer
			// at the next call.
			source
				.for_each(10, &mut |_, _| Ok(ControlFlow::Continue(())))
				.await
				.expect("the source reads");
			let recorded = fake.recorded.load(Ordering::Relaxed);
			let mut visited = 0usize;
			let failed = source
				.for_each(100, &mut |_, _| {
					visited += 1;
					if visited == 3 {
						return Err(Error::Internal("the visitor fails here".into()));
					}
					Ok(ControlFlow::Continue(()))
				})
				.await;
			source.finish().await;
			assert!(
				failed.is_err_and(|e| e.to_string().contains("the visitor fails here")),
				"{budget:?}"
			);
			assert_eq!(
				fake.recorded.load(Ordering::Relaxed),
				recorded,
				"a call that ends in a visitor error records nothing: {budget:?}"
			);
		}
	}

	#[tokio::test]
	async fn a_failure_follows_the_entries_that_came_before_it() {
		let mut reader = fake(300);
		reader.fail_key = Some(key_of(137));
		let reader: Arc<dyn Reader> = Arc::new(reader);
		let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, TEN)
			.await
			.expect("the source reads");
		let (seen, error) = drive(&mut source, &[20], None).await;
		source.finish().await;
		assert!(error.is_some_and(|e| e.contains("the fake fails here")));
		let expected: Vec<Vec<u8>> = (0..137).map(key_of).collect();
		assert_eq!(seen, expected);
	}

	#[tokio::test]
	async fn a_consumer_that_stops_before_a_failing_fill_never_sees_the_failure() {
		let mut reader = fake(2000);
		reader.fail_key = Some(key_of(1500));
		let reader: Arc<dyn Reader> = Arc::new(reader);
		let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, TEN)
			.await
			.expect("the source reads");
		for _ in 0..4 {
			source
				.for_each(100, &mut |_, _| Ok(ControlFlow::Continue(())))
				.await
				.expect("the source reads");
		}
		source.finish().await;
	}

	#[tokio::test]
	async fn a_range_of_one_chunk_starts_no_producer() {
		probe::reset();
		let reader: Arc<dyn Reader> = Arc::new(fake(100));
		let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, TEN)
			.await
			.expect("the source reads");
		let (seen, none) = drive(&mut source, &[500], None).await;
		source.finish().await;
		assert_eq!((seen.len(), none), (100, None));
		assert_eq!(probe::started(), 0);
	}

	#[tokio::test]
	async fn a_source_without_a_budget_never_reads_ahead() {
		probe::reset();
		let reader: Arc<dyn Reader> = Arc::new(fake(2000));
		let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, None)
			.await
			.expect("the source reads");
		let (seen, none) = drive(&mut source, &[100], None).await;
		source.finish().await;
		assert_eq!((seen.len(), none), (2000, None));
		assert_eq!(probe::started(), 0);
	}

	#[tokio::test]
	async fn the_producer_holds_no_more_than_two_pages_ahead() {
		let fake = Arc::new(fake(2000));
		let reader: Arc<dyn Reader> = Arc::clone(&fake) as Arc<dyn Reader>;
		let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, TEN)
			.await
			.expect("the source reads");
		let mut consumed = 0usize;
		let mut most = 0usize;
		loop {
			let stats = source
				.for_each(7, &mut |_, _| {
					consumed += 1;
					most = most.max(fake.read.load(Ordering::Relaxed) - consumed);
					// Give the producer every chance to run on.
					Ok(ControlFlow::Continue(()))
				})
				.await
				.expect("the source reads");
			tokio::task::yield_now().await;
			if stats.rows < 7 {
				break;
			}
		}
		source.finish().await;
		assert_eq!(consumed, 2000);
		assert!(most <= 21, "at most two pages and the entry that opens a third: {most}");
		assert_eq!(fake.recorded.load(Ordering::Relaxed), 2000, "the visited entries are metered");
	}

	/// The keys a scan of 100 entries visits when the range is cut at the key of entry `edge`
	/// after the first page was read and before the second was, and how many entries the cursors
	/// read.
	async fn cut_scan(dir: Direction, edge: usize) -> (Vec<Vec<u8>>, usize) {
		// One permit for the first chunk, which the caller's cursor reads.
		let gate = Arc::new(Semaphore::new(1));
		let mut fake = fake(100);
		fake.gate = Some(Arc::clone(&gate));
		let fake = Arc::new(fake);
		let reader: Arc<dyn Reader> = Arc::clone(&fake) as Arc<dyn Reader>;
		let mut source =
			ChunkSource::open(&reader, whole(), dir, None, TEN).await.expect("the source reads");
		let cell = Arc::clone(&source.cut);
		let mut seen = Vec::new();
		source
			.for_each(5, &mut |key, _| {
				seen.push(key.to_vec());
				Ok(ControlFlow::Continue(()))
			})
			.await
			.expect("the source reads");
		let consume = async {
			loop {
				let stats = source
					.for_each(1000, &mut |key, _| {
						seen.push(key.to_vec());
						Ok(ControlFlow::Continue(()))
					})
					.await
					.expect("the source reads");
				if stats.rows < 1000 {
					break;
				}
			}
		};
		let release = async {
			// The first page of the producer: entries 5 to 14 in the direction of the scan.
			gate.add_permits(1);
			while fake.read.load(Ordering::Relaxed) < 15 {
				tokio::task::yield_now().await;
			}
			// The producer now waits for a permit to fill its second page.
			cell.publish(&key_of(edge));
			gate.add_permits(100);
		};
		tokio::join!(consume, release);
		let read = fake.read.load(Ordering::Relaxed);
		source.finish().await;
		(seen, read)
	}

	#[tokio::test]
	async fn the_producer_reads_no_entry_into_a_part_cut_off_ahead_of_it() {
		let (seen, read) = cut_scan(Direction::Forward, 25).await;
		assert_eq!(seen, (0..=25).map(key_of).collect::<Vec<_>>());
		assert_eq!(
			read, 26,
			"the entry at the edge is the last one read, and the last one visited"
		);
	}

	#[tokio::test]
	async fn a_backward_producer_reads_no_entry_before_the_edge() {
		let (seen, read) = cut_scan(Direction::Backward, 75).await;
		assert_eq!(seen, (74..100).rev().map(key_of).collect::<Vec<_>>());
		assert_eq!(
			read, 26,
			"the entry before the edge is the last one read, and the last one visited"
		);
	}

	/// The keys a scan over a transaction's whole keyspace visits when the range is cut at the
	/// `at`th key in the direction of the scan, after `calls` calls of seven entries: the first
	/// call reads through the caller's cursor and the others through the producer. The consumer
	/// ends at the first key in the cut-off part, as the coordinator's front run does.
	async fn cut_over_a_cursor(
		reader: &Arc<dyn Reader>,
		dir: Direction,
		budget: Option<usize>,
		all: &[Vec<u8>],
		calls: usize,
		at: usize,
	) -> Vec<Vec<u8>> {
		let mut source =
			ChunkSource::open(reader, RawRange::of_bytes(&[0], &[0xff; 4]), dir, None, budget)
				.await
				.expect("the source reads");
		let edge = all[at].clone();
		let past = |key: &[u8]| match dir {
			Direction::Forward => key >= edge.as_slice(),
			Direction::Backward => key < edge.as_slice(),
		};
		let mut seen: Vec<Vec<u8>> = Vec::new();
		let mut cut = false;
		let mut call = 0usize;
		loop {
			if call == calls && !cut {
				source.cut(&edge);
				cut = true;
			}
			call += 1;
			let mut ended = false;
			let stats = source
				.for_each(7, &mut |key, _| {
					seen.push(key.to_vec());
					if cut && past(key) {
						ended = true;
						return Ok(ControlFlow::Break(()));
					}
					Ok(ControlFlow::Continue(()))
				})
				.await
				.expect("the source reads");
			if ended || stats.rows < 7 {
				break;
			}
		}
		source.finish().await;
		seen
	}

	/// A cut over real cursors, whenever it is published, ends the scan at the first entry in the
	/// cut-off part and visits every entry before it, the entries a cursor of the caller visits.
	#[tokio::test]
	async fn a_cut_published_while_the_producer_reads_ends_the_scan_at_the_first_entry_past_it() {
		use crate::exec::operators::test_util::TestDb;
		let db =
			TestDb::new("FOR $i IN 0..600 { CREATE type::record('t', $i) SET n = $i; };").await;
		let txn = Arc::new(
			db.datastore()
				.transaction(crate::kvs::TransactionType::Read)
				.await
				.expect("a transaction"),
		);
		let reader: Arc<dyn Reader> = Arc::clone(&txn) as Arc<dyn Reader>;
		for dir in [Direction::Forward, Direction::Backward] {
			let mut direct =
				ChunkSource::open(&reader, RawRange::of_bytes(&[0], &[0xff; 4]), dir, None, None)
					.await
					.expect("the source reads");
			let (all, none) = drive(&mut direct, &[100], None).await;
			direct.finish().await;
			assert_eq!(none, None);
			assert!(all.len() > 600, "the table and the catalog it lives in: {}", all.len());
			for budget in [Some(1 << 10), Some(1 << 20)] {
				for calls in [0usize, 1, 2, 5, 20] {
					for at in [3, all.len() / 3, all.len() - 2] {
						let seen = cut_over_a_cursor(&reader, dir, budget, &all, calls, at).await;
						// The first entry in the cut-off part is the `at`th, scanning forward and
						// the one after it scanning backward. Entries are visited without the
						// check until the cut is published, after seven for each call before it.
						let first_past = if dir == Direction::Forward {
							at
						} else {
							at + 1
						};
						let end = (first_past.max(7 * calls) + 1).min(all.len());
						assert!(
							seen == all[..end],
							"{dir:?} budget {budget:?} calls {calls} at {at}: {} entries, {end} expected",
							seen.len()
						);
					}
				}
			}
		}
		txn.cancel().await.expect("the transaction ends");
	}

	#[tokio::test]
	async fn finishing_a_source_whose_producer_waits_joins_it() {
		probe::reset();
		let reader: Arc<dyn Reader> = Arc::new(fake(5000));
		let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, TEN)
			.await
			.expect("the source reads");
		source
			.for_each(500, &mut |_, _| Ok(ControlFlow::Continue(())))
			.await
			.expect("the source reads");
		source
			.for_each(3, &mut |_, _| Ok(ControlFlow::Continue(())))
			.await
			.expect("the source reads");
		assert_eq!(probe::started(), 1);
		source.finish().await;
		assert_eq!(probe::finished(), 1);
	}

	/// Whether the source holds, as the page it visits, one whose every entry it has visited.
	fn holds_a_spent_page(source: &ChunkSource<'_>) -> bool {
		match &source.mode {
			Mode::Ahead(ahead) => {
				ahead.current.as_ref().is_some_and(|(page, at)| *at >= page.batch().len())
			}
			Mode::Direct {
				..
			} => false,
		}
	}

	#[tokio::test]
	async fn a_page_whose_last_entry_stops_the_visit_is_handed_back() {
		probe::reset();
		let reader: Arc<dyn Reader> = Arc::new(fake(300));
		let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, TEN)
			.await
			.expect("the source reads");
		let mut seen = 0usize;
		loop {
			// Every entry stops its call, so the last entry of every page does.
			let stats = source
				.for_each(500, &mut |_, _| {
					seen += 1;
					Ok(ControlFlow::Break(()))
				})
				.await
				.expect("the source reads");
			assert!(!holds_a_spent_page(&source), "after {seen} entries");
			if stats.rows == 0 {
				break;
			}
		}
		source.finish().await;
		assert_eq!(seen, 300);
		assert_eq!(probe::started(), 1);
	}

	#[tokio::test]
	async fn a_range_the_first_short_calls_exhaust_starts_no_producer() {
		probe::reset();
		let mut fake = fake(5);
		fake.start_after = NORMAL_BATCH_SIZE;
		let reader: Arc<dyn Reader> = Arc::new(fake);
		let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, TEN)
			.await
			.expect("the source reads");
		// Calls cut short of a chunk, as the fuel left cuts them, each visiting all it asks for.
		let (seen, none) = drive(&mut source, &[4], None).await;
		source.finish().await;
		assert_eq!((seen.len(), none), (5, None));
		assert_eq!(probe::started(), 0);
	}

	#[tokio::test]
	async fn a_range_of_more_than_a_chunk_starts_a_producer_after_a_chunk_of_short_calls() {
		probe::reset();
		let mut fake = fake(2000);
		fake.start_after = NORMAL_BATCH_SIZE;
		let reader: Arc<dyn Reader> = Arc::new(fake);
		let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, TEN)
			.await
			.expect("the source reads");
		let (seen, none) = drive(&mut source, &[100], None).await;
		source.finish().await;
		assert_eq!((seen.len(), none), (2000, None));
		assert_eq!(probe::started(), 1);
	}

	/// A producer that finds no core spare any more hands the rest of the range to a cursor of the
	/// source's own: every entry is visited once and in order, and none is read twice. Once cores
	/// are spare again, the source starts a producer again.
	#[tokio::test]
	async fn a_producer_hands_off_once_no_core_is_spare() {
		for dir in [Direction::Forward, Direction::Backward] {
			for limit in [1u32, 7, 33] {
				probe::reset();
				let fake = Arc::new(fake(500));
				let reader: Arc<dyn Reader> = Arc::clone(&fake) as _;
				let mut source = ChunkSource::open(&reader, whole(), dir, None, TEN)
					.await
					.expect("the source reads");
				let mut seen = Vec::new();
				loop {
					// Busy from the 100th entry visited to the 300th.
					fake.busy.store((100..300).contains(&seen.len()), Ordering::Relaxed);
					let stats = source
						.for_each(limit, &mut |key, _| {
							seen.push(key.to_vec());
							Ok(ControlFlow::Continue(()))
						})
						.await
						.expect("the source reads");
					if stats.rows < u64::from(limit) {
						break;
					}
				}
				source.finish().await;
				let mut expected: Vec<Vec<u8>> = (0..500).map(key_of).collect();
				if dir == Direction::Backward {
					expected.reverse();
				}
				assert_eq!(seen, expected, "{dir:?} limit {limit}");
				assert_eq!(fake.read.load(Ordering::Relaxed), 500, "no entry is read twice");
				assert_eq!(probe::started(), 2, "{dir:?} limit {limit}: handed off, then again");
				assert_eq!(probe::started(), probe::finished());
			}
		}
	}

	#[tokio::test]
	async fn finishing_waits_for_a_chunk_the_producer_is_reading() {
		probe::reset();
		// One permit for the caller's first chunk; the producer's first chunk waits for another.
		let gate = Arc::new(Semaphore::new(1));
		let mut fake = fake(100);
		fake.gate = Some(Arc::clone(&gate));
		let fake = Arc::new(fake);
		let reader: Arc<dyn Reader> = Arc::clone(&fake) as Arc<dyn Reader>;
		let mut source = ChunkSource::open(&reader, whole(), Direction::Forward, None, TEN)
			.await
			.expect("the source reads");
		source
			.for_each(5, &mut |_, _| Ok(ControlFlow::Continue(())))
			.await
			.expect("the source reads");
		// The next call starts the producer, whose chunk read waits on the gate; the call itself
		// visits nothing. The producer runs on this thread until it waits there.
		source.for_each(0, &mut |_, _| Ok(ControlFlow::Continue(()))).await.expect("the call");
		assert_eq!(probe::started(), 1);
		for _ in 0..20 {
			tokio::task::yield_now().await;
		}
		let finished = std::sync::atomic::AtomicBool::new(false);
		let finish = async {
			source.finish().await;
			finished.store(true, Ordering::Release);
		};
		let release = async {
			for _ in 0..20 {
				tokio::task::yield_now().await;
			}
			assert!(!finished.load(Ordering::Acquire), "finish returned with a chunk in flight");
			gate.add_permits(1);
		};
		tokio::join!(finish, release);
		assert!(finished.load(Ordering::Acquire));
		assert_eq!(probe::joined(), 1);
	}
}
