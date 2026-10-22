//! Windows of record reads for the links a `FETCH` and a dereference follow.
//!
//! A site that consumes a collection of record ids in order (a `FETCH` path's array, `.*`,
//! `friends.name`) reads the records of a run of its ids at once, through one multi-get of the
//! transaction, and then consumes the ids one at a time: the window changes only where a position's
//! bytes come from. A window holds hits only. It decodes nothing, resolves no lightweight edge and
//! never raises a failed read, so a position the window does not serve (a missing record, a
//! lightweight edge, a cache entry without data, a failed read) reads for itself where the site
//! consumes it, and raises there what its own read raises. The site that decodes a position's
//! stored bytes holds the record in the transaction's cache, as a read of the record for itself
//! does ([`fetch_raw_deref_slot`](super::fetch::fetch_raw_deref_slot)).
//!
//! A window is sized by bytes as a scan batch is: the first is a small probe, and each later one
//! is sized from the stored bytes of the one before it, within `scan_batch_bytes` and
//! `NORMAL_BATCH_SIZE` keys. An id that repeats within a window is read once.
//!
//! The buffers live in a [`LinkPool`] a pipeline's state owns, so a window is not built per call.
//! A site takes a window out of the pool for the length of its loop, which lets a position's own
//! evaluation (a computed field, a permission) reach another site that takes a window of its own.

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, PoisonError};

use surrealdb_kvs::GetMultiResult;
use surrealdb_kvs::consts::NORMAL_BATCH_SIZE;
use surrealdb_strand::Strand;

use super::scan::common::{
	RECORD_PROBE_ROWS, ensure_below_memory_threshold, next_batch_len, probe_batch_len,
};
use crate::catalog::{DatabaseId, Generation, NamespaceId, Record};
use crate::exec::ExecutionContext;
use crate::key::KVKey;
use crate::key::schema::RecordKey;
use crate::kvs::tx::{RecordKeys, StoredSlot};
use crate::val::{RecordId, Value};

/// What a window holds for one position: the record the transaction's cache held with the
/// generation its stored bytes carry, the stored bytes the multi-get found, or nothing, in which
/// case the position reads for itself.
pub(crate) enum Slot<'w> {
	Cached(Arc<Record>, Option<Generation>),
	Stored(&'w [u8]),
	Unread,
}

/// The end of a chain of ids that share a hash.
const NO_ID: u32 = u32::MAX;

/// The most positions one window holds.
const MAX_WINDOW: usize = NORMAL_BATCH_SIZE as usize;

/// One window of reads, and the buffers that fill it.
#[derive(Default)]
pub(crate) struct LinkWindow {
	/// The distinct ids of the fill in progress. Empty between fills; held only so the vector
	/// keeps its allocation.
	ids: Vec<&'static RecordId>,
	keys: RecordKeys,
	/// What the multi-get found for each distinct id.
	slots: Vec<StoredSlot>,
	found: GetMultiResult,
	/// For each position of the window, the distinct id it names.
	of: Vec<u32>,
	/// For each position of the window, its position in the collection it was taken from.
	positions: Vec<u32>,
	/// The head of the chain of distinct ids that share a hash, by hash.
	heads: HashMap<u64, u32>,
	/// For each distinct id, the next one that shares its hash.
	chain: Vec<u32>,
	/// How many positions of the window have been consumed.
	taken: usize,
	/// The most positions the next window takes, sized from the last.
	len: usize,
	/// The byte budget `len` was sized for.
	budget: usize,
}

impl LinkWindow {
	/// Drops what the window holds and keeps its allocations.
	fn clear(&mut self) {
		self.slots.clear();
		self.found.values.clear();
		self.of.clear();
		self.positions.clear();
		self.heads.clear();
		self.chain.clear();
		self.taken = 0;
	}

	/// Forgets the size the next window was sized to, so that it is read as a probe: a size is
	/// learned from the records one run read and says nothing of the records of another.
	fn forget_size(&mut self) {
		self.len = 0;
	}

	/// The most distinct ids the next window takes, sized from the last; the budget the size was
	/// derived for is `ctx`'s.
	fn size(&mut self, ctx: &ExecutionContext) -> usize {
		let budget = ctx.root().ctx.config.exec.scan_batch_bytes;
		if self.budget != budget || self.len == 0 {
			self.budget = budget;
			self.len = probe_batch_len(budget, RECORD_PROBE_ROWS.min(MAX_WINDOW));
		}
		self.len
	}

	/// Reads the records of the next run of the record ids of `values[from..]` that `include`
	/// admits, in order, and holds them as the window: the positions of the ids it took are
	/// consumed in order through [`Self::slot`].
	///
	/// A failed read is not raised: every position of the window then reads for itself.
	async fn fill(
		&mut self,
		ctx: &ExecutionContext,
		values: &[Value],
		from: usize,
		include: &(dyn Fn(&RecordId) -> bool + Sync),
	) -> crate::expr::FlowResult<()> {
		self.clear();
		let cap = self.size(ctx);
		let mut ids: Vec<&RecordId> = std::mem::take(&mut self.ids);
		for (at, value) in values.iter().enumerate().skip(from) {
			if self.positions.len() >= cap {
				break;
			}
			let Value::RecordId(rid) = value else {
				continue;
			};
			if !include(rid) {
				continue;
			}
			let hash = {
				let mut hasher = std::hash::DefaultHasher::new();
				rid.hash(&mut hasher);
				hasher.finish()
			};
			let mut cursor = self.heads.get(&hash).copied().unwrap_or(NO_ID);
			let mut distinct = None;
			while cursor != NO_ID {
				if *ids[cursor as usize] == *rid {
					distinct = Some(cursor);
					break;
				}
				cursor = self.chain[cursor as usize];
			}
			let distinct = match distinct {
				Some(distinct) => distinct,
				None => {
					let distinct = ids.len() as u32;
					let next = self.heads.insert(hash, distinct).unwrap_or(NO_ID);
					self.chain.push(next);
					ids.push(rid);
					distinct
				}
			};
			self.positions.push(at as u32);
			self.of.push(distinct);
		}
		let read = self.read(ctx, &ids).await;
		ids.clear();
		self.ids = ids.into_iter().map(|_| -> &'static RecordId { unreachable!() }).collect();
		#[cfg(test)]
		tests::note_fill(self.positions.len());
		read
	}

	/// Reads the records `ids` name, one slot each, and sizes the next window from what was found.
	/// A failed read is not raised: every id is left to read for itself.
	async fn read(
		&mut self,
		ctx: &ExecutionContext,
		ids: &[&RecordId],
	) -> crate::expr::FlowResult<()> {
		let read = match ctx.database() {
			Ok(db_ctx) => {
				db_ctx
					.txn()
					.getm_records_stored(
						db_ctx.ns_ctx.ns.namespace_id,
						db_ctx.db.database_id,
						ids,
						&mut self.keys,
						&mut self.slots,
						&mut self.found,
					)
					.await
			}
			Err(e) => Err(anyhow::Error::from(e)),
		};
		match read {
			Ok(()) => {
				self.len = next_batch_len(
					usize::try_from(self.found.value_bytes).unwrap_or(usize::MAX),
					usize::try_from(self.found.records).unwrap_or(usize::MAX),
					self.len,
					self.budget,
					MAX_WINDOW,
				);
			}
			Err(_) => {
				self.slots.clear();
				self.slots.extend((0..ids.len()).map(|_| StoredSlot::Absent));
				self.found.values.clear();
			}
		}
		ensure_below_memory_threshold()
	}

	/// Whether [`Self::distinct_slot`] answers one for the distinct id `distinct`.
	fn serves(&self, distinct: usize) -> bool {
		match self.slots.get(distinct) {
			Some(StoredSlot::Cached(..)) => true,
			Some(StoredSlot::Stored(found)) => {
				matches!(self.found.values.get(*found), Some(Some(_)))
			}
			Some(StoredSlot::Absent) | None => false,
		}
	}

	/// What the window holds for the distinct id `distinct`: its cached record or its stored
	/// bytes, and `None` where the read found nothing to serve.
	fn distinct_slot(&self, distinct: usize) -> Option<Slot<'_>> {
		match self.slots.get(distinct)? {
			StoredSlot::Cached(record, generation) => {
				Some(Slot::Cached(Arc::clone(record), *generation))
			}
			StoredSlot::Stored(found) => match self.found.values.get(*found) {
				Some(Some(bytes)) => Some(Slot::Stored(bytes)),
				_ => None,
			},
			StoredSlot::Absent => None,
		}
	}

	/// The slot of the position of the collection `at`, the next the window holds. A position the
	/// window does not hold next reads for itself.
	fn slot(&mut self, at: usize) -> Slot<'_> {
		let Some(&position) = self.positions.get(self.taken) else {
			return Slot::Unread;
		};
		if position as usize != at {
			return Slot::Unread;
		}
		let distinct = self.of[self.taken] as usize;
		self.taken += 1;
		self.distinct_slot(distinct).unwrap_or(Slot::Unread)
	}

	/// Whether every position of the window has been consumed.
	fn spent(&self) -> bool {
		self.taken >= self.positions.len()
	}
}

/// The windows a pipeline's state keeps for its sites to take.
///
/// A window is held boxed so that a site, whose own frame and future stand on the stack of every
/// level of a computed field's cycle of links, holds a pointer to it and not the window.
#[derive(Default)]
#[expect(clippy::vec_box, reason = "a site holds the window by pointer, not by value")]
pub(crate) struct LinkPool(Mutex<Vec<Box<LinkWindow>>>);

impl LinkPool {
	fn take(&self) -> Box<LinkWindow> {
		self.0.lock().unwrap_or_else(PoisonError::into_inner).pop().unwrap_or_default()
	}

	fn put(&self, mut window: Box<LinkWindow>) {
		window.clear();
		self.0.lock().unwrap_or_else(PoisonError::into_inner).push(window);
	}

	/// Drops what the pooled windows hold and the size they were last sized to, keeping their
	/// allocations.
	pub(crate) fn clear(&self) {
		for window in self.0.lock().unwrap_or_else(PoisonError::into_inner).iter_mut() {
			window.clear();
			window.forget_size();
		}
	}
}

/// How many record ids of `values` `include` admits.
fn admitted(values: &[Value], include: &impl Fn(&RecordId) -> bool) -> usize {
	values
		.iter()
		.filter(|value| matches!(value, Value::RecordId(rid) if include(rid)))
		.take(2)
		.count()
}

/// The windows one site reads a collection through, taken from a pool for the length of its loop.
pub(crate) struct Windows<'p> {
	pool: &'p LinkPool,
	window: Option<Box<LinkWindow>>,
}

impl<'p> Windows<'p> {
	/// The windows for the collection `values`, or `None` where `pool` is not given or fewer than
	/// two of its ids are admitted by `include`: below two there is no read to save.
	pub(crate) fn of(
		pool: Option<&'p LinkPool>,
		values: &[Value],
		include: &impl Fn(&RecordId) -> bool,
	) -> Option<Self> {
		let pool = pool?;
		(admitted(values, include) >= 2).then_some(Self {
			pool,
			window: None,
		})
	}

	/// Reads the window the record id `values[at]`, which `include` admits, belongs to when the
	/// one before it is spent. Positions are asked for in order.
	///
	/// The read runs out of line: a site's future holds this call's, so the stack frame of a site
	/// that reaches itself again through a computed field does not hold the future of a read.
	pub(crate) async fn prepare(
		&mut self,
		ctx: &ExecutionContext,
		values: &[Value],
		at: usize,
		include: &(dyn Fn(&RecordId) -> bool + Sync),
	) -> crate::expr::FlowResult<()> {
		let window = self.window.get_or_insert_with(|| self.pool.take());
		if window.spent() {
			Box::pin(window.fill(ctx, values, at, include)).await?;
		}
		Ok(())
	}

	/// The slot of the position `at`, after [`Self::prepare`] for it.
	pub(crate) fn slot(&mut self, at: usize) -> Slot<'_> {
		self.window.as_mut().map_or(Slot::Unread, |window| window.slot(at))
	}
}

impl Drop for Windows<'_> {
	fn drop(&mut self) {
		if let Some(window) = self.window.take() {
			self.pool.put(window);
		}
	}
}

/// The records of the links a batch of source rows hold, read before the rows run.
///
/// A `FETCH` of top-level link fields, or a dereference `link.field` in a projection, reads one
/// link per row, so a collection window never sees two ids. The window reads the links of a run of
/// the batch's rows at once instead: each is keyed by the encoded key of its record, and a position
/// of the `FETCH` or the dereference that names the key is served the bytes the window holds,
/// whichever row it stands in. A window holds the distinct keys of a run
/// of rows, sized as a collection window is; the caller runs the rows a fill covers and fills the
/// window again for the rows after them, so a batch is read a window at a time.
///
/// The window's bytes are not charged to a query's memory reservation. They are bounded as a scan
/// batch's are, by the `scan_batch_bytes` a fill is sized to (plus the one record a fill always
/// takes) and by `NORMAL_BATCH_SIZE` keys, and a batch's state holds one window at a time, replaced
/// by the next fill and released with the state.
///
/// A key is held as its encoded bytes, not as the [`RecordId`] that names it: the rows the ids
/// stand in are moved into the run after the fill, so the window borrows none of them, and an
/// owned id would be a clone that allocates for each key.
#[derive(Default)]
pub(crate) struct RowsWindow {
	window: LinkWindow,
	/// The namespace and database the window's keys are in.
	scope: Option<(NamespaceId, DatabaseId)>,
	/// The encoded key of each distinct id, back to back.
	encoded: Vec<u8>,
	/// The span of each distinct id's key in [`Self::encoded`].
	spans: Vec<(u32, u32)>,
	/// The head of the chain of distinct ids that share a key hash, by hash.
	heads: HashMap<u64, u32>,
	/// For each distinct id, the next one that shares its key hash.
	chain: Vec<u32>,
	/// The key of the id being asked for, encoded; held for one lookup.
	probe: Mutex<Vec<u8>>,
}

fn key_hash(key: &[u8]) -> u64 {
	let mut hasher = std::hash::DefaultHasher::new();
	key.hash(&mut hasher);
	hasher.finish()
}

fn encode_key(buffer: &mut Vec<u8>, scope: (NamespaceId, DatabaseId), rid: &RecordId) -> bool {
	RecordKey {
		ns: scope.0,
		db: scope.1,
		tb: Cow::Borrowed(&rid.table),
		id: Cow::Borrowed(&rid.key),
	}
	.encode_buffer(buffer)
	.is_ok()
}

impl RowsWindow {
	/// Drops what the window holds and the size its next fill was sized to, keeping its buffers.
	/// The fills of one run size each from the one before; a run starts from a probe.
	pub(crate) fn clear(&mut self) {
		self.window.clear();
		self.window.forget_size();
		self.scope = None;
		self.encoded.clear();
		self.spans.clear();
		self.heads.clear();
		self.chain.clear();
	}

	/// Reads the records the `fields` of a run of the leading rows of `rows` hold links to: a field
	/// that is a record id, or, where `arrays` is set, an array whose record-id elements are keys.
	/// Anything else gives no key. Answers how many rows the run covers, at least one where `rows`
	/// holds any: the window holds every key of those rows, except for a last row whose keys alone
	/// pass the size of a window, whose keys past it read for themselves. A failed read leaves
	/// every key unread; the only error raised is the refusal of a query past the memory threshold.
	pub(crate) async fn fill(
		&mut self,
		ctx: &ExecutionContext,
		rows: &[Value],
		fields: &[Strand],
		arrays: bool,
	) -> crate::expr::FlowResult<usize> {
		self.encoded.clear();
		self.spans.clear();
		self.heads.clear();
		self.chain.clear();
		let Ok(db_ctx) = ctx.database() else {
			self.window.clear();
			self.scope = None;
			return Ok(rows.len());
		};
		let scope = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
		self.scope = Some(scope);
		let cap = self.window.size(ctx);
		let mut ids: Vec<&RecordId> = std::mem::take(&mut self.window.ids);
		let mut covered = rows.len();
		'rows: for (at, row) in rows.iter().enumerate() {
			let Value::Object(object) = row else {
				continue;
			};
			let before = ids.len();
			for field in fields {
				let links: &[Value] = match object.get(field.as_str()) {
					Some(Value::Array(links)) if arrays => &links.0,
					Some(link @ Value::RecordId(_)) => std::slice::from_ref(link),
					_ => continue,
				};
				for link in links {
					let Value::RecordId(rid) = link else {
						continue;
					};
					let start = self.encoded.len();
					if !encode_key(&mut self.encoded, scope, rid) {
						self.encoded.truncate(start);
						continue;
					}
					let hash = key_hash(&self.encoded[start..]);
					let mut cursor = self.heads.get(&hash).copied().unwrap_or(NO_ID);
					let mut seen = false;
					while cursor != NO_ID {
						let (first, last) = self.spans[cursor as usize];
						if self.encoded[first as usize..last as usize] == self.encoded[start..] {
							seen = true;
							break;
						}
						cursor = self.chain[cursor as usize];
					}
					if seen {
						self.encoded.truncate(start);
						continue;
					}
					if ids.len() >= cap {
						self.encoded.truncate(start);
						// A row that added no key is left to the next fill, unless it is the first.
						covered = if ids.len() > before || at == 0 {
							at + 1
						} else {
							at
						};
						break 'rows;
					}
					let distinct = ids.len() as u32;
					self.chain.push(self.heads.insert(hash, distinct).unwrap_or(NO_ID));
					self.spans.push((start as u32, self.encoded.len() as u32));
					ids.push(rid);
				}
			}
		}
		let read = self.window.read(ctx, &ids).await;
		#[cfg(test)]
		tests::note_ahead(ids.len());
		ids.clear();
		self.window.ids =
			ids.into_iter().map(|_| -> &'static RecordId { unreachable!() }).collect();
		#[cfg(test)]
		tests::refused()?;
		read.map(|()| covered)
	}

	/// What the window holds for the record `rid` names in the namespace and database of `ctx`:
	/// [`Self::slot_at`] for the scope `ctx` runs in. `None` where `ctx` has no database.
	pub(crate) fn slot(&self, ctx: &ExecutionContext, rid: &RecordId) -> Option<Slot<'_>> {
		let db_ctx = ctx.database().ok()?;
		self.slot_at((db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id), rid)
	}

	/// What the window holds for the record `rid` names in `scope`: its cached record or its stored
	/// bytes. `None` for a record the window does not hold, that it found no bytes for, or that
	/// `scope` names in another namespace or database than the one the window was filled for.
	pub(crate) fn slot_at(
		&self,
		scope: (NamespaceId, DatabaseId),
		rid: &RecordId,
	) -> Option<Slot<'_>> {
		if self.scope != Some(scope) {
			return None;
		}
		self.window.distinct_slot(self.find(rid)?)
	}

	/// Whether the window serves the record `rid` names: [`Self::slot`] answers one.
	pub(crate) fn holds(&self, rid: &RecordId) -> bool {
		self.find(rid).is_some_and(|distinct| self.window.serves(distinct))
	}

	/// The distinct id of the record `rid` names, among those the window keyed.
	fn find(&self, rid: &RecordId) -> Option<usize> {
		let scope = self.scope?;
		if self.spans.is_empty() {
			return None;
		}
		let mut probe = self.probe.lock().unwrap_or_else(PoisonError::into_inner);
		probe.clear();
		if !encode_key(&mut probe, scope, rid) {
			return None;
		}
		let mut cursor = self.heads.get(&key_hash(&probe)).copied().unwrap_or(NO_ID);
		while cursor != NO_ID {
			let (first, last) = self.spans[cursor as usize];
			if self.encoded[first as usize..last as usize] == probe[..] {
				return Some(cursor as usize);
			}
			cursor = self.chain[cursor as usize];
		}
		None
	}
}

#[cfg(test)]
pub(crate) mod tests {
	use std::cell::Cell;

	use super::*;
	use crate::catalog::providers::TableProvider;
	use crate::exec::operators::test_util::TestDb;

	thread_local! {
		/// How many windows this thread filled, and how many positions they took.
		static FILLS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
		/// How many rows windows this thread filled, and how many distinct keys they took.
		static AHEAD: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
		/// How many collections of links this thread fetched by overlapping reads.
		static EACH: Cell<usize> = const { Cell::new(0) };
		/// Whether a rows window's fill on this thread fails as the process past its memory
		/// threshold fails one.
		pub(crate) static REFUSE_AHEAD: Cell<bool> = const { Cell::new(false) };
	}

	/// The failure a rows window's fill raises where the test pins one.
	pub(super) fn refused() -> crate::expr::FlowResult<()> {
		if REFUSE_AHEAD.with(Cell::get) {
			return Err(crate::expr::ControlFlow::Err(anyhow::Error::new(
				crate::kvs::DatastoreError::QueryBeyondMemoryThreshold,
			)));
		}
		Ok(())
	}

	pub(crate) fn note_each() {
		EACH.with(|each| each.set(each.get() + 1));
	}

	/// The collections of links this thread has fetched by overlapping reads.
	pub(crate) fn eaches() -> usize {
		EACH.with(Cell::get)
	}

	pub(super) fn note_ahead(keys: usize) {
		AHEAD.with(|ahead| {
			let (count, total) = ahead.get();
			ahead.set((count + 1, total + keys));
		});
	}

	/// The rows windows this thread has filled, and the distinct keys they took.
	pub(crate) fn aheads() -> (usize, usize) {
		AHEAD.with(Cell::get)
	}

	pub(super) fn note_fill(positions: usize) {
		FILLS.with(|fills| {
			let (count, total) = fills.get();
			fills.set((count + 1, total + positions));
		});
	}

	/// The windows this thread has filled, and the positions they took.
	pub(crate) fn fills() -> (usize, usize) {
		FILLS.with(Cell::get)
	}

	/// Forgets the windows this thread has filled.
	pub(crate) fn reset_fills() {
		FILLS.with(|fills| fills.set((0, 0)));
		AHEAD.with(|ahead| ahead.set((0, 0)));
		EACH.with(|each| each.set(0));
	}

	fn rid(table: &str, n: i64) -> Value {
		Value::RecordId(RecordId::new(table.into(), n))
	}

	/// What the slot of each position of a window the ids `values` filled holds, as a word.
	fn words(window: &mut LinkWindow, values: &[Value]) -> Vec<&'static str> {
		let mut out = Vec::new();
		for (at, value) in values.iter().enumerate() {
			if matches!(value, Value::RecordId(_)) {
				out.push(match window.slot(at) {
					Slot::Cached(..) => "cached",
					Slot::Stored(_) => "stored",
					Slot::Unread => "unread",
				});
			}
		}
		out
	}

	#[tokio::test]
	async fn a_window_holds_the_stored_bytes_of_the_records_that_exist() {
		let db = TestDb::new("CREATE person:1 SET n = 1; CREATE person:3 SET n = 3;").await;
		let ctx = db.exec_ctx().await;
		let values = vec![
			rid("person", 1),
			Value::from(7),
			rid("person", 2),
			rid("person", 3),
			rid("person", 1),
		];
		let mut window = LinkWindow::default();
		window.fill(&ctx, &values, 0, &|_| true).await.expect("fills");
		assert_eq!(window.positions, vec![0, 2, 3, 4], "the ids of the collection, in order");
		assert_eq!(window.slots.len(), 3, "an id that repeats is read once");
		assert_eq!(window.found.records, 2, "the missing record has no bytes");
		assert_eq!(words(&mut window, &values), ["stored", "unread", "stored", "stored"]);
		assert!(window.spent());
	}

	#[tokio::test]
	async fn a_window_serves_a_cached_record_from_the_cache_and_inserts_none() {
		let db = TestDb::new("CREATE person:1 SET n = 1; CREATE person:2 SET n = 2;").await;
		let ctx = db.exec_ctx().await;
		let values = vec![rid("person", 1), rid("person", 2)];
		let mut window = LinkWindow::default();
		window.fill(&ctx, &values, 0, &|_| true).await.expect("fills");
		assert_eq!(words(&mut window, &values), ["stored", "stored"]);
		// Nothing was cached by the first window, so a second still reads the bytes.
		window.fill(&ctx, &values, 0, &|_| true).await.expect("fills");
		assert_eq!(words(&mut window, &values), ["stored", "stored"]);
		// A record the transaction has read is served from its cache.
		let record = RecordId::new("person".into(), 2);
		let db_ctx = ctx.database().expect("a database context");
		db_ctx
			.txn()
			.get_record(
				db_ctx.ns_ctx.ns.namespace_id,
				db_ctx.db.database_id,
				&record.table,
				&record.key,
				None,
			)
			.await
			.expect("reads");
		window.fill(&ctx, &values, 0, &|_| true).await.expect("fills");
		assert_eq!(words(&mut window, &values), ["stored", "cached"]);
	}

	#[tokio::test]
	async fn a_window_takes_the_ids_the_site_admits_and_a_run_at_a_time() {
		let db =
			TestDb::new("FOR $i IN 0..40 { CREATE type::record('person', $i) SET n = $i; };").await;
		let ctx = db.exec_ctx().await;
		let values: Vec<Value> = (0..40).map(|n| rid("person", n)).collect();
		let admit = |rid: &RecordId| rid.key != crate::val::RecordIdKey::Number(5);
		let mut window = LinkWindow::default();
		window.fill(&ctx, &values, 0, &admit).await.expect("fills");
		assert_eq!(window.positions.len(), 32, "the first window is a probe of 32");
		assert!(!window.positions.contains(&5), "an id the site does not admit is left out");
		assert!(!window.spent());
		// The consumer asks position by position, and skips the one it does not admit.
		for at in (0..40).filter(|at| *at != 5).take(32) {
			assert!(matches!(window.slot(at), Slot::Stored(_)), "position {at}");
		}
		assert!(window.spent());
		window.fill(&ctx, &values, 33, &admit).await.expect("fills");
		assert_eq!(window.positions.first(), Some(&33));
		assert!(window.positions.len() > 1, "the window grew from the probe");
	}

	#[tokio::test]
	async fn a_failed_read_is_not_raised_and_every_position_reads_for_itself() {
		let db = TestDb::new("CREATE person:1 SET n = 1; CREATE person:2 SET n = 2;").await;
		let ctx = db.exec_ctx().await;
		let values = vec![rid("person", 1), rid("person", 2)];
		// Close the transaction the window reads through.
		ctx.database().expect("a database context").txn().cancel().await.expect("closes");
		let mut window = LinkWindow::default();
		window.fill(&ctx, &values, 0, &|_| true).await.expect("a failed read is not raised");
		assert_eq!(words(&mut window, &values), ["unread", "unread"]);
	}

	#[tokio::test]
	async fn a_pool_gives_a_window_back_empty() {
		let pool = LinkPool::default();
		let mut window = pool.take();
		window.positions.push(3);
		pool.put(window);
		let window = pool.take();
		assert!(window.positions.is_empty() && window.spent());
	}

	fn row(author: Option<Value>, friends: Vec<Value>) -> Value {
		let mut object = crate::val::Object::default();
		if let Some(author) = author {
			object.insert("author", author);
		}
		object.insert("friends", Value::Array(friends.into()));
		Value::Object(object)
	}

	fn named(fields: &[&str]) -> Vec<Strand> {
		fields.iter().map(|field| Strand::from(*field)).collect()
	}

	#[tokio::test]
	async fn a_rows_window_serves_the_records_the_fields_of_its_rows_link() {
		let db = TestDb::new(
			"FOR $i IN 0..6 { CREATE type::record('user', $i) SET n = $i; }; CREATE user:9 SET n = 9;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let rows = vec![
			row(Some(rid("user", 1)), vec![rid("user", 2), rid("user", 3), Value::from(7)]),
			row(None, vec![rid("user", 2), rid("missing", 1)]),
			row(Some(Value::from("not a link")), vec![]),
			Value::from(5),
			row(Some(rid("user", 4)), vec![rid("user", 1)]),
		];
		let mut ahead = RowsWindow::default();
		ahead.fill(&ctx, &rows, &named(&["author", "friends"]), true).await.expect("fills");
		let held = |n: i64| {
			let Value::RecordId(rid) = rid("user", n) else {
				unreachable!()
			};
			ahead.holds(&rid)
		};
		assert!(held(1) && held(2) && held(3) && held(4), "every link of a keyed field is held");
		assert!(!held(5), "a record no row links is not held");
		let RowsWindow {
			window,
			..
		} = &ahead;
		assert_eq!(window.slots.len(), 5, "four users and the missing table's one id, each once");
		let Value::RecordId(missing) = rid("missing", 1) else {
			unreachable!()
		};
		assert!(!ahead.holds(&missing), "a record the read found no bytes for is read for itself");
		assert!(ahead.slot(&ctx, &missing).is_none());
		let Value::RecordId(two) = rid("user", 2) else {
			unreachable!()
		};
		assert!(matches!(ahead.slot(&ctx, &two), Some(Slot::Stored(_))));
		// The same id in another namespace or database is another record.
		let (ns, db) = ahead.scope.expect("the fill's scope");
		assert!(matches!(ahead.slot_at((ns, db), &two), Some(Slot::Stored(_))));
		assert!(ahead.slot_at((NamespaceId(ns.0 + 1), db), &two).is_none(), "another namespace");
		assert!(ahead.slot_at((ns, DatabaseId(db.0 + 1)), &two).is_none(), "another database");
	}

	#[tokio::test]
	async fn a_rows_window_takes_no_more_keys_than_one_window_holds() {
		let db =
			TestDb::new("FOR $i IN 0..100 { CREATE type::record('user', $i) SET n = $i; };").await;
		let ctx = db.exec_ctx().await;
		let rows: Vec<Value> = (0..100).map(|n| row(Some(rid("user", n)), vec![])).collect();
		let mut ahead = RowsWindow::default();
		ahead.fill(&ctx, &rows, &named(&["author"]), true).await.expect("fills");
		assert_eq!(ahead.window.slots.len(), 32, "the first window is a probe of 32");
		let Value::RecordId(first) = rid("user", 0) else {
			unreachable!()
		};
		let Value::RecordId(last) = rid("user", 99) else {
			unreachable!()
		};
		assert!(ahead.holds(&first), "the keys of the first rows are taken");
		assert!(!ahead.holds(&last), "the keys past the window read for themselves");
		ahead.fill(&ctx, &rows, &named(&["author"]), true).await.expect("fills");
		assert!(ahead.window.slots.len() > 32, "the next window grew from the first");
	}

	#[tokio::test]
	async fn a_run_starts_from_a_probe_whatever_size_the_run_before_it_grew_to() {
		let db =
			TestDb::new("FOR $i IN 0..300 { CREATE type::record('user', $i) SET n = $i; };").await;
		let ctx = db.exec_ctx().await;
		let rows: Vec<Value> = (0..300).map(|n| row(Some(rid("user", n)), vec![])).collect();
		let mut ahead = RowsWindow::default();
		for _ in 0..3 {
			ahead.fill(&ctx, &rows, &named(&["author"]), true).await.expect("fills");
		}
		assert!(ahead.window.slots.len() > 32, "the fills of a run grow");
		// The next run reads other records, whose size the last run says nothing of.
		ahead.clear();
		ahead.fill(&ctx, &rows, &named(&["author"]), true).await.expect("fills");
		assert_eq!(ahead.window.slots.len(), 32, "a run starts from the probe");
		// A pooled window keeps its size for the sites of one run, and forgets it with the run.
		let pool = LinkPool::default();
		let values: Vec<Value> = (0..300).map(|n| rid("user", n)).collect();
		let mut window = pool.take();
		for _ in 0..3 {
			window.fill(&ctx, &values, 0, &|_| true).await.expect("fills");
		}
		assert!(window.len > 32);
		pool.put(window);
		assert!(pool.take().len > 32, "the next site of the run keeps the size");
		pool.clear();
		assert_eq!(pool.take().len, 0, "the next run does not");
	}

	#[tokio::test]
	async fn a_rows_window_holds_nothing_before_it_is_filled_or_after_a_failed_read() {
		let db = TestDb::new("CREATE user:1 SET n = 1; CREATE user:2 SET n = 2;").await;
		let ctx = db.exec_ctx().await;
		let Value::RecordId(one) = rid("user", 1) else {
			unreachable!()
		};
		let mut ahead = RowsWindow::default();
		assert!(!ahead.holds(&one) && ahead.slot(&ctx, &one).is_none());
		ctx.database().expect("a database context").txn().cancel().await.expect("closes");
		let rows = vec![row(Some(rid("user", 1)), vec![rid("user", 2)])];
		ahead.fill(&ctx, &rows, &named(&["author", "friends"]), true).await.expect("not raised");
		assert!(!ahead.holds(&one), "a failed read holds nothing");
	}

	#[tokio::test]
	async fn a_cleared_rows_window_holds_nothing_and_starts_over_from_the_probe() {
		let db =
			TestDb::new("FOR $i IN 0..100 { CREATE type::record('user', $i) SET n = $i; };").await;
		let ctx = db.exec_ctx().await;
		let rows: Vec<Value> = (0..100).map(|n| row(Some(rid("user", n)), vec![])).collect();
		let mut ahead = RowsWindow::default();
		ahead.fill(&ctx, &rows, &named(&["author"]), true).await.expect("fills");
		let learned = ahead.window.len;
		assert!(learned > 32, "the size grew from the probe: {learned}");
		let Value::RecordId(first) = rid("user", 0) else {
			unreachable!()
		};
		assert!(ahead.holds(&first));
		ahead.clear();
		assert!(!ahead.holds(&first) && ahead.slot(&ctx, &first).is_none());
		ahead.fill(&ctx, &rows, &named(&["author"]), true).await.expect("fills");
		assert_eq!(ahead.window.slots.len(), 32, "the next fill is a probe");
	}
}
