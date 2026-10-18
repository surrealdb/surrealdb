//! The host services the IR's execution reaches through [`Services`](super::services::Services).
//!
//! A host that has none answers `None` from `Services::ir_host`, and a program that needs one is
//! not served by that host. The engine's host answers with [`EngineIrHost`], built from the
//! running statement's context, so everything an IR instruction asks of the engine is defined
//! here.
//!
//! An instruction the interpreter hands to the host ([`IrFlow::Host`](super::ir_exec::IrFlow))
//! reaches [`run`], which answers where the row resumes.
//!
//! # The read buffer
//!
//! A statement's [`ReadBuffer`] holds the stored bytes of records a [`BatchRead`] read ahead of
//! the positions that consume them. It holds only hits: a key the batched read found absent is
//! not recorded, so the consumer reads it itself and raises what the serial run raises. A fill
//! reads with one plain `getm_raw` on the statement's transaction, which neither locks a key nor
//! feeds the transaction's record cache. A failed read raises its error and leaves the partition
//! empty and uncovered; the other partitions stay valid.
//!
//! A hit is served only to a consumer of the transaction and the authority frame the fill ran in
//! ([`FrameClass`]), by the exact encoded key, and never to a versioned read. Coverage, which
//! says whether the current position needs a fill, only avoids refilling: a hit is served
//! whether or not its position is covered.
//!
//! Nothing here outlives the statement: the buffer is held by the statement's root context, and
//! its partitions, pages and counters are dropped with it. The buffer's mutex is never held
//! across an await.

#![deny(clippy::wildcard_enum_match_arm)]

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use smallvec::SmallVec;
use surrealdb_kvs::Page;

use super::fuel;
use super::interp::State;
use super::ir_exec::{PendingCandidate, held_ahead, ir_state, scalar_of};
use super::ir_instr::{
	BatchRead, BufferScope, IrForm, IrInstr, LookAhead, SiteKey, WriteResumeSpec,
};
use super::morsel::Stop;
use super::program::Program;
use super::scalar::Scalar;
use crate::catalog::{DatabaseId, NamespaceId, Record, TableLayouts};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::key::KVKey;
use crate::key::schema::RecordKey;
use crate::kvs::{LayoutSet, Transaction};
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// The most keys a statement-scope fill reads.
const PREFETCH_STATEMENT_MAX_KEYS: usize = 256;

/// How many times the execution config's `scan_batch_bytes` the whole buffer may hold. Before a
/// fill the pages of other partitions are cleared, least recently filled first, until the bytes
/// the buffer holds and the bytes the fill may add fit within it. A fill may add at most
/// `scan_batch_bytes`, whatever its read's `max_bytes`, so one fill never clears the whole buffer.
const READ_BUFFER_MAX_FACTOR: usize = 2;

/// The services IR-lowered instructions call on their host.
pub(super) trait IrHostService: Send + Sync {
	/// The context of the statement the host serves.
	fn ctx(&self) -> &ExecutionContext;
}

/// The IR host service of the engine this process runs.
pub(super) struct EngineIrHost<'a> {
	ctx: &'a ExecutionContext,
}

impl<'a> EngineIrHost<'a> {
	/// The service of the statement `ctx` runs.
	pub(super) fn new(ctx: &'a ExecutionContext) -> Self {
		Self {
			ctx,
		}
	}
}

impl IrHostService for EngineIrHost<'_> {
	fn ctx(&self) -> &ExecutionContext {
		self.ctx
	}
}

/// Where the row resumes after the host ran an IR instruction.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(super) enum IrResume {
	/// At the instruction at this position.
	At(usize),
	/// The row is done; the pipeline moves to its next.
	EndRow,
	/// The pipeline needs no more rows.
	StopPipeline,
}

/// Runs `instr`, the instruction at `at` of the pipeline the row runs through, on `host`.
///
/// `row` is the reader the row's interpreter stretch ran over, a concrete type so that the
/// future holding it stays `Send`. An instruction that cannot complete fails with an error; it
/// never resumes the row as if it had.
///
/// A [`BatchRead`] fills the statement's read buffer for the window of positions it names and
/// resumes at the next instruction. The instruction answers the error of a failed read, and a
/// program that reached it with no buffer, which the interpreter's own arm makes unreachable.
///
/// A [`WriteResume`](IrForm::WriteResume) installs the candidate the interpreter's arm took: it
/// raises the statement's cancellation, deadline or memory threshold as the write body's first
/// instruction does, installs the candidate's record and context as the record in flight, restores
/// the registers the prefix wrote, and resumes at the instruction its stop names. A candidate that
/// is missing, or whose stop names no resumption, is an internal error.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run<'r>(
	host: &dyn IrHostService,
	instr: &IrInstr,
	at: usize,
	_program: &'r Program,
	_row: &'r (dyn super::row::RowReader + Sync),
	regs: &mut [Scalar<'r>],
	state: &mut State,
) -> FlowResult<IrResume> {
	if let IrForm::BatchRead(read) = instr.form() {
		let window = gather(read, at, regs, state)?;
		fill(&window.buffer, read, window.program_id, &window.elems, window.remaining, host.ctx())
			.await
			.map_err(ControlFlow::Err)?;
		return Ok(IrResume::At(at + 1));
	}
	if let IrForm::WriteResume(spec) = instr.form() {
		return resume_candidate(host, spec, regs, state);
	}
	Err(ControlFlow::Err(instr.unexecuted()))
}

/// Resumes the candidate the interpreter's `WriteResume` arm held in `state`, as [`run`] says.
fn resume_candidate<'r>(
	host: &dyn IrHostService,
	spec: &WriteResumeSpec,
	regs: &mut [Scalar<'r>],
	state: &mut State,
) -> FlowResult<IrResume> {
	let Some(write) = state.write.as_deref_mut() else {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"a write candidate was pending with no write state"
		)));
	};
	let Some(candidate) = write.take_pending() else {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"a write resume reached the host with no candidate"
		)));
	};
	let PendingCandidate {
		row,
		ctx,
		live,
		stop,
	} = candidate;
	let target = resume_target(spec, &stop)?;
	fuel::interrupted(host.ctx())?;
	write.resume_candidate(row, ctx);
	restore_live(live, regs)?;
	Ok(IrResume::At(target))
}

/// The instruction a candidate that stopped at `stop` resumes at: the cut for a prefix that
/// reached it, the skip for a record a stage skipped. A prefix that went off the cut path or
/// raised hands the coordinator no candidate to resume.
fn resume_target(spec: &WriteResumeSpec, stop: &Stop) -> FlowResult<usize> {
	match stop {
		Stop::Cut => Ok(spec.cut as usize),
		Stop::Skip => Ok(spec.skip as usize),
		Stop::OffPath | Stop::Failed(_) => Err(ControlFlow::Err(anyhow::anyhow!(
			"a write candidate that stopped off the cut path was resumed"
		))),
	}
}

/// Writes each `(register, value)` of `live` into the register file.
fn restore_live<'r>(live: SmallVec<[(u16, Value); 8]>, regs: &mut [Scalar<'r>]) -> FlowResult<()> {
	for (register, value) in live {
		let Some(slot) = regs.get_mut(register as usize) else {
			return Err(ControlFlow::Err(anyhow::anyhow!(
				"a write candidate restores register {register}, which the row does not hold"
			)));
		};
		*slot = scalar_of(value);
	}
	Ok(())
}

/// What a [`BatchRead`] fills from: the buffer and the program's identity in it, and the window
/// of positions.
struct Gathered {
	buffer: Arc<ReadBuffer>,
	program_id: usize,
	/// The current element and the elements held ahead of it, at most `max_positions`. A
	/// position that holds no record id is `NONE`.
	elems: Vec<Value>,
	/// The number of elements the loop's iterator holds.
	remaining: usize,
}

/// Reads the window of `read` from the registers and the iterator it names. Reads only held
/// values: nothing is drawn from a source.
fn gather(
	read: &BatchRead,
	at: usize,
	regs: &[Scalar<'_>],
	state: &mut State,
) -> FlowResult<Gathered> {
	let (buffer, program_id) = match ir_state(state) {
		Some(ir) => (ir.buffer.clone(), ir.program_id),
		None => (None, 0),
	};
	let Some(buffer) = buffer else {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"the batched read at instruction {at} ran with no read buffer"
		)));
	};
	let window = (read.max_positions as usize).max(1);
	let (elems, remaining) = match read.ahead {
		LookAhead::Iter {
			slot,
			elem,
		} => {
			let held = held_ahead(&state.iters, slot);
			let Some(current) = regs.get(elem as usize) else {
				return Err(ControlFlow::Err(anyhow::anyhow!(
					"the batched read at instruction {at} names register {elem}, which the row does not hold"
				)));
			};
			let mut elems = Vec::with_capacity(window.min(held.len() + 1));
			elems.push(current.as_whole().map_or(Value::None, key_carrier));
			elems.extend(held.iter().take(window - 1).map(key_carrier));
			(elems, held.len())
		}
		LookAhead::None => (Vec::new(), 0),
		LookAhead::Elems {
			..
		} => {
			return Err(ControlFlow::Err(anyhow::anyhow!(
				"the batched read at instruction {at} looks ahead over an array, which no lowering emits"
			)));
		}
	};
	Ok(Gathered {
		buffer,
		program_id,
		elems,
		remaining,
	})
}

/// What a window position contributes to a fill: a record id as itself, and anything else, which
/// names no key, as `NONE`, so that a position holding a large value is not copied.
fn key_carrier(value: &Value) -> Value {
	if matches!(value, Value::RecordId(_)) {
		value.clone()
	} else {
		Value::None
	}
}

/// The read buffer of `ctx`'s statement, or `None` when the statement batches nothing.
///
/// This body answers `None` for every context, so no statement batches a read through it.
pub(super) fn read_buffer_of(_ctx: &ExecutionContext) -> Option<&Arc<ReadBuffer>> {
	None
}

/// The authority frame a fill ran in, which a consumer must share to be served from it.
///
/// The buffer holds stored bytes before any step that depends on the frame (permissions, field
/// views, computed fields), so nothing is shared across authority frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FrameClass {
	perms_suppressed: bool,
	skip_fetch_perms: bool,
	/// Whether the frame refuses a write reached from it.
	no_write: bool,
	/// The identity of the authentication the statement runs as.
	auth: usize,
}

impl FrameClass {
	/// The frame of a partition no fill has run in.
	const UNSET: Self = Self {
		perms_suppressed: false,
		skip_fetch_perms: false,
		no_write: false,
		auth: 0,
	};

	/// The frame `ctx` runs in.
	pub(crate) fn of(ctx: &ExecutionContext) -> Self {
		let root = ctx.root();
		Self {
			perms_suppressed: root.perms_suppressed,
			skip_fetch_perms: root.skip_fetch_perms,
			no_write: root.no_write.is_some(),
			auth: Arc::as_ptr(&root.auth) as usize,
		}
	}
}

/// What a partition has filled of the positions a loop's current position can reach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Cover {
	/// A look-ahead over an iterator's held elements: the current position is covered while the
	/// iterator holds at least `stop` elements. A fill of `n` positions with `remaining`
	/// elements held sets `stop = remaining - (n - 1)`.
	Remaining {
		stop: usize,
	},
	/// A look-ahead over an array by position: the positions before `until` are covered.
	/// Reserved with `LookAhead::Elems`, which no lowering emits.
	#[allow(dead_code, reason = "reserved for `LookAhead::Elems`")]
	Position {
		until: usize,
	},
	/// The statement-scope keys were read.
	Filled,
	/// Nothing is covered: the partition is new, was reset at a loop entry, or lost its page to
	/// an eviction.
	Uncovered,
}

/// One partition of the buffer: the keys one instruction site read, and what it read for them.
struct Partition {
	/// The identity of the running program, so that a nested unit's sites do not meet its
	/// caller's.
	program: usize,
	scope: BufferScope,
	/// The identity of the transaction the page was read in.
	txn: usize,
	frame: FrameClass,
	cover: Cover,
	/// The fill counter at the last fill, for evicting the least recently filled.
	last_fill: u64,
	/// The encode buffer of a fill: the encoded keys of the window, concatenated. Reused.
	keys: Vec<u8>,
	/// The keys and values of the hits. Reused.
	page: Page,
	/// The hash of an encoded key, to the entries of `page` with that hash. Reused.
	index: HashMap<u64, SmallVec<[u32; 1]>>,
	/// The layouts that admitted the packed entries of `page`, one per table, which an entry is
	/// decoded against.
	layouts: SmallVec<[(TableName, Arc<TableLayouts>); 1]>,
}

impl Partition {
	fn new(program: usize, scope: BufferScope) -> Self {
		Self {
			program,
			scope,
			txn: 0,
			frame: FrameClass::UNSET,
			cover: Cover::Uncovered,
			last_fill: 0,
			keys: Vec::new(),
			page: Page::default(),
			index: HashMap::new(),
			layouts: SmallVec::new(),
		}
	}

	/// Drops the entries held, keeping the buffers, and uncovers the partition.
	fn clear(&mut self) {
		self.page.clear();
		self.index.clear();
		self.layouts.clear();
		self.cover = Cover::Uncovered;
	}
}

/// The partitions of a buffer and what they hold.
#[derive(Default)]
struct Partitions {
	/// One per instruction site and program live in the statement.
	parts: SmallVec<[Partition; 2]>,
	/// The bytes of every page, so that a fill can keep the whole within its bound.
	held_bytes: usize,
	/// The number of fills, which orders the partitions by recency.
	fills: u64,
}

impl Partitions {
	/// Whether any partition holds an entry a consumer can be served.
	fn serves(&self) -> bool {
		self.parts.iter().any(|part| !part.index.is_empty())
	}

	/// The partition of `program` and `scope`, taken out, or a new one. The page it held is
	/// dropped from the held bytes.
	fn take(&mut self, program: usize, scope: BufferScope) -> Partition {
		match self.parts.iter().position(|p| p.program == program && p.scope == scope) {
			Some(at) => {
				let part = self.parts.swap_remove(at);
				self.held_bytes = self.held_bytes.saturating_sub(part.page.bytes());
				part
			}
			None => Partition::new(program, scope),
		}
	}

	/// Clears the pages of the partitions, least recently filled first, until `incoming` bytes
	/// more fit within `bound`, or no page is left.
	fn make_room(&mut self, incoming: usize, bound: usize) {
		while self.held_bytes.saturating_add(incoming) > bound {
			let Some(oldest) = self
				.parts
				.iter_mut()
				.filter(|part| part.page.bytes() > 0)
				.min_by_key(|part| part.last_fill)
			else {
				return;
			};
			self.held_bytes = self.held_bytes.saturating_sub(oldest.page.bytes());
			oldest.clear();
		}
	}
}

/// A statement's read buffer. See the [module docs](self).
pub(crate) struct ReadBuffer {
	/// Whether any partition holds an entry. Read first by every consumer, so that a statement
	/// whose fills found nothing pays one relaxed load per read.
	live: AtomicBool,
	/// Never held across an await: a fill takes its partition out, awaits the read, and puts it
	/// back. A lookup holds it for one search.
	inner: Mutex<Partitions>,
	/// The number of entries [`forget`] has dropped. Each drop is a write to a key the buffer
	/// held.
	forgotten: AtomicU64,
}

impl ReadBuffer {
	/// An empty buffer. Allocates nothing beyond the struct.
	#[allow(dead_code, reason = "request L16-2 builds one per statement")]
	pub(crate) fn new() -> Self {
		Self {
			live: AtomicBool::new(false),
			inner: Mutex::new(Partitions::default()),
			forgotten: AtomicU64::new(0),
		}
	}

	/// The number of entries [`forget`] has dropped.
	#[allow(dead_code, reason = "request L16-2 gives the executor the buffer to read it from")]
	pub(crate) fn forgotten(&self) -> u64 {
		self.forgotten.load(Ordering::Relaxed)
	}

	/// The state under the lock. A panic while the lock was held leaves partitions that are
	/// still consistent, so a poisoned lock is taken as it is.
	fn lock(&self) -> MutexGuard<'_, Partitions> {
		self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
	}

	/// Whether the partition of `program` and `scope` covers the current position, where
	/// `remaining` is the number of elements the loop's iterator holds, or `None` for a scope
	/// with no positions ahead.
	pub(super) fn covers(
		&self,
		program: usize,
		scope: BufferScope,
		remaining: Option<usize>,
	) -> bool {
		let inner = self.lock();
		let Some(part) = inner.parts.iter().find(|p| p.program == program && p.scope == scope)
		else {
			return false;
		};
		match (part.cover, remaining) {
			(
				Cover::Remaining {
					stop,
				},
				Some(remaining),
			) => remaining >= stop,
			(Cover::Filled, None) => true,
			(
				Cover::Remaining {
					..
				}
				| Cover::Position {
					..
				}
				| Cover::Filled
				| Cover::Uncovered,
				_,
			) => false,
		}
	}

	/// Empties the partition of `program` and `scope`, keeping its buffers, and uncovers it: the
	/// loop it serves is entered afresh.
	pub(super) fn reset(&self, program: usize, scope: BufferScope) {
		let mut inner = self.lock();
		let Some(at) = inner.parts.iter().position(|p| p.program == program && p.scope == scope)
		else {
			return;
		};
		let freed = inner.parts[at].page.bytes();
		inner.parts[at].clear();
		inner.held_bytes = inner.held_bytes.saturating_sub(freed);
		let serves = inner.serves();
		self.live.store(serves, Ordering::Relaxed);
	}

	/// The partition of `program` and `scope` for a fill in `txn` and `frame`: emptied, with the
	/// room for `incoming` bytes made within `bound` by clearing the pages of others.
	fn begin(
		&self,
		program: usize,
		scope: BufferScope,
		txn: usize,
		frame: FrameClass,
		incoming: usize,
		bound: usize,
	) -> Partition {
		let mut inner = self.lock();
		let mut part = inner.take(program, scope);
		part.clear();
		part.keys.clear();
		part.txn = txn;
		part.frame = frame;
		if part.page.capacity() != incoming {
			part.page = Page::with_capacity(incoming);
		}
		inner.make_room(incoming, bound);
		let serves = inner.serves();
		self.live.store(serves, Ordering::Relaxed);
		part
	}

	/// Puts a filled partition back.
	fn finish(&self, mut part: Partition) {
		let mut inner = self.lock();
		inner.fills += 1;
		part.last_fill = inner.fills;
		inner.held_bytes = inner.held_bytes.saturating_add(part.page.bytes());
		inner.parts.push(part);
		let serves = inner.serves();
		self.live.store(serves, Ordering::Relaxed);
	}

	/// The record `rid` names, decoded from the entry under the encoded key `key` of a partition
	/// read in `txn` and `frame`. The entry is decoded under the lock, which borrows the page
	/// instead of copying the stored bytes out of it.
	fn serve(&self, txn: usize, frame: FrameClass, key: &[u8], rid: &RecordId) -> Option<Value> {
		let hash = key_hash(key);
		let inner = self.lock();
		for part in inner.parts.iter().filter(|p| p.txn == txn && p.frame == frame) {
			let Some(found) = part.index.get(&hash) else {
				continue;
			};
			let batch = part.page.batch();
			for &entry in found {
				let Some((stored, value)) = batch.get(entry as usize) else {
					continue;
				};
				if stored == key {
					// A value that does not decode is the consumer's to read and to raise.
					let layouts = part
						.layouts
						.iter()
						.find(|(table, _)| *table == rid.table)
						.map(|(_, layouts)| &**layouts);
					return Record::decode_admitted(value, layouts, rid.clone())
						.ok()
						.map(|record| record.data);
				}
			}
		}
		None
	}

	/// Drops the entry under the encoded key `key` from every partition, and counts the drop.
	fn drop_key(&self, key: &[u8]) {
		let hash = key_hash(key);
		let mut inner = self.lock();
		let mut dropped = false;
		for part in inner.parts.iter_mut() {
			let batch = part.page.batch();
			let Some(found) = part.index.get_mut(&hash) else {
				continue;
			};
			let before = found.len();
			found
				.retain(|entry| batch.get(*entry as usize).is_none_or(|(stored, _)| stored != key));
			if found.len() != before {
				dropped = true;
				if found.is_empty() {
					part.index.remove(&hash);
				}
			}
		}
		if dropped {
			self.forgotten.fetch_add(1, Ordering::Relaxed);
			let serves = inner.serves();
			self.live.store(serves, Ordering::Relaxed);
		}
	}
}

/// The hash a partition's index files an encoded key under.
fn key_hash(key: &[u8]) -> u64 {
	let mut hasher = DefaultHasher::new();
	key.hash(&mut hasher);
	hasher.finish()
}

/// The stored record `rid` names in the namespace `ns` and database `db`, if the statement's
/// read buffer holds it for `ctx`'s transaction and authority frame.
///
/// Answers `None` before encoding anything when the read is versioned, when the context holds
/// no buffer, or when the buffer holds no entry. A hit is a record that exists, never a
/// missing one: a record the batched read did not find is read by the consumer.
#[allow(dead_code, reason = "request L16-2 places the call in `fetch_raw_record_in`")]
pub(crate) fn served_record(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	rid: &RecordId,
	version: Option<u64>,
) -> Option<Value> {
	serve_from(read_buffer_of(ctx).map(|buffer| &**buffer), ctx, ns, db, rid, version)
}

/// [`served_record`] over `buffer`, the statement's buffer if it has one.
pub(super) fn serve_from(
	buffer: Option<&ReadBuffer>,
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	rid: &RecordId,
	version: Option<u64>,
) -> Option<Value> {
	if version.is_some() {
		return None;
	}
	let buffer = buffer?;
	if !buffer.live.load(Ordering::Relaxed) || matches!(rid.key, RecordIdKey::Range(_)) {
		return None;
	}
	let txn = ctx.root().ctx.try_tx()?;
	let mut key = Vec::new();
	record_key(ns, db, &rid.table, &rid.key).encode_buffer(&mut key).ok()?;
	buffer.serve(Arc::as_ptr(txn) as usize, FrameClass::of(ctx), &key, rid)
}

/// Drops the entry under the encoded key `key` from the statement's read buffer, as the write of
/// that key obliges. Returns at once when the context holds no buffer or the buffer holds
/// nothing.
#[allow(dead_code, reason = "request L16-2 places the calls beside the VM's record writes")]
pub(crate) fn forget(ctx: &ExecutionContext, key: &[u8]) {
	if let Some(buffer) = read_buffer_of(ctx)
		&& buffer.live.load(Ordering::Relaxed)
	{
		buffer.drop_key(key);
	}
}

/// The key of the record `id` of table `tb`.
fn record_key<'a>(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'a TableName,
	id: &'a RecordIdKey,
) -> RecordKey<'a> {
	RecordKey {
		ns,
		db,
		tb: Cow::Borrowed(tb),
		id: Cow::Borrowed(id),
	}
}

/// What a fill reads in: the statement's transaction and database, and the bound of its buffer.
struct FillTarget {
	ns: NamespaceId,
	db: DatabaseId,
	txn: Arc<Transaction>,
	frame: FrameClass,
	/// What the whole buffer holds at most, a fill's own page aside.
	bound: usize,
	/// What one fill's page may hold at most: the config's `scan_batch_bytes`.
	share: usize,
}

impl FillTarget {
	fn of(ctx: &ExecutionContext) -> Option<Self> {
		let db_ctx = ctx.database().ok()?;
		let share = ctx.root().ctx.config.exec.scan_batch_bytes;
		Some(Self {
			ns: db_ctx.ns_ctx.ns.namespace_id,
			db: db_ctx.db.database_id,
			txn: ctx.try_txn()?,
			frame: FrameClass::of(ctx),
			bound: READ_BUFFER_MAX_FACTOR.saturating_mul(share),
			share,
		})
	}
}

/// The keys of a fill's window, in the order they are read.
#[derive(Default)]
struct Window<'a> {
	/// The keys read.
	keys: Vec<RecordKey<'a>>,
	/// The table of each key.
	tables: Vec<&'a TableName>,
	/// Where each key's encoding lies in the partition's encode buffer.
	spans: Vec<(usize, usize)>,
	/// The window position each key was taken for.
	positions: Vec<u32>,
	/// The positions whose keys were all taken.
	requested: usize,
}

/// Fills the partition of `read.scope` of `buffer` for the window of positions `elems` holds,
/// the current element first and the elements held ahead of it after it.
///
/// `remaining` is the number of elements the loop's iterator holds, of which `elems` takes the
/// first `elems.len() - 1`; the partition's coverage is counted from it. A statement-scope read
/// has no elements, and its keys are the literals of `read.keys`.
///
/// One `getm_raw` reads the window's distinct keys on the statement's transaction. Each hit is
/// admitted against its table's layouts, which the partition keeps for its decode; a hit that is
/// not admitted is not buffered, and its consumer reads it and raises what it raises. The hits are
/// copied into the partition's page in key order up to `read.max_bytes`, capped at the config's
/// `scan_batch_bytes`, the first hit always;
/// the positions from the first whose hit did not fit are left uncovered. A context with no
/// transaction and a key that does not encode end the fill with what it had, and a consumer that
/// is not served reads for itself and raises what it raises. A failed read is raised, with the
/// partition left empty and uncovered. The transaction's record cache is not touched.
pub(super) async fn fill(
	buffer: &ReadBuffer,
	read: &BatchRead,
	program_id: usize,
	elems: &[Value],
	remaining: usize,
	ctx: &ExecutionContext,
) -> anyhow::Result<()> {
	let Some(target) = FillTarget::of(ctx) else {
		return Ok(());
	};
	let incoming = (read.max_bytes as usize).min(target.share);
	let mut part = buffer.begin(
		program_id,
		read.scope,
		Arc::as_ptr(&target.txn) as usize,
		target.frame,
		incoming,
		target.bound,
	);
	let cap = match read.scope {
		BufferScope::Statement {
			..
		} => PREFETCH_STATEMENT_MAX_KEYS,
		BufferScope::Loop {
			..
		} => (read.max_positions as usize).saturating_mul(read.keys.len()).max(1),
	};
	let Window {
		keys,
		tables,
		spans,
		positions,
		requested,
	} = collect(&mut part, read, elems, &target, cap);
	let mut covered = requested;
	// Collection indexes the window's keys by span. The index is cleared here and rebuilt over
	// the page's entries as the hits are pushed.
	part.index.clear();
	if !keys.is_empty() {
		let stored = match target.txn.getm_raw(keys, None).await {
			Ok(stored) => stored,
			Err(error) => {
				part.clear();
				buffer.finish(part);
				return Err(error);
			}
		};
		let mut admit = LayoutSet::new(target.ns, target.db, None);
		for (at, value) in stored.into_iter().enumerate() {
			// A key found absent is not buffered: its consumer reads it itself.
			let Some(value) = value else {
				continue;
			};
			let table = tables[at];
			match admit.admit_with_layouts(&target.txn, table, &value).await {
				Ok((_, Some(layouts))) => {
					if !part.layouts.iter().any(|(held, _)| held == table) {
						part.layouts.push((table.clone(), Arc::clone(layouts)));
					}
				}
				Ok((_, None)) => {}
				Err(_) => continue,
			}
			let (start, end) = spans[at];
			if !part.page.push(&part.keys[start..end], &value) {
				covered = covered.min(positions[at] as usize);
				break;
			}
			let entry = (part.page.len() - 1) as u32;
			part.index.entry(key_hash(&part.keys[start..end])).or_default().push(entry);
		}
	}
	let covered = covered.max(1);
	part.cover = match read.ahead {
		LookAhead::Iter {
			..
		} => Cover::Remaining {
			stop: remaining.saturating_sub(covered - 1),
		},
		LookAhead::None => Cover::Filled,
		LookAhead::Elems {
			..
		} => Cover::Uncovered,
	};
	buffer.finish(part);
	Ok(())
}

/// Encodes the keys of the window into `part.keys`, skipping a key already taken, a range key,
/// and a key that does not encode, and stopping when `cap` keys are taken.
///
/// While keys are taken the partition's index files each key's span in the window under its
/// hash, to find duplicates; [`fill`] rebuilds it over the page once the read is done.
fn collect<'a>(
	part: &mut Partition,
	read: &'a BatchRead,
	elems: &'a [Value],
	target: &FillTarget,
	cap: usize,
) -> Window<'a> {
	let mut window = Window::default();
	let positions = elems.len().min((read.max_positions as usize).max(1)).max(1);
	'positions: for position in 0..positions {
		for site in read.keys.iter() {
			match site {
				SiteKey::Elem => {
					if let Some(Value::RecordId(rid)) = elems.get(position)
						&& !take(part, &mut window, target, &rid.table, &rid.key, position, cap)
					{
						break 'positions;
					}
				}
				SiteKey::Literal(literals) => {
					for literal in literals.iter() {
						if !take(
							part,
							&mut window,
							target,
							&literal.table,
							&literal.key,
							position,
							cap,
						) {
							break 'positions;
						}
					}
				}
			}
		}
		window.requested = position + 1;
	}
	window
}

/// Takes the key of the record `id` of `tb` for window position `position` into `window`.
/// Answers whether the fill goes on: it does not once `cap` keys are taken.
fn take<'a>(
	part: &mut Partition,
	window: &mut Window<'a>,
	target: &FillTarget,
	tb: &'a TableName,
	id: &'a RecordIdKey,
	position: usize,
	cap: usize,
) -> bool {
	if matches!(id, RecordIdKey::Range(_)) {
		return true;
	}
	if window.spans.len() >= cap {
		return false;
	}
	let key = record_key(target.ns, target.db, tb, id);
	let start = part.keys.len();
	if key.encode_buffer(&mut part.keys).is_err() {
		part.keys.truncate(start);
		return true;
	}
	let hash = key_hash(&part.keys[start..]);
	let seen = part.index.get(&hash).is_some_and(|found| {
		found.iter().any(|&at| {
			let (first, last) = window.spans[at as usize];
			part.keys[first..last] == part.keys[start..]
		})
	});
	if seen {
		part.keys.truncate(start);
		return true;
	}
	part.index.entry(hash).or_default().push(window.spans.len() as u32);
	window.spans.push((start, part.keys.len()));
	window.positions.push(position as u32);
	window.keys.push(key);
	window.tables.push(tb);
	true
}

#[cfg(test)]
impl ReadBuffer {
	/// The bytes the pages of every partition hold.
	pub(super) fn held_bytes(&self) -> usize {
		self.lock().held_bytes
	}

	/// The coverage of the partition of `program` and `scope`, if it exists.
	pub(super) fn cover_of(&self, program: usize, scope: BufferScope) -> Option<Cover> {
		let inner = self.lock();
		inner.parts.iter().find(|p| p.program == program && p.scope == scope).map(|p| p.cover)
	}

	/// The entries and the capacity of the page of the partition of `program` and `scope`.
	pub(super) fn page_of(&self, program: usize, scope: BufferScope) -> Option<(usize, usize)> {
		let inner = self.lock();
		inner
			.parts
			.iter()
			.find(|p| p.program == program && p.scope == scope)
			.map(|p| (p.page.len(), p.page.capacity()))
	}

	/// The programs and scopes of the partitions that hold entries, least recently filled first.
	pub(super) fn filled_by_age(&self) -> Vec<(usize, BufferScope)> {
		let inner = self.lock();
		let mut held: Vec<_> =
			inner.parts.iter().filter(|p| !p.page.is_empty()).map(|p| (p.last_fill, p)).collect();
		held.sort_by_key(|(fill, _)| *fill);
		held.into_iter().map(|(_, p)| (p.program, p.scope)).collect()
	}

	/// Whether the buffer reports that an entry may be served.
	pub(super) fn is_live(&self) -> bool {
		self.live.load(Ordering::Relaxed)
	}

	/// Fills the partition of `program` and `scope` with one entry of `bytes` bytes, within
	/// `bound`, as a fill would, and covers it as `cover` says. For a test that needs no
	/// datastore.
	pub(super) fn put_for_test(
		&self,
		program: usize,
		scope: BufferScope,
		bytes: usize,
		bound: usize,
		cover: Cover,
	) {
		let mut part = self.begin(program, scope, 0, FrameClass::UNSET, bytes, bound);
		let _ = part.page.push(b"k", &vec![0u8; bytes.saturating_sub(1)]);
		part.index.entry(key_hash(b"k")).or_default().push(0);
		part.cover = cover;
		self.finish(part);
	}
}

#[cfg(test)]
mod no_static_tests {
	/// Whether `line` declares a `static` item, whatever its visibility.
	fn declares_static(line: &str) -> bool {
		let line = line.trim_start();
		let rest = match line.strip_prefix("pub") {
			Some(rest) => {
				let rest = rest.trim_start();
				match rest.strip_prefix('(') {
					Some(restriction) => {
						restriction.split_once(')').map_or(rest, |(_, after)| after)
					}
					None => rest,
				}
			}
			None => line,
		};
		rest.trim_start().starts_with("static ")
	}

	/// Nothing the read buffer or the IR executors hold stands in a static: no `static` item, no
	/// lazily initialised value and no thread-local. A `OnceLock` field of a per-run struct is a
	/// field, not an item, and is not matched. The needles are built from pieces so that this
	/// file does not match itself, and a comment may name them.
	#[test]
	fn buffer_holds_no_static() {
		let needles = [
			concat!("static", " mut"),
			concat!("Lazy", "Lock"),
			concat!("lazy", "_static!"),
			concat!("thread", "_local!"),
		];
		let sources = [
			("ir_host.rs", include_str!("ir_host.rs")),
			("ir_exec.rs", include_str!("ir_exec.rs")),
		];
		for (file, source) in sources {
			for (number, line) in source.lines().enumerate() {
				if line.trim_start().starts_with("//") {
					continue;
				}
				assert!(!declares_static(line), "{file}:{}: a static item: {line}", number + 1);
				for needle in needles {
					assert!(!line.contains(needle), "{file}:{}: `{needle}`: {line}", number + 1);
				}
			}
		}
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use std::ops::Bound;

	use super::*;
	use crate::exec::operators::fetch::fetch_raw_record;
	use crate::exec::operators::test_util::TestDb;
	use crate::val::RecordIdKeyRange;

	const PROGRAM: usize = 11;

	fn scope() -> BufferScope {
		BufferScope::Loop {
			site: 0,
		}
	}

	/// A look-ahead read of record ids, as a `FOR` loop over an array of them gives.
	fn read(max_positions: u32, max_bytes: u32) -> BatchRead {
		BatchRead {
			scope: scope(),
			ahead: LookAhead::Iter {
				slot: 0,
				elem: 0,
			},
			keys: Box::new([SiteKey::Elem]),
			max_positions,
			max_bytes,
		}
	}

	fn rid(table: &str, n: i64) -> RecordId {
		RecordId::new(table.into(), RecordIdKey::from(n))
	}

	fn ids(table: &str, n: i64) -> Vec<Value> {
		(0..n).map(|i| Value::RecordId(rid(table, i))).collect()
	}

	/// A database holding `t:0` to `t:{n - 1}`, each with the field `n`.
	async fn database(n: i64) -> TestDb {
		let db = TestDb::new("").await;
		let creates: String = (0..n).map(|i| format!("CREATE t:{i} SET n = {i};")).collect();
		db.run(&creates).await;
		db
	}

	fn ids_of(ctx: &ExecutionContext) -> (NamespaceId, DatabaseId) {
		let db_ctx = ctx.database().expect("a database context");
		(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id)
	}

	/// The transaction of `ctx`, holding the layouts of `t`: a fill admits each hit against its
	/// table's layouts, which the transaction reads once, so a count of a fill's reads starts after
	/// that read.
	async fn warmed(ctx: &ExecutionContext) -> Arc<Transaction> {
		let (ns, db) = ids_of(ctx);
		let txn = ctx.txn();
		txn.table_layouts(ns, db, &TableName::from("t"), None).await.expect("the layouts read");
		txn
	}

	/// What the datastore answers for `rid`.
	async fn stored(ctx: &ExecutionContext, rid: &RecordId) -> Option<Value> {
		fetch_raw_record(ctx, rid, None).await.ok().flatten()
	}

	/// One window of 50 record ids read with a window of 64 positions makes one read, and the
	/// buffer then serves each record as the datastore does.
	#[tokio::test]
	async fn fill_one_getm_per_window() {
		let db = database(50).await;
		let ctx = db.exec_ctx().await;
		let (ns, dbid) = ids_of(&ctx);
		let buffer = ReadBuffer::new();
		let txn = warmed(&ctx).await;
		let before = txn.metrics_snapshot_for_test();
		fill(&buffer, &read(64, 1 << 20), PROGRAM, &ids("t", 50), 49, &ctx)
			.await
			.expect("the read succeeds");
		let after = txn.metrics_snapshot_for_test();
		assert_eq!(after.ops_get - before.ops_get, 1, "one read for the whole window");
		assert_eq!(after.keys_read - before.keys_read, 50);
		assert!(buffer.is_live());
		// Every position of the window is covered: the iterator holds 49 elements, and the
		// current position stays covered until none is held.
		assert_eq!(
			buffer.cover_of(PROGRAM, scope()),
			Some(Cover::Remaining {
				stop: 0
			})
		);
		for i in 0..50 {
			let rid = rid("t", i);
			let served = serve_from(Some(&buffer), &ctx, ns, dbid, &rid, None);
			assert!(served.is_some(), "t:{i} is served");
			assert_eq!(served, stored(&ctx, &rid).await, "t:{i} equals the datastore's");
		}
		assert_eq!(buffer.forgotten(), 0);
	}

	/// A window of duplicates reads each key once.
	#[tokio::test]
	async fn fill_deduplicates_the_window() {
		let db = database(3).await;
		let ctx = db.exec_ctx().await;
		let buffer = ReadBuffer::new();
		let window = vec![
			Value::RecordId(rid("t", 1)),
			Value::RecordId(rid("t", 1)),
			Value::RecordId(rid("t", 2)),
			Value::RecordId(rid("t", 1)),
		];
		let txn = warmed(&ctx).await;
		let before = txn.metrics_snapshot_for_test();
		fill(&buffer, &read(8, 1 << 20), PROGRAM, &window, 3, &ctx)
			.await
			.expect("the read succeeds");
		let after = txn.metrics_snapshot_for_test();
		assert_eq!(after.keys_read - before.keys_read, 2);
		assert_eq!(buffer.page_of(PROGRAM, scope()).map(|(entries, _)| entries), Some(2));
	}

	/// Range ids, values that are not record ids and ids of a missing table or a missing record
	/// are read or skipped without an error: nothing is buffered for them, and what is buffered
	/// for the rest is served.
	#[tokio::test]
	async fn fill_raises_nothing() {
		let db = database(2).await;
		let ctx = db.exec_ctx().await;
		let (ns, dbid) = ids_of(&ctx);
		let buffer = ReadBuffer::new();
		let range = RecordIdKey::Range(Box::new(RecordIdKeyRange {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}));
		let window = vec![
			Value::RecordId(RecordId::new("t".into(), range)),
			Value::from(1_i64),
			Value::RecordId(rid("nowhere", 0)),
			Value::RecordId(rid("t", 99)),
			Value::RecordId(rid("t", 1)),
			Value::None,
		];
		fill(&buffer, &read(8, 1 << 20), PROGRAM, &window, 5, &ctx)
			.await
			.expect("the read succeeds");
		assert_eq!(buffer.page_of(PROGRAM, scope()).map(|(entries, _)| entries), Some(1));
		assert!(serve_from(Some(&buffer), &ctx, ns, dbid, &rid("t", 1), None).is_some());
		for missing in [rid("nowhere", 0), rid("t", 99)] {
			assert_eq!(serve_from(Some(&buffer), &ctx, ns, dbid, &missing, None), None);
		}
	}

	/// A fill in a context with no database reads nothing and buffers nothing.
	#[tokio::test]
	async fn fill_without_a_database_buffers_nothing() {
		let ctx = crate::exec::operators::test_util::root_ctx();
		let buffer = ReadBuffer::new();
		fill(&buffer, &read(8, 1 << 20), PROGRAM, &ids("t", 3), 2, &ctx)
			.await
			.expect("the read succeeds");
		assert!(!buffer.is_live());
		assert_eq!(buffer.cover_of(PROGRAM, scope()), None);
	}

	/// A record is served only to a consumer of the transaction and the frame the fill ran in.
	#[tokio::test]
	async fn served_respects_frame_and_txn() {
		let db = database(3).await;
		let ctx = db.exec_ctx().await;
		let (ns, dbid) = ids_of(&ctx);
		let buffer = ReadBuffer::new();
		fill(&buffer, &read(8, 1 << 20), PROGRAM, &ids("t", 3), 2, &ctx)
			.await
			.expect("the read succeeds");
		let target = rid("t", 1);
		assert!(serve_from(Some(&buffer), &ctx, ns, dbid, &target, None).is_some());
		let suppressed = ctx.with_perms_suppressed(true);
		assert_eq!(serve_from(Some(&buffer), &suppressed, ns, dbid, &target, None), None);
		let other = db.exec_ctx().await;
		assert_eq!(serve_from(Some(&buffer), &other, ns, dbid, &target, None), None);
	}

	/// A versioned read, and a context with no buffer, are answered before anything is encoded.
	#[tokio::test]
	async fn served_never_versioned() {
		let db = database(2).await;
		let ctx = db.exec_ctx().await;
		let (ns, dbid) = ids_of(&ctx);
		let buffer = ReadBuffer::new();
		fill(&buffer, &read(8, 1 << 20), PROGRAM, &ids("t", 2), 1, &ctx)
			.await
			.expect("the read succeeds");
		let target = rid("t", 1);
		assert!(serve_from(Some(&buffer), &ctx, ns, dbid, &target, None).is_some());
		assert_eq!(serve_from(Some(&buffer), &ctx, ns, dbid, &target, Some(1)), None);
		assert_eq!(serve_from(None, &ctx, ns, dbid, &target, None), None);
		assert_eq!(served_record(&ctx, ns, dbid, &target, None), None);
		assert_eq!(served_record(&ctx, ns, dbid, &target, Some(1)), None);
	}

	/// The encoded key of `rid`, as a write of it names it.
	fn encoded(ns: NamespaceId, db: DatabaseId, rid: &RecordId) -> Vec<u8> {
		let mut key = Vec::new();
		record_key(ns, db, &rid.table, &rid.key).encode_buffer(&mut key).expect("a key encodes");
		key
	}

	/// A forgotten key is no longer served and is counted once; a key that is not buffered is not
	/// counted.
	#[tokio::test]
	async fn forget_drops_and_counts() {
		let db = database(3).await;
		let ctx = db.exec_ctx().await;
		let (ns, dbid) = ids_of(&ctx);
		let buffer = ReadBuffer::new();
		fill(&buffer, &read(8, 1 << 20), PROGRAM, &ids("t", 3), 2, &ctx)
			.await
			.expect("the read succeeds");
		let target = rid("t", 1);
		buffer.drop_key(&encoded(ns, dbid, &target));
		assert_eq!(buffer.forgotten(), 1);
		assert_eq!(serve_from(Some(&buffer), &ctx, ns, dbid, &target, None), None);
		assert!(serve_from(Some(&buffer), &ctx, ns, dbid, &rid("t", 2), None).is_some());
		buffer.drop_key(&encoded(ns, dbid, &target));
		buffer.drop_key(&encoded(ns, dbid, &rid("t", 77)));
		assert_eq!(buffer.forgotten(), 1);
		// With no buffer in the context the hook returns at once.
		forget(&ctx, &encoded(ns, dbid, &target));
	}

	/// A partition holds at most `max_bytes` and one value beyond the first, and the positions
	/// from the first whose hit did not fit are left uncovered.
	#[tokio::test]
	async fn partition_is_bounded_by_max_bytes() {
		let db = database(50).await;
		let ctx = db.exec_ctx().await;
		let buffer = ReadBuffer::new();
		let max_bytes = 256;
		fill(&buffer, &read(64, max_bytes), PROGRAM, &ids("t", 50), 49, &ctx)
			.await
			.expect("the read succeeds");
		let (entries, capacity) = buffer.page_of(PROGRAM, scope()).expect("a partition");
		assert_eq!(capacity, max_bytes as usize);
		assert!((1..50).contains(&entries), "the page stopped short: {entries}");
		assert!(buffer.held_bytes() <= max_bytes as usize, "{}", buffer.held_bytes());
		match buffer.cover_of(PROGRAM, scope()) {
			Some(Cover::Remaining {
				stop,
			}) => assert!(stop > 0, "positions past the page stay uncovered"),
			other => panic!("a look-ahead partition covers by what is held, not {other:?}"),
		}
	}

	/// A fill whose read claims `u32::MAX` bytes takes at most the config's `scan_batch_bytes`,
	/// so the page another site filled stays held.
	#[tokio::test]
	async fn fill_claims_at_most_the_scan_batch() {
		let db = database(4).await;
		let ctx = db.exec_ctx().await;
		let share = ctx.root().ctx.config.exec.scan_batch_bytes;
		let other = BufferScope::Loop {
			site: 1,
		};
		let buffer = ReadBuffer::new();
		buffer.put_for_test(PROGRAM, other, 1024, READ_BUFFER_MAX_FACTOR * share, Cover::Filled);
		fill(&buffer, &read(1, u32::MAX), PROGRAM, &ids("t", 1), 0, &ctx)
			.await
			.expect("the read succeeds");
		let (_, capacity) = buffer.page_of(PROGRAM, scope()).expect("a partition");
		assert_eq!(capacity, share);
		assert_eq!(buffer.cover_of(PROGRAM, other), Some(Cover::Filled));
		assert_eq!(buffer.filled_by_age(), vec![(PROGRAM, other), (PROGRAM, scope())]);
	}

	/// Before a fill the pages of other partitions are cleared, least recently filled first,
	/// until the held bytes and the fill's own fit within the bound.
	#[test]
	fn buffer_bounded() {
		let buffer = ReadBuffer::new();
		let sites: Vec<BufferScope> = (0..3)
			.map(|site| BufferScope::Loop {
				site,
			})
			.collect();
		for site in &sites {
			buffer.put_for_test(PROGRAM, *site, 40, 100, Cover::Filled);
		}
		// The third page needed room for 40 more bytes beside 80: the first, filled earliest, went.
		assert_eq!(buffer.filled_by_age(), vec![(PROGRAM, sites[1]), (PROGRAM, sites[2])]);
		assert_eq!(buffer.held_bytes(), 80);
		assert_eq!(buffer.cover_of(PROGRAM, sites[0]), Some(Cover::Uncovered));
		// Refilling the second and then the first makes the second the least recently filled.
		buffer.put_for_test(PROGRAM, sites[1], 40, 100, Cover::Filled);
		buffer.put_for_test(PROGRAM, sites[0], 40, 100, Cover::Filled);
		assert_eq!(buffer.filled_by_age(), vec![(PROGRAM, sites[1]), (PROGRAM, sites[0])]);
		assert!(buffer.held_bytes() <= 100);
		// A page larger than the bound is taken alone.
		buffer.put_for_test(PROGRAM, sites[2], 400, 100, Cover::Filled);
		assert_eq!(buffer.filled_by_age(), vec![(PROGRAM, sites[2])]);
		assert_eq!(buffer.held_bytes(), 400);
	}
	fn resume_spec() -> WriteResumeSpec {
		WriteResumeSpec {
			cut: 7,
			skip: 9,
			live: SmallVec::from_slice(&[0, 1]),
		}
	}

	/// A candidate resumes at the cut or the skip its stop names; any other stop is refused.
	#[test]
	fn resume_target_follows_the_stop() {
		let spec = resume_spec();
		assert_eq!(resume_target(&spec, &Stop::Cut).unwrap(), 7);
		assert_eq!(resume_target(&spec, &Stop::Skip).unwrap(), 9);
		assert!(resume_target(&spec, &Stop::OffPath).is_err());
		let failed = Stop::Failed(ControlFlow::Err(anyhow::anyhow!("raised")));
		assert!(resume_target(&spec, &failed).is_err());
	}

	/// Each live value lands in its register, as the scalar kind it is; a register the row does
	/// not hold is an error.
	#[test]
	fn restore_live_writes_registers() {
		let mut regs: Vec<Scalar<'_>> = vec![Scalar::None, Scalar::None];
		let live: SmallVec<[(u16, Value); 8]> =
			SmallVec::from_vec(vec![(1, Value::Bool(true)), (0, Value::from("a"))]);
		restore_live(live, &mut regs).unwrap();
		assert!(matches!(regs[1], Scalar::Bool(true)));
		assert_eq!(regs[0].as_whole(), Some(&Value::from("a")));
		let live: SmallVec<[(u16, Value); 8]> = SmallVec::from_vec(vec![(2, Value::Null)]);
		assert!(restore_live(live, &mut regs).is_err());
	}
}
