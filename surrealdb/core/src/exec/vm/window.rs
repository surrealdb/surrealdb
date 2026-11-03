//! SQL window functions: each call appends one field to every row, computed
//! from the rows of the row's partition, with the rows handed on in the
//! order they arrived.
//!
//! The rows arrive sorted by their partition keys and then their order keys.
//! Two adjacent rows whose partition keys compare unequal begin a partition;
//! within one, two adjacent rows unequal on an order key under its
//! [`KeyOrder`] begin a peer group. Both facts are taken when a row arrives,
//! against the keys of the row before it, and kept as two flags on the row,
//! so the keys themselves are never held.
//!
//! Every call is answered with forward-only cursors over the rows of the
//! partition:
//!
//! - the emit cursor, at the row being answered;
//! - the peer cursor, ahead of it, finding where the row's peer group ends;
//! - per frame-reading call, a head cursor adding rows to the call's frame state as the frame's end
//!   advances, and a tail cursor taking them out as its start advances; for every supported frame
//!   both ends are non-decreasing in the row answered;
//! - per `lag`, `lead` and moving `nth_value`, one cursor at the row it reads.
//!
//! The partition's size is known once a row of the next partition has
//! arrived or the input is finished. A row is ready, and is answered, once
//! every call can be answered for it from the rows that have arrived.
//!
//! The rows between the slowest cursor that still reads and the last row to
//! arrive are held, each as its flags, its arguments and its row; the row
//! itself is released once it is handed on. What a call holds:
//!
//! | calls | rows held |
//! |---|---|
//! | `row_number`, `rank`, `dense_rank` | the row answered |
//! | `lag(k)` / `lead(k)` | `k` behind / ahead of it |
//! | frames ending at `CURRENT ROW` in `ROWS`, starting `UNBOUNDED` or `k PRECEDING` | none / `k` behind |
//! | frames ending `k FOLLOWING` | `k` ahead |
//! | a `RANGE` frame ending at `CURRENT ROW` | the row's peer group |
//! | `percent_rank`, `cume_dist`, `ntile`, frames ending `UNBOUNDED FOLLOWING` | the partition |
//!
//! Beside the rows, a call keeps its frame state: a running count or sum, the
//! extreme of a frame that only grows, or for `min` and `max` over a frame
//! whose start moves, a monotonic deque of at most the frame's rows. That
//! state, the cursors' open readers and the partition boundaries not yet
//! passed are charged to a pinned account, which insists when its lease is
//! short.
//!
//! The rows held are charged to the query's lease. When the lease refuses
//! them and the query has a temporary store, every row held in memory is
//! written, in arrival order, as one run, and the window keeps going. A
//! cursor behind the rows in memory reads the runs through its own
//! sequential reader, opened on the run it is in and dropped once it leaves
//! it, so at most `2 + 3 × calls` readers are open at once, one more while a
//! sum whose frame holds a float is summed afresh. A run is deleted once every
//! cursor is past it. With no store the rows stay in memory, held as bytes
//! the query cannot spill.
//!
//! `lag` and `lead` take their offset, `nth_value` its `n` and `ntile` its
//! bucket count from the first row of each partition. A later row whose own
//! argument differs fails: the cursors read at a fixed distance, and the
//! buckets are cut once per partition.

// Driven by the VM's window step.
#![allow(dead_code)]

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;

use surrealdb_strand::Strand;

use super::interp::compare_keys;
use super::program::KeyOrder;
use super::spill::write_run;
use crate::exec::memory::runs::{
	BreakerKind, Record, Run, RunOrder, RunSource, RunSpec, buffer_bound,
};
use crate::exec::memory::size::{value_bytes, values_bytes};
use crate::exec::memory::{Reservation, Verdict};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::expr::logical::{WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunction};
use crate::val::number::Wide;
use crate::val::{Number, TryAdd as _, TrySub as _, Value};

/// What a window computes.
#[derive(Debug, Clone)]
pub(crate) struct WindowSpec {
	/// Number of leading keys of each input that are partition keys; the rest are order keys.
	pub(crate) partitions: usize,
	/// How each order key compares, one per order key. As visible as
	/// [`KeyOrder`].
	pub(super) order: Vec<KeyOrder>,
	pub(crate) calls: Vec<CallSpec>,
}

/// One call of a window.
#[derive(Debug, Clone)]
pub(crate) struct CallSpec {
	/// The field the call's value is written to.
	pub(crate) output: Strand,
	pub(crate) function: WindowFunction,
	/// How many of the row's args this call owns, in call order.
	pub(crate) args: usize,
	/// The rows a frame-reading call reads; ignored by the others.
	pub(crate) frame: WindowFrame,
}

/// One row as the step hands it in. Rows arrive sorted by (partition keys
/// ascending, order keys per [`KeyOrder`]).
#[derive(Debug, Clone)]
pub(crate) struct WindowInput {
	pub(crate) keys: Vec<Value>,
	pub(crate) args: Vec<Value>,
	pub(crate) row: Value,
}

/// The flag of a row that begins a partition.
const PARTITION: u8 = 1;
/// The flag of a row that begins a peer group. A row that begins a
/// partition carries it too.
const PEER: u8 = 2;

fn err(e: anyhow::Error) -> ControlFlow {
	ControlFlow::Err(e)
}

fn internal(message: &str) -> ControlFlow {
	err(anyhow::anyhow!("Internal error: {message}"))
}

fn invalid(function: &str, message: &str) -> ControlFlow {
	err(anyhow::anyhow!(crate::expr::Error::InvalidFunctionArguments {
		name: function.to_owned(),
		message: message.to_owned(),
	}))
}

/// A row held: its flags, its arguments and, until it is handed on, the row.
#[derive(Debug, Clone)]
struct Entry {
	flags: u8,
	args: Vec<Value>,
	row: Value,
}

impl Entry {
	/// What the entry is charged.
	fn bytes(&self) -> usize {
		std::mem::size_of::<Self>() + values_bytes(&self.args) + value_bytes(&self.row)
	}

	/// The entry as a run's record: its flags, then its arguments, as keys.
	fn into_record(self) -> Record {
		let mut keys = Vec::with_capacity(1 + self.args.len());
		keys.push(Value::from(i64::from(self.flags)));
		keys.extend(self.args);
		(keys, self.row)
	}

	fn from_record((keys, row): Record) -> FlowResult<Self> {
		let mut keys = keys.into_iter();
		let flags = if let Some(Value::Number(Number::Int(flags))) = keys.next() {
			u8::try_from(flags).ok()
		} else {
			None
		}
		.ok_or_else(|| internal("a spilled window row has no flags"))?;
		Ok(Self {
			flags,
			args: keys.collect(),
			row,
		})
	}
}

/// A sequential reader over one run, at the row `next` it answers next.
struct Reader {
	/// The end of the run read, which names it among the window's runs.
	end: u64,
	next: u64,
	source: RunSource,
}

/// A position in the window's rows, with the reader it reads spilled rows
/// through and the spilled row it read last.
#[derive(Default)]
struct Cursor {
	pos: u64,
	reader: Option<Reader>,
	last: Option<(u64, Entry)>,
	/// Whether the cursor keeps the row of a spilled entry it reads: only
	/// the emit cursor hands rows on.
	rows: bool,
}

impl Cursor {
	fn at(pos: u64) -> Self {
		Self {
			pos,
			..Self::default()
		}
	}

	/// Drop what the cursor no longer reads: its reader once it is past the
	/// reader's run or the run is released, and a cached row before `floor`.
	fn forget(&mut self, floor: u64) {
		if self.reader.as_ref().is_some_and(|r| r.end <= self.pos.max(floor)) {
			self.reader = None;
		}
		if self.last.as_ref().is_some_and(|(at, _)| *at < floor) {
			self.last = None;
		}
	}

	/// The bytes the cursor holds beyond the window's rows.
	fn bytes(&self, reader_bytes: usize) -> usize {
		let reader = if self.reader.is_some() {
			reader_bytes
		} else {
			0
		};
		reader + self.last.as_ref().map_or(0, |(_, e)| e.bytes())
	}
}

/// Rows written out, holding the rows `start..end`.
struct Spilled {
	start: u64,
	end: u64,
	run: Run,
}

/// The rows held, by their place in the input: the rows from `first` to
/// `mem_start` in runs, the rest, up to `arrived`, in memory.
struct Buffer {
	account: Reservation,
	spec: RunSpec,
	first: u64,
	runs: VecDeque<Spilled>,
	mem_start: u64,
	mem: VecDeque<Entry>,
	/// What the rows in memory are charged.
	bytes: usize,
	arrived: u64,
	/// Runs written, for tests.
	written: usize,
}

impl Buffer {
	/// Hold `entry`, the next row. When the lease does not cover the rows in
	/// memory they are written as a run where the query can spill, and held
	/// as bytes it cannot spill where it cannot.
	async fn push(&mut self, entry: Entry, ctx: &ExecutionContext) -> FlowResult<()> {
		let bytes = entry.bytes();
		self.bytes += bytes;
		self.mem.push_back(entry);
		self.arrived += 1;
		if self.account.charge(bytes) == Verdict::Spill {
			match self.account.memory().filter(|m| m.can_spill()).cloned() {
				Some(memory) => {
					let rows = std::mem::take(&mut self.mem);
					let (start, end) = (self.mem_start, self.arrived);
					let records = rows.into_iter().map(Entry::into_record);
					let run = write_run(&memory, self.spec.clone(), records, ctx).await?;
					self.runs.push_back(Spilled {
						start,
						end,
						run,
					});
					self.written += 1;
					self.mem_start = end;
					self.bytes = 0;
					self.account.clear();
				}
				None => self.account.insist(ctx).await?,
			}
		}
		Ok(())
	}

	/// The row at `pos`, read through `cursor` where it is spilled.
	async fn entry<'a>(
		&'a self,
		cursor: &'a mut Cursor,
		pos: u64,
		ctx: &ExecutionContext,
	) -> FlowResult<&'a Entry> {
		if pos >= self.mem_start {
			let at = usize::try_from(pos - self.mem_start)
				.map_err(|_| internal("a window row is out of reach"))?;
			return self.mem.get(at).ok_or_else(|| internal("a window read a row not yet taken"));
		}
		if !matches!(&cursor.last, Some((at, _)) if *at == pos) {
			let mut entry = self.read_spilled(cursor, pos, ctx).await?;
			if !cursor.rows {
				entry.row = Value::None;
			}
			cursor.last = Some((pos, entry));
		}
		match &cursor.last {
			Some((_, entry)) => Ok(entry),
			None => Err(internal("a window cursor lost the row it read")),
		}
	}

	/// The spilled row at `pos`, read on from the cursor's reader where it is
	/// on that row's run and not past it, or from a reader opened on that run.
	async fn read_spilled(
		&self,
		cursor: &mut Cursor,
		pos: u64,
		ctx: &ExecutionContext,
	) -> FlowResult<Entry> {
		let at = self.runs.partition_point(|r| r.end <= pos);
		let spilled = match self.runs.get(at) {
			Some(spilled) if spilled.start <= pos && pos >= self.first => spilled,
			_ => return Err(internal("a window read a row it released")),
		};
		let reopen = match &cursor.reader {
			Some(reader) => reader.end != spilled.end || reader.next > pos,
			None => true,
		};
		if reopen {
			cursor.reader = None;
			cursor.reader = Some(Reader {
				end: spilled.end,
				next: spilled.start,
				source: spilled.run.read().await.map_err(err)?,
			});
		}
		let Some(reader) = cursor.reader.as_mut() else {
			return Err(internal("a window cursor has no reader"));
		};
		loop {
			let record = reader
				.source
				.next()
				.await
				.map_err(err)?
				.ok_or_else(|| internal("a spilled window run ended early"))?;
			let read = reader.next;
			reader.next += 1;
			if read == pos {
				return Entry::from_record(record);
			}
			super::fuel::tick(ctx, 1).await?;
		}
	}

	/// The row at `pos`, taken out of its entry: the row is handed on once.
	async fn take_row(
		&mut self,
		cursor: &mut Cursor,
		pos: u64,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		if pos >= self.mem_start {
			let at = usize::try_from(pos - self.mem_start)
				.map_err(|_| internal("a window row is out of reach"))?;
			let entry = self
				.mem
				.get_mut(at)
				.ok_or_else(|| internal("a window took a row not yet taken"))?;
			let row = std::mem::take(&mut entry.row);
			self.bytes -= value_bytes(&row) - value_bytes(&Value::None);
			self.account.settle(self.bytes);
			return Ok(row);
		}
		self.entry(cursor, pos, ctx).await?;
		match &mut cursor.last {
			Some((_, entry)) => Ok(std::mem::take(&mut entry.row)),
			None => Err(internal("a window cursor lost the row it read")),
		}
	}

	/// Release every row before `floor`: the rows in memory, and each run
	/// wholly before it. No reader may be open on a released run.
	fn release(&mut self, floor: u64) {
		let floor = floor.min(self.arrived);
		if floor <= self.first {
			return;
		}
		while self.runs.front().is_some_and(|r| r.end <= floor) {
			self.runs.pop_front();
		}
		while self.mem_start < floor {
			match self.mem.pop_front() {
				Some(entry) => self.bytes -= entry.bytes(),
				None => break,
			}
			self.mem_start += 1;
		}
		self.first = floor;
		self.account.settle(self.bytes);
	}

	fn clear(&mut self) {
		self.mem.clear();
		self.runs.clear();
		self.bytes = 0;
		self.first = self.arrived;
		self.mem_start = self.arrived;
		self.account.clear();
	}
}

/// A sum's state: the sum and count of the numbers in the frame, and how many
/// of them a subtraction cannot take back exactly. A sum taken back past such a
/// value is summed afresh from the frame when it is next read.
///
/// It takes its values as the `math::sum` and `math::mean` accumulators behind
/// the `sum` and `avg` aggregates do: a value that is not a number is skipped,
/// and a number is widened before it is added. A `sum` whose integer total
/// leaves `i64` is an error; an `avg`'s total (`exact_total`) carries on as a
/// decimal.
#[derive(Debug, Default)]
struct SumState {
	sum: Option<Number>,
	count: u64,
	inexact: u64,
	stale: bool,
	exact_total: bool,
}

impl SumState {
	fn new(exact_total: bool) -> Self {
		Self {
			exact_total,
			..Self::default()
		}
	}

	fn add(&mut self, value: &Value) -> FlowResult<()> {
		let Value::Number(n) = value else {
			return Ok(());
		};
		let n = n.widened();
		self.count += 1;
		if !exact(&n) {
			self.inexact += 1;
		}
		if !self.stale {
			self.sum = Some(match self.sum.take() {
				Some(sum) if self.exact_total => sum.add_exact(n).map_err(err)?,
				Some(sum) => sum.try_add(n).map_err(err)?,
				None => n,
			});
		}
		Ok(())
	}

	fn retract(&mut self, value: &Value) -> FlowResult<()> {
		let Value::Number(n) = value else {
			return Ok(());
		};
		let n = n.widened();
		self.count -= 1;
		let exact = exact(&n);
		if !exact {
			self.inexact -= 1;
		}
		if self.count == 0 {
			*self = Self::new(self.exact_total);
		} else if !exact || self.inexact > 0 {
			self.stale = true;
		} else if !self.stale {
			self.sum = match self.sum.take() {
				Some(sum) if self.exact_total => Some(sum.sub_exact(n).map_err(err)?),
				Some(sum) => Some(sum.try_sub(n).map_err(err)?),
				None => None,
			};
		}
		Ok(())
	}
}

/// Whether subtracting `n` takes back adding it exactly.
fn exact(n: &Number) -> bool {
	matches!(n.wide(), Wide::Int(_) | Wide::Decimal(_) | Wide::BigDecimal(_))
}

/// A frame-reading call's state over the rows its head has added and its
/// tail not yet taken out.
#[derive(Debug)]
enum Fold {
	/// A call that reads no frame.
	Frameless,
	/// `count()`: the frame's rows, read from its bounds.
	Rows,
	/// `count(value)`.
	Count(u64),
	/// `sum` and `avg`.
	Sum(SumState),
	/// `min` / `max` over a frame whose start never moves.
	Extreme(Option<Value>),
	/// `min` / `max` over a frame whose start moves: each row that could be
	/// the extreme of a later frame, with the values it holds.
	Deque(VecDeque<(u64, Value)>, usize),
	/// `first_value` over a frame starting at the partition: the partition's first value.
	First(Option<Value>),
	/// `first_value` over a frame whose start moves, read at the tail.
	PeekFirst,
	/// `last_value`: the value of the last row added.
	Last(Option<Value>),
	/// `nth_value` over a frame starting at the partition: the partition's `n`th value.
	Nth(Option<Value>),
	/// `nth_value` over a frame whose start moves, read `n - 1` rows past the tail.
	PeekNth,
	/// A call other than `count()` over a frame that is empty for every row.
	Never,
}

impl Fold {
	fn start(spec: &CallSpec) -> Self {
		let moving = spec.frame.start != WindowFrameBound::UnboundedPreceding;
		let never = spec.frame.start == WindowFrameBound::UnboundedFollowing
			|| spec.frame.end == WindowFrameBound::UnboundedPreceding;
		match spec.function {
			WindowFunction::Count if spec.args == 0 => Self::Rows,
			WindowFunction::FirstValue
			| WindowFunction::LastValue
			| WindowFunction::NthValue
			| WindowFunction::Sum
			| WindowFunction::Avg
			| WindowFunction::Count
			| WindowFunction::Min
			| WindowFunction::Max
				if never =>
			{
				Self::Never
			}
			WindowFunction::Count => Self::Count(0),
			WindowFunction::Sum => Self::Sum(SumState::new(false)),
			WindowFunction::Avg => Self::Sum(SumState::new(true)),
			WindowFunction::Min | WindowFunction::Max if moving => Self::Deque(VecDeque::new(), 0),
			WindowFunction::Min | WindowFunction::Max => Self::Extreme(None),
			WindowFunction::FirstValue if moving => Self::PeekFirst,
			WindowFunction::FirstValue => Self::First(None),
			WindowFunction::LastValue => Self::Last(None),
			WindowFunction::NthValue if moving => Self::PeekNth,
			WindowFunction::NthValue => Self::Nth(None),
			WindowFunction::RowNumber
			| WindowFunction::Rank
			| WindowFunction::DenseRank
			| WindowFunction::PercentRank
			| WindowFunction::CumeDist
			| WindowFunction::Ntile
			| WindowFunction::Lag
			| WindowFunction::Lead => Self::Frameless,
		}
	}

	/// Whether adding the row at `pos` reads its value. `nth` is the row
	/// `nth_value` keeps.
	fn reads_added(&self, pos: u64, partition: u64, nth: Option<u64>) -> bool {
		match self {
			Self::Count(_) | Self::Sum(_) | Self::Extreme(_) | Self::Deque(..) | Self::Last(_) => {
				true
			}
			Self::First(v) => v.is_none() && pos == partition,
			Self::Nth(v) => v.is_none() && nth == Some(pos),
			Self::Frameless | Self::Rows | Self::PeekFirst | Self::PeekNth | Self::Never => false,
		}
	}

	/// Whether the head reads any row it is still to add.
	fn head_reads(&self) -> bool {
		match self {
			Self::Count(_) | Self::Sum(_) | Self::Extreme(_) | Self::Deque(..) | Self::Last(_) => {
				true
			}
			Self::First(v) | Self::Nth(v) => v.is_none(),
			Self::Frameless | Self::Rows | Self::PeekFirst | Self::PeekNth | Self::Never => false,
		}
	}

	/// Whether taking a row out of a frame whose start moves reads its value.
	fn retract_reads(&self, moving: bool) -> bool {
		moving && matches!(self, Self::Count(_) | Self::Sum(_))
	}

	/// Whether the tail reads at its position: it takes rows out by their
	/// values, or `first_value` peeks there.
	fn tail_reads(&self, moving: bool) -> bool {
		self.retract_reads(moving) || matches!(self, Self::PeekFirst)
	}

	fn add(&mut self, pos: u64, value: Value, max: bool) -> FlowResult<()> {
		match self {
			Self::Count(n) => {
				if !value.is_nullish() {
					*n += 1;
				}
			}
			Self::Sum(state) => state.add(&value)?,
			Self::Extreme(best) => {
				if !value.is_nullish() {
					let better = match best {
						None => true,
						Some(b) => {
							value.cmp(b)
								== if max {
									Ordering::Greater
								} else {
									Ordering::Less
								}
						}
					};
					if better {
						*best = Some(value);
					}
				}
			}
			Self::Deque(deque, bytes) => {
				if !value.is_nullish() {
					// A row behind a later row at least as good is never the
					// extreme of a frame holding both.
					let beaten = if max {
						Ordering::Greater
					} else {
						Ordering::Less
					};
					while deque.back().is_some_and(|(_, b)| b.cmp(&value) != beaten) {
						if let Some((_, b)) = deque.pop_back() {
							*bytes -= value_bytes(&b);
						}
					}
					*bytes += value_bytes(&value);
					deque.push_back((pos, value));
				}
			}
			Self::First(v) | Self::Nth(v) | Self::Last(v) => *v = Some(value),
			Self::Frameless | Self::Rows | Self::PeekFirst | Self::PeekNth | Self::Never => {}
		}
		Ok(())
	}

	fn retract(&mut self, value: &Value) -> FlowResult<()> {
		match self {
			Self::Count(n) if !value.is_nullish() => *n -= 1,
			Self::Sum(state) => state.retract(value)?,
			Self::Count(_)
			| Self::Frameless
			| Self::Rows
			| Self::Extreme(_)
			| Self::Deque(..)
			| Self::First(_)
			| Self::PeekFirst
			| Self::Last(_)
			| Self::Nth(_)
			| Self::PeekNth
			| Self::Never => {}
		}
		Ok(())
	}

	/// What the fold holds beyond the window's rows.
	fn bytes(&self) -> usize {
		match self {
			Self::Sum(state) => {
				state.sum.as_ref().map_or(0, |sum| value_bytes(&Value::Number(sum.clone())))
			}
			Self::Extreme(v) | Self::First(v) | Self::Last(v) | Self::Nth(v) => {
				v.as_ref().map_or(0, value_bytes)
			}
			Self::Deque(deque, bytes) => bytes + deque.len() * std::mem::size_of::<u64>(),
			Self::Frameless
			| Self::Rows
			| Self::Count(_)
			| Self::PeekFirst
			| Self::PeekNth
			| Self::Never => 0,
		}
	}
}

/// One call's state within the partition being answered.
struct CallRun {
	/// The index of the call's first argument in a row's arguments.
	offset: usize,
	/// Whether the call's frame start moves within a partition.
	moving: bool,
	/// The frame's end: the next row to add.
	head: Cursor,
	/// The frame's start: the next row to take out.
	tail: Cursor,
	/// The row `lag`, `lead` or a moving `nth_value` reads.
	aux: Cursor,
	/// The partition's offset (signed: a negative one reads the other way),
	/// `n` or bucket count; `None` where the partition's first row gives
	/// `NULL`.
	param: Option<i64>,
	fold: Fold,
}

/// A window's rows and the state answering them.
pub(crate) struct WindowRun {
	spec: Arc<WindowSpec>,
	/// Why the spec cannot be run, raised on the first row.
	refused: Option<&'static str>,
	buffer: Buffer,
	/// Charges the call states, the open readers and the boundaries held.
	side: Reservation,
	reader_bytes: usize,
	finished: bool,
	/// The keys of the last row to arrive.
	last_keys: Option<Vec<Value>>,
	/// The starts of the partitions not yet passed, the current one first.
	bounds: VecDeque<u64>,
	/// The row being answered.
	emit: Cursor,
	/// The row the emit cursor has taken the flags of.
	entered: Option<u64>,
	/// Where the peer group of the row answered ends, once found.
	peer: Cursor,
	peer_end: Option<u64>,
	partition: u64,
	group: u64,
	dense: u64,
	/// Whether a call reads where the row's peer group ends.
	needs_peer_end: bool,
	calls: Vec<CallRun>,
}

impl WindowRun {
	/// A window before any row, charging `ctx`'s query where it has one.
	pub(crate) fn new(spec: Arc<WindowSpec>, ctx: &ExecutionContext) -> Self {
		let memory = ctx.ctx().memory();
		let (account, side) = match memory {
			Some(memory) => (memory.reservation(), memory.pinned_reservation()),
			None => (Reservation::unbounded(), Reservation::unbounded()),
		};
		let reader_bytes =
			memory.and_then(|m| m.store()).map_or(0, |store| buffer_bound(store.part_size()));
		let mut offset = 0;
		let mut refused = None;
		let mut needs_peer_end = false;
		let calls = spec
			.calls
			.iter()
			.map(|call| {
				let frame = call.frame;
				let fold = Fold::start(call);
				if !matches!(fold, Fold::Frameless) {
					if frame.units == WindowFrameUnits::Range
						&& [frame.start, frame.end].iter().any(|b| {
							matches!(
								b,
								WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
							)
						}) {
						refused = Some("a RANGE window frame has a numeric offset");
					}
					if frame.units == WindowFrameUnits::Range
						&& frame.end == WindowFrameBound::CurrentRow
					{
						needs_peer_end = true;
					}
				}
				if call.function == WindowFunction::CumeDist {
					needs_peer_end = true;
				}
				let wants = match call.function {
					WindowFunction::Ntile | WindowFunction::Lag | WindowFunction::Lead => 1,
					WindowFunction::NthValue => 2,
					WindowFunction::FirstValue
					| WindowFunction::LastValue
					| WindowFunction::Sum
					| WindowFunction::Avg
					| WindowFunction::Min
					| WindowFunction::Max => 1,
					WindowFunction::RowNumber
					| WindowFunction::Rank
					| WindowFunction::DenseRank
					| WindowFunction::PercentRank
					| WindowFunction::CumeDist
					| WindowFunction::Count => 0,
				};
				if call.args < wants {
					refused = Some("a window call has too few arguments");
				}
				let run = CallRun {
					offset,
					moving: frame.start != WindowFrameBound::UnboundedPreceding,
					head: Cursor::default(),
					tail: Cursor::default(),
					aux: Cursor::default(),
					param: None,
					fold,
				};
				offset += call.args;
				run
			})
			.collect();
		let spec_args = offset;
		Self {
			buffer: Buffer {
				account,
				spec: RunSpec::new(BreakerKind::WindowBuffer, 0, 1 + spec_args, RunOrder::Plain),
				first: 0,
				runs: VecDeque::new(),
				mem_start: 0,
				mem: VecDeque::new(),
				bytes: 0,
				arrived: 0,
				written: 0,
			},
			spec,
			refused,
			side,
			reader_bytes,
			finished: false,
			last_keys: None,
			bounds: VecDeque::new(),
			emit: Cursor {
				rows: true,
				..Cursor::default()
			},
			entered: None,
			peer: Cursor::default(),
			peer_end: None,
			partition: 0,
			group: 0,
			dense: 0,
			needs_peer_end,
			calls,
		}
	}

	/// Takes the next row.
	pub(crate) async fn push(
		&mut self,
		input: WindowInput,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		if let Some(refused) = self.refused {
			return Err(internal(refused));
		}
		if self.finished {
			return Err(internal("a window took a row after its input finished"));
		}
		super::fuel::tick(ctx, 1).await?;
		let WindowInput {
			keys,
			args,
			row,
		} = input;
		let partitions = self.spec.partitions;
		if keys.len() != partitions + self.spec.order.len()
			|| args.len() + 1 != self.buffer.spec.arity as usize
		{
			return Err(internal("a window row has the wrong number of keys or arguments"));
		}
		// The keys are evaluated again over rows the sort beneath ordered by its own evaluation of
		// them. A row whose keys come before its predecessor's (a key that answers differently
		// each time it is evaluated) would split a partition or a peer group, so it is an error.
		let flags = match &self.last_keys {
			None => PARTITION | PEER,
			Some(last) => {
				let partition = last[..partitions]
					.iter()
					.zip(&keys[..partitions])
					.map(|(a, b)| a.cmp(b))
					.find(|order| order.is_ne())
					.unwrap_or(Ordering::Equal);
				let order = match partition {
					Ordering::Equal => {
						compare_keys(&last[partitions..], &keys[partitions..], &self.spec.order)
					}
					Ordering::Less | Ordering::Greater => partition,
				};
				match (partition, order) {
					(_, Ordering::Greater) => {
						return Err(err(anyhow::anyhow!(
							"A window's PARTITION BY or ORDER BY key answered a different value \
							 when the rows were ordered than when the window read them"
						)));
					}
					(Ordering::Less, _) => PARTITION | PEER,
					(_, Ordering::Less) => PEER,
					(_, Ordering::Equal) => 0,
				}
			}
		};
		if flags & PARTITION != 0 {
			self.bounds.push_back(self.buffer.arrived);
		}
		self.last_keys = Some(keys);
		self.buffer
			.push(
				Entry {
					flags,
					args,
					row,
				},
				ctx,
			)
			.await?;
		self.settle_side(ctx).await
	}

	/// No more rows: every held row becomes ready.
	pub(crate) async fn finish(&mut self, _ctx: &ExecutionContext) -> FlowResult<()> {
		self.finished = true;
		Ok(())
	}

	/// Up to `most` ready rows, oldest first, each the input row with every
	/// call's output field set. Empty when none is ready.
	pub(crate) async fn drain(
		&mut self,
		most: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<Vec<Value>> {
		let mut out = Vec::new();
		while out.len() < most {
			let e = self.emit.pos;
			if e >= self.buffer.arrived {
				break;
			}
			if self.entered != Some(e) {
				self.enter(e, ctx).await?;
			}
			if !self.ready(e, ctx).await? {
				break;
			}
			let mut values = Vec::with_capacity(self.calls.len());
			for at in 0..self.calls.len() {
				values.push(self.answer(at, e, ctx).await?);
			}
			let mut row = self.buffer.take_row(&mut self.emit, e, ctx).await?;
			let Value::Object(object) = &mut row else {
				return Err(internal("a window row is not an object"));
			};
			for (call, value) in self.spec.calls.iter().zip(values) {
				if object.insert(call.output.clone(), value).is_some() {
					return Err(err(anyhow::anyhow!(
						"A row holds a field named `{}`, the name a window function is computed into",
						call.output
					)));
				}
			}
			out.push(row);
			self.passed(e + 1);
			self.settle_side(ctx).await?;
			super::fuel::tick(ctx, 1).await?;
		}
		Ok(out)
	}

	/// Releases every buffer, run and lease charge.
	pub(crate) fn clear(&mut self) {
		for cursor in self.cursors_mut() {
			cursor.reader = None;
			cursor.last = None;
		}
		for call in &mut self.calls {
			call.fold = Fold::Frameless;
		}
		self.buffer.clear();
		self.emit.pos = self.buffer.arrived;
		self.bounds.clear();
		self.last_keys = None;
		self.side.clear();
	}

	/// The rows held, in memory or spilled.
	pub(crate) fn held_rows(&self) -> u64 {
		self.buffer.arrived - self.buffer.first
	}

	fn cursors_mut(&mut self) -> impl Iterator<Item = &mut Cursor> {
		[&mut self.emit, &mut self.peer]
			.into_iter()
			.chain(self.calls.iter_mut().flat_map(|c| [&mut c.head, &mut c.tail, &mut c.aux]))
	}

	/// Where the partition being answered ends, once known.
	fn partition_end(&self) -> Option<u64> {
		match self.bounds.get(1) {
			Some(end) => Some(*end),
			None if self.finished => Some(self.buffer.arrived),
			None => None,
		}
	}

	/// Take the flags of row `e`: where it begins a partition, start every
	/// call afresh with the parameters of its first row, and elsewhere check
	/// that the row's parameters are those; where it begins a peer group,
	/// start the group.
	async fn enter(&mut self, e: u64, ctx: &ExecutionContext) -> FlowResult<()> {
		let entry = self.buffer.entry(&mut self.emit, e, ctx).await?;
		let flags = entry.flags;
		if flags & PARTITION != 0 {
			let args = entry.args.clone();
			while self.bounds.front().is_some_and(|b| *b < e) {
				self.bounds.pop_front();
			}
			self.partition = e;
			self.dense = 0;
			self.peer.pos = e;
			for (call, spec) in self.calls.iter_mut().zip(&self.spec.calls) {
				call.head.pos = e;
				call.tail.pos = e;
				call.aux.pos = e;
				call.head.last = None;
				call.tail.last = None;
				call.aux.last = None;
				call.fold = Fold::start(spec);
				call.param = param(spec, &args[call.offset..call.offset + spec.args])?;
			}
		} else {
			for (call, spec) in self.calls.iter().zip(&self.spec.calls) {
				if param(spec, &entry.args[call.offset..call.offset + spec.args])? != call.param {
					return Err(invalid(
						name(spec.function),
						"The argument must be the same for every row of a partition",
					));
				}
			}
		}
		if flags & (PARTITION | PEER) != 0 {
			self.group = e;
			self.dense += 1;
			self.peer_end = None;
		}
		self.entered = Some(e);
		Ok(())
	}

	/// Whether every call can answer row `e` from the rows that have arrived.
	async fn ready(&mut self, e: u64, ctx: &ExecutionContext) -> FlowResult<bool> {
		let end = self.partition_end();
		let arrived = self.buffer.arrived;
		// Whether the rows up to and including `last` are known: arrived, or
		// past the partition's end.
		let reaches = |last: u64| last < arrived || end.is_some();
		if self.needs_peer_end && !self.find_peer_end(e, ctx).await? {
			return Ok(false);
		}
		for (call, spec) in self.calls.iter().zip(&self.spec.calls) {
			let ready = match spec.function {
				WindowFunction::PercentRank | WindowFunction::CumeDist | WindowFunction::Ntile => {
					end.is_some()
				}
				WindowFunction::Lag | WindowFunction::Lead => match call.param {
					Some(k) => match shift(spec.function, k) {
						(true, by) => reaches(e.saturating_add(by)),
						(false, _) => true,
					},
					None => true,
				},
				WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
					true
				}
				WindowFunction::FirstValue
				| WindowFunction::LastValue
				| WindowFunction::NthValue
				| WindowFunction::Sum
				| WindowFunction::Avg
				| WindowFunction::Count
				| WindowFunction::Min
				| WindowFunction::Max => match spec.frame.end {
					WindowFrameBound::Following(k) => reaches(e.saturating_add(k)),
					WindowFrameBound::UnboundedFollowing => end.is_some(),
					WindowFrameBound::UnboundedPreceding
					| WindowFrameBound::Preceding(_)
					| WindowFrameBound::CurrentRow => true,
				},
			};
			if !ready {
				return Ok(false);
			}
		}
		Ok(true)
	}

	/// Move the peer cursor to where row `e`'s peer group ends, answering
	/// whether that is known.
	async fn find_peer_end(&mut self, e: u64, ctx: &ExecutionContext) -> FlowResult<bool> {
		if self.peer_end.is_some() {
			return Ok(true);
		}
		self.peer.pos = self.peer.pos.max(e + 1);
		loop {
			let p = self.peer.pos;
			if p >= self.buffer.arrived {
				if self.finished {
					self.peer_end = Some(p);
					return Ok(true);
				}
				return Ok(false);
			}
			let flags = self.buffer.entry(&mut self.peer, p, ctx).await?.flags;
			if flags & (PARTITION | PEER) != 0 {
				self.peer_end = Some(p);
				return Ok(true);
			}
			self.peer.pos += 1;
			super::fuel::tick(ctx, 1).await?;
		}
	}

	/// Call `at`'s value for row `e`, which is ready.
	async fn answer(&mut self, at: usize, e: u64, ctx: &ExecutionContext) -> FlowResult<Value> {
		let spec = &self.spec.calls[at];
		let partition = self.partition;
		let end = self.partition_end();
		let size = |end: Option<u64>| {
			end.map(|end| end - partition).ok_or_else(|| internal("a window partition has no end"))
		};
		Ok(match spec.function {
			WindowFunction::RowNumber => count_value(e - partition + 1),
			WindowFunction::Rank => count_value(self.group - partition + 1),
			WindowFunction::DenseRank => count_value(self.dense),
			WindowFunction::PercentRank => {
				let size = size(end)?;
				if size <= 1 {
					Value::from(0.0)
				} else {
					Value::from((self.group - partition) as f64 / (size - 1) as f64)
				}
			}
			WindowFunction::CumeDist => {
				let peer_end =
					self.peer_end.ok_or_else(|| internal("a window peer group has no end"))?;
				Value::from((peer_end - partition) as f64 / size(end)? as f64)
			}
			WindowFunction::Ntile => match self.calls[at].param {
				Some(buckets) => {
					count_value(bucket(e - partition, size(end)?, buckets.unsigned_abs()))
				}
				None => Value::Null,
			},
			WindowFunction::Lag | WindowFunction::Lead => self.shifted(at, e, ctx).await?,
			WindowFunction::FirstValue
			| WindowFunction::LastValue
			| WindowFunction::NthValue
			| WindowFunction::Sum
			| WindowFunction::Avg
			| WindowFunction::Count
			| WindowFunction::Min
			| WindowFunction::Max => self.framed(at, e, ctx).await?,
		})
	}

	/// `lag` or `lead` for row `e`.
	async fn shifted(&mut self, at: usize, e: u64, ctx: &ExecutionContext) -> FlowResult<Value> {
		let spec = &self.spec.calls[at];
		let offset = self.calls[at].offset;
		let own = self.buffer.entry(&mut self.emit, e, ctx).await?;
		let own = &own.args[offset..offset + spec.args];
		let fixed = self.calls[at].param;
		if param(spec, own)? != fixed {
			return Err(invalid(
				name(spec.function),
				"The offset must be the same for every row of a partition",
			));
		}
		let Some(k) = fixed else {
			return Ok(Value::Null);
		};
		if k == 0 {
			return Ok(own[0].clone());
		}
		let (ahead, k) = shift(spec.function, k);
		let default = own.get(2).cloned().unwrap_or(Value::Null);
		let target = if ahead {
			let target = e.saturating_add(k);
			match self.partition_end() {
				Some(end) if target >= end => None,
				_ => Some(target),
			}
		} else {
			e.checked_sub(k).filter(|t| *t >= self.partition)
		};
		let Some(target) = target else {
			return Ok(default);
		};
		let call = &mut self.calls[at];
		call.aux.pos = target;
		Ok(self.buffer.entry(&mut call.aux, target, ctx).await?.args[offset].clone())
	}

	/// A frame-reading call's value for row `e`: its frame moved to row
	/// `e`'s, then read.
	async fn framed(&mut self, at: usize, e: u64, ctx: &ExecutionContext) -> FlowResult<Value> {
		let spec = &self.spec.calls[at];
		let partition = self.partition;
		let end = self.partition_end();
		let frame = spec.frame;
		let range = frame.units == WindowFrameUnits::Range;
		let start = match frame.start {
			WindowFrameBound::UnboundedPreceding => partition,
			WindowFrameBound::Preceding(k) => e.saturating_sub(k).max(partition),
			WindowFrameBound::CurrentRow if range => self.group,
			WindowFrameBound::CurrentRow => e,
			WindowFrameBound::Following(k) => e.saturating_add(k),
			WindowFrameBound::UnboundedFollowing => u64::MAX,
		};
		let stop = match frame.end {
			WindowFrameBound::UnboundedPreceding => partition,
			WindowFrameBound::Preceding(k) => (e + 1).saturating_sub(k).max(partition),
			WindowFrameBound::CurrentRow if range => {
				self.peer_end.ok_or_else(|| internal("a window peer group has no end"))?
			}
			WindowFrameBound::CurrentRow => e + 1,
			WindowFrameBound::Following(k) => {
				e.saturating_add(k).saturating_add(1).min(end.unwrap_or(u64::MAX))
			}
			WindowFrameBound::UnboundedFollowing => {
				end.ok_or_else(|| internal("a window partition has no end"))?
			}
		};
		let max = spec.function == WindowFunction::Max;
		let call = &mut self.calls[at];
		let offset = call.offset;
		let buffer = &self.buffer;
		// Take out the rows before the frame's start, then add those up to its end.
		let retract_reads = call.fold.retract_reads(call.moving);
		let until = start.min(call.head.pos);
		if retract_reads {
			while call.tail.pos < until {
				let pos = call.tail.pos;
				call.fold.retract(&buffer.entry(&mut call.tail, pos, ctx).await?.args[offset])?;
				call.tail.pos += 1;
				super::fuel::tick(ctx, 1).await?;
			}
		}
		call.tail.pos = call.tail.pos.max(start);
		call.head.pos = call.head.pos.max(call.tail.pos);
		if let Fold::Deque(deque, bytes) = &mut call.fold {
			while deque.front().is_some_and(|(pos, _)| *pos < start) {
				if let Some((_, v)) = deque.pop_front() {
					*bytes -= value_bytes(&v);
				}
			}
		}
		let nth = call.param.map(|n| partition.saturating_add(n.unsigned_abs() - 1));
		if call.fold.head_reads() {
			while call.head.pos < stop {
				let pos = call.head.pos;
				if call.fold.reads_added(pos, partition, nth) {
					let value = buffer.entry(&mut call.head, pos, ctx).await?.args[offset].clone();
					call.fold.add(pos, value, max)?;
					super::fuel::tick(ctx, 1).await?;
				}
				call.head.pos += 1;
			}
		}
		call.head.pos = call.head.pos.max(stop);
		if start >= stop {
			return Ok(if spec.function == WindowFunction::Count {
				count_value(0)
			} else {
				Value::Null
			});
		}
		Ok(match &mut call.fold {
			Fold::Rows => count_value(stop - start),
			Fold::Count(n) => count_value(*n),
			Fold::Sum(state) => {
				if state.stale {
					let mut fresh = SumState::new(state.exact_total);
					let mut scan = Cursor::at(call.tail.pos);
					for pos in call.tail.pos..call.head.pos {
						fresh.add(&buffer.entry(&mut scan, pos, ctx).await?.args[offset])?;
						super::fuel::tick(ctx, 1).await?;
					}
					*state = fresh;
				}
				match (&state.sum, spec.function) {
					(None, _) => Value::Null,
					(Some(sum), WindowFunction::Avg) => mean(sum, state.count)?,
					(Some(sum), _) => Value::Number(sum.clone()),
				}
			}
			Fold::Extreme(v) | Fold::First(v) | Fold::Last(v) => v.clone().unwrap_or(Value::Null),
			Fold::Deque(deque, _) => deque.front().map_or(Value::Null, |(_, v)| v.clone()),
			Fold::Nth(v) => match nth {
				Some(nth) if nth < stop => v.clone().unwrap_or(Value::Null),
				_ => Value::Null,
			},
			Fold::PeekFirst => buffer.entry(&mut call.tail, start, ctx).await?.args[offset].clone(),
			Fold::PeekNth => match call.param.map(i64::unsigned_abs) {
				Some(n) if start.saturating_add(n - 1) < stop => {
					let target = start + (n - 1);
					call.aux.pos = target;
					buffer.entry(&mut call.aux, target, ctx).await?.args[offset].clone()
				}
				_ => Value::Null,
			},
			Fold::Never => Value::Null,
			Fold::Frameless => return Err(internal("a frameless window call read a frame")),
		})
	}

	/// The emit cursor has passed every row before `next`: release each row
	/// no cursor reads again.
	fn passed(&mut self, next: u64) {
		self.emit.pos = next;
		let partition_ends = self.partition_end() == Some(next);
		let mut floor = next;
		if !partition_ends {
			let partition = self.partition;
			for (call, spec) in self.calls.iter().zip(&self.spec.calls) {
				if call.fold.head_reads() {
					floor = floor.min(call.head.pos);
				}
				if call.fold.tail_reads(call.moving) {
					floor = floor.min(call.tail.pos);
				}
				match (spec.function, call.param) {
					(WindowFunction::Lag | WindowFunction::Lead, Some(k)) => {
						if let (false, by) = shift(spec.function, k) {
							floor = floor.min(next.saturating_sub(by).max(partition));
						}
					}
					(WindowFunction::NthValue, Some(n)) if call.moving => {
						floor = floor.min(call.tail.pos.saturating_add(n.unsigned_abs() - 1));
					}
					_ => {}
				}
			}
		}
		for cursor in self.cursors_mut() {
			cursor.forget(floor);
		}
		self.buffer.release(floor);
	}

	/// Charge what the window holds beside its rows: the call states, the
	/// cursors' readers and cached rows, the last row's keys and the
	/// partition boundaries.
	async fn settle_side(&mut self, ctx: &ExecutionContext) -> FlowResult<()> {
		let reader_bytes = self.reader_bytes;
		let mut total = self.bounds.len() * std::mem::size_of::<u64>()
			+ self.last_keys.as_deref().map_or(0, values_bytes);
		total += self.emit.bytes(reader_bytes) + self.peer.bytes(reader_bytes);
		for call in &self.calls {
			total += call.fold.bytes()
				+ call.head.bytes(reader_bytes)
				+ call.tail.bytes(reader_bytes)
				+ call.aux.bytes(reader_bytes);
		}
		if self.side.settle(total) == Verdict::Spill {
			self.side.insist(ctx).await?;
		}
		Ok(())
	}

	/// The runs the window has written, for tests.
	#[cfg(test)]
	fn runs_written(&self) -> usize {
		self.buffer.written
	}
}

/// The function's name, for its errors.
fn name(function: WindowFunction) -> &'static str {
	match function {
		WindowFunction::Lag => "lag",
		WindowFunction::Lead => "lead",
		WindowFunction::Ntile => "ntile",
		WindowFunction::NthValue => "nth_value",
		WindowFunction::RowNumber
		| WindowFunction::Rank
		| WindowFunction::DenseRank
		| WindowFunction::PercentRank
		| WindowFunction::CumeDist
		| WindowFunction::FirstValue
		| WindowFunction::LastValue
		| WindowFunction::Sum
		| WindowFunction::Avg
		| WindowFunction::Count
		| WindowFunction::Min
		| WindowFunction::Max => "window",
	}
}

/// The integer a call reads from `args`, its own: the offset of `lag` and
/// `lead` (any integer, `1` when absent; a negative one reads the other way),
/// the `n` of `nth_value` and the
/// bucket count of `ntile` (positive). `None` for `NULL`; an error for any
/// other value.
fn param(spec: &CallSpec, args: &[Value]) -> FlowResult<Option<i64>> {
	let (value, least) = match spec.function {
		WindowFunction::Lag | WindowFunction::Lead => match args.get(1) {
			Some(value) => (value, i64::MIN),
			None => return Ok(Some(1)),
		},
		WindowFunction::NthValue => (args.get(1).unwrap_or(&Value::Null), 1),
		WindowFunction::Ntile => (args.first().unwrap_or(&Value::Null), 1),
		WindowFunction::RowNumber
		| WindowFunction::Rank
		| WindowFunction::DenseRank
		| WindowFunction::PercentRank
		| WindowFunction::CumeDist
		| WindowFunction::FirstValue
		| WindowFunction::LastValue
		| WindowFunction::Sum
		| WindowFunction::Avg
		| WindowFunction::Count
		| WindowFunction::Min
		| WindowFunction::Max => return Ok(None),
	};
	if value.is_nullish() {
		return Ok(None);
	}
	let int = if let Value::Number(n) = value
		&& let Wide::Int(i) = n.wide()
	{
		Some(i).filter(|i| *i >= least)
	} else {
		None
	};
	int.map(Some).ok_or_else(|| {
		invalid(
			name(spec.function),
			match least {
				i64::MIN => "The offset must be an integer",
				_ => "The argument must be a positive integer",
			},
		)
	})
}

/// Whether `lag` or `lead` (`function`) with offset `k` reads a row after
/// the current one, and how many rows away: a negative offset reads the
/// other way.
fn shift(function: WindowFunction, k: i64) -> (bool, u64) {
	((function == WindowFunction::Lead) == (k > 0), k.unsigned_abs())
}

/// The 1-based bucket of the row at `index` among `size` rows in `buckets`
/// buckets, the first `size % buckets` one row larger.
fn bucket(index: u64, size: u64, buckets: u64) -> u64 {
	let per = size / buckets;
	let larger = size % buckets;
	let in_larger = larger * (per + 1);
	if index < in_larger {
		index / (per + 1) + 1
	} else {
		larger + (index - in_larger) / per + 1
	}
}

/// The mean of `count` values whose sum is `sum`, as the `math::mean` aggregate answers: a float,
/// whatever the values' kind, so integers do not divide as integers, except that a sum which
/// reached a bigdecimal has an exact mean, a bigdecimal rounded half away from zero.
fn mean(sum: &Number, count: u64) -> FlowResult<Value> {
	Ok(Value::Number(if let Number::BigDecimal(sum) = sum {
		let count = surrealdb_types::BigDecimal::from(count);
		Number::BigDecimal(Box::new(sum.div_default(&count).map_err(|e| err(e.into()))?))
	} else {
		Number::Float(sum.to_float() / count as f64)
	}))
}

fn count_value(n: u64) -> Value {
	Value::from(i64::try_from(n).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use WindowFrameBound::{
		CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding,
	};
	use WindowFunction as F;

	use super::*;
	use crate::ctx::Context;
	use crate::exec::memory::{
		FairGovernor, MemTempStore, MemoryHost, MemorySettings, QueryMemory, TempStore,
	};
	use crate::exec::operators::test_util::root_ctx;
	use crate::val::Object;

	fn rows(start: WindowFrameBound, end: WindowFrameBound) -> WindowFrame {
		WindowFrame {
			units: WindowFrameUnits::Rows,
			start,
			end,
		}
	}

	fn range(start: WindowFrameBound, end: WindowFrameBound) -> WindowFrame {
		WindowFrame {
			units: WindowFrameUnits::Range,
			start,
			end,
		}
	}

	/// The frame SQL gives a call with an `ORDER BY`.
	fn default_frame() -> WindowFrame {
		range(UnboundedPreceding, CurrentRow)
	}

	fn call(output: &str, function: WindowFunction, args: usize, frame: WindowFrame) -> CallSpec {
		CallSpec {
			output: Strand::new(output),
			function,
			args,
			frame,
		}
	}

	/// A spec over one partition key (where `partitioned`) and one ascending
	/// order key (where `ordered`).
	fn spec(partitioned: bool, ordered: bool, calls: Vec<CallSpec>) -> Arc<WindowSpec> {
		let asc = KeyOrder {
			ascending: true,
			collate: false,
			numeric: false,
		};
		Arc::new(WindowSpec {
			partitions: usize::from(partitioned),
			order: if ordered {
				vec![asc]
			} else {
				Vec::new()
			},
			calls,
		})
	}

	/// A row object `{ id }`.
	fn row(id: usize) -> Value {
		let mut object = Object::default();
		object.insert(Strand::new("id"), Value::from(id as i64));
		Value::Object(object)
	}

	/// Inputs for `spec`, the `i`th with id `i`, partition `parts[i]`, order
	/// key `keys[i]` and the args `args(i)` gives.
	fn inputs(
		spec: &WindowSpec,
		parts: &[i64],
		keys: &[i64],
		args: impl Fn(usize) -> Vec<Value>,
	) -> Vec<WindowInput> {
		(0..keys.len())
			.map(|i| {
				let mut k = Vec::new();
				if spec.partitions == 1 {
					k.push(Value::from(parts[i]));
				}
				if !spec.order.is_empty() {
					k.push(Value::from(keys[i]));
				}
				WindowInput {
					keys: k,
					args: args(i),
					row: row(i),
				}
			})
			.collect()
	}

	fn ctx_over(memory: Option<Arc<QueryMemory>>) -> ExecutionContext {
		let ExecutionContext::Root(mut root) = root_ctx() else {
			unreachable!("root_ctx builds a root context")
		};
		let mut ctx = Context::new_test();
		ctx.set_memory(memory);
		root.ctx = ctx.freeze();
		ExecutionContext::Root(root)
	}

	/// A query memory with a store of `part`-byte parts, whose lease covers
	/// `floor` bytes and never grows.
	fn tiny_memory(floor: usize, part: usize) -> (Arc<QueryMemory>, Arc<MemTempStore>) {
		let store = Arc::new(MemTempStore::new(part, None));
		let host = MemoryHost {
			governor: Arc::new(FairGovernor::new(1)),
			store: Some(Arc::clone(&store) as Arc<dyn TempStore>),
			settings: MemorySettings {
				floor,
				fan_in: 2,
				chunk: 64,
				wait: Duration::ZERO,
				max: usize::MAX,
			},
		};
		(host.query(), store)
	}

	/// Run `spec` over `input`, draining after every row where `eager` and in
	/// chunks of seven once the input is finished. Answers the rows and the
	/// runs the window wrote; checks nothing is held or charged at the end.
	async fn run_with(
		spec: Arc<WindowSpec>,
		input: Vec<WindowInput>,
		ctx: &ExecutionContext,
		eager: bool,
	) -> FlowResult<(Vec<Value>, usize)> {
		let mut window = WindowRun::new(spec, ctx);
		let result = async {
			let mut out = Vec::new();
			for row in input {
				window.push(row, ctx).await?;
				if eager {
					out.extend(window.drain(usize::MAX, ctx).await?);
				}
			}
			window.finish(ctx).await?;
			loop {
				let chunk = window.drain(7, ctx).await?;
				if chunk.is_empty() {
					break;
				}
				out.extend(chunk);
			}
			assert_eq!(window.held_rows(), 0, "every row was handed on");
			assert_eq!(window.buffer.account.used(), 0, "no row is charged");
			Ok(out)
		}
		.await;
		let written = window.runs_written();
		window.clear();
		result.map(|out| (out, written))
	}

	async fn run(spec: Arc<WindowSpec>, input: Vec<WindowInput>) -> FlowResult<Vec<Value>> {
		run_with(spec, input, &root_ctx(), true).await.map(|(out, _)| out)
	}

	/// The values of field `name` across `rows`.
	fn column(rows: &[Value], name: &str) -> Vec<Value> {
		rows.iter()
			.map(|row| {
				let Value::Object(o) = row else {
					panic!("a window row is not an object");
				};
				o.get(name).cloned().unwrap_or(Value::None)
			})
			.collect()
	}

	fn ints(values: &[i64]) -> Vec<Value> {
		values.iter().map(|v| Value::from(*v)).collect()
	}

	fn floats(values: &[f64]) -> Vec<Value> {
		values.iter().map(|v| Value::from(*v)).collect()
	}

	fn error_of<T: std::fmt::Debug>(result: FlowResult<T>) -> String {
		match result {
			Err(ControlFlow::Err(error)) => format!("{error:#}"),
			other => panic!("expected an error, got {other:?}"),
		}
	}

	// A reference: every value computed from the whole partition at once.

	fn frame_rows(
		frame: WindowFrame,
		i: usize,
		(a, b): (usize, usize),
		(gs, ge): (usize, usize),
	) -> std::ops::Range<usize> {
		let range = frame.units == WindowFrameUnits::Range;
		let (i, a, b, gs, ge) = (i as i64, a as i64, b as i64, gs as i64, ge as i64);
		let s = match frame.start {
			UnboundedPreceding => a,
			Preceding(k) => (i - k as i64).max(a),
			CurrentRow if range => gs,
			CurrentRow => i,
			Following(k) => i + k as i64,
			UnboundedFollowing => b,
		};
		let t = match frame.end {
			UnboundedPreceding => a,
			Preceding(k) => (i - k as i64 + 1).max(a),
			CurrentRow if range => ge,
			CurrentRow => i + 1,
			Following(k) => (i + k as i64 + 1).min(b),
			UnboundedFollowing => b,
		};
		if s >= t {
			0..0
		} else {
			s as usize..t as usize
		}
	}

	fn int(value: &Value) -> i64 {
		if let Value::Number(n) = value
			&& let Wide::Int(i) = n.wide()
		{
			return i;
		}
		panic!("not an int: {value:?}")
	}

	fn reference(spec: &WindowSpec, input: &[WindowInput]) -> Vec<Value> {
		let p = spec.partitions;
		let same = |x: &[Value], y: &[Value]| x.iter().zip(y).all(|(a, b)| a == b);
		let peers = |x: &WindowInput, y: &WindowInput| {
			same(&x.keys[..p], &y.keys[..p])
				&& compare_keys(&x.keys[p..], &y.keys[p..], &spec.order) == Ordering::Equal
		};
		let mut out = Vec::new();
		let mut a = 0;
		while a < input.len() {
			let mut b = a + 1;
			while b < input.len() && same(&input[a].keys[..p], &input[b].keys[..p]) {
				b += 1;
			}
			let n = b - a;
			let mut dense = 0;
			for i in a..b {
				let mut gs = i;
				while gs > a && peers(&input[gs - 1], &input[i]) {
					gs -= 1;
				}
				let mut ge = i + 1;
				while ge < b && peers(&input[ge], &input[i]) {
					ge += 1;
				}
				if gs == i {
					dense += 1;
				}
				let mut row = input[i].row.clone();
				let mut offset = 0;
				for c in &spec.calls {
					let args = |j: usize| input[j].args[offset..offset + c.args].to_vec();
					let own = args(i);
					let value = match c.function {
						F::RowNumber => Value::from((i - a + 1) as i64),
						F::Rank => Value::from((gs - a + 1) as i64),
						F::DenseRank => Value::from(dense as i64),
						F::PercentRank => Value::from(if n == 1 {
							0.0
						} else {
							(gs - a) as f64 / (n - 1) as f64
						}),
						F::CumeDist => Value::from((ge - a) as f64 / n as f64),
						F::Ntile => {
							let buckets = int(&args(a)[0]) as usize;
							let (per, larger) = (n / buckets, n % buckets);
							let x = i - a;
							let bucket = if x < larger * (per + 1) {
								x / (per + 1) + 1
							} else {
								larger + (x - larger * (per + 1)) / per + 1
							};
							Value::from(bucket as i64)
						}
						F::Lag | F::Lead => {
							let k = own.get(1).map_or(1, int);
							let target = if c.function == F::Lag {
								i as i64 - k
							} else {
								i as i64 + k
							};
							if target >= a as i64 && target < b as i64 {
								args(target as usize)[0].clone()
							} else {
								own.get(2).cloned().unwrap_or(Value::Null)
							}
						}
						F::FirstValue
						| F::LastValue
						| F::NthValue
						| F::Sum
						| F::Avg
						| F::Count
						| F::Min
						| F::Max => {
							let frame = frame_rows(c.frame, i, (a, b), (gs, ge));
							let values: Vec<Value> = frame
								.clone()
								.map(|j| args(j).first().cloned().unwrap_or(Value::None))
								.collect();
							let present: Vec<Value> =
								values.iter().filter(|v| !v.is_nullish()).cloned().collect();
							let sum = || {
								present.iter().cloned().reduce(|x, y| x.try_add(y).expect("a sum"))
							};
							match c.function {
								F::Count if c.args == 0 => Value::from(frame.len() as i64),
								F::Count => Value::from(present.len() as i64),
								F::Sum => sum().unwrap_or(Value::Null),
								F::Avg => match sum() {
									Some(Value::Number(s)) => Value::Number(Number::Float(
										s.to_float() / present.len() as f64,
									)),
									Some(other) => panic!("a sum that is not a number: {other:?}"),
									None => Value::Null,
								},
								F::Min => present.iter().min().cloned().unwrap_or(Value::Null),
								F::Max => present.iter().max().cloned().unwrap_or(Value::Null),
								F::FirstValue => values.first().cloned().unwrap_or(Value::Null),
								F::LastValue => values.last().cloned().unwrap_or(Value::Null),
								F::NthValue => {
									let nth = int(&own[1]) as usize;
									values.get(nth - 1).cloned().unwrap_or(Value::Null)
								}
								F::RowNumber
								| F::Rank
								| F::DenseRank
								| F::PercentRank
								| F::CumeDist
								| F::Ntile
								| F::Lag
								| F::Lead => unreachable!("answered above"),
							}
						}
					};
					if let Value::Object(o) = &mut row {
						o.insert(c.output.clone(), value);
					}
					offset += c.args;
				}
				out.push(row);
			}
			a = b;
		}
		out
	}

	/// Every frame shape a window call can be given.
	fn frames() -> Vec<WindowFrame> {
		vec![
			rows(UnboundedPreceding, CurrentRow),
			rows(Preceding(2), CurrentRow),
			rows(Preceding(1), Following(1)),
			rows(CurrentRow, UnboundedFollowing),
			rows(UnboundedPreceding, UnboundedFollowing),
			rows(Following(2), Following(3)),
			rows(Preceding(3), Preceding(1)),
			rows(UnboundedPreceding, Preceding(2)),
			rows(Following(1), UnboundedFollowing),
			rows(Preceding(1), Preceding(3)),
			rows(CurrentRow, CurrentRow),
			rows(Following(1), Preceding(1)),
			rows(UnboundedPreceding, UnboundedPreceding),
			rows(UnboundedFollowing, UnboundedFollowing),
			range(UnboundedPreceding, CurrentRow),
			range(CurrentRow, CurrentRow),
			range(CurrentRow, UnboundedFollowing),
			range(UnboundedPreceding, UnboundedFollowing),
		]
	}

	/// A call of every function, the frame-reading ones over every frame.
	fn every_call() -> Vec<CallSpec> {
		let none = default_frame();
		let mut calls = vec![
			call("rn", F::RowNumber, 0, none),
			call("rk", F::Rank, 0, none),
			call("dr", F::DenseRank, 0, none),
			call("pr", F::PercentRank, 0, none),
			call("cd", F::CumeDist, 0, none),
			call("nt", F::Ntile, 1, none),
			call("lag", F::Lag, 1, none),
			call("lag2", F::Lag, 3, none),
			call("lag0", F::Lag, 2, none),
			call("lead", F::Lead, 1, none),
			call("lead3", F::Lead, 3, none),
			call("lagneg", F::Lag, 2, none),
			call("leadneg", F::Lead, 3, none),
		];
		let functions = [
			(F::Sum, 1),
			(F::Avg, 1),
			(F::Count, 0),
			(F::Count, 1),
			(F::Min, 1),
			(F::Max, 1),
			(F::FirstValue, 1),
			(F::LastValue, 1),
			(F::NthValue, 2),
		];
		for (f, frame) in frames().into_iter().enumerate() {
			for (function, args) in functions {
				calls.push(call(&format!("{function:?}{args}_{f}"), function, args, frame));
			}
		}
		calls
	}

	/// The args each row gives `calls`: `value` where a call reads one, and
	/// the constants the calls are named for.
	fn args_of(calls: &[CallSpec], value: &Value) -> Vec<Value> {
		let mut out = Vec::new();
		for c in calls {
			match (c.function, c.output.as_str()) {
				(F::Ntile, _) => out.push(Value::from(3)),
				(_, "lag2") => out.extend([value.clone(), Value::from(2), Value::from(-1)]),
				(_, "lag0") => out.extend([value.clone(), Value::from(0)]),
				(_, "lead3") => out.extend([value.clone(), Value::from(3), Value::from(-1)]),
				(_, "lagneg") => out.extend([value.clone(), Value::from(-2)]),
				(_, "leadneg") => out.extend([value.clone(), Value::from(-3), Value::from(-1)]),
				(F::NthValue, _) => out.extend([value.clone(), Value::from(2)]),
				_ => out.extend(std::iter::repeat_n(value.clone(), c.args)),
			}
		}
		out
	}

	/// Deterministic rows in the order a window reads them (by partition, then
	/// order key, where `partitioned`): `n` rows over `parts` partitions, order keys
	/// with ties, and values with `NULL`s, `NONE`s and, where `float`, floats.
	fn generated(
		n: usize,
		parts: u64,
		partitioned: bool,
		float: bool,
	) -> (Vec<i64>, Vec<i64>, Vec<Value>) {
		let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
		let mut next = move || {
			seed ^= seed << 13;
			seed ^= seed >> 7;
			seed ^= seed << 17;
			seed
		};
		let mut rows: Vec<(i64, i64, Value)> = (0..n)
			.map(|_| {
				let p = (next() % parts) as i64;
				let k = (next() % 6) as i64;
				let v = match next() % 10 {
					0 => Value::Null,
					1 => Value::None,
					x if float && x < 5 => Value::from((next() % 1000) as f64 / 8.0),
					_ => Value::from((next() % 100) as i64 - 50),
				};
				(p, k, v)
			})
			.collect();
		if partitioned {
			rows.sort_by_key(|r| (r.0, r.1));
		} else {
			rows.sort_by_key(|r| r.1);
		}
		let parts = rows.iter().map(|r| r.0).collect();
		let keys = rows.iter().map(|r| r.1).collect();
		let values = rows.into_iter().map(|r| r.2).collect();
		(parts, keys, values)
	}

	/// Run every call over generated rows, partitioned and ordered three ways
	/// and drained two ways, against the reference. Answers the runs written.
	async fn agrees_with_reference(
		n: usize,
		parts: u64,
		float: bool,
		ctx: &ExecutionContext,
	) -> usize {
		let calls = every_call();
		let mut written = 0;
		for (partitioned, ordered) in [(true, true), (false, true), (true, false)] {
			let spec = spec(partitioned, ordered, calls.clone());
			let (p, k, v) = generated(n, parts, partitioned, float);
			let input = inputs(&spec, &p, &k, |i| args_of(&calls, &v[i]));
			let want = reference(&spec, &input);
			for eager in [true, false] {
				let (got, runs) =
					run_with(Arc::clone(&spec), input.clone(), ctx, eager).await.expect("a window");
				written += runs;
				assert_eq!(got.len(), want.len());
				for (i, (g, w)) in got.iter().zip(&want).enumerate() {
					assert_eq!(g, w, "row {i}, partitioned {partitioned}, ordered {ordered}");
				}
			}
		}
		written
	}

	#[tokio::test]
	async fn every_function_and_frame_agrees_with_the_reference() {
		agrees_with_reference(160, 4, false, &root_ctx()).await;
	}

	#[tokio::test]
	async fn float_sums_over_moving_frames_agree_with_the_reference() {
		agrees_with_reference(120, 3, true, &root_ctx()).await;
	}

	#[tokio::test]
	async fn a_spilled_window_answers_what_an_unspilled_one_does() {
		let (memory, store) = tiny_memory(2048, 256);
		let ctx = ctx_over(Some(Arc::clone(&memory)));
		let written = agrees_with_reference(90, 3, true, &ctx).await;
		assert!(written > 0, "the window never spilled");
		assert_eq!(memory.lease().held(), 0, "the lease is given back");
		assert_eq!(store.runs(), 0, "every run is deleted");
	}

	#[tokio::test]
	async fn a_window_reads_long_runs_through_its_cursors() {
		// Few calls, so each run holds many rows and the cursors read on
		// through a run rather than reopening it.
		let (memory, store) = tiny_memory(16 << 10, 1024);
		let ctx = ctx_over(Some(Arc::clone(&memory)));
		let calls = vec![
			call("rn", F::RowNumber, 0, default_frame()),
			call("s", F::Sum, 1, rows(Preceding(5), Following(5))),
			call("lead3", F::Lead, 3, default_frame()),
			call("lag2", F::Lag, 3, default_frame()),
			call("cd", F::CumeDist, 0, default_frame()),
			call("fv", F::FirstValue, 1, rows(Preceding(4), CurrentRow)),
			call("nv", F::NthValue, 2, rows(Preceding(6), CurrentRow)),
			call("mx", F::Max, 1, rows(Preceding(3), Following(2))),
		];
		for partitioned in [false, true] {
			let spec = spec(partitioned, true, calls.clone());
			let (p, k, v) = generated(1500, 5, partitioned, false);
			let input = inputs(&spec, &p, &k, |i| args_of(&calls, &v[i]));
			let want = reference(&spec, &input);
			let (unspilled, _) = run_with(Arc::clone(&spec), input.clone(), &root_ctx(), false)
				.await
				.expect("a window");
			let (got, written) = run_with(spec, input, &ctx, false).await.expect("a window");
			assert!(written > 0, "the window never spilled");
			assert_eq!(got, unspilled);
			assert_eq!(got, want);
		}
		assert_eq!(memory.lease().held(), 0, "the lease is given back");
		assert_eq!(store.runs(), 0, "every run is deleted");
	}

	#[tokio::test]
	async fn a_window_with_no_store_holds_its_rows_past_the_lease() {
		let host = MemoryHost {
			governor: Arc::new(FairGovernor::new(1)),
			store: None,
			settings: MemorySettings {
				floor: 1024,
				fan_in: 2,
				chunk: 64,
				wait: Duration::ZERO,
				max: usize::MAX,
			},
		};
		let memory = host.query();
		let ctx = ctx_over(Some(Arc::clone(&memory)));
		let written = agrees_with_reference(60, 2, false, &ctx).await;
		assert_eq!(written, 0, "a window with no store spilled");
		assert_eq!(memory.lease().held(), 0, "the lease is given back");
	}

	#[tokio::test]
	async fn ranks_answer_ties_as_peers() {
		let spec = spec(
			false,
			true,
			vec![
				call("rn", F::RowNumber, 0, default_frame()),
				call("rk", F::Rank, 0, default_frame()),
				call("dr", F::DenseRank, 0, default_frame()),
				call("pr", F::PercentRank, 0, default_frame()),
				call("cd", F::CumeDist, 0, default_frame()),
			],
		);
		let input = inputs(&spec, &[], &[1, 1, 2, 3, 3, 3], |_| Vec::new());
		let out = run(spec, input).await.expect("a window");
		assert_eq!(column(&out, "id"), ints(&[0, 1, 2, 3, 4, 5]), "rows keep their order");
		assert_eq!(column(&out, "rn"), ints(&[1, 2, 3, 4, 5, 6]));
		assert_eq!(column(&out, "rk"), ints(&[1, 1, 3, 4, 4, 4]));
		assert_eq!(column(&out, "dr"), ints(&[1, 1, 2, 3, 3, 3]));
		assert_eq!(column(&out, "pr"), floats(&[0.0, 0.0, 0.4, 0.6, 0.6, 0.6]));
		assert_eq!(column(&out, "cd"), floats(&[2.0 / 6.0, 2.0 / 6.0, 0.5, 1.0, 1.0, 1.0]));
	}

	#[tokio::test]
	async fn a_parameter_that_changes_within_a_partition_fails() {
		let order = [1, 2, 3];
		for (function, args, of) in [
			(
				F::NthValue,
				2,
				(|i| vec![Value::from(i as i64), Value::from(1 + i as i64)])
					as fn(usize) -> Vec<Value>,
			),
			(F::Ntile, 1, |i| vec![Value::from(2 + i as i64)]),
			(F::Ntile, 1, |i| {
				vec![Value::from(if i == 0 {
					2
				} else {
					0
				})]
			}),
			(F::Lag, 2, |i| vec![Value::from(i as i64), Value::from(1 + i as i64)]),
		] {
			let spec = spec(false, true, vec![call("p", function, args, default_frame())]);
			let input = inputs(&spec, &[], &order, of);
			let got = run(spec, input).await;
			assert!(got.is_err(), "{function:?} answered {got:?}");
		}
	}

	#[tokio::test]
	async fn rows_with_no_order_key_are_all_peers() {
		let spec = spec(
			false,
			false,
			vec![
				call("rk", F::Rank, 0, default_frame()),
				call("cd", F::CumeDist, 0, default_frame()),
				call("s", F::Sum, 1, default_frame()),
			],
		);
		let input = inputs(&spec, &[], &[0, 0, 0], |i| vec![Value::from(i as i64)]);
		let out = run(spec, input).await.expect("a window");
		assert_eq!(column(&out, "rk"), ints(&[1, 1, 1]));
		assert_eq!(column(&out, "cd"), floats(&[1.0, 1.0, 1.0]));
		assert_eq!(column(&out, "s"), ints(&[3, 3, 3]));
	}

	#[tokio::test]
	async fn a_partition_of_one_has_a_percent_rank_of_zero() {
		let spec = spec(true, true, vec![call("pr", F::PercentRank, 0, default_frame())]);
		let input = inputs(&spec, &[1, 2, 2], &[0, 0, 1], |_| Vec::new());
		let out = run(spec, input).await.expect("a window");
		assert_eq!(column(&out, "pr"), floats(&[0.0, 0.0, 1.0]));
	}

	#[tokio::test]
	async fn partitions_start_afresh() {
		let spec = spec(
			true,
			true,
			vec![
				call("rn", F::RowNumber, 0, default_frame()),
				call("s", F::Sum, 1, default_frame()),
			],
		);
		let input = inputs(&spec, &[1, 1, 1, 2, 2, 3], &[0, 1, 2, 0, 1, 0], |i| {
			vec![Value::from(i as i64 + 1)]
		});
		let out = run(spec, input).await.expect("a window");
		assert_eq!(column(&out, "id"), ints(&[0, 1, 2, 3, 4, 5]));
		assert_eq!(column(&out, "rn"), ints(&[1, 2, 3, 1, 2, 1]));
		assert_eq!(column(&out, "s"), ints(&[1, 3, 6, 4, 9, 6]));
	}

	#[tokio::test]
	async fn the_default_frame_reaches_the_row_s_peers() {
		let spec = spec(
			false,
			true,
			vec![
				call("s", F::Sum, 1, default_frame()),
				call("lv", F::LastValue, 1, default_frame()),
				call("rows", F::Sum, 1, rows(UnboundedPreceding, CurrentRow)),
			],
		);
		let input = inputs(&spec, &[], &[1, 2, 2, 3], |i| vec![Value::from(i as i64 + 1); 3]);
		let out = run(spec, input).await.expect("a window");
		assert_eq!(column(&out, "s"), ints(&[1, 6, 6, 10]));
		assert_eq!(column(&out, "lv"), ints(&[1, 3, 3, 4]));
		assert_eq!(column(&out, "rows"), ints(&[1, 3, 6, 10]));
	}

	#[tokio::test]
	async fn rows_frames_count_rows_clamped_to_the_partition() {
		let spec = spec(
			false,
			true,
			vec![
				call("back", F::Sum, 1, rows(Preceding(2), CurrentRow)),
				call("around", F::Sum, 1, rows(Preceding(1), Following(1))),
				call("ahead", F::Sum, 1, rows(Following(1), Following(2))),
				call("rest", F::Sum, 1, rows(CurrentRow, UnboundedFollowing)),
				call("n", F::Count, 0, rows(Preceding(1), Following(1))),
			],
		);
		let input = inputs(&spec, &[], &[0, 1, 2, 3, 4], |i| vec![Value::from(1i64 << i); 4]);
		let out = run(spec, input).await.expect("a window");
		let null = Value::Null;
		assert_eq!(column(&out, "back"), ints(&[1, 3, 7, 14, 28]));
		assert_eq!(column(&out, "around"), ints(&[3, 7, 14, 28, 24]));
		let mut ahead = ints(&[6, 12, 24, 16]);
		ahead.push(null);
		assert_eq!(column(&out, "ahead"), ahead);
		assert_eq!(column(&out, "rest"), ints(&[31, 30, 28, 24, 16]));
		assert_eq!(column(&out, "n"), ints(&[2, 3, 3, 3, 2]));
	}

	#[tokio::test]
	async fn aggregates_skip_null_and_none() {
		let frame = rows(UnboundedPreceding, CurrentRow);
		let spec = spec(
			false,
			true,
			vec![
				call("s", F::Sum, 1, frame),
				call("a", F::Avg, 1, frame),
				call("c", F::Count, 1, frame),
				call("n", F::Count, 0, frame),
				call("mn", F::Min, 1, frame),
				call("mx", F::Max, 1, rows(Preceding(1), CurrentRow)),
			],
		);
		let values = [Value::Null, Value::from(4), Value::None, Value::from(2)];
		let input = inputs(&spec, &[], &[0, 1, 2, 3], |i| vec![values[i].clone(); 5]);
		let out = run(spec, input).await.expect("a window");
		let null = Value::Null;
		assert_eq!(column(&out, "s"), vec![null.clone(), 4.into(), 4.into(), 6.into()]);
		assert_eq!(column(&out, "a"), vec![null.clone(), 4.into(), 4.into(), 3.into()]);
		assert_eq!(column(&out, "c"), ints(&[0, 1, 1, 2]));
		assert_eq!(column(&out, "n"), ints(&[1, 2, 3, 4]));
		assert_eq!(column(&out, "mn"), vec![null.clone(), 4.into(), 4.into(), 2.into()]);
		assert_eq!(column(&out, "mx"), vec![null, 4.into(), 4.into(), 2.into()]);
	}

	#[tokio::test]
	async fn frame_values_answer_null_over_too_few_rows() {
		let spec = spec(
			false,
			true,
			vec![
				call("fv", F::FirstValue, 1, rows(Preceding(1), CurrentRow)),
				call("lv", F::LastValue, 1, rows(CurrentRow, UnboundedFollowing)),
				call("nv", F::NthValue, 2, rows(UnboundedPreceding, CurrentRow)),
				call("empty", F::FirstValue, 1, rows(UnboundedPreceding, Preceding(1))),
			],
		);
		let input = inputs(&spec, &[], &[0, 1, 2], |i| {
			let v = Value::from(i as i64 * 10);
			vec![v.clone(), v.clone(), v.clone(), Value::from(2), v]
		});
		let out = run(spec, input).await.expect("a window");
		let null = Value::Null;
		assert_eq!(column(&out, "fv"), ints(&[0, 0, 10]));
		assert_eq!(column(&out, "lv"), ints(&[20, 20, 20]));
		assert_eq!(column(&out, "nv"), vec![null.clone(), 10.into(), 10.into()]);
		assert_eq!(column(&out, "empty"), vec![null, 0.into(), 0.into()]);
	}

	#[tokio::test]
	async fn ntile_gives_the_first_buckets_the_extra_rows() {
		let spec = spec(false, true, vec![call("nt", F::Ntile, 1, default_frame())]);
		let keys: Vec<i64> = (0..10).collect();
		let input = inputs(&spec, &[], &keys, |_| vec![Value::from(4)]);
		let out = run(Arc::clone(&spec), input).await.expect("a window");
		assert_eq!(column(&out, "nt"), ints(&[1, 1, 1, 2, 2, 2, 3, 3, 4, 4]));
		let input = inputs(&spec, &[], &[0, 1, 2], |_| vec![Value::from(5)]);
		let out = run(spec, input).await.expect("a window");
		assert_eq!(column(&out, "nt"), ints(&[1, 2, 3]));
	}

	#[tokio::test]
	async fn lag_and_lead_answer_their_default_outside_the_partition() {
		let spec = spec(
			true,
			true,
			vec![
				call("lag", F::Lag, 1, default_frame()),
				call("lead2", F::Lead, 3, default_frame()),
				call("lag0", F::Lag, 2, default_frame()),
			],
		);
		let input = inputs(&spec, &[1, 1, 1, 1, 2, 2], &[0, 1, 2, 3, 0, 1], |i| {
			let v = Value::from(i as i64 * 10);
			vec![v.clone(), v.clone(), Value::from(2), Value::from(-1), v, Value::from(0)]
		});
		let out = run(spec, input).await.expect("a window");
		let v = |i: i64| Value::from(i * 10);
		assert_eq!(column(&out, "lag"), vec![Value::Null, v(0), v(1), v(2), Value::Null, v(4)]);
		let mut lead = vec![v(2), v(3)];
		lead.extend(ints(&[-1, -1, -1, -1]));
		assert_eq!(column(&out, "lead2"), lead);
		assert_eq!(column(&out, "lag0"), (0..6).map(v).collect::<Vec<_>>());
	}

	#[tokio::test]
	async fn an_empty_input_answers_no_rows() {
		let spec = spec(true, true, every_call());
		let out = run(spec, Vec::new()).await.expect("a window");
		assert!(out.is_empty());
	}

	#[tokio::test]
	async fn invalid_arguments_fail() {
		let ntile = spec(false, true, vec![call("nt", F::Ntile, 1, default_frame())]);
		let input = inputs(&ntile, &[], &[0, 1], |_| vec![Value::from(0)]);
		let error = error_of(run(ntile, input).await);
		assert!(error.contains("positive integer"), "{error}");

		let lag = spec(false, true, vec![call("lag", F::Lag, 2, default_frame())]);
		let input = inputs(&lag, &[], &[0, 1], |i| vec![Value::from(1), Value::from(i as i64 + 1)]);
		let error = error_of(run(lag, input).await);
		assert!(error.contains("same for every row"), "{error}");

		let lead = spec(false, true, vec![call("lead", F::Lead, 2, default_frame())]);
		let input = inputs(&lead, &[], &[0], |_| vec![Value::from(1), Value::from(1.5)]);
		let error = error_of(run(lead, input).await);
		assert!(error.contains("must be an integer"), "{error}");

		let nth = spec(false, true, vec![call("nv", F::NthValue, 2, default_frame())]);
		let input = inputs(&nth, &[], &[0], |_| vec![Value::from(1), Value::from(0)]);
		let error = error_of(run(nth, input).await);
		assert!(error.contains("positive integer"), "{error}");
	}

	#[tokio::test]
	async fn a_range_frame_with_an_offset_is_refused() {
		let spec = spec(false, true, vec![call("s", F::Sum, 1, range(Preceding(1), CurrentRow))]);
		let input = inputs(&spec, &[], &[0], |_| vec![Value::from(1)]);
		let error = error_of(run(spec, input).await);
		assert!(error.contains("Internal error"), "{error}");
	}

	/// Push `count` rows of one partition through `spec`, draining after each,
	/// and answer the most rows the window held at once.
	async fn most_held(spec: Arc<WindowSpec>, count: usize) -> u64 {
		let ctx = root_ctx();
		let mut window = WindowRun::new(Arc::clone(&spec), &ctx);
		let calls = spec.calls.clone();
		let mut most = 0;
		let mut handed = 0;
		for i in 0..count {
			let args = args_of(&calls, &Value::from(i as i64));
			let input = WindowInput {
				keys: vec![Value::from(i as i64)],
				args,
				row: row(i),
			};
			window.push(input, &ctx).await.expect("a row");
			most = most.max(window.held_rows());
			handed += window.drain(usize::MAX, &ctx).await.expect("rows").len();
			most = most.max(window.held_rows());
		}
		window.finish(&ctx).await.expect("finished");
		handed += window.drain(usize::MAX, &ctx).await.expect("rows").len();
		assert_eq!(handed, count);
		window.clear();
		most
	}

	#[tokio::test]
	async fn row_number_holds_only_the_row_it_answers() {
		let spec = spec(
			false,
			true,
			vec![
				call("rn", F::RowNumber, 0, default_frame()),
				call("rk", F::Rank, 0, default_frame()),
				call("dr", F::DenseRank, 0, default_frame()),
			],
		);
		let most = most_held(spec, 10_000).await;
		assert!(most <= 1, "held {most} rows");
	}

	#[tokio::test]
	async fn streaming_calls_hold_rows_bounded_by_their_offsets() {
		let cases = [
			(call("lag2", F::Lag, 3, default_frame()), 3),
			(call("lead3", F::Lead, 3, default_frame()), 4),
			(call("s", F::Sum, 1, rows(UnboundedPreceding, CurrentRow)), 1),
			(call("s", F::Sum, 1, rows(Preceding(4), CurrentRow)), 6),
			(call("m", F::Min, 1, rows(Preceding(4), CurrentRow)), 2),
			(call("f", F::FirstValue, 1, rows(Preceding(4), CurrentRow)), 6),
			(call("n", F::NthValue, 2, rows(Preceding(4), CurrentRow)), 5),
			(call("s", F::Sum, 1, rows(Preceding(2), Following(3))), 7),
			// The peer group ends only once the row after it has arrived.
			(call("s", F::Sum, 1, default_frame()), 2),
		];
		for (call, bound) in cases {
			let name = format!("{:?} {:?}", call.function, call.frame);
			let most = most_held(spec(false, true, vec![call]), 2_000).await;
			assert!(most <= bound, "{name} held {most} rows, more than {bound}");
		}
	}
}
