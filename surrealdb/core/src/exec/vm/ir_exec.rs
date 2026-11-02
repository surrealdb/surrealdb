//! The interpreter's execution of [`IrInstr`] forms.
//!
//! Every [`IrInstr`] produces an answer: no form is unreachable at run time, and an instruction
//! that cannot complete fails its row with an error rather than being skipped. A form whose run
//! state is absent gives the empty answer, which is what a host without IR run state (the fold
//! host, a morsel worker) sees: a memo cell is empty, no read buffer exists, no write candidate
//! is pending and no write prefix runs. [`IrInstr::unexecuted`] is the answer of a form with no
//! executor.
//!
//! An executor sees what every other instruction of the interpreter sees: the program, the
//! pipeline, the row's reader, the row's registers and the run's state. A form resolves an
//! operand as the interpreter does, a `Const` through [`Program::consts`] and a `Field` through
//! the row. A form that needs the host answers [`IrFlow::Host`], and the host runs it through
//! [`ir_host::run`](super::ir_host::run).
//!
//! # The batch kernel
//!
//! [`run_kernel`] runs a [`KernelSpec`]'s whole loop in one instruction, a batch of positions at a
//! time. Per batch a guard picks the typed arm, which holds each column unboxed and runs an
//! operation natively only where that gives exactly what the scalar function gives, or the
//! generic arm, which holds every value boxed and runs the scalar function per position. Both arms
//! call the functions the interpreter's per-element instructions call, so values, errors and
//! error order equal the serial body's at every batch width.
//!
//! # Run state and its glue
//!
//! The forms keep an [`IrRunState`] across the rows of one program run, one per
//! [`State`]. `State` does not belong to this module, so the run state is reached through glue
//! functions that report it absent while `State` has no such field.
//! - [`split`] borrows the run state and the other parts of `State` the arms read, disjointly. It
//!   is `None` while `State` holds no run state.
//! - [`ir_state`] borrows the run state alone, for code that needs nothing else. It is `None` while
//!   `State` holds no run state.
//! - [`read_buffer_of`](super::ir_host::read_buffer_of), in `ir_host.rs`, answers the read buffer
//!   of a statement's context, `None` while the context holds none. The buffer is read from the
//!   root context.
//! - `worker_ctx`, in `morsel/run.rs`, answers the context a morsel's worker runs under, which
//!   holds no read buffer.
//! - A write body's candidate state lives in the statement's `WriteState`: whether the body runs as
//!   a compute prefix, where the prefix stopped, and the candidate pending for the next run of the
//!   body. A form that finds no write state answers its absent case as described above.

#![deny(clippy::wildcard_enum_match_arm)]

use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::{Arc, OnceLock};

use anyhow::bail;
use smallvec::SmallVec;

use super::block::ForIter;
use super::fuel;
use super::interp::State;
use super::ir_host::ReadBuffer;
use super::ir_instr::{
	BufferScope, ColKind, IrForm, IrInstr, KReg, KRegRepr, KernelInput, KernelOp, KernelSink,
	KernelSpec, LookAhead,
};
use super::morsel::Stop;
use super::program::{ArithOp, CompareOp, Operand, Pipeline, Program, UnaryOp};
use super::row::RowReader;
use super::scalar::{Held, Scalar};
use super::write::WriteState;
use crate::exec::memory::size::kept_bytes;
use crate::exec::memory::{QueryMemory, Reservation, Verdict as Charge};
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::WriteRow;
use crate::val::{Number, Value};

/// Where the row goes after an IR instruction the interpreter ran.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(super) enum IrFlow {
	/// The next instruction.
	Fall,
	/// The instruction at `target`. A backward target is an iteration of a loop, so it spends fuel
	/// as any backward jump does.
	Goto(usize),
	/// The row is done; the pipeline moves to its next.
	EndRow,
	/// The pipeline needs no more rows.
	StopPipeline,
	/// The instruction needs the host: the interpreter answers
	/// [`Step::Ir`](super::interp::Step::Ir) at this instruction, and the host's answer says where
	/// the row resumes.
	Host,
}

/// The run state IR forms keep across the rows of one program run. One per [`State`]; dropped
/// with it, so nothing in it outlives the run.
#[allow(dead_code, reason = "request L16-1 installs the state in `State`; unit tests build it")]
#[derive(Default)]
pub(super) struct IrRunState {
	/// The memo cells, by cell number. A statement's state holds every cell of its program from
	/// the start ([`size_memos`]), so a morsel's state shares each cell with it and with every
	/// other morsel. A cell past the vector's end is empty, and an offer to one grows this vector
	/// alone: no other run state sees it. A cell the vector shares with another run state (the
	/// morsel runtime's `Under::memos`) is replaced in this vector alone when reset.
	pub(super) memos: Vec<Arc<MemoCell>>,
	/// The statement's read buffer, which the batched-read forms fill and clear. `None` when the
	/// statement batches nothing, and always `None` for a morsel worker's state: a worker never
	/// holds the coordinator's buffer.
	pub(super) buffer: Option<Arc<ReadBuffer>>,
	/// The identity of the program the state runs, which names its partitions in the buffer.
	pub(super) program_id: usize,
}

/// A candidate the coordinator installs for a position: the record a compute prefix left in
/// flight, its table's write context where the prefix resolved it, the registers the prefix wrote
/// that the residue reads, and where the prefix stopped.
pub(in crate::exec::vm) struct PendingCandidate {
	/// The record in flight after the prefix.
	pub(super) row: WriteRow,
	/// The write context of the record's table, which a prefix that stopped before resolving it
	/// leaves unresolved.
	pub(super) ctx: Option<Arc<WriteCtx>>,
	/// The registers the prefix wrote that the residue reads, as `(register, value)`.
	pub(super) live: SmallVec<[(u16, Value); 8]>,
	/// Where the prefix stopped.
	pub(super) stop: Stop,
}

impl PendingCandidate {
	/// Turns the candidate into the record the gate passes over: it resumes at the skip, matched
	/// by no condition and having changed nothing, as a record the gate skips ends before any stage
	/// of its compute half runs.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(super) fn pass_over(&mut self) {
		self.stop = Stop::Skip;
		self.row.matched = false;
		self.row.mutated = false;
	}
}

/// Where the compute prefix of a record stopped and the registers it left for the residue.
#[cfg_attr(target_family = "wasm", allow(dead_code))]
pub(in crate::exec::vm) struct PrefixStop {
	pub(super) stop: Stop,
	pub(super) live: SmallVec<[(u16, Value); 8]>,
	/// Whether the part ahead of the cut encodes the after-image of the record it left in flight.
	pub(super) encode: bool,
}

/// One memo cell: the kept value of a hoisted, row-independent region, or the refusal to keep
/// it.
///
/// The slot is set at most once between resets, and only ever through [`OnceLock::set`], so a
/// cell shared between a coordinator and its morsels' workers is filled by whichever offers first
/// and read by every other without a wait.
#[derive(Default)]
pub(super) struct MemoCell {
	slot: OnceLock<MemoSlot>,
}

/// What a memo cell holds once it has been offered a value.
#[allow(dead_code, reason = "the reservation is held for its drop, which releases the bytes")]
pub(super) enum MemoSlot {
	/// The value, and the lease bytes its holding is charged.
	Value(Value, Reservation),
	/// The query's memory does not cover the value: nothing is kept, and a read of the cell
	/// answers as an empty one.
	Refused,
}

/// The parts of [`State`] the IR arms read beside the run state, borrowed disjointly from it.
pub(super) struct StateParts<'s> {
	/// A block's `FOR` iterators, by slot.
	pub(super) iters: &'s [Option<ForIter>],
	/// The query's memory, where memo values are charged.
	pub(super) memory: Option<&'s Arc<QueryMemory>>,
	/// The write statement's state, for the write forms.
	pub(super) write: Option<&'s mut WriteState>,
}

/// The run state of `state` and the other parts the arms read.
///
/// `None` while `State` holds no run state.
/// One function borrows every part, so the borrows
/// stay disjoint.
pub(super) fn split(state: &mut State) -> (Option<&mut IrRunState>, StateParts<'_>) {
	(
		None,
		StateParts {
			iters: &state.iters,
			memory: state.memory.as_ref(),
			write: state.write.as_deref_mut(),
		},
	)
}

/// The run state of `state` alone.
///
/// `None` while `State` holds no run state.
pub(super) fn ir_state(_state: &mut State) -> Option<&mut IrRunState> {
	None
}

/// Ends a write prefix at the marker that stops it with `stop`, carrying the values of the
/// registers `live` of `regs`, or falls through on the coordinator, where a marker is the next
/// instruction.
///
/// A prefix needs a write state to record the stop in: one that runs without is an error, because
/// its candidate would carry no stop.
fn stop_prefix<'r>(
	write: Option<&mut WriteState>,
	stop: Stop,
	live: &[u16],
	encode: bool,
	regs: &[Scalar<'r>],
) -> anyhow::Result<IrFlow> {
	let Some(write) = write.filter(|write| write.in_prefix_mode()) else {
		return Ok(IrFlow::Fall);
	};
	let mut held = SmallVec::new();
	for register in live {
		let Some(scalar) = regs.get(*register as usize) else {
			bail!("a write prefix carries register {register}, which the row does not hold");
		};
		held.push((*register, scalar.to_value()));
	}
	write.stop_prefix(PrefixStop {
		stop,
		live: held,
		encode,
	});
	Ok(IrFlow::EndRow)
}

/// Runs `instr`, the instruction at `pc` of `pipeline`, over `regs`, and answers where the row
/// goes next.
pub(super) fn run<'r>(
	instr: &IrInstr,
	program: &'r Program,
	pipeline: &'r Pipeline,
	row: &'r dyn RowReader,
	regs: &mut [Scalar<'r>],
	state: &mut State,
	pc: usize,
) -> anyhow::Result<IrFlow> {
	let (ir, parts) = split(state);
	run_form(instr, ir, parts, program, pipeline, row, regs, pc)
}

/// Runs `instr` against the run state `ir` and the state parts `parts`, with no view of
/// [`State`] itself. [`run`] is this over the parts of the interpreter's state; a test calls it
/// with constructed ones.
///
/// Every form has an arm, and each arm gives its absent answer when `ir` is
/// `None`. The write forms read whether the body runs as a compute prefix from the write state.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_form<'r>(
	instr: &IrInstr,
	ir: Option<&mut IrRunState>,
	parts: StateParts<'_>,
	program: &'r Program,
	_pipeline: &'r Pipeline,
	row: &'r dyn RowReader,
	regs: &mut [Scalar<'r>],
	_pc: usize,
) -> anyhow::Result<IrFlow> {
	match instr.form() {
		// The kernel runs every batch of its loop in this one instruction.
		IrForm::Kernel(spec) => {
			run_kernel(spec, program, row, regs)?;
			Ok(IrFlow::Fall)
		}
		// Neither batched-read form charges fuel: a fuel unit the serial run does not spend could
		// move the first error. With no buffer there is nothing to fill and nothing to clear.
		IrForm::BatchReset {
			site,
		} => {
			if let Some(ir) = ir
				&& let Some(buffer) = &ir.buffer
			{
				buffer.reset(
					ir.program_id,
					BufferScope::Loop {
						site: *site,
					},
				);
			}
			Ok(IrFlow::Fall)
		}
		// Only a fill needs the host, because only a fill awaits the store. A window the buffer
		// covers falls through to the loop body.
		IrForm::BatchRead(read) => {
			let Some(ir) = ir else {
				return Ok(IrFlow::Fall);
			};
			let Some(buffer) = &ir.buffer else {
				return Ok(IrFlow::Fall);
			};
			let covered = match read.ahead {
				LookAhead::None => buffer.covers(ir.program_id, read.scope, None),
				LookAhead::Iter {
					slot,
					..
				} => buffer.covers(
					ir.program_id,
					read.scope,
					Some(held_ahead(parts.iters, slot).len()),
				),
				// Not produced by the lowering: no look-ahead over an array by position is emitted.
				LookAhead::Elems {
					..
				} => true,
			};
			Ok(if covered {
				IrFlow::Fall
			} else {
				IrFlow::Host
			})
		}
		// A payload-free marker: no held failure exists to consume.
		IrForm::SkipRow => Ok(IrFlow::Fall),
		// With no run state every cell is empty, so the region that computes the value runs.
		IrForm::MemoGet(get) => {
			let Some(ir) = ir else {
				return Ok(IrFlow::Fall);
			};
			let Some(MemoSlot::Value(value, _)) = kept(ir, get.cell) else {
				return Ok(IrFlow::Fall);
			};
			let value = value.clone();
			let Some(dst) = regs.get_mut(get.dst as usize) else {
				bail!("memo destination register {} is not in the register file", get.dst);
			};
			*dst = scalar_of(value);
			Ok(IrFlow::Goto(get.hit as usize))
		}
		// The kept value is judged in place: nothing is copied out of the cell.
		IrForm::MemoTruthy(truthy) => {
			let Some(ir) = ir else {
				return Ok(IrFlow::Fall);
			};
			Ok(match kept(ir, truthy.cell) {
				Some(MemoSlot::Value(value, _)) => IrFlow::Goto(if value.is_truthy() {
					truthy.then as usize
				} else {
					truthy.otherwise as usize
				}),
				Some(MemoSlot::Refused) | None => IrFlow::Fall,
			})
		}
		// An offer is best effort: with no run state it is refused, and the program's answer does
		// not depend on whether a value was kept.
		IrForm::MemoPut(put) => {
			let Some(ir) = ir else {
				return Ok(IrFlow::Fall);
			};
			let index = put.cell as usize;
			if ir.memos.get(index).is_some_and(|cell| cell.slot.get().is_some()) {
				return Ok(IrFlow::Fall);
			}
			let Some(src) = regs.get(put.src as usize) else {
				bail!("memo source register {} is not in the register file", put.src);
			};
			let slot = keep(src.to_value(), parts.memory);
			if ir.memos.len() <= index {
				ir.memos.resize_with(index + 1, Arc::default);
			}
			// A cell filled since the check above, by a worker sharing it, keeps its value; this
			// slot is dropped and its bytes are released.
			let _ = ir.memos[index].slot.set(slot);
			Ok(IrFlow::Fall)
		}
		IrForm::MemoReset(reset) => {
			if let Some(ir) = ir {
				let first = reset.first as usize;
				for index in first..first + reset.count as usize {
					reset_cell(ir, index);
				}
			}
			Ok(IrFlow::Fall)
		}
		// A prefix never resumes a candidate. The coordinator hands a pending one to the host,
		// which needs the registers and the context; with none pending the body runs in place.
		IrForm::WriteResume(_) => Ok(match parts.write {
			Some(write) if !write.in_prefix_mode() && write.has_pending() => IrFlow::Host,
			Some(_) | None => IrFlow::Fall,
		}),
		// A prefix ends at the first marker it reaches and says which; the coordinator runs on.
		IrForm::WriteCut(spec) => {
			stop_prefix(parts.write, Stop::Cut, &spec.live, spec.encode, regs)
		}
		IrForm::WriteSkip => stop_prefix(parts.write, Stop::Skip, &[], false, regs),
		IrForm::WriteOffPath => stop_prefix(parts.write, Stop::OffPath, &[], false, regs),
	}
}

/// The elements the `FOR` iterator in `slot` holds beyond the current one: the remaining elements
/// of an array, and none for a range or an empty slot, whose integers are not held.
pub(super) fn held_ahead(iters: &[Option<ForIter>], slot: u16) -> &[Value] {
	match iters.get(slot as usize) {
		Some(Some(ForIter::Array(items))) => items.as_slice(),
		Some(Some(ForIter::Range(_))) | Some(None) | None => &[],
	}
}

/// The slot of memo cell `cell` of `ir`, if the cell exists and has been offered a value.
fn kept(ir: &IrRunState, cell: u16) -> Option<&MemoSlot> {
	ir.memos.get(cell as usize).and_then(|cell| cell.slot.get())
}

/// The slot that keeps `value` under the query's `memory`: the value with the lease bytes it is
/// charged, measured as the program's ledger measures a value it keeps, or a refusal when the
/// lease does not cover them. A value with no heap is charged nothing and is always kept.
///
/// A kept value is best effort and must never count toward the query's pinned ceiling, which the
/// serial run does not hold it against: a query that cannot spill charges its reservations as
/// pinned bytes, so there the value is refused. A query that can spill charges it to the lease
/// alone.
fn keep(value: Value, memory: Option<&Arc<QueryMemory>>) -> MemoSlot {
	let bytes = kept_bytes(&value);
	let mut reservation = match memory {
		Some(memory) if bytes > 0 => {
			if !memory.can_spill() {
				return MemoSlot::Refused;
			}
			memory.reservation()
		}
		Some(_) | None => Reservation::unbounded(),
	};
	match reservation.charge(bytes) {
		Charge::Hold => MemoSlot::Value(value, reservation),
		// The reservation drops here, so a refused value holds no bytes.
		Charge::Spill => MemoSlot::Refused,
	}
}

/// The register content that denotes the owned `value`: a scalar kind held as its kind, anything
/// else whole.
pub(super) fn scalar_of(value: Value) -> Scalar<'static> {
	match value {
		Value::None => Scalar::None,
		Value::Null => Scalar::Null,
		Value::Bool(v) => Scalar::Bool(v),
		Value::Number(v) => Scalar::Number(v),
		Value::Duration(v) => Scalar::Duration(v),
		Value::Datetime(v) => Scalar::Datetime(v),
		Value::Uuid(v) => Scalar::Uuid(v),
		other @ (Value::String(_)
		| Value::Array(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => Scalar::Value(Held::Owned(other)),
	}
}

/// Empties cell `index` of `ir`. A cell only this run state holds is emptied in place, which
/// releases what it held and allocates nothing; a cell another run state shares is replaced in
/// this one's vector by an empty cell, so the other keeps its value. A cell that does not exist
/// is empty already.
fn reset_cell(ir: &mut IrRunState, index: usize) {
	let Some(cell) = ir.memos.get_mut(index) else {
		return;
	};
	match Arc::get_mut(cell) {
		Some(cell) => drop(cell.slot.take()),
		None => *cell = Arc::default(),
	}
}

/// Empties the memo cells `owned` names, as a group's start does for the cells whose region the
/// group owns.
#[allow(dead_code, reason = "request L16-1 calls it from `start_group` over `Program.memo_owner`")]
pub(super) fn reset_cells(ir: &mut IrRunState, owned: impl Iterator<Item = u16>) {
	for cell in owned {
		reset_cell(ir, cell as usize);
	}
}

/// Makes `ir` hold cells `0..count`, each empty unless it holds one already. A cell exists as a
/// shared `Arc` only if it exists when [`memos_of`] clones the vector, so a statement whose morsels
/// are to share what one keeps sizes its run state to the program's cell count before the first
/// morsel is handed its cells.
#[allow(dead_code, reason = "request L16-1 calls it at a statement program's start")]
pub(super) fn size_memos(ir: &mut IrRunState, count: usize) {
	if ir.memos.len() < count {
		ir.memos.resize_with(count, Arc::default);
	}
}

/// The memo cells of `ir`, shared: what a morsel's worker is to read and fill beside the run that
/// handed it work. With no run state there are none.
pub(super) fn memos_of(ir: Option<&mut IrRunState>) -> Vec<Arc<MemoCell>> {
	ir.map_or_else(Vec::new, |ir| ir.memos.clone())
}

/// Makes `memos` the cells of `ir`, which the worker's own offers and reads then share with
/// every run state holding the same cells. With no run state they are dropped: the worker's
/// regions evaluate every time they are reached.
pub(super) fn install_memos(ir: Option<&mut IrRunState>, memos: Vec<Arc<MemoCell>>) {
	if let Some(ir) = ir {
		ir.memos = memos;
	}
}

/// Runs the loop of `spec` over the array in `regs[spec.items]` a batch of positions at a time,
/// appending to the array or set in `regs[spec.dst]`.
///
/// The kernel visits the positions `0..len` and reads and writes no other register. A batch spans
/// at most [`KernelSpec::batch`]`.max_iterations` positions, and holds the answer every position
/// gives before any of it reaches `dst`: the positions before the first failing one append, in
/// order, and then that failure is raised, so `dst` and the error equal what the serial body
/// leaves. A failure at a lower position wins over one at a higher position, whichever operation
/// raised it.
///
/// Each batch charges its positions to the fuel meter once and never yields; the run is seen to
/// be cancelled or out of time at the instruction after it.
///
/// A spec whose registers do not describe a well-formed body, or whose `items` register holds
/// anything but an array, answers an error before any position runs.
pub(super) fn run_kernel(
	spec: &KernelSpec,
	program: &Program,
	row: &dyn RowReader,
	regs: &mut [Scalar<'_>],
) -> anyhow::Result<()> {
	validate(spec, regs.len())?;
	// `dst` leaves the register file for the run, so that `items` is read from it while `dst` is
	// appended to. It is put back on every path.
	let dst = spec.dst as usize;
	let mut held = std::mem::replace(&mut regs[dst], Scalar::None);
	let answer = kernel_over(spec, program, row, regs, &mut held);
	regs[dst] = held;
	answer
}

/// What one input of the kernel reads, resolved once per run.
enum Resolved<'p> {
	/// The element.
	Elem,
	/// The named field of the element.
	Field(&'p str),
	/// The value of an operand the body does not change between positions.
	Invariant(Value),
}

/// The elements a batch reads and where its positions start.
struct KernelSource<'a> {
	items: &'a [Value],
	resolved: &'a [Resolved<'a>],
	/// What a field the element lacks reads as.
	none: &'a Value,
	base: usize,
}

/// The run of [`run_kernel`] once `regs` and `spec` are known to be well formed.
fn kernel_over(
	spec: &KernelSpec,
	program: &Program,
	row: &dyn RowReader,
	regs: &[Scalar<'_>],
	dst: &mut Scalar<'_>,
) -> anyhow::Result<()> {
	let items: &[Value] = match regs[spec.items as usize].as_whole() {
		Some(Value::Array(array)) => &array.0,
		Some(other) => {
			bail!("kernel items register {} holds {other:?}, which is not an array", spec.items)
		}
		None => bail!(
			"kernel items register {} holds {:?}, which is not an array",
			spec.items,
			regs[spec.items as usize]
		),
	};
	// A loop over no elements reads nothing, as the serial loop does not.
	if items.is_empty() {
		return Ok(());
	}
	let mut resolved = Vec::with_capacity(spec.inputs.len());
	for input in &spec.inputs {
		resolved.push(match input {
			KernelInput::Elem => Resolved::Elem,
			KernelInput::ElemField(field) => {
				Resolved::Field(
					program.fields.get(*field as usize).map(|name| name.as_str()).ok_or_else(
						|| anyhow::anyhow!("kernel field {field} is not in the program"),
					)?,
				)
			}
			KernelInput::Invariant(operand) => {
				Resolved::Invariant(invariant(*operand, program, row, regs)?)
			}
		});
	}
	let none = Value::None;
	let width = (spec.batch.max_iterations as usize).max(1);
	let mut scratch = Scratch::new(spec, width.min(items.len()));
	drive(spec, &resolved, items, &none, &mut scratch, dst)
}

/// The value of an operand the body does not change between positions.
fn invariant(
	operand: Operand,
	program: &Program,
	row: &dyn RowReader,
	regs: &[Scalar<'_>],
) -> anyhow::Result<Value> {
	Ok(match operand {
		Operand::Reg(reg) => match regs.get(reg as usize) {
			Some(scalar) => scalar.to_value(),
			None => bail!("kernel invariant register {reg} is not in the register file"),
		},
		Operand::Const(index) => match program.consts.get(index as usize) {
			Some(value) => value.clone(),
			None => bail!("kernel constant {index} is not in the program"),
		},
		Operand::Field(index) => match program.fields.get(index as usize) {
			Some(name) => row.field(name)?.into_value(),
			None => bail!("kernel field {index} is not in the program"),
		},
	})
}

/// Runs every batch of the loop over `items` against `scratch`.
fn drive<'a>(
	spec: &KernelSpec,
	resolved: &'a [Resolved<'a>],
	items: &'a [Value],
	none: &'a Value,
	scratch: &mut Scratch<'a>,
	dst: &mut Scalar<'_>,
) -> anyhow::Result<()> {
	let width = (spec.batch.max_iterations as usize).max(1);
	let mut base = 0;
	while base < items.len() {
		let n = width.min(items.len() - base);
		let source = KernelSource {
			items,
			resolved,
			none,
			base,
		};
		let typed = type_holds(spec.col, &source, n);
		let mut verdict = scratch.evaluate(spec, &source, n, typed)?;
		// A result that does not fit the column the typed arm gave its register is no failure of
		// the batch: the batch runs again on the generic arm, which holds any value.
		if typed && matches!(verdict, Verdict::Decline) {
			verdict = scratch.evaluate(spec, &source, n, false)?;
		}
		let failure = match verdict {
			Verdict::Done(failure) => failure,
			Verdict::Decline => bail!("the generic arm of a kernel declined a batch"),
		};
		fuel::charge(fuel::units(n));
		let upto = failure.as_ref().map_or(n, |(position, _)| *position);
		let out = &scratch.cols[spec.out.0 as usize];
		for position in 0..upto {
			match spec.sink {
				KernelSink::Map => push_array(dst, out.at(position).to_value())?,
				KernelSink::FilterElem {
					collect,
				} => {
					if out.truthy_at(position) {
						let element = items[base + position].clone();
						if collect {
							push_collect(dst, element);
						} else {
							push_array(dst, element)?;
						}
					}
				}
			}
		}
		if let Some((_, error)) = failure {
			return Err(error);
		}
		base += n;
	}
	Ok(())
}

/// `ArrayPush`: appends `value` to the array `dst` holds.
fn push_array(dst: &mut Scalar<'_>, value: Value) -> anyhow::Result<()> {
	if let Scalar::Value(held) = dst
		&& let Value::Array(items) = held.to_mut()
	{
		items.push(value);
		return Ok(());
	}
	bail!("appending to {dst:?}, which is not an array")
}

/// `CollectPush`: appends `value` to the array `dst` holds, or inserts it into the set.
fn push_collect(dst: &mut Scalar<'_>, value: Value) {
	if let Scalar::Value(held) = dst {
		let held = held.to_mut();
		if let Value::Array(array) = held {
			array.push(value);
		} else if let Value::Set(set) = held {
			set.insert(value);
		}
	}
}

/// Whether every element-derived input of the positions `source.base..source.base + n` holds a
/// value of the kind `col`. An element field the element lacks reads `NONE`, which is of no kind.
fn type_holds(col: ColKind, source: &KernelSource<'_>, n: usize) -> bool {
	let span = &source.items[source.base..source.base + n];
	source.resolved.iter().all(|input| match input {
		Resolved::Elem => span.iter().all(|element| is_kind(col, element)),
		Resolved::Field(name) => {
			span.iter().all(|element| is_kind(col, field_of(element, name, source.none)))
		}
		Resolved::Invariant(_) => true,
	})
}

/// Whether `value` is of the kind `col`.
fn is_kind(col: ColKind, value: &Value) -> bool {
	match col {
		ColKind::Int => matches!(value, Value::Number(Number::Int(_))),
		ColKind::Float => matches!(value, Value::Number(Number::Float(_))),
		ColKind::Bool => matches!(value, Value::Bool(_)),
		ColKind::Str => matches!(value, Value::String(_)),
	}
}

/// Field `name` of `element`; `none` when the element is no object or lacks the field.
fn field_of<'a>(element: &'a Value, name: &str, none: &'a Value) -> &'a Value {
	if let Value::Object(object) = element
		&& let Some(value) = object.get(name)
	{
		return value;
	}
	none
}

/// Checks that `spec` describes a body that runs without reading a register no operation or
/// live mask provides: every register an operation names is in `spec.regs`, an operation reads
/// only registers written before it (or masks no operation writes, which are the live mask), a
/// mask is read from a mask register, and no operation writes a register it reads.
fn validate(spec: &KernelSpec, regs: usize) -> anyhow::Result<()> {
	let (items, dst) = (spec.items as usize, spec.dst as usize);
	if items >= regs || dst >= regs {
		bail!("kernel registers {items} and {dst} are not both in a register file of {regs}");
	}
	if items == dst {
		bail!("kernel register {items} is both the items and the destination");
	}
	let count = spec.regs.len();
	let mut written_by_an_op = SmallVec::<[bool; 16]>::from_elem(false, count);
	for op in &spec.ops {
		if let Some(written) = written_register(*op)
			&& let Some(slot) = written_by_an_op.get_mut(written.0 as usize)
		{
			*slot = true;
		}
	}
	let mut ready = SmallVec::<[bool; 16]>::from_elem(false, count);
	let known = |reg: KReg| -> anyhow::Result<usize> {
		let index = reg.0 as usize;
		if index < count {
			Ok(index)
		} else {
			bail!("kernel register {index} is not among the {count} the spec declares")
		}
	};
	// A register an operation reads: written before, or a mask nothing writes.
	let read = |reg: KReg, ready: &[bool]| -> anyhow::Result<usize> {
		let index = known(reg)?;
		let live_mask = !written_by_an_op[index] && spec.regs[index] == KRegRepr::Mask;
		if ready[index] || live_mask {
			Ok(index)
		} else {
			bail!("kernel register {index} is read before an operation writes it")
		}
	};
	let read_mask = |reg: KReg, ready: &[bool]| -> anyhow::Result<usize> {
		let index = read(reg, ready)?;
		if spec.regs[index] == KRegRepr::Mask {
			Ok(index)
		} else {
			bail!("kernel register {index} is read as a mask but is not one")
		}
	};
	let write = |reg: KReg, reads: &[usize]| -> anyhow::Result<usize> {
		let index = known(reg)?;
		if reads.contains(&index) {
			bail!("kernel register {index} is written by an operation that reads it")
		}
		Ok(index)
	};
	for op in &spec.ops {
		match *op {
			KernelOp::Input {
				input,
				dst,
			} => {
				if input as usize >= spec.inputs.len() {
					bail!(
						"kernel input {input} is not among the {} the spec declares",
						spec.inputs.len()
					);
				}
				let dst = write(dst, &[])?;
				if spec.regs[dst] == KRegRepr::Mask {
					bail!("kernel register {dst} holds an input but is a mask");
				}
				ready[dst] = true;
			}
			KernelOp::Cmp {
				a,
				b,
				dst,
				under,
				..
			}
			| KernelOp::Arith {
				a,
				b,
				dst,
				under,
				..
			} => {
				let reads = [read(a, &ready)?, read(b, &ready)?, read_mask(under, &ready)?];
				ready[write(dst, &reads)?] = true;
			}
			KernelOp::Neg {
				a,
				dst,
				under,
			}
			| KernelOp::Not {
				a,
				dst,
				under,
			} => {
				let reads = [read(a, &ready)?, read_mask(under, &ready)?];
				ready[write(dst, &reads)?] = true;
			}
			KernelOp::Truthy {
				a,
				dst,
				under,
			} => {
				let reads = [read(a, &ready)?, read_mask(under, &ready)?];
				let dst = write(dst, &reads)?;
				if spec.regs[dst] != KRegRepr::Mask {
					bail!("kernel register {dst} holds a mask but is not one");
				}
				ready[dst] = true;
			}
			KernelOp::MaskAnd {
				a,
				b,
				dst,
			}
			| KernelOp::MaskAndNot {
				a,
				b,
				dst,
			} => {
				let reads = [read_mask(a, &ready)?, read_mask(b, &ready)?];
				let dst = write(dst, &reads)?;
				if spec.regs[dst] != KRegRepr::Mask {
					bail!("kernel register {dst} holds a mask but is not one");
				}
				ready[dst] = true;
			}
			KernelOp::Blend {
				mask,
				then,
				else_,
				dst,
			} => {
				let reads = [read_mask(mask, &ready)?, read(then, &ready)?, read(else_, &ready)?];
				ready[write(dst, &reads)?] = true;
			}
		}
	}
	read(spec.out, &ready)?;
	Ok(())
}

/// The register `op` writes.
fn written_register(op: KernelOp) -> Option<KReg> {
	Some(match op {
		KernelOp::Input {
			dst,
			..
		}
		| KernelOp::Cmp {
			dst,
			..
		}
		| KernelOp::Arith {
			dst,
			..
		}
		| KernelOp::Neg {
			dst,
			..
		}
		| KernelOp::Not {
			dst,
			..
		}
		| KernelOp::Truthy {
			dst,
			..
		}
		| KernelOp::MaskAnd {
			dst,
			..
		}
		| KernelOp::MaskAndNot {
			dst,
			..
		}
		| KernelOp::Blend {
			dst,
			..
		} => dst,
	})
}

/// How a column holds its values for the batch in flight.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Store {
	/// Not yet shaped for a batch.
	#[default]
	Unset,
	Int,
	Float,
	Bool,
	Str,
	/// One [`Value`] per position.
	Boxed,
	/// One boolean per position: whether the position is live.
	Mask,
}

/// One kernel register's values over the positions of a batch.
///
/// Each store has its own buffer, and a buffer keeps its capacity from batch to batch, so a run
/// allocates a column once. A position an operation did not run over holds a placeholder that no
/// live position reads.
#[derive(Default)]
struct Column<'a> {
	store: Store,
	ints: Vec<i64>,
	floats: Vec<f64>,
	bools: Vec<bool>,
	strs: Vec<&'a str>,
	vals: Vec<Cow<'a, Value>>,
}

impl<'a> Column<'a> {
	/// A column for a register of `repr`, with room for `capacity` positions in the buffer its
	/// typed arm uses.
	fn with_capacity(repr: KRegRepr, capacity: usize) -> Self {
		let mut column = Self::default();
		match repr {
			KRegRepr::Col(ColKind::Int) => column.ints.reserve(capacity),
			KRegRepr::Col(ColKind::Float) => column.floats.reserve(capacity),
			KRegRepr::Col(ColKind::Bool) | KRegRepr::Mask => column.bools.reserve(capacity),
			KRegRepr::Col(ColKind::Str) => column.strs.reserve(capacity),
			KRegRepr::Boxed => column.vals.reserve(capacity),
		}
		column
	}

	/// Shapes the column to hold `n` placeholders in `store`, or `n` live positions when `live`.
	fn reset(&mut self, store: Store, n: usize, live: bool) {
		self.store = store;
		match store {
			Store::Unset => {}
			Store::Int => {
				self.ints.clear();
				self.ints.resize(n, 0);
			}
			Store::Float => {
				self.floats.clear();
				self.floats.resize(n, 0.0);
			}
			Store::Bool | Store::Mask => {
				self.bools.clear();
				self.bools.resize(n, live);
			}
			Store::Str => {
				self.strs.clear();
				self.strs.resize(n, "");
			}
			Store::Boxed => {
				self.vals.clear();
				self.vals.resize_with(n, || Cow::Owned(Value::None));
			}
		}
	}

	/// The value at `p`.
	fn at(&self, p: usize) -> Scalar<'_> {
		match self.store {
			// A register no operation wrote is a mask or is refused by `validate`.
			Store::Unset => Scalar::None,
			Store::Int => Scalar::Number(Number::Int(self.ints[p])),
			Store::Float => Scalar::Number(Number::Float(self.floats[p])),
			Store::Bool | Store::Mask => Scalar::Bool(self.bools[p]),
			Store::Str => Scalar::Str(self.strs[p]),
			Store::Boxed => Scalar::of(&self.vals[p]),
		}
	}

	/// Whether the value at `p` is truthy, as [`Value::is_truthy`] answers.
	fn truthy_at(&self, p: usize) -> bool {
		match self.store {
			Store::Unset => false,
			Store::Int => self.ints[p] != 0,
			Store::Float => self.floats[p] != 0.0,
			Store::Bool | Store::Mask => self.bools[p],
			Store::Str => !self.strs[p].is_empty(),
			Store::Boxed => self.vals[p].is_truthy(),
		}
	}

	/// Holds `value` at `p` as an input. `false` when the column cannot hold it.
	fn load(&mut self, p: usize, value: &'a Value) -> anyhow::Result<bool> {
		Ok(match self.store {
			Store::Boxed => {
				self.vals[p] = Cow::Borrowed(value);
				true
			}
			Store::Int => {
				if let Value::Number(Number::Int(v)) = value {
					self.ints[p] = *v;
					true
				} else {
					false
				}
			}
			Store::Float => {
				if let Value::Number(Number::Float(v)) = value {
					self.floats[p] = *v;
					true
				} else {
					false
				}
			}
			Store::Bool => {
				if let Value::Bool(v) = value {
					self.bools[p] = *v;
					true
				} else {
					false
				}
			}
			Store::Str => {
				if let Value::String(v) = value {
					self.strs[p] = v.as_str();
					true
				} else {
					false
				}
			}
			Store::Unset | Store::Mask => {
				bail!("a kernel input was loaded into a column that holds none")
			}
		})
	}

	/// Holds the owned `value` at `p`. `false` when the column's kind cannot hold it. A mask holds
	/// booleans only, and anything else is an error.
	fn put_value(&mut self, p: usize, value: Value) -> anyhow::Result<bool> {
		Ok(match self.store {
			Store::Boxed => {
				self.vals[p] = Cow::Owned(value);
				true
			}
			Store::Int => {
				if let Value::Number(Number::Int(v)) = value {
					self.ints[p] = v;
					true
				} else {
					false
				}
			}
			Store::Float => {
				if let Value::Number(Number::Float(v)) = value {
					self.floats[p] = v;
					true
				} else {
					false
				}
			}
			Store::Bool => {
				if let Value::Bool(v) = value {
					self.bools[p] = v;
					true
				} else {
					false
				}
			}
			Store::Str => false,
			Store::Mask => {
				if let Value::Bool(v) = value {
					self.bools[p] = v;
					true
				} else {
					bail!("a kernel mask register was given {value:?}, which is not a boolean")
				}
			}
			Store::Unset => bail!("a kernel result was stored in a column that holds none"),
		})
	}

	/// Holds `v` at `p`; `false` when the column's kind cannot hold a boolean.
	fn put_bool(&mut self, p: usize, v: bool) -> anyhow::Result<bool> {
		match self.store {
			Store::Bool | Store::Mask => {
				self.bools[p] = v;
				Ok(true)
			}
			Store::Boxed => {
				self.vals[p] = Cow::Owned(Value::Bool(v));
				Ok(true)
			}
			Store::Int | Store::Float | Store::Str => Ok(false),
			Store::Unset => bail!("a kernel result was stored in a column that holds none"),
		}
	}

	/// Holds `v` at `p`; `false` when the column's kind cannot hold an integer.
	fn put_int(&mut self, p: usize, v: i64) -> anyhow::Result<bool> {
		match self.store {
			Store::Int => {
				self.ints[p] = v;
				Ok(true)
			}
			Store::Boxed => {
				self.vals[p] = Cow::Owned(Value::Number(Number::Int(v)));
				Ok(true)
			}
			Store::Float | Store::Bool | Store::Str => Ok(false),
			Store::Mask => bail!("a kernel mask register was given an integer"),
			Store::Unset => bail!("a kernel result was stored in a column that holds none"),
		}
	}

	/// Holds `v` at `p`; `false` when the column's kind cannot hold a float.
	fn put_float(&mut self, p: usize, v: f64) -> anyhow::Result<bool> {
		match self.store {
			Store::Float => {
				self.floats[p] = v;
				Ok(true)
			}
			Store::Boxed => {
				self.vals[p] = Cow::Owned(Value::Number(Number::Float(v)));
				Ok(true)
			}
			Store::Int | Store::Bool | Store::Str => Ok(false),
			Store::Mask => bail!("a kernel mask register was given a float"),
			Store::Unset => bail!("a kernel result was stored in a column that holds none"),
		}
	}
}

/// The failures of the batch in flight.
#[derive(Default)]
struct Faults {
	/// Whether the position failed an operation; such a position is live in no later mask.
	dead: Vec<bool>,
	/// The failure of the lowest failing position.
	first: Option<(usize, anyhow::Error)>,
}

impl Faults {
	fn reset(&mut self, n: usize) {
		self.dead.clear();
		self.dead.resize(n, false);
		self.first = None;
	}

	/// Records that an operation failed at `p` with `error`. The position's own first failure
	/// stands, and a lower position's failure replaces a higher one's.
	fn record(&mut self, p: usize, error: anyhow::Error) {
		self.dead[p] = true;
		let lower = match &self.first {
			Some((at, _)) => p < *at,
			None => true,
		};
		if lower {
			self.first = Some((p, error));
		}
	}

	/// Whether `p` is live in `under` and has not failed.
	fn live(&self, under: &Column<'_>, p: usize) -> bool {
		under.bools[p] && !self.dead[p]
	}
}

/// How a batch ended.
enum Verdict {
	/// Every operation ran, and the lowest failing position's failure is the batch's, if any.
	Done(Option<(usize, anyhow::Error)>),
	/// An operation's answer did not fit the column its register has in the typed arm.
	Decline,
}

/// The buffers one kernel run reuses across its batches: nothing outlives the run.
struct Scratch<'a> {
	cols: Vec<Column<'a>>,
	/// Whether no operation writes the register, so that it is the live mask a body starts under.
	live: Vec<bool>,
	faults: Faults,
}

impl<'a> Scratch<'a> {
	/// Buffers for a run whose batches span at most `width` positions.
	fn new(spec: &KernelSpec, width: usize) -> Self {
		let mut live = vec![true; spec.regs.len()];
		for op in &spec.ops {
			if let Some(written) = written_register(*op)
				&& let Some(slot) = live.get_mut(written.0 as usize)
			{
				*slot = false;
			}
		}
		let mut faults = Faults::default();
		faults.dead.reserve(width);
		Self {
			cols: spec.regs.iter().map(|repr| Column::with_capacity(*repr, width)).collect(),
			live,
			faults,
		}
	}

	/// Evaluates the body over the `n` positions of `source`, on the typed arm when `typed`.
	fn evaluate(
		&mut self,
		spec: &KernelSpec,
		source: &KernelSource<'a>,
		n: usize,
		typed: bool,
	) -> anyhow::Result<Verdict> {
		for (index, repr) in spec.regs.iter().enumerate() {
			let store = match *repr {
				KRegRepr::Col(kind) if typed => match kind {
					ColKind::Int => Store::Int,
					ColKind::Float => Store::Float,
					ColKind::Bool => Store::Bool,
					ColKind::Str => Store::Str,
				},
				KRegRepr::Col(_) | KRegRepr::Boxed => Store::Boxed,
				KRegRepr::Mask => Store::Mask,
			};
			let live = store == Store::Mask && self.live[index];
			self.cols[index].reset(store, n, live);
		}
		self.faults.reset(n);
		for op in &spec.ops {
			if !self.run_op(*op, source, n)? {
				return Ok(Verdict::Decline);
			}
		}
		Ok(Verdict::Done(self.faults.first.take()))
	}

	/// Runs `body` with register `dst` taken out of the file, so that it is written while the
	/// other registers are read. `validate` refuses an operation that reads its own destination.
	fn with_dst<R>(
		&mut self,
		dst: KReg,
		body: impl FnOnce(&[Column<'a>], &mut Column<'a>, &mut Faults) -> R,
	) -> R {
		let index = dst.0 as usize;
		let mut taken = std::mem::take(&mut self.cols[index]);
		let answer = body(&self.cols, &mut taken, &mut self.faults);
		self.cols[index] = taken;
		answer
	}

	/// Runs one operation over the positions live for it. `false` when the typed arm cannot hold
	/// an answer.
	fn run_op(
		&mut self,
		op: KernelOp,
		source: &KernelSource<'a>,
		n: usize,
	) -> anyhow::Result<bool> {
		match op {
			KernelOp::Input {
				input,
				dst,
			} => self.with_dst(dst, |_, d, _| load_input(source, input, d, n)),
			KernelOp::Cmp {
				op,
				a,
				b,
				dst,
				under,
			} => self.with_dst(dst, |cols, d, faults| {
				op_cmp(
					op,
					&cols[a.0 as usize],
					&cols[b.0 as usize],
					d,
					&cols[under.0 as usize],
					faults,
					n,
				)
			}),
			KernelOp::Arith {
				op,
				a,
				b,
				dst,
				under,
			} => self.with_dst(dst, |cols, d, faults| {
				op_arith(
					op,
					&cols[a.0 as usize],
					&cols[b.0 as usize],
					d,
					&cols[under.0 as usize],
					faults,
					n,
				)
			}),
			KernelOp::Neg {
				a,
				dst,
				under,
			} => self.with_dst(dst, |cols, d, faults| {
				op_unary(Unary::Neg, &cols[a.0 as usize], d, &cols[under.0 as usize], faults, n)
			}),
			KernelOp::Not {
				a,
				dst,
				under,
			} => self.with_dst(dst, |cols, d, faults| {
				op_unary(Unary::Not, &cols[a.0 as usize], d, &cols[under.0 as usize], faults, n)
			}),
			KernelOp::Truthy {
				a,
				dst,
				under,
			} => self.with_dst(dst, |cols, d, faults| {
				let (a, under) = (&cols[a.0 as usize], &cols[under.0 as usize]);
				for p in 0..n {
					d.bools[p] = faults.live(under, p) && a.truthy_at(p);
				}
				Ok(true)
			}),
			KernelOp::MaskAnd {
				a,
				b,
				dst,
			} => self.with_dst(dst, |cols, d, _| {
				let (a, b) = (&cols[a.0 as usize], &cols[b.0 as usize]);
				for p in 0..n {
					d.bools[p] = a.bools[p] && b.bools[p];
				}
				Ok(true)
			}),
			KernelOp::MaskAndNot {
				a,
				b,
				dst,
			} => self.with_dst(dst, |cols, d, _| {
				let (a, b) = (&cols[a.0 as usize], &cols[b.0 as usize]);
				for p in 0..n {
					d.bools[p] = a.bools[p] && !b.bools[p];
				}
				Ok(true)
			}),
			KernelOp::Blend {
				mask,
				then,
				else_,
				dst,
			} => self.with_dst(dst, |cols, d, _| {
				let (mask, then, else_) =
					(&cols[mask.0 as usize], &cols[then.0 as usize], &cols[else_.0 as usize]);
				for p in 0..n {
					let chosen = if mask.bools[p] {
						then
					} else {
						else_
					};
					if !d.put_value(p, chosen.at(p).to_value())? {
						return Ok(false);
					}
				}
				Ok(true)
			}),
		}
	}
}

/// Loads input number `input` into `d` for the `n` positions of `source`.
fn load_input<'a>(
	source: &KernelSource<'a>,
	input: u8,
	d: &mut Column<'a>,
	n: usize,
) -> anyhow::Result<bool> {
	let (items, resolved, none) = (source.items, source.resolved, source.none);
	let Some(input) = resolved.get(input as usize) else {
		bail!("kernel input {input} is not among the {} the run resolved", resolved.len());
	};
	for p in 0..n {
		let value: &'a Value = match input {
			Resolved::Elem => &items[source.base + p],
			Resolved::Field(name) => field_of(&items[source.base + p], name, none),
			Resolved::Invariant(value) => value,
		};
		if !d.load(p, value)? {
			return Ok(false);
		}
	}
	Ok(true)
}

/// A comparison a column pair of one kind answers natively.
#[derive(Clone, Copy)]
enum Rel {
	Eq,
	Ne,
	Lt,
	Le,
	Gt,
	Ge,
}

impl Rel {
	/// The relation `op` is when it is one of the six orderings and equalities.
	fn of(op: CompareOp) -> Option<Self> {
		match op {
			CompareOp::Equal | CompareOp::ExactEqual => Some(Rel::Eq),
			CompareOp::NotEqual => Some(Rel::Ne),
			CompareOp::LessThan => Some(Rel::Lt),
			CompareOp::LessThanEqual => Some(Rel::Le),
			CompareOp::MoreThan => Some(Rel::Gt),
			CompareOp::MoreThanEqual => Some(Rel::Ge),
			CompareOp::AllEqual
			| CompareOp::AnyEqual
			| CompareOp::Contain
			| CompareOp::NotContain
			| CompareOp::ContainAll
			| CompareOp::ContainAny
			| CompareOp::ContainNone
			| CompareOp::Inside
			| CompareOp::NotInside
			| CompareOp::AllInside
			| CompareOp::AnyInside
			| CompareOp::NoneInside
			| CompareOp::Outside
			| CompareOp::Intersects => None,
		}
	}

	/// Whether the relation holds between operands that order as `ordering`.
	fn holds(self, ordering: Ordering) -> bool {
		match self {
			Rel::Eq => ordering == Ordering::Equal,
			Rel::Ne => ordering != Ordering::Equal,
			Rel::Lt => ordering == Ordering::Less,
			Rel::Le => ordering != Ordering::Greater,
			Rel::Gt => ordering == Ordering::Greater,
			Rel::Ge => ordering != Ordering::Less,
		}
	}
}

/// The answer of `rel` over the pair at `p`, when both columns are of one kind a native
/// comparison answers exactly as the scalar comparison does.
fn native_cmp(rel: Rel, a: &Column<'_>, b: &Column<'_>, p: usize) -> Option<bool> {
	match (a.store, b.store) {
		(Store::Int, Store::Int) => Some(rel.holds(a.ints[p].cmp(&b.ints[p]))),
		// A float number orders by a total order that agrees with IEEE where neither is NaN.
		(Store::Float, Store::Float) => {
			a.floats[p].partial_cmp(&b.floats[p]).map(|ordering| rel.holds(ordering))
		}
		(Store::Bool, Store::Bool) => Some(rel.holds(a.bools[p].cmp(&b.bools[p]))),
		// Strings compare natively for equality only; their order is the scalar function's.
		(Store::Str, Store::Str) => match rel {
			Rel::Eq => Some(a.strs[p] == b.strs[p]),
			Rel::Ne => Some(a.strs[p] != b.strs[p]),
			Rel::Lt | Rel::Le | Rel::Gt | Rel::Ge => None,
		},
		_ => None,
	}
}

/// `Cmp`: `d[p] = a[p] op b[p]` for every position live in `under`.
fn op_cmp<'a>(
	op: CompareOp,
	a: &Column<'a>,
	b: &Column<'a>,
	d: &mut Column<'a>,
	under: &Column<'a>,
	faults: &mut Faults,
	n: usize,
) -> anyhow::Result<bool> {
	let rel = Rel::of(op);
	for p in 0..n {
		if !faults.live(under, p) {
			continue;
		}
		let stored = match rel.and_then(|rel| native_cmp(rel, a, b, p)) {
			Some(answer) => d.put_bool(p, answer)?,
			None => match op.apply_scalar(&a.at(p), &b.at(p)) {
				Ok(answer) => d.put_value(p, answer.to_value())?,
				Err(error) => {
					faults.record(p, error);
					true
				}
			},
		};
		if !stored {
			return Ok(false);
		}
	}
	Ok(true)
}

/// An arithmetic a column pair of one kind answers natively.
#[derive(Clone, Copy)]
enum Calc {
	Add,
	Sub,
	Mul,
	Rem,
}

impl Calc {
	/// The arithmetic `op` is when a native path can answer it.
	fn of(op: ArithOp) -> Option<Self> {
		match op {
			ArithOp::Add => Some(Calc::Add),
			ArithOp::Subtract => Some(Calc::Sub),
			ArithOp::Multiply => Some(Calc::Mul),
			ArithOp::Remainder => Some(Calc::Rem),
			ArithOp::Divide
			| ArithOp::Power
			| ArithOp::Range {
				..
			} => None,
		}
	}

	/// `a op b` over integers; `None` where the scalar function raises.
	fn ints(self, a: i64, b: i64) -> Option<i64> {
		match self {
			Calc::Add => a.checked_add(b),
			Calc::Sub => a.checked_sub(b),
			Calc::Mul => a.checked_mul(b),
			Calc::Rem => a.checked_rem(b),
		}
	}

	/// `a op b` over floats, which is IEEE arithmetic.
	fn floats(self, a: f64, b: f64) -> f64 {
		match self {
			Calc::Add => a + b,
			Calc::Sub => a - b,
			Calc::Mul => a * b,
			Calc::Rem => a % b,
		}
	}
}

/// `Arith`: `d[p] = a[p] op b[p]` for every position live in `under`.
fn op_arith<'a>(
	op: ArithOp,
	a: &Column<'a>,
	b: &Column<'a>,
	d: &mut Column<'a>,
	under: &Column<'a>,
	faults: &mut Faults,
	n: usize,
) -> anyhow::Result<bool> {
	let calc = Calc::of(op);
	for p in 0..n {
		if !faults.live(under, p) {
			continue;
		}
		let native = match (calc, a.store, b.store) {
			(Some(calc), Store::Int, Store::Int) => {
				calc.ints(a.ints[p], b.ints[p]).map(|v| d.put_int(p, v))
			}
			(Some(calc), Store::Float, Store::Float) => {
				Some(d.put_float(p, calc.floats(a.floats[p], b.floats[p])))
			}
			_ => None,
		};
		let stored = match native {
			Some(stored) => stored?,
			None => match op.apply(a.at(p).to_value(), b.at(p).to_value()) {
				Ok(answer) => d.put_value(p, answer)?,
				Err(error) => {
					faults.record(p, error);
					true
				}
			},
		};
		if !stored {
			return Ok(false);
		}
	}
	Ok(true)
}

/// A unary operation of a kernel body.
#[derive(Clone, Copy)]
enum Unary {
	Neg,
	Not,
}

/// `Neg` and `Not`: `d[p] = op a[p]` for every position live in `under`.
fn op_unary<'a>(
	op: Unary,
	a: &Column<'a>,
	d: &mut Column<'a>,
	under: &Column<'a>,
	faults: &mut Faults,
	n: usize,
) -> anyhow::Result<bool> {
	for p in 0..n {
		if !faults.live(under, p) {
			continue;
		}
		let native = match (op, a.store) {
			(Unary::Neg, Store::Int) => a.ints[p].checked_neg().map(|v| d.put_int(p, v)),
			(Unary::Neg, Store::Float) => Some(d.put_float(p, -a.floats[p])),
			(Unary::Not, Store::Bool) => Some(d.put_bool(p, !a.bools[p])),
			_ => None,
		};
		let stored = match native {
			Some(stored) => stored?,
			None => {
				let scalar = match op {
					Unary::Neg => UnaryOp::Negate,
					Unary::Not => UnaryOp::Not,
				};
				match scalar.apply(a.at(p).to_value(), &[]) {
					Ok(answer) => d.put_value(p, answer)?,
					Err(error) => {
						faults.record(p, error);
						true
					}
				}
			}
		};
		if !stored {
			return Ok(false);
		}
	}
	Ok(true)
}
