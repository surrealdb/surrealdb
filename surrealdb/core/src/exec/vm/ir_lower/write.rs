//! Write fan-out emission and the release guard.
//!
//! The emitters append the write forms to a code buffer. The guard refuses a program that plans
//! morsels over a pipeline that writes, unless the pipeline carries a write cut.
//!
//! # The forms
//!
//! A write body that fans out is cut in two: the part ahead of the cut computes a candidate row on
//! any task, and the part after it, the residue, applies candidates in delivery order.
//!
//! - [`emit_resume`] is the body's first instruction. It installs a pending candidate and jumps to
//!   `cut` or `skip`, and falls through when none is pending.
//! - [`emit_cut`] is placed before the residue, only in the arm of the body that reaches it. It is
//!   a no-op in a serial run, whose next instruction is the `cut` label.
//! - [`emit_skip`] is placed immediately before the end that skips the row.
//! - [`emit_off_path`] is placed at the first instruction of every arm a row can take that is not
//!   the cut path.
//!
//! # `live`
//!
//! [`WriteCutEmit::live`] lists the registers written ahead of the cut and read after it. It is
//! computed from the emitted code by [`live_across`], a linear scan that holds no flow analysis:
//! it never follows a jump, and counts a register as written when any instruction ahead of the cut
//! writes it and as read when any instruction after the cut reads it. The result is a superset of
//! the registers the residue needs, which only costs bytes. The scan assumes no jump leads from
//! the code after the cut back into the code ahead of it, which holds for a write body without a
//! loop of its own. An instruction the scan has no entry for counts as writing and reading every
//! register.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;

use crate::exec::AccessMode;
use crate::exec::ir::error::IrError;
use crate::exec::planner::placement::{Combine, MorselPlan, Parallel};
use crate::exec::vm::ir_instr::{IrForm, IrInstr, WriteCutSpec, WriteResumeSpec};
use crate::exec::vm::program::{BlockInstr, Instr, Label, Operand, Pipeline, Program, ROW, Reg};

/// The labels and live registers of one write body's cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::exec::vm) struct WriteCutEmit {
	/// Where a candidate that stopped at the cut continues.
	pub(in crate::exec::vm) cut: Label,
	/// Where a candidate that stopped at the skip continues.
	pub(in crate::exec::vm) skip: Label,
	/// The registers written ahead of the cut and read after it.
	pub(in crate::exec::vm) live: SmallVec<[u16; 8]>,
	/// Whether the part ahead of the cut encodes the record's after-image.
	pub(in crate::exec::vm) encode: bool,
}

impl WriteCutEmit {
	/// The cut of `code` at index `cut_at`, with `live` computed by [`live_across`].
	///
	/// `registers` is the register count of the pipeline `code` belongs to, [`ROW`] included.
	#[allow(dead_code)] // Called by the statement entry that lowers a `phys.write`.
	pub(in crate::exec::vm) fn scanned(
		code: &[Instr],
		cut_at: usize,
		registers: usize,
		cut: Label,
		skip: Label,
	) -> Self {
		Self {
			cut,
			skip,
			live: live_across(code, cut_at, registers),
			encode: false,
		}
	}
}

/// Appends the `WriteResume` that opens a write body.
#[allow(dead_code)] // Called by the statement entry that lowers a `phys.write`.
pub(in crate::exec::vm) fn emit_resume(code: &mut Vec<Instr>, cut: &WriteCutEmit) {
	code.push(Instr::Ir(IrInstr::write_resume(WriteResumeSpec {
		cut: cut.cut,
		skip: cut.skip,
		live: cut.live.clone(),
	})));
}

/// Appends the `WriteCut` that ends the part of the body that runs ahead of the ordered apply.
#[allow(dead_code)] // Called by the statement entry that lowers a `phys.write`.
pub(in crate::exec::vm) fn emit_cut(code: &mut Vec<Instr>, cut: &WriteCutEmit) {
	code.push(Instr::Ir(IrInstr::write_cut(WriteCutSpec {
		live: cut.live.clone(),
		encode: cut.encode,
	})));
}

/// Appends the `WriteSkip` that precedes the end that skips the row.
#[allow(dead_code)] // Called by the statement entry that lowers a `phys.write`.
pub(in crate::exec::vm) fn emit_skip(code: &mut Vec<Instr>) {
	code.push(Instr::Ir(IrInstr::write_skip()));
}

/// Appends the `WriteOffPath` that opens an arm of the body that is not the cut path.
#[allow(dead_code)] // Called by the statement entry that lowers a `phys.write`.
pub(in crate::exec::vm) fn emit_off_path(code: &mut Vec<Instr>) {
	code.push(Instr::Ir(IrInstr::write_off_path()));
}

/// The registers written by an instruction of `code[..cut_at]` and read by one of `code[cut_at..]`,
/// in ascending order and without [`ROW`].
///
/// `registers` is the register count of the pipeline, [`ROW`] included. A `cut_at` past the end
/// of `code` is treated as the end of `code`.
#[allow(dead_code)] // Called by `WriteCutEmit::scanned`.
pub(in crate::exec::vm) fn live_across(
	code: &[Instr],
	cut_at: usize,
	registers: usize,
) -> SmallVec<[u16; 8]> {
	let cut_at = cut_at.min(code.len());
	let mut written = Registers::new(registers);
	let mut read = Registers::new(registers);
	let mut touches = Vec::new();
	for (at, instr) in code.iter().enumerate() {
		let ahead = at < cut_at;
		touches.clear();
		if !touched(instr, &mut touches) {
			let unknown = if ahead {
				&mut written
			} else {
				&mut read
			};
			unknown.all = true;
			continue;
		}
		for touch in &touches {
			match touch {
				Touch::Write(reg) if ahead => written.mark(*reg),
				Touch::Read(reg) if !ahead => read.mark(*reg),
				Touch::Write(_) | Touch::Read(_) => {}
			}
		}
	}
	(u32::from(ROW) + 1..registers as u32)
		.filter_map(|reg| Reg::try_from(reg).ok())
		.filter(|reg| written.holds(*reg) && read.holds(*reg))
		.collect()
}

/// A set of registers, with a flag for every register.
struct Registers {
	held: Vec<bool>,
	all: bool,
}

impl Registers {
	fn new(registers: usize) -> Self {
		Self {
			held: vec![false; registers],
			all: false,
		}
	}

	fn mark(&mut self, reg: Reg) {
		if let Some(slot) = self.held.get_mut(usize::from(reg)) {
			*slot = true;
		}
	}

	fn holds(&self, reg: Reg) -> bool {
		self.all || self.held.get(usize::from(reg)).copied().unwrap_or(false)
	}
}

/// One register an instruction reads or writes.
enum Touch {
	Read(Reg),
	Write(Reg),
}

/// Appends the registers `instr` reads and writes to `out`, and answers whether `instr` has an
/// entry. A register an instruction updates in place is read and written.
///
/// An instruction with no entry answers `false` and appends nothing: the caller counts it as
/// reading and writing every register.
fn touched(instr: &Instr, out: &mut Vec<Touch>) -> bool {
	let read = |out: &mut Vec<Touch>, operand: &Operand| {
		if let Operand::Reg(reg) = operand {
			out.push(Touch::Read(*reg));
		}
	};
	let update = |out: &mut Vec<Touch>, reg: Reg| {
		out.push(Touch::Read(reg));
		out.push(Touch::Write(reg));
	};
	match instr {
		Instr::CoerceField {
			reg,
			..
		} => update(out, *reg),
		Instr::CompareFieldInt {
			dst,
			..
		}
		| Instr::NewArray {
			dst,
		}
		| Instr::NewSet {
			dst,
		}
		| Instr::NewObject {
			dst,
		}
		| Instr::LoadParam {
			dst,
			..
		}
		| Instr::LoadSlot {
			dst,
			..
		}
		| Instr::LoadRow {
			dst,
		}
		| Instr::LoadPath {
			dst,
			..
		} => out.push(Touch::Write(*dst)),
		Instr::JumpIfFieldInt {
			..
		}
		| Instr::FilterFieldInt {
			..
		}
		| Instr::CallAllowed {
			..
		}
		| Instr::Jump {
			..
		}
		| Instr::TryEnd
		// Entering a block evaluated in place reads and writes no register.
		| Instr::Block(BlockInstr::Enter)
		| Instr::EndRow
		| Instr::StopPipeline => {}
		Instr::JumpIfCompare {
			left,
			right,
			..
		}
		| Instr::FilterCompare {
			left,
			right,
			..
		} => {
			read(out, left);
			read(out, right);
		}
		Instr::Compare {
			left,
			right,
			dst,
			..
		}
		| Instr::Arith {
			left,
			right,
			dst,
			..
		} => {
			read(out, left);
			read(out, right);
			out.push(Touch::Write(*dst));
		}
		Instr::Unary {
			src,
			dst,
			..
		}
		| Instr::Access {
			src,
			dst,
			..
		}
		| Instr::Each {
			src,
			dst,
			..
		}
		| Instr::Load {
			dst,
			src,
		} => {
			read(out, src);
			out.push(Touch::Write(*dst));
		}
		Instr::ArrayPush {
			array: reg,
			value,
		}
		| Instr::CollectPush {
			collection: reg,
			value,
		}
		| Instr::SetField {
			object: reg,
			value,
			..
		} => {
			update(out, *reg);
			read(out, value);
		}
		Instr::Throw {
			src,
		}
		| Instr::EmitEnd {
			src,
		}
		| Instr::Filter {
			cond: src,
			..
		}
		| Instr::JumpIf {
			cond: src,
			..
		}
		| Instr::JumpIfNullish {
			cond: src,
			..
		}
		| Instr::JumpIfNone {
			cond: src,
			..
		} => read(out, src),
		Instr::Emit {
			src,
		} => out.push(Touch::Read(*src)),
		Instr::Nav {
			src,
			index,
			dst,
			..
		} => {
			read(out, src);
			if let Some(index) = index {
				read(out, index);
			}
			out.push(Touch::Write(*dst));
		}
		Instr::Call {
			args,
			dst,
			elem,
			..
		} => {
			for arg in args {
				read(out, arg);
			}
			if let Some(elem) = elem {
				out.push(Touch::Read(*elem));
			}
			out.push(Touch::Write(*dst));
		}
		Instr::Method {
			recv,
			args,
			dst,
			..
		} => {
			read(out, recv);
			for arg in args {
				read(out, arg);
			}
			out.push(Touch::Write(*dst));
		}
		Instr::BuildObject {
			dst,
			fields,
		} => {
			for (_, value) in fields.iter() {
				read(out, value);
			}
			out.push(Touch::Write(*dst));
		}
		// A write instruction works on the record in flight, which the write state holds, and
		// carries nothing in the registers.
		Instr::Write(_) => {}
		Instr::Ir(ir) => return ir_touched(ir, out),
		Instr::BeginComputing {
			..
		}
		| Instr::LandRow {
			..
		}
		| Instr::BindRow {
			..
		}
		| Instr::JumpIfPermissionsBypassed {
			..
		}
		| Instr::SortKey {
			..
		}
		| Instr::Fetch {
			..
		}
		| Instr::FilterBegin {
			..
		}
		| Instr::MapBegin {
			..
		}
		| Instr::FunctionEnter {
			..
		}
		| Instr::FunctionBind {
			..
		}
		| Instr::FunctionReturn {
			..
		}
		| Instr::FunctionCall {
			..
		}
		| Instr::ArrayArg {
			..
		}
		| Instr::ElemNext {
			..
		}
		| Instr::ClosureBind {
			..
		}
		| Instr::ClosureReturn {
			..
		}
		| Instr::Dive {
			..
		}
		| Instr::Subquery {
			..
		}
		| Instr::MakeClosure {
			..
		}
		| Instr::ClosureField {
			..
		}
		| Instr::RequireClosure {
			..
		}
		| Instr::Invoke {
			..
		}
		| Instr::Lookup {
			..
		}
		| Instr::Signal {
			..
		}
		| Instr::Extract {
			..
		}
		| Instr::KnnRank {
			..
		}
		| Instr::Measure {
			..
		}
		| Instr::Mock {
			..
		}
		| Instr::Recurse {
			..
		}
		| Instr::Destructure {
			..
		}
		| Instr::RecordId {
			..
		}
		| Instr::MethodResolve {
			..
		}
		| Instr::MethodClosure {
			..
		}
		| Instr::Skip {
			..
		}
		| Instr::Take {
			..
		}
		| Instr::JumpIfLimitReached {
			..
		}
		| Instr::SeedObject {
			..
		}
		| Instr::FetchRow {
			..
		}
		| Instr::RemoveField {
			..
		}
		| Instr::RemovePath {
			..
		}
		| Instr::Omit {
			..
		}
		| Instr::WriteProjection {
			..
		}
		| Instr::BuildObjectEmit {
			..
		}
		| Instr::SplitPush {
			..
		}
		| Instr::SortPush {
			..
		}
		| Instr::Distinct {
			..
		}
		| Instr::JoinProbe {
			..
		}
		| Instr::VerdictJump {
			..
		}
		| Instr::VerdictSet {
			..
		}
		| Instr::JoinPair {
			..
		}
		| Instr::GroupKey {
			..
		}
		| Instr::GroupSelect {
			..
		}
		| Instr::AccUpdate {
			..
		}
		| Instr::Next {
			..
		}
		| Instr::Store(_)
		| Instr::Block(_)
		| Instr::TryNone {
			..
		}
		| Instr::MethodRegion {
			..
		}
		| Instr::PermissionRegion {
			..
		}
		| Instr::GroupFirstNeeded {
			..
		}
		| Instr::GroupFirstSet {
			..
		}
		| Instr::CallIndexFn {
			..
		}
		| Instr::Matches {
			..
		}
		| Instr::ExternalEnter {
			..
		}
		| Instr::ExternalCall {
			..
		}
		| Instr::Degree {
			..
		}
		| Instr::RepeatRecurse {
			..
		}
		| Instr::JumpIfNotObject {
			..
		}
		| Instr::JumpIfOpaque {
			..
		}
		| Instr::InvalidControlFlow
		| Instr::JumpIfNotRecordId {
			..
		}
		| Instr::DocumentRoot {
			..
		}
		| Instr::JudgeCopy {
			..
		} => return false,
	}
	true
}

/// [`touched`] for an IR form. Every form is named, so a new form is decided here when it is
/// added: it either reads or writes registers that this scan must report, or it touches none.
fn ir_touched(ir: &IrInstr, out: &mut Vec<Touch>) -> bool {
	match ir.form() {
		IrForm::MemoGet(get) => out.push(Touch::Write(get.dst)),
		IrForm::MemoPut(put) => out.push(Touch::Read(put.src)),
		IrForm::Kernel(_)
		| IrForm::BatchRead(_)
		| IrForm::BatchReset {
			..
		}
		| IrForm::SkipRow
		| IrForm::MemoReset(_)
		| IrForm::MemoTruthy(_) => return false,
		// A marker restores or ends the part of the body that wrote the registers it lists: it
		// reads and writes none of its own.
		IrForm::WriteResume(_) | IrForm::WriteCut(_) | IrForm::WriteSkip | IrForm::WriteOffPath => {
		}
	}
	true
}

/// Whether `pipeline`'s body opens with the `WriteResume` of a write cut.
pub(in crate::exec::vm) fn holds_cut(pipeline: &Pipeline) -> bool {
	matches!(
		pipeline.code.first(),
		Some(Instr::Ir(ir)) if matches!(ir.form(), IrForm::WriteResume(_))
	)
}

/// Refuses a read-write program that holds a morsel decision on a pipeline of its own unless
/// `cut_of` answers `true` for that pipeline's index, and any program that holds a write cut in
/// a pipeline of its own that is not planned as an ordered apply, whose body would then run its
/// halves in one task with the markers of two. The refusal is an internal error, raised in
/// release builds too.
///
/// A pipeline of a group is exempt: it is placed with its group. With `cut_of` answering `false`
/// for every index, the rule is that a writing program holds no pipeline of its own that runs as
/// morsels. The error carries that rule's text, "a writing program planned morsels".
#[allow(dead_code)] // Called by the statement entry that lowers a `phys.write`.
pub(in crate::exec::vm) fn guard_writing_morsels(
	program: &Program,
	cut_of: &dyn Fn(usize) -> bool,
) -> Result<(), IrError> {
	if program.access_mode == AccessMode::ReadWrite
		&& program.pipelines.iter().enumerate().any(|(index, pipeline)| {
			pipeline.group.is_none()
				&& matches!(pipeline.parallel, Parallel::Morsels(_))
				&& !cut_of(index)
		}) {
		return Err(IrError::Invariant {
			what: "a writing program planned morsels",
		});
	}
	if program.pipelines.iter().any(|pipeline| {
		pipeline.group.is_none()
			&& holds_cut(pipeline)
			&& !matches!(
				pipeline.parallel,
				Parallel::Morsels(MorselPlan {
					combine: Combine::OrderedApply,
					..
				})
			)
	}) {
		return Err(IrError::Invariant {
			what: "a write cut in a pipeline not planned as an ordered apply",
		});
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::planner::placement::{Combine, MergeOrder, MorselInput, MorselPlan};
	use crate::exec::vm::ir_lower::emit::Emitter;
	use crate::exec::vm::program::{ArithOp, Pipeline, Source};

	fn load(dst: Reg) -> Instr {
		Instr::Load {
			dst,
			src: Operand::Const(0),
		}
	}

	fn add(dst: Reg, left: Reg, right: Reg) -> Instr {
		Instr::Arith {
			op: ArithOp::Add,
			left: Operand::Reg(left),
			right: Operand::Reg(right),
			dst,
		}
	}

	fn form(instr: &Instr) -> &IrForm {
		let Instr::Ir(ir) = instr else {
			panic!("expected an IR form, got {instr:?}");
		};
		ir.form()
	}

	#[test]
	fn write_emission_shape() {
		// The prefix computes r1 and r2, the cut path reads r2, and an off-path arm reads r1.
		let mut body = vec![load(1), add(2, 1, 1)];
		let cut_at = body.len();
		body.push(add(3, 2, 2));
		let emit = WriteCutEmit::scanned(&body, cut_at, 4, 10, 11);
		assert_eq!(emit.live.as_slice(), &[2]);

		let mut code = Vec::new();
		emit_resume(&mut code, &emit);
		code.extend(body[..cut_at].iter().cloned());
		emit_off_path(&mut code);
		emit_cut(&mut code, &emit);
		code.extend(body[cut_at..].iter().cloned());
		emit_skip(&mut code);
		code.push(Instr::EndRow);

		// `WriteResume` is first and holds the cut's labels and live registers.
		let IrForm::WriteResume(resume) = form(&code[0]) else {
			panic!("the body opens with a WriteResume");
		};
		assert_eq!((resume.cut, resume.skip), (10, 11));
		assert_eq!(resume.live.as_slice(), &[2]);
		// `WriteCut` appears exactly where `emit_cut` was called.
		let cuts: Vec<usize> = code
			.iter()
			.enumerate()
			.filter(
				|(_, instr)| matches!(instr, Instr::Ir(ir) if matches!(ir.form(), IrForm::WriteCut(_))),
			)
			.map(|(at, _)| at)
			.collect();
		assert_eq!(cuts, vec![1 + cut_at + 1]);
		let IrForm::WriteCut(cut) = form(&code[cuts[0]]) else {
			panic!("a WriteCut");
		};
		assert_eq!(cut.live.as_slice(), &[2]);
		// `WriteOffPath` sits where the arm opens, and `WriteSkip` just before the skipping end.
		assert!(matches!(form(&code[1 + cut_at]), IrForm::WriteOffPath));
		let end = code.len() - 1;
		assert!(matches!(code[end], Instr::EndRow));
		assert!(matches!(form(&code[end - 1]), IrForm::WriteSkip));
	}

	#[test]
	fn live_is_written_ahead_and_read_after() {
		let code = [
			load(1),
			load(2),
			add(3, 1, 2),
			add(4, 3, 2),
			Instr::Emit {
				src: 4,
			},
		];
		// Cut after the third instruction: r3 and r2 are read after it and written before it. r1
		// is written before it and never read after, r4 is written only after it.
		assert_eq!(live_across(&code, 3, 5).as_slice(), &[2, 3]);
		// The row register is never live.
		let code = [
			Instr::Load {
				dst: 1,
				src: Operand::Reg(ROW),
			},
			Instr::Emit {
				src: 1,
			},
		];
		assert_eq!(live_across(&code, 1, 2).as_slice(), &[1]);
	}

	#[test]
	fn an_instruction_without_an_entry_touches_every_register() {
		let code = [
			load(1),
			Instr::InvalidControlFlow,
			Instr::Emit {
				src: 1,
			},
		];
		// The instruction ahead of the cut writes every register, and the emit reads r1.
		assert_eq!(live_across(&code, 2, 3).as_slice(), &[1]);
		let code = [load(1), load(2), Instr::InvalidControlFlow];
		// The instruction after the cut reads every register; both are written ahead of it.
		assert_eq!(live_across(&code, 2, 3).as_slice(), &[1, 2]);
	}

	fn program(access_mode: AccessMode, pipelines: Vec<Pipeline>) -> Program {
		Emitter::new().assemble_program(pipelines, access_mode).unwrap()
	}

	fn pipeline(parallel: Parallel, group: Option<u16>) -> Pipeline {
		let mut pipeline = Emitter::new().finish_pipeline(Source::Once).unwrap();
		pipeline.parallel = parallel;
		pipeline.group = group;
		pipeline
	}

	fn morsels() -> Parallel {
		Parallel::Morsels(MorselPlan {
			input: MorselInput::Batches,
			combine: Combine::Concat,
			paged: None,
			prunes: false,
			merge: MergeOrder::Serial,
			ordered_fold: Vec::new(),
		})
	}

	fn resume() -> Instr {
		Instr::Ir(IrInstr::write_resume(WriteResumeSpec {
			cut: 0,
			skip: 0,
			live: SmallVec::new(),
		}))
	}

	fn ordered_apply() -> Parallel {
		Parallel::Morsels(MorselPlan {
			input: MorselInput::Batches,
			combine: Combine::OrderedApply,
			paged: None,
			prunes: false,
			merge: MergeOrder::Serial,
			ordered_fold: Vec::new(),
		})
	}

	#[test]
	fn guard_refuses_a_cut_in_a_pipeline_not_planned_as_an_ordered_apply() {
		let never = |_: usize| false;
		let cut = |parallel: Parallel, group: Option<u16>| {
			let mut held = pipeline(parallel, group);
			held.code.insert(0, resume());
			held
		};
		for parallel in [Parallel::Serial, morsels()] {
			let writing = program(AccessMode::ReadWrite, vec![cut(parallel, None)]);
			let err = guard_writing_morsels(&writing, &|_| true).unwrap_err();
			assert!(
				err.to_string()
					.contains("a write cut in a pipeline not planned as an ordered apply"),
				"{err}"
			);
			assert!(matches!(err, IrError::Invariant { .. }), "{err}");
		}
		// An ordered apply that holds its cut is admitted, and one that holds none is refused as
		// any other pipeline planned as morsels.
		let held = program(AccessMode::ReadWrite, vec![cut(ordered_apply(), None)]);
		assert!(guard_writing_morsels(&held, &|_| true).is_ok());
		let bare = program(AccessMode::ReadWrite, vec![pipeline(ordered_apply(), None)]);
		assert!(guard_writing_morsels(&bare, &never).is_err());
		// The cut of a pipeline of a group is placed with its group.
		let grouped = program(AccessMode::ReadWrite, vec![cut(Parallel::Serial, Some(0))]);
		assert!(guard_writing_morsels(&grouped, &never).is_ok());
	}

	#[test]
	fn guard_matches_main_without_cut() {
		let never = |_: usize| false;
		let writing = program(
			AccessMode::ReadWrite,
			vec![pipeline(Parallel::Serial, None), pipeline(morsels(), None)],
		);
		let err = guard_writing_morsels(&writing, &never).unwrap_err();
		assert!(err.to_string().contains("a writing program planned morsels"), "{err}");
		// The refusal is an internal error: a placement bug, never a statement to decline.
		assert!(matches!(err, IrError::Invariant { .. }), "{err}");

		// A cut on the pipeline that holds the morsels admits it, and only on that pipeline.
		assert!(guard_writing_morsels(&writing, &|index| index == 1).is_ok());
		assert!(guard_writing_morsels(&writing, &|index| index == 0).is_err());

		// A read-only program, a serial one and a pipeline of a group are not refused.
		let read_only = program(AccessMode::ReadOnly, vec![pipeline(morsels(), None)]);
		assert!(guard_writing_morsels(&read_only, &never).is_ok());
		let serial = program(AccessMode::ReadWrite, vec![pipeline(Parallel::Serial, None)]);
		assert!(guard_writing_morsels(&serial, &never).is_ok());
		let grouped = program(AccessMode::ReadWrite, vec![pipeline(morsels(), Some(0))]);
		assert!(guard_writing_morsels(&grouped, &never).is_ok());
	}
}
