//! The control flow of one statement: blocks, `LET`, `IF`, `FOR`, `RETURN`, `BREAK`, `CONTINUE`,
//! `SLEEP`, `THROW`, the statements that run as groups of their own and the errors raised where a
//! statement stands, lowered into the instructions of one program.
//!
//! A statement program is one pipeline over [`Source::Once`]. Its statements are the ops of the
//! top region in the order they were written, and its value is the value the region yields. The
//! control flow is the program's own instructions: a `FOR` is a jump back to its head, `BREAK`
//! and `CONTINUE` are jumps to its exit and to its next element, and a `RETURN` is
//! [`BlockInstr::Return`], which leaves the program with the value.
//!
//! # Statements and values
//!
//! The ops of a statement list are walked in order. A statement op (`phys.eval`, `phys.let`,
//! `phys.block`, `phys.if`, a `FOR` `phys.loop`, `phys.sleep`, `phys.return`, `phys.break`,
//! `phys.next`, `expr.throw`, `expr.raise`) lowers where it stands, and so does an `INFO`, which
//! is a group of one pipeline. An expression op, and a `phys.loop` that stands in value position,
//! lowers where its first reader in the list asks for it, as everywhere in [`Lowerer`]; one that
//! nothing in the list reads (no reader, or only readers in regions nested in it) lowers where it
//! stands, since it may raise or write. A loop of value position that the physical phase wrapped
//! in a `phys.pipeline` lowers where its wrapper stands. Any other op is the part of a pipeline,
//! a statement's region, a write or a unit, which its owner lowers, so one met as a statement of
//! a list is an invariant break.
//!
//! # Groups
//!
//! A read, a write or a schema statement of the list is a `phys.statement`, whose region holds the
//! statement's own ops. It is lowered as a group of the program: its pipelines, paging clauses,
//! distinct sets and sorts are appended to the program's pools at the lengths the groups before it
//! left, its permission frames are a range of the pool of its own, and its tail (`ONLY`,
//! `VERSION`, `TIMEOUT`) is the group's. A plain `EXPLAIN` is a group of one pipeline that loads
//! the rendering of the plan, a constant, and runs nothing of the statement it explains; an
//! `EXPLAIN ANALYZE` is the statement it explains as a group, run by [`BlockInstr::RunAnalyzed`],
//! which renders the analysis the program holds over what the run measured, and a signal the
//! statement raises ends it instead of leaving. A schema statement is a group of one pipeline over
//! [`Source::Once`] ([`super::ddl`]) that answers `NONE`; it makes the program one that writes, and
//! the draws it takes of the ids reserved for it are the program's reservations. An `INFO` is a
//! group of one pipeline over [`Source::Once`] ([`super::info`]) that answers its object. The
//! statement is the instruction [`BlockInstr::RunEmbedded`] over the program's running state, in
//! the frame the list is in, so the names the list binds in slots are read by the group's pipelines
//! from the same slots. A statement whose region raises (the refusal of a statement the planner
//! refuses to run) is lowered as that raise, which ends the list when the statement is reached; the
//! authority checks (`phys.ddl_guard`) that stand before the raise run first.
//!
//! A statement the binder marked late-bound (`served`) is, in a program that serves such statements
//! ([`LowerCx::serves_late`]), run by [`BlockInstr::RunServed`]: the compile driver answers the
//! statement as written ([`UnitRequest::ServedStatement`]), and the run plans it for the catalog
//! its transaction reads when the list reaches it, reading the names the list binds around it
//! through the frame. Its region stands for nothing. A program that serves none refuses it.
//!
//! A statement after one that changes the catalog resolves the session's namespace and database
//! again before it runs, and a `BREAK` or `CONTINUE` a statement signals goes to the loop that
//! takes it, through a pad that restores the frames the list saved since.
//!
//! # Raises
//!
//! `expr.throw` and an `expr.raise` of a loop control end the list with the instruction that
//! raises ([`Lowerer::raise_terminator`]). An `expr.raise` of a refusal ends it with
//! [`StoreInstr::Refuse`], which raises the refusal when the statement is reached. A misplaced
//! transaction control is a step of the request driver, so one reaching a program answers
//! [`IrError::Invariant`].
//!
//! # Scopes
//!
//! A scope runs in a frame of its own ([`BlockInstr::Save`], [`BlockInstr::Restore`]), so the
//! names it binds in the frame end with it. The scopes are:
//!
//! - a `{ ... }` block (`RegionKind::Block` and `ValueBlock`);
//! - an arm of an `IF` that holds a statement other than an evaluation or a further `IF`. An arm
//!   that holds nothing else binds no name and holds no `RETURN` of its own, so it runs in the
//!   frame of the `IF`; the arms of a further `IF` in it are scopes in their own right;
//! - each iteration of any `FOR`, which saves the frame before its body and restores it after.
//!
//! None of them is a context a `RETURN` ends (`RegionProperties::return_rule`), and no region a
//! statement list holds is one: a `RETURN` leaves the program with its value
//! ([`BlockInstr::Return`]), for the run that holds the program to take it, which is the
//! statement, the function or closure call, or the evaluation of a context the program is a unit
//! of. A `BREAK` or `CONTINUE` restores the frames saved since its loop's iteration began before
//! it jumps, so the loop finds the frame it saved.
//!
//! # Names
//!
//! The value a `LET` binds and the element of a `FOR` are read by a [`Instr::LoadSlot`] of the
//! slot [`BlockInstr::Bind`] set. A `LET` also binds its name in the frame when what runs in its
//! scope may read parameters by name: any instruction that is not the program's own work, any
//! call or method site, and any pipeline of its own that reads the value, which reads it by name.
//! A scope that holds a statement's group keeps its name in the frame too, because the group's
//! pipelines read it through code the list does not hold.
//! A `FOR` element is bound the same way under the parameter the loop states
//! ([`StepAttrs::Param`]); a loop that states none binds its element in its slot alone, so a
//! pipeline of its own that reads it answers `NotYet`.

use surrealdb_strand::Strand;

use super::super::program::{
	AggregateSink, Analysis, BlockInstr, Collect, GroupSpec, Instr, JoinSpec, Label, LimitSpec,
	Operand, Pipeline, Reg, ServedSpec, SortSpec, Source, SplitSpec, StoreInstr,
};
use super::ddl::{Reservations, lower_guard, schema_op};
use super::dispatch::{Coverage, check_write_region, coverage_of, timed_write_in, write_of};
use super::emit::Emitter;
use super::explain_hook::{Explained, Purpose, explain_of};
use super::expr::{Claim, LoopPads, Lowerer, Needs, ReturnScope, invariant};
use super::info::InfoLowered;
use super::pipe::{PoolBase, Pools, ReadScope, Reads, is_for_pipeline, path_steps};
use super::place::{Provenance, parallel_of};
use super::units::{LowerCx, UnitRequest};
use super::write_stmt::WritePipelines;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, RaiseKind};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::{LetAttrs, LetBinding, LoopKind, StepAttrs};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::vm::dispatch::Reach;
use crate::exec::{AccessMode, ContextLevel};
use crate::val::Value;

/// What a statement held in an expression of a list does with a signal: a statement list holds
/// no context, so a `RETURN` propagates out of the program to the run that holds it, and a loop
/// signal passes to the loop around the list.
const IN_A_LIST: ReturnScope = ReturnScope {
	context: None,
	frame: None,
	loops_fail: false,
};

/// Whether a loop of kind `kind` evaluates an expression in value position, or is a `FOR`, and so
/// is no group of its own.
fn is_value_loop(kind: LoopKind) -> bool {
	match kind {
		LoopKind::For
		| LoopKind::ElementFilter
		| LoopKind::Broadcast
		| LoopKind::Closure
		| LoopKind::Recurse
		| LoopKind::GraphStep => true,
		LoopKind::Select | LoopKind::Write => false,
	}
}

/// A statement program lowered from a container whose top region holds control flow.
pub(super) struct BlockProgram {
	/// The program's one pipeline.
	pub(super) pipeline: Pipeline,
	/// What the lowered values need of the program.
	pub(super) needs: Needs,
	/// The instructions each expression op emitted for itself, for the claim check.
	pub(super) claims: Vec<Claim>,
	/// `ReadWrite` where the program sleeps, `ReadOnly` otherwise.
	pub(super) access_mode: AccessMode,
	/// Whether the program is a `RETURN` standing as the statement, which reports its value as
	/// rows.
	pub(super) rows: bool,
	/// The statements of the list that run as groups of their own.
	pub(super) groups: Groups,
	/// The ids the program's schema statements take of the draws reserved for them, and the tables
	/// they define for the program's writes to share.
	pub(super) reservations: Reservations,
}

/// The statements of a list that run as groups of the program, with the pools their pipelines
/// number from.
///
/// The program's pipelines are these, in the order the groups were lowered, then the list's own.
#[derive(Default)]
pub(super) struct Groups {
	/// The pipelines of every group, in the order they run.
	pub(super) pipelines: Vec<Pipeline>,
	/// For each of `pipelines`, the instructions each op of its body emitted for itself.
	pub(super) claims: Vec<Vec<Claim>>,
	/// The spec of each group, indexed by the statement's group.
	pub(super) specs: Vec<GroupSpec>,
	/// The paging clauses the groups' pipelines name.
	pub(super) limits: Vec<LimitSpec>,
	/// For each distinct set of the groups, the group whose start empties it.
	pub(super) distincts: Vec<Option<u16>>,
	/// The sorts the groups' pipelines feed and read.
	pub(super) sorts: Vec<SortSpec>,
	/// The aggregates the groups' pipelines feed and read.
	pub(super) aggregates: Vec<AggregateSink>,
	/// The splits the groups' pipelines feed and read.
	pub(super) splits: Vec<SplitSpec>,
	/// The joins the groups' pipelines feed and read.
	pub(super) joins: Vec<JoinSpec>,
	/// For each probe the groups' pipelines read through, the group whose start empties it.
	pub(super) probes: Vec<Option<u16>>,
	/// Where each of the groups' pipelines came from, at its index in the program.
	pub(super) provenance: Provenance,
	/// What `EXPLAIN ANALYZE` of each analysed statement renders, in the order they were lowered.
	pub(super) analyses: Vec<Analysis>,
	/// The late-bound statements the program serves when it reaches them, in the order they were
	/// lowered.
	pub(super) served: Vec<ServedSpec>,
}

impl Groups {
	/// How many statements the list has lowered that read the names it binds through the frame:
	/// those that run as groups of the program and those it serves.
	fn statements(&self) -> usize {
		self.specs.len() + self.served.len()
	}

	/// The index the next group takes, and where the pools of its pipelines start, for a statement
	/// an expression runs as a group of the program ([`super::nested`]).
	pub(super) fn next_group(&self) -> Result<(u16, PoolBase), IrError> {
		let group = u16::try_from(self.specs.len())
			.map_err(|_| IrError::Capacity("vm statement groups"))?;
		Ok((group, self.base()?))
	}

	/// Where the pools of the next group start: after everything the groups so far hold.
	fn base(&self) -> Result<PoolBase, IrError> {
		let index = |len: usize, pool: &'static str| {
			u16::try_from(len).map_err(|_| IrError::Capacity(pool))
		};
		Ok(PoolBase {
			pipelines: self.pipelines.len(),
			limits: index(self.limits.len(), "vm limits")?,
			distincts: index(self.distincts.len(), "vm distinct sets")?,
			sorts: index(self.sorts.len(), "vm sorts")?,
			aggregates: index(self.aggregates.len(), "vm aggregates")?,
			splits: index(self.splits.len(), "vm splits")?,
			joins: index(self.joins.len(), "vm joins")?,
			probes: index(self.probes.len(), "vm probes")?,
			groups: index(self.specs.len() + 1, "vm groups")?,
		})
	}

	/// Appends the one pipeline of a group that holds no pool entries (a schema statement, an
	/// `INFO`, a plain `EXPLAIN`), with the instructions each op of its body emitted for itself.
	fn append_pipeline(&mut self, pipeline: Pipeline, claims: Vec<Claim>, spec: GroupSpec) {
		self.pipelines.push(pipeline);
		self.claims.push(claims);
		self.specs.push(spec);
	}

	/// Appends what a group lowered to the pools.
	pub(super) fn append(&mut self, reads: Reads, spec: GroupSpec) {
		let Pools {
			pipelines,
			claims,
			limits,
			distincts,
			sorts,
			aggregates,
			splits,
			joins,
			probes,
			groups,
			provenance,
		} = reads.into_pools();
		self.pipelines.extend(pipelines);
		self.claims.extend(claims);
		self.limits.extend(limits);
		self.distincts.extend(distincts);
		self.sorts.extend(sorts);
		self.aggregates.extend(aggregates);
		self.splits.extend(splits);
		self.joins.extend(joins);
		self.probes.extend(probes);
		self.provenance.pipelines.extend(provenance.pipelines);
		// The groups nested in the statement's sources took the indices after its own.
		self.specs.push(spec);
		self.specs.extend(groups);
	}
}

/// Whether `region` ends where it stands rather than running as a group: it holds the raise of an
/// error, or ends in a `BREAK`, `CONTINUE` or `RETURN` its statement diverges to before it reads
/// a row.
fn region_raises(ir: &Ir, region: RegionId) -> Result<bool, IrError> {
	let mut last = None;
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::block: op"))?;
		match &data.kind {
			OpKind::Expr(ExprOp::Raise(_) | ExprOp::Throw) => return Ok(true),
			OpKind::Phys(
				PhysOp::Timeout
				| PhysOp::Statement {
					served: None,
					..
				},
			) => {
				if let Some(inner) = data.regions.first()
					&& region_raises(ir, *inner)?
				{
					return Ok(true);
				}
			}
			OpKind::Expr(_) | OpKind::Phys(_) | OpKind::Logical(_) => {}
		}
		last = Some(data);
	}
	Ok(last.is_some_and(|data| {
		matches!(&data.kind, OpKind::Phys(PhysOp::Break | PhysOp::Next | PhysOp::Return))
	}))
}

/// Whether the top region of `ir` holds control flow, so that its program is a statement program
/// of this module: a block, an `IF`, a `FOR` loop, `SLEEP`, a `RETURN`, `BREAK` or `CONTINUE`, a
/// `THROW`, an error raised where the statement stands, a statement that runs as a group, or a
/// `LET` of a local binding.
pub(super) fn holds_control(ir: &Ir) -> Result<bool, IrError> {
	for op in ops_in(ir, ir.top()) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::block: op"))?;
		let control = match &data.kind {
			OpKind::Phys(PhysOp::Pipeline(_)) => is_for_pipeline(ir, op),
			OpKind::Phys(PhysOp::Timeout) => match data.regions.first() {
				Some(region) => region_raises(ir, *region)?,
				None => false,
			},
			OpKind::Phys(PhysOp::Block | PhysOp::Sleep | PhysOp::Return)
			| OpKind::Phys(PhysOp::Statement {
				..
			})
			| OpKind::Phys(PhysOp::If {
				..
			})
			| OpKind::Phys(PhysOp::Break | PhysOp::Next)
			| OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::For,
				..
			})
			| OpKind::Expr(ExprOp::Throw | ExprOp::Raise(_)) => true,
			OpKind::Phys(PhysOp::Let(attrs)) => attrs.binding == LetBinding::Local,
			OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => false,
		};
		if control {
			return Ok(true);
		}
	}
	Ok(false)
}

/// Lowers the top region of `ir` as one statement program into `em`, under the lowering context
/// `cx`, which holds the limits of the request's configuration and the units the program asks for.
///
/// The result register starts as `NONE`, takes the value the top region yields, and is emitted as
/// the program's one row. A region the top region ends in a terminator that leaves the program
/// yields nothing, and the row is the `NONE` the register started with.
///
/// `row`, where given, is the argument of the top block that names the row the program is run
/// over, which is not read as an object.
pub(super) fn lower_program(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	row: Option<ValueId>,
) -> Result<BlockProgram, IrError> {
	let top = ir.top();
	let rows = {
		let mut returns = false;
		for op in ops_in(ir, top) {
			let data = ir.op(op).ok_or(IrError::Stale("ir_lower::block: op"))?;
			returns |= matches!(&data.kind, OpKind::Phys(PhysOp::Return));
		}
		returns
	};
	let mut blocks = Blocks::new(em, ir, cx);
	if let Some(row) = row {
		blocks.lw.bind_row(row, false);
	}
	let result = blocks.reg()?;
	let none = blocks.none()?;
	blocks.load(result, none);
	if let Exit::Yields(Some(value)) = blocks.statements(top, true)? {
		blocks.load(result, value);
	}
	blocks.emit(Instr::Emit {
		src: result,
	});
	blocks.emit(Instr::EndRow);
	let (needs, claims, writes, groups, reservations) = blocks.finish();
	Ok(BlockProgram {
		pipeline: em.finish_pipeline(Source::Once)?,
		needs,
		claims,
		access_mode: match writes {
			true => AccessMode::ReadWrite,
			false => AccessMode::ReadOnly,
		},
		rows,
		groups,
		reservations,
	})
}

/// Lowers the top region of `ir`, the statement that the `EXPLAIN ANALYZE` `explain` it holds
/// explains, as one statement program into `em`: the explained statement is the program's one
/// group, which the program runs as a block runs an analysed statement, and the program's row is
/// the rendering of what that run measured.
pub(super) fn lower_analysed_program(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	explain: OpId,
) -> Result<BlockProgram, IrError> {
	let mut blocks = Blocks::new(em, ir, cx);
	let result = blocks.reg()?;
	let statement = u16::try_from(blocks.lw.block_groups().specs.len())
		.map_err(|_| IrError::Capacity("vm statement groups"))?;
	let base = blocks.lw.block_groups().base()?;
	if let Some((write, held, duration)) = written_in(ir, ir.top())? {
		let analysis = blocks.lw.explain_analysis(explain, Purpose::Measured)?;
		blocks.write_group(write, held, duration, base, statement)?;
		blocks.run_analyzed(statement, analysis, result, Collect::Block, None)?;
		let finished = blocks.finish_analysed(result);
		return analysed_program(em, finished);
	}
	let (explained, permissions) =
		blocks.lw.lower_explain_group(ir.top(), explain, base, statement, Purpose::Measured)?;
	let analysis = match explained {
		Explained::Plain(_) => {
			return Err(invariant(
				"a plain EXPLAIN is rendered by a program of its own, never run",
			));
		}
		Explained::Analysed {
			scope,
			analysis,
		} => {
			let ReadScope {
				reads,
				tail,
			} = scope;
			let scalar = tail.as_ref().is_some_and(|tail| tail.only);
			let spec = reads.group_spec(permissions, tail, scalar, false)?;
			blocks.lw.block_groups().append(reads, spec);
			analysis
		}
		Explained::AnalysedValue {
			pipeline,
			claims,
			analysis,
			..
		} => {
			blocks.lw.block_groups().append_pipeline(
				pipeline,
				claims,
				value_group_spec(base, permissions),
			);
			analysis
		}
	};
	blocks.run_analyzed(statement, analysis, result, Collect::Block, None)?;
	let finished = blocks.finish_analysed(result);
	analysed_program(em, finished)
}

/// The program of an analysed statement from what its lowering into `em` left: its own pipeline
/// emits the one row of the rendering.
fn analysed_program(
	em: &mut Emitter,
	(needs, claims, writes, groups, reservations): (Needs, Vec<Claim>, bool, Groups, Reservations),
) -> Result<BlockProgram, IrError> {
	Ok(BlockProgram {
		pipeline: em.finish_pipeline(Source::Once)?,
		needs,
		claims,
		access_mode: match writes {
			true => AccessMode::ReadWrite,
			false => AccessMode::ReadOnly,
		},
		rows: false,
		groups,
		reservations,
	})
}

/// How a statement list ends.
#[derive(Debug, Clone, Copy)]
enum Exit {
	/// The list yields its last statement's value, or nothing.
	Yields(Option<Operand>),
	/// The list ends in a terminator that leaves it: a `RETURN`, `BREAK`, `CONTINUE` or `THROW`.
	Diverges,
}

/// A loop being lowered: where its `BREAK` and `CONTINUE` jump to.
struct LoopTargets {
	/// How many frames are saved when its body starts.
	depth: usize,
	/// The label bound at the restore that ends the loop's last iteration.
	broken: Label,
	/// The label bound at the restore that ends an iteration and starts the next.
	continued: Label,
}

/// A `LET`'s binding whose scope is being lowered.
struct SlotScope {
	/// The instruction index of the [`BlockInstr::Bind`].
	bind: Label,
	/// The name the `LET` binds.
	name: String,
	/// The instruction index its scope starts at.
	code: usize,
	/// The call and method sites the program held when the scope started.
	sites: (usize, usize),
	/// How many reads of a name by a pipeline of its own had been lowered when the scope started.
	named: usize,
	/// How many groups the list had lowered when the scope started.
	grouped: usize,
}

/// What runs a statement of the list: its group, or the compile driver's plan of it.
#[derive(Debug, Clone, Copy)]
enum Runner {
	/// The group `n` of the program, run by [`BlockInstr::RunEmbedded`].
	Embedded(u16),
	/// The late-bound statement `n` of the program, run by [`BlockInstr::RunServed`].
	Served(u16),
}

/// The lowering of one statement program.
struct Blocks<'a> {
	lw: Lowerer<'a>,
	ir: &'a Ir,
	/// The next slot of the running state a name is bound in.
	slots: u16,
	/// The next iterator of the running state a `FOR` iterates with.
	iterators: u16,
	/// How many frames are saved at this point of the program.
	saved: usize,
	loops: Vec<LoopTargets>,
	scopes: Vec<SlotScope>,
	/// Whether anything lowered writes: a `SLEEP`.
	writes: bool,
	/// The draws the program's schema statements take.
	reservations: Reservations,
	/// Whether a statement lowered so far may have changed the catalog the statements after it run
	/// against.
	catalog_changed: bool,
}

impl<'a> Blocks<'a> {
	fn new(em: &'a mut Emitter, ir: &'a Ir, cx: &'a mut LowerCx<'_>) -> Self {
		let mut lw = Lowerer::in_context(em, ir, cx);
		// The statements that run as groups of the program are held by the lowerer, which adds the
		// groups of the statements its expressions run in place.
		lw.hold_block_groups();
		lw.catch_returns(IN_A_LIST);
		Self {
			lw,
			ir,
			slots: 0,
			iterators: 0,
			saved: 0,
			loops: Vec::new(),
			scopes: Vec::new(),
			writes: false,
			reservations: Reservations::default(),
			catalog_changed: false,
		}
	}

	/// What the lowered values need of the program, the instructions each op emitted for itself,
	/// whether the program writes, the groups of its statements and what its schema statements
	/// drew.
	fn finish(mut self) -> (Needs, Vec<Claim>, bool, Groups, Reservations) {
		let groups = self.lw.take_block_groups();
		(self.lw.needs(), self.lw.take_claims(), self.writes, groups, self.reservations)
	}

	/// Ends the program of an analysed statement, whose one row is the rendering left in `result`,
	/// and answers what [`Blocks::finish`] does.
	fn finish_analysed(mut self, result: Reg) -> (Needs, Vec<Claim>, bool, Groups, Reservations) {
		self.emit(Instr::Emit {
			src: result,
		});
		self.emit(Instr::EndRow);
		self.finish()
	}

	fn reg(&mut self) -> Result<Reg, IrError> {
		self.lw.emitter().reg()
	}

	fn label(&mut self) -> Result<Label, IrError> {
		self.lw.emitter().label()
	}

	fn bind(&mut self, label: Label) -> Result<(), IrError> {
		self.lw.emitter().bind(label)
	}

	fn emit(&mut self, instr: Instr) -> Label {
		self.lw.emitter().emit(instr)
	}

	fn block(&mut self, instr: BlockInstr) -> Label {
		self.emit(Instr::Block(instr))
	}

	fn jump(&mut self, target: Label) {
		self.emit(Instr::Jump {
			target,
		});
	}

	fn load(&mut self, dst: Reg, src: Operand) {
		self.emit(Instr::Load {
			dst,
			src,
		});
	}

	/// The constant `NONE`.
	fn none(&mut self) -> Result<Operand, IrError> {
		Ok(Operand::Const(self.lw.emitter().constant(Value::None)?))
	}

	/// The register that holds `operand`, loaded into a new one when it is not in a register.
	fn reg_of(&mut self, operand: Operand) -> Result<Reg, IrError> {
		match operand {
			Operand::Reg(reg) => Ok(reg),
			Operand::Const(_) | Operand::Field(_) => {
				let reg = self.reg()?;
				self.load(reg, operand);
				Ok(reg)
			}
		}
	}

	/// Gives the result of `data`, when it has one, the operand `operand`.
	fn define(&mut self, data: &OpData, operand: Operand) {
		if let Some(result) = data.results.first() {
			self.lw.bind_arg(*result, operand);
		}
	}

	/// The `N` operands of `data`.
	fn operands<const N: usize>(data: &OpData) -> Result<[ValueId; N], IrError> {
		<[ValueId; N]>::try_from(data.operands.as_slice())
			.map_err(|_| IrError::Stale("ir_lower::block: operand arity"))
	}

	/// The `N` regions of `data`.
	fn regions<const N: usize>(data: &OpData) -> Result<[RegionId; N], IrError> {
		<[RegionId; N]>::try_from(data.regions.as_slice())
			.map_err(|_| IrError::Stale("ir_lower::block: region arity"))
	}

	/// The kind of `region`.
	fn kind_of(&self, region: RegionId) -> Result<RegionKind, IrError> {
		self.ir
			.region(region)
			.map(|data| data.kind)
			.ok_or(IrError::Stale("ir_lower::block: region"))
	}

	/// The arguments of the entry block of `region`.
	fn args_of(&self, region: RegionId) -> Result<Vec<ValueId>, IrError> {
		let block = self
			.ir
			.region(region)
			.and_then(|data| data.blocks.first().copied())
			.and_then(|block| self.ir.block(block))
			.ok_or(IrError::Stale("ir_lower::block: region block"))?;
		Ok(block.args.to_vec())
	}

	/// Whether the arm `region` binds no name and holds no `RETURN` of its own: it holds
	/// expressions, evaluations, further `IF`s and its terminator alone.
	fn is_plain_arm(&self, region: RegionId) -> Result<bool, IrError> {
		for op in ops_in(self.ir, region) {
			let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::block: op"))?;
			if !matches!(
				&data.kind,
				OpKind::Expr(_) | OpKind::Phys(PhysOp::Yield | PhysOp::Eval | PhysOp::If { .. })
			) {
				return Ok(false);
			}
		}
		Ok(true)
	}

	/// Lowers the statements of `region` in order and answers how the list ends.
	///
	/// The values lowered inside the region, and the names it binds, end with it: the region's
	/// code runs on one path only. `top` is the top region of the program, which may end with no
	/// terminator.
	fn statements(&mut self, region: RegionId, top: bool) -> Result<Exit, IrError> {
		let held = self.lw.scope();
		let outer_region = self.lw.enter(region);
		let outer_scopes = self.scopes.len();
		let lowered = self.statement_list(region, top);
		self.lw.leave(outer_region);
		self.lw.restore_scope(held);
		let exit = lowered?;
		self.close_scopes(outer_scopes)?;
		Ok(exit)
	}

	fn statement_list(&mut self, region: RegionId, top: bool) -> Result<Exit, IrError> {
		let ir = self.ir;
		let ops: Vec<OpId> = ops_in(ir, region).collect();
		let mut ended = None;
		for (lowered, op) in ops.iter().enumerate() {
			if ended.is_some() {
				// A statement that raises when it is reached ends the list before its terminator,
				// which is then never reached; any other op ends the list by being the terminator.
				let raised = lowered
					.checked_sub(1)
					.and_then(|last| ops.get(last))
					.and_then(|last| ir.op(*last))
					.is_some_and(|last| {
						matches!(
							last.kind,
							OpKind::Phys(PhysOp::Statement { .. } | PhysOp::Timeout)
						)
					});
				if raised {
					break;
				}
				return Err(IrError::Stale("ir_lower::block: an op after a terminator"));
			}
			let data = ir.op(*op).ok_or(IrError::Stale("ir_lower::block: op"))?;
			// A statement after one that changed the catalog runs in the frame the change left,
			// whose names are resolved again: it raises for a missing namespace or database where
			// it reads one, not before the program starts.
			let required = self.catalog_changed.then(|| self.lw.required());
			ended = self.padded_statement(*op, data)?;
			if let Some(required) = required {
				self.lw.restore_required(required);
			}
		}
		let exit = match (ended, top) {
			(Some(exit), _) => exit,
			(None, true) => Exit::Yields(None),
			(None, false) => return Err(IrError::Stale("ir_lower::block: a region with no end")),
		};
		Ok(exit)
	}

	/// [`Self::statement`], with a `BREAK` or `CONTINUE` that a statement held in one of its
	/// expressions signals taken by the `FOR` around the list: the expression's instruction jumps
	/// to a pad emitted after the statement, which restores the frames saved since the loop's
	/// iteration began and jumps to the loop's exit or its next element. Outside a `FOR` the signal
	/// propagates.
	fn padded_statement(&mut self, op: OpId, data: &OpData) -> Result<Option<Exit>, IrError> {
		let Some(&LoopTargets {
			depth,
			broken,
			continued,
		}) = self.loops.last()
		else {
			return self.statement(op, data);
		};
		let pads = LoopPads {
			on_break: self.label()?,
			on_continue: self.label()?,
		};
		let saved = self.saved;
		let (ended, taken) = self.with_lw_loop_pads(pads, |this| this.statement(op, data))?;
		if taken {
			let over = self.label()?;
			self.jump(over);
			for (pad, target) in [(pads.on_break, broken), (pads.on_continue, continued)] {
				self.bind(pad)?;
				for _ in depth..saved {
					self.block(BlockInstr::Restore);
				}
				self.jump(target);
			}
			self.bind(over)?;
		}
		Ok(ended)
	}

	/// Runs `body` with the expression lowerer's loop pads set to `pads`, and answers whether a
	/// statement held in an expression took them.
	fn with_lw_loop_pads<T>(
		&mut self,
		pads: LoopPads,
		body: impl FnOnce(&mut Self) -> Result<T, IrError>,
	) -> Result<(T, bool), IrError> {
		let outer = self.lw.set_loop_pads(Some(pads));
		let lowered = body(self);
		let taken = self.lw.restore_loop_pads(outer);
		lowered.map(|lowered| (lowered, taken))
	}

	/// Lowers the op `op`; answers how the list ends when `op` is the terminator that ends it.
	fn statement(&mut self, op: OpId, data: &OpData) -> Result<Option<Exit>, IrError> {
		match &data.kind {
			OpKind::Phys(PhysOp::Yield) | OpKind::Expr(ExprOp::Yield) => {
				let yielded = match data.operands.as_slice() {
					[] => None,
					[value] => Some(self.lw.value(*value)?),
					_ => return Err(invariant("a list yields at most one value")),
				};
				Ok(Some(Exit::Yields(yielded)))
			}
			OpKind::Phys(PhysOp::Eval) => {
				let [value] = Self::operands::<1>(data)?;
				let value = self.lw.value(value)?;
				self.define(data, value);
				Ok(None)
			}
			OpKind::Phys(PhysOp::Let(attrs)) => {
				self.local_let(data, attrs)?;
				Ok(None)
			}
			OpKind::Phys(PhysOp::Block) => {
				let [region] = Self::regions::<1>(data)?;
				let dst = self.reg()?;
				self.scope(region, dst)?;
				self.define(data, Operand::Reg(dst));
				Ok(None)
			}
			OpKind::Phys(PhysOp::If {
				..
			}) => {
				self.branch(data)?;
				Ok(None)
			}
			OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::For,
				first,
				step,
				..
			}) => {
				let param = match step.as_deref() {
					None => None,
					Some(StepAttrs::Param(name)) => Some(name),
					Some(_) => return Err(invariant("a FOR states a step its kind does not take")),
				};
				self.for_loop(data, *first, param)?;
				Ok(None)
			}
			OpKind::Phys(PhysOp::Sleep) => {
				let [duration] = Self::operands::<1>(data)?;
				let duration = self.lw.value(duration)?;
				let duration = self.reg_of(duration)?;
				self.block(BlockInstr::Sleep {
					duration,
				});
				self.writes = true;
				let none = self.none()?;
				self.define(data, none);
				Ok(None)
			}
			OpKind::Phys(PhysOp::Return) => {
				let [value] = Self::operands::<1>(data)?;
				let value = self.lw.value(value)?;
				self.leave_scope(value)?;
				Ok(Some(Exit::Diverges))
			}
			OpKind::Phys(PhysOp::Break) => {
				self.leave_loop(true)?;
				Ok(Some(Exit::Diverges))
			}
			OpKind::Phys(PhysOp::Next) => {
				self.leave_loop(false)?;
				Ok(Some(Exit::Diverges))
			}
			OpKind::Expr(ExprOp::Throw) => {
				self.lw.raise_terminator(op)?;
				Ok(Some(Exit::Diverges))
			}
			OpKind::Expr(ExprOp::Raise(kind)) => {
				match &**kind {
					RaiseKind::Refused(refusal) => {
						self.emit(Instr::Store(StoreInstr::Refuse {
							refusal: refusal.clone(),
						}));
					}
					RaiseKind::Misplaced(_) => {
						return Err(IrError::Invariant {
							what: "a misplaced transaction control is run by the request driver, never lowered as a program",
						});
					}
					RaiseKind::InvalidControlFlow | RaiseKind::Folded(_) => {
						self.lw.raise_terminator(op)?;
					}
				}
				Ok(Some(Exit::Diverges))
			}
			OpKind::Expr(ExprOp::DepthCharge {
				..
			}) => Err(invariant(
				"expr.depth_charge has no producer: no rewrite removes a charged call",
			)),
			OpKind::Phys(PhysOp::Loop {
				kind:
					LoopKind::ElementFilter
					| LoopKind::Broadcast
					| LoopKind::Closure
					| LoopKind::Recurse
					| LoopKind::GraphStep,
				..
			})
			| OpKind::Expr(_) => {
				self.run_where_stands(data)?;
				Ok(None)
			}
			OpKind::Phys(PhysOp::Access(_) | PhysOp::Show(_)) => Err(IrError::Invariant {
				what: "ACCESS and SHOW CHANGES are statements of a request, never of a block",
			}),
			OpKind::Phys(PhysOp::Statement {
				changes_catalog,
				served,
				..
			}) => self.group_statement(op, data, *changes_catalog, *served),
			OpKind::Phys(PhysOp::DdlGuard {
				guard,
			}) => {
				lower_guard(self.lw.em, guard)?;
				Ok(None)
			}
			OpKind::Phys(PhysOp::Kill | PhysOp::Live(_)) => Err(IrError::Invariant {
				what: "a KILL or a LIVE is a statement of a request, never of a list",
			}),
			OpKind::Phys(PhysOp::Pipeline(_)) => {
				self.wrapped_loop(op, data)?;
				Ok(None)
			}
			OpKind::Phys(PhysOp::Info {
				..
			}) => {
				self.info_statement(op, data)?;
				Ok(None)
			}
			OpKind::Phys(PhysOp::Fetch) => {
				self.value_fetch(data)?;
				Ok(None)
			}
			// SECURITY: a statement that writes the request's session lowers only as a statement
			// of the request's own list, so code a list holds (a function body, an event) can
			// never switch the caller's namespace or database, set its options or bind one of its
			// parameters.
			OpKind::Phys(PhysOp::Option {
				..
			}) => Err(IrError::Invariant {
				what: "an OPTION is a statement of a request, never of a list",
			}),
			OpKind::Phys(PhysOp::Use {
				..
			}) => Err(IrError::Invariant {
				what: "a USE is a statement of a request, never of a list",
			}),
			OpKind::Phys(PhysOp::Timeout) => {
				let [duration] = Self::operands::<1>(data)?;
				let [deadline] = Self::regions::<1>(data)?;
				let _ = self.lw.value(duration)?;
				self.statements(deadline, false).map(Some)
			}
			OpKind::Phys(_) => match coverage_of(&data.kind).1 {
				Coverage::ViaRequest => Err(invariant(
					"a request step is driven from its attributes and never lowered as a program",
				)),
				Coverage::ViaWrite => Err(invariant(
					"a row, stage or clause of a write is lowered by its write, not as a statement",
				)),
				Coverage::ViaUnit => Err(invariant(
					"the body of a subscription or events unit is lowered by the unit it tops, not as a statement",
				)),
				// A source, a streaming or breaking op, a `SELECT` or write loop, a statement's own
				// op, an events or unit op: each stands in a pipeline or in a statement's region,
				// which the list lowers as a group, so none is a statement of a list.
				Coverage::Native | Coverage::ViaLoop | Coverage::NotYet => Err(invariant(
					"an op of a pipeline, a statement or a unit is lowered by its owner, never as a statement of a list",
				)),
			},
			OpKind::Logical(_) => Err(invariant("a logical operation in a physical container")),
		}
	}

	/// A loop the physical phase wrapped in a pipeline, run in place: the pipeline holds the loop
	/// and the yield of its value, and the pipeline's value is the loop's.
	///
	/// A `FOR` runs serially, as the statement it is; a placement that batches it is an invariant
	/// break, since a `FOR` never is. A loop in value position (an element filter, a broadcast, a
	/// closure builtin, a graph step) is lowered where the wrapper stands, which is where the
	/// binder emitted the loop: just before the expression that reads its value. A `SELECT` or a
	/// write loop belongs to the statement whose region holds it, so the list never meets one.
	fn wrapped_loop(&mut self, op: OpId, data: &OpData) -> Result<(), IrError> {
		let [region] = Self::regions::<1>(data)?;
		let inner: Vec<OpId> = ops_in(self.ir, region).collect();
		let [looped, ended] = inner[..] else {
			return Err(invariant(
				"a pipeline of a list holds one loop and the yield of its value",
			));
		};
		let loop_data = self.ir.op(looped).ok_or(IrError::Stale("ir_lower::block: op"))?;
		let ends =
			self.ir.op(ended).is_some_and(|held| matches!(held.kind, OpKind::Phys(PhysOp::Yield)));
		let OpKind::Phys(PhysOp::Loop {
			kind,
			..
		}) = &loop_data.kind
		else {
			return Err(invariant("a pipeline of a list holds a loop"));
		};
		if !ends {
			return Err(invariant("a pipeline of a list ends in the yield of its loop's value"));
		}
		match kind {
			LoopKind::For => {
				if self.ir.placement(op).is_some()
					&& parallel_of(self.ir, op, None)?
						!= crate::exec::planner::placement::Parallel::Serial
				{
					return Err(invariant("a FOR loop placed to run batched"));
				}
				self.statement(looped, loop_data)?;
				if let (Some(outer), Some(inner)) =
					(data.results.first(), loop_data.results.first())
				{
					let value = self.lw.value(*inner)?;
					self.lw.bind_arg(*outer, value);
				}
			}
			LoopKind::ElementFilter
			| LoopKind::Broadcast
			| LoopKind::Closure
			| LoopKind::Recurse
			| LoopKind::GraphStep => {
				if let (Some(outer), Some(inner)) =
					(data.results.first(), loop_data.results.first())
				{
					// The loop stands in the pipeline's region, so its value is read from inside
					// it.
					let held = self.lw.enter(region);
					let value = self.lw.value(*inner);
					self.lw.leave(held);
					self.lw.bind_arg(*outer, value?);
				}
			}
			LoopKind::Select | LoopKind::Write => {
				return Err(invariant(
					"a SELECT or write loop is lowered by the statement that holds it, never by a list",
				));
			}
		}
		Ok(())
	}

	/// Lowers the expression or value loop `op` where it stands, so that it runs once, in the order
	/// it was written, whatever reads it.
	///
	/// The ops of a list stand in the order they are evaluated, and a statement between an
	/// expression and the op that reads it may write what the expression reads, so the expression
	/// is evaluated before that statement runs, and its readers take the value it answered. One
	/// read from a region nested in the list is not skipped whenever that region is, and one read
	/// by nothing still evaluates, since the expression may raise or write. A literal emits
	/// nothing.
	fn run_where_stands(&mut self, data: &OpData) -> Result<(), IrError> {
		let Some(result) = data.results.first() else {
			return Ok(());
		};
		if matches!(&data.kind, OpKind::Expr(ExprOp::Literal(_))) {
			return Ok(());
		}
		self.lw.value(*result)?;
		Ok(())
	}

	/// The `FETCH` of a `RETURN` whose value is no read: the records its paths reach in the one
	/// value it reads, fetched in place where the list reaches it. The value is copied first, as
	/// the fetch takes the register it reads. The walk may read records of any table.
	fn value_fetch(&mut self, data: &OpData) -> Result<(), IrError> {
		let [value] = Self::operands::<1>(data)?;
		if data.regions.is_empty() {
			return Err(invariant("a fetch with no path"));
		}
		let value = self.lw.value(value)?;
		let src = self.reg()?;
		self.load(src, value);
		let mut paths = Vec::with_capacity(data.regions.len());
		for region in &data.regions {
			paths.push(path_steps(&mut self.lw, self.ir, *region)?);
		}
		let fetch = self.lw.emitter().fetch(paths, false)?;
		self.lw.reaches(Reach::any());
		let dst = self.reg()?;
		self.emit(Instr::Fetch {
			fetch,
			src,
			dst,
		});
		self.define(data, Operand::Reg(dst));
		Ok(())
	}

	/// `INFO`: a group of one pipeline over [`Source::Once`] that emits the statement's object as
	/// its one row, run where the list reaches it ([`BlockInstr::RunEmbedded`]).
	///
	/// The statement reads the catalog and writes nothing, so its group is no write and starts
	/// without the record-user database check a statement that reads rows takes. The context level
	/// its subject needs is the one the statement's run resolves, and the statements after one that
	/// changed the catalog resolve the session's names again first.
	fn info_statement(&mut self, op: OpId, data: &OpData) -> Result<(), IrError> {
		let statement = u16::try_from(self.lw.block_groups().specs.len())
			.map_err(|_| IrError::Capacity("vm statement groups"))?;
		let base = self.lw.block_groups().base()?;
		let (info, permissions) = self.lw.lower_info_group(op, statement)?;
		let InfoLowered {
			pipeline,
			needs,
			claims,
		} = info;
		let spec = GroupSpec {
			write: None,
			scalar: true,
			rowless: true,
			sorts: base.sorts..base.sorts,
			aggregates: base.aggregates..base.aggregates,
			permissions,
			version: None,
			timeout: None,
			only: false,
		};
		self.lw.block_groups().append_pipeline(pipeline, claims, spec);
		let dst = self.reg()?;
		let collect = self.collect_of(op, data)?;
		let resolve = self.catalog_changed.then_some(needs.context);
		self.run_group(Runner::Embedded(statement), dst, collect, resolve)?;
		self.define(data, Operand::Reg(dst));
		Ok(())
	}

	/// A statement that runs as a group of its own: lowered as the group `statement` of the
	/// program and run where the list reaches it. A schema statement is a group of one pipeline
	/// that answers `NONE`, and makes the program one that writes.
	///
	/// A statement whose region raises is lowered as the raise, which ends the list. A statement
	/// the binder marked late-bound is planned for the catalog its transaction reads when the list
	/// reaches it, in a program that serves such statements ([`LowerCx::serves_late`]); a program
	/// that serves none refuses it.
	fn group_statement(
		&mut self,
		op: OpId,
		data: &OpData,
		changes_catalog: bool,
		served: Option<u32>,
	) -> Result<Option<Exit>, IrError> {
		let [region] = Self::regions::<1>(data)?;
		if let Some(written) = served {
			if !self.lw.serves_late() {
				return Err(IrError::NotYet {
					what: "a late-bound statement of a program that serves none",
				});
			}
			// The statement is planned for the catalog its transaction reads when the list
			// reaches it, and resolves the session's names as that plan needs.
			return self.served_statement(op, data, changes_catalog, written, ContextLevel::Root);
		}
		if self.raises(region)? {
			return match self.statements(region, false)? {
				Exit::Diverges => Ok(Some(Exit::Diverges)),
				Exit::Yields(_) => {
					Err(IrError::Stale("ir_lower::block: a statement that raises yields"))
				}
			};
		}
		if self.evaluates_only(region)? {
			// An expression of the list that reads the clock: it is no group, so it runs where it
			// stands, after the instant its statement starts at is taken.
			self.block(BlockInstr::Clock);
			return match self.statements(region, false)? {
				Exit::Diverges => Ok(Some(Exit::Diverges)),
				Exit::Yields(value) => {
					if let Some(value) = value {
						self.define(data, value);
					}
					Ok(None)
				}
			};
		}
		let statement = u16::try_from(self.lw.block_groups().specs.len())
			.map_err(|_| IrError::Capacity("vm statement groups"))?;
		let base = self.lw.block_groups().base()?;
		let mut analysed = None;
		let written = written_in(self.ir, region)?;
		// `EXPLAIN ANALYZE` of a write runs the write as the group and renders what it measured.
		let explained = explain_of(self.ir, region);
		let context =
			if let (Some((explain, true)), Some((write, held, duration))) = (explained, written) {
				analysed = Some(self.lw.explain_analysis(explain, Purpose::Measured)?);
				self.write_group(write, held, duration, base, statement)?
			} else if let Some((explain, _)) = explained {
				let (explained, permissions) = self.lw.lower_explain_group(
					region,
					explain,
					base,
					statement,
					Purpose::Described,
				)?;
				match explained {
					Explained::Plain(pipeline) => {
						// The rendering is one row of constant work: a scalar statement that reads
						// nothing.
						let spec = GroupSpec {
							write: None,
							scalar: true,
							rowless: true,
							sorts: base.sorts..base.sorts,
							aggregates: base.aggregates..base.aggregates,
							permissions,
							version: None,
							timeout: None,
							only: false,
						};
						self.lw.block_groups().append_pipeline(pipeline, Vec::new(), spec);
						ContextLevel::Root
					}
					Explained::Analysed {
						scope,
						analysis,
					} => {
						let ReadScope {
							reads,
							tail,
						} = scope;
						// A group with rows checks the record user's access in the database it
						// starts in, so it reads the database whatever it reads itself.
						let context = reads.needs.context.max(ContextLevel::Database);
						let scalar = tail.as_ref().is_some_and(|tail| tail.only);
						let spec = reads.group_spec(permissions, tail, scalar, false)?;
						self.lw.block_groups().append(reads, spec);
						analysed = Some(analysis);
						context
					}
					Explained::AnalysedValue {
						pipeline,
						claims,
						needs,
						analysis,
					} => {
						self.lw.block_groups().append_pipeline(
							pipeline,
							claims,
							value_group_spec(base, permissions),
						);
						analysed = Some(analysis);
						needs.context
					}
				}
			} else if let Some((write, held, duration)) = written {
				self.write_group(write, held, duration, base, statement)?
			} else if schema_op(self.ir, region)?.is_some() {
				let (pipeline, permissions, needs) =
					self.lw.lower_schema_group(region, statement, &mut self.reservations)?;
				// A schema statement answers `NONE` as one row and writes, so its program does.
				let spec = GroupSpec {
					write: None,
					scalar: true,
					rowless: true,
					sorts: base.sorts..base.sorts,
					aggregates: base.aggregates..base.aggregates,
					permissions,
					version: None,
					timeout: None,
					only: false,
				};
				self.lw.block_groups().append_pipeline(pipeline, Vec::new(), spec);
				self.writes = true;
				needs.context
			} else {
				let (scope, permissions) = self.lw.lower_group(region, base, statement)?;
				let ReadScope {
					reads,
					tail,
				} = scope;
				// A group with rows checks the record user's access in the database it starts in,
				// so it reads the database whatever it reads itself.
				let context = reads.needs.context.max(ContextLevel::Database);
				let scalar = tail.as_ref().is_some_and(|tail| tail.only);
				let spec = reads.group_spec(permissions, tail, scalar, false)?;
				self.lw.block_groups().append(reads, spec);
				context
			};
		let dst = self.reg()?;
		let collect = self.collect_of(op, data)?;
		// The statements after one that changes the catalog run against what it leaves, so the
		// session's names are resolved again before they run.
		let resolve = self.catalog_changed.then_some(context);
		self.block(BlockInstr::Clock);
		match analysed {
			Some(analysis) => self.run_analyzed(statement, analysis, dst, collect, resolve)?,
			None => self.run_group(Runner::Embedded(statement), dst, collect, resolve)?,
		}
		if changes_catalog {
			self.block(BlockInstr::Resolve);
			self.catalog_changed = true;
		}
		self.define(data, Operand::Reg(dst));
		Ok(None)
	}

	/// Lowers the write `write`, which the region `held` holds with what it is made of and which
	/// the `TIMEOUT` of `duration` bounds where given, as the group `statement` of the program,
	/// numbering its pools from `base`. Answers the context level its run resolves.
	fn write_group(
		&mut self,
		write: OpId,
		held: RegionId,
		duration: Option<ValueId>,
		base: PoolBase,
		statement: u16,
	) -> Result<ContextLevel, IrError> {
		check_write_region(self.ir, held, write)?;
		let (lowered, timeout, permissions) =
			self.lw.lower_write_group(write, held, base, duration, statement)?;
		// A write's value is its one record when it is `ONLY`, and its records otherwise; its
		// clauses are the write's own, so the group's tail is only its `TIMEOUT`.
		let scalar = lowered.spec.only;
		match lowered.pipelines {
			WritePipelines::Own(pipeline) => {
				let spec = GroupSpec {
					write: Some(lowered.spec),
					scalar,
					rowless: false,
					sorts: base.sorts..base.sorts,
					aggregates: base.aggregates..base.aggregates,
					permissions,
					version: None,
					timeout,
					only: false,
				};
				self.lw.block_groups().append_pipeline(pipeline, Vec::new(), spec);
			}
			WritePipelines::Over(reads) => {
				let mut spec = reads.group_spec(permissions, None, scalar, false)?;
				spec.write = Some(lowered.spec);
				spec.timeout = timeout;
				self.lw.block_groups().append(reads, spec);
			}
		}
		self.writes = true;
		Ok(lowered.needs.context.max(ContextLevel::Database))
	}

	/// The index of the late-bound statement the program serves next, the statement as written
	/// that the container names by `written`, which holds the names the list binds around it and
	/// the depth it is planned at, and which the compile driver is asked to plan when the program
	/// reaches it ([`UnitRequest::ServedStatement`]).
	fn served_spec(&mut self, written: u32) -> Result<u16, IrError> {
		let at = u16::try_from(self.lw.block_groups().served.len())
			.map_err(|_| IrError::Capacity("vm late-bound statements"))?;
		let mut shadowed: Vec<String> = Vec::new();
		for scope in &self.scopes {
			if !shadowed.contains(&scope.name) {
				shadowed.push(scope.name.clone());
			}
		}
		let depth = self.lw.unit_depth();
		self.lw.need_unit(UnitRequest::ServedStatement {
			position: u32::from(at),
			written,
		})?;
		self.lw.block_groups().served.push(ServedSpec {
			shadowed,
			depth,
		});
		Ok(at)
	}

	/// A late-bound statement the program serves, the statement as written that the container
	/// names by `written`, whose own region stands for nothing: it is run with `context` resolved.
	fn served_statement(
		&mut self,
		op: OpId,
		data: &OpData,
		changes_catalog: bool,
		written: u32,
		context: ContextLevel,
	) -> Result<Option<Exit>, IrError> {
		let dst = self.reg()?;
		let collect = self.collect_of(op, data)?;
		let served = self.served_spec(written)?;
		self.block(BlockInstr::Clock);
		self.run_group(Runner::Served(served), dst, collect, Some(context))?;
		if changes_catalog {
			self.block(BlockInstr::Resolve);
			self.catalog_changed = true;
		}
		self.define(data, Operand::Reg(dst));
		Ok(None)
	}

	/// Whether `region`, the region of a statement, holds the evaluation of an expression and
	/// nothing that runs as a group: its ops are expression ops, the control flow and the loops
	/// an expression holds in value position, and its yield.
	fn evaluates_only(&self, region: RegionId) -> Result<bool, IrError> {
		for op in ops_in(self.ir, region) {
			let data = self.ir.op(op).ok_or(IrError::Stale("ir_lower::block: op"))?;
			let evaluates = match &data.kind {
				OpKind::Expr(_)
				| OpKind::Phys(PhysOp::Yield | PhysOp::Block)
				| OpKind::Phys(PhysOp::If {
					..
				}) => true,
				OpKind::Phys(PhysOp::Loop {
					kind,
					..
				}) => is_value_loop(*kind),
				OpKind::Phys(PhysOp::Pipeline(_)) => {
					let [inner] = Self::regions::<1>(data)?;
					ops_in(self.ir, inner).any(|looped| {
						matches!(
							self.ir.op(looped).map(|held| &held.kind),
							Some(OpKind::Phys(PhysOp::Loop { kind, .. })) if is_value_loop(*kind)
						)
					})
				}
				OpKind::Phys(_) | OpKind::Logical(_) => false,
			};
			if !evaluates {
				return Ok(false);
			}
		}
		Ok(true)
	}

	/// Whether `region`, the region of a statement, ends where it stands rather than running as a
	/// group: it holds the raise of an error, or ends in a `BREAK`, `CONTINUE` or `RETURN` its
	/// statement diverges to before it reads a row (a source that is the loop control itself).
	fn raises(&self, region: RegionId) -> Result<bool, IrError> {
		region_raises(self.ir, region)
	}

	/// How the run of the statement `op` gathers its value: as the `LET` or the `RETURN` that
	/// reads it takes it, dropped in the body of a `FOR` (or in an arm of an `IF` in one), and as
	/// a block's statement otherwise.
	fn collect_of(&self, op: OpId, data: &OpData) -> Result<Collect, IrError> {
		let reader = data.results.first().and_then(|result| match self.ir.uses(*result) {
			[only] => self.ir.op(only.user).map(|user| &user.kind),
			_ => None,
		});
		match reader {
			Some(OpKind::Phys(PhysOp::Let(_))) => return Ok(Collect::Let),
			Some(OpKind::Phys(PhysOp::Return)) => return Ok(Collect::Return),
			_ => {}
		}
		let mut region = self.ir.enclosing_region(op);
		loop {
			match self.kind_of(region)? {
				RegionKind::StatementFor => return Ok(Collect::Discard),
				RegionKind::StatementIf => {
					let owner = self
						.ir
						.parent_op(region)
						.ok_or(IrError::Stale("ir_lower::block: a region with no owner"))?;
					region = self.ir.enclosing_region(owner);
				}
				RegionKind::Request
				| RegionKind::Transaction(_)
				| RegionKind::Statement(_)
				| RegionKind::Block
				| RegionKind::ValueBlock
				| RegionKind::ValueIf
				| RegionKind::ValueFor
				| RegionKind::Loop
				| RegionKind::Subquery
				| RegionKind::FunctionBody
				| RegionKind::ClosureBody
				| RegionKind::PermissionFrame
				| RegionKind::FieldFrame(_)
				| RegionKind::EventFrame
				| RegionKind::SubscriberFrame
				| RegionKind::Clause(_)
				| RegionKind::LazyOperand
				| RegionKind::ErrorScope(_)
				| RegionKind::ChooseArm
				| RegionKind::Pipeline
				| RegionKind::JoinLookupArm
				| RegionKind::WriteStage
				| RegionKind::CatalogUnit
				| RegionKind::Deadline
				| RegionKind::StoredClause(_) => return Ok(Collect::Block),
			}
		}
	}

	/// Emits the run of the analysed group `statement`, which leaves in `dst` the rendering of
	/// `analysis` over what the run measured, and the program's analyses hold `analysis`.
	///
	/// A `BREAK`, `CONTINUE` or `RETURN` the statement signals ends it and is not raised, so no
	/// loop or context takes it.
	fn run_analyzed(
		&mut self,
		statement: u16,
		analysis: Analysis,
		dst: Reg,
		collect: Collect,
		resolve: Option<ContextLevel>,
	) -> Result<(), IrError> {
		let at = u16::try_from(self.lw.block_groups().analyses.len())
			.map_err(|_| IrError::Capacity("vm analyses"))?;
		self.lw.block_groups().analyses.push(analysis);
		self.block(BlockInstr::RunAnalyzed {
			statement,
			analysis: at,
			dst,
			collect,
			resolve,
		});
		Ok(())
	}

	/// Emits the run of the statement `runner` names, its value left in `dst`.
	///
	/// A `BREAK` or `CONTINUE` the statement signals goes to the loop that takes it, through a pad
	/// that restores the frames saved since that loop's iteration began: the statement runs in
	/// whatever frames the list has saved at this point. A `RETURN` it signals leaves the program.
	fn run_group(
		&mut self,
		runner: Runner,
		dst: Reg,
		collect: Collect,
		resolve: Option<ContextLevel>,
	) -> Result<(), IrError> {
		let looped = self.loops.last().map(|loop_| (loop_.depth, loop_.broken, loop_.continued));
		let pads = match looped {
			Some(_) => Some((self.label()?, self.label()?)),
			None => None,
		};
		let on_break = pads.map(|(on_break, _)| on_break);
		let on_continue = pads.map(|(_, on_continue)| on_continue);
		self.block(match runner {
			Runner::Embedded(statement) => BlockInstr::RunEmbedded {
				statement,
				dst,
				collect,
				on_break,
				on_continue,
				resolve,
			},
			Runner::Served(served) => BlockInstr::RunServed {
				served,
				dst,
				collect,
				on_break,
				on_continue,
				resolve,
			},
		});
		if pads.is_none() {
			return Ok(());
		}
		let over = self.label()?;
		self.jump(over);
		if let (Some((depth, broken, continued)), Some((on_break, on_continue))) = (looped, pads) {
			self.bind(on_break)?;
			for _ in depth..self.saved {
				self.block(BlockInstr::Restore);
			}
			self.jump(broken);
			self.bind(on_continue)?;
			for _ in depth..self.saved {
				self.block(BlockInstr::Restore);
			}
			self.jump(continued);
		}
		self.bind(over)
	}

	/// `LET`: the value is coerced to the declared kind, checked for depth and bound in a slot of
	/// its own, for the statements after it in the list.
	fn local_let(&mut self, data: &OpData, attrs: &LetAttrs) -> Result<(), IrError> {
		if attrs.binding != LetBinding::Local {
			return Err(IrError::Invariant {
				what: "a LET of a session parameter is a statement of a request, never of a list",
			});
		}
		let [value] = Self::operands::<1>(data)?;
		let value = self.lw.value(value)?;
		let bound = self.reg()?;
		self.load(bound, value);
		let name = self.lw.emitter().field(attrs.name.as_str())?;
		if let Some(kind) = &attrs.ty {
			let kind = self.lw.emitter().kind(kind.clone())?;
			self.block(BlockInstr::CoerceKind {
				value: bound,
				kind,
				name,
			});
		}
		self.block(BlockInstr::CheckDepth {
			value: bound,
		});
		let slot = self.next_slot()?;
		let bind = self.block(BlockInstr::Bind {
			name,
			value: bound,
			slot,
			frame: true,
		});
		let em = self.lw.emitter();
		let (code, sites) = (em.code().len(), em.call_sites());
		let named = self.lw.named_reads();
		self.scopes.push(SlotScope {
			bind,
			name: attrs.name.as_str().to_owned(),
			code,
			sites,
			named,
			grouped: self.lw.block_groups().statements(),
		});
		if let Some(result) = data.results.first() {
			self.lw.bind_slot_value(*result, slot, Some(attrs.name.clone()));
		}
		Ok(())
	}

	fn next_slot(&mut self) -> Result<u16, IrError> {
		let slot = self.slots;
		self.slots = slot.checked_add(1).ok_or(IrError::Capacity("vm slots"))?;
		Ok(slot)
	}

	/// Ends the scopes of the names bound since `outer` scopes were open, innermost first.
	///
	/// A name is bound in its slot alone when everything lowered in its scope is the program's own
	/// work, which reads the name through the slot, and no call or method site was added: nothing
	/// reads it by name through the frame.
	fn close_scopes(&mut self, outer: usize) -> Result<(), IrError> {
		while self.scopes.len() > outer {
			let Some(scope) = self.scopes.pop() else {
				break;
			};
			// A group's pipelines read the names of the frame through code that is not the list's
			// own, so a scope that holds one keeps its name in the frame.
			let by_name = self.lw.read_by_name_since(scope.named, &scope.name)
				|| self.lw.block_groups().statements() > scope.grouped;
			let em = self.lw.emitter();
			let code = em.code().get(scope.code..).unwrap_or(&[]);
			let frame_free = !by_name
				&& em.call_sites() == scope.sites
				&& code.iter().all(lowered_whole)
				&& !code.iter().any(|instr| match instr {
					Instr::LoadParam {
						param,
						..
					} => em.param_name(*param) == Some(scope.name.as_str()),
					Instr::CoerceField {
						..
					}
					| Instr::BeginComputing {
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
					| Instr::CompareFieldInt {
						..
					}
					| Instr::JumpIfFieldInt {
						..
					}
					| Instr::JumpIfCompare {
						..
					}
					| Instr::Compare {
						..
					}
					| Instr::Arith {
						..
					}
					| Instr::Unary {
						..
					}
					| Instr::NewArray {
						..
					}
					| Instr::ArrayPush {
						..
					}
					| Instr::LoadSlot {
						..
					}
					| Instr::SortKey {
						..
					}
					| Instr::Fetch {
						..
					}
					| Instr::Access {
						..
					}
					| Instr::Each {
						..
					}
					| Instr::Throw {
						..
					}
					| Instr::FilterBegin {
						..
					}
					| Instr::MapBegin {
						..
					}
					| Instr::CollectPush {
						..
					}
					| Instr::Nav {
						..
					}
					| Instr::CallAllowed {
						..
					}
					| Instr::Call {
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
					| Instr::Method {
						..
					}
					| Instr::MethodClosure {
						..
					}
					| Instr::JumpIfNullish {
						..
					}
					| Instr::JumpIfNone {
						..
					}
					| Instr::JumpIf {
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
					| Instr::NewObject {
						..
					}
					| Instr::SeedObject {
						..
					}
					| Instr::BuildObject {
						..
					}
					| Instr::LoadPath {
						..
					}
					| Instr::LoadRow {
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
					| Instr::SetField {
						..
					}
					| Instr::WriteProjection {
						..
					}
					| Instr::Load {
						..
					}
					| Instr::Emit {
						..
					}
					| Instr::EmitEnd {
						..
					}
					| Instr::BuildObjectEmit {
						..
					}
					| Instr::FilterFieldInt {
						..
					}
					| Instr::FilterCompare {
						..
					}
					| Instr::Filter {
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
					| Instr::Jump {
						..
					}
					| Instr::Next {
						..
					}
					| Instr::Write(_)
					| Instr::Store(_)
					| Instr::Block(_)
					| Instr::EndRow
					| Instr::StopPipeline
					| Instr::TryNone {
						..
					}
					| Instr::TryEnd
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
					| Instr::NewSet {
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
					}
					| Instr::Ir(_) => false,
				});
			if frame_free {
				em.set_bind_frame(scope.bind, false)?;
			}
		}
		Ok(())
	}

	/// A scope: the statements of `region` run in a frame of their own, and its value, the value
	/// the region yields, is left in `dst`. A `RETURN` in it ends the context the scope stands in.
	fn scope(&mut self, region: RegionId, dst: Reg) -> Result<Exit, IrError> {
		self.block(BlockInstr::Save);
		self.saved += 1;
		let exit = self.statements(region, false)?;
		match exit {
			Exit::Yields(Some(value)) => self.load(dst, value),
			Exit::Yields(None) => {
				let none = self.none()?;
				self.load(dst, none);
			}
			Exit::Diverges => {}
		}
		self.saved -= 1;
		self.block(BlockInstr::Restore);
		Ok(exit)
	}

	/// `IF`: the arm of the condition's truth, else the other arm, each leaving its value in the
	/// op's result register.
	fn branch(&mut self, data: &OpData) -> Result<(), IrError> {
		let [condition] = Self::operands::<1>(data)?;
		let [then, otherwise] = Self::regions::<2>(data)?;
		let dst = self.reg()?;
		let cond = self.lw.value(condition)?;
		let else_arm = self.label()?;
		let end = self.label()?;
		self.emit(Instr::JumpIf {
			cond,
			truthy: false,
			target: else_arm,
		});
		self.arm(then, dst)?;
		self.jump(end);
		self.bind(else_arm)?;
		self.arm(otherwise, dst)?;
		self.bind(end)?;
		self.define(data, Operand::Reg(dst));
		Ok(())
	}

	/// An arm of an `IF`, its value left in `dst`.
	fn arm(&mut self, region: RegionId, dst: Reg) -> Result<(), IrError> {
		if self.is_plain_arm(region)? {
			match self.statements(region, false)? {
				Exit::Yields(Some(value)) => self.load(dst, value),
				Exit::Yields(None) => {
					let none = self.none()?;
					self.load(dst, none);
				}
				Exit::Diverges => {}
			}
			return Ok(());
		}
		match self.kind_of(region)? {
			RegionKind::ValueIf | RegionKind::StatementIf => {}
			RegionKind::Request
			| RegionKind::Transaction(_)
			| RegionKind::Statement(_)
			| RegionKind::Block
			| RegionKind::ValueBlock
			| RegionKind::StatementFor
			| RegionKind::ValueFor
			| RegionKind::Loop
			| RegionKind::Subquery
			| RegionKind::FunctionBody
			| RegionKind::ClosureBody
			| RegionKind::PermissionFrame
			| RegionKind::FieldFrame(_)
			| RegionKind::EventFrame
			| RegionKind::SubscriberFrame
			| RegionKind::Clause(_)
			| RegionKind::LazyOperand
			| RegionKind::ErrorScope(_)
			| RegionKind::ChooseArm
			| RegionKind::Pipeline
			| RegionKind::JoinLookupArm
			| RegionKind::WriteStage
			| RegionKind::CatalogUnit
			| RegionKind::Deadline
			| RegionKind::StoredClause(_) => {
				return Err(IrError::Stale("ir_lower::block: an arm of another region kind"));
			}
		}
		self.scope(region, dst)?;
		Ok(())
	}

	/// `FOR`: the range is evaluated once, then the body runs for each element with the element
	/// bound in a slot, the frame saved before and restored after each iteration. `CONTINUE`
	/// moves to the next element and `BREAK` leaves. The loop's value is `NONE`: a `RETURN` in its
	/// body ends the context the loop stands in.
	///
	/// A loop with a first body runs it for its first element and the other body for the rest, a
	/// register noting that the first has run.
	fn for_loop(
		&mut self,
		data: &OpData,
		first: bool,
		param: Option<&Strand>,
	) -> Result<(), IrError> {
		let [source] = Self::operands::<1>(data)?;
		let (body, first_body) = match (first, data.regions.as_slice()) {
			(false, [body]) => (*body, None),
			(true, [body, first_body]) => (*body, Some(*first_body)),
			_ => return Err(IrError::Stale("ir_lower::block: region arity")),
		};
		let dst = self.reg()?;
		self.iterate(source, body, first_body, param)?;
		let none = self.none()?;
		self.load(dst, none);
		self.define(data, Operand::Reg(dst));
		Ok(())
	}

	/// The iteration of a `FOR`: from the evaluation of its range to its exit.
	///
	/// The element is bound in a slot each iteration. With the loop's `param`, it is also bound
	/// in the frame under that name, which a scope closes as it closes a `LET`'s: the frame binding
	/// stays only where something the body runs reads the name through the frame.
	fn iterate(
		&mut self,
		source: ValueId,
		body: RegionId,
		first_body: Option<RegionId>,
		param: Option<&Strand>,
	) -> Result<(), IrError> {
		let range = self.lw.value(source)?;
		let range = self.reg_of(range)?;
		let peeled = match first_body {
			Some(_) => {
				let peeled = self.reg()?;
				let not_yet = Operand::Const(self.lw.emitter().constant(Value::Bool(false))?);
				self.load(peeled, not_yet);
				Some(peeled)
			}
			None => None,
		};
		let iterator = self.iterators;
		self.iterators = iterator.checked_add(1).ok_or(IrError::Capacity("vm iterators"))?;
		self.block(BlockInstr::IterStart {
			src: range,
			slot: iterator,
		});
		let element = self.reg()?;
		let head = self.label()?;
		let exit = self.label()?;
		self.bind(head)?;
		self.block(BlockInstr::IterNext {
			slot: iterator,
			dst: element,
			done: exit,
		});
		self.block(BlockInstr::Save);
		self.saved += 1;
		let slot = self.next_slot()?;
		let name = self.lw.emitter().field(param.map_or("", Strand::as_str))?;
		let bind = self.block(BlockInstr::Bind {
			name,
			value: element,
			slot,
			frame: param.is_some(),
		});
		let outer_scopes = self.scopes.len();
		if let Some(param) = param {
			let em = self.lw.emitter();
			let (code, sites) = (em.code().len(), em.call_sites());
			let named = self.lw.named_reads();
			self.scopes.push(SlotScope {
				bind,
				name: param.as_str().to_owned(),
				code,
				sites,
				named,
				grouped: self.lw.block_groups().statements(),
			});
		}
		self.bind_element(body, slot, param)?;
		let broken = self.label()?;
		let continued = self.label()?;
		self.loops.push(LoopTargets {
			depth: self.saved,
			broken,
			continued,
		});
		let lowered = match (first_body, peeled) {
			(Some(first_body), Some(peeled)) => {
				self.bind_element(first_body, slot, param)?;
				self.peeled_body(first_body, body, peeled)
			}
			_ => self.statements(body, false).map(|_| ()),
		};
		self.loops.pop();
		self.saved -= 1;
		lowered?;
		self.close_scopes(outer_scopes)?;
		self.block(BlockInstr::Restore);
		self.jump(head);
		self.bind(continued)?;
		self.block(BlockInstr::Restore);
		self.jump(head);
		self.bind(broken)?;
		self.block(BlockInstr::Restore);
		self.bind(exit)?;
		Ok(())
	}

	/// Makes the element argument of the loop body `region` a read of `slot`, bound in the frame
	/// as `param` when the loop names one. The position argument has no source in the program:
	/// no `FOR` of the language names it, so a body that reads it is an invariant break.
	fn bind_element(
		&mut self,
		region: RegionId,
		slot: u16,
		param: Option<&Strand>,
	) -> Result<(), IrError> {
		let args = self.args_of(region)?;
		let (element, rest) = args
			.split_first()
			.ok_or(IrError::Stale("ir_lower::block: a loop body with no element"))?;
		self.lw.bind_slot_value(*element, slot, param.cloned());
		if rest.iter().any(|arg| !self.ir.uses(*arg).is_empty()) {
			return Err(invariant("a FOR body reads a position its statement never names"));
		}
		Ok(())
	}

	/// The body of a loop with a first body: `first` while `peeled` is false, which it sets, and
	/// `rest` once it is true.
	fn peeled_body(&mut self, first: RegionId, rest: RegionId, peeled: Reg) -> Result<(), IrError> {
		let to_rest = self.label()?;
		let done = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(peeled),
			truthy: true,
			target: to_rest,
		});
		let yes = Operand::Const(self.lw.emitter().constant(Value::Bool(true))?);
		self.load(peeled, yes);
		self.statements(first, false)?;
		self.jump(done);
		self.bind(to_rest)?;
		self.statements(rest, false)?;
		self.bind(done)?;
		Ok(())
	}

	/// `RETURN` of `value`: no region a statement list holds is a context, so the `RETURN` leaves
	/// the program with its value, for the run that holds the program to take it.
	fn leave_scope(&mut self, value: Operand) -> Result<(), IrError> {
		let value = self.reg_of(value)?;
		self.block(BlockInstr::Return {
			value,
		});
		Ok(())
	}

	/// `BREAK`, or `CONTINUE` when not `leaves`: the frames saved inside the loop's iteration are
	/// restored on the way out of them. A loop control of a list this program holds no loop around
	/// is written in a loop of an enclosing program (a clause of a statement in that loop's body),
	/// so it signals out of the program to the run that takes it; the logical phase raises a
	/// control that no loop is written around before it reaches a program.
	fn leave_loop(&mut self, leaves: bool) -> Result<(), IrError> {
		let Some(targets) = self.loops.last() else {
			let frame = self.lw.frame.program;
			self.emit(Instr::Signal {
				brk: leaves,
				frame,
			});
			return Ok(());
		};
		let (depth, target) = match leaves {
			true => (targets.depth, targets.broken),
			false => (targets.depth, targets.continued),
		};
		for _ in depth..self.saved {
			self.block(BlockInstr::Restore);
		}
		self.jump(target);
		Ok(())
	}
}

/// The spec of a group of one pipeline over [`Source::Once`] that evaluates a value and emits it
/// as its one row: a scalar statement that reads no rows, whose pools start at `base`.
fn value_group_spec(base: PoolBase, permissions: std::ops::Range<u16>) -> GroupSpec {
	GroupSpec {
		write: None,
		scalar: true,
		rowless: true,
		sorts: base.sorts..base.sorts,
		aggregates: base.aggregates..base.aggregates,
		permissions,
		version: None,
		timeout: None,
		only: false,
	}
}

/// The write `region` holds, alone or in the `Deadline` region of the `TIMEOUT` that bounds it,
/// with the region that holds it and the timeout's duration.
fn written_in(
	ir: &Ir,
	region: RegionId,
) -> Result<Option<(OpId, RegionId, Option<ValueId>)>, IrError> {
	match timed_write_in(ir, region)? {
		Some((timed, write)) => {
			let held = ir.op(timed).ok_or(IrError::Stale("ir_lower::block: op"))?;
			match (held.operands.as_slice(), held.regions.as_slice()) {
				([duration], [deadline]) => Ok(Some((write, *deadline, Some(*duration)))),
				_ => Err(invariant("a phys.timeout with other than one operand and one region")),
			}
		}
		None => Ok(write_of(ir, region).map(|write| (write, region, None))),
	}
}

/// Whether `instr` is the program's own work, which reads a block name only through its slot: no
/// compiled expression, host call or record fetch.
fn lowered_whole(instr: &Instr) -> bool {
	matches!(
		instr,
		Instr::Block(_)
			| Instr::Compare { .. }
			| Instr::Arith { .. }
			| Instr::Unary { .. }
			| Instr::NewArray { .. }
			| Instr::ArrayPush { .. }
			| Instr::LoadParam { .. }
			| Instr::LoadSlot { .. }
			| Instr::Nav { .. }
			| Instr::JumpIfNullish { .. }
			| Instr::JumpIfNone { .. }
			| Instr::JumpIf { .. }
			| Instr::NewObject { .. }
			| Instr::BuildObject { .. }
			| Instr::Load { .. }
			| Instr::Emit { .. }
			| Instr::EmitEnd { .. }
			| Instr::Jump { .. }
			| Instr::Signal { .. }
			| Instr::EndRow
	)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::super::tests::Builder;
	use super::*;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::vm::ir_lower::lower;
	use crate::exec::vm::program::Program;
	use crate::expr::{BinaryOperator, Kind};

	fn lowered(b: &mut Builder) -> Program {
		b.physical();
		match lower(&b.ir, &FactCache::new()) {
			Ok(lowered) => lowered.program,
			Err(error) => panic!("the container lowers: {error}"),
		}
	}

	/// The code of the program's own pipeline: the one stamped with no group, which follows the
	/// pipelines of its statements' groups.
	fn code(program: &Program) -> &[Instr] {
		let own: Vec<_> =
			program.pipelines.iter().filter(|pipeline| pipeline.group.is_none()).collect();
		assert_eq!(own.len(), 1);
		assert!(program.pipelines.last().is_some_and(|last| last.group.is_none()));
		&own[0].code
	}

	fn blocks(program: &Program) -> Vec<&BlockInstr> {
		code(program)
			.iter()
			.filter_map(|instr| {
				if let Instr::Block(block) = instr {
					Some(block)
				} else {
					None
				}
			})
			.collect()
	}

	fn binds(program: &Program) -> Vec<(u16, bool)> {
		blocks(program)
			.into_iter()
			.filter_map(|block| {
				if let BlockInstr::Bind {
					slot,
					frame,
					..
				} = block
				{
					Some((*slot, *frame))
				} else {
					None
				}
			})
			.collect()
	}

	fn count(program: &Program, wanted: fn(&BlockInstr) -> bool) -> usize {
		blocks(program).into_iter().filter(|block| wanted(block)).count()
	}

	fn saves(program: &Program) -> usize {
		count(program, |block| matches!(block, BlockInstr::Save))
	}

	fn restores(program: &Program) -> usize {
		count(program, |block| matches!(block, BlockInstr::Restore))
	}

	/// The op of `kind` over `operands`, with one result.
	fn statement(b: &mut Builder, op: PhysOp, operands: &[ValueId]) -> OpId {
		b.add_phys_results(op, operands, 1)
	}

	fn let_attrs(name: &str, ty: Option<Kind>) -> PhysOp {
		PhysOp::Let(Box::new(LetAttrs {
			name: Strand::new(name),
			binding: LetBinding::Local,
			ty,
		}))
	}

	fn for_op() -> PhysOp {
		PhysOp::Loop {
			kind: LoopKind::For,
			first: false,
			batch: crate::exec::ir::phys::ops::Batch {
				max_iterations: 1,
				max_bytes: 1,
			},
			unroll: 1,
			value: false,
			step: None,
		}
	}

	/// A `FOR` standing in value position.
	fn value_for_op() -> PhysOp {
		let mut op = for_op();
		if let PhysOp::Loop {
			value,
			..
		} = &mut op
		{
			*value = true;
		}
		op
	}

	/// `{ 1 + 2 }` yielded by the top region.
	fn block_of_sum() -> Builder {
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::ValueBlock, &[]);
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let sum = b.binary(BinaryOperator::Add, one, two);
		b.add_phys(PhysOp::Yield, &[sum]);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		b
	}

	#[test]
	fn a_block_runs_in_a_frame_of_its_own_and_yields_its_value() {
		let program = lowered(&mut block_of_sum());
		assert!(program.scalar);
		assert!(!program.rows);
		assert_eq!(program.access_mode, AccessMode::ReadOnly);
		assert_eq!((saves(&program), restores(&program)), (1, 1));
		let code = code(&program);
		assert!(matches!(code.last(), Some(Instr::EndRow)));
		assert!(code.iter().any(|instr| matches!(instr, Instr::Arith { .. })));
	}

	#[test]
	fn a_container_with_no_control_is_not_a_statement_program() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Eval, &[one]);
		b.physical();
		assert!(!holds_control(&b.ir).unwrap());
		let mut b = block_of_sum();
		b.physical();
		assert!(holds_control(&b.ir).unwrap());
	}

	/// `{ LET $a = 1; $a + 2 }`.
	fn let_then_read(ty: Option<Kind>, with_call: bool) -> Builder {
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let one = b.literal(Value::from(1_i64));
		let bound = statement(&mut b, let_attrs("a", ty), &[one]);
		let a = b.result_of(bound);
		let two = b.literal(Value::from(2_i64));
		let mut sum = b.binary(BinaryOperator::Add, a, two);
		if with_call {
			sum = b.call("math::abs", &[sum]);
		}
		b.add_phys(PhysOp::Yield, &[sum]);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		b
	}

	#[test]
	fn a_let_is_bound_in_a_slot_and_read_by_slot() {
		let program = lowered(&mut let_then_read(None, false));
		let instrs = blocks(&program);
		assert!(instrs.iter().any(|block| matches!(block, BlockInstr::CheckDepth { .. })));
		assert!(!instrs.iter().any(|block| matches!(block, BlockInstr::CoerceKind { .. })));
		assert_eq!(binds(&program), vec![(0, false)]);
		assert!(code(&program).iter().any(|instr| matches!(
			instr,
			Instr::LoadSlot {
				slot: 0,
				..
			}
		)));
	}

	#[test]
	fn a_typed_let_coerces_before_it_binds() {
		let program = lowered(&mut let_then_read(Some(Kind::Int), false));
		let order: Vec<_> = blocks(&program)
			.into_iter()
			.filter_map(|block| match block {
				BlockInstr::CoerceKind {
					..
				} => Some("coerce"),
				BlockInstr::CheckDepth {
					..
				} => Some("depth"),
				BlockInstr::Bind {
					..
				} => Some("bind"),
				BlockInstr::RunEmbedded {
					..
				}
				| BlockInstr::Enter
				| BlockInstr::Save
				| BlockInstr::Restore
				| BlockInstr::Resolve
				| BlockInstr::Clock
				| BlockInstr::Return {
					..
				}
				| BlockInstr::IterStart {
					..
				}
				| BlockInstr::IterNext {
					..
				}
				| BlockInstr::Sleep {
					..
				}
				| BlockInstr::Nested {
					..
				}
				| BlockInstr::Member {
					..
				}
				| BlockInstr::RunServed {
					..
				}
				| BlockInstr::RunAnalyzed {
					..
				} => None,
			})
			.collect();
		assert_eq!(order, vec!["coerce", "depth", "bind"]);
		assert_eq!(program.kinds, vec![Kind::Int]);
	}

	#[test]
	fn a_let_read_by_a_call_binds_in_the_frame_too() {
		let program = lowered(&mut let_then_read(None, true));
		assert_eq!(binds(&program), vec![(0, true)]);
	}

	/// A top region that is `FOR $i IN [1, 2] { <body> }`, `body` building the statements of the
	/// loop body from the element, and yielding nothing.
	fn for_over(body: impl FnOnce(&mut Builder, ValueId)) -> Builder {
		for_over_op(for_op(), body)
	}

	/// The `FOR` of [`for_over`] as the loop op `op`.
	fn for_over_op(op: PhysOp, body: impl FnOnce(&mut Builder, ValueId)) -> Builder {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let range = b.add(ExprOp::Array, &[one, two]);
		let looped = statement(&mut b, op, &[range]);
		b.open(looped, RegionKind::StatementFor, &[TypeKnowledge::Unknown, TypeKnowledge::Unknown]);
		let element = b.block_args()[0];
		body(&mut b, element);
		b.close();
		let value = b.result_of(looped);
		b.add_phys(PhysOp::Yield, &[value]);
		b
	}

	#[test]
	fn a_for_loops_over_its_range_with_three_landings() {
		let program = lowered(&mut for_over(|b, element| {
			let two = b.literal(Value::from(2_i64));
			let sum = b.binary(BinaryOperator::Add, element, two);
			statement(b, PhysOp::Eval, &[sum]);
			b.add_phys(PhysOp::Yield, &[]);
		}));
		assert_eq!(count(&program, |block| matches!(block, BlockInstr::IterStart { .. })), 1);
		assert_eq!(count(&program, |block| matches!(block, BlockInstr::IterNext { .. })), 1);
		// One save per iteration, restored at the end of the body, at the continue landing and at
		// the break landing.
		assert_eq!(saves(&program), 1);
		assert_eq!(restores(&program), 3);
		// The element is bound in its slot alone.
		assert_eq!(binds(&program), vec![(0, false)]);
		// The body jumps back to the head, so the pipeline holds a backward jump.
		let code = code(&program);
		let head = code
			.iter()
			.position(|instr| matches!(instr, Instr::Block(BlockInstr::IterNext { .. })))
			.expect("the loop has a head");
		let back = code
			.iter()
			.filter(|instr| matches!(instr, Instr::Jump { target } if *target as usize == head))
			.count();
		assert_eq!(back, 2);
		assert!(!program.pipelines[0].in_place);
	}

	/// `FOR $i IN [1, 2] { <body> }` with the loop stating its parameter `i`.
	fn named_for_over(body: impl FnOnce(&mut Builder, ValueId)) -> Builder {
		let mut op = for_op();
		if let PhysOp::Loop {
			step,
			..
		} = &mut op
		{
			*step = Some(Box::new(StepAttrs::Param(Strand::new("i"))));
		}
		for_over_op(op, body)
	}

	#[test]
	fn a_named_for_element_is_bound_in_the_frame_only_where_it_is_read_by_name() {
		// FOR $i IN [1, 2] { $i + 2 }: the program's own work reads the slot.
		let program = lowered(&mut named_for_over(|b, element| {
			let two = b.literal(Value::from(2_i64));
			let sum = b.binary(BinaryOperator::Add, element, two);
			statement(b, PhysOp::Eval, &[sum]);
			b.add_phys(PhysOp::Yield, &[]);
		}));
		assert_eq!(binds(&program), vec![(0, false)]);
		assert!(program.fields.iter().any(|field| field.as_str() == "i"));
		// FOR $i IN [1, 2] { math::abs($i) }: a call may read the name through the frame.
		let program = lowered(&mut named_for_over(|b, element| {
			let called = b.call("math::abs", &[element]);
			statement(b, PhysOp::Eval, &[called]);
			b.add_phys(PhysOp::Yield, &[]);
		}));
		assert_eq!(binds(&program), vec![(0, true)]);
	}

	#[test]
	fn break_and_continue_restore_the_frames_saved_inside_the_iteration() {
		// FOR $i IN [1, 2] { { BREAK } }: the inner block's frame is restored before the jump.
		let program = lowered(&mut for_over(|b, _| {
			let block = statement(b, PhysOp::Block, &[]);
			b.open(block, RegionKind::Block, &[]);
			b.add_phys(PhysOp::Break, &[]);
			b.close();
			b.add_phys(PhysOp::Yield, &[]);
		}));
		// Saves: the iteration and the block. Restores: the break's restore of the block's frame,
		// the three landings of the loop, and the end of the block.
		assert_eq!(saves(&program), 2);
		assert_eq!(restores(&program), 5);
		let program = lowered(&mut for_over(|b, _| {
			b.add_phys(PhysOp::Next, &[]);
		}));
		// A `CONTINUE` directly in the body restores nothing of its own.
		assert_eq!(restores(&program), 3);
	}

	#[test]
	fn a_return_inside_a_block_leaves_the_program() {
		// { RETURN 1 }: a block is no context, so the `RETURN` leaves the program with its value
		// rather than ending the block.
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let one = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Return, &[one]);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		let program = lowered(&mut b);
		assert!(blocks(&program).iter().any(|block| matches!(block, BlockInstr::Return { .. })));
		assert_eq!(saves(&program), 1);
		assert!(!code(&program).iter().any(|instr| matches!(instr, Instr::Jump { .. })));
	}

	#[test]
	fn a_return_no_scope_catches_leaves_the_program_and_reports_rows() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Return, &[one]);
		let program = lowered(&mut b);
		assert!(program.rows);
		assert!(blocks(&program).iter().any(|block| matches!(block, BlockInstr::Return { .. })));
		// A `RETURN` in a statement-position `IF` arm passes outward, so it is the same signal.
		let mut b = Builder::new();
		let truth = b.literal(Value::Bool(true));
		let branch = statement(
			&mut b,
			PhysOp::If {
				value: false,
			},
			&[truth],
		);
		b.open(branch, RegionKind::StatementIf, &[]);
		let one = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Return, &[one]);
		b.close();
		b.open(branch, RegionKind::StatementIf, &[]);
		let none = b.literal(Value::None);
		b.add_phys(PhysOp::Yield, &[none]);
		b.close();
		let value = b.result_of(branch);
		b.add_phys(PhysOp::Yield, &[value]);
		let program = lowered(&mut b);
		assert!(!program.rows);
		assert!(blocks(&program).iter().any(|block| matches!(block, BlockInstr::Return { .. })));
		// The arm that holds a statement runs in a frame of its own.
		assert_eq!(saves(&program), 1);
	}

	#[test]
	fn an_if_jumps_over_the_arm_the_condition_does_not_pick() {
		let mut b = Builder::new();
		let cond = b.literal(Value::Bool(true));
		let branch = statement(
			&mut b,
			PhysOp::If {
				value: true,
			},
			&[cond],
		);
		b.open(branch, RegionKind::ValueIf, &[]);
		let one = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Yield, &[one]);
		b.close();
		b.open(branch, RegionKind::ValueIf, &[]);
		let two = b.literal(Value::from(2_i64));
		b.add_phys(PhysOp::Yield, &[two]);
		b.close();
		let value = b.result_of(branch);
		b.add_phys(PhysOp::Yield, &[value]);
		let program = lowered(&mut b);
		// Arms of expressions alone need no frame.
		assert_eq!(saves(&program), 0);
		let code = code(&program);
		let jumps = code
			.iter()
			.filter(|instr| {
				matches!(
					instr,
					Instr::JumpIf {
						truthy: false,
						..
					}
				)
			})
			.count();
		assert_eq!(jumps, 1);
		let loads = code.iter().filter(|instr| matches!(instr, Instr::Load { .. })).count();
		// The result's NONE, one value per arm, and the result of the top region.
		assert_eq!(loads, 4);
	}

	#[test]
	fn a_return_in_a_value_position_for_leaves_the_program() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let range = b.add(ExprOp::Array, &[one]);
		let looped = statement(&mut b, value_for_op(), &[range]);
		b.open(looped, RegionKind::ValueFor, &[TypeKnowledge::Unknown, TypeKnowledge::Unknown]);
		let seven = b.literal(Value::from(7_i64));
		b.add_phys(PhysOp::Return, &[seven]);
		b.close();
		let value = b.result_of(looped);
		b.add_phys(PhysOp::Yield, &[value]);
		let program = lowered(&mut b);
		assert!(blocks(&program).iter().any(|block| matches!(block, BlockInstr::Return { .. })));
		// Only the iteration is saved; the loop restores at the end of its body and at each of
		// its two landings.
		assert_eq!(saves(&program), 1);
		assert_eq!(restores(&program), 3);
	}

	#[test]
	fn sleep_makes_the_program_read_write() {
		let mut b = Builder::new();
		let duration = b.literal(Value::from(1_i64));
		let slept = statement(&mut b, PhysOp::Sleep, &[duration]);
		let value = b.result_of(slept);
		b.add_phys(PhysOp::Yield, &[value]);
		let program = lowered(&mut b);
		assert_eq!(program.access_mode, AccessMode::ReadWrite);
		assert!(blocks(&program).iter().any(|block| matches!(block, BlockInstr::Sleep { .. })));
	}

	#[test]
	fn a_throw_raises_its_value_and_ends_the_list() {
		let mut b = Builder::new();
		let message = b.literal(Value::from("boom"));
		b.terminate(ExprOp::Throw, &[message]);
		let program = lowered(&mut b);
		assert!(code(&program).iter().any(|instr| matches!(instr, Instr::Throw { .. })));
	}

	fn raising(kind: RaiseKind) -> Builder {
		let mut b = Builder::new();
		b.terminate(ExprOp::Raise(Box::new(kind)), &[]);
		b
	}

	#[test]
	fn a_loop_control_no_loop_takes_raises_its_error() {
		let program = lowered(&mut raising(RaiseKind::InvalidControlFlow));
		assert!(code(&program).iter().any(|instr| matches!(instr, Instr::InvalidControlFlow)));
		assert_eq!(program.access_mode, AccessMode::ReadOnly);
	}

	#[test]
	fn a_refused_statement_raises_its_refusal_when_reached() {
		let refusal = crate::exec::vm::payload::Refusal::NamespaceEmpty;
		let program = lowered(&mut raising(RaiseKind::Refused(Box::new(refusal.clone()))));
		let code = code(&program);
		let refuse = code
			.iter()
			.position(|instr| matches!(instr, Instr::Store(StoreInstr::Refuse { .. })))
			.expect("the program raises the refusal");
		let Instr::Store(StoreInstr::Refuse {
			refusal: held,
		}) = &code[refuse]
		else {
			unreachable!();
		};
		assert_eq!(**held, refusal);
		// What follows the refusal answers `NONE`, as the statement does when it is not reached.
		assert!(matches!(code.last(), Some(Instr::EndRow)));
	}

	#[test]
	fn a_misplaced_control_is_never_lowered_as_a_program() {
		let mut b = raising(RaiseKind::Misplaced(
			crate::exec::ir::expr::ops::MisplacedControl::NestedBegin,
		));
		b.physical();
		assert!(matches!(lower(&b.ir, &FactCache::new()), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_request_step_in_a_block_is_an_invariant_error() {
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		b.add_phys(
			PhysOp::Option {
				name: Strand::new("IMPORT"),
				value: true,
			},
			&[],
		);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		b.physical();
		assert!(matches!(lower(&b.ir, &FactCache::new()), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_live_in_a_block_is_an_invariant_error() {
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		b.add_phys(
			PhysOp::Live(Box::new(crate::exec::ir::logical::ops::LiveAttrs {
				id: crate::val::Uuid(uuid::Uuid::nil()),
				node: crate::val::Uuid(uuid::Uuid::nil()),
				fields: crate::exec::ir::iface::statements::LiveShape::Diff,
				template: crate::exec::ir::iface::statements::DefinitionTemplate {
					ty: "StoredSubscriptionDefinition",
					value: Value::None,
					holes: SmallVec::new(),
				},
			})),
			&[],
		);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		b.physical();
		assert!(matches!(lower(&b.ir, &FactCache::new()), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_kill_in_a_block_is_an_invariant_error() {
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let id = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Kill, &[id]);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		b.physical();
		assert!(matches!(lower(&b.ir, &FactCache::new()), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_value_position_loop_is_lowered_where_it_is_read() {
		// The loop stands in the list as an expression: it is skipped as a statement and lowered
		// by the `LET` that reads it.
		let mut b = Builder::new();
		let xs = b.add(
			ExprOp::FreeParam {
				name: "xs".into(),
			},
			&[],
		);
		let mut op = for_op();
		if let PhysOp::Loop {
			kind,
			..
		} = &mut op
		{
			*kind = LoopKind::ElementFilter;
		}
		let looped = b.add_phys_value(op, &[xs]);
		b.open_with(
			looped,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| {
				let truth = b.add(ExprOp::Truthy, &[args[0]]);
				b.yield_phys(truth);
			},
		);
		let filtered = b.result_of(looped);
		let bound = statement(&mut b, let_attrs("a", None), &[filtered]);
		let a = b.result_of(bound);
		b.add_phys(PhysOp::Yield, &[a]);
		let program = lowered(&mut b);
		assert!(code(&program).iter().any(|instr| matches!(instr, Instr::FilterBegin { .. })));
	}

	/// The error lowering `b` answers.
	fn broken(b: &mut Builder) -> IrError {
		b.physical();
		match lower(&b.ir, &FactCache::new()) {
			Err(error) => error,
			Ok(_) => panic!("the container lowers"),
		}
	}

	#[test]
	fn an_op_of_a_pipeline_met_as_a_statement_of_a_list_is_an_invariant_break() {
		// A streaming op stands in a pipeline, never in a list.
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let rows = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Filter, &[rows]);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		assert!(matches!(broken(&mut b), IrError::Invariant { .. }));

		// A `SELECT` loop belongs to the statement that holds it.
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let one = b.literal(Value::from(1_i64));
		let mut select = for_op();
		if let PhysOp::Loop {
			kind,
			..
		} = &mut select
		{
			*kind = LoopKind::Select;
		}
		statement(&mut b, select, &[one]);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		assert!(matches!(broken(&mut b), IrError::Invariant { .. }));
	}

	#[test]
	fn a_list_yields_at_most_one_value() {
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		b.add_phys(PhysOp::Yield, &[one, two]);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		assert!(matches!(broken(&mut b), IrError::Invariant { .. }));
	}

	#[test]
	fn a_loop_that_is_not_a_for_at_the_top_is_refused_by_name() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let mut other = for_op();
		if let PhysOp::Loop {
			kind,
			..
		} = &mut other
		{
			*kind = LoopKind::Select;
		}
		statement(&mut b, other, &[one]);
		assert_eq!(b.refused(), "ir_lower::lower: phys.loop");
	}

	#[test]
	fn an_expression_no_op_of_its_list_reads_runs_where_it_stands() {
		// A call nothing reads still runs: it may raise or write.
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let minus = b.literal(Value::from(-1_i64));
		b.call("math::abs", &[minus]);
		let one = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Yield, &[one]);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		let program = lowered(&mut b);
		assert_eq!(
			code(&program).iter().filter(|instr| matches!(instr, Instr::Call { .. })).count(),
			1
		);
	}

	#[test]
	fn an_expression_only_a_nested_region_reads_runs_once_before_the_region() {
		// `{ <call>; { <call> } }`-shaped: the outer call is read by the yield of a nested block,
		// which would otherwise lower it on its own path alone.
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let minus = b.literal(Value::from(-1_i64));
		let called = b.call("math::abs", &[minus]);
		let inner = statement(&mut b, PhysOp::Block, &[]);
		b.open(inner, RegionKind::Block, &[]);
		b.add_phys(PhysOp::Yield, &[called]);
		b.close();
		let value = b.result_of(inner);
		b.add_phys(PhysOp::Yield, &[value]);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		let program = lowered(&mut b);
		let calls: Vec<usize> = code(&program)
			.iter()
			.enumerate()
			.filter_map(|(at, instr)| matches!(instr, Instr::Call { .. }).then_some(at))
			.collect();
		let saves: Vec<usize> = code(&program)
			.iter()
			.enumerate()
			.filter_map(|(at, instr)| matches!(instr, Instr::Block(BlockInstr::Save)).then_some(at))
			.collect();
		// The outer block saves its scope before its first statement; the nested block's save,
		// the last, follows the call.
		assert_eq!(calls.len(), 1);
		assert!(saves.last().is_some_and(|save| calls[0] < *save), "{saves:?} {calls:?}");
	}

	#[test]
	fn a_for_body_that_reads_its_position_is_an_invariant_break() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let range = b.add(ExprOp::Array, &[one, two]);
		let looped = statement(&mut b, for_op(), &[range]);
		b.open_with(
			looped,
			RegionKind::StatementFor,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| {
				b.add_phys(PhysOp::Eval, &[args[1]]);
				b.add_phys(PhysOp::Yield, &[]);
			},
		);
		assert!(matches!(broken(&mut b), IrError::Invariant { .. }));
	}

	// -----------------------------------------------------------------------------------------
	// Statements that run as groups
	// -----------------------------------------------------------------------------------------

	use smallvec::SmallVec;
	use surrealdb_strand::TableName;

	use crate::exec::ir::iface::StatementCommit;
	use crate::exec::ir::phys::ops::{PipelineAttrs, PipelineId, TableScanAttrs};
	use crate::exec::ir::phys::placement::{NO_BACKEND_CAPS, Placement, install, place_all};
	use crate::exec::planner::CatalogWithin;
	use crate::kvs::Direction;

	/// The `phys.pipeline` of id `id` over a scan of the table `t`, in the open region.
	fn scan_pipeline(b: &mut Builder, id: u32) -> OpId {
		let op = b.create_kind(
			OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
				id: PipelineId(id),
				edges: SmallVec::new(),
				logical: None,
			}))),
			&[],
			1,
		);
		b.open(op, RegionKind::Pipeline, &[]);
		let scan = b.create_kind(
			OpKind::Phys(PhysOp::TableScan(Box::new(TableScanAttrs {
				probes: None,
				bounds: None,
				table: TableName::from("t"),
				direction: Direction::Forward,
				topk: None,
				lightweight: false,
				id_order: false,
				prefilter: None,
				read: Default::default(),
			}))),
			&[],
			1,
		);
		let rows = b.result_of(scan);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[rows], 0);
		b.close();
		op
	}

	fn statement_op(changes_catalog: bool) -> PhysOp {
		PhysOp::Statement {
			changes_catalog,
			served: None,
			within: CatalogWithin::default(),
			raises_when_reached: false,
		}
	}

	/// A `phys.statement` in the open region whose own region holds the pipeline of a scan with
	/// the id `id` and yields its rows. Answers the statement.
	fn read_statement(b: &mut Builder, id: u32, changes_catalog: bool) -> OpId {
		let op = b.add_phys_results(statement_op(changes_catalog), &[], 1);
		b.open(op, RegionKind::Statement(StatementCommit::InTxn), &[TypeKnowledge::Unknown]);
		let pipeline = scan_pipeline(b, id);
		let rows = b.result_of(pipeline);
		b.yield_phys(rows);
		b.close();
		op
	}

	/// Places the pipelines of `b` serially and lowers it.
	fn lowered_placed(mut b: Builder) -> Program {
		b.physical();
		let mut facts = FactCache::new();
		let decided = place_all(
			&b.ir,
			&mut facts,
			crate::exec::ir::analysis::cx::builtin_registry(),
			&Placement::default(),
			NO_BACKEND_CAPS,
		)
		.expect("the container is a DAG");
		install(b.ir.placement_table_mut(), decided);
		match lower(&b.ir, &FactCache::new()) {
			Ok(lowered) => lowered.program,
			Err(error) => panic!("the container lowers: {error}"),
		}
	}

	/// `{ <statements> }` yielding the value of the last statement `statements` builds.
	fn block_of_statements(statements: impl FnOnce(&mut Builder) -> OpId) -> Builder {
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let last = statements(&mut b);
		let value = b.result_of(last);
		b.yield_phys(value);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		b
	}

	fn runs(program: &Program) -> Vec<&BlockInstr> {
		blocks(program)
			.into_iter()
			.filter(|block| matches!(block, BlockInstr::RunEmbedded { .. }))
			.collect()
	}

	#[test]
	fn a_statement_of_a_list_is_a_group_of_the_program() {
		let program = lowered_placed(block_of_statements(|b| read_statement(b, 0, false)));
		// The group's pipeline is the program's first, stamped with the group, and the list's own
		// pipeline is the last, stamped with none.
		assert_eq!(program.pipelines.len(), 2);
		assert_eq!(program.pipelines[0].group, Some(0));
		assert_eq!(program.pipelines[1].group, None);
		assert_eq!(program.groups.len(), 1);
		assert!(!program.groups[0].only);
		assert!(!program.groups[0].scalar);
		assert!(program.groups[0].timeout.is_none() && program.groups[0].version.is_none());
		assert_eq!(program.required_context, ContextLevel::Database);
		// A `RETURN` the statement signals leaves the program: a block is no context.
		match runs(&program).as_slice() {
			[
				BlockInstr::RunEmbedded {
					statement: 0,
					collect: Collect::Block,
					on_break: None,
					on_continue: None,
					resolve: None,
					..
				},
			] => {}
			other => panic!("one run of the first group, found {other:?}"),
		}
	}

	#[test]
	fn the_pools_of_a_group_start_where_the_groups_before_it_left_them() {
		let program = lowered_placed(block_of_statements(|b| {
			read_statement(b, 0, false);
			read_statement(b, 1, false)
		}));
		assert_eq!(program.groups.len(), 2);
		let stamps: Vec<_> = program.pipelines.iter().map(|pipeline| pipeline.group).collect();
		assert_eq!(stamps, vec![Some(0), Some(1), None]);
		// Neither group is a sort, and the second's permission frames follow the first's.
		assert_eq!(program.groups[0].sorts, 0..0);
		assert_eq!(program.groups[1].sorts, 0..0);
		assert_eq!(program.groups[0].permissions.end, program.groups[1].permissions.start);
		let statements: Vec<u16> = runs(&program)
			.into_iter()
			.filter_map(|run| {
				if let BlockInstr::RunEmbedded {
					statement,
					..
				} = run
				{
					Some(*statement)
				} else {
					None
				}
			})
			.collect();
		assert_eq!(statements, vec![0, 1]);
	}

	#[test]
	fn a_let_gathers_the_value_of_a_statement_as_a_let_does() {
		let program = lowered_placed(block_of_statements(|b| {
			let rows = read_statement(b, 0, false);
			let value = b.result_of(rows);
			let bound = statement(b, let_attrs("rows", None), &[value]);
			let rows = b.result_of(bound);
			b.add_phys_results(PhysOp::Eval, &[rows], 1)
		}));
		assert!(matches!(
			runs(&program).as_slice(),
			[BlockInstr::RunEmbedded {
				collect: Collect::Let,
				..
			}]
		));
	}

	#[test]
	fn a_return_gathers_the_value_of_a_statement_as_a_return_does() {
		let mut b = Builder::new();
		let rows = read_statement(&mut b, 0, false);
		let value = b.result_of(rows);
		b.add_phys(PhysOp::Return, &[value]);
		let program = lowered_placed(b);
		assert!(program.rows);
		assert!(matches!(
			runs(&program).as_slice(),
			[BlockInstr::RunEmbedded {
				collect: Collect::Return,
				..
			}]
		));
	}

	#[test]
	fn a_statement_after_one_that_changes_the_catalog_resolves_the_session_again() {
		let program = lowered_placed(block_of_statements(|b| {
			read_statement(b, 0, true);
			read_statement(b, 1, false)
		}));
		let order: Vec<(&'static str, Option<ContextLevel>)> = blocks(&program)
			.into_iter()
			.filter_map(|block| match block {
				BlockInstr::RunEmbedded {
					resolve,
					..
				} => Some(("run", *resolve)),
				BlockInstr::Resolve => Some(("resolve", None)),
				BlockInstr::Bind {
					..
				}
				| BlockInstr::CoerceKind {
					..
				}
				| BlockInstr::CheckDepth {
					..
				}
				| BlockInstr::Enter
				| BlockInstr::Save
				| BlockInstr::Restore
				| BlockInstr::Clock
				| BlockInstr::Return {
					..
				}
				| BlockInstr::IterStart {
					..
				}
				| BlockInstr::IterNext {
					..
				}
				| BlockInstr::Sleep {
					..
				}
				| BlockInstr::Nested {
					..
				}
				| BlockInstr::Member {
					..
				}
				| BlockInstr::RunServed {
					..
				}
				| BlockInstr::RunAnalyzed {
					..
				} => None,
			})
			.collect();
		assert_eq!(
			order,
			vec![("run", None), ("resolve", None), ("run", Some(ContextLevel::Database))]
		);
	}

	#[test]
	fn a_statement_in_a_for_body_is_dropped_and_hands_its_signals_to_the_loop() {
		let program = lowered_placed(for_over(|b, _| {
			read_statement(b, 0, false);
			b.add_phys(PhysOp::Yield, &[]);
		}));
		let run = runs(&program);
		let [
			BlockInstr::RunEmbedded {
				collect: Collect::Discard,
				on_break: Some(_),
				on_continue: Some(_),
				..
			},
		] = run.as_slice()
		else {
			panic!("one run that discards its value and signals to the loop, found {run:?}");
		};
		// The pads that take the signals restore the frames saved since the loop's iteration began
		// and are jumped over by the statement's own code.
		let code = code(&program);
		let at = code
			.iter()
			.position(|instr| matches!(instr, Instr::Block(BlockInstr::RunEmbedded { .. })))
			.expect("the statement runs");
		assert!(matches!(code[at + 1], Instr::Jump { .. }));
	}

	fn store_names(program: &Program, group: Option<u16>) -> Vec<String> {
		program
			.pipelines
			.iter()
			.filter(|pipeline| pipeline.group == group)
			.flat_map(|pipeline| pipeline.code.iter())
			.filter_map(|instr| {
				if let Instr::Store(store) = instr {
					Some(
						format!("{store:?}").split([' ', '{', '(']).next().unwrap_or("").to_owned(),
					)
				} else {
					None
				}
			})
			.collect()
	}

	/// A `phys.statement` in the open region that holds the pipeline of a scan with the id `id`
	/// and the `EXPLAIN` of it, which yields the explanation. Answers the statement.
	fn explain_statement(b: &mut Builder, id: u32, analyze: bool) -> OpId {
		let op = b.add_phys_results(statement_op(false), &[], 1);
		b.open(op, RegionKind::Statement(StatementCommit::InTxn), &[TypeKnowledge::Unknown]);
		let pipeline = scan_pipeline(b, id);
		let rows = b.result_of(pipeline);
		let explain = b.add_phys_results(
			PhysOp::Explain {
				analyze,
				format: crate::expr::ExplainFormat::Text,
			},
			&[rows],
			1,
		);
		let explained = b.result_of(explain);
		b.yield_phys(explained);
		b.close();
		op
	}

	#[test]
	fn a_plain_explain_of_a_list_is_a_group_that_loads_the_rendering() {
		let program = lowered_placed(block_of_statements(|b| explain_statement(b, 0, false)));
		// The scan is not lowered: the group is the one pipeline that answers the rendering.
		assert_eq!(program.pipelines.len(), 2);
		assert_eq!(program.pipelines[0].group, Some(0));
		assert!(matches!(program.pipelines[0].source, Source::Once));
		assert!(program.groups[0].scalar && program.groups[0].rowless);
		assert!(program.analyses.is_empty());
		assert!(program.pipelines[0].code.iter().any(|instr| matches!(instr, Instr::Load { .. })));
		assert!(matches!(
			runs(&program).as_slice(),
			[BlockInstr::RunEmbedded {
				statement: 0,
				..
			}]
		));
	}

	#[test]
	fn an_analysed_explain_of_a_list_runs_the_statement_and_renders_its_analysis() {
		for redact in [false, true] {
			let mut b = block_of_statements(|b| explain_statement(b, 0, true));
			b.physical();
			let mut facts = FactCache::new();
			let decided = place_all(
				&b.ir,
				&mut facts,
				crate::exec::ir::analysis::cx::builtin_registry(),
				&Placement::default(),
				NO_BACKEND_CAPS,
			)
			.expect("the container is a DAG");
			install(b.ir.placement_table_mut(), decided);
			let mut cx =
				crate::exec::vm::ir_lower::LowerCx::detached().with_redact_volatile(redact);
			let program = match crate::exec::vm::ir_lower::dispatch::lower_with(
				&mut cx,
				&b.ir,
				&FactCache::new(),
			) {
				Ok(lowered) => lowered.program,
				Err(error) => panic!("the container lowers: {error}"),
			};
			// The statement the explain holds is the group, the pipeline the scan; its analysis is
			// the program's.
			assert_eq!(program.groups.len(), 1);
			assert_eq!(program.pipelines[0].group, Some(0));
			assert!(
				matches!(program.pipelines[0].source, Source::Scan { .. }),
				"{:?}",
				program.pipelines[0].source
			);
			assert_eq!(program.analyses.len(), 1);
			assert_eq!(program.analyses[0].redact, redact);
			assert!(blocks(&program).into_iter().any(|block| matches!(
				block,
				BlockInstr::RunAnalyzed {
					statement: 0,
					analysis: 0,
					..
				}
			)));
			assert!(runs(&program).is_empty());
		}
	}

	#[test]
	fn an_analysed_explain_at_the_top_stays_refused() {
		let mut b = Builder::new();
		let pipeline = scan_pipeline(&mut b, 0);
		let rows = b.result_of(pipeline);
		b.add_phys_results(
			PhysOp::Explain {
				analyze: true,
				format: crate::expr::ExplainFormat::Text,
			},
			&[rows],
			1,
		);
		b.physical();
		assert!(lower(&b.ir, &FactCache::new()).is_err());
	}

	#[test]
	fn a_refused_schema_statement_runs_its_guards_and_then_raises() {
		let refusal = crate::exec::vm::payload::Refusal::NamespaceEmpty;
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let refused = b.add_phys_results(statement_op(false), &[], 1);
		b.open(refused, RegionKind::Statement(StatementCommit::InTxn), &[TypeKnowledge::Unknown]);
		b.add_phys(
			PhysOp::DdlGuard {
				guard: crate::exec::ir::iface::statements::DdlGuard::RefuseNoWrite,
			},
			&[],
		);
		b.terminate(ExprOp::Raise(Box::new(RaiseKind::Refused(Box::new(refusal)))), &[]);
		b.close();
		let value = b.result_of(refused);
		b.yield_phys(value);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		let program = lowered(&mut b);
		assert!(program.groups.is_empty());
		let stores: Vec<&StoreInstr> = code(&program)
			.iter()
			.filter_map(|instr| {
				if let Instr::Store(store) = instr {
					Some(store)
				} else {
					None
				}
			})
			.collect();
		assert!(matches!(
			stores.as_slice(),
			[StoreInstr::RefuseNoWrite, StoreInstr::Refuse { .. }]
		));
	}

	#[test]
	fn a_statement_that_raises_when_reached_ends_the_list() {
		let refusal = crate::exec::vm::payload::Refusal::NamespaceEmpty;
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let refused = b.add_phys_results(statement_op(false), &[], 1);
		b.open(refused, RegionKind::Statement(StatementCommit::InTxn), &[TypeKnowledge::Unknown]);
		b.terminate(ExprOp::Raise(Box::new(RaiseKind::Refused(Box::new(refusal)))), &[]);
		b.close();
		let value = b.result_of(refused);
		b.yield_phys(value);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		let program = lowered(&mut b);
		assert!(program.groups.is_empty());
		assert!(
			code(&program)
				.iter()
				.any(|instr| matches!(instr, Instr::Store(StoreInstr::Refuse { .. })))
		);
	}

	/// A late-bound `phys.statement` in the open region that names the written statement `id` and
	/// holds the scan with the id `id`.
	fn late_read_statement(b: &mut Builder, id: u32) -> OpId {
		let op = b.add_phys_results(
			PhysOp::Statement {
				changes_catalog: false,
				served: Some(id),
				within: CatalogWithin::default(),
				raises_when_reached: false,
			},
			&[],
			1,
		);
		b.open(op, RegionKind::Statement(StatementCommit::InTxn), &[TypeKnowledge::Unknown]);
		let pipeline = scan_pipeline(b, id);
		let rows = b.result_of(pipeline);
		b.yield_phys(rows);
		b.close();
		op
	}

	/// Places the pipelines of `b` serially and lowers it as a program that serves late-bound
	/// statements, answering the program and the units it asked for.
	fn serving_placed(mut b: Builder) -> (Program, Vec<UnitRequest>) {
		b.physical();
		let mut facts = FactCache::new();
		let decided = place_all(
			&b.ir,
			&mut facts,
			crate::exec::ir::analysis::cx::builtin_registry(),
			&Placement::default(),
			NO_BACKEND_CAPS,
		)
		.expect("the container is a DAG");
		install(b.ir.placement_table_mut(), decided);
		let mut cx = crate::exec::vm::ir_lower::LowerCx::detached();
		match crate::exec::vm::ir_lower::lower_serving(&mut cx, &b.ir, &FactCache::new()) {
			Ok(lowered) => {
				let needs = lowered.needs.into_iter().map(|needed| needed.request).collect();
				(lowered.program, needs)
			}
			Err(error) => panic!("the container lowers: {error:?}"),
		}
	}

	#[test]
	fn a_late_bound_statement_of_a_program_that_serves_none_is_refused() {
		let mut b = block_of_statements(|b| late_read_statement(b, 0));
		b.physical();
		let mut facts = FactCache::new();
		let decided = place_all(
			&b.ir,
			&mut facts,
			crate::exec::ir::analysis::cx::builtin_registry(),
			&Placement::default(),
			NO_BACKEND_CAPS,
		)
		.expect("the container is a DAG");
		install(b.ir.placement_table_mut(), decided);
		assert!(lower(&b.ir, &FactCache::new()).is_err());
	}

	#[test]
	fn a_late_bound_statement_of_a_serving_program_is_planned_when_the_list_reaches_it() {
		let (program, needs) = serving_placed(block_of_statements(|b| {
			let one = b.literal(Value::from(1_i64));
			statement(b, let_attrs("a", None), &[one]);
			late_read_statement(b, 3)
		}));
		// The region stands for nothing: the compile driver answers the statement as written, and
		// the run plans it for the catalog it reads when it is reached.
		assert!(program.groups.is_empty());
		assert_eq!(
			program.served,
			vec![ServedSpec {
				shadowed: vec!["a".to_owned()],
				depth: 0,
			}]
		);
		assert_eq!(
			needs,
			vec![UnitRequest::ServedStatement {
				position: 0,
				written: 3,
			}]
		);
		assert!(runs(&program).is_empty());
		assert!(blocks(&program).into_iter().any(|block| matches!(
			block,
			BlockInstr::RunServed {
				served: 0,
				collect: Collect::Block,
				resolve: Some(ContextLevel::Root),
				..
			}
		)));
		// The names the list binds are read through the frame by what the run plans.
		assert_eq!(binds(&program), vec![(0, true)]);
	}

	#[test]
	fn late_bound_statements_are_served_in_list_order_by_the_statement_each_names() {
		let (program, needs) = serving_placed(block_of_statements(|b| {
			late_read_statement(b, 1);
			late_read_statement(b, 0)
		}));
		assert_eq!(program.served.len(), 2);
		assert_eq!(
			needs,
			vec![
				UnitRequest::ServedStatement {
					position: 0,
					written: 1,
				},
				UnitRequest::ServedStatement {
					position: 1,
					written: 0,
				},
			]
		);
	}

	#[test]
	fn a_group_reads_a_name_the_list_binds_through_the_frame_too() {
		// `{ LET $a = 1; SELECT * FROM t }`: the group's code may read `$a` by name, so the `LET`
		// binds it in the frame as well as in its slot.
		let program = lowered_placed(block_of_statements(|b| {
			let one = b.literal(Value::from(1_i64));
			statement(b, let_attrs("a", None), &[one]);
			read_statement(b, 0, false)
		}));
		assert_eq!(binds(&program), vec![(0, true)]);
	}

	#[test]
	fn a_for_the_physical_phase_wrapped_in_a_pipeline_runs_in_place() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let range = b.add(ExprOp::Array, &[one, two]);
		let wrapper = b.create_kind(
			OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
				id: PipelineId(0),
				edges: SmallVec::new(),
				logical: None,
			}))),
			&[],
			1,
		);
		b.open(wrapper, RegionKind::Pipeline, &[]);
		let looped = statement(&mut b, for_op(), &[range]);
		b.open(looped, RegionKind::StatementFor, &[TypeKnowledge::Unknown, TypeKnowledge::Unknown]);
		b.add_phys(PhysOp::Yield, &[]);
		b.close();
		let value = b.result_of(looped);
		b.yield_phys(value);
		b.close();
		let value = b.result_of(wrapper);
		b.add_phys(PhysOp::Yield, &[value]);
		let program = lowered(&mut b);
		// The wrapper is no read statement: its loop is the program's own, and no pipeline of a
		// group stands in front of the program's.
		assert_eq!(program.pipelines.len(), 1);
		assert_eq!(count(&program, |block| matches!(block, BlockInstr::IterStart { .. })), 1);
	}

	/// An `INFO FOR DB` in the open region, which answers its object.
	fn info_statement_op(b: &mut Builder) -> OpId {
		b.add_phys_results(
			PhysOp::Info {
				subject: crate::exec::ir::iface::statements::InfoSubject::Db,
				structure: false,
			},
			&[],
			1,
		)
	}

	#[test]
	fn an_info_of_a_list_is_a_group_of_one_scalar_pipeline_that_writes_nothing() {
		let program = lowered(&mut block_of_statements(info_statement_op));
		assert_eq!(program.groups.len(), 1);
		let spec = &program.groups[0];
		assert!(spec.write.is_none());
		assert!(spec.scalar && spec.rowless);
		assert_eq!(program.pipelines.len(), 2);
		assert_eq!(program.pipelines[0].group, Some(0));
		assert!(
			program.pipelines[0].code.iter().any(|instr| matches!(instr, Instr::Store(_))),
			"the group's pipeline reads the catalog"
		);
		assert!(matches!(
			runs(&program).as_slice(),
			[BlockInstr::RunEmbedded {
				statement: 0,
				resolve: None,
				..
			}]
		));
	}

	#[test]
	fn a_value_loop_the_physical_phase_wrapped_in_a_pipeline_lowers_where_its_wrapper_stands() {
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let xs = b.add(
			ExprOp::FreeParam {
				name: "xs".into(),
			},
			&[],
		);
		let wrapper = b.create_kind(
			OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
				id: PipelineId(0),
				edges: SmallVec::new(),
				logical: None,
			}))),
			&[],
			1,
		);
		b.open(wrapper, RegionKind::Pipeline, &[]);
		let mut filter = for_op();
		if let PhysOp::Loop {
			kind,
			..
		} = &mut filter
		{
			*kind = LoopKind::ElementFilter;
		}
		let looped = b.add_phys_value(filter, &[xs]);
		b.open_with(
			looped,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| {
				let truth = b.add(ExprOp::Truthy, &[args[0]]);
				b.yield_phys(truth);
			},
		);
		let filtered = b.result_of(looped);
		b.yield_phys(filtered);
		b.close();
		let wrapped = b.result_of(wrapper);
		let bound = statement(&mut b, let_attrs("a", None), &[wrapped]);
		let a = b.result_of(bound);
		b.add_phys(PhysOp::Yield, &[a]);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		let program = lowered(&mut b);
		assert!(code(&program).iter().any(|instr| matches!(instr, Instr::FilterBegin { .. })));
	}

	#[test]
	fn a_pipeline_of_a_list_that_holds_no_loop_is_an_invariant_break() {
		let mut b = Builder::new();
		let block = statement(&mut b, PhysOp::Block, &[]);
		b.open(block, RegionKind::Block, &[]);
		let wrapper = b.create_kind(
			OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
				id: PipelineId(0),
				edges: SmallVec::new(),
				logical: None,
			}))),
			&[],
			1,
		);
		b.open(wrapper, RegionKind::Pipeline, &[]);
		let one = b.literal(Value::from(1_i64));
		b.yield_phys(one);
		b.close();
		let wrapped = b.result_of(wrapper);
		b.add_phys(PhysOp::Yield, &[wrapped]);
		b.close();
		let value = b.result_of(block);
		b.add_phys(PhysOp::Yield, &[value]);
		assert!(matches!(broken(&mut b), IrError::Invariant { .. }));
	}
}
