//! Lowering of the `expr` dialect's operand subset.
//!
//! Each op lowers to the instruction the VM defines for its operator, with the payload the op's
//! attributes fix: the same operator emits the same instruction with the same payload. This file
//! holds no optimisation, no common-subexpression elimination, no constant folding and no
//! reordering of operands the VM evaluates in order.
//!
//! # Values
//!
//! A [`Lowerer`] lowers the op that defines a value the first time the value is read, and the
//! operands of an op in the order the op names them, each inside the arm of its op. An SSA value
//! is lowered once per scope: a second read of it is the operand the first read answered.
//!
//! A region of a lazy operator (the right side of a short-circuit operator, an arm of an `if`) is
//! a scope of its own, because the instructions that computed what it lowered run on one path
//! only. A value the region reads from outside is one an earlier read of the enclosing scope has
//! already lowered. A region that would be the first to read an outer value answers `NotYet`: the
//! value would be skipped when the region is skipped and evaluated again by a later read outside
//! it. A literal emits no instruction and is exempt. The ops of the subset share no value across a
//! lazy boundary when the container is built from an expression tree, so this refuses nothing
//! such a container holds.
//!
//! # Block arguments
//!
//! A block argument is a value no op defines, so the caller that owns the region names where it is
//! read from before lowering the region: [`Lowerer::bind_arg`] gives it an operand,
//! [`Lowerer::bind_row`] makes it the row of the pipeline being lowered, and the region of a lazy
//! op takes its arguments from the op that owns it. An argument nothing bound answers `NotYet`.
//!
//! A row is read as a value of its own (`LoadRow`), and a field of an object row is an operand of
//! the row itself ([`Operand::Field`]) when [`Lowerer::bind_row`] says the rows are objects. Over
//! any other value a field is a path step ([`Instr::Access`]), which the host answers.
//!
//! # Regions that end in an error
//!
//! A region ends in `expr.yield` of its value (`phys.yield` in the body of a loop), or in a
//! terminator that raises (`expr.throw`, or `expr.raise` of a loop control with no loop around
//! it). The region then yields no value: the
//! instruction that raises is emitted and a register nothing reads stands for the value, which the
//! code after the raise never reads.
//!
//! # Fixed payloads
//!
//! Every instruction is emitted in the authority frame the lowerer is in: the program's own
//! (`frame: None`) until [`Lowerer::in_frame`] enters the permission frame of the op that owns
//! the region being lowered. An op that runs under any other frame answers `NotYet`. A call is
//! entered at plan depth 0.
//!
//! A stored parameter read (`expr.free_param`, `expr.session_param`, `expr.catalog_param`) is a
//! `LoadParam` with no guard: the guard (the clause hash, the host-code pipeline and the verdict
//! scope) is catalog-derived and planned ahead of the program, which this lowering does not hold.
//! The VM judges such a read through the parameter unit it loads by name.
//!
//! What lowering reads of the request's configuration is [`LowerLimits`].

#![deny(clippy::wildcard_enum_match_arm)]

use std::ops::Bound;
use std::sync::Arc;

use surrealdb_strand::Strand;
use surrealdb_types::ToSql;

use super::super::dispatch::Reach;
use super::super::plain::ValueData;
use super::super::program::{
	ArithOp, CallSpec, Catch, CompareOp, ExternalCallee, ExternalSpec, FrameMode, GroupSpec,
	HostScalar, Instr, KeyShape as RecordKey, Label, MatchesSpec, MethodSpec, NavOp, Operand,
	ParamSpec, Pipeline, RecordIdSpec, Reg, ReturnCatch, Source, StoreInstr, UnaryOp, UnitFrame,
	UnitId, is_permission_frame, unit_input_name,
};
use super::block::Groups;
use super::closure::{wrapped_value_loop, wraps_value_for};
use super::ddl::{Reservations, schema_op};
use super::emit::{BoundSlot, Emitter};
use super::explain_hook::Explained;
use super::info::InfoLowered;
use super::pipe::{PoolBase, ReadScope, Reads};
use super::stage::StatementNesting;
use super::units::{LowerCx, UnitRequest, UnitSink};
use super::write_stmt::LoweredWrite;
use crate::exec::config::ExecConfig;
use crate::exec::field_path::FieldPath;
use crate::exec::function::FunctionRegistry;
#[cfg(test)]
use crate::exec::ir::analysis::cx::builtin_registry;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashMap;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ControlTarget, ancestors, control_target, ops_in};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{
	CallAttrs, ConvertMode, DispatchTarget, ExprOp, KeyBound, KeyShape, NowWhich, OnFail,
	RaiseKind, ShortCircuitOp,
};
use crate::exec::ir::iface::ids::{FrameId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{RegionProperties, ReturnRule};
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::ir::phys::matches::{QueryOperand, query_operand};
use crate::exec::ir::phys::ops::{PhysOp, PhysTag};
use crate::exec::vm::matches::MatchProbe;
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::logical::{LogicalExpr, LogicalLiteral, LogicalPostfixOperator, Schema};
use crate::expr::operator::MatchesOperator;
use crate::expr::{BinaryOperator, Function, PrefixOperator};
use crate::val::{RecordId, RecordIdKey, Value};

/// Which ops a [`Lowerer`] lowers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Subset {
	/// The operand subset: every op the lowering has an arm for.
	Operand,
	/// The subset of the plan-time folder: the operand subset without the ops that read the
	/// request's bindings or the statement's clock.
	Fold,
}

/// What lowering reads of the request's configuration.
///
/// The limits are values of the configuration the request runs under, copied into the
/// instructions that enforce them, so an instruction holds the limit of the request that lowered
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LowerLimits {
	/// The most bytes of record ids one `expr.mock` may generate before the read is refused.
	pub(crate) generation_allocation_limit: usize,
	/// The deepest bound a recursion may state before the statement is refused.
	pub(crate) idiom_recursion_limit: u32,
}

impl LowerLimits {
	/// The limits `config` sets.
	pub(crate) fn of(config: &ExecConfig) -> Self {
		Self {
			generation_allocation_limit: config.generation_allocation_limit,
			idiom_recursion_limit: config.idiom_recursion_limit,
		}
	}
}

impl Default for LowerLimits {
	/// The limits of the default configuration.
	fn default() -> Self {
		Self::of(&ExecConfig::default())
	}
}

/// What the program a [`Lowerer`] emitted into needs of its run, beyond its instructions.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Needs {
	/// The most demanding context level of any builtin called.
	pub(super) context: ContextLevel,
	/// Whether something lowered keeps the rows of a statement running one after another: any
	/// call of a builtin or a method. Lowering does not read what a callee does, so it answers
	/// for every call, which only keeps rows serial.
	pub(super) keeps_row_order: bool,
	/// The tables the instructions lowered may read through a value they are given, which the
	/// program holds as its reach. `None` when nothing lowered reads one.
	pub(super) reach: Option<Reach>,
	/// What the instructions lowered may do to the transaction: a write where one calls a builtin
	/// that runs statements or invokes a closure whose body the program does not hold.
	pub(super) access_mode: AccessMode,
}

impl Needs {
	/// What nothing lowered needs.
	pub(super) const fn none() -> Needs {
		Needs {
			context: ContextLevel::Root,
			keeps_row_order: false,
			reach: None,
			access_mode: AccessMode::ReadOnly,
		}
	}

	/// What `self` and `other` need between them.
	pub(super) fn merged(&self, other: &Needs) -> Needs {
		Needs {
			context: self.context.max(other.context),
			keeps_row_order: self.keeps_row_order || other.keeps_row_order,
			reach: match (&self.reach, &other.reach) {
				(Some(held), Some(more)) => Some(held.clone().union(more.clone())),
				(held, more) => held.clone().or_else(|| more.clone()),
			},
			access_mode: self.access_mode.combine(other.access_mode),
		}
	}
}

/// What a lowered `expr.dispatch` leaves for the `expr.method` that reads it.
#[derive(Debug, Clone, Copy)]
pub(super) struct Resolved {
	/// The index of the call site in the method pool.
	pub(super) method: u32,
	/// Where the receiver is read from.
	pub(super) recv: Operand,
	/// The position a receiver that holds a closure under the method's name continues at. Absent
	/// for a method whose implementations leave no receiver for a closure to answer.
	pub(super) closure: Option<Label>,
}

/// What a lowered `expr.dispatch` of a method leaves for the `expr.method` that reads it.
#[derive(Debug, Clone, Copy)]
pub(super) enum Dispatched {
	/// A method a builtin names.
	Builtin(Resolved),
	/// A method no builtin names: the closure the receiver in `recv` holds under the method's
	/// name, read into `callee` ([`Instr::ClosureField`]).
	Field {
		recv: Operand,
		callee: Reg,
	},
}

/// The values lowered so far on the path being lowered.
#[derive(Debug, Clone, Default)]
pub(super) struct Scope {
	values: FxHashMap<ValueId, Operand>,
	resolved: FxHashMap<ValueId, Dispatched>,
}

/// The state of a [`Lowerer`] set aside while a pipeline is built beside the one it lowers.
pub(super) struct Beside {
	program_frame: Option<u16>,
	aside: bool,
	scope: Scope,
	rows: FxHashMap<ValueId, bool>,
	claims: Vec<Claim>,
	region: Option<RegionId>,
	catch: Option<ReturnScope>,
	loop_pads: Option<LoopPads>,
	statement_doc: Option<Reg>,
	element: Option<Reg>,
	each_guard: Option<(ValueId, Reg)>,
	closure_root: Option<ValueId>,
}

/// The labels a `BREAK` and a `CONTINUE` that a statement held in an expression signals jump to,
/// where the expression stands in a statement list of the program's own code inside a `FOR`: the
/// pads the list emits after the statement being lowered, which restore the frames saved since the
/// loop's iteration began and jump to its exit and to its next element.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LoopPads {
	pub(super) on_break: Label,
	pub(super) on_continue: Label,
}

/// Where a `RETURN` that a statement held in an expression signals is taken, and what a `BREAK` or
/// `CONTINUE` it signals does.
///
/// A `RETURN` sets the value of the context it stands in and ends it
/// ([`RegionProperties::return_rule`]). Where the code being lowered evaluates that context, the
/// context is [`ReturnScope::context`]; elsewhere the signal propagates to the run that holds the
/// code, which takes it for the context it evaluates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReturnScope {
	/// Where the context being lowered takes a `RETURN`: the register its value is left in and the
	/// label past the rest of its evaluation. `None` where no context of the code being lowered
	/// encloses it.
	pub(super) context: Option<ReturnCatch>,
	/// The authority frame the context is evaluated in. A signal raised in a permission frame
	/// other than it belongs to the statement that owns that frame.
	pub(super) frame: Option<u16>,
	/// Whether a `BREAK` or `CONTINUE` signalled there is an error.
	pub(super) loops_fail: bool,
}

/// Whether `region` is a context a `RETURN` held in it ends: a `RETURN` in the region, or in a
/// region nested in it that passes the `RETURN` outward, lands on it.
pub(super) fn ends_context(ir: &Ir, region: RegionId) -> bool {
	if ir.region(region).is_none_or(|held| held.kind.return_rule() != ReturnRule::CatchesReturn) {
		return false;
	}
	let mut ends = false;
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		let lands = ir.op(op).is_some_and(|data| {
			matches!(data.kind.tag(), OpTag::Logical(LogTag::Return) | OpTag::Phys(PhysTag::Return))
		}) && control_target(ir, op) == ControlTarget::Region(region);
		if lands {
			ends = true;
			return Walk::Stop;
		}
		Walk::Continue
	});
	ends
}

/// The region the op that defines `value` stands in, or the region whose block argument it is.
pub(super) fn defining_region(ir: &Ir, value: ValueId) -> Option<RegionId> {
	match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => Some(ir.enclosing_region(op)),
		ValueDef::BlockArg {
			block,
			..
		} => ir.block(block).map(|held| held.parent),
	}
}

/// An instruction an op emitted for itself: the op and the instruction's index in the code of the
/// emitter at that time.
///
/// An op's own instructions exclude those of its operands and of the bodies of its regions, which
/// belong to the ops that emitted them.
pub(super) type Claim = (OpId, Label);

/// Lowers values of one container into one emitter.
pub(super) struct Lowerer<'a> {
	pub(super) em: &'a mut Emitter,
	pub(super) ir: &'a Ir,
	registry: &'a FunctionRegistry,
	pub(super) subset: Subset,
	limits: LowerLimits,
	/// The authority frame of the code being lowered.
	pub(super) frame: Frame,
	pub(super) scope: Scope,
	/// The block arguments that are the row of the pipeline being lowered, each with whether the
	/// rows are objects.
	rows: FxHashMap<ValueId, bool>,
	/// The region whose block is being lowered, inside a lazy operator's region. `None` outside
	/// every region the lowerer entered.
	pub(super) region: Option<RegionId>,
	/// The ops being lowered, innermost last: the op an instruction is emitted for.
	pub(super) owners: Vec<OpId>,
	claims: Vec<Claim>,
	context: ContextLevel,
	pub(super) keeps_row_order: bool,
	reach: Option<Reach>,
	/// What the catalog declares for the rows of the pipeline being lowered: the kinds a typed
	/// comparison of a field reads. `None` reads as an open schema.
	pub(super) schema: Option<Arc<Schema>>,
	access_mode: AccessMode,
	/// The element of the broadcast whose body is being lowered, where that body applies `.*` to
	/// the element directly, and the register that is true when the broadcast runs its body once
	/// over a lone value. A `.*` of that element is taken only over a record id while the
	/// broadcast walks a collection, and as written over a lone value. A pipeline built beside
	/// ([`Lowerer::beside`]) holds no guard, since the register is not one of its own.
	pub(super) each_guard: Option<(ValueId, Reg)>,
	/// The unit table of the compile the lowerer lowers for. A lowerer built without one answers
	/// `NotYet` for an op that runs a unit.
	units: Option<&'a mut dyn UnitSink>,
	/// Where each value a block binds is read from: a `LET`'s value and a `FOR`'s element.
	slots: FxHashMap<ValueId, BoundSlot>,
	/// The block arguments a unit's top region binds to names of the frame the unit runs in, each
	/// read by the name through the frame in every pipeline.
	named_args: FxHashMap<ValueId, Strand>,
	/// Whether the expression being lowered is a schema statement's evaluated value, whose method
	/// calls name their argument errors as code a write reaches does
	/// ([`MethodSpec::writing`](super::super::program::MethodSpec)).
	writing_labels: bool,
	/// Whether the pipeline being lowered is a closure body that reads the row it is invoked over.
	closure_row: bool,
	/// The block argument that is the document root of the frame a closure body is invoked over,
	/// where the body reads it apart from its row.
	closure_root: Option<ValueId>,
	/// Whether the pipeline being lowered is one built beside the program's own, which runs in a
	/// state of its own and reads a block's names through the frame.
	aside: bool,
	/// The names a pipeline built beside read through the frame, in the order they were read.
	named: Vec<Strand>,
	/// Where a `RETURN` signalled by a statement held in the expression being lowered is taken.
	/// `None` leaves the signal to propagate.
	catch: Option<ReturnScope>,
	/// Where a `BREAK` or `CONTINUE` signalled by a statement held in the expression being lowered
	/// jumps, when the expression stands in a statement list inside a `FOR`; `None` leaves the
	/// signal to propagate. Whether a statement took them is noted in `loop_pads_taken`.
	loop_pads: Option<LoopPads>,
	loop_pads_taken: std::cell::Cell<bool>,
	/// The register that holds the document a statement held in the expression is taken over as
	/// its `$this` and `$parent`, where the code being lowered sets one aside from the row.
	statement_doc: Option<Reg>,
	/// The register that holds the element of the element body being lowered: the value a call of
	/// code the host runs takes as its current value. `None` outside every element body, where the
	/// host hands such a call the row.
	element: Option<Reg>,
	/// The frame the units of the compile are lowered for, which a call of a user-defined function
	/// is lowered for as well. `None` for a lowerer built without a lowering context.
	pub(super) unit_frame: Option<UnitFrame>,
	/// The re-entry depth the units of the compile are lowered at.
	pub(super) depth: u32,
	/// The groups of the statement program this lowerer lowers the list of ([`super::block`]),
	/// held for it. `None` for a lowerer of any other code.
	block_groups: Option<Groups>,
	/// The reads of the pipelines of the read statement whose body this lowerer lowers, the
	/// pipeline in flight among them. `None` for a lowerer of any other code.
	reads: Option<&'a mut Reads>,
}

/// Whether the lowering of the op that makes `used` reads that operand. A call reads none of the
/// row names the binder passes after its arguments but the first, which it compares with the
/// element of the element body it stands in or with the document root of the closure body it
/// stands in ([`CallAttrs::row_names`]).
fn lowered_use(ir: &Ir, used: &crate::exec::ir::core::value::Use) -> bool {
	let Some(data) = ir.op(used.user) else {
		return true;
	};
	let OpKind::Expr(ExprOp::Call(call)) = &data.kind else {
		return true;
	};
	let first_row_name = data.operands.len().saturating_sub(usize::from(call.row_names));
	usize::from(used.operand) <= first_row_name
}

/// Where the value `v` of `ir` is read from once the instructions emitted into `em` have run.
pub(super) fn operand(
	em: &mut Emitter,
	ir: &Ir,
	registry: &FunctionRegistry,
	v: ValueId,
) -> Result<Operand, IrError> {
	Lowerer::new(em, ir, registry).value(v)
}

/// The arguments of the call `data`: its operands without the row names the binder passes after
/// them ([`CallAttrs::row_names`]).
pub(super) fn call_args<'d>(data: &'d OpData, call: &CallAttrs) -> Result<&'d [ValueId], IrError> {
	data.operands
		.len()
		.checked_sub(usize::from(call.row_names))
		.and_then(|len| data.operands.get(..len))
		.ok_or(IrError::Stale("ir_lower::expr: a call's row names"))
}

/// The error of a shape no producer builds: a state the planning protocol rules out, with a detail
/// that is a fixed text.
pub(super) fn invariant(what: &'static str) -> IrError {
	IrError::Invariant {
		what,
	}
}

/// Whether the key `shape` names is generated when the record id is built, whole or at a bound of
/// a range.
fn key_generates(shape: &KeyShape) -> bool {
	let bound_generates = |bound: &Bound<KeyBound>| match bound {
		Bound::Included(key) | Bound::Excluded(key) => matches!(key, KeyBound::Generate(_)),
		Bound::Unbounded => false,
	};
	match shape {
		KeyShape::Generate(_) => true,
		KeyShape::Range(range) => bound_generates(&range.start) || bound_generates(&range.end),
		KeyShape::Number(_)
		| KeyShape::String(_)
		| KeyShape::Uuid(_)
		| KeyShape::Array {
			..
		}
		| KeyShape::Object {
			..
		} => false,
	}
}

pub(super) fn not_yet<T>(what: &'static str) -> Result<T, IrError> {
	Err(IrError::NotYet {
		what,
	})
}

/// The `N` operands of `data`, which holds no other number of them.
pub(super) fn exactly<const N: usize>(data: &OpData) -> Result<[ValueId; N], IrError> {
	<[ValueId; N]>::try_from(data.operands.as_slice())
		.map_err(|_| IrError::Stale("ir_lower::expr: operand arity"))
}

/// The `at`th region of `data`, which holds `count` of them.
pub(super) fn region_at(data: &OpData, at: usize, count: usize) -> Result<RegionId, IrError> {
	if data.regions.len() != count {
		return Err(IrError::Stale("ir_lower::expr: region arity"));
	}
	data.regions.get(at).copied().ok_or(IrError::Stale("ir_lower::expr: region arity"))
}

/// The authority frame the code being lowered runs in.
#[derive(Clone, Copy, Debug)]
pub(super) struct Frame {
	/// The frame of the container that the ops of the code carry.
	pub(super) ir: FrameId,
	/// The permission frame of the program that the instructions name, when the code runs in one.
	pub(super) program: Option<u16>,
	/// Whether the code is a definer's, which holds none of the caller's own bindings.
	pub(super) privileged: bool,
}

impl Frame {
	/// The program's own frame: the caller's, in which nothing is privileged.
	const OWN: Frame = Frame {
		ir: FrameId::new(0),
		program: None,
		privileged: false,
	};
}

/// What a statement list's lowering ([`super::block`]) reads and writes of a [`Lowerer`]: the
/// emitter it shares, the values statements and block arguments define, the scopes it opens and
/// the instructions it emits for an op.
impl<'a> Lowerer<'a> {
	/// Makes the value `v` a read of slot `slot` of the running state, in every scope. `name` is
	/// the name the value is bound to in the frame, which a pipeline built beside the program's
	/// own reads it by.
	pub(super) fn bind_slot_value(&mut self, v: ValueId, slot: u16, name: Option<Strand>) {
		let bound = BoundSlot {
			slot,
			name,
		};
		self.em.bind_slot(v, bound.clone());
		self.slots.insert(v, bound);
	}

	/// Makes the method calls this lowerer lowers name their argument errors as code a write
	/// reaches does where `writing` holds: a schema statement's evaluated value.
	pub(super) fn label_as_writing(&mut self, writing: bool) {
		self.writing_labels = writing;
	}

	/// Makes the block argument `arg` the row a closure body is invoked over.
	pub(super) fn bind_closure_row(&mut self, arg: ValueId) {
		self.closure_row = true;
		self.bind_row(arg, false);
	}

	/// Makes the block argument `arg` the document root of the frame a closure body is invoked
	/// over: the row of that frame's evaluation, or the row where it has none.
	pub(super) fn bind_closure_root(&mut self, arg: ValueId) {
		self.closure_root = Some(arg);
	}

	/// Whether `value` is the document root of the frame the closure body being lowered is
	/// invoked over ([`Lowerer::bind_closure_root`]).
	pub(super) fn is_closure_root(&self, value: ValueId) -> bool {
		self.closure_root == Some(value)
	}

	/// Makes the block argument `arg` a read of the parameter `name` of the frame the program runs
	/// in, in every pipeline and scope.
	pub(super) fn bind_named_arg(&mut self, arg: ValueId, name: Strand) {
		self.named_args.insert(arg, name);
	}

	/// How many reads of a block's name by a pipeline built beside have been lowered.
	pub(super) fn named_reads(&self) -> usize {
		self.named.len()
	}

	/// Whether a pipeline built beside has read `name` through the frame since `since` reads had
	/// been lowered.
	pub(super) fn read_by_name_since(&self, since: usize, name: &str) -> bool {
		self.named.get(since..).unwrap_or(&[]).iter().any(|read| read.as_str() == name)
	}

	/// The scope as it stands, to be restored by [`Lowerer::restore_scope`] when a region whose
	/// code runs on one path only has been lowered.
	pub(super) fn scope(&self) -> Scope {
		self.scope.clone()
	}

	/// Returns to `scope`, forgetting the values lowered since it was taken.
	pub(super) fn restore_scope(&mut self, scope: Scope) {
		self.scope = scope;
	}

	/// Whether the reads of the principal the code is lowered for go unchecked by permissions in
	/// every frame ([`UnitSink::reads_checked`]), unlike [`Lowerer::reads_unchecked`], which is
	/// about the frame being lowered. Code lowered with no unit context is lowered for checked
	/// reads.
	pub(super) fn caller_reads_unchecked(&self) -> bool {
		self.units.as_deref().is_some_and(|units| !units.reads_checked())
	}

	/// Whether the value `v` has been lowered in the scope being lowered.
	pub(super) fn is_lowered(&self, v: ValueId) -> bool {
		self.scope.values.contains_key(&v)
	}

	/// Enters `region`: a value defined outside it that no read has lowered is lowered where the
	/// region reads it, into the scope being lowered, which a caller whose region runs on one path
	/// only takes and restores around it. Answers the region to give back to [`Lowerer::leave`].
	pub(super) fn enter(&mut self, region: RegionId) -> Option<RegionId> {
		self.region.replace(region)
	}

	/// Leaves the region [`Lowerer::enter`] entered.
	pub(super) fn leave(&mut self, outer: Option<RegionId>) {
		self.region = outer;
	}

	/// Lowers the read statement `region` holds as the group `group` of the program, numbering its
	/// pools from `base`, and answers what it left with the range of permission frames it filled.
	///
	/// The pipeline in flight is set aside while the group's pipelines are built, so none of its
	/// code becomes a group's. What the group needs of the program is merged into this lowerer's.
	/// A lowerer built without a lowering context answers [`IrError::Invariant`]: every lowerer of
	/// the operand subset is built in one ([`Lowerer::in_context`]), and the plan-time folder's
	/// subset refuses a statement before it is lowered.
	pub(super) fn lower_group(
		&mut self,
		region: RegionId,
		base: PoolBase,
		group: u16,
	) -> Result<(ReadScope, std::ops::Range<u16>), IrError> {
		let ir = self.ir;
		let Some(units) = self.units.as_deref_mut() else {
			return Err(invariant("a block statement lowered with no lowering context"));
		};
		// A read of the row of the pipeline being lowered is a read of `$parent` in the group.
		let parents = self.em.read_as_parent(self.rows.keys().copied().collect());
		let aside = self.em.set_aside();
		let mark = self.em.open_group(group);
		let lowered = units.lower_statement(&mut *self.em, ir, region, base);
		let closed = self.em.close_group(mark);
		self.em.restore_parent_rows(parents);
		self.em.restore(aside);
		let scope = lowered?;
		let permissions = closed?;
		self.absorb(&scope.reads.needs);
		if scope.reads.table.is_some() {
			self.requires(ContextLevel::Database);
		}
		Ok((scope, permissions))
	}

	/// Lowers the write whose op is `write`, of the statement `region` holds, as the group `group`
	/// of the program, numbering its pools from `base`, and answers what it lowered, the host
	/// scalar of its `TIMEOUT` duration `timeout` when it has one, and the range of permission
	/// frames it filled.
	///
	/// The pipeline in flight is set aside while the write's pipelines are built, so none of its
	/// code becomes the group's. What the write needs of the program is merged into this
	/// lowerer's: a transaction that writes, at least at the database level. Answers `NotYet` for
	/// a lowerer built without a lowering context.
	pub(super) fn lower_write_group(
		&mut self,
		write: OpId,
		region: RegionId,
		base: PoolBase,
		timeout: Option<ValueId>,
		group: u16,
	) -> Result<(LoweredWrite, Option<HostScalar>, std::ops::Range<u16>), IrError> {
		let ir = self.ir;
		let Some(units) = self.units.as_deref_mut() else {
			return not_yet("ir_lower::expr: a block statement with no lowering context");
		};
		// A read of the row of the pipeline being lowered is a read of `$parent` in the group.
		let parents = self.em.read_as_parent(self.rows.keys().copied().collect());
		let aside = self.em.set_aside();
		let mark = self.em.open_group(group);
		let lowered =
			units.lower_write(&mut *self.em, ir, write, region, base).and_then(|lowered| {
				match timeout {
					Some(value) => {
						let (scalar, needs) = units.host_scalar(&mut *self.em, ir, value)?;
						Ok((lowered, Some((scalar, needs))))
					}
					None => Ok((lowered, None)),
				}
			});
		let closed = self.em.close_group(mark);
		self.em.restore_parent_rows(parents);
		self.em.restore(aside);
		let (lowered, timed) = lowered?;
		let permissions = closed?;
		self.absorb(&lowered.needs);
		let timeout = timed.map(|(scalar, needs)| {
			self.absorb(&needs);
			scalar
		});
		self.requires(ContextLevel::Database);
		Ok((lowered, timeout, permissions))
	}

	/// Lowers the `EXPLAIN` the statement `region` holds, whose op is `explain`, as the group
	/// `group` of the program, numbering the pools of an analysed statement from `base` and
	/// building its tree for `purpose`, and answers what it lowered with the range of permission
	/// frames it filled.
	///
	/// The pipeline in flight is set aside while the group is built, and what the group needs of
	/// the program is merged into this lowerer's. A lowerer built without a lowering context
	/// answers [`IrError::Invariant`], as [`Lowerer::lower_group`] does.
	pub(super) fn lower_explain_group(
		&mut self,
		region: RegionId,
		explain: OpId,
		base: PoolBase,
		group: u16,
		purpose: super::explain_hook::Purpose,
	) -> Result<(Explained, std::ops::Range<u16>), IrError> {
		let ir = self.ir;
		let Some(units) = self.units.as_deref_mut() else {
			return Err(invariant("a block statement lowered with no lowering context"));
		};
		// A read of the row of the pipeline being lowered is a read of `$parent` in the group.
		let parents = self.em.read_as_parent(self.rows.keys().copied().collect());
		let aside = self.em.set_aside();
		let mark = self.em.open_group(group);
		let lowered = units.lower_explained(&mut *self.em, ir, region, explain, base, purpose);
		let closed = self.em.close_group(mark);
		self.em.restore_parent_rows(parents);
		self.em.restore(aside);
		let explained = lowered?;
		let permissions = closed?;
		match &explained {
			Explained::Analysed {
				scope,
				..
			} => {
				self.absorb(&scope.reads.needs);
				if scope.reads.table.is_some() {
					self.requires(ContextLevel::Database);
				}
			}
			Explained::AnalysedValue {
				needs,
				..
			} => self.absorb(needs),
			Explained::Plain(_) => {}
		}
		Ok((explained, permissions))
	}

	/// What `EXPLAIN ANALYZE` of the statement the `phys.explain` `explain` explains renders, its
	/// tree built for `purpose` and redacted as the lowering context redacts.
	pub(super) fn explain_analysis(
		&mut self,
		explain: OpId,
		purpose: super::explain_hook::Purpose,
	) -> Result<crate::exec::vm::program::Analysis, IrError> {
		let ir = self.ir;
		let Some(units) = self.units.as_deref_mut() else {
			return Err(invariant("a block statement lowered with no lowering context"));
		};
		units.explain_analysis(ir, explain, purpose)
	}

	/// Lowers the schema statement `region` holds as the group `group` of the program, and answers
	/// its pipeline, the range of permission frames it filled and what it needs of the program.
	///
	/// The pipeline in flight is set aside while the statement's pipeline is built, so none of its
	/// code becomes the group's. The pipeline reads one row from [`Source::Once`] and answers
	/// `NONE`; what the statement needs of the program is merged into this lowerer's, and the
	/// program is one that writes, which the caller records. `reservations` takes the draws of the
	/// statement. A region that holds no schema statement answers [`IrError::Invariant`]: the
	/// caller lowers a region as a schema group only where [`schema_op`] finds one.
	pub(super) fn lower_schema_group(
		&mut self,
		region: RegionId,
		group: u16,
		reservations: &mut Reservations,
	) -> Result<(Pipeline, std::ops::Range<u16>, Needs), IrError> {
		let ir = self.ir;
		let Some(op) = schema_op(ir, region)? else {
			return Err(invariant("a schema group over a region that holds no schema statement"));
		};
		let aside = self.em.set_aside();
		let mark = self.em.open_group(group);
		let lowered = self.schema_pipeline(op, reservations);
		let closed = self.em.close_group(mark);
		self.em.restore(aside);
		let (pipeline, needs) = lowered?;
		let permissions = closed?;
		self.absorb(&needs);
		Ok((pipeline, permissions, needs))
	}

	/// Lowers the `INFO` statement whose op is `op` as the group `group` of the program, and
	/// answers what it lowered with the range of permission frames it filled.
	///
	/// The pipeline in flight is set aside while the statement's pipeline is built, so none of its
	/// code becomes the group's. The pipeline reads one row from [`Source::Once`] and emits the
	/// statement's object as its one row; what the statement needs of the program is merged into
	/// this lowerer's.
	pub(super) fn lower_info_group(
		&mut self,
		op: OpId,
		group: u16,
	) -> Result<(InfoLowered, std::ops::Range<u16>), IrError> {
		let ir = self.ir;
		let Some(units) = self.units.as_deref_mut() else {
			return Err(invariant("a block lowers its statements in a lowering context"));
		};
		let aside = self.em.set_aside();
		let mark = self.em.open_group(group);
		let lowered = units.lower_info(&mut *self.em, ir, op);
		let closed = self.em.close_group(mark);
		self.em.restore(aside);
		let info = lowered?;
		let permissions = closed?;
		self.absorb(&info.needs);
		Ok((info, permissions))
	}

	/// The pipeline of the schema statement whose op is `op`, lowered into the emitter, and what it
	/// needs.
	fn schema_pipeline(
		&mut self,
		op: OpId,
		reservations: &mut Reservations,
	) -> Result<(Pipeline, Needs), IrError> {
		let facts = FactCache::new();
		let Some(units) = self.units.as_deref_mut() else {
			return Err(invariant("a schema statement lowered with no lowering context"));
		};
		let needs = units.lower_schema(self.em, self.ir, &facts, op, reservations)?;
		Ok((self.em.finish_pipeline(Source::Once)?, needs))
	}

	/// Records that the program needs what `needs` says of the code lowered elsewhere into it.
	pub(super) fn absorb(&mut self, needs: &Needs) {
		self.context = self.context.max(needs.context);
		self.keeps_row_order |= needs.keeps_row_order;
		if let Some(reach) = &needs.reach {
			self.reaches(reach.clone());
		}
		self.access_mode = self.access_mode.combine(needs.access_mode);
	}

	/// Appends `instr` as an instruction of `owner`, which its claim is checked against.
	pub(super) fn emit_for(&mut self, owner: OpId, instr: Instr) -> Label {
		self.owners.push(owner);
		let at = self.emit(instr);
		self.owners.pop();
		at
	}
}

impl<'a> Lowerer<'a> {
	/// A lowerer of the operand subset into `em`, resolving builtins against `registry`.
	pub(super) fn new(em: &'a mut Emitter, ir: &'a Ir, registry: &'a FunctionRegistry) -> Self {
		let slots = em.bound_slots().clone();
		Self {
			em,
			ir,
			registry,
			subset: Subset::Operand,
			limits: LowerLimits::default(),
			frame: Frame::OWN,
			scope: Scope::default(),
			rows: FxHashMap::default(),
			region: None,
			owners: Vec::new(),
			claims: Vec::new(),
			context: ContextLevel::Root,
			keeps_row_order: false,
			reach: None,
			schema: None,
			access_mode: AccessMode::ReadOnly,
			each_guard: None,
			units: None,
			slots,
			named_args: FxHashMap::default(),
			writing_labels: false,
			closure_row: false,
			closure_root: None,
			aside: false,
			named: Vec::new(),
			catch: None,
			loop_pads: None,
			loop_pads_taken: std::cell::Cell::new(false),
			statement_doc: None,
			element: None,
			unit_frame: None,
			depth: 0,
			block_groups: None,
			reads: None,
		}
	}

	/// This lowerer, lowering an expression of the body of a pipeline of `reads`: a statement the
	/// expression runs as a group of the program is added to `reads`.
	pub(super) fn with_reads(self, reads: &'a mut Reads) -> Self {
		Self {
			reads: Some(reads),
			..self
		}
	}

	/// Holds the groups of the statement program this lowerer lowers the list of: the statements
	/// of the list that run as groups of their own, and the statements its expressions run as
	/// groups of the program.
	pub(super) fn hold_block_groups(&mut self) {
		self.block_groups.get_or_insert_with(Groups::default);
	}

	/// The groups [`Lowerer::hold_block_groups`] holds.
	pub(super) fn block_groups(&mut self) -> &mut Groups {
		self.block_groups.get_or_insert_with(Groups::default)
	}

	/// Takes the groups [`Lowerer::hold_block_groups`] holds, leaving none held.
	pub(super) fn take_block_groups(&mut self) -> Groups {
		self.block_groups.take().unwrap_or_default()
	}

	/// The index the next group of the program takes and where the pools of its pipelines start,
	/// where this lowerer's code may run a statement as a group of the program: the code of a
	/// statement program's list or of the body of a pipeline of a read statement, which run over
	/// the program's running state. `None` for code that runs over a state of its own (a pipeline
	/// built beside the program's own) and for a lowerer that holds neither.
	pub(super) fn next_group(&self) -> Result<Option<(u16, PoolBase)>, IrError> {
		if self.aside {
			return Ok(None);
		}
		match (&self.reads, &self.block_groups) {
			(Some(reads), _) => reads.next_group().map(Some),
			(None, Some(groups)) => groups.next_group().map(Some),
			(None, None) => Ok(None),
		}
	}

	/// Whether the code being lowered takes a statement over a document it sets aside
	/// ([`Lowerer::with_statement_doc`]) rather than over the row.
	pub(super) fn sets_statement_document(&self) -> bool {
		self.statement_doc.is_some()
	}

	/// Appends the group lowered at [`Lowerer::next_group`], whose reads are `group` and whose
	/// spec is `spec`.
	pub(super) fn append_group(&mut self, group: Reads, spec: GroupSpec) -> Result<(), IrError> {
		match (self.reads.as_deref_mut(), self.block_groups.as_mut()) {
			(Some(reads), _) => reads.append_group(group, spec),
			(None, Some(groups)) => groups.append(group, spec),
			(None, None) => return Err(invariant("a group appended where no group is held")),
		}
		Ok(())
	}

	/// This lowerer, reading the declared kinds of the rows from `schema`.
	pub(super) fn with_schema(self, schema: Option<Arc<Schema>>) -> Self {
		Self {
			schema,
			..self
		}
	}

	/// A lowerer of the operand subset into `em` that resolves builtins against the registry of
	/// `cx`, enforces the limits of the request `cx` lowers for, and asks `cx` for the units the
	/// ops it lowers run.
	///
	/// The code starts in the container frame the ops of `ir`'s top region carry. A unit copied
	/// out of a region keeps its source's frame table and the frame of that region's ops (a
	/// statement written in a permission predicate carries the predicate's frame), and runs in the
	/// frame the instruction that runs it names, so its instructions name no permission frame of
	/// their own.
	pub(super) fn in_context(em: &'a mut Emitter, ir: &'a Ir, cx: &'a mut LowerCx<'_>) -> Self {
		let registry = cx.registry();
		let top = ops_in(ir, ir.top()).next().and_then(|op| ir.op(op)).map(|data| data.frame);
		Self {
			frame: Frame {
				ir: top.unwrap_or(Frame::OWN.ir),
				..Frame::OWN
			},
			registry: cx.registry(),
			limits: cx.limits(),
			unit_frame: Some(cx.frame().clone()),
			depth: cx.depth(),
			units: Some(cx),
			..Self::new(em, ir, registry)
		}
	}

	/// Reports `request`, which holds no slot of the unit table, to the compile.
	///
	/// A lowerer built without a unit table answers [`IrError::Invariant`], as
	/// [`Lowerer::unit_for`] does.
	pub(super) fn need_unit(&mut self, request: UnitRequest) -> Result<(), IrError> {
		match self.units.as_deref_mut() {
			Some(units) => units.need(request),
			None => Err(invariant("a unit asked for with no lowering context")),
		}
	}

	/// Whether the program being lowered serves the late-bound statements of its blocks when it
	/// reaches them. A lowerer built without a lowering context serves none.
	pub(super) fn serves_late(&self) -> bool {
		self.units.as_deref().is_some_and(|units| units.serves_late())
	}

	/// The re-entry depth the program is lowered at.
	pub(super) fn unit_depth(&self) -> u32 {
		self.units.as_deref().map_or(0, |units| units.depth())
	}

	/// Makes `sites` the measure of the stored code this lowerer lowers, answering the one it
	/// replaces; a lowerer with no lowering context holds none.
	pub(super) fn resite(
		&mut self,
		sites: Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>>,
	) -> Option<std::sync::Arc<crate::exec::vm::depth::ExprDepth>> {
		self.units.as_deref_mut().and_then(|units| units.resite(sites))
	}

	/// What the stored code being lowered spends on the way to its shallowest call of
	/// `fn::name`, beyond the call's own re-entry and what entering the code charged: nothing for
	/// a statement the session runs ([`crate::exec::vm::depth::ExprDepth::call_extra`]).
	pub(super) fn call_site_depth(&self, name: &str) -> u8 {
		let extra = self
			.units
			.as_deref()
			.and_then(|units| units.depth_sites())
			.map_or(0, |sites| sites.call_extra(name));
		u8::try_from(extra).unwrap_or(u8::MAX)
	}

	/// This lowerer, asking `sink` for the units the ops it lowers run and reading builtins from
	/// `registry` and the limits `limits`.
	pub(super) fn with_context(
		self,
		sink: &'a mut dyn UnitSink,
		registry: &'a FunctionRegistry,
		limits: LowerLimits,
		unit_frame: Option<UnitFrame>,
		depth: u32,
	) -> Self {
		Self {
			registry,
			limits,
			unit_frame,
			depth,
			units: Some(sink),
			..self
		}
	}

	/// The slot of the unit table that holds the unit `request` names.
	///
	/// A lowerer built without a unit table answers [`IrError::Invariant`]: every lowerer of the
	/// operand subset is built in a lowering context ([`Lowerer::in_context`]), and the plan-time
	/// folder's subset refuses every op that runs a unit before it asks for one.
	///
	/// The unit of a statement in an expression stands where the region it is copied from does
	/// ([`StatementNesting::around`]), which the computation-depth count of the events a write in
	/// it fires resumes from.
	pub(super) fn unit_for(&mut self, request: UnitRequest) -> Result<UnitId, IrError> {
		let around = if let UnitRequest::Subquery {
			region,
		} = &request
		{
			StatementNesting::around(self.ir, *region)
		} else {
			StatementNesting::default()
		};
		match self.units.as_deref_mut() {
			Some(units) => units.unit_within(request, around),
			None => Err(invariant("a unit asked for with no lowering context")),
		}
	}

	/// This lowerer, reading the limits `limits` where an instruction enforces one.
	#[allow(dead_code)] // Called with the request's configuration by the compile driver.
	pub(super) fn with_limits(self, limits: LowerLimits) -> Self {
		Self {
			limits,
			..self
		}
	}

	/// A lowerer of the subset the plan-time folder runs.
	pub(super) fn for_fold(
		em: &'a mut Emitter,
		ir: &'a Ir,
		registry: &'a FunctionRegistry,
	) -> Self {
		Self {
			subset: Subset::Fold,
			..Self::new(em, ir, registry)
		}
	}

	/// The registry the lowerer resolves builtins against.
	pub(super) fn registry(&self) -> &'a FunctionRegistry {
		self.registry
	}

	/// The emitter the lowerer emits into, for a caller that emits instructions of its own between
	/// the values it reads.
	pub(super) fn emitter(&mut self) -> &mut Emitter {
		self.em
	}

	/// Refuses an op that runs under an authority frame other than the one being lowered.
	///
	/// A region owned by an op that runs under a frame of its own (a permission predicate, a
	/// computed field's body) is lowered inside [`Lowerer::in_frame`], so the ops it holds carry
	/// that frame and the instructions emitted for them name its program frame.
	pub(super) fn require_frame(&self, data: &OpData) -> Result<(), IrError> {
		match data.frame == self.frame.ir {
			true => Ok(()),
			false => not_yet("ir_lower::expr: authority frame"),
		}
	}

	/// Runs `body` with the code lowered in the container frame `ir`, which the program names as
	/// permission frame `program`, and in a privileged frame, which holds none of the caller's own
	/// bindings. The frame is the one outside again afterwards, on every path out.
	pub(super) fn in_frame<T>(
		&mut self,
		ir: FrameId,
		program: u16,
		body: impl FnOnce(&mut Self) -> Result<T, IrError>,
	) -> Result<T, IrError> {
		let outer = std::mem::replace(
			&mut self.frame,
			Frame {
				ir,
				program: Some(program),
				privileged: true,
			},
		);
		let lowered = body(self);
		self.frame = outer;
		lowered
	}

	/// Runs `body` with the code lowered in the container frame `ir`, in a privileged frame that no
	/// program frame names: a clause of a field, which the host runs in the frame it binds for the
	/// field's place (the field's `AUTH LIMIT`, the place's parameters), so the instructions name
	/// no permission frame and a parameter is read in the frame the code runs in. The frame is the
	/// one outside again afterwards, on every path out.
	pub(super) fn in_host_frame<T>(
		&mut self,
		ir: FrameId,
		body: impl FnOnce(&mut Self) -> Result<T, IrError>,
	) -> Result<T, IrError> {
		let outer = std::mem::replace(
			&mut self.frame,
			Frame {
				ir,
				program: None,
				privileged: true,
			},
		);
		let lowered = body(self);
		self.frame = outer;
		lowered
	}

	/// Whether the code being lowered is the body of a pipeline of a read statement, which runs
	/// once per run of the group it stands in, rather than the list of a statement program, which
	/// a loop of the program may run again in the state it ran in before.
	pub(super) fn lowers_a_read_pipeline(&self) -> bool {
		self.reads.is_some()
	}

	/// Whether the code being lowered is a definer's code run in a privileged frame, which holds
	/// none of the caller's bindings: the permission, gate, layer and field-clause frames, and
	/// every call made from one.
	pub(super) fn is_privileged(&self) -> bool {
		self.frame.privileged || is_permission_frame(self.frame.program)
	}

	/// Whether the code being lowered runs in an authority frame of its own (a permission
	/// predicate's, a field clause's) rather than in the frame its container's code starts in.
	pub(super) fn in_frame_of_its_own(&self) -> bool {
		self.frame.program.is_some() || self.frame.privileged
	}

	/// Whether the code being lowered reads with its permissions unchecked: a predicate a guard,
	/// a write gate or a derivation for a predicate judges, whose host steps read records
	/// unchecked. A computed field's body and an index column read as the statement does.
	pub(super) fn reads_unchecked(&self) -> bool {
		self.frame
			.program
			.filter(|frame| is_permission_frame(Some(*frame)))
			.and_then(|frame| self.em.permission_at(frame))
			.is_some_and(|spec| {
				matches!(spec.mode, FrameMode::Guard(_) | FrameMode::Gate(_) | FrameMode::Derived)
			})
	}

	/// What the instructions lowered so far need of the program that holds them.
	pub(super) fn needs(&self) -> Needs {
		Needs {
			context: self.context,
			keeps_row_order: self.keeps_row_order,
			reach: self.reach.clone(),
			access_mode: self.access_mode,
		}
	}

	/// The limits of the request's configuration the instructions lowered enforce.
	pub(super) fn limits(&self) -> LowerLimits {
		self.limits
	}

	/// Records that the program needs at least the context level `level`.
	pub(super) fn requires(&mut self, level: ContextLevel) {
		self.context = self.context.max(level);
	}

	/// The context level the program needs for what was lowered so far, which
	/// [`Self::restore_required`] takes back to.
	pub(super) fn required(&self) -> ContextLevel {
		self.context
	}

	/// Takes the context level the program needs back to `level`, from [`Self::required`]: what was
	/// lowered since runs after the session's names are resolved again, and raises for a missing
	/// level where it reads it rather than before the program starts.
	pub(super) fn restore_required(&mut self, level: ContextLevel) {
		self.context = level;
	}

	/// Records that the instructions lowered may write through the transaction.
	pub(super) fn writes(&mut self) {
		self.access_mode = AccessMode::ReadWrite;
	}

	/// Records that the instructions lowered may read the tables `reach` names, through a value
	/// they are given.
	pub(super) fn reaches(&mut self, reach: Reach) {
		self.reach = Some(match self.reach.take() {
			Some(held) => held.union(reach),
			None => reach,
		});
	}

	/// Gives the block argument `arg` the operand it is read from, in the scope being lowered.
	///
	/// The owner of the region the argument belongs to calls this before the region is lowered. A
	/// region of a lazy op is given its arguments through [`Lowerer::region_value_with`].
	#[allow(dead_code)] // Called by the lowering of the ops that own a region with arguments.
	pub(super) fn bind_arg(&mut self, arg: ValueId, operand: Operand) {
		self.scope.values.insert(arg, operand);
	}

	/// Makes the block argument `arg` the row of the pipeline being lowered. `objects` says the
	/// rows are objects, so a field of the row is an operand of the row itself.
	///
	/// The row is read as a value of its own where it is read other than as the base of a field of
	/// an object row. A pipeline of another body, built beside this one, has a row of its own and
	/// binds it afresh.
	#[allow(dead_code)] // Called by the lowering of the pipelines that read rows.
	pub(super) fn bind_row(&mut self, arg: ValueId, objects: bool) {
		self.rows.insert(arg, objects);
	}

	/// Whether the block argument `row` is the row of the pipeline being lowered, and the rows are
	/// objects.
	pub(super) fn is_object_row(&self, row: ValueId) -> bool {
		self.rows.get(&row) == Some(&true)
	}

	/// Whether the block argument `row` is the row of the pipeline being lowered, whatever the
	/// shape of its rows.
	pub(super) fn is_pipeline_row(&self, row: ValueId) -> bool {
		self.rows.contains_key(&row)
	}

	/// Whether the op being lowered is the only op that reads `value`.
	pub(super) fn read_only_by_owner(&self, value: ValueId) -> bool {
		let Some(owner) = self.owners.last() else {
			return false;
		};
		matches!(self.ir.uses(value), [only] if only.user == *owner)
	}

	/// Sets aside what a pipeline built beside the one being lowered must not see of it: the
	/// values lowered, the rows bound, the region entered, the claims recorded, the scope that
	/// takes a `RETURN`, the document of a statement, the element of a body and the guard of a
	/// broadcast's `.*`, whose register is one of the pipeline set aside. The claims
	/// index the code of the pipeline being lowered, so those of the pipeline built beside are not
	/// kept when [`Lowerer::rejoin`] returns.
	pub(super) fn beside(&mut self) -> Beside {
		Beside {
			program_frame: self.frame.program.take(),
			aside: std::mem::replace(&mut self.aside, true),
			scope: std::mem::take(&mut self.scope),
			rows: std::mem::take(&mut self.rows),
			claims: std::mem::take(&mut self.claims),
			region: self.region.take(),
			catch: self.catch.take(),
			loop_pads: self.loop_pads.take(),
			statement_doc: self.statement_doc.take(),
			element: self.element.take(),
			each_guard: self.each_guard.take(),
			closure_root: self.closure_root.take(),
		}
	}

	/// Returns to the pipeline `beside` was set aside from.
	pub(super) fn rejoin(&mut self, beside: Beside) {
		self.frame.program = beside.program_frame;
		self.aside = beside.aside;
		self.scope = beside.scope;
		self.rows = beside.rows;
		self.claims = beside.claims;
		self.region = beside.region;
		self.catch = beside.catch;
		self.loop_pads = beside.loop_pads;
		self.statement_doc = beside.statement_doc;
		self.element = beside.element;
		self.each_guard = beside.each_guard;
		self.closure_root = beside.closure_root;
	}

	/// Runs `body` under `scope`. The scope that held before is back afterwards, on every path out.
	pub(super) fn catching<T>(
		&mut self,
		scope: ReturnScope,
		body: impl FnOnce(&mut Self) -> Result<T, IrError>,
	) -> Result<T, IrError> {
		let outer = self.catch.replace(scope);
		let lowered = body(self);
		self.catch = outer;
		lowered
	}

	/// Makes `scope` where a `RETURN` that a statement held in an expression signals is taken, for
	/// everything this lowerer lowers from here on.
	pub(super) fn catch_returns(&mut self, scope: ReturnScope) {
		self.catch = Some(scope);
	}

	/// The value of one evaluation of a context, which `body` lowers: the value it yields, or the
	/// value of a `RETURN` a statement held in it signals, which ends the evaluation there. The
	/// context's value is left in a register of its own, past a label the `RETURN` jumps to.
	///
	/// A `RETURN` skips whatever of the evaluation follows it, so the values lowered in `body` are
	/// forgotten afterwards, and a later reader lowers its own.
	pub(super) fn context_value(
		&mut self,
		body: impl FnOnce(&mut Self) -> Result<Operand, IrError>,
	) -> Result<Operand, IrError> {
		let (value, catch) = self.context_catch(body)?;
		self.emit(Instr::Load {
			dst: catch.value,
			src: value,
		});
		self.em.bind(catch.label)?;
		Ok(Operand::Reg(catch.value))
	}

	/// Lowers what `body` lowers as the evaluation of a context, and answers it with where a
	/// `RETURN` a statement held in it signals is taken: the register the `RETURN`'s value is left
	/// in, and a label the caller binds past the rest of the evaluation, which the `RETURN` skips.
	///
	/// The values lowered in `body` are forgotten afterwards, as in [`Self::context_value`].
	pub(super) fn context_catch<T>(
		&mut self,
		body: impl FnOnce(&mut Self) -> Result<T, IrError>,
	) -> Result<(T, ReturnCatch), IrError> {
		let catch = ReturnCatch {
			value: self.em.reg()?,
			label: self.em.label()?,
		};
		let scope = ReturnScope {
			context: Some(catch),
			frame: self.frame.program,
			loops_fail: self.loops_fail(),
		};
		let held = self.scope.clone();
		let lowered = self.catching(scope, body);
		self.scope = held;
		Ok((lowered?, catch))
	}

	/// Whether a `BREAK` or `CONTINUE` signalled in the code being lowered is an error, as the
	/// [`ReturnScope`] it is lowered under says.
	pub(super) fn loops_fail(&self) -> bool {
		self.catch.is_some_and(|scope| scope.loops_fail)
	}

	/// The catch of the statement whose instruction is emitted next: a `RETURN` it signals ends
	/// the context of the code being lowered, and a `BREAK` or `CONTINUE` is an error where the
	/// scope says so, and otherwise jumps to the loop pads the code being lowered holds, if any.
	///
	/// `None` where the code being lowered holds no [`ReturnScope`], and in a permission frame
	/// other than the one the context is evaluated in: a signal raised in a permission frame
	/// belongs to the statement that owns the frame.
	pub(super) fn own_catch(&self) -> Option<Catch> {
		let scope = self.catch?;
		if is_permission_frame(self.frame.program) && scope.frame != self.frame.program {
			return None;
		}
		let pads = match scope.loops_fail {
			true => None,
			false => self.loop_pads,
		};
		if pads.is_some() {
			self.loop_pads_taken.set(true);
		}
		Some(Catch {
			on_return: scope.context,
			loops_fail: scope.loops_fail,
			on_break: pads.map(|pads| pads.on_break),
			on_continue: pads.map(|pads| pads.on_continue),
		})
	}

	/// Makes `pads` where a `BREAK` or `CONTINUE` signalled by a statement held in the code lowered
	/// from here on jumps, and answers what held before, for [`Self::restore_loop_pads`].
	pub(super) fn set_loop_pads(&mut self, pads: Option<LoopPads>) -> (Option<LoopPads>, bool) {
		let outer = std::mem::replace(&mut self.loop_pads, pads);
		(outer, self.loop_pads_taken.replace(false))
	}

	/// Puts back the pads [`Self::set_loop_pads`] answered, and answers whether a statement took
	/// the pads it set.
	pub(super) fn restore_loop_pads(&mut self, outer: (Option<LoopPads>, bool)) -> bool {
		self.loop_pads = outer.0;
		self.loop_pads_taken.replace(outer.1)
	}

	/// Runs `body` with a statement in the code it lowers taken over the document in `doc`, as
	/// its `$this` and `$parent`. The document that held before is back afterwards, on every
	/// path out.
	#[allow(dead_code)] // Called by the lowering of the columns and omissions that set a document.
	pub(super) fn with_statement_doc<T>(
		&mut self,
		doc: Option<Reg>,
		body: impl FnOnce(&mut Self) -> Result<T, IrError>,
	) -> Result<T, IrError> {
		let outer = std::mem::replace(&mut self.statement_doc, doc);
		let lowered = body(self);
		self.statement_doc = outer;
		lowered
	}

	/// The register that holds the document a statement in the code being lowered is taken over.
	///
	/// A document set aside by [`Lowerer::with_statement_doc`] is that one. In a pipeline built
	/// beside the one in flight that runs over a row, it is the document root of the row's
	/// evaluation, and the row where there is none, so a statement written in an element body
	/// reads the element it is applied to. Anywhere else the statement takes the context's own
	/// document, which is no register.
	pub(super) fn statement_document(&mut self) -> Result<Option<Reg>, IrError> {
		if let Some(doc) = self.statement_doc {
			return Ok(Some(doc));
		}
		if !self.aside || self.rows.is_empty() {
			return Ok(None);
		}
		let row = self.em.reg()?;
		self.emit(Instr::LoadRow {
			dst: row,
		});
		let root = self.em.reg()?;
		self.emit(Instr::DocumentRoot {
			or: Operand::Reg(row),
			dst: root,
		});
		Ok(Some(root))
	}

	/// Refuses, in the fold subset, an op the plan-time folder's eligibility check
	/// (`ir::fold::evaluate::eligible_under`) refuses before the fold host lowers anything: a tag
	/// outside its op subset, a read of the clock or of the principal, or a call whose effects are
	/// not pure. The fold host lowers only a value that check admitted, so the refusal is an
	/// invariant naming the op as `what`.
	pub(super) fn refuse_in_fold(&self, what: &'static str) -> Result<(), IrError> {
		match self.subset {
			Subset::Fold => Err(invariant(what)),
			Subset::Operand => Ok(()),
		}
	}

	/// The instructions emitted so far, each with the op it was emitted for, and none again on a
	/// later call.
	pub(super) fn take_claims(&mut self) -> Vec<Claim> {
		std::mem::take(&mut self.claims)
	}

	/// Appends `instr`, recording the op being lowered as its owner.
	pub(super) fn emit(&mut self, instr: Instr) -> Label {
		let at = self.em.emit(instr);
		if let Some(owner) = self.owners.last() {
			self.claims.push((*owner, at));
		}
		at
	}

	/// Ends the one row of a scalar pipeline: loads `src` into a register, emits it and ends the
	/// row, as the instructions of the op `owner`.
	pub(super) fn emit_scalar_row(&mut self, owner: OpId, src: Operand) -> Result<(), IrError> {
		self.owners.push(owner);
		let dst = self.em.reg()?;
		self.emit(Instr::Load {
			dst,
			src,
		});
		self.emit(Instr::Emit {
			src: dst,
		});
		self.emit(Instr::EndRow);
		self.owners.pop();
		Ok(())
	}

	/// The op that defines `v`, when no read has lowered it yet and `v` is its result.
	fn unlowered_def(&self, v: ValueId) -> Option<OpId> {
		if self.scope.values.contains_key(&v) || self.slots.contains_key(&v) {
			return None;
		}
		match self.ir.value(v)?.def {
			ValueDef::OpResult {
				op,
				index: 0,
			} => Some(op),
			ValueDef::OpResult {
				..
			}
			| ValueDef::BlockArg {
				..
			} => None,
		}
	}

	/// Where the value `v` is read from once the instructions emitted so far have run.
	///
	/// Lowers the op that defines `v` unless an earlier read did. A block argument is read from
	/// what the lowering that entered its region bound it to: a row, a slot, a named parameter,
	/// an operand, an input of the unit or `$parent`. One bound to none of these is an invariant
	/// break.
	pub(super) fn value(&mut self, v: ValueId) -> Result<Operand, IrError> {
		if let Some(held) = self.scope.values.get(&v) {
			return Ok(*held);
		}
		if let Some(bound) = self.slots.get(&v).cloned() {
			// Definer code holds none of the caller's bindings, which a block's names are.
			if self.is_privileged() {
				return Err(invariant("definer code reads a name a block of the caller binds"));
			}
			let dst = self.em.reg()?;
			if self.aside {
				// A slot value is bound by a `FOR` or a `LET` of a block, or by a closure body's
				// parameters and captures, and each names its parameter: the binder states a
				// `FOR`'s as its step and conversion keeps it.
				let Some(name) = bound.name else {
					return Err(invariant(
						"a block value read beside the pipeline in flight is unnamed",
					));
				};
				let param = self.em.param(ParamSpec {
					name: name.clone(),
					guard: None,
				})?;
				self.em.emit(Instr::LoadParam {
					param,
					dst,
					frame: None,
				});
				self.named.push(name);
			} else {
				self.em.emit(Instr::LoadSlot {
					slot: bound.slot,
					dst,
				});
			}
			let operand = Operand::Reg(dst);
			self.scope.values.insert(v, operand);
			return Ok(operand);
		}
		let ir = self.ir;
		let data = ir.value(v).ok_or(IrError::Stale("ir_lower::expr: value"))?;
		let operand = match data.def {
			ValueDef::OpResult {
				op,
				index: 0,
			} => {
				let defining = ir.op(op).ok_or(IrError::Stale("ir_lower::expr: op"))?;
				if !defining.regions.is_empty() {
					self.lower_reads_of_regions(op, defining)?;
				}
				self.owners.push(op);
				let lowered = self.op(defining);
				self.owners.pop();
				lowered?
			}
			// Every op's arity states at most one result, which the verifier checks.
			ValueDef::OpResult {
				..
			} => return Err(invariant("a value read from a result past an op's first")),
			ValueDef::BlockArg {
				..
			} => {
				if self.rows.contains_key(&v) {
					let dst = self.em.reg()?;
					self.emit(Instr::LoadRow {
						dst,
					});
					Operand::Reg(dst)
				} else if self.closure_root == Some(v) {
					let row = self.em.reg()?;
					self.emit(Instr::LoadRow {
						dst: row,
					});
					let dst = self.em.reg()?;
					self.emit(Instr::DocumentRoot {
						or: Operand::Reg(row),
						dst,
					});
					Operand::Reg(dst)
				} else if let Some(at) = self.unit_input(v) {
					self.load_unit_input(at)?
				} else if self.em.is_parent_row(v) {
					self.load_parent()?
				} else if let Some(name) = self.named_args.get(&v).cloned() {
					let param = self.em.param(ParamSpec {
						name,
						guard: None,
					})?;
					let dst = self.em.reg()?;
					self.em.emit(Instr::LoadParam {
						param,
						dst,
						frame: self.frame.program,
					});
					Operand::Reg(dst)
				} else {
					// Every lowering that enters a region binds the arguments the region reads
					// before its body, and the compile driver counts a unit's inputs for each
					// copied unit it lowers. The statement timestamp is read only by `expr.now`,
					// which does not read it.
					return Err(invariant("a block argument nothing bound"));
				}
			}
		};
		self.scope.values.insert(v, operand);
		Ok(operand)
	}

	/// The position of `v` among the inputs of the unit being lowered: the trailing arguments of
	/// the container's top block that the lowering context counts ([`UnitSink::unit_inputs`]).
	fn unit_input(&self, v: ValueId) -> Option<usize> {
		let count = usize::from(self.units.as_deref()?.unit_inputs());
		if count == 0 {
			return None;
		}
		let top = self.ir.region(self.ir.top())?.blocks.first().copied()?;
		let args = &self.ir.block(top)?.args;
		let own = args.len().checked_sub(count)?;
		args.get(own..)?.iter().position(|arg| *arg == v)
	}

	/// The `at`th input of the unit being lowered, read from the context the instruction that
	/// runs the unit binds it in, under [`unit_input_name`]`(at)`.
	///
	/// A read in a privileged frame is no read of a caller's binding: the unit's code is a region
	/// of the requester's, so an input of a unit copied from definer code is a value of that
	/// definer code, which the requester read under its own frame's rules.
	fn load_unit_input(&mut self, at: usize) -> Result<Operand, IrError> {
		let param = self.em.param(ParamSpec {
			name: unit_input_name(at),
			guard: None,
		})?;
		let dst = self.em.reg()?;
		self.em.emit(Instr::LoadParam {
			param,
			dst,
			frame: self.frame.program,
		});
		Ok(Operand::Reg(dst))
	}

	/// `$parent`, which the instruction that runs a statement as a group of the program binds to
	/// the row it evaluates the statement for.
	fn load_parent(&mut self) -> Result<Operand, IrError> {
		let param = self.em.param(ParamSpec {
			name: Strand::from("parent"),
			guard: None,
		})?;
		let dst = self.em.reg()?;
		self.em.emit(Instr::LoadParam {
			param,
			dst,
			frame: self.frame.program,
		});
		Ok(Operand::Reg(dst))
	}

	/// Lowers, in program order, each expression op before `owner` in its block that a region of
	/// `owner` reads at any depth and no read has lowered yet, so that the region finds it in
	/// scope.
	///
	/// Such an op stands where the binder placed it, or where common-subexpression elimination
	/// left the earlier of two equal values, which dominates the later one: either way the op is
	/// evaluated before `owner` whenever `owner` is, as the expression is written. A value a region
	/// reads that is defined further out, and that no lowering of its owner put in scope (a region
	/// a statement lowering enters itself), is lowered where the region reads it, in the region's
	/// own scope.
	pub(super) fn lower_reads_of_regions(
		&mut self,
		owner: OpId,
		data: &OpData,
	) -> Result<(), IrError> {
		let ir = self.ir;
		let block = ir.block(data.parent).ok_or(IrError::Stale("ir_lower::expr: block"))?;
		let at = block
			.ops
			.iter()
			.position(|id| *id == owner)
			.ok_or(IrError::Stale("ir_lower::expr: op position"))?;
		for id in &block.ops[..at] {
			let before = ir.op(*id).ok_or(IrError::Stale("ir_lower::expr: op"))?;
			if !matches!(&before.kind, OpKind::Expr(op) if !matches!(op, ExprOp::Literal(_))) {
				continue;
			}
			let Some(result) = before.results.first().copied() else {
				continue;
			};
			if self.scope.values.contains_key(&result) || self.slots.contains_key(&result) {
				continue;
			}
			let read_inside = ir.uses(result).iter().any(|used| {
				lowered_use(ir, used)
					&& ancestors(ir, used.user).any(|(_, enclosing)| enclosing == Some(owner))
			});
			if read_inside {
				self.value(result)?;
			}
		}
		Ok(())
	}

	/// The operands of `values`, in order.
	pub(super) fn values(&mut self, values: &[ValueId]) -> Result<Vec<Operand>, IrError> {
		values.iter().map(|value| self.value(*value)).collect()
	}

	/// Lowers `data`, with one arm for every op of the dialect.
	fn op(&mut self, data: &OpData) -> Result<Operand, IrError> {
		let op = match &data.kind {
			OpKind::Expr(op) => op,
			OpKind::Phys(PhysOp::Loop {
				..
			}) => return self.value_loop(data),
			// A statement in relational position read as a value, the rows a write takes as its
			// targets: run as the unit its region is, as an `expr.subquery` is.
			OpKind::Phys(PhysOp::Subquery) => {
				return self.subquery(data, &crate::exec::ir::expr::ops::NestedStrategy::Apply);
			}
			// The physical phase wraps every value loop that may fan out in a pipeline of its own
			// (`phys::pipeline::build`); the loop is lowered in place, as its own instructions. A
			// `FOR` in value position is a statement, which runs as the unit its pipeline's region
			// is, where code built beside a statement's list reads it afresh.
			OpKind::Phys(PhysOp::Pipeline(_)) => {
				let ir = self.ir;
				let Some(looped) = wrapped_value_loop(ir, data)? else {
					if wraps_value_for(ir, data)? {
						return self.value_for(data);
					}
					return not_yet("ir_lower::expr: an op outside the expr dialect");
				};
				let loop_data = ir.op(looped).ok_or(IrError::Stale("ir_lower::expr: op"))?;
				self.owners.push(looped);
				let lowered = self.value_loop(loop_data);
				self.owners.pop();
				return lowered;
			}
			OpKind::Phys(PhysOp::Block) => return self.value_block(data),
			OpKind::Logical(_) | OpKind::Phys(_) => {
				return not_yet("ir_lower::expr: an op outside the expr dialect");
			}
		};
		match op {
			ExprOp::Literal(value) => {
				let index = self.em.constant((**value).clone())?;
				Ok(Operand::Const(index))
			}
			ExprOp::Array => {
				let dst = self.em.reg()?;
				self.emit(Instr::NewArray {
					dst,
				});
				for item in &data.operands {
					let value = self.value(*item)?;
					self.emit(Instr::ArrayPush {
						array: dst,
						value,
					});
				}
				Ok(Operand::Reg(dst))
			}
			ExprOp::Set => {
				let dst = self.em.reg()?;
				self.emit(Instr::NewSet {
					dst,
				});
				for item in &data.operands {
					let value = self.value(*item)?;
					self.emit(Instr::CollectPush {
						collection: dst,
						value,
					});
				}
				Ok(Operand::Reg(dst))
			}
			ExprOp::Object {
				keys,
			} => self.object(data, keys),
			ExprOp::RecordId(attrs) => self.record_id(data, attrs),
			ExprOp::FreeParam {
				name,
			} => {
				self.refuse_in_fold("ir_lower::fold: expr.free_param")?;
				self.load_param(data, name)
			}
			ExprOp::SessionParam {
				name,
			} => {
				self.refuse_in_fold("ir_lower::fold: expr.session_param")?;
				self.load_param(data, name)
			}
			ExprOp::CatalogParam {
				name,
			} => {
				self.refuse_in_fold("ir_lower::fold: expr.catalog_param")?;
				self.load_param(data, name)
			}
			ExprOp::Table {
				name,
			} => {
				self.refuse_in_fold("ir_lower::fold: expr.table")?;
				Ok(Operand::Const(self.em.constant(Value::Table(name.clone()))?))
			}
			ExprOp::Mock(mock) => {
				self.refuse_in_fold("ir_lower::fold: expr.mock")?;
				let dst = self.em.reg()?;
				self.emit(Instr::Mock {
					mock: Arc::new((**mock).clone()),
					limit: self.limits.generation_allocation_limit,
					dst,
				});
				Ok(Operand::Reg(dst))
			}
			ExprOp::Constant {
				c,
			} => {
				self.refuse_in_fold("ir_lower::fold: expr.constant")?;
				Ok(Operand::Const(self.em.constant(c.compute())?))
			}
			ExprOp::Field {
				name,
			} => self.field(data, name, "ir_lower::fold: expr.field"),
			ExprOp::ComputedRead(attrs) => {
				self.field(data, &attrs.field, "ir_lower::fold: expr.computed_read")
			}
			ExprOp::Doc => self.doc(data),
			ExprOp::Access {
				field,
			} => {
				self.require_frame(data)?;
				let [base] = exactly::<1>(data)?;
				let src = self.value(base)?;
				let field = self.em.field(field.as_str())?;
				let dst = self.em.reg()?;
				self.emit(Instr::Access {
					src,
					field,
					dst,
					frame: self.frame.program,
				});
				Ok(Operand::Reg(dst))
			}
			ExprOp::Deref {
				..
			} => Err(invariant("expr.deref has no producer: no binder or pass builds one")),
			ExprOp::Index => {
				let [base, index] = exactly::<2>(data)?;
				let src = self.value(base)?;
				let index = self.value(index)?;
				let dst = self.em.reg()?;
				self.emit(Instr::Nav {
					op: NavOp::Index,
					src,
					index: Some(index),
					dst,
				});
				Ok(Operand::Reg(dst))
			}
			ExprOp::First => self.nav(data, NavOp::First, "ir_lower::fold: expr.first"),
			ExprOp::Last => self.nav(data, NavOp::Last, "ir_lower::fold: expr.last"),
			ExprOp::Each => self.each(data),
			ExprOp::Flatten {
				..
			} => self.nav(data, NavOp::Flatten, "ir_lower::fold: expr.flatten"),
			ExprOp::Dispatch(target) => self.dispatch(data, target),
			ExprOp::Method {
				..
			} => self.method(data),
			ExprOp::Destructure {
				shape,
			} => self.destructure(data, shape),
			ExprOp::Optional => self.optional(data),
			ExprOp::RepeatRecurse => self.repeat_recurse(data),
			ExprOp::SetField {
				field,
			} => self.set_field(data, field),
			ExprOp::TupleRow {
				..
			} => Err(IrError::Invariant {
				what: "expr.tuple_row is lowered by the INSERT whose rows hold it, never as a value",
			}),
			ExprOp::Prefix(operator) => {
				let [subject] = exactly::<1>(data)?;
				let op = match &**operator {
					PrefixOperator::Not => UnaryOp::Not,
					PrefixOperator::Negate => UnaryOp::Negate,
					PrefixOperator::Positive => UnaryOp::Positive,
					PrefixOperator::Range => UnaryOp::RangeTo {
						included: false,
					},
					PrefixOperator::RangeInclusive => UnaryOp::RangeTo {
						included: true,
					},
					PrefixOperator::Cast(_) => {
						return Err(invariant(
							"expr.prefix never holds a cast: the binder writes a cast as expr.convert",
						));
					}
				};
				self.unary(op, subject)
			}
			ExprOp::Convert(attrs) => {
				let [subject] = exactly::<1>(data)?;
				match (attrs.mode, attrs.on_fail) {
					(ConvertMode::Cast, OnFail::Error) => {
						let src = self.value(subject)?;
						let kind = self.em.kind(attrs.kind.clone())?;
						let dst = self.em.reg()?;
						self.emit(Instr::Unary {
							op: UnaryOp::Cast {
								kind,
							},
							src,
							dst,
						});
						Ok(Operand::Reg(dst))
					}
					(ConvertMode::Cast, OnFail::None) => Err(invariant(
						"expr.convert answers none on failure only as a coercion: no binder builds a cast that does",
					)),
					(ConvertMode::Coerce, OnFail::Error | OnFail::None) => Err(invariant(
						"expr.convert as a coercion has no producer: a coercion is a field, a typed binding or a parameter, which their statements lower",
					)),
				}
			}
			ExprOp::Postfix {
				op: operator,
			} => {
				let [subject] = exactly::<1>(data)?;
				let op = match operator {
					LogicalPostfixOperator::Range => UnaryOp::RangeFrom {
						excluded: false,
					},
					LogicalPostfixOperator::RangeSkip => UnaryOp::RangeFrom {
						excluded: true,
					},
				};
				self.unary(op, subject)
			}
			ExprOp::Invoke => self.invoke(data),
			ExprOp::Binary {
				op: operator,
			} => {
				let [left, right] = exactly::<2>(data)?;
				if let Some(op) = CompareOp::of(operator) {
					if let Some(typed) = self.field_int_compare(data, op, left, right)? {
						return Ok(typed);
					}
					let left = self.value(left)?;
					if let (CompareOp::Inside | CompareOp::NotInside, Some(subquery)) =
						(op, self.unlowered_def(right))
						&& let Some(member) =
							self.member(subquery, left, matches!(op, CompareOp::NotInside))?
					{
						return Ok(member);
					}
					let right = self.value(right)?;
					let dst = self.em.reg()?;
					self.emit(Instr::Compare {
						op,
						left,
						right,
						dst,
					});
					Ok(Operand::Reg(dst))
				} else if let Some(op) = ArithOp::of(operator) {
					let left = self.value(left)?;
					let right = self.value(right)?;
					let dst = self.em.reg()?;
					self.emit(Instr::Arith {
						op,
						left,
						right,
						dst,
					});
					Ok(Operand::Reg(dst))
				} else {
					if let BinaryOperator::Matches(matches) = operator {
						self.matches(matches, left, right)
					} else if let BinaryOperator::NearestNeighbor(_) = operator {
						self.nearest(left, right)
					} else {
						not_yet(
							"ir_lower::expr: expr.binary with an operator no instruction answers",
						)
					}
				}
			}
			ExprOp::ShortCircuit {
				op: operator,
			} => self.short_circuit(data, *operator),
			ExprOp::Call(call) => {
				self.require_frame(data)?;
				self.call(data, call)
			}
			ExprOp::ClosureCreate(attrs) => self.closure_create(data, attrs),
			ExprOp::If => self.if_else(data),
			ExprOp::Throw => Err(invariant("expr.throw ends a region and defines no value")),
			ExprOp::Subquery(strategy) => self.subquery(data, strategy),
			// A yield's arity states no result, so no value names one as its definer.
			ExprOp::Yield => Err(invariant("expr.yield defines no value")),
			ExprOp::Truthy => self.truthy(data),
			// The binder builds an aggregate call only in a field of the statement's grouping,
			// whose `phys.aggregate` reads each call's answer from the group; no other op reads it.
			ExprOp::Agg {
				..
			} => Err(invariant("expr.agg outside the grouping that answers it")),
			ExprOp::Now {
				which,
			} => self.now(data, *which),
			ExprOp::Raise(_) => Err(invariant("expr.raise ends a region and defines no value")),
			ExprOp::DepthCharge {
				..
			} => Err(invariant(
				"expr.depth_charge has no producer: no rewrite removes a charged call",
			)),
		}
	}

	/// `op` over `subject`.
	fn unary(&mut self, op: UnaryOp, subject: ValueId) -> Result<Operand, IrError> {
		let src = self.value(subject)?;
		let dst = self.em.reg()?;
		self.emit(Instr::Unary {
			op,
			src,
			dst,
		});
		Ok(Operand::Reg(dst))
	}

	/// An object of `data`'s operands under `keys`: the values are evaluated in order, then the
	/// object is built from them.
	fn object(&mut self, data: &OpData, keys: &[String]) -> Result<Operand, IrError> {
		if keys.len() != data.operands.len() {
			return Err(IrError::Stale("ir_lower::expr: object arity"));
		}
		let mut fields = Vec::with_capacity(keys.len());
		for (key, item) in keys.iter().zip(&data.operands) {
			let value = self.value(*item)?;
			let path = self.em.path(FieldPath::field(key.as_str()))?;
			fields.push((path, value));
		}
		let dst = self.em.reg()?;
		if fields.is_empty() {
			self.emit(Instr::NewObject {
				dst,
			});
		} else {
			self.emit(Instr::BuildObject {
				dst,
				fields: fields.into_boxed_slice(),
			});
		}
		Ok(Operand::Reg(dst))
	}

	/// The value of the left operand when it decides, otherwise the value of the region that holds
	/// the right operand.
	fn short_circuit(&mut self, data: &OpData, op: ShortCircuitOp) -> Result<Operand, IrError> {
		let [left] = exactly::<1>(data)?;
		let right = region_at(data, 0, 1)?;
		let left = self.value(left)?;
		let dst = self.em.reg()?;
		self.emit(Instr::Load {
			dst,
			src: left,
		});
		let decided = self.em.label()?;
		let cond = Operand::Reg(dst);
		self.emit(match op {
			ShortCircuitOp::And => Instr::JumpIf {
				cond,
				truthy: false,
				target: decided,
			},
			ShortCircuitOp::NullCoalescing => Instr::JumpIfNullish {
				cond,
				nullish: false,
				target: decided,
			},
			ShortCircuitOp::Or | ShortCircuitOp::TernaryCondition => Instr::JumpIf {
				cond,
				truthy: true,
				target: decided,
			},
		});
		let right = self.region_value(right)?;
		self.emit(Instr::Load {
			dst,
			src: right,
		});
		self.em.bind(decided)?;
		Ok(Operand::Reg(dst))
	}

	/// The value of the region of the arm the condition selects.
	fn if_else(&mut self, data: &OpData) -> Result<Operand, IrError> {
		let [condition] = exactly::<1>(data)?;
		let then = region_at(data, 0, 2)?;
		let otherwise = region_at(data, 1, 2)?;
		let dst = self.em.reg()?;
		let cond = self.value(condition)?;
		let else_arm = self.em.label()?;
		let end = self.em.label()?;
		self.emit(Instr::JumpIf {
			cond,
			truthy: false,
			target: else_arm,
		});
		let value = self.region_value(then)?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		self.emit(Instr::Jump {
			target: end,
		});
		self.em.bind(else_arm)?;
		let value = self.region_value(otherwise)?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		self.em.bind(end)?;
		Ok(Operand::Reg(dst))
	}

	/// The truthiness of the operand as a boolean.
	fn truthy(&mut self, data: &OpData) -> Result<Operand, IrError> {
		let [subject] = exactly::<1>(data)?;
		let cond = self.value(subject)?;
		let yes = self.em.constant(Value::Bool(true))?;
		let no = self.em.constant(Value::Bool(false))?;
		let dst = self.em.reg()?;
		let falsy = self.em.label()?;
		let end = self.em.label()?;
		self.emit(Instr::JumpIf {
			cond,
			truthy: false,
			target: falsy,
		});
		self.emit(Instr::Load {
			dst,
			src: Operand::Const(yes),
		});
		self.emit(Instr::Jump {
			target: end,
		});
		self.em.bind(falsy)?;
		self.emit(Instr::Load {
			dst,
			src: Operand::Const(no),
		});
		self.em.bind(end)?;
		Ok(Operand::Reg(dst))
	}

	/// The value `region` yields, lowered in a scope of its own.
	///
	/// The region's block ends in `expr.yield` of one value or in a terminator that raises. An op
	/// of the block that nothing reads runs where it stands: it and every op before it are
	/// lowered in program order before the terminator, and each other op runs where its reader
	/// needs it. A region that raises yields a register nothing reads.
	///
	/// A block argument of the region is read from where [`Lowerer::bind_arg`] and
	/// [`Lowerer::bind_row`] said before the call.
	///
	/// A region that a `RETURN` held in it ends ([`ends_context`]) is lowered as the evaluation of
	/// a context ([`Lowerer::context_value`]): its value is the `RETURN`'s where one is signalled.
	pub(super) fn region_value(&mut self, region: RegionId) -> Result<Operand, IrError> {
		let held = self.scope.clone();
		let outer = self.region.replace(region);
		let lowered = match ends_context(self.ir, region) {
			true => self.context_value(|lowerer| lowerer.region_body(region)),
			false => self.region_body(region),
		};
		self.region = outer;
		self.scope = held;
		lowered
	}

	/// [`Lowerer::region_value`] of a region whose block arguments are read from `args`, in order.
	///
	/// The region holds exactly as many arguments as `args` names, which the op that owns the
	/// region is built with. The arguments are read from `args` in the region alone.
	pub(super) fn region_value_with(
		&mut self,
		region: RegionId,
		args: &[Operand],
	) -> Result<Operand, IrError> {
		let params = self.region_args(region)?;
		if params.len() != args.len() {
			return Err(IrError::Stale("ir_lower::expr: region argument arity"));
		}
		let held = self.scope.clone();
		for (param, operand) in params.iter().zip(args) {
			self.scope.values.insert(*param, *operand);
		}
		let lowered = self.region_value(region);
		self.scope = held;
		lowered
	}

	/// [`Lowerer::region_value_with`] of an element body: the region's first argument is read from
	/// `element`, which is the element of the body while it is lowered, and the rest from `rest`.
	/// The element that held before is back afterwards, on every path out.
	pub(super) fn element_value(
		&mut self,
		region: RegionId,
		element: Reg,
		rest: &[Operand],
	) -> Result<Operand, IrError> {
		let mut args = Vec::with_capacity(rest.len() + 1);
		args.push(Operand::Reg(element));
		args.extend_from_slice(rest);
		let outer = self.element.replace(element);
		let lowered = self.region_value_with(region, &args);
		self.element = outer;
		lowered
	}

	/// The register that holds the element of the element body being lowered, if any.
	pub(super) fn element(&self) -> Option<Reg> {
		self.element
	}

	/// The value `value`, lowered as a value of `region`: the ops of the region it depends on are
	/// lowered where they are read, and the ones it does not depend on are not lowered. A block
	/// argument of the region is read from where [`Lowerer::bind_arg`] said before the call.
	///
	/// A region that a `RETURN` held in it ends ([`ends_context`]) is lowered as the evaluation of
	/// a context ([`Lowerer::context_value`]), as [`Lowerer::region_value`] lowers one.
	pub(super) fn region_operand(
		&mut self,
		region: RegionId,
		value: ValueId,
	) -> Result<Operand, IrError> {
		let held = self.scope.clone();
		let outer = self.region.replace(region);
		let lowered = match ends_context(self.ir, region) {
			true => self.context_value(|lowerer| lowerer.value(value)),
			false => self.value(value),
		};
		self.region = outer;
		self.scope = held;
		lowered
	}

	/// The block arguments of the first block of `region`.
	pub(super) fn region_args(&self, region: RegionId) -> Result<Vec<ValueId>, IrError> {
		let ir = self.ir;
		let block = ir
			.region(region)
			.and_then(|data| data.blocks.first().copied())
			.and_then(|block| ir.block(block))
			.ok_or(IrError::Stale("ir_lower::expr: region block"))?;
		Ok(block.args.to_vec())
	}

	/// Lowers the terminator `op` of a region that raises: the instruction that raises, after the
	/// value it raises.
	///
	/// `expr.throw` raises its operand. `expr.raise` of a loop control raises the error such a
	/// control raises where no loop takes it. A refusal (a statement the planner refuses when it is
	/// reached, the whole body of a unit included) raises its error through
	/// [`StoreInstr::Refuse`] when the code reaches it, as a statement list raises it. A misplaced
	/// transaction control is a step of the request driver, so one reaching a program answers
	/// [`IrError::Invariant`]. An error a fold left to be raised when reached answers an
	/// invariant: error folding is off, so none is built. `op` is a throw or a raise: its callers
	/// match the terminator first.
	pub(super) fn raise_terminator(&mut self, op: OpId) -> Result<(), IrError> {
		let ir = self.ir;
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::expr: op"))?;
		match &data.kind {
			OpKind::Expr(ExprOp::Throw) => {
				self.require_frame(data)?;
				let [thrown] = exactly::<1>(data)?;
				let src = self.value(thrown)?;
				self.owners.push(op);
				self.emit(Instr::Throw {
					src,
				});
				self.owners.pop();
				Ok(())
			}
			OpKind::Expr(ExprOp::Raise(kind)) => match &**kind {
				RaiseKind::InvalidControlFlow => {
					self.require_frame(data)?;
					exactly::<0>(data)?;
					self.owners.push(op);
					self.emit(Instr::InvalidControlFlow);
					self.owners.pop();
					Ok(())
				}
				RaiseKind::Folded(_) => Err(invariant(
					"expr.raise of a folded error has no producer: error folding is off",
				)),
				RaiseKind::Misplaced(_) => Err(invariant(
					"a misplaced transaction control is run by the request driver, never lowered as a program",
				)),
				// Emitted with no owner, as a statement list emits it: the store step's class is
				// not the raise's claim, and the refusal reads nothing of the row.
				RaiseKind::Refused(refusal) => {
					exactly::<0>(data)?;
					self.em.emit(Instr::Store(StoreInstr::Refuse {
						refusal: refusal.clone(),
					}));
					Ok(())
				}
			},
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
				Err(invariant("a raise terminator that is neither expr.throw nor expr.raise"))
			}
		}
	}

	/// The scalar the host evaluates for `value`, outside the body that names it: a paging clause,
	/// the record a source names, a write's targets. A literal is the constant it is, and any
	/// other value is a host pipeline that emits it.
	pub(super) fn host_scalar(&mut self, value: ValueId) -> Result<HostScalar, IrError> {
		let ir = self.ir;
		let literal = match ir.value(value).map(|data| data.def) {
			Some(ValueDef::OpResult {
				op,
				index: 0,
			}) => match ir.op(op).map(|data| &data.kind) {
				Some(OpKind::Expr(ExprOp::Literal(literal))) => Some((**literal).clone()),
				Some(OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_)) | None => None,
			},
			Some(ValueDef::OpResult {
				..
			}) => None,
			Some(ValueDef::BlockArg {
				..
			}) => None,
			None => return Err(IrError::Stale("ir_lower::expr: value")),
		};
		if let Some(constant) = literal {
			let constant = ValueData::new(constant, "a host scalar")
				.map_err(|_| IrError::Stale("ir_lower::expr: a host scalar"))?;
			return Ok(HostScalar::Const(constant));
		}
		Ok(HostScalar::Code(self.host_code(value)?))
	}

	/// The host pipeline, of the program's host pipelines, that emits `value` once, over no row.
	///
	/// The pipeline is built beside the one in flight from the ops that define `value`, and shares
	/// only the program's pools with it: it holds no register of the pipeline in flight, so every
	/// value it reads is lowered afresh in it.
	pub(super) fn host_code(&mut self, value: ValueId) -> Result<u16, IrError> {
		let aside = self.em.set_aside();
		let beside = self.beside();
		let built = self.value(value).and_then(|operand| self.emit_value_pipeline(operand));
		self.rejoin(beside);
		self.em.restore(aside);
		self.em.host_code(built?)
	}

	/// The host pipeline, of the program's host pipelines, that emits the value `region` yields
	/// once, over the row the host hands it, which the region's one block argument names.
	///
	/// The pipeline is built beside the one in flight and shares only the program's pools with it.
	/// The row is an object row of its own: a record id is fetched before it is read.
	pub(super) fn host_code_over_row(&mut self, region: RegionId) -> Result<u16, IrError> {
		let [row] = self.region_args(region)?[..] else {
			return Err(IrError::Stale("ir_lower::expr: a host region takes the row"));
		};
		let aside = self.em.set_aside();
		let beside = self.beside();
		self.bind_row(row, true);
		let built = self.region_value(region).and_then(|value| self.emit_value_pipeline(value));
		self.rejoin(beside);
		self.em.restore(aside);
		self.em.host_code(built?)
	}

	/// Ends the pipeline built beside the one in flight, as the pipeline of one row that emits
	/// `value`: a pipeline over no source, which runs once and serially.
	pub(super) fn emit_value_pipeline(&mut self, value: Operand) -> Result<Pipeline, IrError> {
		let dst = self.em.reg()?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		self.emit(Instr::Emit {
			src: dst,
		});
		self.emit(Instr::EndRow);
		self.em.finish_pipeline(Source::Once)
	}

	/// The value `region` yields, lowered in the scope being lowered: the region of an op that
	/// runs where it stands, whose ops are evaluated as the enclosing scope's.
	pub(super) fn eager_region_value(&mut self, region: RegionId) -> Result<Operand, IrError> {
		self.region_body(region)
	}

	fn region_body(&mut self, region: RegionId) -> Result<Operand, IrError> {
		let ir = self.ir;
		let ops: Vec<_> = ops_in(ir, region).collect();
		// The physical boundary's verification holds every region a value is lowered from to one
		// block that ends in its one terminator.
		let Some((last, body)) = ops.split_last() else {
			return Err(invariant("a value region with no terminator"));
		};
		// An op nothing reads (an operand evaluated before a sibling diverged, as in
		// `[fn::f(), THROW 'x']`) runs for its effect where it stands. Every op up to the last of
		// them is lowered in program order before the terminator, so they run in the order
		// written; each op after the last unread one runs where its reader needs it.
		let unread = body.iter().rposition(|id| {
			ir.op(*id).is_some_and(|data| {
				data.results.iter().all(|result| ir.uses(*result).is_empty())
					&& !matches!(&data.kind, OpKind::Expr(ExprOp::Literal(_)))
			})
		});
		if let Some(through) = unread {
			for id in &body[..=through] {
				let data = ir.op(*id).ok_or(IrError::Stale("ir_lower::expr: op"))?;
				// The verifier holds an op of no result to the end of its block: only a
				// terminator defines no value.
				let Some(result) = data.results.first() else {
					return Err(invariant("an op of a value region that defines no value"));
				};
				self.value(*result)?;
			}
		}
		let terminator = ir.op(*last).ok_or(IrError::Stale("ir_lower::expr: op"))?;
		let value = match (&terminator.kind, terminator.operands.as_slice()) {
			(OpKind::Expr(ExprOp::Yield) | OpKind::Phys(PhysOp::Yield), [yielded]) => {
				self.value(*yielded)?
			}
			(OpKind::Expr(ExprOp::Throw | ExprOp::Raise(_)), _) => {
				self.raise_terminator(*last)?;
				Operand::Reg(self.em.reg()?)
			}
			(OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_), _) => {
				return not_yet("ir_lower::expr: a region that does not yield one value");
			}
		};
		Ok(value)
	}

	/// A read of the parameter `name`, resolved by the VM when the program runs: the request's own
	/// bindings and the session first, then the stored parameter, then `NONE`.
	fn load_param(&mut self, data: &OpData, name: &Strand) -> Result<Operand, IrError> {
		self.require_frame(data)?;
		exactly::<0>(data)?;
		let param = self.em.param(ParamSpec {
			name: name.clone(),
			guard: None,
		})?;
		let dst = self.em.reg()?;
		self.emit(Instr::LoadParam {
			param,
			dst,
			frame: self.frame.program,
		});
		Ok(Operand::Reg(dst))
	}

	/// The record id of `attrs`. A key of literal parts is a constant. Any other key is built by
	/// an `Instr::RecordId` whose spec names the operands that hold its computed parts, which are
	/// evaluated in the order the key names them.
	fn record_id(
		&mut self,
		data: &OpData,
		attrs: &crate::exec::ir::expr::ops::RecordIdAttrs,
	) -> Result<Operand, IrError> {
		let fixed = match &attrs.key_shape {
			KeyShape::Number(n) => Some(RecordIdKey::Number(*n)),
			KeyShape::String(s) => Some(RecordIdKey::String(s.clone())),
			KeyShape::Uuid(u) => Some(RecordIdKey::Uuid(*u)),
			KeyShape::Generate(_)
			| KeyShape::Array {
				..
			}
			| KeyShape::Object {
				..
			}
			| KeyShape::Range(_) => None,
		};
		if let Some(key) = fixed {
			exactly::<0>(data)?;
			let value = Value::RecordId(RecordId::new(attrs.table.clone(), key));
			return Ok(Operand::Const(self.em.constant(value)?));
		}
		// A generated key draws entropy, which the folder's eligibility check refuses. A key built
		// of computed parts holds no entropy and folds as the operand subset lowers it.
		if key_generates(&attrs.key_shape) {
			self.refuse_in_fold("ir_lower::fold: expr.record_id with a generated key")?;
		}
		let mut operands = data.operands.iter().copied();
		let key = self.record_key(&attrs.key_shape, &mut operands)?;
		if operands.next().is_some() {
			return Err(IrError::Stale(
				"ir_lower::expr: a record id holds an operand its key does not use",
			));
		}
		let record = self.em.record_id(RecordIdSpec {
			table: attrs.table.clone(),
			key,
		})?;
		let dst = self.em.reg()?;
		self.emit(Instr::RecordId {
			record,
			dst,
		});
		Ok(Operand::Reg(dst))
	}

	/// The key `shape` names, the instructions that compute its parts emitted in the order the
	/// parts are evaluated. `operands` yields the values the shape names, in order.
	///
	/// An array key is the array of its elements and an object key the object of its values, each
	/// built in a register beforehand; a range holds the keys of its bounds.
	pub(super) fn record_key(
		&mut self,
		shape: &KeyShape,
		operands: &mut std::iter::Copied<std::slice::Iter<'_, ValueId>>,
	) -> Result<RecordKey, IrError> {
		use RecordKey as Key;
		let fixed = |key: RecordIdKey| {
			ValueData::new(key, "a record key")
				.map(Key::Fixed)
				.map_err(|_| IrError::Stale("ir_lower::expr: a record key"))
		};
		Ok(match shape {
			KeyShape::Number(n) => fixed(RecordIdKey::Number(*n))?,
			KeyShape::String(s) => fixed(RecordIdKey::String(s.clone()))?,
			KeyShape::Uuid(u) => fixed(RecordIdKey::Uuid(*u))?,
			KeyShape::Generate(generator) => Key::Generate(generator.clone()),
			KeyShape::Array {
				len,
			} => {
				let array = self.em.reg()?;
				self.emit(Instr::NewArray {
					dst: array,
				});
				for _ in 0..*len {
					let item = operands.next().ok_or(IrError::Stale(
						"ir_lower::expr: a record id key lacks an element",
					))?;
					let value = self.value(item)?;
					self.emit(Instr::ArrayPush {
						array,
						value,
					});
				}
				Key::Array(Operand::Reg(array))
			}
			KeyShape::Object {
				keys,
			} => {
				let object = self.em.reg()?;
				self.emit(Instr::NewObject {
					dst: object,
				});
				for key in keys.iter() {
					let item = operands
						.next()
						.ok_or(IrError::Stale("ir_lower::expr: a record id key lacks a value"))?;
					let value = self.value(item)?;
					let path = self.em.path(FieldPath::field(key.as_str()))?;
					self.emit(Instr::SetField {
						object,
						path,
						value,
					});
				}
				Key::Object(Operand::Reg(object))
			}
			KeyShape::Range(range) => {
				let start = self.key_bound(&range.start, operands)?;
				let end = self.key_bound(&range.end, operands)?;
				Key::Range(Box::new(start), Box::new(end))
			}
		})
	}

	/// The key at one end of a range, which holds the operands of its parts.
	fn key_bound(
		&mut self,
		bound: &Bound<KeyBound>,
		operands: &mut std::iter::Copied<std::slice::Iter<'_, ValueId>>,
	) -> Result<Bound<RecordKey>, IrError> {
		let (key, wrap): (_, fn(RecordKey) -> Bound<RecordKey>) = match bound {
			Bound::Unbounded => return Ok(Bound::Unbounded),
			Bound::Included(key) => (key, Bound::Included),
			Bound::Excluded(key) => (key, Bound::Excluded),
		};
		let shape = match key {
			KeyBound::Number(n) => KeyShape::Number(*n),
			KeyBound::String(s) => KeyShape::String(s.clone()),
			KeyBound::Uuid(u) => KeyShape::Uuid(*u),
			KeyBound::Generate(generator) => KeyShape::Generate(generator.clone()),
			KeyBound::Array {
				len,
			} => KeyShape::Array {
				len: *len,
			},
			KeyBound::Object {
				keys,
			} => KeyShape::Object {
				keys: keys.clone(),
			},
		};
		Ok(wrap(self.record_key(&shape, operands)?))
	}

	/// The op `data` defines, the op being lowered.
	fn owner(&self) -> Result<OpId, IrError> {
		self.owners.last().copied().ok_or(IrError::Stale("ir_lower::expr: the op being lowered"))
	}

	/// The `MATCHES` operator being lowered, over `left` and `right`, as the physical plan decided
	/// it ([`crate::exec::ir::phys::matches`]): [`Instr::Matches`] judging the row against the
	/// operator's spec. An operator no select loop decided is `false`, the answer for an operator
	/// no scope holds.
	///
	/// The probe is the left operand's field path and the right operand's query: a literal's text,
	/// or, for a parameter or any other expression the plan resolves ([`resolved_query`]), the
	/// value the instruction reads when it runs. Any other right operand makes no probe.
	/// An operand an error cannot name is shown as the planner rebuilds it, or else the operator is
	/// shown as the statement wrote it. A left operand that probes a position is read for the rows
	/// no index answers for: a path off the row by a host pipeline the operator runs only then,
	/// and a value the container is handed as the instruction's `left` operand.
	fn matches(
		&mut self,
		operator: &MatchesOperator,
		left: ValueId,
		right: ValueId,
	) -> Result<Operand, IrError> {
		let ir = self.ir;
		let owner = self.owner()?;
		let Some(attr) = ir.matches_attr(owner) else {
			self.unread(left, owner)?;
			self.unread(right, owner)?;
			return Ok(Operand::Const(self.em.constant(Value::Bool(false))?));
		};
		let (registered, executor_tables) = (attr.registered, attr.executor_tables.clone());
		let left_expr = path_expr(ir, left);
		// A left operand the container is handed as a value reads the position the decision
		// resolved. Only a single field: a longer path crosses a record link, which is read off
		// the row, and the handed value is not the row.
		let decided = match &left_expr {
			Some(_) => None,
			None => attr.field.clone().filter(|path| path.segments().len() == 1),
		};
		let path = left_expr.is_some() || decided.is_some();
		let field = left_expr.as_ref().and_then(LogicalExpr::field_path).or(decided);
		let (probe, right_expr, read_query) = match (path, query_operand(ir, right)) {
			(path, QueryOperand::Literal(value)) => {
				let query = value.to_raw_string();
				// Errors name the query the index searches for, which a probe resolves.
				let shown = if path {
					Some(LogicalExpr::Literal(LogicalLiteral::String(query.as_str().into())))
				} else {
					crate::exec::planner::util::try_value_to_logical_expr(value)
				};
				let probe = path.then(|| MatchProbe {
					field: field.clone(),
					query: Some(query),
				});
				(probe, shown, false)
			}
			(path, QueryOperand::Named(name)) => {
				let shown = name.map(|name| LogicalExpr::Param(crate::expr::Param::from(name)));
				let probe = path.then(|| MatchProbe {
					field: field.clone(),
					query: None,
				});
				(probe, shown, path)
			}
			(path, QueryOperand::Other)
				if path
					&& resolved_query(
						ir,
						right,
						path_row(ir, left),
						self.units.as_deref().is_none_or(|units| units.reads_checked()),
					) =>
			{
				let probe = MatchProbe {
					field,
					query: None,
				};
				(Some(probe), shown_expr(ir, right), true)
			}
			(_, QueryOperand::Other) => (None, shown_expr(ir, right), false),
		};
		let left_is_path = left_expr.is_some();
		let left_shown = left_expr.or_else(|| shown_expr(ir, left));
		let sql = match (left_shown, right_expr, subquery_root(self.units.as_deref(), ir, right)) {
			// A statement operand is named by the operator at the top of the plan `EXPLAIN`
			// renders for it, as an expression holding the statement shows it.
			(Some(left), _, Some(root)) => format!(
				"{} {} ({root})",
				left.to_sql(),
				BinaryOperator::Matches(operator.clone()).to_sql()
			),
			(Some(left), Some(right), None) => LogicalExpr::Binary {
				left: Box::new(left),
				op: BinaryOperator::Matches(operator.clone()),
				right: Box::new(right),
			}
			.to_sql(),
			_ => rebuilt_sql(ir, owner)
				.or_else(|| ir.op(owner).and_then(|data| data.loc.text()).map(str::to_owned))
				.unwrap_or_else(|| BinaryOperator::Matches(operator.clone()).to_sql()),
		};
		// The operand is read for a row no index answers for — one a full table
		// source did not produce — and for a path that crosses a record link,
		// whatever the row. A path is read off the row by a host pipeline; a
		// value the container is handed holds the operand already, and is read
		// as it is.
		let (left_code, left_value) =
			match (left_is_path, probe.as_ref().and_then(|p| p.field.as_ref())) {
				(true, Some(_)) => match path_row(ir, left) {
					Some(row) => (Some(self.host_code_reading_row(left, row)?), None),
					None => (None, None),
				},
				(false, Some(_)) => (None, Some(self.value(left)?)),
				(_, None) => (None, None),
			};
		let query = if read_query {
			Some(self.value(right)?)
		} else {
			self.unread(right, owner)?;
			None
		};
		self.unread(left, owner)?;
		let matches = self.em.matches(MatchesSpec {
			operator: operator.clone(),
			probe,
			registered,
			executor_tables,
			sql,
			left: left_code,
		})?;
		self.keeps_row_order = true;
		let dst = self.em.reg()?;
		self.emit(Instr::Matches {
			matches,
			query,
			left: left_value,
			dst,
			frame: self.frame.program,
		});
		Ok(Operand::Reg(dst))
	}

	/// The call of index function `name` being lowered, over `args`, bound to the clause or
	/// operator of the scope the physical plan states for it
	/// ([`crate::exec::ir::phys::matches::IndexScope`]): [`Instr::CallIndexFn`]. The reference
	/// argument is read here, not evaluated: it is the clause's number, a literal in range. A
	/// clause whose query is a name bound around the statement hands the call its value as the
	/// last argument. A call that names no clause of its scope is refused for the statement
	/// ([`Lowerer::unbound_index_call`]).
	fn index_call(&mut self, name: &str, args: &[ValueId]) -> Result<Operand, IrError> {
		use super::super::program::{ExtraQuery, IndexBinding};
		use crate::exec::function::IndexContextKind;
		use crate::exec::ir::phys::matches::ClauseQuery;
		let ir = self.ir;
		let Some(function) = self.registry.get_index_function(name) else {
			return Err(IrError::Stale("ir_lower::expr: an index function"));
		};
		let Some(scope) = ir.index_scope(self.owner()?) else {
			return Err(self.unbound_index_call(
				name,
				args,
				None,
				"ir_lower::expr: expr.call of an index function no clause covers",
			));
		};
		let mut args: Vec<ValueId> = args.to_vec();
		let mut query_operands: Vec<Query> = Vec::new();
		let binding = match function.index_context_kind() {
			IndexContextKind::FullText => {
				let Some(at) = function.index_ref_arg_index().filter(|at| *at < args.len()) else {
					return Err(self.unbound_index_call(
						name,
						&args,
						Some(scope),
						"ir_lower::expr: an index function without its reference",
					));
				};
				let written = args.clone();
				let reference = args.remove(at);
				self.unread(reference, self.owner()?)?;
				let rf = match query_operand(ir, reference) {
					QueryOperand::Literal(Value::Number(number)) => match number {
						crate::val::Number::Int(n) if (0..=255).contains(&n) => n as u8,
						crate::val::Number::Float(n)
							if (0.0..=255.0).contains(&n) && n.fract() == 0.0 =>
						{
							n as u8
						}
						crate::val::Number::I8(n) if n >= 0 => n as u8,
						crate::val::Number::U8(n) => n,
						crate::val::Number::I16(n) if (0..=255).contains(&n) => n as u8,
						crate::val::Number::U16(n) if n <= 255 => n as u8,
						crate::val::Number::I32(n) if (0..=255).contains(&n) => n as u8,
						crate::val::Number::U32(n) if n <= 255 => n as u8,
						crate::val::Number::U64(n) if n <= 255 => n as u8,
						crate::val::Number::F16(n)
							if (0.0..=255.0).contains(&n.to_f32()) && n.to_f32().fract() == 0.0 =>
						{
							n.to_f32() as u8
						}
						crate::val::Number::F32(n)
							if (0.0..=255.0).contains(&n) && n.fract() == 0.0 =>
						{
							n as u8
						}
						crate::val::Number::Int(_)
						| crate::val::Number::Float(_)
						| crate::val::Number::Decimal(_)
						| crate::val::Number::BigDecimal(_)
						| crate::val::Number::I8(_)
						| crate::val::Number::I16(_)
						| crate::val::Number::U16(_)
						| crate::val::Number::I32(_)
						| crate::val::Number::U32(_)
						| crate::val::Number::U64(_)
						| crate::val::Number::F16(_)
						| crate::val::Number::F32(_) => {
							return Err(self.unbound_index_call(
								name,
								&written,
								Some(scope),
								"ir_lower::expr: an index reference out of range",
							));
						}
					},
					QueryOperand::Literal(_) | QueryOperand::Named(_) | QueryOperand::Other => {
						return Err(self.unbound_index_call(
							name,
							&written,
							Some(scope),
							"ir_lower::expr: an index reference that is not a literal",
						));
					}
				};
				let Some((clause, others)) = scope.resolve_union(rf) else {
					return Err(self.unbound_index_call(
						name,
						&written,
						Some(scope),
						"ir_lower::expr: an index reference no clause declares",
					));
				};
				// The other clauses come first among the trailing arguments, so the primary
				// clause's argument, if it has one, is the last.
				let mut extra = Vec::with_capacity(others.len());
				for other in others {
					match &other.query {
						ClauseQuery::Text(text) => extra.push(ExtraQuery::Text(text.clone())),
						ClauseQuery::Param(param) => {
							query_operands.push(Query::Param(param.clone()));
							extra.push(ExtraQuery::Arg);
						}
						ClauseQuery::Bound(value) => {
							query_operands.push(Query::Bound(*value));
							extra.push(ExtraQuery::Arg);
						}
					}
				}
				let (text, query_arg) = match &clause.query {
					ClauseQuery::Text(text) => (text.clone(), false),
					ClauseQuery::Param(param) => {
						query_operands.push(Query::Param(param.clone()));
						(String::new(), true)
					}
					ClauseQuery::Bound(value) => {
						query_operands.push(Query::Bound(*value));
						(String::new(), true)
					}
				};
				IndexBinding::FullText {
					table: scope
						.table
						.clone()
						.unwrap_or_else(|| crate::val::TableName::from("unknown".to_owned())),
					path: clause.field.clone(),
					query: text,
					query_arg,
					extra,
				}
			}
			IndexContextKind::Knn => {
				if !scope.knn {
					return Err(self.unbound_index_call(
						name,
						&args,
						Some(scope),
						"ir_lower::expr: a KNN index function no KNN operator covers",
					));
				}
				if let Some(at) = function.index_ref_arg_index()
					&& at < args.len()
				{
					let reference = args.remove(at);
					self.unread(reference, self.owner()?)?;
				}
				IndexBinding::Knn {
					slot: self.em.knn_slot(),
				}
			}
		};
		self.keeps_row_order = true;
		let call = self.em.call(CallSpec {
			name: name.to_owned(),
			plan_depth: self.depth,
			index: Some(binding),
			closure_args: Vec::new(),
		})?;
		self.emit(Instr::CallAllowed {
			call,
			frame: self.frame.program,
		});
		let mut operands = self.values(&args)?;
		for query in query_operands {
			match query {
				Query::Param(name) => {
					let param = self.em.param(ParamSpec {
						name,
						guard: None,
					})?;
					let dst = self.em.reg()?;
					self.emit(Instr::LoadParam {
						param,
						dst,
						frame: self.frame.program,
					});
					operands.push(Operand::Reg(dst));
				}
				Query::Bound(value) => operands.push(self.value(value)?),
			}
		}
		let dst = self.em.reg()?;
		self.emit(Instr::CallIndexFn {
			call,
			args: operands,
			dst,
			frame: self.frame.program,
		});
		Ok(Operand::Reg(dst))
	}

	/// The error for the call of index function `name` over `args` (as written, its reference
	/// among them) that names no clause of `scope`, the scope the physical plan states for it:
	/// the refusal the statement is answered with, which reads as the planner's error for an
	/// index reference that is no literal in range, a statement whose `WHERE` holds no `MATCHES`
	/// clause or `KNN` operator, or no `MATCHES` clause with the reference. A call in a closure
	/// body over a row or a permission frame is not refused: `what` names the lowering that does
	/// not cover it.
	fn unbound_index_call(
		&self,
		name: &str,
		args: &[ValueId],
		scope: Option<&crate::exec::ir::phys::matches::IndexScope>,
		what: &'static str,
	) -> IrError {
		use crate::exec::function::IndexContextKind;
		if self.closure_row || self.frame.program.is_some() {
			return IrError::NotYet {
				what,
			};
		}
		let Some(function) = self.registry.get_index_function(name) else {
			return IrError::Stale("ir_lower::expr: an index function");
		};
		let message = match function.index_context_kind() {
			IndexContextKind::Knn => {
				format!("Index function '{name}': no KNN operator found in WHERE condition")
			}
			IndexContextKind::FullText => {
				let rf = function.index_ref_arg_index().and_then(|at| args.get(at)).and_then(
					|reference| match query_operand(self.ir, *reference) {
						QueryOperand::Literal(Value::Number(number)) => match number {
							crate::val::Number::Int(n) if (0..=255).contains(&n) => Some(n as u8),
							crate::val::Number::Float(n)
								if (0.0..=255.0).contains(&n) && n.fract() == 0.0 =>
							{
								Some(n as u8)
							}
							crate::val::Number::I8(n) if n >= 0 => Some(n as u8),
							crate::val::Number::U8(n) => Some(n),
							crate::val::Number::I16(n) if (0..=255).contains(&n) => Some(n as u8),
							crate::val::Number::U16(n) if n <= 255 => Some(n as u8),
							crate::val::Number::I32(n) if (0..=255).contains(&n) => Some(n as u8),
							crate::val::Number::U32(n) if n <= 255 => Some(n as u8),
							crate::val::Number::U64(n) if n <= 255 => Some(n as u8),
							crate::val::Number::F16(n)
								if (0.0..=255.0).contains(&n.to_f32())
									&& n.to_f32().fract() == 0.0 =>
							{
								Some(n.to_f32() as u8)
							}
							crate::val::Number::F32(n)
								if (0.0..=255.0).contains(&n) && n.fract() == 0.0 =>
							{
								Some(n as u8)
							}
							crate::val::Number::Int(_)
							| crate::val::Number::Float(_)
							| crate::val::Number::Decimal(_)
							| crate::val::Number::BigDecimal(_)
							| crate::val::Number::I8(_)
							| crate::val::Number::I16(_)
							| crate::val::Number::U16(_)
							| crate::val::Number::I32(_)
							| crate::val::Number::U32(_)
							| crate::val::Number::U64(_)
							| crate::val::Number::F16(_)
							| crate::val::Number::F32(_) => None,
						},
						QueryOperand::Literal(_) | QueryOperand::Named(_) | QueryOperand::Other => {
							None
						}
					},
				);
				let mut available: Vec<u8> = scope
					.map(|scope| scope.clauses.iter().map(|clause| clause.rf).collect())
					.unwrap_or_default();
				available.sort_unstable();
				available.dedup();
				match rf {
					None => format!(
						"Index function '{name}': index_ref argument must be a literal integer in range 0..255"
					),
					Some(_) if available.is_empty() => format!(
						"Index function '{name}': no MATCHES clause found in WHERE condition"
					),
					Some(rf) => format!(
						"Index function '{name}': no MATCHES clause found for match_ref {rf} (available: {:?})",
						available.iter().collect::<Vec<_>>()
					),
				}
			}
		};
		IrError::Rejected(Box::new(crate::exec::vm::payload::Refusal::Query {
			message,
		}))
	}

	/// The `KNN` operator being lowered, which no search answers whole. Neither operand is
	/// evaluated. Where no search is in scope for it the operator is `false`. Where one is, the
	/// operator holds for a row whose record a search of the statement yielded before the row was
	/// read, unless the plan marks it unsearched (a write whose subjects no search reads), which
	/// has no instruction. The check is a
	/// [`Instr::CallIndexFn`] bound to the statement's distance slot
	/// ([`IndexBinding::KnnHolds`](super::super::program::IndexBinding::KnnHolds)).
	fn nearest(&mut self, left: ValueId, right: ValueId) -> Result<Operand, IrError> {
		let owner = self.owner()?;
		if self.ir.knn_unsearched(owner) {
			return not_yet("ir_lower::expr: a KNN operator no search answers");
		}
		self.unread(left, owner)?;
		self.unread(right, owner)?;
		if self.ir.knn_scoped(owner) {
			let call = self.em.call(CallSpec {
				name: "vector::distance::knn".to_owned(),
				plan_depth: self.depth,
				index: Some(super::super::program::IndexBinding::KnnHolds {
					slot: self.em.knn_slot(),
				}),
				closure_args: Vec::new(),
			})?;
			self.keeps_row_order = true;
			let dst = self.em.reg()?;
			self.emit(Instr::CallIndexFn {
				call,
				args: Vec::new(),
				dst,
				frame: self.frame.program,
			});
			return Ok(Operand::Reg(dst));
		}
		Ok(Operand::Const(self.em.constant(Value::Bool(false))?))
	}

	/// Marks `value`, an operand `consumer` reads without evaluating it, as lowered, and so each
	/// operand of the op defining it that only that op reads: no instruction computes them, and a
	/// region holding them still accounts for every op it holds. A literal is the constant it is.
	/// A value something else reads is left for that reader to lower.
	fn unread(&mut self, value: ValueId, consumer: OpId) -> Result<(), IrError> {
		let ir = self.ir;
		if self.scope.values.contains_key(&value)
			|| !ir.uses(value).iter().all(|used| used.user == consumer)
		{
			return Ok(());
		}
		let Some(ValueDef::OpResult {
			op,
			..
		}) = ir.value(value).map(|data| data.def)
		else {
			return Ok(());
		};
		let Some(data) = ir.op(op) else {
			return Err(IrError::Stale("ir_lower::expr: op"));
		};
		if let OpKind::Expr(ExprOp::Literal(literal)) = &data.kind {
			let index = self.em.constant((**literal).clone())?;
			self.scope.values.insert(value, Operand::Const(index));
			return Ok(());
		}
		if !data.regions.is_empty() || data.results.len() != 1 {
			return Ok(());
		}
		let none = self.em.constant(Value::None)?;
		self.scope.values.insert(value, Operand::Const(none));
		for operand in data.operands.clone() {
			self.unread(operand, op)?;
		}
		Ok(())
	}

	/// The host pipeline, of the program's host pipelines, that emits `value` once over the row
	/// the host hands it, which the block argument `row` names.
	fn host_code_reading_row(&mut self, value: ValueId, row: ValueId) -> Result<u16, IrError> {
		let aside = self.em.set_aside();
		let beside = self.beside();
		self.bind_row(row, true);
		let built = self.value(value).and_then(|value| self.emit_value_pipeline(value));
		self.rejoin(beside);
		self.em.restore(aside);
		self.em.host_code(built?)
	}

	/// The call `data` of the function `call` names.
	fn call(&mut self, data: &OpData, call: &CallAttrs) -> Result<Operand, IrError> {
		let args = call_args(data, call)?;
		match &call.function {
			Function::Normal(name) => {
				if self.registry.is_index_function(name) {
					return self.index_call(name, args);
				}
				// The plan-time folder never runs a builtin that runs statements.
				if crate::expr::method::is_writer_builtin(name) {
					self.refuse_in_fold("ir_lower::fold: expr.call of a builtin that writes")?;
				}
				if let Some(counted) = self.degree_count(name, args)? {
					return Ok(counted);
				}
				if let Some(measured) = self.measure_call(name, args)? {
					return Ok(measured);
				}
				self.builtin_call(name, args)
			}
			Function::Custom(name) => self.user_call(data, call, name),
			Function::Script(script) => self.script_call(data, call, &script.0),
			Function::Model(model) => self.host_call(
				data,
				call,
				ExternalCallee::Model {
					name: model.name.to_string(),
					version: model.version.to_string(),
				},
			),
			Function::Module(module, sub, _) => self.host_call(
				data,
				call,
				ExternalCallee::Module {
					storage: format!("mod::{module}"),
					sub: sub.clone(),
					silo: false,
				},
			),
			Function::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			} => self.host_call(
				data,
				call,
				ExternalCallee::Module {
					storage: format!("silo::{org}::{pkg}::<{major}.{minor}.{patch}>"),
					sub: sub.clone(),
					silo: true,
				},
			),
		}
	}

	/// The call `data` of a model or a module export, `callee`, run by the host
	/// ([`Instr::ExternalEnter`], the arguments, [`Instr::ExternalCall`]): entering it checks the
	/// capability, reads the definition the transaction holds and judges its `PERMISSIONS`. No
	/// predicate is lowered for the call, so a definition that states one fails the call closed.
	/// A module export may run statements, so its call writes; a model computes its value alone.
	fn host_call(
		&mut self,
		data: &OpData,
		call: &CallAttrs,
		callee: ExternalCallee,
	) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.call of a model or a module")?;
		let args = call_args(data, call)?;
		let writes = matches!(callee, ExternalCallee::Module { .. });
		let external = self.em.external(ExternalSpec {
			callee,
			plan_depth: self.depth,
			guard: None,
		})?;
		let dst = self.external_call(external, args)?;
		if writes {
			self.writes();
		}
		Ok(dst)
	}

	/// The call `data` of the embedded script `source`, run by the host's script runtime.
	///
	/// [`Instr::ExternalEnter`] checks the session may run scripts before the arguments are
	/// evaluated, then the arguments are evaluated in turn and [`Instr::ExternalCall`] runs the
	/// script with them. Its current value is the element of the element body the call is written
	/// in, and the row the host hands it anywhere else. A script may run statements, so the call
	/// writes; it runs in the session it is called in and asks for no context level of its own.
	/// Calls of code the host runs keep rows in order.
	fn script_call(
		&mut self,
		data: &OpData,
		call: &CallAttrs,
		source: &str,
	) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.call of a script")?;
		let args = call_args(data, call)?;
		let external = self.em.external(ExternalSpec {
			callee: ExternalCallee::Script {
				source: source.to_owned(),
			},
			plan_depth: self.depth,
			guard: None,
		})?;
		let dst = self.external_call(external, args)?;
		self.writes();
		Ok(dst)
	}

	/// [`Instr::ExternalEnter`] of the entry `external`, the arguments `args` in turn, then
	/// [`Instr::ExternalCall`], in the authority frame being lowered; the register that holds the
	/// call's value.
	fn external_call(&mut self, external: u32, args: &[ValueId]) -> Result<Operand, IrError> {
		let frame = self.frame.program;
		self.emit(Instr::ExternalEnter {
			external,
			frame,
		});
		let args = self.values(args)?;
		let this = self.element.map(Operand::Reg);
		let dst = self.em.reg()?;
		self.emit(Instr::ExternalCall {
			external,
			args,
			this,
			dst,
			frame,
		});
		self.keeps_row_order = true;
		Ok(Operand::Reg(dst))
	}

	/// A call of the builtin `name`: the capability check, the arguments in turn, then the call by
	/// name through the function registry.
	pub(super) fn builtin_call(
		&mut self,
		name: &str,
		args: &[ValueId],
	) -> Result<Operand, IrError> {
		let call = self.allowed_builtin(name)?;
		let args = self.values(args)?;
		let dst = self.em.reg()?;
		self.emit_builtin_call(call, args, dst);
		Ok(Operand::Reg(dst))
	}

	/// The call site of the builtin `name` ([`Lowerer::call_site`]), after the check that the
	/// capabilities admit the call: what [`Lowerer::builtin_call`] emits before the arguments.
	pub(super) fn allowed_builtin(&mut self, name: &str) -> Result<u32, IrError> {
		let call = self.call_site(name)?;
		self.emit(Instr::CallAllowed {
			call,
			frame: self.frame.program,
		});
		Ok(call)
	}

	/// The call site of builtin `name`, with what calling it asks of the program: the context
	/// level it requires, the row order its answers keep and a write transaction where it runs
	/// statements.
	fn call_site(&mut self, name: &str) -> Result<u32, IrError> {
		let registry = self.registry;
		let required = match registry.get_projection(name) {
			Some(projection) => projection.required_context(),
			None => registry
				.get(name)
				.map_or(ContextLevel::Root, |function| function.required_context()),
		};
		self.context = self.context.max(required);
		self.keeps_row_order = true;
		// A builtin that runs statements needs a write transaction whatever its arguments are.
		if crate::expr::method::is_writer_builtin(name) {
			self.writes();
		}
		let closure_args: Vec<u16> = registry.get(name).map_or_else(Vec::new, |function| {
			function.closure_args().iter().filter_map(|&at| u16::try_from(at).ok()).collect()
		});
		self.em.call(CallSpec {
			name: name.to_owned(),
			plan_depth: self.depth,
			index: None,
			closure_args,
		})
	}

	/// The call of the builtin at call site `call` over `args` into `dst`, after
	/// [`Lowerer::allowed_builtin`].
	pub(super) fn emit_builtin_call(&mut self, call: u32, args: Vec<Operand>, dst: Reg) {
		self.emit(Instr::Call {
			call,
			args,
			dst,
			frame: self.frame.program,
			elem: self.element,
		});
	}

	/// The call of the two-argument `vector::*` builtin `name` over `args` as a vector measure
	/// ([`Instr::Measure`]), which the interpreter answers where the row lies; `None` where the
	/// call is not one a measure answers, for [`Self::builtin_call`] to lower.
	///
	/// A measure answers a call in a pipeline body of a read statement, in the statement's own
	/// frame and over no element, with one argument a field of the row of objects being read
	/// and the other a literal or a parameter that names no part of the row: such a query is read
	/// once, as the run starts, where the call reads it once per row, unless its read judges a
	/// permission against the row. A measure computes the builtin itself, so a name the
	/// registry resolves to another function lowers as its call.
	fn measure_call(&mut self, name: &str, args: &[ValueId]) -> Result<Option<Operand>, IrError> {
		let Some(kind) = super::super::measure::MeasureKind::of(name) else {
			return Ok(None);
		};
		if !self.registry.is_builtin(name)
			|| self.subset != Subset::Operand
			|| self.reads.is_none()
			|| self.aside
			|| self.closure_row
			|| self.element.is_some()
			|| self.frame.program.is_some()
		{
			return Ok(None);
		}
		let [first, second] = args else {
			return Ok(None);
		};
		let (field, query, query_first) = match (self.row_field(*first), self.row_field(*second)) {
			(Some(field), _) if self.row_independent(*second) => (field, *second, false),
			(_, Some(field)) if self.row_independent(*first) => (field, *first, true),
			_ => return Ok(None),
		};
		let query = match self.defining(query).map(|data| &data.kind) {
			Some(OpKind::Expr(
				ExprOp::FreeParam {
					name,
				}
				| ExprOp::CatalogParam {
					name,
				},
			)) => super::super::measure::MeasureQuery::Param(self.em.param(ParamSpec {
				name: name.clone(),
				guard: None,
			})?),
			_ => match self.value(query)? {
				Operand::Const(index) => super::super::measure::MeasureQuery::Const(index),
				Operand::Reg(_) | Operand::Field(_) => return Ok(None),
			},
		};
		let call = self.call_site(name)?;
		let field = self.em.field(field.as_str())?;
		let dst = self.em.reg()?;
		self.emit(Instr::Measure {
			measure: Arc::new(super::super::measure::MeasureSpec {
				kind,
				call,
				query,
				query_first,
			}),
			field,
			dst,
		});
		Ok(Some(Operand::Reg(dst)))
	}

	/// The name of the field of the row of objects being read that `value` is, where it is one.
	fn row_field(&self, value: ValueId) -> Option<Strand> {
		let data = self.defining(value)?;
		match &data.kind {
			OpKind::Expr(ExprOp::Field {
				name,
			}) => match data.operands.as_slice() {
				[row] if self.is_object_row(*row) => Some(name.clone()),
				_ => None,
			},
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
		}
	}

	/// Whether `value` names no part of the row: a literal, or a parameter other than the names
	/// that read the row or its document. A parameter the catalog defines may still judge its
	/// `PERMISSIONS` against the row it is read for; the run settles which
	/// ([`MeasureSettled::per_row`](super::super::measure::MeasureSettled)).
	fn row_independent(&self, value: ValueId) -> bool {
		let Some(data) = self.defining(value) else {
			return false;
		};
		match &data.kind {
			OpKind::Expr(
				ExprOp::Literal(_)
				| ExprOp::Constant {
					..
				},
			) => true,
			OpKind::Expr(ExprOp::CatalogParam {
				..
			}) => true,
			OpKind::Expr(ExprOp::FreeParam {
				name,
			}) => !matches!(name.as_str(), "this" | "self" | "parent"),
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
		}
	}

	/// The op whose first result is `value`, where it is held in the authority frame of the code
	/// being lowered: an op of another frame is lowered by its own rule.
	fn defining(&self, value: ValueId) -> Option<&OpData> {
		match self.ir.value(value)?.def {
			ValueDef::OpResult {
				op,
				index: 0,
			} => self.ir.op(op).filter(|data| data.frame == self.frame.ir),
			ValueDef::OpResult {
				..
			}
			| ValueDef::BlockArg {
				..
			} => None,
		}
	}

	/// The read of the statement clock, as the call of the clock builtin that reads the reading
	/// `which` names: its answer is the statement's instant, which the builtin reads from the
	/// context, so no instruction holds it and the op's timestamp operand is not read.
	fn now(&mut self, data: &OpData, which: NowWhich) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.now")?;
		self.require_frame(data)?;
		let builtin = match which {
			NowWhich::Datetime => "time::now",
			NowWhich::UnixSeconds => "time::unix",
			NowWhich::UnixMillis => "time::millis",
			NowWhich::UnixMicros => "time::micros",
			NowWhich::UnixNanos => "time::nano",
		};
		self.builtin_call(builtin, &[])
	}

	/// The check that a method resolves for its receiver, which runs before the arguments are
	/// evaluated. The receiver is the operand the check reads.
	fn dispatch(&mut self, data: &OpData, target: &DispatchTarget) -> Result<Operand, IrError> {
		let name = match target {
			DispatchTarget::Method(name) => name,
			DispatchTarget::Invoke => return self.dispatch_invoke(data),
		};
		self.require_frame(data)?;
		let [base] = exactly::<1>(data)?;
		let result =
			data.results.first().copied().ok_or(IrError::Stale("ir_lower::expr: dispatch"))?;
		let recv = self.value(base)?;
		let dispatched = match self.registry.get_method(name.as_str()) {
			Some(_) => Dispatched::Builtin(self.resolve_method(name.as_str(), recv)?),
			None => self.closure_field(name, recv)?,
		};
		self.scope.resolved.insert(result, dispatched);
		Ok(recv)
	}

	/// The read of the closure the receiver in `recv` holds under the name `name` of a method no
	/// builtin names, before the call's arguments are evaluated: a receiver that holds none raises
	/// the unknown-method error naming its type there.
	fn closure_field(&mut self, name: &Strand, recv: Operand) -> Result<Dispatched, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.dispatch of a method no builtin names")?;
		let callee = self.em.reg()?;
		self.emit(Instr::ClosureField {
			name: name.clone(),
			recv,
			dst: callee,
		});
		self.keeps_row_order = true;
		Ok(Dispatched::Field {
			recv,
			callee,
		})
	}

	/// The check that the method `name` resolves for the receiver in `recv`, and what the call of
	/// the method reads of it.
	///
	/// A method with a generic implementation resolves for any receiver. One with an object
	/// implementation leaves no receiver for a closure field to answer. Any other may be answered
	/// by a closure the receiver holds under the method's name.
	pub(super) fn resolve_method(
		&mut self,
		name: &str,
		recv: Operand,
	) -> Result<Resolved, IrError> {
		let registry = self.registry;
		let Some(descriptor) = registry.get_method(name) else {
			return Err(invariant(
				"a method resolved as a builtin names one: a dispatch reads a closure field for any other name, and the binder builds a method's closure loop only where a builtin takes a closure",
			));
		};
		self.keeps_row_order = true;
		let method = self.em.method(MethodSpec {
			name: descriptor.name.to_owned(),
			kinds: descriptor.kind_mask(),
			has_fallback: descriptor.fallback.is_some(),
			plan_depth: 0,
			closure_args: registry
				.method_closure_positions(name)
				.into_iter()
				.filter_map(|at| u16::try_from(at).ok())
				.collect(),
			writing: self.writing_labels,
		})?;
		let closure = if descriptor.fallback.is_some() {
			None
		} else if descriptor.dispatches_for_an_object() {
			self.emit(Instr::MethodResolve {
				method,
				recv,
				closure: None,
			});
			None
		} else {
			let label = self.em.label()?;
			self.emit(Instr::MethodResolve {
				method,
				recv,
				closure: Some(label),
			});
			Some(label)
		};
		Ok(Resolved {
			method,
			recv,
			closure,
		})
	}

	/// The call of the method its dispatch resolved, on the receiver and the arguments.
	///
	/// A receiver that may hold a closure under the method's name takes a second path to that
	/// closure. Each path evaluates the arguments for its own call, because the receiver is
	/// resolved before they are.
	fn method(&mut self, data: &OpData) -> Result<Operand, IrError> {
		self.require_frame(data)?;
		let Some((&[dispatched, receiver], args)) = data.operands.split_first_chunk::<2>() else {
			return Err(IrError::Stale("ir_lower::expr: operand arity"));
		};
		self.value(dispatched)?;
		let resolved = *self
			.scope
			.resolved
			.get(&dispatched)
			.ok_or(IrError::Stale("ir_lower::expr: a method without its dispatch"))?;
		let recv = match resolved {
			Dispatched::Builtin(resolved) => resolved.recv,
			Dispatched::Field {
				recv,
				..
			} => recv,
		};
		if self.value(receiver)? != recv {
			return Err(IrError::Stale("ir_lower::expr: a method on another receiver"));
		}
		match resolved {
			Dispatched::Builtin(resolved) => self.call_resolved(resolved, args),
			Dispatched::Field {
				callee,
				..
			} => self.call_field(callee, args),
		}
	}

	/// The invocation of the closure field read into `callee`, with the values `args` evaluated
	/// for it. The field's body is not one the program holds, so the call may write, and rows stay
	/// serial.
	fn call_field(&mut self, callee: Reg, args: &[ValueId]) -> Result<Operand, IrError> {
		let args = self.values(args)?;
		let dst = self.em.reg()?;
		self.emit(Instr::Invoke {
			target: Operand::Reg(callee),
			args,
			dst,
			frame: self.frame.program,
			elem: None,
		});
		self.keeps_row_order = true;
		self.writes();
		Ok(Operand::Reg(dst))
	}

	/// The call of the method `resolved` names, on the receiver it was resolved for, with the
	/// values `args` evaluated for it.
	pub(super) fn call_resolved(
		&mut self,
		resolved: Resolved,
		args: &[ValueId],
	) -> Result<Operand, IrError> {
		let recv = resolved.recv;
		let method = resolved.method;
		let dst = self.em.reg()?;
		let Some(closure) = resolved.closure else {
			let args = self.values(args)?;
			self.emit(Instr::Method {
				method,
				recv,
				args,
				dst,
				frame: self.frame.program,
			});
			return Ok(Operand::Reg(dst));
		};
		let held = self.scope.clone();
		let builtin_args = self.values(args)?;
		self.emit(Instr::Method {
			method,
			recv,
			args: builtin_args,
			dst,
			frame: self.frame.program,
		});
		let done = self.em.label()?;
		self.emit(Instr::Jump {
			target: done,
		});
		self.em.bind(closure)?;
		self.scope = held.clone();
		let closure_args = self.values(args)?;
		self.emit(Instr::MethodClosure {
			method,
			recv,
			args: closure_args,
			dst,
			frame: self.frame.program,
		});
		self.scope = held;
		self.em.bind(done)?;
		Ok(Operand::Reg(dst))
	}
}

/// `value` as the path of fields it reads off a row, when it is one: fields, accesses and
/// `.*` over a block argument.
fn path_expr(ir: &Ir, value: ValueId) -> Option<LogicalExpr> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	let data = ir.op(op)?;
	let OpKind::Expr(kind) = &data.kind else {
		return None;
	};
	let base = *data.operands.first()?;
	if let ExprOp::Field {
		name,
	} = kind
	{
		return matches!(ir.value(base)?.def, ValueDef::BlockArg { .. })
			.then(|| LogicalExpr::Field(name.clone()));
	}
	if let ExprOp::Access {
		field,
	} = kind
	{
		return Some(LogicalExpr::Access {
			base: Box::new(path_expr(ir, base)?),
			field: field.clone(),
		});
	}
	if let ExprOp::Each = kind {
		return Some(LogicalExpr::Each(Box::new(path_expr(ir, base)?)));
	}
	None
}

/// `value` as an error names it: a path off the row, a literal or a parameter. `None` for any
/// other expression.
fn shown_expr(ir: &Ir, value: ValueId) -> Option<LogicalExpr> {
	if let Some(path) = path_expr(ir, value) {
		return Some(path);
	}
	match query_operand(ir, value) {
		QueryOperand::Literal(value) => {
			crate::exec::planner::util::try_value_to_logical_expr(value)
		}
		QueryOperand::Named(Some(name)) => Some(LogicalExpr::Param(crate::expr::Param::from(name))),
		QueryOperand::Named(None) | QueryOperand::Other => None,
	}
}

/// The name of the operator at the top of the plan `EXPLAIN` renders for `value`, when a statement
/// in value position computes it and `units` serves `EXPLAIN`.
fn subquery_root(units: Option<&dyn UnitSink>, ir: &Ir, value: ValueId) -> Option<String> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	let data = ir.op(op)?;
	let OpKind::Expr(ExprOp::Subquery(_)) = &data.kind else {
		return None;
	};
	units?.region_root_name(ir, *data.regions.first()?)
}

/// Whether the plan resolves `value`, the query of a `MATCHES`: it reads neither `row`, a session
/// parameter, nor a statement in value position or any other region, and, where the caller's
/// reads are `checked`, no record. It is then read from literals, names bound around the statement
/// and, for an unchecked caller, the records it names; a checked caller reads a record under its
/// table's permissions, which no plan resolves.
fn resolved_query(ir: &Ir, value: ValueId, row: Option<ValueId>, checked: bool) -> bool {
	let mut pending = vec![value];
	let mut budget = 64usize;
	while let Some(value) = pending.pop() {
		let Some(budget_left) = budget.checked_sub(1) else {
			return false;
		};
		budget = budget_left;
		if Some(value) == row {
			return false;
		}
		let Some(data) = ir.value(value) else {
			return false;
		};
		let ValueDef::OpResult {
			op,
			..
		} = data.def
		else {
			continue;
		};
		let Some(held) = ir.op(op) else {
			return false;
		};
		match &held.kind {
			OpKind::Expr(ExprOp::SessionParam {
				..
			}) => return false,
			OpKind::Expr(ExprOp::RecordId(_)) if checked => return false,
			OpKind::Expr(ExprOp::Literal(literal)) if checked && holds_record_id(literal) => {
				return false;
			}
			OpKind::Expr(_) if held.regions.is_empty() => {
				pending.extend(held.operands.iter().copied());
			}
			OpKind::Expr(_) => return false,
			OpKind::Logical(_) | OpKind::Phys(_) => {}
		}
	}
	true
}

/// Whether `value` is a record id or holds one at any depth: a collection literal holds the record
/// ids written in it, and a step over it may read their records.
fn holds_record_id(value: &Value) -> bool {
	match value {
		Value::RecordId(_) => true,
		Value::Array(arr) => arr.iter().any(holds_record_id),
		Value::Set(set) => set.iter().any(holds_record_id),
		Value::Object(obj) => obj.values().any(holds_record_id),
		Value::Range(range) => [&range.start, &range.end].into_iter().any(|bound| match bound {
			std::ops::Bound::Included(value) | std::ops::Bound::Excluded(value) => {
				holds_record_id(value)
			}
			std::ops::Bound::Unbounded => false,
		}),
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Closure(_) => false,
	}
}

/// The text of the `MATCHES` operator `op` as the planner rebuilds it, when the rebuild reads it
/// whole and names no value it stands in for.
fn rebuilt_sql(ir: &Ir, op: OpId) -> Option<String> {
	let value = *ir.op(op)?.results.first()?;
	let rebuilt = crate::exec::ir::phys::access_expr::predicate(ir, value).ok()?;
	let ([(_, expr)], true, true) =
		(rebuilt.conjuncts.as_slice(), rebuilt.withheld.is_empty(), rebuilt.operands.is_empty())
	else {
		return None;
	};
	// A value the rebuild has no form for stands as a name of its own making.
	let text = expr.to_sql();
	(!text.contains("__opaque") && !text.contains("__operand")).then_some(text)
}

/// The block argument the path `value` reads its fields off ([`path_expr`]).
fn path_row(ir: &Ir, value: ValueId) -> Option<ValueId> {
	let mut value = value;
	loop {
		match ir.value(value)?.def {
			ValueDef::BlockArg {
				..
			} => return Some(value),
			ValueDef::OpResult {
				op,
				..
			} => value = *ir.op(op)?.operands.first()?,
		}
	}
}

/// Where an index function reads its clause's query when it runs.
enum Query {
	Param(Strand),
	Bound(ValueId),
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::AccessMode;
	use crate::exec::ir::expr::ops::{ConvertAttrs, FoldedError, KeyRange};
	use crate::exec::ir::iface::region_kind::RegionKind;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::payload::Refusal;
	use crate::exec::vm::program::{Program, Source};
	use crate::expr::BinaryOperator;
	use crate::val::Datetime;

	/// Lowers `v` of `b` into a program of one pipeline that ends the row.
	fn lower_one(b: &Builder, v: ValueId) -> Result<(Operand, Program), IrError> {
		let mut em = Emitter::new();
		let operand = operand(&mut em, &b.ir, builtin_registry(), v)?;
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly)?;
		Ok((operand, program))
	}

	fn lowered(b: &Builder, v: ValueId) -> (Operand, Program) {
		match lower_one(b, v) {
			Ok(lowered) => lowered,
			Err(error) => panic!("the value lowers: {error}"),
		}
	}

	fn adds(program: &Program) -> usize {
		program.pipelines[0]
			.code
			.iter()
			.filter(|instr| {
				matches!(
					instr,
					Instr::Arith {
						op: ArithOp::Add,
						..
					}
				)
			})
			.count()
	}

	#[test]
	fn now_is_never_a_const() {
		let mut b = Builder::new();
		// The statement's instant is the op's operand, which the emitted code does not read.
		let instant =
			b.literal(Value::Datetime(Datetime::from(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH)));
		let now = b.add(
			ExprOp::Now {
				which: NowWhich::Datetime,
			},
			&[instant],
		);
		let (operand, program) = lowered(&b, now);
		assert!(program.consts.is_empty(), "no datetime constant is held");
		assert_eq!(program.calls.len(), 1);
		assert_eq!(program.calls[0].name, "time::now");
		let code = &program.pipelines[0].code;
		assert_eq!(
			code[0],
			Instr::CallAllowed {
				call: 0,
				frame: None
			}
		);
		assert_eq!(
			code[1],
			Instr::Call {
				call: 0,
				args: Vec::new(),
				dst: 1,
				frame: None,
				elem: None
			}
		);
		assert_eq!(operand, Operand::Reg(1));
	}

	#[test]
	fn a_clock_read_of_another_reading_calls_the_builtin_that_reads_it() {
		for (which, name) in [
			(NowWhich::UnixSeconds, "time::unix"),
			(NowWhich::UnixMillis, "time::millis"),
			(NowWhich::UnixMicros, "time::micros"),
			(NowWhich::UnixNanos, "time::nano"),
		] {
			let mut b = Builder::new();
			let instant = b.literal(Value::None);
			let now = b.add(
				ExprOp::Now {
					which,
				},
				&[instant],
			);
			let (_, program) = lowered(&b, now);
			assert!(program.consts.is_empty(), "no instant is held");
			assert_eq!(program.calls.len(), 1);
			assert_eq!(program.calls[0].name, name);
		}
	}

	#[test]
	fn free_param_is_a_slot_read() {
		let mut b = Builder::new();
		let param = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		let (operand, program) = lowered(&b, param);
		assert!(program.consts.is_empty(), "a parameter is never a constant");
		assert_eq!(program.params.len(), 1);
		assert_eq!(program.params[0].name.as_str(), "x");
		assert_eq!(program.params[0].guard, None);
		assert_eq!(
			program.pipelines[0].code[0],
			Instr::LoadParam {
				param: 0,
				dst: 1,
				frame: None
			}
		);
		assert_eq!(operand, Operand::Reg(1));
	}

	#[test]
	fn a_value_is_lowered_once_per_scope() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let sum = b.binary(BinaryOperator::Add, one, two);
		let twice = b.binary(BinaryOperator::Multiply, sum, sum);
		let (_, program) = lowered(&b, twice);
		assert_eq!(adds(&program), 1);
	}

	#[test]
	fn a_value_first_read_inside_a_lazy_region_is_lowered_once_before_it() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let sum = b.binary(BinaryOperator::Add, one, two);
		let left = b.literal(Value::Bool(true));
		let lazy = b.short_circuit(ShortCircuitOp::And, left, |_| sum);
		let both = b.binary(BinaryOperator::Add, lazy, sum);
		// The region is the first to read the sum, which is lowered once before the region: the
		// region and the read outside both reuse its operand.
		let (_, program) = lowered(&b, both);
		assert_eq!(adds(&program), 2);
	}

	#[test]
	fn a_value_read_before_a_lazy_region_is_lowered_once() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let sum = b.binary(BinaryOperator::Add, one, two);
		let left = b.literal(Value::Bool(true));
		let lazy = b.short_circuit(ShortCircuitOp::And, left, |_| sum);
		let both = b.binary(BinaryOperator::Add, sum, lazy);
		let (_, program) = lowered(&b, both);
		// The read outside the region came first, so the region reuses its operand.
		assert_eq!(adds(&program), 2);
	}

	#[test]
	fn a_literal_read_inside_a_lazy_region_needs_no_earlier_read() {
		let mut b = Builder::new();
		let left = b.literal(Value::Bool(true));
		let right = b.literal(Value::Bool(false));
		let lazy = b.short_circuit(ShortCircuitOp::And, left, |_| right);
		assert!(lower_one(&b, lazy).is_ok());
	}

	#[test]
	fn an_op_of_a_region_that_nothing_reads_runs_inside_the_region() {
		let mut b = Builder::new();
		let left = b.literal(Value::Bool(true));
		let owner = b.add_with_regions(
			ExprOp::ShortCircuit {
				op: ShortCircuitOp::And,
			},
			&[left],
		);
		b.region(owner, RegionKind::LazyOperand, |b| {
			// A call that nothing reads precedes the yielded value.
			b.call("rand::float", &[]);
			b.literal(Value::Bool(false))
		});
		let value = b.result_of(owner);
		// The call runs where the region runs, after the jump that skips it, and only there.
		let (_, program) = lowered(&b, value);
		let code = &program.pipelines[0].code;
		let jump = code.iter().position(|instr| matches!(instr, Instr::JumpIf { .. }));
		let calls: Vec<usize> = code
			.iter()
			.enumerate()
			.filter_map(|(at, instr)| matches!(instr, Instr::Call { .. }).then_some(at))
			.collect();
		assert_eq!(calls.len(), 1, "{code:?}");
		assert!(jump.is_some_and(|jump| jump < calls[0]), "{code:?}");
	}

	#[test]
	fn a_generic_method_needs_no_resolution() {
		let mut b = Builder::new();
		let receiver = b.literal(Value::from(5_i64));
		let result = b.method("to_string", receiver, &[]);
		let (_, program) = lowered(&b, result);
		assert_eq!(program.methods.len(), 1);
		assert!(program.methods[0].has_fallback);
		assert!(matches!(program.pipelines[0].code[0], Instr::Method { .. }));
	}

	#[test]
	fn a_method_is_resolved_before_its_call_and_may_take_a_closure_path() {
		let mut b = Builder::new();
		let receiver = b.literal(Value::from("abc"));
		let result = b.method("len", receiver, &[]);
		let (_, program) = lowered(&b, result);
		let descriptor =
			builtin_registry().get_method("len").expect("the registry holds the method");
		let code = &program.pipelines[0].code;
		let closure_paths =
			code.iter().filter(|instr| matches!(instr, Instr::MethodClosure { .. })).count();
		let may_hold_a_closure =
			descriptor.fallback.is_none() && !descriptor.dispatches_for_an_object();
		assert_eq!(closure_paths, usize::from(may_hold_a_closure));
		assert_eq!(matches!(code[0], Instr::MethodResolve { .. }), descriptor.fallback.is_none());
	}

	/// What lowering `v` of `b` needs of its program.
	fn needs_of(b: &Builder, v: ValueId) -> Needs {
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		lowerer.value(v).expect("the value lowers");
		lowerer.needs()
	}

	/// The code of the program's one pipeline.
	fn code_of(program: &Program) -> &[Instr] {
		&program.pipelines[0].code
	}

	#[test]
	fn session_and_catalog_params_are_unguarded_slot_reads() {
		for op in [
			ExprOp::SessionParam {
				name: "auth".into(),
			},
			ExprOp::CatalogParam {
				name: "limit".into(),
			},
		] {
			let mut b = Builder::new();
			let param = b.add(op, &[]);
			let (operand, program) = lowered(&b, param);
			assert!(program.consts.is_empty(), "a parameter is never a constant");
			assert_eq!(program.params.len(), 1);
			assert_eq!(program.params[0].guard, None);
			assert_eq!(
				code_of(&program)[0],
				Instr::LoadParam {
					param: 0,
					dst: 1,
					frame: None
				}
			);
			assert_eq!(operand, Operand::Reg(1));
		}
	}

	#[test]
	fn table_and_constant_are_constants() {
		let mut b = Builder::new();
		let table = b.add(
			ExprOp::Table {
				name: "person".into(),
			},
			&[],
		);
		let pi = b.add(
			ExprOp::Constant {
				c: crate::expr::Constant::MathPi,
			},
			&[],
		);
		let (table, program) = lowered(&b, table);
		assert_eq!(table, Operand::Const(0));
		assert_eq!(program.consts.len(), 1);
		assert!(code_of(&program).iter().all(|instr| matches!(instr, Instr::EndRow)));
		let (pi, program) = lowered(&b, pi);
		assert_eq!(pi, Operand::Const(0));
		assert_eq!(program.consts[0], crate::expr::Constant::MathPi.compute());
	}

	#[test]
	fn a_mock_holds_the_limit_it_was_lowered_under() {
		let mut b = Builder::new();
		let mock = b.add(ExprOp::Mock(Box::new(crate::expr::Mock::Count("t".into(), 3))), &[]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry())
				.with_limits(LowerLimits {
					generation_allocation_limit: 7,
					..LowerLimits::default()
				});
		let operand = lowerer.value(mock).expect("the mock lowers");
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline ends");
		assert_eq!(operand, Operand::Reg(1));
		assert!(matches!(
			pipeline.code[0],
			Instr::Mock {
				limit: 7,
				dst: 1,
				..
			}
		));
		// The default limit is the default configuration's.
		assert_eq!(
			LowerLimits::default().generation_allocation_limit,
			ExecConfig::default().generation_allocation_limit
		);
	}

	/// A record id of `table` with the key `shape`, over `operands`.
	fn record_id(b: &mut Builder, shape: KeyShape, operands: &[ValueId]) -> ValueId {
		b.add(
			ExprOp::RecordId(Box::new(crate::exec::ir::expr::ops::RecordIdAttrs {
				table: "t".into(),
				key_shape: shape,
			})),
			operands,
		)
	}

	/// A record id held anywhere in a literal is one a step over the literal may read, so a checked
	/// caller's `MATCHES` query that holds one is not the plan's to resolve.
	#[test]
	fn a_record_id_anywhere_in_a_literal_is_held() {
		let record = Value::RecordId(RecordId::new("t".into(), RecordIdKey::Number(1)));
		let nested = |value: Value| Value::Array(vec![Value::from(1_i64), value].into());
		assert!(super::holds_record_id(&record));
		assert!(super::holds_record_id(&nested(record.clone())));
		assert!(super::holds_record_id(&nested(nested(record.clone()))));
		assert!(super::holds_record_id(&Value::Object(crate::val::Object::from(
			std::collections::BTreeMap::from([(surrealdb_strand::Strand::from("a"), record)])
		))));
		assert!(!super::holds_record_id(&nested(Value::from("t:1"))));
	}

	#[test]
	fn a_record_id_with_computed_key_parts_is_built_by_its_instruction() {
		// An array key: the elements are evaluated and pushed in order.
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let param = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		let id = record_id(
			&mut b,
			KeyShape::Array {
				len: 2,
			},
			&[one, param],
		);
		let (operand, program) = lowered(&b, id);
		let code = code_of(&program);
		// The array is made first, and each element is evaluated just before it is pushed.
		assert!(matches!(code[0], Instr::NewArray { .. }));
		assert!(matches!(code[1], Instr::ArrayPush { .. }));
		assert!(matches!(code[2], Instr::LoadParam { .. }));
		assert!(matches!(code[3], Instr::ArrayPush { .. }));
		assert!(matches!(
			code[4],
			Instr::RecordId {
				record: 0,
				..
			}
		));
		assert!(matches!(operand, Operand::Reg(_)));
		assert_eq!(program.record_ids.len(), 1);
		assert!(matches!(program.record_ids[0].key, RecordKey::Array(Operand::Reg(_))));

		// An object key: one `SetField` per value, under its key.
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let id = record_id(
			&mut b,
			KeyShape::Object {
				keys: Box::new(["a".to_owned()]),
			},
			&[one],
		);
		let (_, program) = lowered(&b, id);
		assert!(code_of(&program).iter().any(|instr| matches!(instr, Instr::SetField { .. })));
		assert!(matches!(program.record_ids[0].key, RecordKey::Object(Operand::Reg(_))));

		// A generated key holds no operand and is generated when the instruction runs.
		let mut b = Builder::new();
		let id = record_id(&mut b, KeyShape::Generate(crate::expr::RecordIdKeyGen::Ulid), &[]);
		let (_, program) = lowered(&b, id);
		assert!(matches!(program.record_ids[0].key, RecordKey::Generate(_)));

		// A range holds the keys of its bounds, start before end, each with its operands.
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let id = record_id(
			&mut b,
			KeyShape::Range(Box::new(KeyRange {
				start: Bound::Included(KeyBound::Array {
					len: 1,
				}),
				end: Bound::Excluded(KeyBound::Number(5)),
			})),
			&[one],
		);
		let (_, program) = lowered(&b, id);
		let RecordKey::Range(start, end) = &program.record_ids[0].key else {
			panic!("a range key is a range");
		};
		assert!(matches!(**start, Bound::Included(RecordKey::Array(_))));
		assert!(matches!(**end, Bound::Excluded(RecordKey::Fixed(_))));

		// A key that names more operands than the op holds is not a key.
		let mut b = Builder::new();
		let id = record_id(
			&mut b,
			KeyShape::Array {
				len: 1,
			},
			&[],
		);
		assert!(matches!(lower_one(&b, id), Err(IrError::Stale(_))));
	}

	#[test]
	fn a_record_id_of_literal_parts_is_a_constant() {
		let mut b = Builder::new();
		let id = record_id(&mut b, KeyShape::Number(4), &[]);
		let (operand, program) = lowered(&b, id);
		assert_eq!(operand, Operand::Const(0));
		assert!(program.record_ids.is_empty());
	}

	/// A conditional whose then arm yields `1` and whose else arm ends in `terminator`.
	fn if_then_else_ending_in(b: &mut Builder, terminator: impl FnOnce(&mut Builder)) -> ValueId {
		let condition = b.literal(Value::Bool(true));
		let owner = b.add_with_regions(ExprOp::If, &[condition]);
		b.region(owner, RegionKind::ValueIf, |b| b.literal(Value::from(1_i64)));
		b.open_with(owner, RegionKind::ValueIf, &[], |b, _| terminator(b));
		b.result_of(owner)
	}

	#[test]
	fn a_region_that_throws_yields_a_register_nothing_reads() {
		let mut b = Builder::new();
		let value = if_then_else_ending_in(&mut b, |b| {
			let thrown = b.literal(Value::from("boom"));
			b.terminate(ExprOp::Throw, &[thrown]);
		});
		let (operand, program) = lowered(&b, value);
		let code = code_of(&program);
		let throw = code
			.iter()
			.position(|instr| {
				matches!(
					instr,
					Instr::Throw {
						src: Operand::Const(_)
					}
				)
			})
			.expect("the arm throws its operand");
		// The code after the raise loads the register that stands for the arm's value.
		assert!(matches!(code[throw + 1], Instr::Load { .. }));
		assert!(matches!(operand, Operand::Reg(_)));
	}

	/// The names of the parameters `program`'s pipeline loads, in the order it loads them, and
	/// the position of its `Throw`.
	fn loads_before_throw(program: &Program) -> (Vec<Strand>, usize) {
		let code = code_of(program);
		let loads = code
			.iter()
			.filter_map(|instr| {
				if let Instr::LoadParam {
					param,
					..
				} = instr
				{
					Some(program.params[*param as usize].name.clone())
				} else {
					None
				}
			})
			.collect();
		let throw = code
			.iter()
			.position(|instr| matches!(instr, Instr::Throw { .. }))
			.expect("the arm throws");
		(loads, throw)
	}

	#[test]
	fn an_op_nothing_reads_runs_in_program_order_before_the_region_diverges() {
		let param = |b: &mut Builder, name: &str| {
			b.add(
				ExprOp::FreeParam {
					name: name.into(),
				},
				&[],
			)
		};
		// `[$a, THROW $b]`: `$a` is read by nothing, as the array is never built, and runs first.
		let mut b = Builder::new();
		let value = if_then_else_ending_in(&mut b, |b| {
			param(b, "a");
			let thrown = param(b, "b");
			b.terminate(ExprOp::Throw, &[thrown]);
		});
		let (_, program) = lowered(&b, value);
		let (loads, throw) = loads_before_throw(&program);
		assert_eq!(loads, [Strand::new("a"), Strand::new("b")]);
		let last_load = code_of(&program)
			.iter()
			.rposition(|instr| matches!(instr, Instr::LoadParam { .. }))
			.expect("the loads");
		assert!(last_load < throw, "both loads run before the raise");

		// `$b` is read by the throw and written before `$a`, which nothing reads: the order
		// written is kept, not the order the terminator reads in.
		let mut b = Builder::new();
		let value = if_then_else_ending_in(&mut b, |b| {
			let thrown = param(b, "b");
			param(b, "a");
			b.terminate(ExprOp::Throw, &[thrown]);
		});
		let (_, program) = lowered(&b, value);
		let (loads, _) = loads_before_throw(&program);
		assert_eq!(loads, [Strand::new("b"), Strand::new("a")]);

		// `$a` is read only inside an arm of the `IF` nothing reads: it stands before the `IF`, so
		// it runs before the arm is chosen, as written.
		let mut b = Builder::new();
		let value = if_then_else_ending_in(&mut b, |b| {
			let a = param(b, "a");
			let condition = b.literal(Value::Bool(true));
			let inner = b.add_with_regions(ExprOp::If, &[condition]);
			b.region(inner, RegionKind::ValueIf, |b| {
				let one = b.literal(Value::from(1_i64));
				b.binary(BinaryOperator::Add, a, one)
			});
			b.region(inner, RegionKind::ValueIf, |b| b.literal(Value::None));
			let thrown = b.literal(Value::from("boom"));
			b.terminate(ExprOp::Throw, &[thrown]);
		});
		let (_, program) = lowered(&b, value);
		let (loads, _) = loads_before_throw(&program);
		assert_eq!(loads, [Strand::new("a")]);
		let code = code_of(&program);
		let load = code
			.iter()
			.position(|instr| matches!(instr, Instr::LoadParam { .. }))
			.expect("the load");
		let arm = code
			.iter()
			.rposition(|instr| matches!(instr, Instr::Arith { .. }))
			.expect("the arm adds");
		let choice = code[..arm]
			.iter()
			.rposition(|instr| matches!(instr, Instr::JumpIf { .. }))
			.expect("the inner IF chooses its arm");
		assert!(load < choice, "`$a` runs before the inner IF chooses its arm");
	}

	/// An `IF` whose arms read `a`, a value of the region around it that nothing else reads: the
	/// `IF`'s value, the `IF` and `a`.
	fn if_reading_outer(b: &mut Builder) -> (ValueId, OpId, ValueId) {
		let a = b.add(
			ExprOp::FreeParam {
				name: "a".into(),
			},
			&[],
		);
		let condition = b.literal(Value::Bool(true));
		let owner = b.add_with_regions(ExprOp::If, &[condition]);
		b.region(owner, RegionKind::ValueIf, |b| {
			let one = b.literal(Value::from(1_i64));
			b.binary(BinaryOperator::Add, a, one)
		});
		b.region(owner, RegionKind::ValueIf, |b| b.literal(Value::None));
		(b.result_of(owner), owner, a)
	}

	#[test]
	fn a_value_only_a_nested_region_reads_is_lowered_before_the_op_that_owns_the_region() {
		let mut b = Builder::new();
		let (value, ..) = if_reading_outer(&mut b);
		let (_, program) = lowered(&b, value);
		let code = code_of(&program);
		let load = code
			.iter()
			.position(|instr| matches!(instr, Instr::LoadParam { .. }))
			.expect("the load");
		let choice = code
			.iter()
			.position(|instr| matches!(instr, Instr::JumpIf { .. }))
			.expect("the IF chooses its arm");
		assert!(load < choice, "`$a` is lowered before the IF, where it stands");
		assert_eq!(
			code.iter().filter(|instr| matches!(instr, Instr::LoadParam { .. })).count(),
			1,
			"`$a` is lowered once"
		);
	}

	#[test]
	fn a_region_entered_on_its_own_lowers_an_outer_value_in_its_own_scope() {
		let mut b = Builder::new();
		let (_, owner, outer) = if_reading_outer(&mut b);
		let arm = b.ir.op(owner).expect("the IF").regions[0];
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		lowerer.region_value(arm).expect("the arm lowers");
		assert!(!lowerer.is_lowered(outer), "the arm's scope ends with the arm");
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline");
		assert!(
			pipeline.code.iter().any(|instr| matches!(instr, Instr::LoadParam { .. })),
			"the arm reads `$a` where it runs"
		);
	}

	#[test]
	fn a_unit_copied_out_of_a_permission_predicate_lowers_in_the_frame_its_ops_carry() {
		let mut b = Builder::new();
		let frame = b.frame(crate::expr::logical::EvalFrame::permission(None));
		let value = b.in_frame(frame, |b| {
			let a = b.add(
				ExprOp::FreeParam {
					name: "a".into(),
				},
				&[],
			);
			let one = b.literal(Value::from(1_i64));
			b.binary(BinaryOperator::Add, a, one)
		});
		let mut cx = LowerCx::detached();
		let mut em = Emitter::new();
		let mut lowerer = Lowerer::in_context(&mut em, &b.ir, &mut cx);
		lowerer.value(value).expect("the unit's ops lower in their own frame");
		assert_eq!(lowerer.frame.ir, frame);
		assert_eq!(lowerer.frame.program, None, "the unit names no permission frame of its own");
	}

	#[test]
	fn a_region_that_raises_a_misplaced_loop_control_raises_its_error() {
		let mut b = Builder::new();
		let value = if_then_else_ending_in(&mut b, |b| {
			b.terminate(ExprOp::Raise(Box::new(RaiseKind::InvalidControlFlow)), &[]);
		});
		let (_, program) = lowered(&b, value);
		assert!(code_of(&program).iter().any(|instr| matches!(instr, Instr::InvalidControlFlow)));
	}

	#[test]
	fn a_refusal_is_raised_where_reached_and_a_folded_error_has_no_producer() {
		let refused = Refusal::Query {
			message: "refused".to_owned(),
		};
		let mut b = Builder::new();
		let value = if_then_else_ending_in(&mut b, |b| {
			b.terminate(
				ExprOp::Raise(Box::new(RaiseKind::Refused(Box::new(refused.clone())))),
				&[],
			);
		});
		let (_, program) = lowered(&b, value);
		assert!(code_of(&program).iter().any(|instr| matches!(
			instr,
			Instr::Store(StoreInstr::Refuse { refusal }) if **refusal == refused
		)));

		let mut b = Builder::new();
		let value = if_then_else_ending_in(&mut b, |b| {
			b.terminate(
				ExprOp::Raise(Box::new(RaiseKind::Folded(FoldedError {
					text: "boom".to_owned(),
				}))),
				&[],
			);
		});
		assert!(matches!(lower_one(&b, value), Err(IrError::Invariant { .. })));

		// A misplaced transaction control is a step of the request driver.
		let mut b = Builder::new();
		let value = if_then_else_ending_in(&mut b, |b| {
			b.terminate(
				ExprOp::Raise(Box::new(RaiseKind::Misplaced(
					crate::exec::ir::expr::ops::MisplacedControl::NestedBegin,
				))),
				&[],
			);
		});
		assert!(matches!(lower_one(&b, value), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_group_lowered_with_no_lowering_context_is_an_invariant() {
		let mut b = Builder::new();
		let owner = b.add_with_regions(
			ExprOp::Subquery(crate::exec::ir::expr::ops::NestedStrategy::Apply),
			&[],
		);
		b.region(owner, RegionKind::Subquery, |b| b.literal(Value::from(1_i64)));
		let region = b.ir.op(owner).expect("the subquery").regions[0];
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(
			lowerer.lower_group(region, PoolBase::default(), 0),
			Err(IrError::Invariant { .. })
		));
		assert!(matches!(
			lowerer.lower_explain_group(
				region,
				owner,
				PoolBase::default(),
				0,
				super::super::explain_hook::Purpose::Described
			),
			Err(IrError::Invariant { .. })
		));
		assert!(matches!(
			lowerer.need_unit(UnitRequest::LoopBody {
				region
			}),
			Err(IrError::Invariant { .. })
		));
		// The region holds no schema statement, which the caller checks before asking.
		let mut reservations = Reservations::default();
		assert!(matches!(
			lowerer.lower_schema_group(region, 0, &mut reservations),
			Err(IrError::Invariant { .. })
		));
	}

	#[test]
	fn a_value_no_arity_allows_is_an_invariant() {
		// A second result of an op: every op's arity states at most one.
		let mut b = Builder::new();
		let two =
			b.create_kind(OpKind::Expr(ExprOp::Literal(Box::new(Value::from(1_i64)))), &[], 2);
		let second = b.ir.op(two).expect("the literal").results[1];
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(lowerer.value(second), Err(IrError::Invariant { .. })));
		// A result of a yield: its arity states none.
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let yielded = b.create_kind(OpKind::Expr(ExprOp::Yield), &[one], 1);
		let result = b.ir.op(yielded).expect("the yield").results[0];
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(lowerer.value(result), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_refusal_is_emitted_with_no_claim_and_a_non_raise_is_an_invariant() {
		let mut b = Builder::new();
		let raise = b.terminate(
			ExprOp::Raise(Box::new(RaiseKind::Refused(Box::new(Refusal::Query {
				message: "refused".to_owned(),
			})))),
			&[],
		);
		let one = b.literal(Value::from(1_i64));
		let yielded = b.terminate(ExprOp::Yield, &[one]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		lowerer.raise_terminator(raise).expect("the refusal lowers");
		assert!(lowerer.take_claims().is_empty());
		assert!(matches!(
			lowerer.raise_terminator(yielded),
			Err(IrError::Invariant {
				what: "a raise terminator that is neither expr.throw nor expr.raise"
			})
		));
		assert!(matches!(em.code(), [Instr::Store(StoreInstr::Refuse { .. })]));
	}

	#[test]
	fn a_value_region_with_no_terminator_is_an_invariant() {
		let mut b = Builder::new();
		let condition = b.literal(Value::Bool(true));
		let owner = b.add_with_regions(ExprOp::If, &[condition]);
		b.region(owner, RegionKind::ValueIf, |b| b.literal(Value::from(1_i64)));
		b.open_with(owner, RegionKind::ValueIf, &[], |_, _| {});
		let value = b.result_of(owner);
		assert!(matches!(
			lower_one(&b, value),
			Err(IrError::Invariant {
				what: "a value region with no terminator"
			})
		));
	}

	#[test]
	fn a_terminator_lowers_through_its_own_entry() {
		let mut b = Builder::new();
		let thrown = b.literal(Value::from("boom"));
		let throw = b.terminate(ExprOp::Throw, &[thrown]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		lowerer.raise_terminator(throw).expect("the throw lowers");
		let claims = lowerer.take_claims();
		assert_eq!(claims.len(), 1, "the raise is the one instruction the throw emitted");
		assert_eq!(claims[0].0, throw);
		assert!(matches!(em.code(), [Instr::Throw { .. }]));
	}

	#[test]
	fn a_shape_no_producer_builds_is_an_invariant_error() {
		let shapes: [(&str, ExprOp, usize); 5] = [
			(
				"expr.deref",
				ExprOp::Deref {
					table_hint: None,
				},
				1,
			),
			(
				"a coercion",
				ExprOp::Convert(Box::new(ConvertAttrs {
					mode: ConvertMode::Coerce,
					kind: crate::expr::Kind::Int,
					site: crate::exec::ir::expr::ops::ConvertSite(0),
					on_fail: OnFail::Error,
				})),
				1,
			),
			(
				"a cast that answers none",
				ExprOp::Convert(Box::new(ConvertAttrs {
					mode: ConvertMode::Cast,
					kind: crate::expr::Kind::Int,
					site: crate::exec::ir::expr::ops::ConvertSite(0),
					on_fail: OnFail::None,
				})),
				1,
			),
			(
				"a prefix cast",
				ExprOp::Prefix(Box::new(PrefixOperator::Cast(crate::expr::Kind::Int))),
				1,
			),
			("a terminator read as a value", ExprOp::Throw, 1),
		];
		for (what, op, operands) in shapes {
			let mut b = Builder::new();
			let subject = b.literal(Value::from(1_i64));
			let args = vec![subject; operands];
			let value = b.add(op, &args);
			assert!(
				matches!(lower_one(&b, value), Err(IrError::Invariant { .. })),
				"{what} is an invariant error"
			);
		}
	}

	#[test]
	fn the_folders_subset_holds_no_op_its_eligibility_check_refuses() {
		let ops = [
			ExprOp::SessionParam {
				name: "auth".into(),
			},
			ExprOp::CatalogParam {
				name: "limit".into(),
			},
			ExprOp::Table {
				name: "person".into(),
			},
			ExprOp::Mock(Box::new(crate::expr::Mock::Count("t".into(), 1))),
			ExprOp::Constant {
				c: crate::expr::Constant::MathPi,
			},
		];
		for op in ops {
			let mut b = Builder::new();
			let value = b.add(op, &[]);
			let mut em = Emitter::new();
			let mut lowerer = Lowerer::for_fold(
				&mut em,
				&b.ir,
				crate::exec::ir::analysis::cx::builtin_registry(),
			);
			assert!(matches!(lowerer.value(value), Err(IrError::Invariant { .. })));
		}
		// A generated key draws entropy, whole or at a bound of a range.
		let generated = [
			KeyShape::Generate(crate::expr::RecordIdKeyGen::Ulid),
			KeyShape::Range(Box::new(KeyRange {
				start: Bound::Included(KeyBound::Generate(crate::expr::RecordIdKeyGen::Rand)),
				end: Bound::Unbounded,
			})),
		];
		for shape in generated {
			let mut b = Builder::new();
			let id = record_id(&mut b, shape, &[]);
			let mut em = Emitter::new();
			let mut lowerer = Lowerer::for_fold(
				&mut em,
				&b.ir,
				crate::exec::ir::analysis::cx::builtin_registry(),
			);
			assert!(matches!(lowerer.value(id), Err(IrError::Invariant { .. })));
		}
	}

	#[test]
	fn the_folder_builds_a_record_id_whose_key_is_computed() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let id = record_id(
			&mut b,
			KeyShape::Array {
				len: 2,
			},
			&[one, two],
		);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::for_fold(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		let Operand::Reg(dst) = lowerer.value(id).expect("a computed key folds") else {
			panic!("a computed key is built in a register");
		};
		let code = em.code();
		assert!(code.iter().any(|i| matches!(i, Instr::NewArray { .. })));
		assert_eq!(code.iter().filter(|i| matches!(i, Instr::ArrayPush { .. })).count(), 2);
		assert!(matches!(code.last(), Some(Instr::RecordId { dst: held, .. }) if *held == dst));
	}

	#[test]
	fn a_block_value_is_read_by_name_beside_and_by_slot_in_flight() {
		let mut b = Builder::new();
		let bound = b.add(
			ExprOp::FreeParam {
				name: "a".into(),
			},
			&[],
		);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		lowerer.bind_slot_value(bound, 3, Some("a".into()));
		match lowerer.host_scalar(bound).expect("a named value is a scalar") {
			HostScalar::Code(index) => assert_eq!(index, 0),
			HostScalar::Const(_) => panic!("a bound value is read when the host evaluates it"),
		}
		assert_eq!(lowerer.named_reads(), 1);
		assert!(lowerer.read_by_name_since(0, "a"));
		assert!(!lowerer.read_by_name_since(1, "a"));
		assert!(!lowerer.read_by_name_since(0, "b"));
		lowerer.value(bound).expect("the value lowers in the pipeline in flight");
		assert_eq!(lowerer.named_reads(), 1, "the pipeline in flight reads the slot");
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline ends");
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly).expect("a program");
		assert!(matches!(program.host_code[0].code[0], Instr::LoadParam { .. }));
		assert!(matches!(
			program.pipelines[0].code[0],
			Instr::LoadSlot {
				slot: 3,
				..
			}
		));
	}

	#[test]
	fn an_unnamed_block_value_read_beside_is_an_invariant() {
		let mut b = Builder::new();
		let element = b.add(
			ExprOp::FreeParam {
				name: "i".into(),
			},
			&[],
		);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		lowerer.bind_slot_value(element, 0, None);
		assert!(matches!(
			lowerer.host_scalar(element),
			Err(IrError::Invariant {
				what: "a block value read beside the pipeline in flight is unnamed"
			})
		));
	}

	#[test]
	fn a_host_scalar_is_a_constant_or_a_pipeline_beside_the_one_in_flight() {
		let mut b = Builder::new();
		let literal = b.literal(Value::from(5_i64));
		let param = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		// The pipeline in flight holds code of its own, which the host pipeline leaves alone.
		let held = lowerer.em.reg().expect("a register");
		lowerer.emit(Instr::Load {
			dst: held,
			src: Operand::Const(0),
		});
		match lowerer.host_scalar(literal).expect("a literal is a scalar") {
			HostScalar::Const(value) => assert_eq!(*value, Value::from(5_i64)),
			HostScalar::Code(_) => panic!("a literal is the constant it is"),
		}
		match lowerer.host_scalar(param).expect("a parameter is a scalar") {
			HostScalar::Code(index) => assert_eq!(index, 0),
			HostScalar::Const(_) => panic!("a parameter is read when the host evaluates it"),
		}
		// The same value read again in the pipeline in flight is lowered there, not reused from
		// the host pipeline's registers.
		let operand = lowerer.value(param).expect("the parameter lowers");
		assert_eq!(operand, Operand::Reg(2));
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline ends");
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly).expect("a program");
		assert_eq!(program.host_code.len(), 1);
		let host = &program.host_code[0];
		assert!(matches!(host.source, Source::Once));
		assert_eq!(
			host.registers, 3,
			"the row, the parameter and the register the value is emitted from"
		);
		assert!(matches!(
			host.code[0],
			Instr::LoadParam {
				dst: 1,
				..
			}
		));
		assert!(matches!(
			host.code[1],
			Instr::Load {
				dst: 2,
				src: Operand::Reg(1)
			}
		));
		assert!(matches!(
			host.code[2],
			Instr::Emit {
				src: 2
			}
		));
		assert!(matches!(host.code[3], Instr::EndRow));
		assert!(matches!(
			program.pipelines[0].code[0],
			Instr::Load {
				dst: 1,
				..
			}
		));
		assert!(matches!(
			program.pipelines[0].code[1],
			Instr::LoadParam {
				dst: 2,
				..
			}
		));
	}

	#[test]
	fn a_call_or_a_method_keeps_row_order() {
		// Lowering does not read what a callee does, so every call keeps the rows serial.
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let call = b.call("math::abs", &[one]);
		assert!(needs_of(&b, call).keeps_row_order);

		let mut b = Builder::new();
		let receiver = b.literal(Value::from("abc"));
		let method = b.method("to_string", receiver, &[]);
		assert!(needs_of(&b, method).keeps_row_order);

		// Nothing is called, so nothing keeps them.
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let sum = b.binary(BinaryOperator::Add, one, two);
		assert!(!needs_of(&b, sum).keeps_row_order);
	}

	#[test]
	fn only_a_builtin_that_runs_statements_needs_a_write_transaction() {
		for (name, mode) in [
			("math::abs", AccessMode::ReadOnly),
			("eval::surql", AccessMode::ReadWrite),
			("eval::gql", AccessMode::ReadWrite),
			("api::invoke", AccessMode::ReadWrite),
		] {
			let mut b = Builder::new();
			let one = b.literal(Value::from(1_i64));
			let call = b.call(name, &[one]);
			assert_eq!(needs_of(&b, call).access_mode, mode, "{name}");
		}
		// Nothing is called, so nothing writes, and the needs of two reads are the greater.
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let sum = b.binary(BinaryOperator::Add, one, two);
		let needs = needs_of(&b, sum);
		assert_eq!(needs.access_mode, AccessMode::ReadOnly);
		let writing = Needs {
			access_mode: AccessMode::ReadWrite,
			..needs.clone()
		};
		assert_eq!(needs.merged(&writing).access_mode, AccessMode::ReadWrite);
	}

	#[test]
	fn a_method_no_builtin_names_invokes_the_receivers_closure_field() {
		const NAME: &str = "not_a_builtin_method";
		let mut b = Builder::new();
		let receiver = b.literal(Value::from("abc"));
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let arg = b.binary(BinaryOperator::Add, one, two);
		let method = b.method(NAME, receiver, &[arg]);
		let (operand, program) = lowered(&b, method);
		let code = code_of(&program);
		// The field is read before the argument is evaluated, so a receiver that holds none raises
		// the unknown-method error first, then the closure is invoked with the argument.
		let field = code
			.iter()
			.position(|i| matches!(i, Instr::ClosureField { name, .. } if name.as_str() == NAME))
			.expect("the field is read");
		let add = code
			.iter()
			.position(|i| matches!(i, Instr::Arith { .. }))
			.expect("the argument is evaluated");
		let invoke = code
			.iter()
			.position(|i| matches!(i, Instr::Invoke { .. }))
			.expect("the field is invoked");
		assert!(field < add && add < invoke, "{code:?}");
		let Instr::ClosureField {
			dst: callee,
			..
		} = code[field]
		else {
			unreachable!()
		};
		let Instr::Invoke {
			target,
			ref args,
			dst,
			..
		} = code[invoke]
		else {
			unreachable!()
		};
		assert_eq!(target, Operand::Reg(callee));
		assert_eq!(args.len(), 1);
		assert_eq!(operand, Operand::Reg(dst));
		assert!(
			!code.iter().any(|i| matches!(i, Instr::MethodResolve { .. } | Instr::Method { .. }))
		);
		// The field's body is not the program's, so the call may write and keeps rows serial.
		let needs = needs_of(&b, method);
		assert_eq!(needs.access_mode, AccessMode::ReadWrite);
		assert!(needs.keeps_row_order);
		// The plan-time folder's eligibility check refuses the call's opaque body first.
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::for_fold(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(lowerer.value(method), Err(IrError::Invariant { .. })));
	}
}
