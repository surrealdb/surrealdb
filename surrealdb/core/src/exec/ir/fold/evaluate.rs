//! The plan-time folder: runs a constant expression's program in the restricted host and answers
//! its value, its raise or the reason it cannot be folded.
//!
//! A value of more than the size bound, a clock read, a random draw, a sequence advance, a read
//! of a record and a read of the principal are never folded.
//!
//! Eligibility is decided from the container and the builtin table before anything runs, never by
//! running and catching. The evaluator is a [`FoldRunner`]: it lowers one eligible value and runs
//! it in a host that holds no transaction, no session and no datastore. The folder keeps no
//! memo: the planning session's fact cache is the only place an outcome is stored.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::HashSet;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::Arc;

use crate::dbs::capabilities::Capabilities;
use crate::exec::function::{FunctionRegistry, ScalarFunction};
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ancestors;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag, FoldedError};
use crate::exec::ir::iface::bounds::FOLD_MAX_BYTES;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::fallibility::{ContextDep, Fallibility, FallibilityClass};
use crate::exec::ir::iface::fold::{FoldOutcome, FoldRequest, NotFoldable, PlanTimeFolder};
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::iface::premise::PremiseSet;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::memory::size::value_bytes;
use crate::expr::Function;
use crate::expr::logical::EffectSet;
use crate::val::Value;

/// What running a folded expression answered.
#[derive(Debug)]
pub(crate) enum FoldRun {
	/// The value, and the computation depth the evaluation charged.
	Value(Value, u32),
	/// Evaluating the expression raises a value-class error.
	Raised(FoldedError),
	/// The host did not evaluate the expression.
	Refused(NotFoldable),
}

/// Lowers and runs one eligible value in the restricted host.
pub(crate) trait FoldRunner: Send + Sync {
	/// Evaluates `value` of `ir`, admitting only the builtins of `registry` that `capabilities`
	/// allows. The value has passed [`eligible`].
	fn run<'a>(
		&'a self,
		ir: &'a Ir,
		registry: &'a FunctionRegistry,
		value: ValueId,
		capabilities: &'a CapabilityPolicy,
	) -> Pin<Box<dyn Future<Output = FoldRun> + Send + 'a>>;

	/// Evaluates `values`, answering one run per value in order, each the run [`FoldRunner::run`]
	/// gives that value alone, or `None` when the runner has no batched form, in which case the
	/// caller runs each value with [`FoldRunner::run`]. Never suspends.
	fn run_all(
		&self,
		_ir: &Ir,
		_registry: &FunctionRegistry,
		_values: &[ValueId],
		_capabilities: &CapabilityPolicy,
	) -> Option<Vec<FoldRun>> {
		None
	}
}

/// The runner that refuses every value. It is used while no restricted host is installed.
pub(crate) struct NoRunner;

impl FoldRunner for NoRunner {
	fn run<'a>(
		&'a self,
		_ir: &'a Ir,
		_registry: &'a FunctionRegistry,
		_value: ValueId,
		_capabilities: &'a CapabilityPolicy,
	) -> Pin<Box<dyn Future<Output = FoldRun> + Send + 'a>> {
		Box::pin(ready(FoldRun::Refused(NotFoldable::NotYet)))
	}

	fn run_all(
		&self,
		_ir: &Ir,
		_registry: &FunctionRegistry,
		values: &[ValueId],
		_capabilities: &CapabilityPolicy,
	) -> Option<Vec<FoldRun>> {
		Some(values.iter().map(|_| FoldRun::Refused(NotFoldable::NotYet)).collect())
	}
}

/// Which builtins a fold may call.
#[derive(Clone)]
pub(crate) enum CapabilityPolicy {
	/// Every builtin the table marks pure and deterministic, except one behind an experimental
	/// target, which this policy never enables.
	AllowAll,
	/// The builtins the request's capabilities allow.
	Request(Arc<Capabilities>),
}

impl CapabilityPolicy {
	/// Whether a fold may call the builtin `name`: the function-name capability allows it.
	pub(crate) fn allows_call(&self, name: &str) -> bool {
		match self {
			CapabilityPolicy::AllowAll => true,
			CapabilityPolicy::Request(capabilities) => capabilities.allows_function_name(name),
		}
	}
}

/// The plan-time folder: [`eligible`] decides, the runner evaluates.
pub(crate) struct VmFolder {
	capabilities: CapabilityPolicy,
	runner: &'static dyn FoldRunner,
}

impl VmFolder {
	pub(crate) const fn new(
		capabilities: CapabilityPolicy,
		runner: &'static dyn FoldRunner,
	) -> Self {
		Self {
			capabilities,
			runner,
		}
	}

	/// The folder for one planning request, with the request's capabilities and the installed
	/// runner.
	pub(crate) fn for_request(capabilities: Arc<Capabilities>) -> Self {
		Self::new(CapabilityPolicy::Request(capabilities), RUNNER)
	}
}

/// The runner every request's folder uses. It is [`NoRunner`], so every eligible value
/// answers [`NotFoldable::NotYet`].
pub(crate) static RUNNER: &dyn FoldRunner = &NoRunner;

/// A folder that allows every builtin the table marks pure, for tests and the interface suite. It
/// uses [`RUNNER`], so installing the host there installs it here.
pub(crate) static BUILTIN_FOLDER: VmFolder = VmFolder::new(CapabilityPolicy::AllowAll, RUNNER);

impl PlanTimeFolder for VmFolder {
	/// Answers [`NotFoldable`] for a value [`eligible`] refuses without running anything. A
	/// value past [`FOLD_MAX_BYTES`] is [`NotFoldable::TooLarge`]. Every outcome carries no
	/// premise: nothing the fold relied on outlives the planning session.
	fn fold<'a>(
		&'a self,
		ir: &'a Ir,
		cx: &'a EffectCx<'a>,
		req: FoldRequest,
	) -> Pin<Box<dyn Future<Output = FoldOutcome> + Send + 'a>> {
		let mut facts = FactCache::new();
		if let Err(why) = eligible_under(ir, &mut facts, cx, req.value, &self.capabilities) {
			return Box::pin(ready(FoldOutcome::NotFoldable(why)));
		}
		drop(facts);
		Box::pin(async move {
			outcome_of(self.runner.run(ir, cx.registry, req.value, &self.capabilities).await)
		})
	}

	/// The outcomes of [`PlanTimeFolder::fold`] for each request, judged against one fact cache
	/// and evaluated through one batched run where the runner has one.
	///
	/// Sharing the cache is sound because the container does not change during the batch and the
	/// cache holds only answers that are functions of the container and the context: it holds no
	/// fold outcome, so no judgement reads another request's answer. Each eligible value is run
	/// under a budget of its own and keeps its own classification and size check, so the outcome
	/// of a value is the one it gets alone.
	fn fold_all<'a>(
		&'a self,
		ir: &'a Ir,
		cx: &'a EffectCx<'a>,
		reqs: &'a [FoldRequest],
	) -> Pin<Box<dyn Future<Output = Vec<FoldOutcome>> + Send + 'a>> {
		let mut facts = FactCache::new();
		let mut judged: Vec<Option<FoldOutcome>> = Vec::with_capacity(reqs.len());
		let mut eligible: Vec<ValueId> = Vec::new();
		for req in reqs {
			match eligible_under(ir, &mut facts, cx, req.value, &self.capabilities) {
				Ok(()) => {
					eligible.push(req.value);
					judged.push(None);
				}
				Err(why) => judged.push(Some(FoldOutcome::NotFoldable(why))),
			}
		}
		drop(facts);
		if eligible.is_empty() {
			return Box::pin(ready(assemble(judged, Vec::new())));
		}
		match self.runner.run_all(ir, cx.registry, &eligible, &self.capabilities) {
			Some(runs) => Box::pin(ready(assemble(judged, runs))),
			None => Box::pin(async move {
				let mut runs = Vec::with_capacity(eligible.len());
				for value in eligible {
					runs.push(self.runner.run(ir, cx.registry, value, &self.capabilities).await);
				}
				assemble(judged, runs)
			}),
		}
	}
}

/// The outcome a run answers. A value past [`FOLD_MAX_BYTES`] is [`NotFoldable::TooLarge`].
fn outcome_of(run: FoldRun) -> FoldOutcome {
	match run {
		FoldRun::Value(value, depth) => {
			if value_bytes(&value) > FOLD_MAX_BYTES {
				FoldOutcome::NotFoldable(NotFoldable::TooLarge)
			} else {
				FoldOutcome::Constant {
					value,
					premises: PremiseSet::new(),
					depth,
				}
			}
		}
		FoldRun::Raised(error) => FoldOutcome::Raises {
			error,
			premises: PremiseSet::new(),
			depth: 0,
		},
		FoldRun::Refused(why) => FoldOutcome::NotFoldable(why),
	}
}

/// The outcome of each request in order: the judgement where one refused, otherwise the outcome
/// of the next run of `runs`.
fn assemble(judged: Vec<Option<FoldOutcome>>, runs: Vec<FoldRun>) -> Vec<FoldOutcome> {
	let mut runs = runs.into_iter();
	judged
		.into_iter()
		.map(|held| match held {
			Some(outcome) => outcome,
			None => match runs.next() {
				Some(run) => outcome_of(run),
				None => FoldOutcome::NotFoldable(NotFoldable::NotYet),
			},
		})
		.collect()
}

/// The ops a value is computed from, and whether it reads a value no op of the tree defines.
struct Tree {
	/// The defining op of the value, every op it reaches through operands, and every op in the
	/// regions of those ops.
	ops: Vec<OpId>,
	/// A block argument bound outside the tree is read.
	open_leaf: bool,
}

/// Collects the tree `root` is computed from. A value other than `root` whose fold outcome is a
/// constant is a leaf and is not entered.
fn collect(ir: &Ir, facts: &FactCache, root: ValueId) -> Tree {
	let mut seen: HashSet<OpId> = HashSet::new();
	let mut ops: Vec<OpId> = Vec::new();
	let mut open_leaf = false;
	let mut pending: Vec<ValueId> = vec![root];
	let mut done: HashSet<ValueId> = HashSet::new();
	while let Some(value) = pending.pop() {
		if !done.insert(value) {
			continue;
		}
		if value != root && matches!(facts.fold_outcome(value), Some(FoldOutcome::Constant { .. }))
		{
			continue;
		}
		match ir.value(value).map(|data| data.def) {
			Some(ValueDef::OpResult {
				op,
				..
			}) => {
				if !seen.insert(op) {
					continue;
				}
				ops.push(op);
				let Some(data) = ir.op(op) else {
					open_leaf = true;
					continue;
				};
				pending.extend(data.operands.iter().copied());
				for region in data.regions.iter().copied() {
					ir.walk(region, WalkOrder::PreOrder, &mut |inner| {
						if seen.insert(inner) {
							ops.push(inner);
							if let Some(inner_data) = ir.op(inner) {
								pending.extend(inner_data.operands.iter().copied());
							}
						}
						Walk::Continue
					});
				}
			}
			Some(ValueDef::BlockArg {
				block,
				..
			}) => {
				let owner = ir
					.block(block)
					.and_then(|held| ir.parent_op(held.parent))
					.filter(|owner| seen.contains(owner));
				if owner.is_none() {
					open_leaf = true;
				}
			}
			None => open_leaf = true,
		}
	}
	Tree {
		ops,
		open_leaf,
	}
}

/// Whether the op kind `tag` is one a fold may evaluate.
fn in_fold_subset(tag: OpTag) -> bool {
	match tag {
		OpTag::Expr(tag) => match tag {
			ExprTag::Literal
			| ExprTag::Array
			| ExprTag::Set
			| ExprTag::Object
			| ExprTag::RecordId
			| ExprTag::Binary
			| ExprTag::Prefix
			| ExprTag::Postfix
			| ExprTag::Convert
			| ExprTag::Access
			| ExprTag::Index
			| ExprTag::Dispatch
			| ExprTag::Method
			| ExprTag::Call
			| ExprTag::Truthy
			| ExprTag::ShortCircuit
			| ExprTag::If
			| ExprTag::Yield => true,
			ExprTag::FreeParam
			| ExprTag::SessionParam
			| ExprTag::CatalogParam
			| ExprTag::Table
			| ExprTag::Mock
			| ExprTag::Constant
			| ExprTag::Field
			| ExprTag::ComputedRead
			| ExprTag::Doc
			| ExprTag::Deref
			| ExprTag::First
			| ExprTag::Last
			| ExprTag::Each
			| ExprTag::Flatten
			| ExprTag::Destructure
			| ExprTag::Optional
			| ExprTag::RepeatRecurse
			| ExprTag::SetField
			| ExprTag::TupleRow
			| ExprTag::Invoke
			| ExprTag::ClosureCreate
			| ExprTag::Throw
			| ExprTag::Subquery
			| ExprTag::Agg
			| ExprTag::Now
			| ExprTag::Raise
			| ExprTag::DepthCharge => false,
		},
		OpTag::Logical(_) | OpTag::Phys(_) => false,
	}
}

/// The scalar builtins the call op `kind` may reach: the named builtin of a call, every
/// implementation of a method. `None` when `kind` is no call, or names no registered builtin.
fn builtins_of<'r>(
	registry: &'r FunctionRegistry,
	kind: &OpKind,
) -> Option<Vec<&'r dyn ScalarFunction>> {
	match kind {
		OpKind::Expr(ExprOp::Call(function)) => match &function.function {
			Function::Normal(name) => registry.get(name).map(|found| vec![found]),
			Function::Custom(_)
			| Function::Script(_)
			| Function::Model(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => None,
		},
		OpKind::Expr(ExprOp::Method {
			name,
		}) => registry.get_method(name.as_str()).map(|descriptor| {
			descriptor
				.type_impls
				.iter()
				.map(|(_, function)| *function)
				.chain(descriptor.fallback)
				.collect()
		}),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// Whether `kind` is a call of a builtin by name or a method: an op whose callee the capability
/// and purity checks inspect.
fn is_call(kind: &OpKind) -> bool {
	matches!(kind, OpKind::Expr(ExprOp::Call(_)) | OpKind::Expr(ExprOp::Method { .. }))
}

/// Whether the builtin may be evaluated at plan time: pure, deterministic, synchronous and
/// running no closure it is handed.
fn is_foldable_builtin(function: &dyn ScalarFunction) -> bool {
	function.is_pure()
		&& function.is_deterministic()
		&& !function.is_async()
		&& function.closure_args().is_empty()
}

/// Whether the op `op` reads a field off a literal that is no record id and holds no array: an
/// object, a geometry or a scalar. Such a read answers from the literal alone.
fn reads_a_field_of_a_plain_literal(ir: &Ir, op: OpId) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	if !matches!(data.kind, OpKind::Expr(ExprOp::Access { .. })) {
		return false;
	}
	let Some(base) = data.operands.first().and_then(|operand| ir.value(*operand)) else {
		return false;
	};
	let ValueDef::OpResult {
		op: defining,
		..
	} = base.def
	else {
		return false;
	};
	matches!(
		ir.op(defining).map(|held| &held.kind),
		Some(OpKind::Expr(ExprOp::Literal(held)))
			if matches!(
				&**held,
				Value::Object(_)
					| Value::Geometry(_)
					| Value::None
					| Value::Null
					| Value::Bool(_)
					| Value::Number(_)
					| Value::String(_)
					| Value::Bytes(_)
					| Value::Duration(_)
					| Value::Datetime(_)
					| Value::Date(_)
					| Value::Time(_)
					| Value::Timestamp(_)
					| Value::Interval(_)
					| Value::Uuid(_)
					| Value::Regex(_)
			)
	)
}

/// Whether `value` may be folded, decided from the container and the builtin table only.
///
/// The first failing check answers, in this order:
///
/// 1. a clock read: `Clock`;
/// 2. a call of a stored function: `HostRefused("udf")`, because its body is unseen and its effects
///    are the top of the lattice, which would otherwise be misread as a read of the principal;
/// 3. a read of the principal: `ContextDep(PrincipalValue)`, or `PrincipalRole` when that is the
///    only principal dependence the analysis names;
/// 4. a read of records: `ReadsRecords`;
/// 5. any other effect: `Effect`;
/// 6. any context dependence other than the capabilities, then a failure class beyond `Value`:
///    `ContextDep` and `Fallibility`. The `CONTEXT` class of a call rests on the capability check,
///    which check 10 enforces;
/// 7. an op outside the foldable subset: `NotConstantInputs`;
/// 8. a leaf that is neither a literal nor a value with a stored constant outcome, such as a `LET`
///    result or a parameter: `NotConstantInputs`;
/// 9. an op inside an error-catching region between the value and its statement:
///    `HostRefused("error scope")`;
/// 10. a builtin the policy does not allow: `CapabilityDenied`;
/// 11. a builtin that is impure, non-deterministic, asynchronous, runs a closure or is not in the
///     table: `HostRefused("impure builtin")`.
///
/// The effect and failure answers are the analysis's over every op the value is computed from.
pub(crate) fn eligible(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	value: ValueId,
) -> Result<(), NotFoldable> {
	eligible_under(ir, facts, cx, value, &CapabilityPolicy::AllowAll)
}

/// [`eligible`] under the capability policy a folder holds.
pub(crate) fn eligible_under(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	value: ValueId,
	policy: &CapabilityPolicy,
) -> Result<(), NotFoldable> {
	let tree = collect(ir, facts, value);
	let kinds: Vec<(OpId, &OpKind)> =
		tree.ops.iter().filter_map(|op| ir.op(*op).map(|data| (*op, &data.kind))).collect();

	let mut joined = effects::bottom();
	let mut failures = Fallibility::INFALLIBLE;
	for (op, _) in &kinds {
		// A field read off an object, geometry or scalar literal reads no record: only a record
		// id, or an array that may hold one, is fetched.
		if reads_a_field_of_a_plain_literal(ir, *op) {
			failures = failures.union(Fallibility::VALUE);
			continue;
		}
		joined = effects::join(&joined, &effects::of_with(ir, facts, cx, *op));
		failures = failures.union(fallibility::of_with(ir, facts, cx, *op));
	}

	// 1. the clock
	let reads_now = kinds.iter().any(|(_, kind)| matches!(kind, OpKind::Expr(ExprOp::Now { .. })));
	if reads_now || joined.set.contains(EffectSet::CLOCK) {
		return Err(NotFoldable::Clock);
	}

	// 2. a stored function
	let calls_udf = kinds.iter().any(|(_, kind)| match kind {
		OpKind::Expr(ExprOp::Call(function)) => matches!(function.function, Function::Custom(_)),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	});
	if calls_udf {
		return Err(NotFoldable::HostRefused("udf"));
	}

	// 3. the principal
	let reads_session_param =
		kinds.iter().any(|(_, kind)| matches!(kind, OpKind::Expr(ExprOp::SessionParam { .. })));
	let names_value = joined.context_deps.contains(&ContextDep::PrincipalValue);
	let names_role = joined.context_deps.contains(&ContextDep::PrincipalRole);
	if reads_session_param
		|| joined.set.contains(EffectSet::READS_PRINCIPAL)
		|| names_value
		|| names_role
	{
		let role_only = names_role
			&& !reads_session_param
			&& !names_value
			&& !joined.set.contains(EffectSet::READS_PRINCIPAL);
		return Err(NotFoldable::ContextDep(if role_only {
			ContextDep::PrincipalRole
		} else {
			ContextDep::PrincipalValue
		}));
	}

	// 4. records
	if joined.set.contains(EffectSet::READS_DATA) {
		return Err(NotFoldable::ReadsRecords);
	}

	// 5. every other effect
	if joined.set != EffectSet::PURE {
		return Err(NotFoldable::Effect(joined.set));
	}

	// 6. context dependences and the failure class
	if let Some(dep) = joined.context_deps.iter().find(|dep| **dep != ContextDep::Capabilities) {
		return Err(NotFoldable::ContextDep(dep.clone()));
	}
	let explained = joined.context_deps.contains(&ContextDep::Capabilities);
	let class = if failures.contains(Fallibility::DATA) {
		FallibilityClass::Data
	} else if failures.contains(Fallibility::CONTEXT) && !explained {
		FallibilityClass::Context
	} else if failures.contains(Fallibility::VALUE) {
		FallibilityClass::Value
	} else {
		FallibilityClass::Infallible
	};
	if class > FallibilityClass::Value {
		return Err(NotFoldable::Fallibility(class));
	}

	// 7. the op subset
	if kinds.iter().any(|(_, kind)| !in_fold_subset(kind.tag())) {
		return Err(NotFoldable::NotConstantInputs);
	}

	// 8. literal leaves
	if tree.open_leaf {
		return Err(NotFoldable::NotConstantInputs);
	}

	// 9. error scopes
	for (op, _) in &kinds {
		for (region, _) in ancestors(ir, *op) {
			match ir.region(region).map(|held| held.kind) {
				Some(RegionKind::ErrorScope(_)) => {
					return Err(NotFoldable::HostRefused("error scope"));
				}
				Some(RegionKind::Statement(_)) => break,
				Some(_) | None => {}
			}
		}
	}

	// 10. capabilities, then 11. purity
	let callees: Vec<Option<Vec<&dyn ScalarFunction>>> = kinds
		.iter()
		.filter(|(_, kind)| is_call(kind))
		.map(|(_, kind)| builtins_of(cx.registry, kind))
		.collect();
	for callee in callees.iter().flatten() {
		if callee.iter().any(|function| !policy.allows_call(function.name())) {
			return Err(NotFoldable::CapabilityDenied);
		}
	}
	for callee in &callees {
		match callee {
			Some(functions) => {
				if functions.iter().any(|function| !is_foldable_builtin(*function)) {
					return Err(NotFoldable::HostRefused("impure builtin"));
				}
			}
			None => return Err(NotFoldable::HostRefused("impure builtin")),
		}
	}
	Ok(())
}
