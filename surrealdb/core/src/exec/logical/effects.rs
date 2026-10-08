//! The registry as the oracle the logical layer's effect fold consults.
//!
//! `effects_of` (`crate::expr::logical`) derives what an expression does from
//! the declared effects of the builtins it calls. Those declarations live on
//! the registry, so this is where the fold and the registry meet; every
//! predicate the rules and the physical planner ask of an expression is a
//! field of the fold's answer, read through one of the two oracles here.

use std::collections::HashSet;

use crate::exec::function::FunctionRegistry;
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::logical::{
	EffectOracle, EffectSet, LogicalCall, LogicalExpr, LogicalInfo, LogicalOp, effects_of,
};
use crate::expr::{Base, Function};

/// The registry's declarations, with each user-defined function call
/// answering for the call alone: the oracle of a summary that names the
/// functions it calls rather than folding their bodies in
/// ([`crate::exec::vm::footprint`]).
struct LateBound<'a>(Oracle<'a>);

impl EffectOracle for LateBound<'_> {
	fn builtin(&self, name: &str) -> Option<EffectSet> {
		self.0.builtin(name)
	}

	fn closure_args(&self, name: &str) -> Option<&'static [usize]> {
		self.0.closure_args(name)
	}

	fn method_always_dispatches(&self, name: &str) -> bool {
		self.0.method_always_dispatches(name)
	}

	fn evaluates_against_row(&self, name: &str) -> bool {
		self.0.evaluates_against_row(name)
	}

	fn permission_checked_param(&self, name: &str) -> bool {
		self.0.permission_checked_param(name)
	}

	fn custom_function(&self, _name: &str) -> Option<EffectSet> {
		Some(EffectSet::READS_ROW)
	}
}

/// [`effects_of_plan`], with each user-defined function call answering for
/// the call alone ([`LateBound`]).
pub(crate) fn effects_of_plan_late_bound(
	op: &LogicalOp,
	registry: &FunctionRegistry,
) -> PlanEffects {
	let oracle = LateBound(Oracle::new(registry));
	let mut out = PlanEffects::default();
	fold_plan(op, &oracle, &mut out);
	out
}

/// The registry's declarations. Every parameter read is taken to be in hand,
/// so `Effects::may_raise` is not the question to ask through this one; see
/// [`ParamsOnly`].
pub(super) struct Oracle<'a> {
	registry: &'a FunctionRegistry,
}

impl<'a> Oracle<'a> {
	pub(super) fn new(registry: &'a FunctionRegistry) -> Self {
		Self {
			registry,
		}
	}
}

impl EffectOracle for Oracle<'_> {
	fn builtin(&self, name: &str) -> Option<EffectSet> {
		self.registry.get(name).map(|function| function.effects())
	}

	fn closure_args(&self, name: &str) -> Option<&'static [usize]> {
		self.registry.get(name).map(|function| function.closure_args())
	}

	fn method_always_dispatches(&self, name: &str) -> bool {
		self.registry.get_method(name).is_some_and(|d| d.dispatches_for_an_object())
	}

	fn evaluates_against_row(&self, name: &str) -> bool {
		self.registry.is_projection(name)
	}

	fn permission_checked_param(&self, _name: &str) -> bool {
		false
	}
}

/// The bind pass's classification of which parameter reads fetch a `DEFINE
/// PARAM` judged per read, and nothing of the registry: what `may_raise` is
/// asked with, by callers that hold no registry. `Effects::may_raise`
/// consults nothing else of the oracle; the other fields of an `Effects`
/// computed through this are not answers and are not read.
pub(super) struct ParamsOnly<'a>(pub(super) &'a HashSet<String>);

impl EffectOracle for ParamsOnly<'_> {
	fn builtin(&self, _name: &str) -> Option<EffectSet> {
		None
	}

	fn closure_args(&self, _name: &str) -> Option<&'static [usize]> {
		None
	}

	fn method_always_dispatches(&self, _name: &str) -> bool {
		false
	}

	fn evaluates_against_row(&self, _name: &str) -> bool {
		false
	}

	fn permission_checked_param(&self, name: &str) -> bool {
		self.0.contains(name)
	}
}

/// The effects of `expr`, from the registry's declarations.
pub(crate) fn effects_of_expr(
	expr: &LogicalExpr,
	registry: &FunctionRegistry,
) -> crate::expr::logical::Effects {
	effects_of(expr, &Oracle::new(registry))
}

/// The effects of a closure body `body`: those of the expression and of every
/// plan written inside it, with what the builtins' declarations do not say. A
/// schema statement is [`EffectSet::WRITES_DATA`], since it writes the
/// catalog through the transaction. A statement reading rows, a graph or
/// reference lookup, and a step that fetches the record its base may name
/// are [`EffectSet::READS_DATA`]. What a unit of the body declares on its
/// head.
pub(crate) fn effects_of_body(body: &LogicalExpr, registry: &FunctionRegistry) -> EffectSet {
	use std::ops::ControlFlow;
	let mut out = PlanEffects::default();
	fold_scalar(body, &Oracle::new(registry), &mut out);
	let _ = body.visit_deep(&mut |expr| {
		if let LogicalExpr::Subquery(plan) = expr {
			let _ = plan.visit_ops_deep(&mut |op| {
				if matches!(op, LogicalOp::Ddl(_)) {
					out.set |= EffectSet::WRITES_DATA;
				}
				if matches!(op, LogicalOp::Source(_) | LogicalOp::Match(_) | LogicalOp::Write(_)) {
					out.set |= EffectSet::READS_DATA;
				}
				ControlFlow::<()>::Continue(())
			});
		}
		if matches!(expr, LogicalExpr::Lookup { .. })
			|| fetched_base(expr).is_some_and(crate::expr::logical::may_dereference)
		{
			out.set |= EffectSet::READS_DATA;
		}
		ControlFlow::<()>::Continue(())
	});
	out.set
}

/// The base of `expr` where `expr` is a step that fetches the record its
/// base names: a field read, `.*`, a destructure or a `[WHERE …]` filter.
fn fetched_base(expr: &LogicalExpr) -> Option<&LogicalExpr> {
	if let LogicalExpr::Access {
		base,
		..
	}
	| LogicalExpr::Each(base)
	| LogicalExpr::Destructure {
		base,
		..
	}
	| LogicalExpr::Filter {
		base,
		..
	} = expr
	{
		return Some(base.as_ref());
	}
	None
}

/// The effects of every scalar a plan evaluates, unioned.
///
/// [`effects_of`](crate::expr::logical::effects_of) answers for one expression;
/// this answers for a whole plan, by walking every scalar the plan holds —
/// including those inside nested plans — and combining what each declares.
///
/// The union is the conservative answer, and the right one for asking whether a
/// rewrite changed what a statement does: a rule may reshape the plan freely, but
/// the set of effects its scalars carry is not its to alter. A rule that drops a
/// call to something with `ENTROPY`, or evaluates one twice where the statement
/// asked for once, changes this answer.
///
/// `impure` is carried alongside the set, since a plan holding one expression
/// that may not be evaluated twice is itself one that may not be.
pub(crate) fn effects_of_plan(op: &LogicalOp, registry: &FunctionRegistry) -> PlanEffects {
	let oracle = Oracle::new(registry);
	let mut out = PlanEffects::default();
	fold_plan(op, &oracle, &mut out);
	out
}

/// [`effects_of_plan`]'s walk, which descends through nested plans.
///
/// `visit_scalars_deep` stops at a [`LogicalExpr::Subquery`] — the plan inside is
/// the caller's to walk — so a statement written in expression position, such as
/// a `CREATE` in a projection, carries its effects here only because this
/// descends into it. Without that the fold reports such a statement as merely
/// impure, and a rule that moved or dropped the write inside it would go unseen.
fn fold_plan(op: &LogicalOp, oracle: &dyn EffectOracle, out: &mut PlanEffects) {
	// This node's own scalars, and any plan written inside one of them.
	let walked: Result<(), std::convert::Infallible> = op.visit_scalars(&mut |scalar| {
		fold_scalar(scalar, oracle, out);
		Ok(())
	});
	// The callback cannot fail, so the walk cannot.
	let Ok(()) = walked;

	// A mutation's write is the operator's, not any scalar's, so the fold above
	// cannot see it.
	if matches!(op, LogicalOp::Write(_)) {
		out.set |= EffectSet::WRITES_DATA;
	}

	for child in op.children() {
		fold_plan(child, oracle, out);
	}
}

/// `expr`'s own effects, and those of every plan written inside it.
fn fold_scalar(expr: &LogicalExpr, oracle: &dyn EffectOracle, out: &mut PlanEffects) {
	let effects = effects_of(expr, oracle);
	out.set |= effects.set;
	out.impure |= effects.impure;
	out.index_operator |= effects.index_operator;
	if let LogicalExpr::Subquery(plan) = expr {
		fold_plan(plan, oracle, out);
	}
	expr.for_each_child(&mut |child| fold_scalar(child, oracle, out));
}

/// What [`effects_of_plan`] answers: the union over a plan's scalars.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlanEffects {
	/// The union of every scalar's declared effects.
	pub set: EffectSet,
	/// Whether some scalar forbids evaluating it twice, or not at all, in place
	/// of once.
	pub impure: bool,
	/// Whether some scalar holds a `MATCHES` or `KNN`, which an access path
	/// answers rather than evaluation.
	pub index_operator: bool,
}

// ---------------------------------------------------------------------------
// The transaction and session level an expression needs
// ---------------------------------------------------------------------------

/// What evaluating an expression asks of the transaction and the session: the
/// access mode it needs and the namespace and database it must have selected.
///
/// The executor validates both before it runs a program, so each is the
/// maximum over every node the expression holds that raises it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Needs {
	pub access_mode: AccessMode,
	pub context: ContextLevel,
}

impl Needs {
	/// What an expression that reads no row and calls nothing needs.
	pub(crate) const NOTHING: Self = Self {
		access_mode: AccessMode::ReadOnly,
		context: ContextLevel::Root,
	};

	/// What an expression needs that holds both `self` and `other`.
	pub(crate) fn join(self, other: Self) -> Self {
		Self {
			access_mode: self.access_mode.combine(other.access_mode),
			context: self.context.max(other.context),
		}
	}

	fn at(context: ContextLevel) -> Self {
		Self {
			access_mode: AccessMode::ReadOnly,
			context,
		}
	}

	fn writing(self) -> Self {
		Self {
			access_mode: AccessMode::ReadWrite,
			..self
		}
	}
}

/// Collects into `out` the stored functions `expr` calls — `fn::`, module and
/// silo functions — including those a statement written inside it calls.
pub(crate) fn stored_functions_of(expr: &LogicalExpr, out: &mut Vec<Function>) {
	if let LogicalExpr::Call(call) = expr
		&& matches!(
			call.receiver,
			Function::Custom(_) | Function::Module(..) | Function::Silo { .. }
		) && !out.contains(&call.receiver)
	{
		out.push(call.receiver.clone());
	}
	if let LogicalExpr::Subquery(op) = expr {
		let walked: Result<(), std::convert::Infallible> = op.visit_scalars_deep(&mut |scalar| {
			stored_functions_of(scalar, out);
			Ok(())
		});
		let Ok(()) = walked;
	}
	expr.for_each_child(&mut |child| stored_functions_of(child, out));
}

/// What `expr` needs of the transaction and the session.
///
/// `stored` answers for a stored function the access mode its body needs,
/// which the catalog decides and the expression cannot.
pub(crate) fn needs_of_expr(
	expr: &LogicalExpr,
	registry: &FunctionRegistry,
	stored: &dyn Fn(&Function) -> AccessMode,
) -> Needs {
	NeedsFold {
		registry,
		stored,
	}
	.expr(expr)
}

struct NeedsFold<'a> {
	registry: &'a FunctionRegistry,
	stored: &'a dyn Fn(&Function) -> AccessMode,
}

impl NeedsFold<'_> {
	/// What `expr`'s own children need, joined.
	fn children(&self, expr: &LogicalExpr) -> Needs {
		let mut needs = Needs::NOTHING;
		expr.for_each_child(&mut |child| needs = needs.join(self.expr(child)));
		needs
	}

	fn expr(&self, expr: &LogicalExpr) -> Needs {
		use ContextLevel::{Database, Root};
		match expr {
			LogicalExpr::Literal(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::RepeatRecurse(_) => Needs::NOTHING,
			// A field read of a record id fetches the record.
			LogicalExpr::Field(_) => Needs::at(Database),
			LogicalExpr::Access {
				..
			}
			| LogicalExpr::Each(_)
			| LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::Recurse(_) => self.children(expr).join(Needs::at(Database)),
			// A closure's body runs where the closure is called: the literal
			// needs nothing of the session, and of the transaction what its
			// body needs.
			LogicalExpr::Closure(closure) => Needs {
				access_mode: self.expr(&closure.body).access_mode,
				context: Root,
			},
			LogicalExpr::Invoke {
				target,
				..
			} => {
				let needs = self.children(expr);
				if matches!(**target, LogicalExpr::Closure(_)) {
					needs
				} else {
					needs.writing()
				}
			}
			LogicalExpr::Method {
				name,
				..
			} => {
				let needs = self.children(expr);
				if self.registry.get_method(name.as_str()).is_some() {
					needs
				} else {
					needs.writing()
				}
			}
			LogicalExpr::Call(call) => self.call(call),
			LogicalExpr::Subquery(op) => self.plan(op),
			LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Optional {
				..
			}
			| LogicalExpr::Broadcast {
				..
			}
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Prefix {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Binary {
				..
			}
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_) => self.children(expr),
		}
	}

	fn call(&self, call: &LogicalCall) -> Needs {
		let args = call.args.iter().fold(Needs::NOTHING, |needs, arg| needs.join(self.expr(arg)));
		match &call.receiver {
			Function::Normal(name) => {
				let mut needs = args;
				if crate::expr::method::is_writer_builtin(name) {
					needs = needs.writing();
				}
				let declared = if self.registry.is_projection(name) {
					self.registry
						.get_projection(name)
						.map_or(ContextLevel::Database, |f| f.required_context())
				} else if self.registry.is_index_function(name) {
					self.registry
						.get_index_function(name)
						.map_or(ContextLevel::Database, |f| f.required_context())
				} else {
					self.registry.get(name).map_or(ContextLevel::Root, |f| f.required_context())
				};
				needs.join(Needs::at(declared))
			}
			Function::Custom(_) => Needs {
				access_mode: (self.stored)(&call.receiver),
				context: ContextLevel::Database,
			}
			.join(args),
			// A script runs in the session it is called in, so it asks for no
			// level of its own, and its arguments' levels are not its own.
			Function::Script(_) => Needs {
				access_mode: AccessMode::ReadWrite,
				context: ContextLevel::Root,
			},
			Function::Model(_) => args.join(Needs::at(ContextLevel::Database)),
			Function::Module(..)
			| Function::Silo {
				..
			} => Needs {
				access_mode: (self.stored)(&call.receiver),
				context: ContextLevel::Database,
			}
			.join(args),
		}
	}

	/// What a statement written in expression position needs: a write wherever
	/// the plan holds a write, a schema statement, a `SLEEP` or an expression
	/// that writes, and a selected database wherever it reads or writes a
	/// table.
	fn plan(&self, op: &LogicalOp) -> Needs {
		let mut needs = Needs::NOTHING;
		// `EXPLAIN` of a plan it does not run needs nothing of what the plan
		// holds.
		if let LogicalOp::Explain(explain) = op
			&& !explain.analyze
		{
			return needs;
		}
		if matches!(op, LogicalOp::Write(_) | LogicalOp::Ddl(_) | LogicalOp::Sleep(_)) {
			needs = needs.writing();
		}
		// A schema statement and a `SLEEP` resolve the session themselves and
		// ask for no level of it.
		if matches!(op, LogicalOp::Source(_) | LogicalOp::Write(_)) {
			needs = needs.join(Needs::at(ContextLevel::Database));
		}
		// `*` dereferences record ids.
		if let LogicalOp::Project(project) = op
			&& project.fields.has_all_selection()
		{
			needs = needs.join(Needs::at(ContextLevel::Database));
		}
		if let LogicalOp::Info(info) = op {
			needs = needs.join(Needs::at(match &**info {
				LogicalInfo::Root {
					..
				} => ContextLevel::Root,
				LogicalInfo::Ns {
					..
				} => ContextLevel::Namespace,
				LogicalInfo::User {
					base,
					..
				} => match base {
					Some(Base::Root) | None => ContextLevel::Root,
					Some(Base::Ns) => ContextLevel::Namespace,
					Some(Base::Db) => ContextLevel::Database,
				},
				LogicalInfo::Db {
					..
				}
				| LogicalInfo::Table {
					..
				}
				| LogicalInfo::Index {
					..
				} => ContextLevel::Database,
			}));
		}
		op.for_each_scalar(&mut |scalar| needs = needs.join(self.expr(scalar)));
		for child in op.children() {
			needs = needs.join(self.plan(child));
		}
		needs
	}
}
