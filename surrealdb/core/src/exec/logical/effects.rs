//! The registry as the oracle the logical layer's effect fold consults.
//!
//! `effects_of` (`crate::expr::logical`) derives what an expression does from
//! the declared effects of the builtins it calls. Those declarations live on
//! the registry, so this is where the fold and the registry meet; every
//! predicate the membership recogniser and the physical planner ask of an expression is a
//! field of the fold's answer, read through one of the two oracles here.

use super::summaries::CatalogSummaries;
use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{EffectOracle, EffectSet, LogicalExpr, LogicalOp, effects_of};

/// The registry's declarations. Every parameter read is taken to be in hand,
/// so `Effects::may_raise` is not the question to ask through this one.
pub(crate) struct Oracle<'a> {
	registry: &'a FunctionRegistry,
}

impl<'a> Oracle<'a> {
	pub(crate) fn new(registry: &'a FunctionRegistry) -> Self {
		Self {
			registry,
		}
	}
}

impl EffectOracle for Oracle<'_> {
	/// A scalar builtin answers its declared effects. An index function reads the row it is
	/// called on and the index state the statement's search or full-text clause holds for it.
	fn builtin(&self, name: &str) -> Option<EffectSet> {
		match self.registry.get(name) {
			Some(function) => Some(function.effects()),
			None => self
				.registry
				.is_index_function(name)
				.then_some(EffectSet::READS_ROW.union(EffectSet::READS_DATA)),
		}
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

/// The registry's declarations, with each stored function a [`CatalogSummaries`] holds answering
/// by its summary.
///
/// A call a summary answers for is [`EffectSet::READS_ROW`] and the summary's effects, and with
/// `opaque` also [`EffectSet::OPAQUE_BODY`]: a stored function called from a statement stays an
/// unseen body to the pipelines of that statement, whatever its summary says, and the summary
/// adds what the body does besides. A body folded into a summary, whose callees are the
/// definitions the summary is made of, takes `opaque` off. A call the summaries do not name
/// answers as the registry alone does.
pub(crate) struct SummaryOracle<'a> {
	inner: Oracle<'a>,
	summaries: &'a CatalogSummaries,
	opaque: bool,
}

impl<'a> SummaryOracle<'a> {
	pub(crate) fn registry(&self) -> &'a FunctionRegistry {
		self.inner.registry
	}

	pub(crate) fn summaries(&self) -> &'a CatalogSummaries {
		self.summaries
	}

	/// The oracle of the pipelines of a statement.
	pub(crate) fn new(registry: &'a FunctionRegistry, summaries: &'a CatalogSummaries) -> Self {
		Self {
			inner: Oracle::new(registry),
			summaries,
			opaque: true,
		}
	}

	/// The oracle of a body that a summary is made of: its callees' summaries are theirs alone.
	pub(super) fn of_body(registry: &'a FunctionRegistry, summaries: &'a CatalogSummaries) -> Self {
		Self {
			opaque: false,
			..Self::new(registry, summaries)
		}
	}
}

impl EffectOracle for SummaryOracle<'_> {
	fn builtin(&self, name: &str) -> Option<EffectSet> {
		self.inner.builtin(name)
	}

	fn closure_args(&self, name: &str) -> Option<&'static [usize]> {
		self.inner.closure_args(name)
	}

	fn method_always_dispatches(&self, name: &str) -> bool {
		self.inner.method_always_dispatches(name)
	}

	fn evaluates_against_row(&self, name: &str) -> bool {
		self.inner.evaluates_against_row(name)
	}

	fn permission_checked_param(&self, name: &str) -> bool {
		self.inner.permission_checked_param(name)
	}

	fn custom_function(&self, name: &str) -> Option<EffectSet> {
		let summary = self.summaries.function(name)?;
		let own = summary.effects().union(EffectSet::READS_ROW);
		Some(match self.opaque {
			true => own.union(EffectSet::OPAQUE_BODY),
			false => own,
		})
	}
}

/// The effects of `expr`, from the registry's declarations.
pub(crate) fn effects_of_expr(
	expr: &LogicalExpr,
	registry: &FunctionRegistry,
) -> crate::expr::logical::Effects {
	effects_of(expr, &Oracle::new(registry))
}

/// Whether evaluating `expr` may read the statement clock: it calls a builtin that declares
/// [`EffectSet::CLOCK`], applies a body the expression does not hold ([`EffectSet::OPAQUE_BODY`]),
/// or holds a statement, whose reads and writes of stored data run clauses (`DEFAULT`, `VALUE`,
/// events, computed fields, permissions) the expression does not show. A result of `false` is a
/// guarantee that no read of the clock is made on behalf of the expression.
pub(crate) fn may_read_clock(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	use std::ops::ControlFlow;
	let oracle = Oracle::new(registry);
	if effects_of(expr, &oracle).set.intersects(EffectSet::CLOCK.union(EffectSet::OPAQUE_BODY)) {
		return true;
	}
	expr.visit_deep(&mut |inner| {
		if matches!(inner, LogicalExpr::Subquery(_)) {
			ControlFlow::Break(())
		} else {
			ControlFlow::Continue(())
		}
	})
	.is_break()
}

/// The effects of a closure body `body`: those of the expression and of every
/// plan written inside it, with what the builtins' declarations do not say. A
/// schema statement is [`EffectSet::WRITES_DATA`], since it writes the
/// catalog through the transaction. A statement reading rows, a graph or
/// reference lookup, and a step that fetches the record its base may name
/// are [`EffectSet::READS_DATA`]. The builtins the body calls add the effects
/// their declarations state, as [`effects_of`] reads them.
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

/// [`effects_of_plan`] as `oracle` answers.
pub(crate) fn effects_of_plan_with(op: &LogicalOp, oracle: &dyn EffectOracle) -> PlanEffects {
	let mut out = PlanEffects::default();
	fold_plan(op, oracle, &mut out);
	out
}

/// What evaluating `expr` does, as `oracle` answers: the effects [`effects_of`] derives, and
/// those of everything the expression reaches that the declarations of the builtins do not state.
///
/// - Every statement written in the expression ([`plan_effects`]).
/// - The implementations of every method it calls ([`FunctionRegistry::method_effects`]).
/// - The body of every `COMPUTED` field it reads by name: a read of the name is a read of the field
///   of every table the summaries hold a field of that name for, since the expression does not say
///   which table's row it is evaluated against.
pub(crate) fn scalar_effects(expr: &LogicalExpr, oracle: &SummaryOracle<'_>) -> EffectSet {
	use std::ops::ControlFlow;
	let mut set = effects_of(expr, oracle).set;
	let _: ControlFlow<()> = expr.visit_deep(&mut |inner| {
		if let LogicalExpr::Subquery(plan) = inner {
			set = set
				.union(effects_of_plan_with(plan, oracle).set)
				.union(blocking_effects(plan))
				.union(permission_effects(plan, oracle));
		} else {
			set = set.union(reached_effects(inner, oracle));
		}
		ControlFlow::Continue(())
	});
	set
}

/// What running the statement `op` does, as `oracle` answers: the effects of every scalar of
/// every operator and of the statements nested in them ([`effects_of_plan_with`]), the effects
/// [`scalar_effects`] adds for the methods and `COMPUTED` fields they reach, and the `COMPUTED`
/// fields a table permission of the statement derives for itself ([`permission_computed`]).
pub(crate) fn plan_effects(op: &LogicalOp, oracle: &SummaryOracle<'_>) -> EffectSet {
	use std::ops::ControlFlow;
	let mut set = effects_of_plan_with(op, oracle)
		.set
		.union(blocking_effects(op))
		.union(permission_effects(op, oracle));
	let walked: Result<(), std::convert::Infallible> = op.visit_scalars_deep(&mut |scalar| {
		let _: ControlFlow<()> = scalar.visit_deep(&mut |inner| {
			set = set.union(reached_effects(inner, oracle));
			ControlFlow::Continue(())
		});
		Ok(())
	});
	let Ok(()) = walked;
	set
}

/// [`plan_effects`] of the write statement `op`, without the write `op` itself makes.
///
/// A write that applies its effects in iteration order writes through that apply and nothing
/// else: the statement's remaining effects are those of its expressions, of the statements nested
/// in them and of the `COMPUTED` fields its permissions derive. A nested write, a function that
/// writes and an `eval` still count. `op` that is no write answers [`plan_effects`].
pub(crate) fn plan_effects_beneath_write(op: &LogicalOp, oracle: &SummaryOracle<'_>) -> EffectSet {
	use std::ops::ControlFlow;
	let LogicalOp::Write(_) = op else {
		return plan_effects(op, oracle);
	};
	let mut out = PlanEffects::default();
	let walked: Result<(), std::convert::Infallible> = op.visit_scalars(&mut |scalar| {
		fold_scalar(scalar, oracle, &mut out);
		Ok(())
	});
	let Ok(()) = walked;
	for child in op.children() {
		fold_plan(child, oracle, &mut out);
	}
	let mut set = out.set.union(blocking_effects(op)).union(permission_effects(op, oracle));
	let walked: Result<(), std::convert::Infallible> = op.visit_scalars_deep(&mut |scalar| {
		let _: ControlFlow<()> = scalar.visit_deep(&mut |inner| {
			set = set.union(reached_effects(inner, oracle));
			ControlFlow::Continue(())
		});
		Ok(())
	});
	let Ok(()) = walked;
	set
}

/// `BLOCKING` where the statement `op`, or a statement nested in it, is a `SLEEP`: the statement
/// blocks as the builtin of that name does, and no scalar says so.
fn blocking_effects(op: &LogicalOp) -> EffectSet {
	use std::ops::ControlFlow;
	let mut set = EffectSet::PURE;
	let _: ControlFlow<()> = op.visit_ops_deep(&mut |node| {
		if matches!(node, LogicalOp::Sleep(_)) {
			set = set.union(EffectSet::BLOCKING);
		}
		ControlFlow::Continue(())
	});
	set
}

/// What the node `expr` reaches beyond its own effects: the implementations of a method it
/// calls, and the body of a `COMPUTED` field it reads.
fn reached_effects(expr: &LogicalExpr, oracle: &SummaryOracle<'_>) -> EffectSet {
	if let LogicalExpr::Method {
		name,
		..
	} = expr
	{
		oracle.registry().method_effects(name.as_str())
	} else if let LogicalExpr::Field(name) = expr {
		oracle.summaries().computed_reads(name.as_str())
	} else {
		EffectSet::PURE
	}
}

/// The effects of the `COMPUTED` fields every table permission of the statement `op` derives for
/// itself.
fn permission_effects(op: &LogicalOp, oracle: &SummaryOracle<'_>) -> EffectSet {
	use std::ops::ControlFlow;
	let mut set = EffectSet::PURE;
	let _: ControlFlow<()> = op.visit_ops_deep(&mut |node| {
		let derived = permission_computed(node, oracle.registry(), oracle.summaries());
		set = set.union(derived.unwrap_or(EffectSet::PURE));
		ControlFlow::Continue(())
	});
	set
}

/// The effects of the `COMPUTED` fields the permission `node` derives for itself, or `None` when
/// `node` is no permission, or its predicate reads none of them, or its rows do not belong to a
/// table whose resolved schema names them.
///
/// A permission judges its predicate over a copy of the row on which it has derived the computed
/// fields the predicate reads; that is the one place a `COMPUTED` body runs that no projection of
/// the plan holds. A predicate that reads the row other than by name may read any of the table's.
pub(crate) fn permission_computed(
	node: &LogicalOp,
	registry: &FunctionRegistry,
	summaries: &CatalogSummaries,
) -> Option<EffectSet> {
	let LogicalOp::PermissionFilter(filter) = node else {
		return None;
	};
	let (table, schema) = crate::exec::planner::fold::permission_subject(&filter.input)?;
	let reads = crate::exec::planner::fold::computed_reads(&filter.predicate, &schema, registry)?;
	Some(match reads {
		Some(names) => names
			.iter()
			.fold(EffectSet::PURE, |set, name| set.union(summaries.computed_of(&table, name))),
		None => summaries.computed_of_table(&table),
	})
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
	// cannot see it: a write statement's, and a `MATCH`'s write stages'.
	let writes = matches!(op, LogicalOp::Write(_))
		|| matches!(op, LogicalOp::Match(matched) if matched.plan.has_mutations());
	if writes {
		out.set |= EffectSet::WRITES_DATA;
	}

	for child in op.children() {
		fold_plan(child, oracle, out);
	}
}

/// `expr`'s own effects, and those of every plan written inside it.
///
/// The effects of `expr` are those of the whole tree under it, a nested statement not entered:
/// each node joins its children's. They are computed once at the root, and the walk below it only
/// finds the nested statements.
fn fold_scalar(expr: &LogicalExpr, oracle: &dyn EffectOracle, out: &mut PlanEffects) {
	let effects = effects_of(expr, oracle);
	out.set |= effects.set;
	out.impure |= effects.impure;
	out.index_operator |= effects.index_operator;
	fold_nested_plans(expr, oracle, out);
}

/// The effects of every plan written inside `expr`, at any depth.
fn fold_nested_plans(expr: &LogicalExpr, oracle: &dyn EffectOracle, out: &mut PlanEffects) {
	if let LogicalExpr::Subquery(plan) = expr {
		fold_plan(plan, oracle, out);
	}
	expr.for_each_child(&mut |child| fold_nested_plans(child, oracle, out));
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

#[cfg(test)]
mod tests {
	use super::*;

	fn plan_effects_of(sql: &str) -> PlanEffects {
		let registry = FunctionRegistry::with_builtins();
		effects_of_plan_with(&super::super::lower_test(sql), &Oracle::new(&registry))
	}

	/// A statement written at any depth of a scalar's nesting is entered once, and the scalar's
	/// own effects are those of the whole tree under it.
	#[test]
	fn a_plan_nested_under_literals_is_folded() {
		let write = plan_effects_of("RETURN [{ a: [(UPDATE t SET x = 1)] }]");
		assert!(write.set.contains(EffectSet::WRITES_DATA));
		let read = plan_effects_of("RETURN [{ a: [(SELECT * FROM t)] }]");
		assert!(!read.set.contains(EffectSet::WRITES_DATA));
		let none = plan_effects_of("RETURN [{ a: [1, 2] }]");
		assert!(none.set.is_deterministic());
		assert!(!none.set.contains(EffectSet::WRITES_DATA));
	}

	/// A non-deterministic call deep in a scalar reaches the plan's effects.
	#[test]
	fn an_effect_deep_in_a_scalar_reaches_the_plan() {
		let deep = plan_effects_of("RETURN [{ a: [rand::float()] }]");
		assert!(!deep.set.is_deterministic());
		assert!(deep.impure);
		let nested = plan_effects_of("RETURN [{ a: [(SELECT VALUE rand::float() FROM t)] }]");
		assert!(!nested.set.is_deterministic());
	}
}
