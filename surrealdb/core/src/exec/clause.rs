//! A stored expression the engine evaluates.
//!
//! [`Clause`] is the one type every holder of a stored expression keeps — an
//! index column, a permission guard, a computed field, a scan predicate, a
//! range bound, a write's value. It is cheap to clone: each variant shares its
//! payload.
//!
//! A clause is one of:
//!
//! - a constant, which evaluates to its value without reading the context;
//! - a [`ScalarUnit`], the expression run as a VM unit, described by the [`ScalarMetadata`] the
//!   planner read from its logical form;
//! - a [`ComputedFieldGuard`], a clause judged over a row whose computed fields are derived first;
//! - a clause the engine cannot compile, which fails when it is evaluated.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::permission::ComputedFieldGuard;
use crate::exec::scalar_unit::ScalarUnit;
use crate::exec::{Error as ExecError, EvalContext};
use crate::expr::FlowResult;
use crate::expr::logical::LogicalExpr;
use crate::val::Value;

/// An inert description of a scalar expression: the access mode it needs of
/// the transaction, the constant it folds to if it is one, and its canonical
/// SQL text. It evaluates nothing; the clause built from it runs the
/// expression's VM unit.
#[derive(Debug)]
pub(crate) struct ScalarMetadata {
	expr: LogicalExpr,
	literal: Option<Value>,
}

impl ScalarMetadata {
	/// The description of `expr`, which folds to `literal` where it is a
	/// constant.
	pub(crate) fn new(expr: LogicalExpr, literal: Option<Value>) -> Self {
		Self {
			expr,
			literal,
		}
	}

	/// The value the expression folds to, if it is a constant.
	pub(crate) fn try_literal(&self) -> Option<&Value> {
		self.literal.as_ref()
	}
}

impl ToSql for ScalarMetadata {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.expr.fmt_sql(f, fmt);
	}
}

/// A clause whose evaluation runs its [`ScalarUnit`]; its metadata reports
/// transaction access and canonical expression text.
#[derive(Debug)]
pub(crate) struct UnitClause {
	unit: ScalarUnit,
	metadata: ScalarMetadata,
}

/// A stored expression the engine evaluates; see the module.
#[derive(Debug, Clone)]
pub(crate) enum Clause {
	/// A constant.
	Literal(Arc<Value>),
	/// An expression run as a VM unit.
	Unit(Arc<UnitClause>),
	/// A clause judged over a row with its computed fields derived.
	Guard(Arc<ComputedFieldGuard>),
	/// A clause the engine cannot compile, with the reason why.
	Uncompilable(Arc<str>),
}

impl Clause {
	/// The constant `value`.
	pub(crate) fn literal(value: Value) -> Self {
		Self::Literal(Arc::new(value))
	}

	/// `expr`, described by `metadata`, evaluated as a unit. A constant is its
	/// value.
	pub(crate) fn of(expr: LogicalExpr, metadata: ScalarMetadata) -> Self {
		if let Some(value) = metadata.try_literal() {
			return Self::literal(value.clone());
		}
		Self::Unit(Arc::new(UnitClause {
			unit: ScalarUnit::new(expr),
			metadata,
		}))
	}

	/// Permission predicate `expr`, described by `metadata`, evaluated as a
	/// unit ([`ScalarUnit::predicate`]). A constant is its value.
	pub(crate) fn predicate(expr: LogicalExpr, metadata: ScalarMetadata) -> Self {
		if let Some(value) = metadata.try_literal() {
			return Self::literal(value.clone());
		}
		Self::Unit(Arc::new(UnitClause {
			unit: ScalarUnit::predicate(expr),
			metadata,
		}))
	}

	/// The clause that cannot be compiled, for `reason`. Evaluating it fails
	/// with `PlannerUnsupported` naming the reason.
	///
	/// The engine compiles catalog clauses (table and field `PERMISSIONS`
	/// guards, `COMPUTED` and `DEFAULT` bodies, event `WHEN` and `THEN`,
	/// materialized view conditions, stored field lists) from their stored
	/// text. A clause with no logical form, or whose logical form holds a
	/// write that no operator compiles, is carried as this variant instead.
	/// Compiling is the decision to run a clause: a statement that never
	/// evaluates it is not refused, and one that does fails closed, so a
	/// permission guard that fails denies.
	pub(crate) fn uncompilable(reason: String) -> Self {
		Self::Uncompilable(Arc::from(reason))
	}

	/// The expression's value in `ctx`.
	///
	/// Returns `FlowResult<Value>` so that control flow (`BREAK`, `CONTINUE`,
	/// `RETURN`) propagates to the caller; an error is wrapped in
	/// `ControlFlow::Err`.
	pub(crate) async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		match self {
			Self::Literal(value) => Ok(Value::clone(value)),
			Self::Unit(clause) => clause.unit.evaluate(&ctx).await,
			Self::Guard(guard) => Box::pin(guard.evaluate(ctx)).await,
			Self::Uncompilable(reason) => Err(ExecError::PlannerUnsupported {
				reason: reason.to_string(),
			}
			.into()),
		}
	}

	/// The expression's value for each of `values`, in order, each evaluated
	/// as the current value of `ctx`, which holds none of its own.
	///
	/// The first control flow signal (`BREAK`, `CONTINUE`, `RETURN`, an error)
	/// aborts the batch.
	pub(crate) async fn evaluate_batch(
		&self,
		ctx: EvalContext<'_>,
		values: &[Value],
	) -> FlowResult<Vec<Value>> {
		if let Self::Guard(guard) = self {
			return Box::pin(guard.evaluate_batch(ctx, values)).await;
		}
		let mut results = Vec::with_capacity(values.len());
		for value in values {
			results.push(self.evaluate(ctx.with_value_and_doc(value)).await?);
		}
		Ok(results)
	}

	/// Whether `self` and `other` are one compiled clause, not two equal ones.
	#[cfg(test)]
	pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::Literal(a), Self::Literal(b)) => Arc::ptr_eq(a, b),
			(Self::Unit(a), Self::Unit(b)) => Arc::ptr_eq(a, b),
			(Self::Guard(a), Self::Guard(b)) => Arc::ptr_eq(a, b),
			(Self::Uncompilable(a), Self::Uncompilable(b)) => Arc::ptr_eq(a, b),
			_ => false,
		}
	}
}

impl ToSql for Clause {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Literal(value) => value.fmt_sql(f, fmt),
			Self::Unit(clause) => clause.metadata.fmt_sql(f, fmt),
			Self::Guard(guard) => guard.fmt_sql(f, fmt),
			Self::Uncompilable(_) => f.push_str("<uncompilable clause>"),
		}
	}
}
