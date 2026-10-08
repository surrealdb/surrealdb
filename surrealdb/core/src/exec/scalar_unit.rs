//! A stored expression evaluated as a VM unit.
//!
//! An expression the engine evaluates per row outside a statement's own
//! program — an index's column or `COUNT` guard, a permission guard, a
//! computed field derived where no read unit serves the read — runs as the
//! body of a closure that takes no argument ([`ScalarUnit`]): the unit the
//! closure machinery lowers for the invoking frame and `VERSION` stamp, and
//! the compile cache keeps by the expression as written
//! ([`crate::exec::vm::closure`]). No operator-tree expression is evaluated.
//!
//! The unit reads the row it is evaluated over as its document, the
//! parameters the expression names from where it is evaluated — a block's
//! local, a write's binding (`$before`, `$after`, `$value`, `$input`,
//! `$event`) or a value of the context — and the session's own values
//! (`$auth`, `$access`, `$token`, `$session`) from its frame when it runs.

use std::sync::Arc;

use surrealdb_cnf::PROTECTED_PARAM_NAMES;
use surrealdb_types::ToSql;

use crate::dbs::Variables;
use crate::exec::physical_expr::EvalContext;
use crate::exec::physical_expr::function::CompiledClosure;
use crate::exec::vm::ClosureDigest;
use crate::expr::logical::{LogicalClosure, LogicalExpr};
use crate::expr::{ControlFlow, FlowResult};
use crate::val::Value;

/// One stored expression, evaluated as the body of a closure that takes no
/// argument.
#[derive(Debug, Clone)]
pub(crate) struct ScalarUnit {
	closure: Arc<LogicalClosure>,
	digest: ClosureDigest,
	/// The names the expression reads as parameters, protected names aside.
	params: Vec<String>,
	/// Whether the expression is a permission predicate, which has nowhere to
	/// send a `RETURN`, `BREAK` or `CONTINUE`: one leaving it is an error, and
	/// never the predicate's verdict.
	predicate: bool,
}

impl ScalarUnit {
	/// The unit of `expr`.
	pub(crate) fn new(expr: LogicalExpr) -> Self {
		let mut params: Vec<String> = Vec::new();
		let _ = expr.visit_deep(&mut |node| {
			if let LogicalExpr::Param(param) = node {
				let name = param.as_str();
				if !PROTECTED_PARAM_NAMES.contains(&name) && !params.iter().any(|n| n == name) {
					params.push(name.to_owned());
				}
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		let body_sql: Arc<str> = Arc::from(expr.to_sql());
		let closure = LogicalClosure {
			args: Vec::new(),
			returns: None,
			body: expr,
			body_sql,
		};
		Self {
			digest: ClosureDigest::of(&closure),
			closure: Arc::new(closure),
			params,
			predicate: false,
		}
	}

	/// The unit of permission predicate `expr`.
	pub(crate) fn predicate(expr: LogicalExpr) -> Self {
		Self {
			predicate: true,
			..Self::new(expr)
		}
	}

	/// The expression's value evaluated in `ctx`, over its document root, or
	/// its current value where it has none: a `RETURN` is the value, except in
	/// a permission predicate, where a signal leaving the expression is the
	/// error the permission operator reports for it.
	pub(crate) async fn evaluate(&self, ctx: &EvalContext<'_>) -> FlowResult<Value> {
		let mut captures = Variables::new();
		for name in &self.params {
			let bound = ctx
				.local_params
				.and_then(|params| params.get(name.as_str()))
				.or_else(|| ctx.write_binding(name))
				.or_else(|| ctx.exec_ctx.value(name));
			if let Some(value) = bound {
				captures.insert(name.as_str(), value.clone());
			}
		}
		let value = CompiledClosure::planned(&self.closure, self.digest, captures).without_call();
		if !self.predicate {
			return Box::pin(value.invoke(ctx, Vec::new())).await;
		}
		match Box::pin(value.evaluate_body(ctx, Vec::new())).await {
			Ok(value) => Ok(value),
			Err(ControlFlow::Err(e)) => Err(ControlFlow::Err(e)),
			Err(signal) => {
				Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
					format!("unexpected control flow in a permission predicate: {signal}"),
				))))
			}
		}
	}
}

/// A stored clause whose evaluation runs its [`ScalarUnit`]: the operator
/// tree's compilation it holds answers what the clause does to the
/// transaction and what context it needs, and is never evaluated.
#[derive(Debug)]
pub(crate) struct UnitClause {
	unit: ScalarUnit,
	compiled: Arc<dyn crate::exec::physical_expr::PhysicalExpr>,
}

impl UnitClause {
	/// `expr`, compiled to `compiled`, evaluated as a unit.
	pub(crate) fn of(
		expr: LogicalExpr,
		compiled: Arc<dyn crate::exec::physical_expr::PhysicalExpr>,
	) -> Arc<dyn crate::exec::physical_expr::PhysicalExpr> {
		// A constant is its value.
		if let Some(value) = compiled.try_literal() {
			return Arc::new(crate::exec::physical_expr::Literal(value.clone()));
		}
		Arc::new(Self {
			unit: ScalarUnit::new(expr),
			compiled,
		})
	}

	/// Permission predicate `expr`, compiled to `compiled`, evaluated as a
	/// unit ([`ScalarUnit::predicate`]).
	pub(crate) fn predicate(
		expr: LogicalExpr,
		compiled: Arc<dyn crate::exec::physical_expr::PhysicalExpr>,
	) -> Arc<dyn crate::exec::physical_expr::PhysicalExpr> {
		if let Some(value) = compiled.try_literal() {
			return Arc::new(crate::exec::physical_expr::Literal(value.clone()));
		}
		Arc::new(Self {
			unit: ScalarUnit::predicate(expr),
			compiled,
		})
	}
}

impl ToSql for UnitClause {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		self.compiled.fmt_sql(f, fmt);
	}
}

impl crate::exec::physical_expr::PhysicalExpr for UnitClause {
	/// The physical form the clause compiled to, which its unit runs.
	fn name(&self) -> &'static str {
		self.compiled.name()
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		self.compiled.required_context()
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> crate::exec::BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move { self.unit.evaluate(&ctx).await })
	}

	fn access_mode(&self) -> crate::exec::AccessMode {
		self.compiled.access_mode()
	}

	fn try_simple_field(&self) -> Option<&str> {
		self.compiled.try_simple_field()
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}
