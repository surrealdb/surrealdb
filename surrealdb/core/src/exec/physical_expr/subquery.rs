use std::sync::Arc;

use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::fan_out::evaluate_each;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel, ExecOperator, ExecutionContext};
use crate::expr::logical::{LogicalExpr, LogicalOp};
use crate::expr::{ControlFlow, FlowResult};
use crate::val::{Array, Value};

/// Scalar subquery - (SELECT ... LIMIT 1)
///
/// The plan is a bytecode program ([`crate::exec::vm::plan_checked`]), planned
/// for the transaction and principal of the planner that compiled the
/// expression.
#[derive(Debug, Clone)]
pub struct ScalarSubquery {
	pub(crate) plan: Arc<dyn ExecOperator>,
	/// Whether the plan is a statement — a block, `RETURN`, `LET`, `FOR` —
	/// whose expressions read the enclosing row as their document, the way
	/// `$this` denotes it. A relational plan (a nested `SELECT`) supplies its
	/// own rows and reaches the enclosing one only through `$parent`/`$this`.
	pub(crate) inherits_row: bool,
}
impl PhysicalExpr for ScalarSubquery {
	fn name(&self) -> &'static str {
		"ScalarSubquery"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Delegate to the subquery plan's context requirements
		self.plan.required_context()
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, crate::expr::FlowResult<Value>> {
		Box::pin(run_subquery(&self.plan, self.inherits_row, ctx))
	}

	/// Batch evaluation for subqueries.
	///
	/// Each row runs the subquery plan, so the mode is the plan's own: a
	/// mutating subquery is evaluated one row at a time.
	fn evaluate_batch<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		values: &'a [Value],
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		Box::pin(evaluate_each(ctx.exec_ctx, self.plan.access_mode(), values, move |value| {
			// Bind both current value and document root so `$parent` / nested
			// subqueries match single-row evaluation (#7154).
			self.evaluate(ctx.with_value_and_doc(value))
		}))
	}

	fn access_mode(&self) -> AccessMode {
		// CRITICAL: Propagate the subquery's access mode!
		// This is why `SELECT *, (UPSERT person) FROM person` is ReadWrite
		self.plan.access_mode()
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		vec![("subquery", &self.plan)]
	}
}

/// The value of subquery `plan` evaluated in `ctx`: the plan run with the
/// enclosing row bound as `$parent` and `$this` — and as the current row when
/// `inherits_row` — and its rows collected, or its one value for a scalar
/// plan.
pub(crate) async fn run_subquery(
	plan: &Arc<dyn ExecOperator>,
	inherits_row: bool,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let subquery_ctx = subquery_context(&ctx, inherits_row);

	// Run on the frame stack of the statement's driver, when there is one:
	// the plan owns what it runs with, so it does not hold this evaluation's
	// frames on the native stack beneath its own.
	crate::exec::trampoline::nest(collect_subquery(Arc::clone(plan), subquery_ctx)).await
}

/// The context a subquery evaluated in `ctx` runs in.
///
/// The enclosing row is bound as `$parent` and `$this`, which lets a
/// correlated subquery such as `(SELECT ... FROM $this.field)` reach the outer
/// document, and is the current row when `inherits_row`. `document_root` is
/// preferred over `current_value` when both are set: nested evaluation (view
/// projections, graph traversals) may advance `current_value` along a field
/// chain while `document_root` still denotes the outer row `$parent` names.
/// The permission-skip flag carries in, so that inner plans executed during a
/// permission predicate do not re-enter table permission checks, which would
/// recurse on cyclic record links. A statement evaluated inside a
/// computed-field body inherits the record being computed, so a dereference of
/// that record in the statement is answered from the row rather than by
/// re-fetching it, which would evaluate the body again.
pub(crate) fn subquery_context(ctx: &EvalContext<'_>, inherits_row: bool) -> ExecutionContext {
	let outer_row = ctx.document_root.or(ctx.current_value);
	let subquery_ctx = if let Some(parent_value) = outer_row {
		let derived = ctx
			.exec_ctx
			.with_param("parent", parent_value.clone())
			.with_param("this", parent_value.clone());
		if inherits_row {
			derived.with_current_row(Arc::new(parent_value.clone()))
		} else {
			derived
		}
	} else {
		ctx.exec_ctx.clone()
	};
	let subquery_ctx = if ctx.skip_fetch_perms {
		subquery_ctx.with_skip_fetch_perms(true)
	} else {
		subquery_ctx
	};
	match &ctx.computing_record {
		Some(record) => subquery_ctx.with_computing_record(Some(Arc::clone(record))),
		None => subquery_ctx,
	}
}

/// The rows of subquery `plan` run in `ctx`, or its one value for a scalar
/// plan.
async fn collect_subquery(plan: Arc<dyn ExecOperator>, ctx: ExecutionContext) -> FlowResult<Value> {
	let mut stream = plan.execute(&ctx)?;
	let mut values = Vec::new();
	while let Some(batch_result) = stream.next().await {
		match batch_result {
			Ok(batch) => values.extend(batch.into_values()),
			Err(ctrl) => return Err(ctrl),
		}
	}
	// A scalar plan (`SELECT ... FROM ONLY`) is its one value, or NONE.
	if plan.output_shape().is_scalar() {
		Ok(values.pop().unwrap_or(Value::None))
	} else {
		Ok(Value::Array(Array(values)))
	}
}

/// A statement in scalar position compiled by a planner that holds no
/// transaction: a compilation made for what the statement does to the
/// transaction and what context it needs — a closure literal's body, a stored
/// clause compiled before a statement runs — which nothing evaluates. The
/// expression holding it runs as a VM unit lowered over a transaction
/// ([`crate::exec::scalar_unit`], [`crate::exec::vm::closure`]), which plans
/// the statement with it.
pub struct UnplannedSubquery {
	op: &'static str,
	/// What the statement asks of the transaction and the session, over every
	/// stored function it may call.
	needs: crate::exec::logical::Needs,
}

impl std::fmt::Debug for UnplannedSubquery {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("UnplannedSubquery").field("op", &self.op).finish()
	}
}

impl UnplannedSubquery {
	/// The statement `op`, over `registry`'s functions.
	pub(crate) fn new(op: &LogicalOp, registry: &crate::exec::function::FunctionRegistry) -> Self {
		let scalar = LogicalExpr::Subquery(Box::new(op.clone()));
		// A stored function's body is read from the catalog, which this
		// compilation cannot do: it may write.
		let needs =
			crate::exec::logical::needs_of_expr(&scalar, registry, &|_| AccessMode::ReadWrite);
		Self {
			op: op.name(),
			needs,
		}
	}
}

impl PhysicalExpr for UnplannedSubquery {
	fn name(&self) -> &'static str {
		"UnplannedSubquery"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		self.needs.context
	}

	fn evaluate<'a>(&'a self, _ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
				"a statement compiled with no transaction was evaluated".to_owned(),
			))))
		})
	}

	fn access_mode(&self) -> AccessMode {
		self.needs.access_mode
	}
}

impl ToSql for UnplannedSubquery {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push('(');
		f.push_str(self.op);
		f.push(')');
	}
}

impl ToSql for ScalarSubquery {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		// ExecOperator doesn't require ToSql, so we use the plan name
		// as a best-effort representation for display/EXPLAIN output.
		f.push('(');
		f.push_str(self.plan.display_name());
		f.push(')');
	}
}
