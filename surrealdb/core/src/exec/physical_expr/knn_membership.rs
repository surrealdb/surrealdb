//! Physical expression for KNN (`<|k, …|>`) operators evaluated as per-row
//! predicates rather than lowered into a KNN source operator.
//!
//! The legacy executor evaluates a KNN expression per row as membership in
//! the result set collected by its KNN checker (`QueryExecutor::knn`), and as
//! `false` when no executor entry exists — projection position, bare
//! expressions, an `Approximate` form with no usable HNSW index, or a `K`
//! form whose query vector the planner cannot compute. The streaming
//! equivalent: the KNN source operators (`KnnScan`, `KnnTopK`) record every
//! yielded row in the per-statement [`KnnContext`], and this expression tests
//! the current row's record id against it. Without a [`KnnContext`] (no KNN
//! in the WHERE condition, or a non-SELECT context) it is always `false`.
//!
//! Operands are deliberately **not** evaluated: the legacy compute path for
//! KNN hands `fnc::operate::knn` the whole expression without computing
//! either side (unlike MATCHES, which receives computed operands), so an
//! erroring or side-effecting operand is never observed there either —
//! `point <|2,EUCLIDEAN|> (1 / 0)` in projection position yields `false`,
//! not a division error. Pinned in
//! `language-tests/tests/language/indexes/knn/knn_bruteforce_nonliteral_vector.surql`.
//!
//! Membership is stable under re-evaluation, so the same conjunct can safely
//! appear both in a KNN source's pushed-down condition and in a downstream
//! filter or projection.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::function::KnnContext;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel};
use crate::expr::FlowResult;
use crate::expr::operator::BinaryOperator;
use crate::val::Value;

/// Evaluates a KNN (`<|k, …|>`) predicate as membership in the statement's
/// KNN result set. See the module docs for the legacy-executor mapping.
pub struct KnnMembershipOp {
	/// Left side expression (kept for `ToSql` display; never evaluated —
	/// see the module docs).
	pub(crate) left: Arc<dyn PhysicalExpr>,
	/// Right side expression (kept for `ToSql` display; never evaluated).
	pub(crate) right: Arc<dyn PhysicalExpr>,
	/// The KNN operator (kept for `ToSql` display).
	pub(crate) operator: BinaryOperator,
	/// The statement's KNN distance context, bound at plan time (the
	/// execution context does not carry it). `None` when the enclosing
	/// statement has no KNN in its WHERE condition.
	knn_ctx: Option<Arc<KnnContext>>,
}

impl KnnMembershipOp {
	/// Create a new KnnMembershipOp.
	pub(crate) fn new(
		left: Arc<dyn PhysicalExpr>,
		right: Arc<dyn PhysicalExpr>,
		operator: BinaryOperator,
		knn_ctx: Option<Arc<KnnContext>>,
	) -> Self {
		Self {
			left,
			right,
			operator,
			knn_ctx,
		}
	}
}

impl PhysicalExpr for KnnMembershipOp {
	fn name(&self) -> &'static str {
		"KnnMembershipOp"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		self.left.required_context().max(self.right.required_context())
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let Some(knn_ctx) = &self.knn_ctx else {
				return Ok(Value::Bool(false));
			};
			let rid = match ctx.current_value {
				Some(Value::Object(obj)) => match obj.get("id") {
					Some(Value::RecordId(rid)) => rid.clone(),
					_ => return Ok(Value::Bool(false)),
				},
				Some(Value::RecordId(rid)) => rid.clone(),
				_ => return Ok(Value::Bool(false)),
			};
			Ok(Value::Bool(knn_ctx.get(&rid).await.is_some()))
		})
	}

	fn access_mode(&self) -> AccessMode {
		self.left.access_mode().combine(self.right.access_mode())
	}
}

impl ToSql for KnnMembershipOp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{} {} {}", self.left, self.operator, self.right);
	}
}

impl std::fmt::Debug for KnnMembershipOp {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("KnnMembershipOp")
			.field("operator", &self.operator)
			.field("bound", &self.knn_ctx.is_some())
			.finish()
	}
}
