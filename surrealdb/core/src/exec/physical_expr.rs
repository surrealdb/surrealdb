use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::{AccessMode, ExecutionContext, OperatorPlan};
use crate::expr::Idiom;
use crate::val::Value;

/// Evaluation context - what's available during expression evaluation.
///
/// This is a borrowed view into the execution context for expression evaluation.
/// It provides access to parameters, namespace/database names, and the current row
/// (for per-row expressions like filters and projections).
#[derive(Clone)]
pub struct EvalContext<'a> {
	pub exec_ctx: &'a ExecutionContext,

	/// Current row for per-row expressions (projections, filters).
	/// None when evaluating in "scalar context" (USE, LIMIT, TIMEOUT, etc.)
	pub current_value: Option<&'a Value>,
}

impl<'a> EvalContext<'a> {
	/// Convert from ExecutionContext enum for expression evaluation.
	///
	/// This extracts the appropriate fields based on the context level:
	/// - Root: params only, no ns/db/txn
	/// - Namespace: params, ns, txn
	/// - Database: params, ns, db, txn
	// pub(crate) fn from_exec_ctx(exec_ctx: &'a ExecutionContext) -> Self {
	// 	match exec_ctx {
	// 		ExecutionContext::Root(r) => Self::scalar(&r.params, None, None, None),
	// 		ExecutionContext::Namespace(n) => {
	// 			Self::scalar(&n.root.params, Some(&n.ns), None, Some(&n.txn))
	// 		}
	// 		ExecutionContext::Database(d) => Self::scalar(
	// 			&d.ns_ctx.root.params,
	// 			Some(&d.ns_ctx.ns),
	// 			Some(&d.db),
	// 			Some(&d.ns_ctx.txn),
	// 		),
	// 	}
	// }

	/// For session-level scalar evaluation (USE, LIMIT, etc.)
	pub(crate) fn from_exec_ctx(exec_ctx: &'a ExecutionContext) -> Self {
		Self {
			exec_ctx,
			current_value: None,
		}
	}

	/// For per-row evaluation (projections, filters)
	pub fn with_value(&self, value: &'a Value) -> Self {
		Self {
			current_value: Some(value),
			..*self
		}
	}
}

#[async_trait]
pub trait PhysicalExpr: ToSql + Send + Sync + Debug {
	fn name(&self) -> &'static str;

	/// Evaluate this expression to a value.
	///
	/// May execute subqueries internally, hence async.
	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value>;

	/// Does this expression reference the current row?
	/// If false, can be evaluated in scalar context.
	fn references_current_value(&self) -> bool;

	/// Returns the access mode for this expression.
	///
	/// This is critical for plan-based mutability analysis:
	/// - If an expression contains a mutation subquery, it must return `ReadWrite`
	/// - Example: `(UPSERT person)` in a SELECT must propagate `ReadWrite` upward
	fn access_mode(&self) -> AccessMode;
}

/// Literal value - "foo", 42, true
#[derive(Debug, Clone)]
pub struct Literal(pub(crate) Value);

#[async_trait]
impl PhysicalExpr for Literal {
	fn name(&self) -> &'static str {
		"Literal"
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		Ok(self.0.clone())
	}

	fn references_current_value(&self) -> bool {
		false
	}

	fn access_mode(&self) -> AccessMode {
		// Literals are always read-only
		AccessMode::ReadOnly
	}
}

impl ToSql for Literal {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.0.fmt_sql(f, fmt);
	}
}

/// Parameter reference - $foo
#[derive(Debug, Clone)]
pub struct Param(pub(crate) String);

#[async_trait]
impl PhysicalExpr for Param {
	fn name(&self) -> &'static str {
		"Param"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		ctx.exec_ctx
			.params()
			.get(self.0.as_str())
			.map(|v| (**v).clone())
			.ok_or_else(|| anyhow::anyhow!("Parameter not found: ${}", self.0))
	}

	fn references_current_value(&self) -> bool {
		false
	}

	fn access_mode(&self) -> AccessMode {
		// Parameter references are read-only
		AccessMode::ReadOnly
	}
}

impl ToSql for Param {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "${}", self.0)
	}
}

/// Field access - foo.bar.baz or just `age`
#[derive(Debug, Clone)]
pub struct Field(pub(crate) Idiom);

#[async_trait]
impl PhysicalExpr for Field {
	fn name(&self) -> &'static str {
		"Field"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let current = ctx
			.current_value
			.ok_or_else(|| anyhow::anyhow!("Field access requires current value"))?;

		// Simple synchronous field access - handles basic idioms
		// This does NOT use the old compute() system
		Ok(get_field_simple(current, &self.0.0))
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		// Field access is read-only
		AccessMode::ReadOnly
	}
}

impl ToSql for Field {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{}", self.0)
	}
}

/// Simple field access without async/compute machinery
/// Handles basic field paths like foo.bar.baz
fn get_field_simple(value: &Value, path: &[crate::expr::part::Part]) -> Value {
	use crate::expr::part::Part;

	if path.is_empty() {
		return value.clone();
	}

	match (value, &path[0]) {
		// Field access on object
		(Value::Object(obj), Part::Field(field_name)) => match obj.get(field_name.as_str()) {
			Some(v) => get_field_simple(v, &path[1..]),
			None => Value::None,
		},
		// Index access on array
		(Value::Array(arr), part) => {
			if let Some(idx) = part.as_old_index() {
				match arr.0.get(idx) {
					Some(v) => get_field_simple(v, &path[1..]),
					None => Value::None,
				}
			} else {
				// For other array operations, return None for now
				Value::None
			}
		}
		// Start part - evaluate the expression (simplified)
		(_, Part::Start(_expr)) => {
			// For simple cases, we can't evaluate expressions here
			// This would require the full compute machinery
			// For now, return None - this will need to be extended later
			Value::None
		}
		// For any other combination, return None
		_ => Value::None,
	}
}

/// Binary operation - left op right (e.g., age > 10)
#[derive(Debug, Clone)]
pub struct BinaryOp {
	pub(crate) left: Arc<dyn PhysicalExpr>,
	pub(crate) op: crate::expr::operator::BinaryOperator,
	pub(crate) right: Arc<dyn PhysicalExpr>,
}

#[async_trait]
impl PhysicalExpr for BinaryOp {
	fn name(&self) -> &'static str {
		"BinaryOp"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		use crate::expr::operator::BinaryOperator;
		use crate::fnc::operate;

		// Evaluate both sides (could parallelize if both are independent)
		let left = self.left.evaluate(ctx.clone()).await?;

		macro_rules! eval {
			($expr:expr) => {
				$expr.evaluate(ctx).await?
			};
		}

		// Apply the operator
		match &self.op {
			BinaryOperator::Add => operate::add(left, eval!(self.right)),
			BinaryOperator::Subtract => operate::sub(left, eval!(self.right)),
			BinaryOperator::Multiply => operate::mul(left, eval!(self.right)),
			BinaryOperator::Divide => operate::div(left, eval!(self.right)),
			BinaryOperator::Remainder => operate::rem(left, eval!(self.right)),
			BinaryOperator::Power => operate::pow(left, eval!(self.right)),

			BinaryOperator::Equal => operate::equal(&left, &eval!(self.right)),
			BinaryOperator::ExactEqual => operate::exact(&left, &eval!(self.right)),
			BinaryOperator::NotEqual => operate::not_equal(&left, &eval!(self.right)),
			BinaryOperator::AllEqual => operate::all_equal(&left, &eval!(self.right)),
			BinaryOperator::AnyEqual => operate::any_equal(&left, &eval!(self.right)),

			BinaryOperator::LessThan => operate::less_than(&left, &eval!(self.right)),
			BinaryOperator::LessThanEqual => operate::less_than_or_equal(&left, &eval!(self.right)),
			BinaryOperator::MoreThan => operate::more_than(&left, &eval!(self.right)),
			BinaryOperator::MoreThanEqual => operate::more_than_or_equal(&left, &eval!(self.right)),

			BinaryOperator::And => {
				// Short-circuit AND
				if !left.is_truthy() {
					Ok(left)
				} else {
					Ok(eval!(self.right))
				}
			}
			BinaryOperator::Or => {
				// Short-circuit OR
				if left.is_truthy() {
					Ok(left)
				} else {
					Ok(eval!(self.right))
				}
			}

			BinaryOperator::Contain => operate::contain(&left, &eval!(self.right)),
			BinaryOperator::NotContain => operate::not_contain(&left, &eval!(self.right)),
			BinaryOperator::ContainAll => operate::contain_all(&left, &eval!(self.right)),
			BinaryOperator::ContainAny => operate::contain_any(&left, &eval!(self.right)),
			BinaryOperator::ContainNone => operate::contain_none(&left, &eval!(self.right)),
			BinaryOperator::Inside => operate::inside(&left, &eval!(self.right)),
			BinaryOperator::NotInside => operate::not_inside(&left, &eval!(self.right)),
			BinaryOperator::AllInside => operate::inside_all(&left, &eval!(self.right)),
			BinaryOperator::AnyInside => operate::inside_any(&left, &eval!(self.right)),
			BinaryOperator::NoneInside => operate::inside_none(&left, &eval!(self.right)),

			BinaryOperator::Outside => operate::outside(&left, &eval!(self.right)),
			BinaryOperator::Intersects => operate::intersects(&left, &eval!(self.right)),

			BinaryOperator::NullCoalescing => {
				if !left.is_nullish() {
					Ok(left)
				} else {
					Ok(eval!(self.right))
				}
			}
			BinaryOperator::TenaryCondition => {
				// Same as OR for this context
				if left.is_truthy() {
					Ok(left)
				} else {
					Ok(eval!(self.right))
				}
			}

			// Range operators - create Range values
			BinaryOperator::Range => {
				// a..b means start: Included(a), end: Excluded(b)
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Included(left),
					end: std::ops::Bound::Excluded(eval!(self.right)),
				})))
			}
			BinaryOperator::RangeInclusive => {
				// a..=b means start: Included(a), end: Included(b)
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Included(left),
					end: std::ops::Bound::Included(eval!(self.right)),
				})))
			}
			BinaryOperator::RangeSkip => {
				// a>..b means start: Excluded(a), end: Excluded(b)
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Excluded(left),
					end: std::ops::Bound::Excluded(eval!(self.right)),
				})))
			}
			BinaryOperator::RangeSkipInclusive => {
				// a>..=b means start: Excluded(a), end: Included(b)
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Excluded(left),
					end: std::ops::Bound::Included(eval!(self.right)),
				})))
			}

			// Match operators require full-text search context
			BinaryOperator::Matches(_) => {
				Err(anyhow::anyhow!("MATCHES operator not yet supported in physical expressions"))
			}

			// Nearest neighbor requires vector index context
			BinaryOperator::NearestNeighbor(_) => {
				Err(anyhow::anyhow!("KNN operator not yet supported in physical expressions"))
			}
		}
	}

	fn references_current_value(&self) -> bool {
		self.left.references_current_value() || self.right.references_current_value()
	}

	fn access_mode(&self) -> AccessMode {
		// Combine both sides' access modes
		self.left.access_mode().combine(self.right.access_mode())
	}
}

impl ToSql for BinaryOp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{} {} {}", self.left, self.op, self.right)
	}
}

/// Unary/Prefix operation - op expr (e.g., -5, !true, +x)
#[derive(Debug, Clone)]
pub struct UnaryOp {
	pub(crate) op: crate::expr::operator::PrefixOperator,
	pub(crate) expr: Arc<dyn PhysicalExpr>,
}

#[async_trait]
impl PhysicalExpr for UnaryOp {
	fn name(&self) -> &'static str {
		"UnaryOp"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		use crate::expr::operator::PrefixOperator;
		use crate::fnc::operate;

		let value = self.expr.evaluate(ctx).await?;

		match &self.op {
			PrefixOperator::Not => operate::not(value),
			PrefixOperator::Negate => operate::neg(value),
			PrefixOperator::Positive => {
				// Positive is essentially a no-op for numbers
				Ok(value)
			}
			PrefixOperator::Range => {
				// ..value creates range with unbounded start, excluded end
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Unbounded,
					end: std::ops::Bound::Excluded(value),
				})))
			}
			PrefixOperator::RangeInclusive => {
				// ..=value creates range with unbounded start, included end
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Unbounded,
					end: std::ops::Bound::Included(value),
				})))
			}
			PrefixOperator::Cast(kind) => {
				// Type casting
				value.cast_to_kind(kind).map_err(|e| anyhow::anyhow!("{}", e))
			}
		}
	}

	fn references_current_value(&self) -> bool {
		self.expr.references_current_value()
	}

	fn access_mode(&self) -> AccessMode {
		// Propagate inner expression's access mode
		self.expr.access_mode()
	}
}

impl ToSql for UnaryOp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{} {}", self.op, self.expr)
	}
}

/// Postfix operation - expr op (e.g., value.., value>..)
#[derive(Debug, Clone)]
pub struct PostfixOp {
	pub(crate) op: crate::expr::operator::PostfixOperator,
	pub(crate) expr: Arc<dyn PhysicalExpr>,
}

#[async_trait]
impl PhysicalExpr for PostfixOp {
	fn name(&self) -> &'static str {
		"PostfixOp"
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		use crate::expr::operator::PostfixOperator;

		let value = self.expr.evaluate(ctx).await?;

		match &self.op {
			PostfixOperator::Range => {
				// value.. creates range with included start, unbounded end
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Included(value),
					end: std::ops::Bound::Unbounded,
				})))
			}
			PostfixOperator::RangeSkip => {
				// value>.. creates range with excluded start, unbounded end
				Ok(Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Excluded(value),
					end: std::ops::Bound::Unbounded,
				})))
			}
			PostfixOperator::MethodCall(..) => {
				Err(anyhow::anyhow!("Method calls not yet supported in physical expressions"))
			}
			PostfixOperator::Call(..) => {
				Err(anyhow::anyhow!("Function calls not yet supported in physical expressions"))
			}
		}
	}

	fn references_current_value(&self) -> bool {
		self.expr.references_current_value()
	}

	fn access_mode(&self) -> AccessMode {
		// Propagate inner expression's access mode
		self.expr.access_mode()
	}
}

impl ToSql for PostfixOp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{} {}", self.expr, self.op)
	}
}

/// Scalar subquery - (SELECT ... LIMIT 1)
#[derive(Debug, Clone)]
pub struct ScalarSubquery {
	pub(crate) plan: Arc<dyn OperatorPlan>,
}

#[async_trait]
impl PhysicalExpr for ScalarSubquery {
	fn name(&self) -> &'static str {
		"ScalarSubquery"
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// TODO: Implement scalar subquery evaluation
		// This requires bridging EvalContext (which has borrowed &Transaction)
		// with ExecutionContext (which needs Arc<Transaction>).
		// Options:
		// 1. Store Arc<Transaction> in EvalContext
		// 2. Add a method to create ExecutionContext from borrowed context
		// 3. Make ExecutionContext work with borrowed Transaction (but this conflicts with 'static
		//    stream requirement)

		Err(anyhow::anyhow!(
			"ScalarSubquery evaluation not yet fully implemented - need Arc<Transaction> in EvalContext"
		))
	}

	fn references_current_value(&self) -> bool {
		// For now, assume subqueries don't reference current value
		// TODO: Track if plan references outer scope for correlated subqueries
		false
	}

	fn access_mode(&self) -> AccessMode {
		// CRITICAL: Propagate the subquery's access mode!
		// This is why `SELECT *, (UPSERT person) FROM person` is ReadWrite
		self.plan.access_mode()
	}
}

impl ToSql for ScalarSubquery {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "TODO: Not implemented")
	}
}
