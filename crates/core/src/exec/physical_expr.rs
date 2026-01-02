

use async_trait::async_trait;
use std::{fmt::Debug, sync::Arc};

use crate::{exec::{ExecutionContext, ExecutionPlan, Parameters}, expr::Idiom, kvs::Transaction, val::Value};


/// Evaluation context - what's available during expression evaluation.
///
/// This is a borrowed view into the execution context for expression evaluation.
/// It provides access to parameters, namespace/database names, and the current row
/// (for per-row expressions like filters and projections).
// Clone is implemented manually because #[derive(Clone)] doesn't work well
// with lifetime parameters when we just have references.
#[derive(Clone, Copy)]
pub struct EvalContext<'a> {
	pub params: &'a Parameters,
	pub ns: Option<&'a str>,
	pub db: Option<&'a str>,
	pub txn: Option<&'a Transaction>,

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
	pub(crate) fn from_exec_ctx(exec_ctx: &'a ExecutionContext) -> Self {
		match exec_ctx {
			ExecutionContext::Root(r) => Self::scalar(&r.params, None, None, None),
			ExecutionContext::Namespace(n) => {
				Self::scalar(&n.root.params, Some(&n.ns.name), None, Some(&n.txn))
			}
			ExecutionContext::Database(d) => Self::scalar(
				&d.ns_ctx.root.params,
				Some(&d.ns_ctx.ns.name),
				Some(&d.db.name),
				Some(&d.ns_ctx.txn),
			),
		}
	}

	/// For session-level scalar evaluation (USE, LIMIT, etc.)
	pub fn scalar(
		params: &'a Parameters,
		ns: Option<&'a str>,
		db: Option<&'a str>,
		txn: Option<&'a Transaction>,
	) -> Self {
		Self {
			params,
			ns,
			db,
			txn,
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
pub trait PhysicalExpr: Send + Sync + Debug {
	/// Evaluate this expression to a value.
	///
	/// May execute subqueries internally, hence async.
	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value>;

	/// Does this expression reference the current row?
	/// If false, can be evaluated in scalar context.
	fn references_current_value(&self) -> bool;
}

/// Literal value - "foo", 42, true
#[derive(Debug, Clone)]
pub struct Literal(pub(crate) Value);

#[async_trait]
impl PhysicalExpr for Literal {
	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		Ok(self.0.clone())
	}

	fn references_current_value(&self) -> bool {
		false
	}
}

/// Parameter reference - $foo
#[derive(Debug, Clone)]
pub struct Param(pub(crate) String);

#[async_trait]
impl PhysicalExpr for Param {
	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		ctx.params
			.get(self.0.as_str())
			.map(|v| (**v).clone())
			.ok_or_else(|| anyhow::anyhow!("Parameter not found: ${}", self.0))
	}

	fn references_current_value(&self) -> bool {
		false
	}
}

/// Field access - foo.bar.baz or just `age`
#[derive(Debug, Clone)]
pub struct Field(pub(crate) Idiom);

#[async_trait]
impl PhysicalExpr for Field {
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
	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		use crate::expr::operator::BinaryOperator;
		use crate::fnc::operate;

		// Evaluate both sides (could parallelize if both are independent)
		let left = self.left.evaluate(ctx.clone()).await?;
		let right = self.right.evaluate(ctx).await?;

		// Apply the operator
		match &self.op {
			BinaryOperator::Add => operate::add(left, right),
			BinaryOperator::Subtract => operate::sub(left, right),
			BinaryOperator::Multiply => operate::mul(left, right),
			BinaryOperator::Divide => operate::div(left, right),
			BinaryOperator::Remainder => operate::rem(left, right),
			BinaryOperator::Power => operate::pow(left, right),

			BinaryOperator::Equal => operate::equal(&left, &right),
			BinaryOperator::ExactEqual => operate::exact(&left, &right),
			BinaryOperator::NotEqual => operate::not_equal(&left, &right),
			BinaryOperator::AllEqual => operate::all_equal(&left, &right),
			BinaryOperator::AnyEqual => operate::any_equal(&left, &right),

			BinaryOperator::LessThan => operate::less_than(&left, &right),
			BinaryOperator::LessThanEqual => operate::less_than_or_equal(&left, &right),
			BinaryOperator::MoreThan => operate::more_than(&left, &right),
			BinaryOperator::MoreThanEqual => operate::more_than_or_equal(&left, &right),

			BinaryOperator::And => {
				// Short-circuit AND
				if !left.is_truthy() {
					Ok(left)
				} else {
					Ok(right)
				}
			}
			BinaryOperator::Or => {
				// Short-circuit OR
				if left.is_truthy() {
					Ok(left)
				} else {
					Ok(right)
				}
			}

			BinaryOperator::Contain => operate::contain(&left, &right),
			BinaryOperator::NotContain => operate::not_contain(&left, &right),
			BinaryOperator::ContainAll => operate::contain_all(&left, &right),
			BinaryOperator::ContainAny => operate::contain_any(&left, &right),
			BinaryOperator::ContainNone => operate::contain_none(&left, &right),
			BinaryOperator::Inside => operate::inside(&left, &right),
			BinaryOperator::NotInside => operate::not_inside(&left, &right),
			BinaryOperator::AllInside => operate::inside_all(&left, &right),
			BinaryOperator::AnyInside => operate::inside_any(&left, &right),
			BinaryOperator::NoneInside => operate::inside_none(&left, &right),

			BinaryOperator::Outside => operate::outside(&left, &right),
			BinaryOperator::Intersects => operate::intersects(&left, &right),

			BinaryOperator::NullCoalescing => {
				if !left.is_nullish() {
					Ok(left)
				} else {
					Ok(right)
				}
			}
			BinaryOperator::TenaryCondition => {
				// Same as OR for this context
				if left.is_truthy() {
					Ok(left)
				} else {
					Ok(right)
				}
			}

			// Range operators not typically used in WHERE clauses
			BinaryOperator::Range
			| BinaryOperator::RangeInclusive
			| BinaryOperator::RangeSkip
			| BinaryOperator::RangeSkipInclusive => {
				Err(anyhow::anyhow!("Range operators not yet supported in physical expressions"))
			}

			// Match operators require full-text search context
			BinaryOperator::Matches(_) => {
				Err(anyhow::anyhow!("MATCHEÍS operator not yet supported in physical expressions"))
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
}

/// Scalar subquery - (SELECT ... LIMIT 1)
#[derive(Debug, Clone)]
pub struct ScalarSubquery {
	pub(crate) plan: Arc<dyn ExecutionPlan>,
}

#[async_trait]
impl PhysicalExpr for ScalarSubquery {
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
}