use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::val::Value;

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

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Combine both operands' context requirements
		self.left.required_context().max(self.right.required_context())
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

			// Match operators require full-text search index context.
			// When evaluated without index context (e.g., in computed fields or expressions),
			// MATCHES returns false. The index planner pushes MATCHES to index scans where
			// the QueryExecutor handles proper full-text search evaluation.
			BinaryOperator::Matches(_) => {
				// Without index executor context, MATCHES returns false
				// This is consistent with the legacy compute path's ExecutorOption::None case
				Ok(Value::Bool(false))
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

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Propagate inner expression's context requirement
		self.expr.required_context()
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

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Propagate inner expression's context requirement
		self.expr.required_context()
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
				// Closure calls are handled by ClosureCallExec in the planner
				// This branch should never be reached
				unreachable!(
					"PostfixOperator::Call should be converted to ClosureCallExec by the planner"
				)
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
