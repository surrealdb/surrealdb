use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ExecOperator};
use crate::expr::FlowResult;
use crate::val::Value;

/// Binary operation - left op right (e.g., age > 10)
#[derive(Debug, Clone)]
pub struct BinaryOp {
	pub(crate) left: Arc<dyn PhysicalExpr>,
	pub(crate) op: crate::expr::operator::BinaryOperator,
	pub(crate) right: Arc<dyn PhysicalExpr>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for BinaryOp {
	fn name(&self) -> &'static str {
		"BinaryOp"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Combine both operands' context requirements
		self.left.required_context().max(self.right.required_context())
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
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
		// Note: operate::* functions return anyhow::Result<Value>.
		// The ? operator converts anyhow::Error to ControlFlow via From impl.
		Ok(match &self.op {
			BinaryOperator::Add => operate::add(left, eval!(self.right))?,
			BinaryOperator::Subtract => operate::sub(left, eval!(self.right))?,
			BinaryOperator::Multiply => operate::mul(left, eval!(self.right))?,
			BinaryOperator::Divide => operate::div(left, eval!(self.right))?,
			BinaryOperator::Remainder => operate::rem(left, eval!(self.right))?,
			BinaryOperator::Power => operate::pow(left, eval!(self.right))?,

			BinaryOperator::Equal => operate::equal(&left, &eval!(self.right))?,
			BinaryOperator::ExactEqual => operate::exact(&left, &eval!(self.right))?,
			BinaryOperator::NotEqual => operate::not_equal(&left, &eval!(self.right))?,
			BinaryOperator::AllEqual => operate::all_equal(&left, &eval!(self.right))?,
			BinaryOperator::AnyEqual => operate::any_equal(&left, &eval!(self.right))?,

			BinaryOperator::LessThan => operate::less_than(&left, &eval!(self.right))?,
			BinaryOperator::LessThanEqual => {
				operate::less_than_or_equal(&left, &eval!(self.right))?
			}
			BinaryOperator::MoreThan => operate::more_than(&left, &eval!(self.right))?,
			BinaryOperator::MoreThanEqual => {
				operate::more_than_or_equal(&left, &eval!(self.right))?
			}

			BinaryOperator::And => {
				// Short-circuit AND
				if !left.is_truthy() {
					left
				} else {
					eval!(self.right)
				}
			}
			BinaryOperator::Or => {
				// Short-circuit OR
				if left.is_truthy() {
					left
				} else {
					eval!(self.right)
				}
			}

			BinaryOperator::Contain => operate::contain(&left, &eval!(self.right))?,
			BinaryOperator::NotContain => operate::not_contain(&left, &eval!(self.right))?,
			BinaryOperator::ContainAll => operate::contain_all(&left, &eval!(self.right))?,
			BinaryOperator::ContainAny => operate::contain_any(&left, &eval!(self.right))?,
			BinaryOperator::ContainNone => operate::contain_none(&left, &eval!(self.right))?,
			BinaryOperator::Inside => operate::inside(&left, &eval!(self.right))?,
			BinaryOperator::NotInside => operate::not_inside(&left, &eval!(self.right))?,
			BinaryOperator::AllInside => operate::inside_all(&left, &eval!(self.right))?,
			BinaryOperator::AnyInside => operate::inside_any(&left, &eval!(self.right))?,
			BinaryOperator::NoneInside => operate::inside_none(&left, &eval!(self.right))?,

			BinaryOperator::Outside => operate::outside(&left, &eval!(self.right))?,
			BinaryOperator::Intersects => operate::intersects(&left, &eval!(self.right))?,

			BinaryOperator::NullCoalescing => {
				if !left.is_nullish() {
					left
				} else {
					eval!(self.right)
				}
			}
			BinaryOperator::TenaryCondition => {
				// Same as OR for this context
				if left.is_truthy() {
					left
				} else {
					eval!(self.right)
				}
			}

			// Range operators - create Range values
			BinaryOperator::Range => {
				// a..b means start: Included(a), end: Excluded(b)
				Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Included(left),
					end: std::ops::Bound::Excluded(eval!(self.right)),
				}))
			}
			BinaryOperator::RangeInclusive => {
				// a..=b means start: Included(a), end: Included(b)
				Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Included(left),
					end: std::ops::Bound::Included(eval!(self.right)),
				}))
			}
			BinaryOperator::RangeSkip => {
				// a>..b means start: Excluded(a), end: Excluded(b)
				Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Excluded(left),
					end: std::ops::Bound::Excluded(eval!(self.right)),
				}))
			}
			BinaryOperator::RangeSkipInclusive => {
				// a>..=b means start: Excluded(a), end: Included(b)
				Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Excluded(left),
					end: std::ops::Bound::Included(eval!(self.right)),
				}))
			}

			// Match operators require full-text search index context.
			BinaryOperator::Matches(_) => {
				// Records reaching this point via FullTextScan are already matches
				Value::Bool(true)
			}

			// Nearest neighbor requires vector index context
			// TODO(stu): IMPLEMENT
			BinaryOperator::NearestNeighbor(_) => {
				return Err(anyhow::anyhow!(
					"KNN operator not yet supported in physical expressions"
				)
				.into());
			}
		})
	}

	fn access_mode(&self) -> AccessMode {
		// Combine both sides' access modes
		self.left.access_mode().combine(self.right.access_mode())
	}

	fn expr_children(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("left", &self.left), ("right", &self.right)]
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		let mut ops = self.left.embedded_operators();
		ops.extend(self.right.embedded_operators());
		ops
	}
}

impl ToSql for BinaryOp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{} {} {}", self.left, self.op, self.right)
	}
}

/// Optimised binary comparison for the common `field op literal` pattern.
///
/// Eliminates async_trait dispatch and per-record `Value::clone()` by inlining
/// field access and storing the literal value directly. Created at plan time
/// when the planner detects a simple `IdiomExpr(FieldPart)` on one side and a
/// `Literal` on the other.
#[derive(Debug, Clone)]
pub struct SimpleBinaryOp {
	pub(crate) field_name: String,
	pub(crate) op: crate::expr::operator::BinaryOperator,
	pub(crate) literal: Value,
	/// When true, the literal is on the left: `literal op field`.
	/// The operand order is swapped for non-commutative operators.
	pub(crate) reversed: bool,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for SimpleBinaryOp {
	fn name(&self) -> &'static str {
		"SimpleBinaryOp"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Field access may trigger record fetch when applied to a RecordId,
		// so we conservatively require database context.
		crate::exec::ContextLevel::Database
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use crate::expr::operator::BinaryOperator;
		use crate::fnc::operate;

		let none = Value::None;
		let current = ctx.current_value.unwrap_or(&none);

		// Fast path: direct object field lookup (covers table scan records).
		// Slow path: fall back to evaluate_field for RecordId auto-fetch, arrays, etc.
		let (field_ref, _owned);
		let field_val: &Value = if let Value::Object(obj) = current {
			field_ref = obj.get(&self.field_name).unwrap_or(&none);
			field_ref
		} else {
			_owned = crate::exec::parts::field::evaluate_field(current, &self.field_name, ctx)
				.await
				.map_err(crate::expr::ControlFlow::Err)?;
			&_owned
		};

		let (left, right) = if self.reversed {
			(&self.literal, field_val)
		} else {
			(field_val, &self.literal)
		};

		Ok(match &self.op {
			BinaryOperator::Equal => operate::equal(left, right)?,
			BinaryOperator::ExactEqual => operate::exact(left, right)?,
			BinaryOperator::NotEqual => operate::not_equal(left, right)?,
			BinaryOperator::AllEqual => operate::all_equal(left, right)?,
			BinaryOperator::AnyEqual => operate::any_equal(left, right)?,

			BinaryOperator::LessThan => operate::less_than(left, right)?,
			BinaryOperator::LessThanEqual => operate::less_than_or_equal(left, right)?,
			BinaryOperator::MoreThan => operate::more_than(left, right)?,
			BinaryOperator::MoreThanEqual => operate::more_than_or_equal(left, right)?,

			BinaryOperator::Contain => operate::contain(left, right)?,
			BinaryOperator::NotContain => operate::not_contain(left, right)?,
			BinaryOperator::ContainAll => operate::contain_all(left, right)?,
			BinaryOperator::ContainAny => operate::contain_any(left, right)?,
			BinaryOperator::ContainNone => operate::contain_none(left, right)?,
			BinaryOperator::Inside => operate::inside(left, right)?,
			BinaryOperator::NotInside => operate::not_inside(left, right)?,
			BinaryOperator::AllInside => operate::inside_all(left, right)?,
			BinaryOperator::AnyInside => operate::inside_any(left, right)?,
			BinaryOperator::NoneInside => operate::inside_none(left, right)?,

			BinaryOperator::Outside => operate::outside(left, right)?,
			BinaryOperator::Intersects => operate::intersects(left, right)?,

			// Unsupported operators should never reach here; the planner only
			// creates SimpleBinaryOp for the operators listed above.
			_ => unreachable!("SimpleBinaryOp created for unsupported operator {:?}", self.op),
		})
	}

	/// Batch evaluation that avoids per-record async dispatch overhead.
	///
	/// Uses the fast Object-field-lookup path for all records. If any record
	/// is not an Object (e.g., a RecordId requiring async fetch), falls back
	/// to per-record `evaluate` for that record.
	async fn evaluate_batch(
		&self,
		ctx: EvalContext<'_>,
		values: &[Value],
	) -> FlowResult<Vec<Value>> {
		use crate::expr::operator::BinaryOperator;
		use crate::fnc::operate;

		// Check if all values are Objects (the common case for table scans).
		// If any value requires async field resolution (e.g., RecordId fetch),
		// fall back to the default sequential evaluate.
		let all_objects = values.iter().all(|v| matches!(v, Value::Object(_)));
		if !all_objects {
			let mut results = Vec::with_capacity(values.len());
			for value in values {
				results.push(self.evaluate(ctx.with_value(value)).await?);
			}
			return Ok(results);
		}

		let none = Value::None;
		let mut results = Vec::with_capacity(values.len());

		// All values are Objects — use fast synchronous field lookup.
		macro_rules! apply_op {
			($op_fn:expr) => {
				for value in values {
					let field_val = match value {
						Value::Object(obj) => obj.get(&self.field_name).unwrap_or(&none),
						_ => unreachable!("checked all_objects above"),
					};
					let (left, right) = if self.reversed {
						(&self.literal, field_val)
					} else {
						(field_val, &self.literal)
					};
					results.push($op_fn(left, right)?);
				}
			};
		}

		match &self.op {
			BinaryOperator::Equal => apply_op!(operate::equal),
			BinaryOperator::ExactEqual => apply_op!(operate::exact),
			BinaryOperator::NotEqual => apply_op!(operate::not_equal),
			BinaryOperator::AllEqual => apply_op!(operate::all_equal),
			BinaryOperator::AnyEqual => apply_op!(operate::any_equal),

			BinaryOperator::LessThan => apply_op!(operate::less_than),
			BinaryOperator::LessThanEqual => apply_op!(operate::less_than_or_equal),
			BinaryOperator::MoreThan => apply_op!(operate::more_than),
			BinaryOperator::MoreThanEqual => apply_op!(operate::more_than_or_equal),

			BinaryOperator::Contain => apply_op!(operate::contain),
			BinaryOperator::NotContain => apply_op!(operate::not_contain),
			BinaryOperator::ContainAll => apply_op!(operate::contain_all),
			BinaryOperator::ContainAny => apply_op!(operate::contain_any),
			BinaryOperator::ContainNone => apply_op!(operate::contain_none),
			BinaryOperator::Inside => apply_op!(operate::inside),
			BinaryOperator::NotInside => apply_op!(operate::not_inside),
			BinaryOperator::AllInside => apply_op!(operate::inside_all),
			BinaryOperator::AnyInside => apply_op!(operate::inside_any),
			BinaryOperator::NoneInside => apply_op!(operate::inside_none),

			BinaryOperator::Outside => apply_op!(operate::outside),
			BinaryOperator::Intersects => apply_op!(operate::intersects),

			_ => unreachable!("SimpleBinaryOp created for unsupported operator {:?}", self.op),
		}

		Ok(results)
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for SimpleBinaryOp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if self.reversed {
			self.literal.fmt_sql(f, fmt);
			f.push(' ');
			write_sql!(f, fmt, "{}", self.op);
			f.push(' ');
			f.push_str(&self.field_name);
		} else {
			f.push_str(&self.field_name);
			f.push(' ');
			write_sql!(f, fmt, "{}", self.op);
			f.push(' ');
			self.literal.fmt_sql(f, fmt);
		}
	}
}

/// Unary/Prefix operation - op expr (e.g., -5, !true, +x)
#[derive(Debug, Clone)]
pub struct UnaryOp {
	pub(crate) op: crate::expr::operator::PrefixOperator,
	pub(crate) expr: Arc<dyn PhysicalExpr>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for UnaryOp {
	fn name(&self) -> &'static str {
		"UnaryOp"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Propagate inner expression's context requirement
		self.expr.required_context()
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use crate::expr::operator::PrefixOperator;
		use crate::fnc::operate;

		let value = self.expr.evaluate(ctx).await?;

		Ok(match &self.op {
			PrefixOperator::Not => operate::not(value)?,
			PrefixOperator::Negate => operate::neg(value)?,
			PrefixOperator::Positive => {
				// Positive is essentially a no-op for numbers
				value
			}
			PrefixOperator::Range => {
				// ..value creates range with unbounded start, excluded end
				Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Unbounded,
					end: std::ops::Bound::Excluded(value),
				}))
			}
			PrefixOperator::RangeInclusive => {
				// ..=value creates range with unbounded start, included end
				Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Unbounded,
					end: std::ops::Bound::Included(value),
				}))
			}
			PrefixOperator::Cast(kind) => {
				// Type casting
				value.cast_to_kind(kind).map_err(|e| anyhow::anyhow!("{}", e))?
			}
		})
	}

	fn access_mode(&self) -> AccessMode {
		// Propagate inner expression's access mode
		self.expr.access_mode()
	}

	fn expr_children(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("operand", &self.expr)]
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		self.expr.embedded_operators()
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

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for PostfixOp {
	fn name(&self) -> &'static str {
		"PostfixOp"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Propagate inner expression's context requirement
		self.expr.required_context()
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use crate::expr::operator::PostfixOperator;

		let value = self.expr.evaluate(ctx).await?;

		Ok(match &self.op {
			PostfixOperator::Range => {
				// value.. creates range with included start, unbounded end
				Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Included(value),
					end: std::ops::Bound::Unbounded,
				}))
			}
			PostfixOperator::RangeSkip => {
				// value>.. creates range with excluded start, unbounded end
				Value::Range(Box::new(crate::val::Range {
					start: std::ops::Bound::Excluded(value),
					end: std::ops::Bound::Unbounded,
				}))
			}
			PostfixOperator::MethodCall(..) => {
				return Err(anyhow::anyhow!(
					"Method calls not yet supported in physical expressions"
				)
				.into());
			}
			PostfixOperator::Call(..) => {
				// Closure calls are handled by ClosureCallExec in the planner
				// This branch should never be reached
				unreachable!(
					"PostfixOperator::Call should be converted to ClosureCallExec by the planner"
				)
			}
		})
	}

	fn access_mode(&self) -> AccessMode {
		// Propagate inner expression's access mode
		self.expr.access_mode()
	}

	fn expr_children(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("operand", &self.expr)]
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		self.expr.embedded_operators()
	}
}

impl ToSql for PostfixOp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{} {}", self.expr, self.op)
	}
}
