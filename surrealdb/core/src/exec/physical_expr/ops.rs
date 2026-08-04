use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ExecOperator};
use crate::expr::FlowResult;
use crate::val::Value;

/// Binary operation - left op right (e.g., age > 10)
#[derive(Debug, Clone)]
pub struct BinaryOp {
	pub(crate) left: Arc<dyn PhysicalExpr>,
	pub(crate) op: crate::expr::operator::BinaryOperator,
	pub(crate) right: Arc<dyn PhysicalExpr>,
}
impl PhysicalExpr for BinaryOp {
	fn name(&self) -> &'static str {
		"BinaryOp"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Combine both operands' context requirements
		self.left.required_context().max(self.right.required_context())
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
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

				// A MATCHES that was not lowered into MatchesOp. The planner routes
				// every parsed `@@` there — including the shapes that can name no
				// index — so this arm is only reachable for a hand-built node. It
				// answers `false`, the legacy outcome for a row with no query
				// executor, because there is no row table here to decide whether
				// the stricter `NoIndexFoundForMatch` applies.
				BinaryOperator::Matches(_) => Value::Bool(false),

				// A KNN expression that was not lowered into KnnScan (HNSW)
				// or KnnTopK (brute-force): projection position, a bare
				// expression, a non-plan-time-computable query vector, or an
				// Approximate form with no index. The legacy executor has no
				// executor entry for these and evaluates them to `false` per
				// row. Extracted conjuncts never reach this arm — they are
				// stripped via strip_knn_from_condition before physical
				// expression compilation.
				BinaryOperator::NearestNeighbor(_) => Value::Bool(false),
			})
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
impl PhysicalExpr for SimpleBinaryOp {
	fn name(&self) -> &'static str {
		"SimpleBinaryOp"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Field access may trigger record fetch when applied to a RecordId,
		// so we conservatively require database context.
		crate::exec::ContextLevel::Database
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			use crate::expr::operator::BinaryOperator;
			use crate::fnc::operate;

			let current = ctx.current_value.unwrap_or(&Value::NONE);

			// Fast path: direct object field lookup (covers table scan records).
			// Slow path: fall back to evaluate_field for RecordId auto-fetch, arrays, etc.
			let (field_ref, owned);
			let field_val: &Value = if let Value::Object(obj) = current {
				field_ref = obj.get(&self.field_name).unwrap_or(&Value::NONE);
				field_ref
			} else {
				owned = crate::exec::parts::field::evaluate_field(current, &self.field_name, ctx)
					.await?;
				&owned
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
		})
	}

	/// Batch evaluation that avoids per-record async dispatch overhead.
	///
	/// Uses the fast Object-field-lookup path for all records. If any record
	/// is not an Object (e.g., a RecordId requiring async fetch), falls back
	/// to per-record `evaluate` for that record.
	fn evaluate_batch<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		values: &'a [Value],
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		Box::pin(async move {
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

			let mut results = Vec::with_capacity(values.len());

			// All values are Objects — use fast synchronous field lookup.
			macro_rules! apply_op {
				($op_fn:expr) => {
					for value in values {
						let field_val = match value {
							Value::Object(obj) => obj.get(&self.field_name).unwrap_or(&Value::NONE),
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
		})
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
impl PhysicalExpr for UnaryOp {
	fn name(&self) -> &'static str {
		"UnaryOp"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Propagate inner expression's context requirement
		self.expr.required_context()
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
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
impl PhysicalExpr for PostfixOp {
	fn name(&self) -> &'static str {
		"PostfixOp"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Propagate inner expression's context requirement
		self.expr.required_context()
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
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

#[cfg(test)]
mod tests {
	use std::ops::Bound;
	use std::sync::Arc;

	use surrealdb_types::ToSql;

	use super::*;
	use crate::exec::operators::test_util::{TestDb, eval, eval_on, physical_expr, root_ctx, val};
	use crate::exec::physical_expr::ScalarSubquery;
	use crate::exec::{ContextLevel, EvalContext};
	use crate::expr::ControlFlow;
	use crate::expr::operator::{
		BinaryOperator, BooleanOperator, MatchesOperator, NearestNeighbor, PostfixOperator,
	};

	/// Compile and evaluate `src` in scalar context, asserting it produced a
	/// value rather than an error or a control-flow signal.
	async fn ok(src: &str) -> Value {
		let ctx = root_ctx();
		match eval(src, &ctx).await {
			Ok(value) => value,
			Err(flow) => panic!("{src} should evaluate, got {flow}"),
		}
	}

	/// Compile and evaluate `src`, asserting it failed, and return the rendered
	/// failure so the caller can assert on the message.
	async fn failure(src: &str) -> String {
		let ctx = root_ctx();
		match eval(src, &ctx).await {
			Ok(value) => panic!("{src} should not evaluate, got {value:?}"),
			Err(flow) => flow.to_string(),
		}
	}

	/// The bounds of a `Value::Range`, for asserting on range operators.
	fn bounds(value: Value) -> (Bound<i64>, Bound<i64>) {
		let Value::Range(range) = value else {
			panic!("expected a range, got {value:?}");
		};
		let int = |b: Bound<Value>| {
			let unwrap = |v: Value| match v {
				Value::Number(crate::val::Number::Int(i)) => i,
				other => panic!("expected an int bound, got {other:?}"),
			};
			match b {
				Bound::Unbounded => Bound::Unbounded,
				Bound::Included(v) => Bound::Included(unwrap(v)),
				Bound::Excluded(v) => Bound::Excluded(unwrap(v)),
			}
		};
		(int(range.start), int(range.end))
	}

	// =========================================================================
	// BinaryOp — arithmetic
	// =========================================================================

	#[tokio::test]
	async fn arithmetic_keeps_the_operand_kind_and_promotes_across_numeric_kinds() {
		assert_eq!(ok("1 + 2").await, Value::from(3));
		assert_eq!(ok("5 - 2").await, Value::from(3));
		assert_eq!(ok("3 * 4").await, Value::from(12));
		assert_eq!(ok("2 ** 10").await, Value::from(1024));
		assert_eq!(ok("5 % 3").await, Value::from(2));

		// Int + Float widens to Float; anything mixed with Decimal settles on
		// Decimal, so a Float operand does not drag the result back to Float.
		assert_eq!(ok("1 + 1.5").await, Value::from(2.5));
		assert_eq!(ok("1dec + 1").await, ok("2dec").await);
		assert_eq!(ok("1dec + 1.5").await, ok("2.5dec").await);

		// `+` is overloaded per operand kind rather than numeric-only.
		assert_eq!(ok("'a' + 'b'").await, Value::from("ab"));
		assert_eq!(ok("[1] + [2]").await, val("[1, 2]").await);
		assert_eq!(ok("{ a: 1 } + { b: 2 }").await, val("{ a: 1, b: 2 }").await);
		assert_eq!(ok("1h + 1h").await, val("2h").await);
	}

	#[tokio::test]
	async fn integer_division_truncates_and_division_by_zero_yields_a_float_nan() {
		// Int / Int stays Int and truncates — 5 / 4 is 1, not 1.25.
		assert_eq!(ok("5 / 4").await, Value::from(1));

		// Divide is the one arithmetic operator that swallows its failure: a
		// zero divisor produces a float NaN instead of an error.
		let nan = ok("1 / 0").await;
		let Value::Number(crate::val::Number::Float(f)) = nan else {
			panic!("expected a float, got {nan:?}");
		};
		assert!(f.is_nan(), "1 / 0 yields NaN, got {f}");

		// Float division never failed in the first place, so it keeps IEEE
		// infinity rather than being rewritten to NaN.
		let inf = ok("1.0 / 0.0").await;
		let Value::Number(crate::val::Number::Float(f)) = inf else {
			panic!("expected a float, got {inf:?}");
		};
		assert!(f.is_infinite() && f.is_sign_positive(), "1.0 / 0.0 yields +inf, got {f}");
	}

	#[tokio::test]
	async fn remainder_power_and_overflow_surface_as_errors_rather_than_saturating() {
		// Unlike divide, remainder propagates the zero-divisor failure.
		assert!(failure("1 % 0").await.contains("remainder"));
		assert!(failure("2 ** 1000").await.contains("raise"));
		// Int arithmetic is checked, so the maximum does not wrap around.
		assert!(failure("9223372036854775807 + 1").await.contains("addition"));
	}

	#[tokio::test]
	async fn mismatched_operand_kinds_are_rejected_instead_of_coerced() {
		let add = failure("1 + 'a'").await;
		assert!(add.contains("int") && add.contains("string"), "got {add}");
		assert!(failure("'a' - 'b'").await.contains("subtraction"));
		assert!(failure("-'a'").await.contains("negate"));
	}

	// =========================================================================
	// BinaryOp — equality
	// =========================================================================

	#[tokio::test]
	async fn equality_is_kind_directed_with_no_string_number_coercion() {
		assert_eq!(ok("1 = '1'").await, Value::Bool(false));
		// Numeric equality does span representations, though.
		assert_eq!(ok("1 = 1.0").await, Value::Bool(true));
		assert_eq!(ok("1 = 1dec").await, Value::Bool(true));

		// NONE and NULL are equal only to themselves, never to each other.
		assert_eq!(ok("NONE = NONE").await, Value::Bool(true));
		assert_eq!(ok("NULL = NULL").await, Value::Bool(true));
		assert_eq!(ok("NONE = NULL").await, Value::Bool(false));
		assert_eq!(ok("NULL = NONE").await, Value::Bool(false));
		assert_eq!(ok("NONE != NULL").await, Value::Bool(true));
	}

	#[tokio::test]
	async fn exact_equal_compares_representations_where_equal_applies_a_regex() {
		// `=` treats a regex operand as a match test...
		assert_eq!(ok("'abc' = /b/").await, Value::Bool(true));
		// ...while `==` compares the two values as they are, so a string is
		// never equal to a regex.
		assert_eq!(ok("'abc' == /b/").await, Value::Bool(false));
		// Numbers compare by value under both, so `==` is not a same-kind test.
		assert_eq!(ok("1 == 1.0").await, Value::Bool(true));
	}

	#[tokio::test]
	async fn any_and_all_equal_fold_over_the_left_operand() {
		assert_eq!(ok("[1, 2] ?= 2").await, Value::Bool(true));
		assert_eq!(ok("[1, 2] ?= 3").await, Value::Bool(false));
		assert_eq!(ok("[2, 2] *= 2").await, Value::Bool(true));
		assert_eq!(ok("[1, 2] *= 2").await, Value::Bool(false));

		// The fold is over an empty sequence for `[]`: `*=` is vacuously true
		// and `?=` vacuously false.
		assert_eq!(ok("[] *= 2").await, Value::Bool(true));
		assert_eq!(ok("[] ?= 2").await, Value::Bool(false));

		// A non-array left operand degenerates to plain equality.
		assert_eq!(ok("2 ?= 2").await, Value::Bool(true));
		assert_eq!(ok("2 *= 3").await, Value::Bool(false));
	}

	// =========================================================================
	// BinaryOp — ordering comparison
	// =========================================================================

	#[tokio::test]
	async fn comparison_across_kinds_falls_back_to_kind_order_instead_of_failing() {
		// Ordering between unlike kinds is decided by the position of the kind
		// in the value enum, so these answer a boolean rather than erroring.
		// NONE sorts before every other kind.
		assert_eq!(ok("NONE < 1").await, Value::Bool(true));
		assert_eq!(ok("NONE > 1").await, Value::Bool(false));
		// Numbers sort before strings.
		assert_eq!(ok("1 < 'a'").await, Value::Bool(true));
		assert_eq!(ok("'a' < 1").await, Value::Bool(false));
	}

	#[tokio::test]
	async fn comparison_of_numbers_is_by_value_across_representations() {
		assert_eq!(ok("1 <= 1.0").await, Value::Bool(true));
		assert_eq!(ok("2 > 1.5").await, Value::Bool(true));
		assert_eq!(ok("1dec < 2").await, Value::Bool(true));
		assert_eq!(ok("2 >= 2.0").await, Value::Bool(true));
		assert_eq!(ok("'a' < 'b'").await, Value::Bool(true));
	}

	// =========================================================================
	// BinaryOp — logical operators
	// =========================================================================

	#[tokio::test]
	async fn and_or_answer_with_the_deciding_operand_not_with_a_boolean() {
		// AND returns the left operand verbatim when it is falsy, and the right
		// operand verbatim otherwise — neither side is coerced to a bool.
		assert_eq!(ok("0 AND 1").await, Value::from(0));
		assert_eq!(ok("NONE AND true").await, Value::None);
		assert_eq!(ok("1 AND 'hello'").await, Value::from("hello"));

		// OR mirrors that: the left operand when truthy, else the right.
		assert_eq!(ok("'a' OR 'b'").await, Value::from("a"));
		assert_eq!(ok("0 OR 'x'").await, Value::from("x"));
		// Emptiness is falsiness, so an empty array falls through.
		assert_eq!(ok("[] OR 'x'").await, Value::from("x"));
	}

	#[tokio::test]
	async fn and_or_leave_the_right_operand_unevaluated_once_the_left_decides() {
		// A THROW on the right proves it was never evaluated.
		assert_eq!(ok("false AND (THROW 'boom')").await, Value::Bool(false));
		assert_eq!(ok("true OR (THROW 'boom')").await, Value::Bool(true));

		// Controls: when the left does not decide, the right does run.
		assert!(failure("true AND (THROW 'boom')").await.contains("boom"));
		assert!(failure("false OR (THROW 'boom')").await.contains("boom"));
	}

	#[tokio::test]
	async fn null_coalescing_skips_only_none_and_null() {
		assert_eq!(ok("NONE ?? 5").await, Value::from(5));
		assert_eq!(ok("NULL ?? 5").await, Value::from(5));
		// Falsy-but-present values are kept: `??` tests nullishness, not truth.
		assert_eq!(ok("false ?? 5").await, Value::Bool(false));
		assert_eq!(ok("0 ?? 5").await, Value::from(0));
		assert_eq!(ok("'' ?? 5").await, Value::from(""));

		// Short-circuit: a present left operand skips the right entirely.
		assert_eq!(ok("1 ?? (THROW 'boom')").await, Value::from(1));
		assert!(failure("NONE ?? (THROW 'boom')").await.contains("boom"));
	}

	#[tokio::test]
	async fn ternary_condition_falls_through_on_any_falsy_left_operand() {
		// `?:` tests truthiness, so unlike `??` it also replaces false and 0.
		assert_eq!(ok("false ?: 5").await, Value::from(5));
		assert_eq!(ok("0 ?: 5").await, Value::from(5));
		assert_eq!(ok("NONE ?: 5").await, Value::from(5));
		assert_eq!(ok("'a' ?: 5").await, Value::from("a"));

		// And it short-circuits on the same rule.
		assert_eq!(ok("1 ?: (THROW 'boom')").await, Value::from(1));
	}

	// =========================================================================
	// BinaryOp — containment
	// =========================================================================

	#[tokio::test]
	async fn the_contains_family_reads_the_left_operand_as_the_container() {
		assert_eq!(ok("[1, 2, 3] CONTAINS 2").await, Value::Bool(true));
		assert_eq!(ok("[1, 2] CONTAINSNOT 3").await, Value::Bool(true));
		assert_eq!(ok("[1, 2, 3] CONTAINSALL [1, 2]").await, Value::Bool(true));
		assert_eq!(ok("[1, 2] CONTAINSALL [1, 3]").await, Value::Bool(false));
		assert_eq!(ok("[1, 2] CONTAINSANY [3, 2]").await, Value::Bool(true));
		assert_eq!(ok("[1, 2] CONTAINSNONE [3]").await, Value::Bool(true));

		// A string container is a substring test.
		assert_eq!(ok("'hello' CONTAINS 'ell'").await, Value::Bool(true));
		// And a string tested against a list of strings asks about every
		// substring, not about elements of a collection.
		assert_eq!(ok("'hello world' CONTAINSALL ['hello', 'world']").await, Value::Bool(true));

		// An object container answers about its keys.
		assert_eq!(ok("{ a: 1 } CONTAINS 'a'").await, Value::Bool(true));
		assert_eq!(ok("{ a: 1 } CONTAINS 'b'").await, Value::Bool(false));
	}

	#[tokio::test]
	async fn the_inside_family_reads_the_right_operand_as_the_container() {
		assert_eq!(ok("2 IN [1, 2, 3]").await, Value::Bool(true));
		assert_eq!(ok("2 NOT IN [1, 3]").await, Value::Bool(true));
		assert_eq!(ok("[1, 2] ALLINSIDE [1, 2, 3]").await, Value::Bool(true));
		assert_eq!(ok("[1, 9] ALLINSIDE [1, 2]").await, Value::Bool(false));
		assert_eq!(ok("[1, 9] ANYINSIDE [1, 2]").await, Value::Bool(true));
		assert_eq!(ok("[9] NONEINSIDE [1, 2]").await, Value::Bool(true));
		// The container may be an object, in which case its keys are searched.
		assert_eq!(ok("'a' IN { a: 1 }").await, Value::Bool(true));
	}

	#[tokio::test]
	async fn membership_in_a_range_honours_each_bound_kind() {
		assert_eq!(ok("5 IN 1..10").await, Value::Bool(true));
		// The end of `..` is excluded, `..=` includes it.
		assert_eq!(ok("10 IN 1..10").await, Value::Bool(false));
		assert_eq!(ok("10 IN 1..=10").await, Value::Bool(true));
		// The start of `..` is included, `>..` excludes it.
		assert_eq!(ok("1 IN 1..10").await, Value::Bool(true));
		assert_eq!(ok("1 IN 1>..10").await, Value::Bool(false));
	}

	#[tokio::test]
	async fn outside_is_the_negation_of_intersects_and_both_ignore_non_geometry() {
		assert_eq!(ok("(1, 2) INTERSECTS (1, 2)").await, Value::Bool(true));
		assert_eq!(ok("(1, 2) OUTSIDE (3, 4)").await, Value::Bool(true));

		// Both operators answer `false` for any non-geometry operand, which
		// makes OUTSIDE vacuously true for values that cannot intersect at all.
		assert_eq!(ok("1 INTERSECTS 2").await, Value::Bool(false));
		assert_eq!(ok("1 OUTSIDE 2").await, Value::Bool(true));
	}

	// =========================================================================
	// BinaryOp — range construction
	// =========================================================================

	#[tokio::test]
	async fn the_four_infix_range_operators_build_the_bounds_their_syntax_names() {
		assert_eq!(bounds(ok("1..5").await), (Bound::Included(1), Bound::Excluded(5)));
		assert_eq!(bounds(ok("1..=5").await), (Bound::Included(1), Bound::Included(5)));
		assert_eq!(bounds(ok("1>..5").await), (Bound::Excluded(1), Bound::Excluded(5)));
		assert_eq!(bounds(ok("1>..=5").await), (Bound::Excluded(1), Bound::Included(5)));
	}

	// =========================================================================
	// BinaryOp — the two index-backed operators
	// =========================================================================

	#[tokio::test]
	async fn a_matches_operator_never_reaches_the_generic_binary_evaluator() {
		// The planner routes every parsed `@@` to MatchesOp, including the two
		// shapes that cannot name an index — a non-idiom left operand, and a
		// right operand that does not resolve to a value at plan time. Neither
		// may answer from this evaluator, which has no index to consult and no
		// row table to decide against.
		let ctx = root_ctx();
		for src in ["'abc' @@ 'zzz'", "title @@ 42", "title @@ other_field"] {
			let expr = physical_expr(src, &ctx).await;
			assert_eq!(
				expr.name(),
				"MatchesOp",
				"{src} must be lowered to MatchesOp, got {}",
				expr.name()
			);
		}
	}

	#[tokio::test]
	async fn an_unindexable_matches_answers_false_off_an_executor_table() {
		// A row that no legacy `QueryExecutor` would have seen — here a value
		// with no record id — answers `false` rather than matching. Under the
		// generic binary evaluator's assumption that any row reaching it came
		// pre-filtered from a FullTextScan, these matched every row.
		let ctx = root_ctx();
		assert_eq!(ok("'abc' @@ 'zzz'").await, Value::Bool(false));

		let row = val("{ title: 'a book' }").await;
		assert_eq!(eval_on("title @@ 42", &row, &ctx).await.unwrap(), Value::Bool(false));
		assert_eq!(eval_on("title @@ other_field", &row, &ctx).await.unwrap(), Value::Bool(false));
	}

	#[tokio::test]
	async fn a_knn_operator_no_scan_lowered_answers_false_for_every_row() {
		// The planner routes every parsed `<|k|>` to KnnMembershipOp, so this
		// arm is only reachable for a hand-built node; it answers `false`,
		// matching the legacy executor's missing-entry semantics.
		let ctx = root_ctx();
		let knn = BinaryOp {
			left: physical_expr("1", &ctx).await,
			op: BinaryOperator::NearestNeighbor(Box::new(NearestNeighbor::KTree(3))),
			right: physical_expr("[1, 2]", &ctx).await,
		};
		let out = knn.evaluate(EvalContext::from_exec_ctx(&ctx)).await.unwrap();
		assert_eq!(out, Value::Bool(false));
	}

	// =========================================================================
	// BinaryOp — flow propagation
	// =========================================================================

	#[tokio::test]
	async fn the_left_operand_is_evaluated_first_and_its_failure_wins() {
		// Both operands fail; the reported failure identifies the evaluation
		// order.
		assert!(failure("(THROW 'left') AND (THROW 'right')").await.contains("left"));
		assert!(failure("(THROW 'left') + (THROW 'right')").await.contains("left"));
		// A non-short-circuiting operator does evaluate the right operand.
		assert!(failure("1 + (THROW 'right')").await.contains("right"));
	}

	#[tokio::test]
	async fn control_flow_out_of_an_operand_passes_through_as_a_signal_not_an_error() {
		let ctx = root_ctx();
		let flow = eval("true AND BREAK", &ctx).await.unwrap_err();
		assert!(matches!(flow, ControlFlow::Break), "got {flow}");

		let flow = eval("true AND CONTINUE", &ctx).await.unwrap_err();
		assert!(matches!(flow, ControlFlow::Continue), "got {flow}");

		let flow = eval("true AND RETURN 7", &ctx).await.unwrap_err();
		match flow {
			ControlFlow::Return(value) => assert_eq!(value, Value::from(7)),
			other => panic!("got {other}"),
		}
	}

	// =========================================================================
	// BinaryOp — metadata the executor acts on
	// =========================================================================

	#[tokio::test]
	async fn access_mode_is_read_write_when_either_operand_can_write() {
		// The executor picks the transaction type from the plan's access mode,
		// so a mutation buried in one operand has to reach the top.
		let ctx = root_ctx();
		assert_eq!(physical_expr("1 = 2", &ctx).await.access_mode(), AccessMode::ReadOnly);
		assert_eq!(
			physical_expr("1 = eval::surql('RETURN 1')", &ctx).await.access_mode(),
			AccessMode::ReadWrite
		);
		assert_eq!(
			physical_expr("eval::surql('RETURN 1') = 1", &ctx).await.access_mode(),
			AccessMode::ReadWrite
		);
	}

	#[tokio::test]
	async fn required_context_is_the_higher_of_the_two_operands() {
		// The executor validates the level before running, so an operand that
		// needs a database must raise the whole expression.
		let ctx = root_ctx();
		assert_eq!(physical_expr("1 + 2", &ctx).await.required_context(), ContextLevel::Root);
		assert_eq!(physical_expr("1 + a.b", &ctx).await.required_context(), ContextLevel::Database);
		assert_eq!(physical_expr("a.b + 1", &ctx).await.required_context(), ContextLevel::Database);
	}

	#[tokio::test]
	async fn explain_traversal_sees_both_operands_left_before_right() {
		let ctx = root_ctx();
		let expr =
			physical_expr("(SELECT VALUE 1 FROM [1]) = (SELECT VALUE 2 FROM [2])", &ctx).await;

		let children = expr.expr_children();
		let roles: Vec<&str> = children.iter().map(|(role, _)| *role).collect();
		assert_eq!(roles, vec!["left", "right"]);

		// Operator sub-trees owned by either operand are concatenated left
		// first, so EXPLAIN prints them beneath the expression in source order.
		let left = children[0].1.downcast_ref::<ScalarSubquery>().expect("left is a subquery");
		let right = children[1].1.downcast_ref::<ScalarSubquery>().expect("right is a subquery");
		let embedded = expr.embedded_operators();
		assert_eq!(embedded.len(), 2);
		assert!(Arc::ptr_eq(embedded[0].1, &left.plan), "the left operand's plan comes first");
		assert!(Arc::ptr_eq(embedded[1].1, &right.plan), "the right operand's plan comes second");

		// An operand with no plan of its own contributes nothing.
		let plain = physical_expr("1 = 2", &ctx).await;
		assert!(plain.embedded_operators().is_empty());
	}

	// =========================================================================
	// SimpleBinaryOp
	// =========================================================================

	#[tokio::test]
	async fn the_planner_lowers_field_op_literal_and_leaves_everything_else_alone() {
		// The pre-decode filter compiles its byte-level predicate by
		// downcasting to these concrete types, so the lowering has to be
		// observable through `as_any`.
		let ctx = root_ctx();
		assert!(physical_expr("age > 10", &ctx).await.downcast_ref::<SimpleBinaryOp>().is_some());
		assert!(physical_expr("10 > age", &ctx).await.downcast_ref::<SimpleBinaryOp>().is_some());
		// Two fields, no literal — stays a general BinaryOp.
		assert!(physical_expr("age > other", &ctx).await.downcast_ref::<BinaryOp>().is_some());
		// A nested path is not a simple field.
		assert!(physical_expr("a.b > 10", &ctx).await.downcast_ref::<BinaryOp>().is_some());
		// Arithmetic is never eligible, however simple the operands.
		assert!(physical_expr("age + 10", &ctx).await.downcast_ref::<BinaryOp>().is_some());
	}

	#[tokio::test]
	async fn an_absent_field_compares_as_none_rather_than_failing() {
		let ctx = root_ctx();
		let row = val("{ name: 'tobie' }").await;

		assert_eq!(eval_on("age = NONE", &row, &ctx).await.unwrap(), Value::Bool(true));
		assert_eq!(eval_on("age > 10", &row, &ctx).await.unwrap(), Value::Bool(false));
		// NONE sorts below every number, so an absent field satisfies `<`.
		assert_eq!(eval_on("age < 10", &row, &ctx).await.unwrap(), Value::Bool(true));
	}

	#[tokio::test]
	async fn the_literal_stays_on_the_side_the_source_put_it() {
		let ctx = root_ctx();
		let row = val("{ age: 20 }").await;

		// `reversed` has to swap the operands back, or every non-commutative
		// comparison with a literal on the left would invert.
		assert_eq!(eval_on("10 < age", &row, &ctx).await.unwrap(), Value::Bool(true));
		assert_eq!(eval_on("10 > age", &row, &ctx).await.unwrap(), Value::Bool(false));
		assert_eq!(eval_on("age > 10", &row, &ctx).await.unwrap(), Value::Bool(true));
		assert_eq!(eval_on("age < 10", &row, &ctx).await.unwrap(), Value::Bool(false));

		// Containment is directional too: the container is whichever side the
		// source named.
		assert_eq!(eval_on("age IN [20, 30]", &row, &ctx).await.unwrap(), Value::Bool(true));
		assert_eq!(eval_on("[20, 30] CONTAINS age", &row, &ctx).await.unwrap(), Value::Bool(true));
	}

	#[tokio::test]
	async fn a_record_id_row_is_dereferenced_before_the_field_is_compared() {
		// The slow path: a row that is not an Object goes through
		// `evaluate_field`, which fetches the record. This is what makes the
		// expression require database context.
		let db = TestDb::new(
			"CREATE person:tobie SET age = 20;
			 CREATE person:jaime SET age = 5;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let expr = physical_expr("age > 10", &ctx).await;

		let tobie = val("person:tobie").await;
		let jaime = val("person:jaime").await;
		let base = EvalContext::from_exec_ctx(&ctx);
		assert_eq!(
			expr.evaluate(base.with_value_and_doc(&tobie)).await.unwrap(),
			Value::Bool(true)
		);
		assert_eq!(
			expr.evaluate(base.with_value_and_doc(&jaime)).await.unwrap(),
			Value::Bool(false)
		);
	}

	#[tokio::test]
	async fn a_field_read_on_an_array_row_collects_one_value_per_element() {
		// `evaluate_field` maps over an array row, so the comparison is applied
		// to the collected list rather than to each element — the folding
		// operators are the ones that then look inside it.
		let ctx = root_ctx();
		let rows = val("[{ age: 20 }, { age: 5 }]").await;

		assert_eq!(eval_on("age ?= 5", &rows, &ctx).await.unwrap(), Value::Bool(true));
		assert_eq!(eval_on("age ?= 99", &rows, &ctx).await.unwrap(), Value::Bool(false));
		assert_eq!(eval_on("age *= 20", &rows, &ctx).await.unwrap(), Value::Bool(false));
	}

	#[tokio::test]
	async fn batch_evaluation_agrees_with_per_row_evaluation() {
		let db = TestDb::new("CREATE person:jaime SET age = 5;").await;
		let ctx = db.exec_ctx().await;
		let expr = physical_expr("age > 10", &ctx).await;
		let base = EvalContext::from_exec_ctx(&ctx);

		// All-Object batch: the synchronous fast path.
		let objects = vec![val("{ age: 20 }").await, val("{ age: 5 }").await, val("{}").await];
		let batched = expr.evaluate_batch(base.clone(), &objects).await.unwrap();
		let mut per_row = Vec::new();
		for row in &objects {
			per_row.push(expr.evaluate(base.with_value_and_doc(row)).await.unwrap());
		}
		assert_eq!(batched, per_row);
		assert_eq!(batched, vec![Value::Bool(true), Value::Bool(false), Value::Bool(false)]);

		// One non-Object row forces the whole batch onto the per-row path; the
		// results must still line up with the input positions.
		let mixed = vec![val("{ age: 20 }").await, val("person:jaime").await];
		let batched = expr.evaluate_batch(base.clone(), &mixed).await.unwrap();
		assert_eq!(batched, vec![Value::Bool(true), Value::Bool(false)]);

		// An empty batch is not an error.
		assert!(expr.evaluate_batch(base, &[]).await.unwrap().is_empty());
	}

	/// Mirror of the operator arms `SimpleBinaryOp` implements. The match is
	/// exhaustive so a new [`BinaryOperator`] variant cannot be added without
	/// classifying it here.
	fn simple_binary_op_has_an_arm(op: &BinaryOperator) -> bool {
		match op {
			BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
			| BinaryOperator::Inside
			| BinaryOperator::NotInside
			| BinaryOperator::AllInside
			| BinaryOperator::AnyInside
			| BinaryOperator::NoneInside
			| BinaryOperator::Outside
			| BinaryOperator::Intersects => true,
			BinaryOperator::Add
			| BinaryOperator::Subtract
			| BinaryOperator::Multiply
			| BinaryOperator::Divide
			| BinaryOperator::Remainder
			| BinaryOperator::Power
			| BinaryOperator::And
			| BinaryOperator::Or
			| BinaryOperator::NullCoalescing
			| BinaryOperator::TenaryCondition
			| BinaryOperator::Range
			| BinaryOperator::RangeInclusive
			| BinaryOperator::RangeSkip
			| BinaryOperator::RangeSkipInclusive
			| BinaryOperator::Matches(_)
			| BinaryOperator::NearestNeighbor(_) => false,
		}
	}

	#[tokio::test]
	async fn every_operator_the_planner_lowers_has_an_arm_in_both_evaluators() {
		let all = [
			BinaryOperator::Add,
			BinaryOperator::Subtract,
			BinaryOperator::Multiply,
			BinaryOperator::Divide,
			BinaryOperator::Remainder,
			BinaryOperator::Power,
			BinaryOperator::Equal,
			BinaryOperator::ExactEqual,
			BinaryOperator::NotEqual,
			BinaryOperator::AllEqual,
			BinaryOperator::AnyEqual,
			BinaryOperator::And,
			BinaryOperator::Or,
			BinaryOperator::NullCoalescing,
			BinaryOperator::TenaryCondition,
			BinaryOperator::LessThan,
			BinaryOperator::LessThanEqual,
			BinaryOperator::MoreThan,
			BinaryOperator::MoreThanEqual,
			BinaryOperator::Contain,
			BinaryOperator::NotContain,
			BinaryOperator::ContainAll,
			BinaryOperator::ContainAny,
			BinaryOperator::ContainNone,
			BinaryOperator::Inside,
			BinaryOperator::NotInside,
			BinaryOperator::AllInside,
			BinaryOperator::AnyInside,
			BinaryOperator::NoneInside,
			BinaryOperator::Outside,
			BinaryOperator::Intersects,
			BinaryOperator::Range,
			BinaryOperator::RangeInclusive,
			BinaryOperator::RangeSkip,
			BinaryOperator::RangeSkipInclusive,
			BinaryOperator::Matches(MatchesOperator {
				rf: None,
				operator: BooleanOperator::And,
			}),
			BinaryOperator::NearestNeighbor(Box::new(NearestNeighbor::KTree(1))),
		];

		let ctx = root_ctx();
		let base = EvalContext::from_exec_ctx(&ctx);
		let row = val("{ age: 10 }").await;
		let rows = [val("{ age: 10 }").await, val("{ age: 11 }").await];

		for op in all {
			// The planner's eligibility test and the arms implemented here must
			// name the same set, or a lowered expression hits `unreachable!`.
			assert_eq!(
				crate::exec::planner::is_simple_binary_eligible(&op),
				simple_binary_op_has_an_arm(&op),
				"eligibility and implementation disagree for {op:?}"
			);
			if !simple_binary_op_has_an_arm(&op) {
				continue;
			}
			let expr = SimpleBinaryOp {
				field_name: "age".to_owned(),
				op: op.clone(),
				literal: Value::from(10),
				reversed: false,
			};
			expr.evaluate(base.with_value_and_doc(&row))
				.await
				.unwrap_or_else(|e| panic!("evaluate failed for {op:?}: {e}"));
			expr.evaluate_batch(base.clone(), &rows)
				.await
				.unwrap_or_else(|e| panic!("evaluate_batch failed for {op:?}: {e}"));
		}
	}

	#[tokio::test]
	async fn a_lowered_comparison_still_demands_database_context_and_stays_read_only() {
		// A field read may dereference a RecordId (see the record-id row test),
		// so the level is Database even though the operands are a name and a
		// constant. It can never write.
		let ctx = root_ctx();
		let expr = physical_expr("age > 10", &ctx).await;
		assert_eq!(expr.required_context(), ContextLevel::Database);
		assert_eq!(expr.access_mode(), AccessMode::ReadOnly);
	}

	#[tokio::test]
	async fn a_lowered_comparison_renders_its_operands_in_source_order() {
		let ctx = root_ctx();
		assert_eq!(physical_expr("age = 10", &ctx).await.to_sql(), "age = 10");
		assert_eq!(physical_expr("10 < age", &ctx).await.to_sql(), "10 < age");
		// The literal is rendered as SurrealQL, so a string keeps its quotes.
		assert_eq!(physical_expr("name = 'tobie'", &ctx).await.to_sql(), "name = 'tobie'");
	}

	#[tokio::test]
	async fn a_lowered_comparison_renders_the_field_name_unquoted() {
		// The field name is written out raw, so a name that needs quoting is
		// rendered as a form that does not parse back.
		let ctx = root_ctx();
		let expr = physical_expr("`my field` = 1", &ctx).await;
		assert_eq!(expr.to_sql(), "my field = 1");
	}

	// =========================================================================
	// UnaryOp
	// =========================================================================

	#[tokio::test]
	async fn not_reduces_any_value_to_a_boolean_by_truthiness() {
		assert_eq!(ok("!true").await, Value::Bool(false));
		assert_eq!(ok("!false").await, Value::Bool(true));
		assert_eq!(ok("!0").await, Value::Bool(true));
		assert_eq!(ok("!1").await, Value::Bool(false));
		assert_eq!(ok("!NONE").await, Value::Bool(true));
		assert_eq!(ok("!NULL").await, Value::Bool(true));
		// Emptiness is falsiness for the container kinds.
		assert_eq!(ok("!''").await, Value::Bool(true));
		assert_eq!(ok("!'x'").await, Value::Bool(false));
		assert_eq!(ok("![]").await, Value::Bool(true));
		assert_eq!(ok("!{}").await, Value::Bool(true));
	}

	#[tokio::test]
	async fn negate_demands_a_number_while_positive_passes_any_value_through() {
		let ctx = root_ctx();
		let row = val("{ age: 20 }").await;

		assert_eq!(eval_on("-age", &row, &ctx).await.unwrap(), Value::from(-20));
		assert!(failure("-'a'").await.contains("negate"));

		// `+` is a no-op for every kind, not just for numbers: it neither
		// converts nor rejects a non-numeric operand.
		assert_eq!(eval_on("+age", &row, &ctx).await.unwrap(), Value::from(20));
		assert_eq!(ok("+'abc'").await, Value::from("abc"));
		assert_eq!(ok("+NONE").await, Value::None);
	}

	#[tokio::test]
	async fn a_cast_prefix_converts_or_reports_the_target_kind_and_input() {
		assert_eq!(ok("<int> '42'").await, Value::from(42));
		assert_eq!(ok("<string> 42").await, Value::from("42"));
		assert_eq!(ok("<bool> 'true'").await, Value::Bool(true));
		assert_eq!(ok("<float> 1").await, Value::from(1.0));

		let err = failure("<int> 'abc'").await;
		assert!(err.contains("int") && err.contains("abc"), "got {err}");
	}

	#[tokio::test]
	async fn the_prefix_range_operators_leave_the_start_unbounded() {
		assert_eq!(bounds(ok("..5").await), (Bound::Unbounded, Bound::Excluded(5)));
		assert_eq!(bounds(ok("..=5").await), (Bound::Unbounded, Bound::Included(5)));
	}

	#[tokio::test]
	async fn a_unary_wrapper_reports_its_operand_metadata_and_propagates_its_failure() {
		let ctx = root_ctx();
		assert_eq!(physical_expr("!true", &ctx).await.required_context(), ContextLevel::Root);
		assert_eq!(physical_expr("!a.b", &ctx).await.required_context(), ContextLevel::Database);
		assert_eq!(physical_expr("!true", &ctx).await.access_mode(), AccessMode::ReadOnly);
		assert_eq!(
			physical_expr("!eval::surql('RETURN 1')", &ctx).await.access_mode(),
			AccessMode::ReadWrite
		);

		assert!(failure("!(THROW 'boom')").await.contains("boom"));
		let flow = eval("!(BREAK)", &ctx).await.unwrap_err();
		assert!(matches!(flow, ControlFlow::Break), "got {flow}");

		// EXPLAIN reaches the operand under a fixed role name.
		let not = physical_expr("!true", &ctx).await;
		let roles: Vec<&str> = not.expr_children().into_iter().map(|(role, _)| role).collect();
		assert_eq!(roles, vec!["operand"]);
	}

	// =========================================================================
	// PostfixOp
	// =========================================================================

	#[tokio::test]
	async fn the_postfix_range_operators_leave_the_end_unbounded() {
		assert_eq!(bounds(ok("5..").await), (Bound::Included(5), Bound::Unbounded));
		assert_eq!(bounds(ok("5>..").await), (Bound::Excluded(5), Bound::Unbounded));
	}

	#[tokio::test]
	async fn a_method_call_postfix_is_rejected_by_the_physical_layer() {
		// Method calls are not lowered here; the arm must fail loudly rather
		// than answer something.
		let ctx = root_ctx();
		let call = PostfixOp {
			op: PostfixOperator::MethodCall("len".to_owned(), vec![]),
			expr: physical_expr("'abc'", &ctx).await,
		};
		let err = call.evaluate(EvalContext::from_exec_ctx(&ctx)).await.unwrap_err();
		assert!(err.to_string().contains("Method calls not yet supported"), "got {err}");
	}

	#[tokio::test]
	async fn a_postfix_wrapper_reports_its_operand_metadata_and_propagates_its_failure() {
		let ctx = root_ctx();
		assert_eq!(physical_expr("1..", &ctx).await.required_context(), ContextLevel::Root);
		assert_eq!(physical_expr("a.b..", &ctx).await.required_context(), ContextLevel::Database);
		assert_eq!(
			physical_expr("eval::surql('RETURN 1')..", &ctx).await.access_mode(),
			AccessMode::ReadWrite
		);

		assert!(failure("(THROW 'boom')..").await.contains("boom"));

		let range = physical_expr("1..", &ctx).await;
		let roles: Vec<&str> = range.expr_children().into_iter().map(|(role, _)| role).collect();
		assert_eq!(roles, vec!["operand"]);
	}

	// =========================================================================
	// ToSql
	// =========================================================================

	#[tokio::test]
	async fn operator_rendering_omits_the_grouping_that_decided_the_evaluation() {
		// Nested operators are rendered without parentheses, so a rendering can
		// name a different expression than the one it came from: this renders
		// as `1 + 2 * 3`, which parses as 7, while the expression evaluates 9.
		let ctx = root_ctx();
		assert_eq!(physical_expr("(1 + 2) * 3", &ctx).await.to_sql(), "1 + 2 * 3");
		assert_eq!(ok("(1 + 2) * 3").await, Value::from(9));
		assert_eq!(ok("1 + 2 * 3").await, Value::from(7));

		// The same holds for a prefix operator over a binary operand.
		assert_eq!(physical_expr("!(true AND false)", &ctx).await.to_sql(), "! true AND false");
		assert_eq!(ok("!(true AND false)").await, Value::Bool(true));
		assert_eq!(ok("!true AND false").await, Value::Bool(false));
	}
}
