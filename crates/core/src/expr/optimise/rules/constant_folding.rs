use anyhow::Result;

use crate::expr::optimise::utils::{constant_to_value, expr_as_value};
use crate::expr::optimise::{OptimiserRule, Transformed};
use crate::expr::{BinaryOperator, Expr, PrefixOperator};
use crate::fnc;
use crate::val::Value;

/// Optimization rule that evaluates constant expressions at compile time.
///
/// This rule identifies operations on constant values (Expr::Value or Expr::Constant)
/// and computes their results, replacing the expression with the computed value.
///
/// Examples:
/// - `1 + 2` → `3`
/// - `10 * 5` → `50`
/// - `true AND false` → `false`
/// - `5 > 3` → `true`
/// - `math::pi * 2` → computed value
///
/// Limitations:
/// - Skips async operations (matches, knn)
/// - Skips function calls (may add whitelist later)
/// - Skips operations on ranges (can't be easily folded)
pub(crate) struct ConstantFolding;

impl OptimiserRule for ConstantFolding {
	fn optimise_expr(&self, expr: Expr) -> Result<Transformed<Expr>> {
		optimise_expr_recursive(expr)
	}
}

fn optimise_expr_recursive(expr: Expr) -> Result<Transformed<Expr>> {
	match expr {
		// Convert constants to values first
		Expr::Constant(ref constant) => match constant_to_value(constant) {
			Ok(value) => Ok(Transformed::yes(Expr::Value(value))),
			Err(_) => Ok(Transformed::no(expr)),
		},

		// Optimise binary operations
		Expr::Binary {
			left,
			op,
			right,
		} => {
			// First, recursively optimise children
			let left_result = optimise_expr_recursive(*left)?;
			let right_result = optimise_expr_recursive(*right)?;
			let transformed = left_result.transformed || right_result.transformed;

			// Try to fold if both sides are values
			let left_value = expr_as_value(&left_result.data);
			let right_value = expr_as_value(&right_result.data);

			if let (Some(left_val), Some(right_val)) = (left_value, right_value)
				&& let Some(folded) = try_fold_binary(&op, left_val, right_val)
			{
				return Ok(Transformed::yes(Expr::Value(folded)));
			}

			Ok(Transformed {
				data: Expr::Binary {
					left: Box::new(left_result.data),
					op,
					right: Box::new(right_result.data),
				},
				transformed,
			})
		}

		// Optimise prefix operations
		Expr::Prefix {
			op,
			expr: inner,
		} => {
			let result = optimise_expr_recursive(*inner)?;

			// Try to fold if the operand is a value
			if let Some(value) = expr_as_value(&result.data)
				&& let Some(folded) = try_fold_prefix(&op, value)
			{
				return Ok(Transformed::yes(Expr::Value(folded)));
			}

			Ok(result.map(|e| Expr::Prefix {
				op,
				expr: Box::new(e),
			}))
		}

		// Optimise postfix operations (limited support)
		Expr::Postfix {
			expr: inner,
			op,
		} => {
			let result = optimise_expr_recursive(*inner)?;
			// Most postfix operations (ranges, method calls) can't be folded
			Ok(result.map(|e| Expr::Postfix {
				expr: Box::new(e),
				op,
			}))
		}

		// Recursively optimise composite expressions
		Expr::Block(block) => {
			let (new_exprs, transformed) = optimise_expr_vec(block.0.clone())?;
			Ok(Transformed {
				data: Expr::Block(Box::new(crate::expr::Block(new_exprs))),
				transformed,
			})
		}

		Expr::FunctionCall(mut call) => {
			let (new_args, transformed) = optimise_expr_vec(call.arguments)?;
			call.arguments = new_args;
			// Don't fold function calls for now
			Ok(Transformed {
				data: Expr::FunctionCall(call),
				transformed,
			})
		}

		Expr::Throw(expr) => {
			let result = optimise_expr_recursive(*expr)?;
			Ok(result.map(|e| Expr::Throw(Box::new(e))))
		}

		Expr::Return(mut output) => {
			let result = optimise_expr_recursive(output.what)?;
			output.what = result.data;
			Ok(Transformed {
				data: Expr::Return(output),
				transformed: result.transformed,
			})
		}

		Expr::IfElse(mut ifelse) => {
			let mut transformed = false;
			let mut new_exprs = Vec::with_capacity(ifelse.exprs.len());
			for (cond, then) in ifelse.exprs {
				let cond_result = optimise_expr_recursive(cond)?;
				transformed = transformed || cond_result.transformed;
				let then_result = optimise_expr_recursive(then)?;
				transformed = transformed || then_result.transformed;
				new_exprs.push((cond_result.data, then_result.data));
			}
			ifelse.exprs = new_exprs;

			if let Some(close) = ifelse.close {
				let close_result = optimise_expr_recursive(close)?;
				transformed = transformed || close_result.transformed;
				ifelse.close = Some(close_result.data);
			}

			Ok(Transformed {
				data: Expr::IfElse(ifelse),
				transformed,
			})
		}

		// Leaf nodes that don't need optimization
		Expr::Value(_)
		| Expr::Literal(_)
		| Expr::Param(_)
		| Expr::Idiom(_)
		| Expr::Table(_)
		| Expr::Mock(_)
		| Expr::Closure(_)
		| Expr::Break
		| Expr::Continue
		| Expr::Select(_)
		| Expr::Create(_)
		| Expr::Update(_)
		| Expr::Upsert(_)
		| Expr::Delete(_)
		| Expr::Relate(_)
		| Expr::Insert(_)
		| Expr::Define(_)
		| Expr::Remove(_)
		| Expr::Rebuild(_)
		| Expr::Alter(_)
		| Expr::Info(_)
		| Expr::Foreach(_)
		| Expr::Let(_)
		| Expr::Sleep(_) => Ok(Transformed::no(expr)),
	}
}

fn optimise_expr_vec(exprs: Vec<Expr>) -> Result<(Vec<Expr>, bool)> {
	let mut transformed = false;
	let mut new_exprs = Vec::with_capacity(exprs.len());
	for expr in exprs {
		let result = optimise_expr_recursive(expr)?;
		transformed = transformed || result.transformed;
		new_exprs.push(result.data);
	}
	Ok((new_exprs, transformed))
}

/// Try to fold a binary operation on two constant values
fn try_fold_binary(op: &BinaryOperator, left: &Value, right: &Value) -> Option<Value> {
	let result = match op {
		// Arithmetic operations
		BinaryOperator::Add => fnc::operate::add(left.clone(), right.clone()),
		BinaryOperator::Subtract => fnc::operate::sub(left.clone(), right.clone()),
		BinaryOperator::Multiply => fnc::operate::mul(left.clone(), right.clone()),
		BinaryOperator::Divide => fnc::operate::div(left.clone(), right.clone()),
		BinaryOperator::Remainder => fnc::operate::rem(left.clone(), right.clone()),
		BinaryOperator::Power => fnc::operate::pow(left.clone(), right.clone()),

		// Comparison operations
		BinaryOperator::Equal => fnc::operate::equal(left, right),
		BinaryOperator::ExactEqual => fnc::operate::exact(left, right),
		BinaryOperator::NotEqual => fnc::operate::not_equal(left, right),
		BinaryOperator::AllEqual => fnc::operate::all_equal(left, right),
		BinaryOperator::AnyEqual => fnc::operate::any_equal(left, right),
		BinaryOperator::LessThan => fnc::operate::less_than(left, right),
		BinaryOperator::LessThanEqual => fnc::operate::less_than_or_equal(left, right),
		BinaryOperator::MoreThan => fnc::operate::more_than(left, right),
		BinaryOperator::MoreThanEqual => fnc::operate::more_than_or_equal(left, right),

		// Logical operations (short-circuiting handled differently)
		BinaryOperator::And => {
			if !left.is_truthy() {
				return Some(left.clone());
			}
			return Some(right.clone());
		}
		BinaryOperator::Or | BinaryOperator::TenaryCondition => {
			if left.is_truthy() {
				return Some(left.clone());
			}
			return Some(right.clone());
		}
		BinaryOperator::NullCoalescing => {
			if !left.is_nullish() {
				return Some(left.clone());
			}
			return Some(right.clone());
		}

		// Set operations
		BinaryOperator::Contain => fnc::operate::contain(left, right),
		BinaryOperator::NotContain => fnc::operate::not_contain(left, right),
		BinaryOperator::ContainAll => fnc::operate::contain_all(left, right),
		BinaryOperator::ContainAny => fnc::operate::contain_any(left, right),
		BinaryOperator::ContainNone => fnc::operate::contain_none(left, right),
		BinaryOperator::Inside => fnc::operate::inside(left, right),
		BinaryOperator::NotInside => fnc::operate::not_inside(left, right),
		BinaryOperator::AllInside => fnc::operate::inside_all(left, right),
		BinaryOperator::AnyInside => fnc::operate::inside_any(left, right),
		BinaryOperator::NoneInside => fnc::operate::inside_none(left, right),
		BinaryOperator::Outside => fnc::operate::outside(left, right),
		BinaryOperator::Intersects => fnc::operate::intersects(left, right),

		// Range operations - these create ranges, not values we can fold
		BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive => return None,

		// Skip async operations
		BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_) => return None,
	};

	result.ok()
}

/// Try to fold a prefix operation on a constant value
fn try_fold_prefix(op: &PrefixOperator, value: &Value) -> Option<Value> {
	let result = match op {
		PrefixOperator::Not => fnc::operate::not(value.clone()),
		PrefixOperator::Negate => fnc::operate::neg(value.clone()),
		PrefixOperator::Positive => Ok(value.clone()),
		// Range operations create ranges, not values
		PrefixOperator::Range | PrefixOperator::RangeInclusive => return None,
		// Cast operations need kind information
		PrefixOperator::Cast(_) => return None,
	};

	result.ok()
}

#[cfg(test)]
mod tests {
	use std::f64::consts as f64c;

	use rstest::rstest;

	use super::*;
	use crate::expr::Constant;
	use crate::val::Number;

	fn int_value(n: i64) -> Expr {
		Expr::Value(Value::Number(Number::Int(n)))
	}

	fn float_value(f: f64) -> Expr {
		Expr::Value(Value::Number(Number::Float(f)))
	}

	fn bool_value(b: bool) -> Expr {
		Expr::Value(Value::Bool(b))
	}

	fn string_value(s: &str) -> Expr {
		Expr::Value(Value::String(s.to_string()))
	}

	fn binary_expr(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
		Expr::Binary {
			left: Box::new(left),
			op,
			right: Box::new(right),
		}
	}

	fn prefix_expr(op: PrefixOperator, expr: Expr) -> Expr {
		Expr::Prefix {
			op,
			expr: Box::new(expr),
		}
	}

	#[rstest]
	// Constant conversion tests
	#[case::math_pi(Expr::Constant(Constant::MathPi), float_value(f64c::PI))]
	#[case::math_e(Expr::Constant(Constant::MathE), float_value(f64c::E))]
	#[case::math_tau(Expr::Constant(Constant::MathTau), float_value(f64c::TAU))]
	#[case::math_inf(Expr::Constant(Constant::MathInf), float_value(f64::INFINITY))]
	#[case::math_neg_inf(Expr::Constant(Constant::MathNegInf), float_value(f64::NEG_INFINITY))]
	// Arithmetic operations - Add
	#[case::add_integers(
		binary_expr(int_value(5), BinaryOperator::Add, int_value(3)),
		int_value(8)
	)]
	#[case::add_negative_integers(
		binary_expr(int_value(-5), BinaryOperator::Add, int_value(3)),
		int_value(-2)
	)]
	#[case::add_floats(
		binary_expr(float_value(2.5), BinaryOperator::Add, float_value(1.5)),
		float_value(4.0)
	)]
	// Arithmetic operations - Subtract
	#[case::subtract_integers(
		binary_expr(int_value(10), BinaryOperator::Subtract, int_value(3)),
		int_value(7)
	)]
	#[case::subtract_negative(
		binary_expr(int_value(5), BinaryOperator::Subtract, int_value(10)),
		int_value(-5)
	)]
	// Arithmetic operations - Multiply
	#[case::multiply_integers(
		binary_expr(int_value(4), BinaryOperator::Multiply, int_value(3)),
		int_value(12)
	)]
	#[case::multiply_by_zero(
		binary_expr(int_value(100), BinaryOperator::Multiply, int_value(0)),
		int_value(0)
	)]
	#[case::multiply_negative(
		binary_expr(int_value(-5), BinaryOperator::Multiply, int_value(3)),
		int_value(-15)
	)]
	// Arithmetic operations - Divide
	#[case::divide_integers(
		binary_expr(int_value(15), BinaryOperator::Divide, int_value(3)),
		int_value(5)
	)]
	#[case::divide_floats(
		binary_expr(float_value(10.0), BinaryOperator::Divide, float_value(2.0)),
		float_value(5.0)
	)]
	// Arithmetic operations - Remainder
	#[case::remainder_integers(
		binary_expr(int_value(10), BinaryOperator::Remainder, int_value(3)),
		int_value(1)
	)]
	#[case::remainder_exact(
		binary_expr(int_value(10), BinaryOperator::Remainder, int_value(5)),
		int_value(0)
	)]
	// Arithmetic operations - Power
	#[case::power_integers(
		binary_expr(int_value(2), BinaryOperator::Power, int_value(3)),
		int_value(8)
	)]
	#[case::power_zero(
		binary_expr(int_value(5), BinaryOperator::Power, int_value(0)),
		int_value(1)
	)]
	// Comparison operations - Equal
	#[case::equal_true(
		binary_expr(int_value(5), BinaryOperator::Equal, int_value(5)),
		bool_value(true)
	)]
	#[case::equal_false(
		binary_expr(int_value(5), BinaryOperator::Equal, int_value(3)),
		bool_value(false)
	)]
	#[case::equal_strings_true(
		binary_expr(string_value("hello"), BinaryOperator::Equal, string_value("hello")),
		bool_value(true)
	)]
	#[case::equal_strings_false(
		binary_expr(string_value("hello"), BinaryOperator::Equal, string_value("world")),
		bool_value(false)
	)]
	// Comparison operations - NotEqual
	#[case::not_equal_true(
		binary_expr(int_value(5), BinaryOperator::NotEqual, int_value(3)),
		bool_value(true)
	)]
	#[case::not_equal_false(
		binary_expr(int_value(5), BinaryOperator::NotEqual, int_value(5)),
		bool_value(false)
	)]
	// Comparison operations - LessThan
	#[case::less_than_true(
		binary_expr(int_value(3), BinaryOperator::LessThan, int_value(5)),
		bool_value(true)
	)]
	#[case::less_than_false(
		binary_expr(int_value(5), BinaryOperator::LessThan, int_value(3)),
		bool_value(false)
	)]
	#[case::less_than_equal(
		binary_expr(int_value(5), BinaryOperator::LessThan, int_value(5)),
		bool_value(false)
	)]
	// Comparison operations - LessThanEqual
	#[case::less_than_equal_true(
		binary_expr(int_value(3), BinaryOperator::LessThanEqual, int_value(5)),
		bool_value(true)
	)]
	#[case::less_than_equal_equal(
		binary_expr(int_value(5), BinaryOperator::LessThanEqual, int_value(5)),
		bool_value(true)
	)]
	#[case::less_than_equal_false(
		binary_expr(int_value(5), BinaryOperator::LessThanEqual, int_value(3)),
		bool_value(false)
	)]
	// Comparison operations - MoreThan
	#[case::more_than_true(
		binary_expr(int_value(5), BinaryOperator::MoreThan, int_value(3)),
		bool_value(true)
	)]
	#[case::more_than_false(
		binary_expr(int_value(3), BinaryOperator::MoreThan, int_value(5)),
		bool_value(false)
	)]
	// Comparison operations - MoreThanEqual
	#[case::more_than_equal_true(
		binary_expr(int_value(5), BinaryOperator::MoreThanEqual, int_value(3)),
		bool_value(true)
	)]
	#[case::more_than_equal_equal(
		binary_expr(int_value(5), BinaryOperator::MoreThanEqual, int_value(5)),
		bool_value(true)
	)]
	#[case::more_than_equal_false(
		binary_expr(int_value(3), BinaryOperator::MoreThanEqual, int_value(5)),
		bool_value(false)
	)]
	// Logical operations - And
	#[case::and_true_true(
		binary_expr(bool_value(true), BinaryOperator::And, bool_value(true)),
		bool_value(true)
	)]
	#[case::and_true_false(
		binary_expr(bool_value(true), BinaryOperator::And, bool_value(false)),
		bool_value(false)
	)]
	#[case::and_false_true(
		binary_expr(bool_value(false), BinaryOperator::And, bool_value(true)),
		bool_value(false)
	)]
	#[case::and_false_false(
		binary_expr(bool_value(false), BinaryOperator::And, bool_value(false)),
		bool_value(false)
	)]
	#[case::and_short_circuit_left(
		binary_expr(bool_value(false), BinaryOperator::And, int_value(42)),
		bool_value(false)
	)]
	#[case::and_passthrough_right(
		binary_expr(bool_value(true), BinaryOperator::And, int_value(42)),
		int_value(42)
	)]
	// Logical operations - Or
	#[case::or_true_true(
		binary_expr(bool_value(true), BinaryOperator::Or, bool_value(true)),
		bool_value(true)
	)]
	#[case::or_true_false(
		binary_expr(bool_value(true), BinaryOperator::Or, bool_value(false)),
		bool_value(true)
	)]
	#[case::or_false_true(
		binary_expr(bool_value(false), BinaryOperator::Or, bool_value(true)),
		bool_value(true)
	)]
	#[case::or_false_false(
		binary_expr(bool_value(false), BinaryOperator::Or, bool_value(false)),
		bool_value(false)
	)]
	#[case::or_short_circuit_left(
		binary_expr(bool_value(true), BinaryOperator::Or, int_value(42)),
		bool_value(true)
	)]
	#[case::or_passthrough_right(
		binary_expr(bool_value(false), BinaryOperator::Or, int_value(42)),
		int_value(42)
	)]
	// Logical operations - NullCoalescing
	#[case::null_coalescing_null_left(
		binary_expr(Expr::Value(Value::Null), BinaryOperator::NullCoalescing, int_value(42)),
		int_value(42)
	)]
	#[case::null_coalescing_none_left(
		binary_expr(Expr::Value(Value::None), BinaryOperator::NullCoalescing, int_value(42)),
		int_value(42)
	)]
	#[case::null_coalescing_value_left(
		binary_expr(int_value(10), BinaryOperator::NullCoalescing, int_value(42)),
		int_value(10)
	)]
	// Prefix operations - Not
	#[case::not_true(prefix_expr(PrefixOperator::Not, bool_value(true)), bool_value(false))]
	#[case::not_false(prefix_expr(PrefixOperator::Not, bool_value(false)), bool_value(true))]
	// Prefix operations - Negate
	#[case::negate_positive(
		prefix_expr(PrefixOperator::Negate, int_value(5)),
		int_value(-5)
	)]
	#[case::negate_negative(
		prefix_expr(PrefixOperator::Negate, int_value(-5)),
		int_value(5)
	)]
	#[case::negate_zero(prefix_expr(PrefixOperator::Negate, int_value(0)), int_value(0))]
	#[case::negate_float(
		prefix_expr(PrefixOperator::Negate, float_value(std::f64::consts::PI)),
		float_value(-std::f64::consts::PI)
	)]
	// Prefix operations - Positive
	#[case::positive_integer(prefix_expr(PrefixOperator::Positive, int_value(5)), int_value(5))]
	#[case::positive_negative(
		prefix_expr(PrefixOperator::Positive, int_value(-5)),
		int_value(-5)
	)]
	// Nested operations
	#[case::nested_arithmetic(
		binary_expr(
			binary_expr(int_value(2), BinaryOperator::Add, int_value(3)),
			BinaryOperator::Multiply,
			int_value(4)
		),
		int_value(20)
	)]
	#[case::nested_comparison(
		binary_expr(
			binary_expr(int_value(5), BinaryOperator::Add, int_value(3)),
			BinaryOperator::Equal,
			int_value(8)
		),
		bool_value(true)
	)]
	#[case::nested_logical(
		binary_expr(
			binary_expr(int_value(5), BinaryOperator::MoreThan, int_value(3)),
			BinaryOperator::And,
			binary_expr(int_value(2), BinaryOperator::LessThan, int_value(10))
		),
		bool_value(true)
	)]
	#[case::nested_prefix_and_binary(
		binary_expr(
			prefix_expr(PrefixOperator::Negate, int_value(5)),
			BinaryOperator::Add,
			int_value(10)
		),
		int_value(5)
	)]
	// Mixed constant and value operations
	#[case::constant_in_binary(
		binary_expr(
			Expr::Constant(Constant::MathPi),
			BinaryOperator::Multiply,
			int_value(2)
		),
		float_value(f64c::PI * 2.0)
	)]
	// Already optimized values - should not change
	#[case::already_value(int_value(42), int_value(42))]
	fn test_constant_folding(#[case] expr: Expr, #[case] expected: Expr) {
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert_eq!(result.data, expected);
	}

	// Test set operations separately due to complexity
	#[test]
	fn test_constant_folding_contain() {
		let array = Expr::Value(Value::Array(crate::val::Array(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3)),
		])));
		let value = int_value(2);
		let expr = binary_expr(array, BinaryOperator::Contain, value);
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert_eq!(result.data, bool_value(true));
	}

	#[test]
	fn test_constant_folding_not_contain() {
		let array = Expr::Value(Value::Array(crate::val::Array(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3)),
		])));
		let value = int_value(5);
		let expr = binary_expr(array, BinaryOperator::NotContain, value);
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert_eq!(result.data, bool_value(true));
	}

	#[test]
	fn test_constant_folding_inside() {
		let value = int_value(2);
		let array = Expr::Value(Value::Array(crate::val::Array(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3)),
		])));
		let expr = binary_expr(value, BinaryOperator::Inside, array);
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert_eq!(result.data, bool_value(true));
	}

	// Test that range operations are NOT folded
	#[test]
	fn test_constant_folding_range_not_folded() {
		let expr = binary_expr(int_value(1), BinaryOperator::Range, int_value(10));
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		// Should still be a Binary expression, not folded to a value
		match result.data {
			Expr::Binary {
				..
			} => {} // Expected
			_ => panic!("Range operations should not be folded"),
		}
	}

	#[test]
	fn test_constant_folding_prefix_range_not_folded() {
		let expr = prefix_expr(PrefixOperator::Range, int_value(10));
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		// Should still be a Prefix expression, not folded
		match result.data {
			Expr::Prefix {
				..
			} => {} // Expected
			_ => panic!("Prefix range operations should not be folded"),
		}
	}

	// Test ExactEqual (strict equality)
	#[test]
	fn test_constant_folding_exact_equal() {
		let expr = binary_expr(int_value(5), BinaryOperator::ExactEqual, int_value(5));
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert_eq!(result.data, bool_value(true));
	}

	// Test AllEqual
	#[test]
	fn test_constant_folding_all_equal() {
		let array =
			Expr::Value(Value::Array(crate::val::Array(vec![Value::Number(Number::Int(5))])));
		let value = int_value(5);
		let expr = binary_expr(array, BinaryOperator::AllEqual, value);
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert_eq!(result.data, bool_value(true));
	}

	// Test AnyEqual
	#[test]
	fn test_constant_folding_any_equal() {
		let array = Expr::Value(Value::Array(crate::val::Array(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(5)),
			Value::Number(Number::Int(10)),
		])));
		let value = int_value(5);
		let expr = binary_expr(array, BinaryOperator::AnyEqual, value);
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert_eq!(result.data, bool_value(true));
	}
}
