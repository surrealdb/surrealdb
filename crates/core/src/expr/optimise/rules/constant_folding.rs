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
	use super::*;
	use crate::val::Number;

	fn int_value(n: i64) -> Expr {
		Expr::Value(Value::Number(Number::Int(n)))
	}

	fn bool_value(b: bool) -> Expr {
		Expr::Value(Value::Bool(b))
	}

	#[test]
	fn test_add_integers() {
		let expr = Expr::Binary {
			left: Box::new(int_value(1)),
			op: BinaryOperator::Add,
			right: Box::new(int_value(2)),
		};
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert!(result.transformed);
		match result.data {
			Expr::Value(Value::Number(Number::Int(3))) => (),
			other => panic!("Expected Value(3), got {:?}", other),
		}
	}

	#[test]
	fn test_multiply_integers() {
		let expr = Expr::Binary {
			left: Box::new(int_value(10)),
			op: BinaryOperator::Multiply,
			right: Box::new(int_value(5)),
		};
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert!(result.transformed);
		match result.data {
			Expr::Value(Value::Number(Number::Int(50))) => (),
			other => panic!("Expected Value(50), got {:?}", other),
		}
	}

	#[test]
	fn test_nested_arithmetic() {
		// (1 + 2) * 3
		let inner = Expr::Binary {
			left: Box::new(int_value(1)),
			op: BinaryOperator::Add,
			right: Box::new(int_value(2)),
		};
		let expr = Expr::Binary {
			left: Box::new(inner),
			op: BinaryOperator::Multiply,
			right: Box::new(int_value(3)),
		};
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert!(result.transformed);
		match result.data {
			Expr::Value(Value::Number(Number::Int(9))) => (),
			other => panic!("Expected Value(9), got {:?}", other),
		}
	}

	#[test]
	fn test_boolean_and() {
		let expr = Expr::Binary {
			left: Box::new(bool_value(true)),
			op: BinaryOperator::And,
			right: Box::new(bool_value(false)),
		};
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert!(result.transformed);
		match result.data {
			Expr::Value(Value::Bool(false)) => (),
			other => panic!("Expected Value(false), got {:?}", other),
		}
	}

	#[test]
	fn test_comparison() {
		let expr = Expr::Binary {
			left: Box::new(int_value(5)),
			op: BinaryOperator::MoreThan,
			right: Box::new(int_value(3)),
		};
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert!(result.transformed);
		match result.data {
			Expr::Value(Value::Bool(true)) => (),
			other => panic!("Expected Value(true), got {:?}", other),
		}
	}

	#[test]
	fn test_negation() {
		let expr = Expr::Prefix {
			op: PrefixOperator::Negate,
			expr: Box::new(int_value(42)),
		};
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert!(result.transformed);
		match result.data {
			Expr::Value(Value::Number(Number::Int(-42))) => (),
			other => panic!("Expected Value(-42), got {:?}", other),
		}
	}

	#[test]
	fn test_not_boolean() {
		let expr = Expr::Prefix {
			op: PrefixOperator::Not,
			expr: Box::new(bool_value(true)),
		};
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert!(result.transformed);
		match result.data {
			Expr::Value(Value::Bool(false)) => (),
			other => panic!("Expected Value(false), got {:?}", other),
		}
	}

	#[test]
	fn test_non_constant_unchanged() {
		let expr = Expr::Binary {
			left: Box::new(Expr::Param(crate::expr::Param::new("x".to_string()))),
			op: BinaryOperator::Add,
			right: Box::new(int_value(2)),
		};
		let result = ConstantFolding.optimise_expr(expr).unwrap();
		assert!(!result.transformed);
	}
}
