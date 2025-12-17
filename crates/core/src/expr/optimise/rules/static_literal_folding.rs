use anyhow::Result;

use crate::expr::optimise::utils::literal_to_value;
use crate::expr::optimise::{OptimiserRule, Transformed};
use crate::expr::{Expr, Literal};

/// Optimization rule that converts static literals to pre-computed values.
///
/// This rule identifies `Expr::Literal` nodes where `literal.is_static()` is true
/// and converts them to `Expr::Value`, eliminating the need to compute them at runtime.
///
/// Examples:
/// - `Literal::Integer(42)` → `Value(Number::Int(42))`
/// - `Literal::Array([1, 2, 3])` → `Value(Array([1, 2, 3]))`
/// - `Literal::Object({a: 5})` → `Value(Object(...))`
///
/// Non-static literals (containing parameters, idioms, etc.) are left unchanged.
pub(crate) struct StaticLiteralFolding;

impl OptimiserRule for StaticLiteralFolding {
	fn name(&self) -> &str {
		"static_literal_folding"
	}

	fn optimise_expr(&self, expr: Expr) -> Result<Transformed<Expr>> {
		optimise_expr_recursive(expr)
	}
}

fn optimise_expr_recursive(expr: Expr) -> Result<Transformed<Expr>> {
	match expr {
		// The main optimization: convert static literals to values
		Expr::Literal(ref literal) if literal.is_static() => {
			// First, recursively optimise children (for arrays/objects)
			let optimised = match expr {
				Expr::Literal(Literal::Array(exprs)) => {
					let (new_exprs, child_transformed) = optimise_expr_vec(exprs)?;
					let new_literal = Literal::Array(new_exprs);
					(Expr::Literal(new_literal), child_transformed)
				}
				Expr::Literal(Literal::Set(exprs)) => {
					let (new_exprs, child_transformed) = optimise_expr_vec(exprs)?;
					let new_literal = Literal::Set(new_exprs);
					(Expr::Literal(new_literal), child_transformed)
				}
				Expr::Literal(Literal::Object(items)) => {
					let (new_items, child_transformed) = optimise_object_entries(items)?;
					let new_literal = Literal::Object(new_items);
					(Expr::Literal(new_literal), child_transformed)
				}
				other => (other, false),
			};

			// Now convert to value
			if let Expr::Literal(ref lit) = optimised.0 {
				match literal_to_value(lit) {
					Ok(value) => Ok(Transformed::yes(Expr::Value(value))),
					Err(_) => {
						// If conversion fails, keep the original
						Ok(Transformed {
							data: optimised.0,
							transformed: optimised.1,
						})
					}
				}
			} else {
				Ok(Transformed {
					data: optimised.0,
					transformed: optimised.1,
				})
			}
		}

		// Recursively optimise composite expressions
		Expr::Prefix {
			op,
			expr,
		} => {
			let result = optimise_expr_recursive(*expr)?;
			Ok(result.map(|e| Expr::Prefix {
				op,
				expr: Box::new(e),
			}))
		}

		Expr::Postfix {
			expr,
			op,
		} => {
			let result = optimise_expr_recursive(*expr)?;
			Ok(result.map(|e| Expr::Postfix {
				expr: Box::new(e),
				op,
			}))
		}

		Expr::Binary {
			left,
			op,
			right,
		} => {
			let left_result = optimise_expr_recursive(*left)?;
			let right_result = optimise_expr_recursive(*right)?;
			let transformed = left_result.transformed || right_result.transformed;
			Ok(Transformed {
				data: Expr::Binary {
					left: Box::new(left_result.data),
					op,
					right: Box::new(right_result.data),
				},
				transformed,
			})
		}

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

		// For statement expressions, optimise their internal expressions
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

		// Leaf nodes and expressions we don't optimise
		Expr::Value(_)
		| Expr::Literal(_)
		| Expr::Param(_)
		| Expr::Idiom(_)
		| Expr::Table(_)
		| Expr::Mock(_)
		| Expr::Constant(_)
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

fn optimise_object_entries(
	items: Vec<crate::expr::ObjectEntry>,
) -> Result<(Vec<crate::expr::ObjectEntry>, bool)> {
	let mut transformed = false;
	let mut new_items = Vec::with_capacity(items.len());
	for item in items {
		let result = optimise_expr_recursive(item.value)?;
		transformed = transformed || result.transformed;
		new_items.push(crate::expr::ObjectEntry {
			key: item.key,
			value: result.data,
		});
	}
	Ok((new_items, transformed))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::Number;

	#[test]
	fn test_static_integer_literal() {
		let expr = Expr::Literal(Literal::Integer(42));
		let result = StaticLiteralFolding.optimise_expr(expr).unwrap();
		assert!(result.transformed);
		match result.data {
			Expr::Value(v) => assert_eq!(v, crate::val::Value::Number(Number::Int(42))),
			_ => panic!("Expected Value"),
		}
	}

	#[test]
	fn test_static_string_literal() {
		let expr = Expr::Literal(Literal::String("hello".to_string()));
		let result = StaticLiteralFolding.optimise_expr(expr).unwrap();
		assert!(result.transformed);
		match result.data {
			Expr::Value(v) => assert_eq!(v, crate::val::Value::String("hello".to_string())),
			_ => panic!("Expected Value"),
		}
	}

	#[test]
	fn test_static_bool_literal() {
		let expr = Expr::Literal(Literal::Bool(true));
		let result = StaticLiteralFolding.optimise_expr(expr).unwrap();
		assert!(result.transformed);
		match result.data {
			Expr::Value(v) => assert_eq!(v, crate::val::Value::Bool(true)),
			_ => panic!("Expected Value"),
		}
	}

	#[test]
	fn test_param_not_optimised() {
		let expr = Expr::Param(crate::expr::Param::new("test".to_string()));
		let result = StaticLiteralFolding.optimise_expr(expr).unwrap();
		assert!(!result.transformed);
	}
}
