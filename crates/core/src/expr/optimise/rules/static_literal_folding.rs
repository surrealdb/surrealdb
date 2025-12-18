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
	use std::collections::BTreeMap;
	use std::str::FromStr;

	use rstest::rstest;
	use rust_decimal::Decimal;

	use super::*;
	use crate::expr::ObjectEntry;
	use crate::val::{
		Array, Bytes, Datetime, Duration, File, Geometry, Number, Object, Range, Regex, TableName,
		Uuid, Value,
	};

	#[rstest]
	// Simple static literals
	#[case::none(Expr::Literal(Literal::None), Transformed::yes(Expr::Value(Value::None)))]
	#[case::null(Expr::Literal(Literal::Null), Transformed::yes(Expr::Value(Value::Null)))]
	#[case::unbounded_range(
		Expr::Literal(Literal::UnboundedRange),
		Transformed::yes(Expr::Value(Value::Range(Box::new(Range::unbounded()))))
	)]
	#[case::bool_true(
		Expr::Literal(Literal::Bool(true)),
		Transformed::yes(Expr::Value(Value::Bool(true)))
	)]
	#[case::bool_false(
		Expr::Literal(Literal::Bool(false)),
		Transformed::yes(Expr::Value(Value::Bool(false)))
	)]
	#[case::integer(
		Expr::Literal(Literal::Integer(42)),
		Transformed::yes(Expr::Value(Value::Number(Number::Int(42))))
	)]
	#[case::integer_negative(
		Expr::Literal(Literal::Integer(-123)),
		Transformed::yes(Expr::Value(Value::Number(Number::Int(-123))))
	)]
	#[case::integer_zero(
		Expr::Literal(Literal::Integer(0)),
		Transformed::yes(Expr::Value(Value::Number(Number::Int(0))))
	)]
	#[case::float(
		Expr::Literal(Literal::Float(std::f64::consts::PI)),
		Transformed::yes(Expr::Value(Value::Number(Number::Float(std::f64::consts::PI))))
	)]
	#[case::float_negative(
		Expr::Literal(Literal::Float(-2.5)),
		Transformed::yes(Expr::Value(Value::Number(Number::Float(-2.5))))
	)]
	#[case::float_zero(
		Expr::Literal(Literal::Float(0.0)),
		Transformed::yes(Expr::Value(Value::Number(Number::Float(0.0))))
	)]
	#[case::decimal(
		Expr::Literal(Literal::Decimal(Decimal::new(12345, 2))),
		Transformed::yes(Expr::Value(Value::Number(Number::Decimal(Decimal::new(12345, 2)))))
	)]
	#[case::string_empty(
		Expr::Literal(Literal::String(String::new())),
		Transformed::yes(Expr::Value(Value::String(String::new())))
	)]
	#[case::string(
		Expr::Literal(Literal::String("hello world".to_string())),
		Transformed::yes(Expr::Value(Value::String("hello world".to_string())))
	)]
	#[case::bytes_empty(
		Expr::Literal(Literal::Bytes(Bytes::default())),
		Transformed::yes(Expr::Value(Value::Bytes(Bytes::default())))
	)]
	#[case::bytes(
		Expr::Literal(Literal::Bytes(Bytes::from(vec![1, 2, 3, 4]))),
		Transformed::yes(Expr::Value(Value::Bytes(Bytes::from(vec![1, 2, 3, 4]))))
	)]
	#[case::regex(
		Expr::Literal(Literal::Regex(Regex::from_str(r"^test$").unwrap())),
		Transformed::yes(Expr::Value(Value::Regex(Regex::from_str(r"^test$").unwrap())))
	)]
	#[case::duration(
		Expr::Literal(Literal::Duration(Duration::from_secs(60))),
		Transformed::yes(Expr::Value(Value::Duration(Duration::from_secs(60))))
	)]
	#[case::uuid(
		Expr::Literal(Literal::Uuid(Uuid::nil())),
		Transformed::yes(Expr::Value(Value::Uuid(Uuid::nil())))
	)]
	#[case::geometry(
		Expr::Literal(Literal::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0)))),
		Transformed::yes(Expr::Value(Value::Geometry(Geometry::Point(geo::Point::new(
			1.0, 2.0
		)))))
	)]
	#[case::file(
		Expr::Literal(Literal::File(File::new("bucket".to_string(), "key".to_string()))),
		Transformed::yes(Expr::Value(Value::File(File::new("bucket".to_string(), "key".to_string()))))
	)]
	// Empty collections
	#[case::array_empty(
		Expr::Literal(Literal::Array(vec![])),
		Transformed::yes(Expr::Value(Value::Array(Array(vec![]))))
	)]
	#[case::set_empty(
		Expr::Literal(Literal::Set(vec![])),
		Transformed::yes(Expr::Value(Value::Set(crate::val::Set::new())))
	)]
	#[case::object_empty(
		Expr::Literal(Literal::Object(vec![])),
		Transformed::yes(Expr::Value(Value::Object(Object(BTreeMap::new()))))
	)]
	// Arrays with static values
	#[case::array_with_integers(
		Expr::Literal(Literal::Array(vec![
			Expr::Literal(Literal::Integer(1)),
			Expr::Literal(Literal::Integer(2)),
			Expr::Literal(Literal::Integer(3))
		])),
		Transformed::yes(Expr::Value(Value::Array(Array(vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3))
		]))))
	)]
	#[case::array_mixed_types(
		Expr::Literal(Literal::Array(vec![
			Expr::Literal(Literal::Integer(42)),
			Expr::Literal(Literal::String("test".to_string())),
			Expr::Literal(Literal::Bool(true))
		])),
		Transformed::yes(Expr::Value(Value::Array(Array(vec![
			Value::Number(Number::Int(42)),
			Value::String("test".to_string()),
			Value::Bool(true)
		]))))
	)]
	// Nested arrays
	#[case::array_nested(
		Expr::Literal(Literal::Array(vec![
			Expr::Literal(Literal::Array(vec![
				Expr::Literal(Literal::Integer(1)),
				Expr::Literal(Literal::Integer(2))
			])),
			Expr::Literal(Literal::Array(vec![
				Expr::Literal(Literal::Integer(3)),
				Expr::Literal(Literal::Integer(4))
			]))
		])),
		Transformed::yes(Expr::Value(Value::Array(Array(vec![
			Value::Array(Array(vec![
				Value::Number(Number::Int(1)),
				Value::Number(Number::Int(2))
			])),
			Value::Array(Array(vec![
				Value::Number(Number::Int(3)),
				Value::Number(Number::Int(4))
			]))
		]))))
	)]
	// Sets with static values
	#[case::set_with_integers(
		Expr::Literal(Literal::Set(vec![
			Expr::Literal(Literal::Integer(1)),
			Expr::Literal(Literal::Integer(2)),
			Expr::Literal(Literal::Integer(3))
		])),
		Transformed::yes(Expr::Value(Value::Set({
			let mut s = crate::val::Set::new();
			s.insert(Value::Number(Number::Int(1)));
			s.insert(Value::Number(Number::Int(2)));
			s.insert(Value::Number(Number::Int(3)));
			s
		})))
	)]
	// Objects with static values
	#[case::object_single_field(
		Expr::Literal(Literal::Object(vec![
			ObjectEntry {
				key: "name".to_string(),
				value: Expr::Literal(Literal::String("Alice".to_string()))
			}
		])),
		Transformed::yes(Expr::Value(Value::Object(Object({
			let mut m = BTreeMap::new();
			m.insert("name".to_string(), Value::String("Alice".to_string()));
			m
		}))))
	)]
	#[case::object_multiple_fields(
		Expr::Literal(Literal::Object(vec![
			ObjectEntry {
				key: "id".to_string(),
				value: Expr::Literal(Literal::Integer(42))
			},
			ObjectEntry {
				key: "name".to_string(),
				value: Expr::Literal(Literal::String("Bob".to_string()))
			},
			ObjectEntry {
				key: "active".to_string(),
				value: Expr::Literal(Literal::Bool(true))
			}
		])),
		Transformed::yes(Expr::Value(Value::Object(Object({
			let mut m = BTreeMap::new();
			m.insert("id".to_string(), Value::Number(Number::Int(42)));
			m.insert("name".to_string(), Value::String("Bob".to_string()));
			m.insert("active".to_string(), Value::Bool(true));
			m
		}))))
	)]
	// Nested objects
	#[case::object_nested(
		Expr::Literal(Literal::Object(vec![
			ObjectEntry {
				key: "user".to_string(),
				value: Expr::Literal(Literal::Object(vec![
					ObjectEntry {
						key: "name".to_string(),
						value: Expr::Literal(Literal::String("Charlie".to_string()))
					}
				]))
			}
		])),
		Transformed::yes(Expr::Value(Value::Object(Object({
			let mut outer = BTreeMap::new();
			let mut inner = BTreeMap::new();
			inner.insert("name".to_string(), Value::String("Charlie".to_string()));
			outer.insert("user".to_string(), Value::Object(Object(inner)));
			outer
		}))))
	)]
	// Already a value - should not be transformed
	#[case::value_unchanged(
		Expr::Value(Value::Number(Number::Int(42))),
		Transformed::no(Expr::Value(Value::Number(Number::Int(42))))
	)]
	fn test_static_literal_folding(#[case] expr: Expr, #[case] expected: Transformed<Expr>) {
		let result = StaticLiteralFolding.optimise_expr(expr).unwrap();
		assert_eq!(result, expected);
	}

	// Test datetime separately due to value equality requirements
	#[test]
	fn test_static_literal_folding_datetime() {
		let dt = Datetime::now();
		let expr = Expr::Literal(Literal::Datetime(dt.clone()));
		let result = StaticLiteralFolding.optimise_expr(expr).unwrap();
		let expected = Transformed::yes(Expr::Value(Value::Datetime(dt)));
		assert_eq!(result, expected);
	}

	// Test RecordId separately as it requires more complex setup
	#[test]
	fn test_static_literal_folding_record_id() {
		use crate::expr::{RecordIdKeyLit, RecordIdLit};
		use crate::val::{RecordId, RecordIdKey};

		// RecordId with string key
		let expr = Expr::Literal(Literal::RecordId(RecordIdLit {
			table: TableName::new("users".to_string()),
			key: RecordIdKeyLit::String("john".to_string()),
		}));

		let result = StaticLiteralFolding.optimise_expr(expr).unwrap();

		let expected_record_id = RecordId {
			table: TableName::new("users".to_string()),
			key: RecordIdKey::String("john".to_string()),
		};
		let expected = Transformed::yes(Expr::Value(Value::RecordId(expected_record_id)));

		assert_eq!(result, expected);
	}

	#[test]
	fn test_static_literal_folding_record_id_number() {
		use crate::expr::{RecordIdKeyLit, RecordIdLit};
		use crate::val::{RecordId, RecordIdKey};

		// RecordId with number key
		let expr = Expr::Literal(Literal::RecordId(RecordIdLit {
			table: TableName::new("items".to_string()),
			key: RecordIdKeyLit::Number(123),
		}));

		let result = StaticLiteralFolding.optimise_expr(expr).unwrap();

		let expected_record_id = RecordId {
			table: TableName::new("items".to_string()),
			key: RecordIdKey::Number(123),
		};
		let expected = Transformed::yes(Expr::Value(Value::RecordId(expected_record_id)));

		assert_eq!(result, expected);
	}

	#[test]
	fn test_static_literal_folding_record_id_uuid() {
		use crate::expr::{RecordIdKeyLit, RecordIdLit};
		use crate::val::{RecordId, RecordIdKey};

		// RecordId with UUID key
		let uuid = Uuid::nil();
		let expr = Expr::Literal(Literal::RecordId(RecordIdLit {
			table: TableName::new("entities".to_string()),
			key: RecordIdKeyLit::Uuid(uuid),
		}));

		let result = StaticLiteralFolding.optimise_expr(expr).unwrap();

		let expected_record_id = RecordId {
			table: TableName::new("entities".to_string()),
			key: RecordIdKey::Uuid(uuid),
		};
		let expected = Transformed::yes(Expr::Value(Value::RecordId(expected_record_id)));

		assert_eq!(result, expected);
	}

	// Test that optimization recurses through composite expressions
	#[test]
	fn test_static_literal_folding_in_binary_expr() {
		use crate::expr::BinaryOperator;

		let expr = Expr::Binary {
			left: Box::new(Expr::Literal(Literal::Integer(5))),
			op: BinaryOperator::Add,
			right: Box::new(Expr::Literal(Literal::Integer(3))),
		};

		let result = StaticLiteralFolding.optimise_expr(expr).unwrap();

		// Both literals should be converted to values
		if let Expr::Binary {
			left,
			right,
			..
		} = result.data
		{
			assert!(matches!(*left, Expr::Value(Value::Number(Number::Int(5)))));
			assert!(matches!(*right, Expr::Value(Value::Number(Number::Int(3)))));
		} else {
			panic!("Expected Binary expression");
		}

		assert!(result.transformed);
	}

	#[test]
	fn test_static_literal_folding_in_function_call() {
		use crate::expr::{Function, FunctionCall};

		let expr = Expr::FunctionCall(Box::new(FunctionCall {
			receiver: Function::Normal("test".to_string()),
			arguments: vec![
				Expr::Literal(Literal::String("arg1".to_string())),
				Expr::Literal(Literal::Integer(42)),
			],
		}));

		let result = StaticLiteralFolding.optimise_expr(expr).unwrap();

		// Arguments should be converted to values
		if let Expr::FunctionCall(call) = result.data {
			assert!(matches!(call.arguments[0], Expr::Value(Value::String(_))));
			assert!(matches!(call.arguments[1], Expr::Value(Value::Number(Number::Int(42)))));
		} else {
			panic!("Expected FunctionCall expression");
		}

		assert!(result.transformed);
	}
}
