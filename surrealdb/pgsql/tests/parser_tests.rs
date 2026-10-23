//! Unit and integration tests for PostgreSQL Pratt expression parser.

use surrealdb_pgsql::ast::expr::Expr;
use surrealdb_pgsql::ast::literal::Literal;
use surrealdb_pgsql::ast::op::{BinaryOperator, UnaryOperator};
use surrealdb_pgsql::parse_expr;

#[test]
fn test_parse_literals() {
	let expr = parse_expr("42").expect("parse integer");
	match expr {
		Expr::Literal {
			lit: Literal::Integer(42),
			..
		} => {}
		other => panic!("expected integer 42, got {:?}", other),
	}

	let expr = parse_expr("'hello'").expect("parse string");
	match expr {
		Expr::Literal {
			lit: Literal::String(ref s),
			..
		} if s == "hello" => {}
		other => panic!("expected string 'hello', got {:?}", other),
	}

	let expr = parse_expr("TRUE").expect("parse boolean");
	match expr {
		Expr::Literal {
			lit: Literal::Boolean(true),
			..
		} => {}
		other => panic!("expected true, got {:?}", other),
	}
}

#[test]
fn test_parse_arithmetic_precedence() {
	// `1 + 2 * 3` must parse as `1 + (2 * 3)`
	let expr = parse_expr("1 + 2 * 3").expect("parse precedence");
	match expr {
		Expr::BinaryOp {
			left,
			op: BinaryOperator::Add,
			right,
			..
		} => {
			assert!(matches!(
				*left,
				Expr::Literal {
					lit: Literal::Integer(1),
					..
				}
			));
			match *right {
				Expr::BinaryOp {
					left: r_left,
					op: BinaryOperator::Mul,
					right: r_right,
					..
				} => {
					assert!(matches!(
						*r_left,
						Expr::Literal {
							lit: Literal::Integer(2),
							..
						}
					));
					assert!(matches!(
						*r_right,
						Expr::Literal {
							lit: Literal::Integer(3),
							..
						}
					));
				}
				other => panic!("expected binary mul, got {:?}", other),
			}
		}
		other => panic!("expected binary add, got {:?}", other),
	}
}

#[test]
fn test_parse_exponent_left_associativity() {
	// `2 ^ 3 ^ 4` must parse as `(2 ^ 3) ^ 4` because gram.y declares `%left '^'`
	let expr = parse_expr("2 ^ 3 ^ 4").expect("parse exponent");
	match expr {
		Expr::BinaryOp {
			left,
			op: BinaryOperator::Exp,
			right,
			..
		} => {
			assert!(matches!(
				*right,
				Expr::Literal {
					lit: Literal::Integer(4),
					..
				}
			));
			match *left {
				Expr::BinaryOp {
					left: l_left,
					op: BinaryOperator::Exp,
					right: l_right,
					..
				} => {
					assert!(matches!(
						*l_left,
						Expr::Literal {
							lit: Literal::Integer(2),
							..
						}
					));
					assert!(matches!(
						*l_right,
						Expr::Literal {
							lit: Literal::Integer(3),
							..
						}
					));
				}
				other => panic!("expected left-nested exp, got {:?}", other),
			}
		}
		other => panic!("expected exp, got {:?}", other),
	}
}

#[test]
fn test_parse_typecast() {
	let expr = parse_expr("x::INTEGER").expect("parse typecast");
	match expr {
		Expr::TypeCast {
			expr,
			data_type,
			..
		} => {
			assert_eq!(data_type.name.parts, vec!["integer"]);
			assert!(matches!(*expr, Expr::ColumnRef(_)));
		}
		other => panic!("expected typecast, got {:?}", other),
	}

	let expr = parse_expr("CAST(123 AS VARCHAR(255))").expect("parse cast func");
	match expr {
		Expr::TypeCast {
			expr,
			data_type,
			..
		} => {
			assert_eq!(data_type.name.parts, vec!["varchar"]);
			assert_eq!(data_type.modifiers, vec!["255"]);
			assert!(matches!(
				*expr,
				Expr::Literal {
					lit: Literal::Integer(123),
					..
				}
			));
		}
		other => panic!("expected cast, got {:?}", other),
	}
}

#[test]
fn test_parse_predicates() {
	// IS NULL
	let expr = parse_expr("x IS NULL").expect("parse is null");
	assert!(matches!(
		expr,
		Expr::IsNull {
			negated: false,
			..
		}
	));

	// IS NOT NULL
	let expr = parse_expr("x IS NOT NULL").expect("parse is not null");
	assert!(matches!(
		expr,
		Expr::IsNull {
			negated: true,
			..
		}
	));

	// BETWEEN a AND b
	let expr = parse_expr("x BETWEEN 1 AND 10").expect("parse between");
	assert!(matches!(
		expr,
		Expr::Between {
			negated: false,
			symmetric: false,
			..
		}
	));

	// IN (1, 2, 3)
	let expr = parse_expr("x IN (1, 2, 3)").expect("parse in list");
	match expr {
		Expr::InList {
			list,
			negated,
			..
		} => {
			assert!(!negated);
			assert_eq!(list.len(), 3);
		}
		other => panic!("expected in list, got {:?}", other),
	}
}

#[test]
fn test_parse_subscript_and_slice() {
	let expr = parse_expr("arr[1]").expect("parse subscript");
	assert!(matches!(expr, Expr::Subscript { .. }));

	let expr = parse_expr("arr[1:5]").expect("parse slice");
	assert!(matches!(expr, Expr::Slice { .. }));
}

#[test]
fn test_parse_function_calls() {
	let expr = parse_expr("count(*)").expect("parse count(*)");
	match expr {
		Expr::FunctionCall {
			name,
			args,
			..
		} => {
			assert_eq!(name.parts, vec!["count"]);
			assert_eq!(args.len(), 1);
		}
		other => panic!("expected func call, got {:?}", other),
	}

	let expr = parse_expr("substring('hello', 1, 2)").expect("parse substring");
	match expr {
		Expr::FunctionCall {
			name,
			args,
			..
		} => {
			assert_eq!(name.parts, vec!["substring"]);
			assert_eq!(args.len(), 3);
		}
		other => panic!("expected func call, got {:?}", other),
	}
}

#[test]
fn test_parse_case_expr() {
	let expr = parse_expr("CASE WHEN x > 0 THEN 'pos' ELSE 'non-pos' END").expect("parse case");
	match expr {
		Expr::Case {
			branches,
			else_expr,
			operand,
			..
		} => {
			assert!(operand.is_none());
			assert_eq!(branches.len(), 1);
			assert!(else_expr.is_some());
		}
		other => panic!("expected case, got {:?}", other),
	}
}

#[test]
fn test_parse_unary_not() {
	let expr = parse_expr("NOT x AND y").expect("parse not");
	match expr {
		Expr::BinaryOp {
			left,
			op: BinaryOperator::And,
			..
		} => {
			assert!(matches!(
				*left,
				Expr::UnaryOp {
					op: UnaryOperator::Not,
					..
				}
			));
		}
		other => panic!("expected binary and with unary not, got {:?}", other),
	}
}

#[test]
fn test_parse_subqueries() {
	// Scalar subquery
	let expr = parse_expr("(SELECT 1)").unwrap();
	assert!(matches!(expr, Expr::Subquery { .. }));

	// EXISTS
	let expr = parse_expr("EXISTS (SELECT 1 FROM users)").unwrap();
	assert!(matches!(expr, Expr::Exists { .. }));

	// IN subquery
	let expr = parse_expr("id IN (SELECT user_id FROM orders)").unwrap();
	match expr {
		Expr::InSubquery {
			negated,
			..
		} => assert!(!negated),
		other => panic!("expected InSubquery, got {other:?}"),
	}

	// NOT IN subquery
	let expr = parse_expr("id NOT IN (SELECT user_id FROM orders)").unwrap();
	match expr {
		Expr::InSubquery {
			negated,
			..
		} => assert!(negated),
		other => panic!("expected InSubquery negated, got {other:?}"),
	}

	// ARRAY subquery
	let expr = parse_expr("ARRAY(SELECT id FROM users)").unwrap();
	assert!(matches!(expr, Expr::ArraySubquery { .. }));

	// Quantified ANY / ALL
	let expr = parse_expr("price > ALL (SELECT price FROM items)").unwrap();
	match expr {
		Expr::Quantified {
			quantifier,
			..
		} => {
			assert_eq!(quantifier, surrealdb_pgsql::Quantifier::All);
		}
		other => panic!("expected Quantified ALL, got {other:?}"),
	}

	let expr = parse_expr("score = ANY (SELECT score FROM benchmarks)").unwrap();
	match expr {
		Expr::Quantified {
			quantifier,
			..
		} => {
			assert_eq!(quantifier, surrealdb_pgsql::Quantifier::Any);
		}
		other => panic!("expected Quantified ANY, got {other:?}"),
	}
}

#[test]
fn test_a_based_integer_literal_that_does_not_fit_64_bits_is_refused() {
	let beyond_a_double = format!("0x{}", "F".repeat(300));
	for literal in [
		"0x8000000000000000",
		"0xFFFFFFFFFFFFFFFFF",
		"0o2000000000000000000000",
		"0b1111111111111111111111111111111111111111111111111111111111111111",
		"0b1_0000000000000000000000000000000000000000000000000000000000000000",
		&beyond_a_double,
	] {
		let err = parse_expr(literal).expect_err(literal);
		assert!(err.to_string().contains("overflow"), "{literal}: {err}");
	}
	for (literal, value) in [
		("9223372036854775807", i64::MAX),
		("0x7FFFFFFFFFFFFFFF", i64::MAX),
		("0o777777777777777777777", i64::MAX),
		("0b0111111111111111111111111111111111111111111111111111111111111111", i64::MAX),
	] {
		match parse_expr(literal).expect(literal) {
			Expr::Literal {
				lit: Literal::Integer(parsed),
				..
			} => assert_eq!(parsed, value, "{literal}"),
			other => panic!("{literal}: expected an integer, got {other:?}"),
		}
	}
}

#[test]
fn test_a_decimal_integer_literal_that_does_not_fit_64_bits_is_a_float() {
	for literal in [
		"9223372036854775808",
		"18446744073709551616",
		"10000000000000000000",
		"123456789012345678901234567890",
		"10_000_000_000_000_000_000",
		"000000000000000000009223372036854775808",
	] {
		let expected = literal.replace('_', "").parse::<f64>().expect(literal);
		match parse_expr(literal).expect(literal) {
			Expr::Literal {
				lit: Literal::Float(parsed),
				..
			} => assert_eq!(parsed, expected, "{literal}"),
			other => panic!("{literal}: expected a float, got {other:?}"),
		}
	}
	let beyond_a_double = "9".repeat(400);
	let err = parse_expr(&beyond_a_double).expect_err("a literal beyond a double has no value");
	assert!(err.to_string().contains("overflow"), "{err}");
}

#[test]
fn test_a_minus_applied_to_the_magnitude_of_the_int8_minimum_is_that_integer() {
	for sql in [
		"-9223372036854775808",
		"- 9223372036854775808",
		"-(9223372036854775808)",
		"-  ( ( 9223372036854775808 ) )",
		"-9_223_372_036_854_775_808",
		"-09223372036854775808",
	] {
		match parse_expr(sql).expect(sql) {
			Expr::Literal {
				lit: Literal::Integer(parsed),
				..
			} => assert_eq!(parsed, i64::MIN, "{sql}"),
			other => panic!("{sql}: expected the int8 minimum, got {other:?}"),
		}
	}
}

#[test]
fn test_a_minus_applied_to_anything_else_is_not_folded_into_the_int8_minimum() {
	for sql in [
		"-9223372036854775809",
		"-9223372036854775808.0",
		"-9223372036854775808e0",
		"-9223372036854775808::int8",
		"-9223372036854775807",
		"+9223372036854775808",
		"-0x8000000000000000",
	] {
		match parse_expr(sql) {
			Ok(Expr::UnaryOp {
				expr,
				..
			}) => assert!(
				!matches!(
					*expr,
					Expr::Literal {
						lit: Literal::Integer(i64::MIN),
						..
					}
				),
				"{sql}: {expr:?}"
			),
			Ok(other) => panic!("{sql}: expected a negation, got {other:?}"),
			Err(err) => assert!(sql.starts_with("-0x") && err.to_string().contains("overflow")),
		}
	}
	match parse_expr("- -9223372036854775808").expect("a negated int8 minimum") {
		Expr::UnaryOp {
			op: UnaryOperator::Minus,
			expr,
			..
		} => assert!(matches!(
			*expr,
			Expr::Literal {
				lit: Literal::Integer(i64::MIN),
				..
			}
		)),
		other => panic!("expected a negation of the int8 minimum, got {other:?}"),
	}
}

#[test]
fn test_the_int8_minimum_is_the_operand_of_a_comparison_and_of_arithmetic() {
	for (sql, operator) in [
		("x = -9223372036854775808", BinaryOperator::Eq),
		("x=-9223372036854775808", BinaryOperator::Eq),
		("x > -9223372036854775808", BinaryOperator::Gt),
		("x>-9223372036854775808", BinaryOperator::Gt),
		("x >= -9223372036854775808", BinaryOperator::GtEq),
		("x < -9223372036854775808", BinaryOperator::Lt),
		("x - -9223372036854775808", BinaryOperator::Sub),
		("x * -9223372036854775808", BinaryOperator::Mul),
	] {
		match parse_expr(sql).expect(sql) {
			Expr::BinaryOp {
				op,
				right,
				..
			} => {
				assert_eq!(op, operator, "{sql}");
				assert!(
					matches!(
						*right,
						Expr::Literal {
							lit: Literal::Integer(i64::MIN),
							..
						}
					),
					"{sql}: {right:?}"
				);
			}
			other => panic!("{sql}: expected a binary operator, got {other:?}"),
		}
	}
	match parse_expr("-9223372036854775808 / -1").expect("a quotient") {
		Expr::BinaryOp {
			left,
			op: BinaryOperator::Div,
			..
		} => assert!(matches!(
			*left,
			Expr::Literal {
				lit: Literal::Integer(i64::MIN),
				..
			}
		)),
		other => panic!("expected a quotient, got {other:?}"),
	}
}

#[test]
fn test_a_json_operator_followed_by_a_negative_integer_is_one_operator_and_a_negation() {
	for (sql, operator) in [
		("data->>-1", BinaryOperator::LongArrow),
		("data->-1", BinaryOperator::Arrow),
		("data ->> -1", BinaryOperator::LongArrow),
	] {
		match parse_expr(sql).expect(sql) {
			Expr::BinaryOp {
				op,
				right,
				..
			} => {
				assert_eq!(op, operator, "{sql}");
				assert!(
					matches!(
						*right,
						Expr::UnaryOp {
							op: UnaryOperator::Minus,
							..
						} | Expr::Literal {
							lit: Literal::Integer(-1),
							..
						}
					),
					"{sql}: {right:?}"
				);
			}
			other => panic!("{sql}: expected a binary operator, got {other:?}"),
		}
	}
}
