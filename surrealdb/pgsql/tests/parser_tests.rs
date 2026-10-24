//! Unit and integration tests for PostgreSQL Pratt expression parser.

use surrealdb_pgsql::ast::expr::Expr;
use surrealdb_pgsql::ast::literal::Literal;
use surrealdb_pgsql::ast::op::{BinaryOperator, UnaryOperator};
use surrealdb_pgsql::parse_expr;

#[test]
fn test_parse_literals() {
	let expr = parse_expr("42").expect("parse integer");
	let Expr::Literal {
		lit: Literal::Integer(42),
		..
	} = expr
	else {
		panic!("expected integer 42, got {:?}", expr);
	};

	let expr = parse_expr("'hello'").expect("parse string");
	let Expr::Literal {
		lit: Literal::String(ref s),
		..
	} = expr
	else {
		panic!("expected string 'hello', got {:?}", expr);
	};
	assert_eq!(s, "hello");

	let expr = parse_expr("TRUE").expect("parse boolean");
	let Expr::Literal {
		lit: Literal::Boolean(true),
		..
	} = expr
	else {
		panic!("expected true, got {:?}", expr);
	};
}

#[test]
fn test_parse_arithmetic_precedence() {
	// `1 + 2 * 3` must parse as `1 + (2 * 3)`
	let expr = parse_expr("1 + 2 * 3").expect("parse precedence");
	let Expr::BinaryOp {
		left,
		op: BinaryOperator::Add,
		right,
		..
	} = expr
	else {
		panic!("expected binary add, got {:?}", expr);
	};
	assert!(matches!(
		*left,
		Expr::Literal {
			lit: Literal::Integer(1),
			..
		}
	));
	let Expr::BinaryOp {
		left: r_left,
		op: BinaryOperator::Mul,
		right: r_right,
		..
	} = *right
	else {
		panic!("expected binary mul, got {:?}", right);
	};
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

#[test]
fn test_parse_exponent_left_associativity() {
	// `2 ^ 3 ^ 4` must parse as `(2 ^ 3) ^ 4` because gram.y declares `%left '^'`
	let expr = parse_expr("2 ^ 3 ^ 4").expect("parse exponent");
	let Expr::BinaryOp {
		left,
		op: BinaryOperator::Exp,
		right,
		..
	} = expr
	else {
		panic!("expected exp, got {:?}", expr);
	};
	assert!(matches!(
		*right,
		Expr::Literal {
			lit: Literal::Integer(4),
			..
		}
	));
	let Expr::BinaryOp {
		left: l_left,
		op: BinaryOperator::Exp,
		right: l_right,
		..
	} = *left
	else {
		panic!("expected left-nested exp, got {:?}", left);
	};
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

#[test]
fn test_parse_typecast() {
	let expr = parse_expr("x::INTEGER").expect("parse typecast");
	let Expr::TypeCast {
		expr: inner_expr,
		data_type,
		..
	} = expr
	else {
		panic!("expected typecast, got {:?}", expr);
	};
	assert_eq!(data_type.name.parts, vec!["integer"]);
	assert!(matches!(*inner_expr, Expr::ColumnRef(_)));

	let expr = parse_expr("CAST(123 AS VARCHAR(255))").expect("parse cast func");
	let Expr::TypeCast {
		expr: inner_expr,
		data_type,
		..
	} = expr
	else {
		panic!("expected cast, got {:?}", expr);
	};
	assert_eq!(data_type.name.parts, vec!["varchar"]);
	assert_eq!(data_type.modifiers, vec!["255"]);
	assert!(matches!(
		*inner_expr,
		Expr::Literal {
			lit: Literal::Integer(123),
			..
		}
	));
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
	let Expr::InList {
		list,
		negated,
		..
	} = expr
	else {
		panic!("expected in list, got {:?}", expr);
	};
	assert!(!negated);
	assert_eq!(list.len(), 3);
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
	let Expr::FunctionCall {
		name,
		args,
		..
	} = expr
	else {
		panic!("expected func call, got {:?}", expr);
	};
	assert_eq!(name.parts, vec!["count"]);
	assert_eq!(args.len(), 1);

	let expr = parse_expr("substring('hello', 1, 2)").expect("parse substring");
	let Expr::FunctionCall {
		name,
		args,
		..
	} = expr
	else {
		panic!("expected func call, got {:?}", expr);
	};
	assert_eq!(name.parts, vec!["substring"]);
	assert_eq!(args.len(), 3);
}

#[test]
fn test_parse_case_expr() {
	let expr = parse_expr("CASE WHEN x > 0 THEN 'pos' ELSE 'non-pos' END").expect("parse case");
	let Expr::Case {
		branches,
		else_expr,
		operand,
		..
	} = expr
	else {
		panic!("expected case, got {:?}", expr);
	};
	assert!(operand.is_none());
	assert_eq!(branches.len(), 1);
	assert!(else_expr.is_some());
}

#[test]
fn test_parse_unary_not() {
	let expr = parse_expr("NOT x AND y").expect("parse not");
	let Expr::BinaryOp {
		left,
		op: BinaryOperator::And,
		..
	} = expr
	else {
		panic!("expected binary and with unary not, got {:?}", expr);
	};
	assert!(matches!(
		*left,
		Expr::UnaryOp {
			op: UnaryOperator::Not,
			..
		}
	));
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
	let Expr::InSubquery {
		negated,
		..
	} = expr
	else {
		panic!("expected InSubquery, got {expr:?}");
	};
	assert!(!negated);

	// NOT IN subquery
	let expr = parse_expr("id NOT IN (SELECT user_id FROM orders)").unwrap();
	let Expr::InSubquery {
		negated,
		..
	} = expr
	else {
		panic!("expected InSubquery negated, got {expr:?}");
	};
	assert!(negated);

	// ARRAY subquery
	let expr = parse_expr("ARRAY(SELECT id FROM users)").unwrap();
	assert!(matches!(expr, Expr::ArraySubquery { .. }));

	// Quantified ANY / ALL
	let expr = parse_expr("price > ALL (SELECT price FROM items)").unwrap();
	let Expr::Quantified {
		quantifier,
		..
	} = expr
	else {
		panic!("expected Quantified ALL, got {expr:?}");
	};
	assert_eq!(quantifier, surrealdb_pgsql::Quantifier::All);

	let expr = parse_expr("score = ANY (SELECT score FROM benchmarks)").unwrap();
	let Expr::Quantified {
		quantifier,
		..
	} = expr
	else {
		panic!("expected Quantified ANY, got {expr:?}");
	};
	assert_eq!(quantifier, surrealdb_pgsql::Quantifier::Any);
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
		let expr = parse_expr(literal).expect(literal);
		let Expr::Literal {
			lit: Literal::Integer(parsed),
			..
		} = expr
		else {
			panic!("{literal}: expected an integer, got {expr:?}")
		};
		assert_eq!(parsed, value, "{literal}");
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
		let expr = parse_expr(literal).expect(literal);
		let Expr::Literal {
			lit: Literal::Float(parsed),
			..
		} = expr
		else {
			panic!("{literal}: expected a float, got {expr:?}")
		};
		assert_eq!(parsed, expected, "{literal}");
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
		let expr = parse_expr(sql).expect(sql);
		let Expr::Literal {
			lit: Literal::Integer(parsed),
			..
		} = expr
		else {
			panic!("{sql}: expected the int8 minimum, got {expr:?}")
		};
		assert_eq!(parsed, i64::MIN, "{sql}");
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
	let negation = parse_expr("- -9223372036854775808").expect("a negated int8 minimum");
	let Expr::UnaryOp {
		op: UnaryOperator::Minus,
		expr,
		..
	} = &negation
	else {
		panic!("expected a negation of the int8 minimum, got {negation:?}")
	};
	assert!(matches!(
		**expr,
		Expr::Literal {
			lit: Literal::Integer(i64::MIN),
			..
		}
	));
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
		let expr = parse_expr(sql).expect(sql);
		let Expr::BinaryOp {
			op,
			right,
			..
		} = expr
		else {
			panic!("{sql}: expected a binary operator, got {expr:?}")
		};
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
	let quotient = parse_expr("-9223372036854775808 / -1").expect("a quotient");
	let Expr::BinaryOp {
		left,
		op: BinaryOperator::Div,
		..
	} = &quotient
	else {
		panic!("expected a quotient, got {quotient:?}")
	};
	assert!(matches!(
		**left,
		Expr::Literal {
			lit: Literal::Integer(i64::MIN),
			..
		}
	));
}

#[test]
fn test_a_json_operator_followed_by_a_negative_integer_is_one_operator_and_a_negation() {
	for (sql, operator) in [
		("data->>-1", BinaryOperator::LongArrow),
		("data->-1", BinaryOperator::Arrow),
		("data ->> -1", BinaryOperator::LongArrow),
	] {
		let expr = parse_expr(sql).expect(sql);
		let Expr::BinaryOp {
			op,
			right,
			..
		} = expr
		else {
			panic!("{sql}: expected a binary operator, got {expr:?}")
		};
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
}
