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
fn test_parse_exponent_right_associativity() {
	// `2 ^ 3 ^ 4` must parse as `2 ^ (3 ^ 4)` because ^ is right-associative in PG
	let expr = parse_expr("2 ^ 3 ^ 4").expect("parse exponent");
	match expr {
		Expr::BinaryOp {
			left,
			op: BinaryOperator::Exp,
			right,
			..
		} => {
			assert!(matches!(
				*left,
				Expr::Literal {
					lit: Literal::Integer(2),
					..
				}
			));
			match *right {
				Expr::BinaryOp {
					left: r_left,
					op: BinaryOperator::Exp,
					right: r_right,
					..
				} => {
					assert!(matches!(
						*r_left,
						Expr::Literal {
							lit: Literal::Integer(3),
							..
						}
					));
					assert!(matches!(
						*r_right,
						Expr::Literal {
							lit: Literal::Integer(4),
							..
						}
					));
				}
				other => panic!("expected right-nested exp, got {:?}", other),
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
