//! Parser unit tests for surrealdb-mysql.

use surrealdb_mysql::ast::expr::Expr;
use surrealdb_mysql::ast::literal::Literal;
use surrealdb_mysql::ast::operator::BinaryOperator;
use surrealdb_mysql::ast::stmt::Statement;
use surrealdb_mysql::parser::{parse_expr, parse_statement, parse_statements};

#[test]
fn test_parse_literals() {
	let e = parse_expr("42").unwrap();
	assert_eq!(e, Expr::Literal(Literal::Integer(42), e.span()));

	let e = parse_expr("'hello'").unwrap();
	assert_eq!(e, Expr::Literal(Literal::String("hello".to_string()), e.span()));

	let e = parse_expr("NULL").unwrap();
	assert_eq!(e, Expr::Literal(Literal::Null, e.span()));

	let e = parse_expr("TRUE").unwrap();
	assert_eq!(e, Expr::Literal(Literal::Boolean(true), e.span()));
}

#[test]
fn test_parse_binary_expressions_precedence() {
	let e = parse_expr("1 + 2 * 3").unwrap();
	if let Expr::BinaryOp {
		left,
		op,
		right,
		..
	} = e
	{
		assert_eq!(op, BinaryOperator::Add);
		assert_eq!(*left, Expr::Literal(Literal::Integer(1), left.span()));
		if let Expr::BinaryOp {
			left: l2,
			op: op2,
			right: r2,
			..
		} = *right
		{
			assert_eq!(op2, BinaryOperator::Mul);
			assert_eq!(*l2, Expr::Literal(Literal::Integer(2), l2.span()));
			assert_eq!(*r2, Expr::Literal(Literal::Integer(3), r2.span()));
		} else {
			panic!("Expected right to be Mul binary op");
		}
	} else {
		panic!("Expected Add binary op");
	}
}

#[test]
fn test_parse_null_safe_equal() {
	let e = parse_expr("a <=> b").unwrap();
	if let Expr::BinaryOp {
		op,
		..
	} = e
	{
		assert_eq!(op, BinaryOperator::NullSafeEqual);
	} else {
		panic!("Expected NullSafeEqual");
	}
}

#[test]
fn test_parse_assignment_operator() {
	let e = parse_expr("@a := 10").unwrap();
	if let Expr::BinaryOp {
		op,
		..
	} = e
	{
		assert_eq!(op, BinaryOperator::Assign);
	} else {
		panic!("Expected Assign");
	}
}

#[test]
fn test_parse_select_statements() {
	let s = parse_statement("SELECT 1").unwrap();
	assert!(matches!(s, Statement::Select(_)));

	let s = parse_statement(
		"SELECT id, name FROM `users` WHERE age >= 18 ORDER BY id DESC LIMIT 10 OFFSET 5",
	)
	.unwrap();
	if let Statement::Select(sel) = s {
		assert_eq!(sel.projection.len(), 2);
		assert!(sel.from.is_some());
		assert!(sel.where_clause.is_some());
		assert_eq!(sel.order_by.len(), 1);
		assert_eq!(sel.order_by[0].asc, false);
		assert!(sel.limit.is_some());
	} else {
		panic!("Expected Select statement");
	}
}

#[test]
fn test_parse_insert_on_duplicate_key_update() {
	let s = parse_statement("INSERT INTO `users` (id, name, visits) VALUES (1, 'Alice', 1) ON DUPLICATE KEY UPDATE visits = visits + 1").unwrap();
	if let Statement::Insert(ins) = s {
		assert_eq!(ins.columns.len(), 3);
		assert_eq!(ins.values.len(), 1);
		assert_eq!(ins.on_duplicate_key_update.len(), 1);
		assert_eq!(ins.on_duplicate_key_update[0].0, "visits");
	} else {
		panic!("Expected Insert statement");
	}
}

#[test]
fn test_parse_update_delete() {
	let s =
		parse_statement("UPDATE `users` SET name = 'Bob', age = 30 WHERE id = 1 LIMIT 1").unwrap();
	assert!(matches!(s, Statement::Update(_)));

	let s = parse_statement("DELETE FROM `users` WHERE id = 1").unwrap();
	assert!(matches!(s, Statement::Delete(_)));
}

#[test]
fn test_parse_ddl_and_admin() {
	let s = parse_statement(
		"CREATE TABLE IF NOT EXISTS `users` (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)",
	)
	.unwrap();
	assert!(matches!(s, Statement::CreateTable(_)));

	let s = parse_statement("DROP TABLE IF EXISTS `users`").unwrap();
	assert!(matches!(s, Statement::DropTable(_)));

	let s = parse_statement("USE my_database").unwrap();
	assert!(matches!(s, Statement::Use(_)));

	let s = parse_statement("SET @@session.time_zone = '+00:00'").unwrap();
	assert!(matches!(s, Statement::Set(_)));

	let s = parse_statement("SHOW TABLES LIKE 'user%'").unwrap();
	assert!(matches!(s, Statement::Show(_)));

	let s = parse_statement("DESCRIBE `users`").unwrap();
	assert!(matches!(s, Statement::Describe(_)));

	let s = parse_statement("START TRANSACTION READ ONLY").unwrap();
	assert!(matches!(s, Statement::Transaction(_)));
}

#[test]
fn test_parse_multiple_statements() {
	let stmts = parse_statements("SELECT 1; SELECT 2; SELECT 3;").unwrap();
	assert_eq!(stmts.len(), 3);
}
