//! A DDL statement lowers to the one expression that answers it.

use surrealdb_expr::expr::Expr;

use super::{
	lower_mysql_ddl, lower_mysql_ddl_definitions, lower_pgsql_ddl, lower_pgsql_ddl_definitions,
};

/// The definitions in the block `exprs` holds as its only expression.
fn block_of(exprs: &[Expr]) -> &[Expr] {
	let [Expr::Block(block)] = exprs else {
		panic!("expected one block, got {exprs:?}");
	};
	&block.0
}

#[test]
fn a_pgsql_statement_with_several_definitions_lowers_to_one_block() {
	for sql in [
		"CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)",
		"ALTER TABLE t ADD COLUMN x INT, ADD COLUMN y INT",
		"DROP TABLE a, b",
	] {
		let stmt = surrealdb_pgsql::parse_statement(sql).expect("parse");
		let definitions = lower_pgsql_ddl_definitions(&stmt).expect("lower definitions");
		assert!(definitions.len() > 1, "{sql}: {definitions:?}");
		let exprs = lower_pgsql_ddl(&stmt).expect("lower");
		assert_eq!(block_of(&exprs), definitions, "{sql}");
	}
}

#[test]
fn a_mysql_statement_with_several_definitions_lowers_to_one_block() {
	for sql in ["CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)", "DROP TABLE a, b"] {
		let stmt = surrealdb_mysql::parse_statement(sql).expect("parse");
		let definitions = lower_mysql_ddl_definitions(&stmt).expect("lower definitions");
		assert!(definitions.len() > 1, "{sql}: {definitions:?}");
		let exprs = lower_mysql_ddl(&stmt).expect("lower");
		assert_eq!(block_of(&exprs), definitions, "{sql}");
	}
}

#[test]
fn a_statement_with_one_definition_lowers_to_that_definition() {
	let stmt = surrealdb_pgsql::parse_statement("DROP TABLE a").expect("parse");
	let definitions = lower_pgsql_ddl_definitions(&stmt).expect("lower definitions");
	assert_eq!(definitions.len(), 1, "{definitions:?}");
	assert_eq!(lower_pgsql_ddl(&stmt).expect("lower"), definitions);

	let stmt = surrealdb_mysql::parse_statement("DROP TABLE a").expect("parse");
	let definitions = lower_mysql_ddl_definitions(&stmt).expect("lower definitions");
	assert_eq!(definitions.len(), 1, "{definitions:?}");
	assert_eq!(lower_mysql_ddl(&stmt).expect("lower"), definitions);
}

#[test]
fn a_statement_with_no_definition_lowers_to_an_empty_block() {
	let stmt =
		surrealdb_pgsql::parse_statement("ALTER TABLE t RENAME COLUMN a TO b").expect("parse");
	assert!(lower_pgsql_ddl_definitions(&stmt).expect("lower definitions").is_empty());
	let exprs = lower_pgsql_ddl(&stmt).expect("lower");
	assert!(block_of(&exprs).is_empty(), "{exprs:?}");
}

#[test]
fn an_interval_with_a_field_qualifier_is_an_interval_that_declares_its_qualifier() {
	use surrealdb_expr::expr::Kind;
	use surrealdb_pgsql::ast::stmt::Statement;

	use super::{map_pgsql_type, pgsql_declared_type};
	for (written, declared, kind) in [
		("INTERVAL", "interval", Kind::Interval),
		("INTERVAL(3)", "interval(3)", Kind::Interval),
		("INTERVAL YEAR", "interval year", Kind::Interval),
		("INTERVAL YEAR TO MONTH", "interval year to month", Kind::Interval),
		("INTERVAL HOUR", "interval hour", Kind::Interval),
		("INTERVAL SECOND(3)", "interval second(3)", Kind::Interval),
		("INTERVAL DAY TO SECOND(3)", "interval day to second(3)", Kind::Interval),
		(
			"INTERVAL MINUTE TO SECOND[]",
			"interval minute to second[]",
			Kind::Array(Box::new(Kind::Interval), None),
		),
	] {
		let sql = format!("CREATE TABLE t (a {written})");
		let Statement::CreateTable(create) = surrealdb_pgsql::parse_statement(&sql).expect("parse")
		else {
			panic!("expected CREATE TABLE");
		};
		let data_type = &create.columns[0].data_type;
		assert_eq!(map_pgsql_type(data_type), kind, "{written}");
		assert_eq!(pgsql_declared_type(data_type).expect("declared").text, declared, "{written}");
	}
}
