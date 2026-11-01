//! `TRUNCATE`: which identity option the statement carries.

use surrealdb_pgsql::ast::stmt::Statement;
use surrealdb_pgsql::{ToPgSql, parse_statement};

fn restarts_identity(sql: &str) -> bool {
	let statement = parse_statement(sql).unwrap_or_else(|e| panic!("failed to parse '{sql}': {e}"));
	let Statement::Truncate(truncate) = &statement else {
		panic!("'{sql}' is not a TRUNCATE: {statement:?}");
	};
	truncate.restart_identity
}

#[test]
fn restart_identity_is_the_only_option_that_restarts_the_sequences() {
	assert!(!restarts_identity("TRUNCATE t"));
	assert!(!restarts_identity("TRUNCATE TABLE t"));
	assert!(!restarts_identity("TRUNCATE TABLE ONLY t CONTINUE IDENTITY"));
	assert!(!restarts_identity("TRUNCATE t CONTINUE IDENTITY CASCADE"));
	assert!(!restarts_identity("TRUNCATE t RESTRICT"));

	assert!(restarts_identity("TRUNCATE t RESTART IDENTITY"));
	assert!(restarts_identity("TRUNCATE TABLE t RESTART IDENTITY"));
	assert!(restarts_identity("TRUNCATE TABLE ONLY s.t RESTART IDENTITY CASCADE"));
	assert!(restarts_identity("truncate t restart identity restrict"));
}

#[test]
fn a_printed_truncate_keeps_its_identity_option() {
	let print = |sql: &str| parse_statement(sql).expect("parse").to_pgsql_string();
	assert_eq!(print("TRUNCATE t"), "TRUNCATE TABLE t");
	assert_eq!(print("TRUNCATE t CONTINUE IDENTITY"), "TRUNCATE TABLE t");
	assert_eq!(print("TRUNCATE t RESTART IDENTITY"), "TRUNCATE TABLE t RESTART IDENTITY");
	for sql in ["TRUNCATE t", "TRUNCATE t RESTART IDENTITY"] {
		let printed = print(sql);
		assert_eq!(restarts_identity(&printed), restarts_identity(sql), "{printed}");
	}
}
