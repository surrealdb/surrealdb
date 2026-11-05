//! A MySQL `REPLACE` that finds an existing row writes it as a new row: each column it leaves out
//! is set to `NULL`, or to its default, so the row it leaves is the one the statement describes.

use surrealdb_expr::expr::Expr;
use surrealdb_types::ToSql;

use crate::catalog::{BinderCatalog, MemoryCatalog};
use crate::lower::write::*;

fn my(sql: &str, catalog: &MemoryCatalog) -> String {
	use surrealdb_mysql::ast::stmt::Statement;
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	if let Statement::Replace(replace) = &stmts[0] {
		return lower_mysql_replace(replace, catalog).unwrap().to_sql();
	}
	if let Statement::Insert(insert) = &stmts[0] {
		return lower_mysql_insert(insert, catalog).unwrap().to_sql();
	}
	panic!("expected an INSERT or a REPLACE: {sql}");
}

/// A table `t` whose column `b` holds NULL when it is left out and whose column `c` takes 5.
fn replace_catalog() -> MemoryCatalog {
	use surrealdb_expr::expr::Literal;
	MemoryCatalog::new()
		.with_null_columns("t", &["b"])
		.with_default_columns("t", &[("c", Expr::Literal(Literal::Integer(5)))])
}

#[test]
fn a_replace_resets_the_columns_it_leaves_out_when_it_hits_an_existing_row() {
	let lowered = my("REPLACE INTO t (id, a) VALUES (1, 5)", &replace_catalog());
	assert_eq!(
		lowered,
		"INSERT INTO t (id, a, b) VALUES (1, 5, NULL) ON DUPLICATE KEY UPDATE id = $input.id, \
		 a = $input.a, b = NULL, c = 5 RETURN *, record::id(id) AS id"
	);
}

#[test]
fn a_replace_that_lists_a_column_keeps_the_value_it_gives() {
	let lowered = my("REPLACE INTO t (id, a, b, c) VALUES (1, 5, 6, 7)", &replace_catalog());
	assert!(lowered.ends_with("c = $input.c RETURN *, record::id(id) AS id"), "{lowered}");
	assert!(!lowered.contains("NULL") && !lowered.contains("c = 5"), "{lowered}");
}

#[test]
fn a_replace_from_a_query_resets_the_columns_it_leaves_out() {
	let lowered = my("REPLACE INTO t (id, a) SELECT 1, 2", &replace_catalog());
	assert!(lowered.contains("b = NULL, c = 5"), "{lowered}");
}

#[test]
fn a_replace_resets_columns_by_the_names_mysql_compares() {
	let lowered = my("REPLACE INTO t (ID, A, B) VALUES (1, 5, NULL)", &replace_catalog());
	assert!(lowered.contains("c = 5") && !lowered.contains("b = NULL"), "{lowered}");
}

#[test]
fn a_replace_of_a_table_the_catalog_does_not_describe_assigns_only_what_it_lists() {
	let lowered = my("REPLACE INTO other (id, a) VALUES (1, 5)", &replace_catalog());
	assert!(
		lowered.ends_with("UPDATE id = $input.id, a = $input.a RETURN *, record::id(id) AS id")
	);
}

#[test]
fn an_insert_that_updates_on_a_duplicate_key_assigns_only_what_it_says() {
	let lowered =
		my("INSERT INTO t (id, a) VALUES (1, 5) ON DUPLICATE KEY UPDATE a = 3", &replace_catalog());
	assert!(lowered.contains("UPDATE a = 3 RETURN"), "{lowered}");
}

#[test]
fn a_catalog_wrapped_for_a_request_names_the_defaults_of_the_wrapped_one() {
	let inner = replace_catalog();
	let staged = crate::StagedCatalog::new(&inner);
	let defaults = staged.default_columns(None, "t");
	assert_eq!(defaults.iter().map(|(column, _)| column.as_str()).collect::<Vec<_>>(), ["c"]);
	assert!(staged.default_columns(None, "other").is_empty());
}

fn stage(staged: &mut crate::StagedCatalog<'_>, sql: &str) {
	use crate::lower::ddl::lower_pgsql_ddl_with;
	let statement = surrealdb_pgsql::parse_statement(sql).unwrap();
	let exprs = lower_pgsql_ddl_with(&statement, staged).unwrap();
	staged.stage(&exprs);
}

fn defaulted(staged: &crate::StagedCatalog<'_>, table: &str) -> Vec<String> {
	staged.default_columns(None, table).into_iter().map(|(column, _)| column).collect()
}

#[test]
fn a_table_a_staged_statement_creates_names_the_columns_that_take_a_default() {
	let mut staged = crate::StagedCatalog::new(&crate::EmptyCatalog);
	stage(
		&mut staged,
		"CREATE TABLE sg (id INT PRIMARY KEY, a INT, d INT DEFAULT 7, e INT DEFAULT 8)",
	);
	assert_eq!(defaulted(&staged, "sg"), ["d", "e"]);
	stage(&mut staged, "ALTER TABLE sg ALTER COLUMN d DROP DEFAULT");
	assert_eq!(defaulted(&staged, "sg"), ["e"]);
	stage(&mut staged, "ALTER TABLE sg DROP COLUMN e");
	assert!(defaulted(&staged, "sg").is_empty());
}

#[test]
fn a_column_a_staged_statement_changes_is_not_taken_from_the_wrapped_catalog() {
	use surrealdb_expr::expr::Literal;
	let mut inner = MemoryCatalog::new()
		.with_default_columns("t", &[("c", Expr::Literal(Literal::Integer(5)))]);
	inner.add_table(crate::TableSchema::new("t", vec![crate::ColumnInfo::new("id", 0)]));
	let mut staged = crate::StagedCatalog::new(&inner);
	assert_eq!(defaulted(&staged, "t"), ["c"]);
	stage(&mut staged, "ALTER TABLE t DROP COLUMN c");
	assert!(defaulted(&staged, "t").is_empty());
}

#[test]
fn a_column_a_staged_statement_adds_to_an_existing_table_takes_its_default() {
	let mut inner = MemoryCatalog::new();
	inner.add_table(crate::TableSchema::new("t", vec![crate::ColumnInfo::new("id", 0)]));
	let mut staged = crate::StagedCatalog::new(&inner);
	stage(&mut staged, "ALTER TABLE t ADD COLUMN e INT DEFAULT 1");
	assert_eq!(defaulted(&staged, "t"), ["e"]);
	// A definition that may do nothing adds none.
	stage(&mut staged, "ALTER TABLE t ADD COLUMN IF NOT EXISTS f INT DEFAULT 2");
	assert_eq!(defaulted(&staged, "t"), ["e"]);
}
