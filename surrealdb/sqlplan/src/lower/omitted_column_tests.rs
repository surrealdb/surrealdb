//! A column an `INSERT` leaves out is written as NULL when its table says it holds NULL there, so
//! that a row that leaves a column out and a row that gives it NULL are one value to a grouping, a
//! `DISTINCT` and a set operation, as they are in PostgreSQL and MySQL.

use surrealdb_expr::expr::Expr;
use surrealdb_types::ToSql;

use crate::catalog::{BinderCatalog, MemoryCatalog};
use crate::lower::write::*;

/// A table `t` whose columns `a` and `b` hold NULL when they are left out.
fn catalog() -> MemoryCatalog {
	MemoryCatalog::new().with_null_columns("t", &["a", "b"])
}

fn pg(sql: &str, catalog: &MemoryCatalog) -> String {
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("expected an INSERT");
	};
	lower_pgsql_insert(insert, catalog).unwrap().to_sql()
}

fn my(sql: &str, catalog: &MemoryCatalog) -> String {
	use surrealdb_mysql::ast::stmt::Statement;
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	if let Statement::Insert(insert) = &stmts[0] {
		return lower_mysql_insert(insert, catalog).unwrap().to_sql();
	}
	if let Statement::Replace(replace) = &stmts[0] {
		return lower_mysql_replace(replace, catalog).unwrap().to_sql();
	}
	panic!("expected an INSERT or a REPLACE: {sql}");
}

/// Whether the lowered `sql` writes NULL to `column`.
fn writes_null(sql: &str, column: &str) -> bool {
	sql.contains(&format!("{column}: NULL")) || sql.contains(&format!("{column} = NULL"))
}

#[test]
fn a_column_the_row_leaves_out_is_written_as_null() {
	let catalog = catalog();
	let lowered = pg("INSERT INTO t (id, a) VALUES (1, 5)", &catalog);
	assert!(lowered.contains("NULL"), "{lowered}");
	let lowered = pg("INSERT INTO t (id, a, b) VALUES (1, 5, 6)", &catalog);
	assert!(!lowered.contains("NULL"), "a listed column keeps its value: {lowered}");
}

#[test]
fn only_the_columns_the_row_leaves_out_are_written_as_null() {
	let catalog = catalog();
	let one_left_out = pg("INSERT INTO t (id, a) VALUES (1, 5)", &catalog);
	let none_left_out = pg("INSERT INTO t (id, a, b) VALUES (1, 5, NULL)", &catalog);
	assert_eq!(one_left_out.matches("NULL").count(), 1, "{one_left_out}");
	assert_eq!(none_left_out.matches("NULL").count(), 1, "{none_left_out}");
	let all_left_out = pg("INSERT INTO t (id) VALUES (1)", &catalog);
	assert_eq!(all_left_out.matches("NULL").count(), 2, "{all_left_out}");
}

#[test]
fn every_row_of_a_multi_row_insert_is_filled() {
	let lowered = pg("INSERT INTO t (id, a) VALUES (1, 5), (2, 6), (3, 7)", &catalog());
	assert_eq!(lowered.matches("NULL").count(), 3, "{lowered}");
}

#[test]
fn a_table_the_catalog_does_not_describe_is_written_as_listed() {
	let lowered = pg("INSERT INTO other (id, a) VALUES (1, 5)", &catalog());
	assert!(!lowered.contains("NULL"), "{lowered}");
	let lowered = pg("INSERT INTO t (id, a) VALUES (1, 5)", &MemoryCatalog::new());
	assert!(!lowered.contains("NULL"), "{lowered}");
}

#[test]
fn a_row_with_no_column_list_is_written_as_given() {
	let lowered = pg("INSERT INTO t VALUES (1, 5)", &catalog());
	assert!(!lowered.contains("NULL"), "the columns it leaves out are not known: {lowered}");
}

#[test]
fn default_values_write_every_null_column() {
	let lowered = pg("INSERT INTO t DEFAULT VALUES", &catalog());
	assert!(writes_null(&lowered, "a") && writes_null(&lowered, "b"), "{lowered}");
}

#[test]
fn an_insert_that_selects_its_rows_names_the_columns_it_leaves_out_as_null() {
	let lowered = pg("INSERT INTO t (id, a) SELECT id, a FROM s", &catalog());
	assert_eq!(lowered.matches("NULL").count(), 1, "{lowered}");
	assert!(lowered.contains("NULL AS b"), "{lowered}");
	let lowered = my("INSERT INTO t (id, a) SELECT id, a FROM s", &catalog());
	assert!(lowered.contains("NULL AS b") && !lowered.contains("NULL AS a"), "{lowered}");
	let lowered = pg("INSERT INTO t (id, a, b) SELECT id, a, b FROM s", &catalog());
	assert!(!lowered.contains("NULL"), "{lowered}");
}

#[test]
fn an_insert_that_selects_a_star_names_the_columns_it_leaves_out_as_null() {
	let lowered = pg("INSERT INTO t (id, a) SELECT * FROM s", &catalog());
	assert!(lowered.contains("NULL AS b") && !lowered.contains("NULL AS a"), "{lowered}");
	let lowered = my("INSERT INTO t (id, a) SELECT * FROM s", &catalog());
	assert!(lowered.contains("NULL AS b") && !lowered.contains("NULL AS a"), "{lowered}");
}

#[test]
fn an_insert_that_lists_no_columns_takes_every_column_from_its_select() {
	for lowered in [
		pg("INSERT INTO t SELECT * FROM s", &catalog()),
		pg("INSERT INTO t SELECT id, a, b FROM s", &catalog()),
		my("INSERT INTO t SELECT id, a, b FROM s", &catalog()),
	] {
		assert!(!lowered.contains("NULL"), "{lowered}");
	}
}

#[test]
fn the_row_an_upsert_inserts_is_filled() {
	let lowered = pg(
		"INSERT INTO t (id, a) VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a \
		 WHERE t.a > 0",
		&catalog(),
	);
	assert!(
		lowered.contains("LET $excluded = { id: 1, a: 5, b: NULL };"),
		"b is left out: {lowered}"
	);
	assert!(
		lowered
			.contains("INSERT INTO t (id, a, b) VALUES ($excluded.id, $excluded.a, $excluded.b)"),
		"the inserted row reads the proposed one: {lowered}"
	);
}

#[test]
fn mysql_fills_the_columns_an_insert_and_a_replace_leave_out() {
	let catalog = catalog();
	for sql in ["INSERT INTO t (id, a) VALUES (1, 5)", "REPLACE INTO t (id, a) VALUES (1, 5)"] {
		let lowered = my(sql, &catalog);
		assert!(lowered.contains("NULL"), "{sql}: {lowered}");
	}
	let lowered = my("INSERT INTO t (id, a, b) VALUES (1, 5, 6)", &catalog);
	assert!(!lowered.contains("NULL"), "{lowered}");
}

#[test]
fn mysql_matches_a_listed_column_whatever_its_case() {
	let lowered = my("INSERT INTO t (id, A) VALUES (1, 5)", &catalog());
	assert_eq!(lowered.matches("NULL").count(), 1, "A is the column a: {lowered}");
}

#[test]
fn postgres_matches_a_listed_column_exactly() {
	let lowered = pg("INSERT INTO t (id, \"A\") VALUES (1, 5)", &catalog());
	assert_eq!(lowered.matches("NULL").count(), 2, "\"A\" is not the column a: {lowered}");
}

#[test]
fn the_lowering_of_an_insert_is_an_insert() {
	let stmts = surrealdb_pgsql::parse_statements("INSERT INTO t (id) VALUES (1)").unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("expected an INSERT");
	};
	let lowered = lower_pgsql_insert(insert, &catalog()).unwrap();
	assert!(matches!(lowered, Expr::Insert(_)));
}

#[test]
fn a_catalog_wrapped_for_a_request_names_the_columns_of_the_wrapped_one() {
	let inner = catalog();
	let staged = crate::StagedCatalog::new(&inner);
	assert_eq!(staged.null_columns(None, "t"), ["a", "b"]);
	assert!(staged.null_columns(None, "other").is_empty());
	let lowered = pg("INSERT INTO t (id, a) VALUES (1, 5)", &inner);
	assert!(lowered.contains("NULL"), "{lowered}");
}

fn stage(staged: &mut crate::StagedCatalog<'_>, sql: &str) {
	use crate::lower::ddl::lower_pgsql_ddl_with;
	let statement = surrealdb_pgsql::parse_statement(sql).unwrap();
	let exprs = lower_pgsql_ddl_with(&statement, staged).unwrap();
	staged.stage(&exprs);
}

#[test]
fn a_table_a_staged_statement_creates_names_the_columns_that_hold_null() {
	let mut staged = crate::StagedCatalog::new(&crate::EmptyCatalog);
	stage(
		&mut staged,
		"CREATE TABLE sg (id INT PRIMARY KEY, a INT, b TEXT, d INT DEFAULT 7, r INT NOT NULL)",
	);
	assert_eq!(staged.null_columns(None, "sg"), ["a", "b"]);
	stage(&mut staged, "ALTER TABLE sg ADD COLUMN c TEXT");
	assert_eq!(staged.null_columns(None, "sg"), ["a", "b", "c"]);
	stage(&mut staged, "ALTER TABLE sg DROP COLUMN b");
	assert_eq!(staged.null_columns(None, "sg"), ["a", "c"]);
	stage(&mut staged, "CREATE TABLE sg (id INT PRIMARY KEY, z INT)");
	assert_eq!(staged.null_columns(None, "sg"), ["z"]);
	stage(&mut staged, "DROP TABLE sg");
	assert!(staged.null_columns(None, "sg").is_empty());
}

#[test]
fn a_column_a_staged_statement_changes_is_not_taken_from_the_wrapped_catalog() {
	let inner = catalog();
	let mut staged = crate::StagedCatalog::new(&inner);
	assert_eq!(staged.null_columns(None, "t"), ["a", "b"]);
	stage(&mut staged, "ALTER TABLE t DROP COLUMN b");
	assert_eq!(staged.null_columns(None, "t"), ["a"]);
}

#[test]
fn a_table_created_if_not_exists_names_the_columns_that_hold_null() {
	let mut inner = MemoryCatalog::new();
	inner.add_table(crate::TableSchema::new("other", vec![crate::ColumnInfo::new("id", 0)]));
	let mut staged = crate::StagedCatalog::new(&inner);
	stage(&mut staged, "CREATE TABLE IF NOT EXISTS sg (id INT PRIMARY KEY, a INT, b TEXT)");
	assert_eq!(staged.null_columns(None, "sg"), ["a", "b"]);
	// A column that a request defines again if it does not exist keeps its first definition.
	stage(&mut staged, "ALTER TABLE sg ADD COLUMN IF NOT EXISTS a INT NOT NULL");
	assert_eq!(staged.null_columns(None, "sg"), ["a", "b"]);
	stage(&mut staged, "ALTER TABLE sg ADD COLUMN IF NOT EXISTS c TEXT");
	assert_eq!(staged.null_columns(None, "sg"), ["a", "b", "c"]);
}

#[test]
fn a_table_created_if_not_exists_that_exists_keeps_the_columns_the_wrapped_catalog_names() {
	let mut inner = catalog();
	inner.add_table(crate::TableSchema::new("t", vec![crate::ColumnInfo::new("id", 0)]));
	let mut staged = crate::StagedCatalog::new(&inner);
	stage(&mut staged, "CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY, a INT, b TEXT)");
	assert_eq!(staged.null_columns(None, "t"), ["a", "b"]);
	// A definition that runs replaces what the wrapped catalog names.
	stage(&mut staged, "ALTER TABLE t DROP COLUMN b");
	assert_eq!(staged.null_columns(None, "t"), ["a"]);
}

#[test]
fn a_column_a_staged_statement_adds_to_an_existing_table_holds_null() {
	let mut inner = catalog();
	inner.add_table(crate::TableSchema::new("t", vec![crate::ColumnInfo::new("id", 0)]));
	let mut staged = crate::StagedCatalog::new(&inner);
	stage(&mut staged, "ALTER TABLE t ADD COLUMN c TEXT");
	assert_eq!(staged.null_columns(None, "t"), ["a", "b", "c"]);
	// A definition that may do nothing, and one with a default, add none.
	stage(&mut staged, "ALTER TABLE t ADD COLUMN IF NOT EXISTS d TEXT");
	stage(&mut staged, "ALTER TABLE t ADD COLUMN e INT DEFAULT 1");
	assert_eq!(staged.null_columns(None, "t"), ["a", "b", "c"]);
}
