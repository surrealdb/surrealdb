//! The `*` of a relation (`t.*`) in the select list of a query through a SQL front end.

use surrealdb_expr::expr::logical::LogicalOp;
use surrealdb_types::ToSql;

use crate::catalog::{BinderCatalog, ColumnInfo, EmptyCatalog, MemoryCatalog, TableSchema};
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::lower::link_tests::projection;
use crate::lower::{lower_pgsql_create_view, lower_pgsql_insert};

fn pg_plan(sql: &str, catalog: &dyn BinderCatalog) -> Result<LogicalOp, PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_pgsql(select, catalog, &DialectPolicy::postgres())
}

fn my_plan(sql: &str, catalog: &dyn BinderCatalog) -> Result<LogicalOp, PlanError> {
	let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_mysql(select, catalog, &DialectPolicy::mysql())
}

fn pg(sql: &str) -> Vec<String> {
	projection(&pg_plan(sql, &EmptyCatalog).expect(sql))
}

fn my(sql: &str) -> Vec<String> {
	projection(&my_plan(sql, &EmptyCatalog).expect(sql))
}

/// `person(id, name)` and `profile(id, bio)`, whose columns are known.
fn described() -> MemoryCatalog {
	let mut catalog = MemoryCatalog::new();
	for (table, columns) in [("person", ["id", "name"]), ("profile", ["id", "bio"])] {
		catalog.add_table(TableSchema::new(
			table,
			columns.iter().enumerate().map(|(i, name)| ColumnInfo::new(*name, i)).collect(),
		));
	}
	catalog
}

#[test]
fn the_star_of_the_one_table_of_a_query_is_the_star_of_the_query() {
	let star = pg("SELECT * FROM person");
	assert_eq!(star, vec!["*".to_string(), "record::id(id) AS id".into()]);
	for sql in [
		"SELECT p.* FROM person p",
		"SELECT person.* FROM person",
		"SELECT public.person.* FROM public.person",
		"SELECT \"p\".* FROM person AS \"p\"",
		"SELECT P.* FROM person p",
		// An alias is ignored, as PostgreSQL does.
		"SELECT p.* AS x FROM person p",
	] {
		assert_eq!(pg(sql), star, "{sql}");
	}
	for sql in ["SELECT p.* FROM person p", "SELECT db.person.* FROM db.person"] {
		assert_eq!(my(sql), star, "{sql}");
	}
}

#[test]
fn a_star_of_a_relation_keeps_its_place_among_the_items() {
	let columns = pg("SELECT name, p.*, age FROM person p");
	assert_eq!(columns[0], "name AS name", "{columns:?}");
	assert_eq!(columns[1], "*", "{columns:?}");
	assert_eq!(columns[2], "record::id(id) AS id", "{columns:?}");
	assert_eq!(columns[3], "age AS age", "{columns:?}");
}

#[test]
fn the_star_of_a_derived_table_is_the_star_of_the_query() {
	assert_eq!(
		pg("SELECT d.* FROM (SELECT * FROM person) d"),
		pg("SELECT * FROM (SELECT * FROM person) d")
	);
	assert_eq!(
		my("SELECT d.* FROM (SELECT * FROM person) d"),
		my("SELECT * FROM (SELECT * FROM person) d")
	);
}

#[test]
fn the_star_of_a_table_whose_columns_are_not_known_is_refused_among_several_tables() {
	for sql in [
		"SELECT p.*, f.x FROM person p JOIN profile f ON p.id = f.id",
		"SELECT f.* FROM person p JOIN profile f ON p.id = f.id",
		"SELECT p.* FROM person p, profile f",
		"SELECT p.* FROM person p LEFT JOIN profile f ON p.id = f.id",
	] {
		let err = pg_plan(sql, &EmptyCatalog).expect_err(sql);
		assert!(
			matches!(&err, PlanError::UnsupportedExpression(message) if message.contains(".* of a table whose columns are not known")),
			"{sql}: {err:?}"
		);
		let err = my_plan(sql, &EmptyCatalog).expect_err(sql);
		assert!(matches!(err, PlanError::UnsupportedExpression(_)), "{sql}: {err:?}");
	}
}

#[test]
fn the_star_of_a_table_whose_columns_are_known_lists_them() {
	let catalog = described();
	let columns = |sql: &str| projection(&pg_plan(sql, &catalog).expect(sql));
	assert_eq!(columns("SELECT p.* FROM person p"), columns("SELECT id, name FROM person p"));
	let joined = columns("SELECT f.*, p.name FROM person p JOIN profile f ON p.id = f.id");
	assert_eq!(joined.len(), 3, "{joined:?}");
	assert!(joined[0].contains("id") && joined[1].contains("bio"), "{joined:?}");
	assert!(joined[2].contains("name"), "{joined:?}");
}

#[test]
fn the_star_of_a_relation_that_is_not_in_the_query_is_refused() {
	for sql in [
		"SELECT x.* FROM person p",
		"SELECT p.* FROM profile f",
		"SELECT a.b.c.d.* FROM person p",
		"SELECT \"P\".* FROM person p",
	] {
		let err = pg_plan(sql, &EmptyCatalog).expect_err(sql);
		assert!(matches!(err, PlanError::MissingFromClauseEntry(_)), "{sql}: {err:?}");
	}
	let err = pg_plan("SELECT x.* FROM person p", &EmptyCatalog).expect_err("x");
	assert_eq!(err.to_string(), "missing FROM-clause entry for table \"x\"");
}

fn insert(sql: &str) -> Result<String, PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("expected INSERT");
	};
	lower_pgsql_insert(insert, &EmptyCatalog).map(|expr| expr.to_sql())
}

#[test]
fn the_star_of_the_table_an_insert_reads_is_its_star() {
	let star = insert("INSERT INTO dst SELECT * FROM src").expect("star");
	for sql in [
		"INSERT INTO dst SELECT s.* FROM src s",
		"INSERT INTO dst SELECT src.* FROM src",
		"INSERT INTO dst SELECT public.src.* FROM public.src",
		"INSERT INTO dst (a, b) SELECT s.* FROM src s",
	] {
		assert_eq!(insert(sql).expect(sql), star, "{sql}");
	}
}

#[test]
fn the_star_of_a_table_next_to_another_item_is_refused_in_an_insert() {
	for sql in [
		"INSERT INTO dst (a, b, c) SELECT s.*, 5 FROM src s",
		"INSERT INTO dst (a, b, c) SELECT 5, s.* FROM src s",
		"INSERT INTO dst (a, b) SELECT s.*, s.* FROM src s",
	] {
		let err = insert(sql).expect_err(sql);
		assert!(
			matches!(&err, PlanError::UnsupportedStatement(message) if message.contains("the * of a table together with another item")),
			"{sql}: {err:?}"
		);
	}
}

#[test]
fn the_star_of_another_table_is_refused_in_an_insert() {
	let err = insert("INSERT INTO dst SELECT x.* FROM src s").expect_err("another table");
	assert_eq!(err, PlanError::MissingFromClauseEntry("x".into()));
	let err = insert("INSERT INTO dst SELECT s.* FROM src s JOIN other o ON s.a = o.a")
		.expect_err("a join");
	// A join is not a source an INSERT can read: the star of one is refused, either as an
	// expression or, when the join is refused before its columns are read, as a statement.
	assert!(
		matches!(err, PlanError::UnsupportedExpression(_) | PlanError::UnsupportedStatement(_)),
		"{err:?}"
	);
}

#[test]
fn the_star_of_a_relation_is_refused_in_a_view() {
	let view = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
		let surrealdb_pgsql::ast::stmt::Statement::CreateView(view) = &stmts[0] else {
			panic!("expected CREATE VIEW");
		};
		lower_pgsql_create_view(view)
	};
	assert!(view("CREATE VIEW v AS SELECT * FROM t").is_ok());
	let err = view("CREATE VIEW v AS SELECT t.* FROM t").expect_err("a view");
	assert!(
		matches!(&err, PlanError::UnsupportedExpression(message) if message.starts_with("t.*"))
	);
}

#[test]
fn the_columns_of_a_star_of_a_relation_are_known_only_from_the_rows_as_for_a_bare_star() {
	let dynamic = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = crate::binder::pgsql::bind_select(
			select,
			&EmptyCatalog,
			&DialectPolicy::postgres(),
			None,
		)
		.expect(sql);
		bound.has_dynamic_columns()
	};
	assert!(dynamic("SELECT * FROM person"));
	assert!(dynamic("SELECT p.* FROM person p"));
	assert!(dynamic("SELECT d.* FROM (SELECT * FROM person) d"));
	assert!(!dynamic("SELECT p.name FROM person p"));
}
