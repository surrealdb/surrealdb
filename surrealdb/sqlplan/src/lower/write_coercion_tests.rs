//! Text written to a typed column is read as the column's type.

use surrealdb_types::ToSql;

use crate::SqlType;
use crate::catalog::{BinderCatalog, MemoryCatalog};
use crate::lower::write::*;

fn pg_insert(sql: &str, catalog: &dyn BinderCatalog) -> String {
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("expected INSERT");
	};
	lower_pgsql_insert(insert, catalog).unwrap().to_sql()
}

fn catalog(ty: SqlType) -> MemoryCatalog {
	let mut catalog = MemoryCatalog::new();
	catalog.add_write_type("t", "c", ty);
	catalog
}

#[test]
fn a_parameter_written_to_an_integer_column_is_cast_when_it_is_text() {
	let sql = pg_insert("INSERT INTO t (id, c) VALUES (1, $1)", &catalog(SqlType::Int4));
	assert!(sql.contains("type::is_string($_1)"), "{sql}");
	assert!(sql.contains("<int> $_1"), "{sql}");
}

#[test]
fn a_parameter_written_to_a_json_column_is_decoded_when_it_is_text() {
	let sql = pg_insert("INSERT INTO t (id, c) VALUES (1, $1)", &catalog(SqlType::Json));
	assert!(sql.contains("encoding::json::decode($_1)"), "{sql}");
}

#[test]
fn the_other_types_a_text_parameter_is_read_as() {
	for (ty, cast) in [
		(SqlType::Numeric, "<decimal> $_1"),
		(SqlType::Float8, "<float> $_1"),
		(SqlType::Bool, "<bool> $_1"),
		(SqlType::TimestampTz, "<datetime> $_1"),
		(SqlType::Uuid, "<uuid> $_1"),
	] {
		let sql = pg_insert("INSERT INTO t (id, c) VALUES (1, $1)", &catalog(ty));
		assert!(sql.contains(cast), "{ty:?}: {sql}");
	}
}

#[test]
fn text_columns_and_undescribed_columns_are_written_as_they_are() {
	for catalog in [catalog(SqlType::Text), catalog(SqlType::Unknown), MemoryCatalog::new()] {
		let sql = pg_insert("INSERT INTO t (id, c) VALUES (1, $1)", &catalog);
		assert!(!sql.contains("type::is_string"), "{sql}");
	}
}

#[test]
fn a_value_that_is_not_a_parameter_or_a_text_literal_is_written_as_it_is() {
	let sql = pg_insert("INSERT INTO t (id, c) VALUES (1, 5)", &catalog(SqlType::Int4));
	assert!(!sql.contains("type::is_string"), "{sql}");
	let sql = pg_insert("INSERT INTO t (id, c) VALUES (1, $1 + 1)", &catalog(SqlType::Int4));
	assert!(!sql.contains("type::is_string"), "{sql}");
}

fn pg_update(sql: &str, catalog: &dyn BinderCatalog) -> String {
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Update(update) = &stmts[0] else {
		panic!("expected UPDATE");
	};
	lower_pgsql_update(update, catalog).unwrap().to_sql()
}

#[test]
fn a_parameter_compared_with_an_integer_id_names_the_integer_key() {
	let mut typed = MemoryCatalog::new();
	typed.add_write_type("t", "id", SqlType::Int4);
	let sql = pg_update("UPDATE t SET c = 1 WHERE id = $1", &typed);
	assert!(sql.contains("<int> $_1"), "{sql}");
	let sql = pg_update("UPDATE t SET c = 1 WHERE id = '7'", &typed);
	assert!(sql.contains("t:7"), "{sql}");
	let sql = pg_update("UPDATE t SET c = 1 WHERE id = $1", &MemoryCatalog::new());
	assert!(!sql.contains("<int>"), "{sql}");
}

#[test]
fn a_select_by_text_key_names_the_integer_key_of_an_integer_id() {
	use crate::lower::link_tests::pg_select_in;
	let mut typed = MemoryCatalog::new();
	typed.add_write_type("t", "id", SqlType::Int4);
	let plan = format!("{:?}", pg_select_in("SELECT * FROM t WHERE id = $1", &typed));
	assert!(plan.contains("Cast(Int)"), "{plan}");
	let plan = format!("{:?}", pg_select_in("SELECT * FROM t WHERE id = '7'", &typed));
	assert!(plan.contains("Number(7)"), "{plan}");
	let plan =
		format!("{:?}", pg_select_in("SELECT * FROM t WHERE id = $1", &MemoryCatalog::new()));
	assert!(!plan.contains("Cast(Int)"), "{plan}");
}

#[test]
fn a_limit_or_offset_parameter_that_holds_text_is_read_as_the_integer_it_spells() {
	use crate::lower::link_tests::pg_select_in;
	let plan =
		format!("{:?}", pg_select_in("SELECT * FROM t LIMIT $1 OFFSET $2", &MemoryCatalog::new()));
	assert_eq!(plan.matches("Cast(Int)").count(), 2, "{plan}");
}

#[test]
fn text_written_to_a_link_column_is_read_as_the_key_type_of_the_table_it_links_to() {
	let mut linked = MemoryCatalog::new().with_link("post", "author", "user");
	linked.add_write_type("user", "id", SqlType::Int4);
	let sql = pg_insert("INSERT INTO post (id, author) VALUES (1, $1)", &linked);
	assert!(sql.contains("<int> $_1"), "{sql}");
}

#[test]
fn a_text_literal_compared_with_a_uuid_id_names_the_uuid_key() {
	use crate::lower::link_tests::pg_select_in;
	let mut typed = MemoryCatalog::new();
	typed.add_write_type("t", "id", SqlType::Uuid);
	let plan = format!(
		"{:?}",
		pg_select_in("SELECT * FROM t WHERE id = '550e8400-e29b-41d4-a716-446655440000'", &typed)
	);
	assert!(plan.contains("Cast(Uuid)"), "{plan}");
}
