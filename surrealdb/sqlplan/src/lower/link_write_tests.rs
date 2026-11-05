//! The lowered shape of the writes of a record link column through a SQL front end.

// A catch-all arm in these tests reports the value a lowering produced instead of the one
// expected; naming every other variant would add nothing to that report.
#![allow(clippy::wildcard_enum_match_arm)]

use std::collections::BTreeSet;

use surrealdb_expr::expr::statements::InsertStatement;
use surrealdb_expr::expr::{
	Data, Expr, Fields, Literal, Output, Param, RecordIdKeyLit, RecordIdLit,
};
use surrealdb_strand::TableName;
use surrealdb_types::ToSql;

use crate::catalog::{BinderCatalog, EmptyCatalog, MemoryCatalog};
use crate::lower::link_expr_tests::{KEY, MENTOR_KEY};
use crate::lower::link_tests::{
	RecordingCatalog, asked, catalog, my_select, pg_select, predicate, projection,
};
use crate::lower::write::*;

fn pg(sql: &str) -> Expr {
	pg_in(sql, &catalog())
}

fn pg_in(sql: &str, catalog: &dyn BinderCatalog) -> Expr {
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Insert(s) => lower_pgsql_insert(s, catalog),
		surrealdb_pgsql::ast::stmt::Statement::Update(s) => lower_pgsql_update(s, catalog),
		surrealdb_pgsql::ast::stmt::Statement::Delete(s) => lower_pgsql_delete(s, catalog),
		surrealdb_pgsql::ast::stmt::Statement::Copy(_)
		| surrealdb_pgsql::ast::stmt::Statement::Select(_)
		| surrealdb_pgsql::ast::stmt::Statement::Truncate(_)
		| surrealdb_pgsql::ast::stmt::Statement::CreateTable(_)
		| surrealdb_pgsql::ast::stmt::Statement::CreateIndex(_)
		| surrealdb_pgsql::ast::stmt::Statement::CreateView(_)
		| surrealdb_pgsql::ast::stmt::Statement::AlterTable(_)
		| surrealdb_pgsql::ast::stmt::Statement::CreateSchema(_)
		| surrealdb_pgsql::ast::stmt::Statement::CreateExtension(_)
		| surrealdb_pgsql::ast::stmt::Statement::CreateType(_)
		| surrealdb_pgsql::ast::stmt::Statement::AlterType(_)
		| surrealdb_pgsql::ast::stmt::Statement::CreateDomain(_)
		| surrealdb_pgsql::ast::stmt::Statement::AlterDomain(_)
		| surrealdb_pgsql::ast::stmt::Statement::CreateCompositeType(_)
		| surrealdb_pgsql::ast::stmt::Statement::Drop(_)
		| surrealdb_pgsql::ast::stmt::Statement::Transaction(_)
		| surrealdb_pgsql::ast::stmt::Statement::Set(_)
		| surrealdb_pgsql::ast::stmt::Statement::Show(_)
		| surrealdb_pgsql::ast::stmt::Statement::Reset(_)
		| surrealdb_pgsql::ast::stmt::Statement::Explain(_)
		| surrealdb_pgsql::ast::stmt::Statement::Prepare(_)
		| surrealdb_pgsql::ast::stmt::Statement::Execute(_)
		| surrealdb_pgsql::ast::stmt::Statement::Deallocate(_)
		| surrealdb_pgsql::ast::stmt::Statement::CreatePropertyGraph(_)
		| surrealdb_pgsql::ast::stmt::Statement::DropPropertyGraph(_)
		| surrealdb_pgsql::ast::stmt::Statement::Empty => panic!("expected a write"),
	}
	.unwrap()
}

fn my(sql: &str) -> Expr {
	my_in(sql, &catalog())
}

fn my_in(sql: &str, catalog: &dyn BinderCatalog) -> Expr {
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Insert(s) => lower_mysql_insert(s, catalog),
		surrealdb_mysql::ast::stmt::Statement::Replace(s) => lower_mysql_replace(s, catalog),
		surrealdb_mysql::ast::stmt::Statement::Update(s) => lower_mysql_update(s, catalog),
		surrealdb_mysql::ast::stmt::Statement::Delete(s) => lower_mysql_delete(s, catalog),
		surrealdb_mysql::ast::stmt::Statement::Select(_)
		| surrealdb_mysql::ast::stmt::Statement::Truncate(_)
		| surrealdb_mysql::ast::stmt::Statement::CreateTable(_)
		| surrealdb_mysql::ast::stmt::Statement::DropTable(_)
		| surrealdb_mysql::ast::stmt::Statement::CreateDatabase(_)
		| surrealdb_mysql::ast::stmt::Statement::DropDatabase(_)
		| surrealdb_mysql::ast::stmt::Statement::CreateIndex(_)
		| surrealdb_mysql::ast::stmt::Statement::DropIndex(_)
		| surrealdb_mysql::ast::stmt::Statement::CreateView(_)
		| surrealdb_mysql::ast::stmt::Statement::DropView(_)
		| surrealdb_mysql::ast::stmt::Statement::AlterTable(_)
		| surrealdb_mysql::ast::stmt::Statement::CreateUser(_)
		| surrealdb_mysql::ast::stmt::Statement::DropUser(_)
		| surrealdb_mysql::ast::stmt::Statement::Grant(_)
		| surrealdb_mysql::ast::stmt::Statement::Revoke(_)
		| surrealdb_mysql::ast::stmt::Statement::LockTables(_)
		| surrealdb_mysql::ast::stmt::Statement::UnlockTables(_)
		| surrealdb_mysql::ast::stmt::Statement::Prepare(_)
		| surrealdb_mysql::ast::stmt::Statement::Execute(_)
		| surrealdb_mysql::ast::stmt::Statement::Deallocate(_)
		| surrealdb_mysql::ast::stmt::Statement::Use(_)
		| surrealdb_mysql::ast::stmt::Statement::Set(_)
		| surrealdb_mysql::ast::stmt::Statement::Show(_)
		| surrealdb_mysql::ast::stmt::Statement::Describe(_)
		| surrealdb_mysql::ast::stmt::Statement::Explain(_)
		| surrealdb_mysql::ast::stmt::Statement::Transaction(_) => panic!("expected a write"),
	}
	.unwrap()
}

fn insert(expr: Expr) -> InsertStatement {
	let Expr::Insert(insert) = expr else {
		panic!("expected INSERT, got {expr:?}");
	};
	*insert
}

/// The `column = value` pairs of the first row of an `INSERT ... VALUES`.
fn row(expr: Expr) -> Vec<(String, String)> {
	let Data::ValuesExpression(rows) = insert(expr).data else {
		panic!("expected VALUES");
	};
	rows[0].iter().map(|(column, value)| (column.to_sql(), value.to_sql())).collect()
}

/// The `column = value` pairs of the update that `INSERT` runs for a row that exists.
fn on_conflict(expr: Expr) -> Vec<(String, String)> {
	let Some(Data::UpdateExpression(assignments)) = insert(expr).update else {
		panic!("expected an update");
	};
	assignments.iter().map(|a| (a.place.to_sql(), a.value.to_sql())).collect()
}

/// The `column = value` pairs of an `UPDATE`.
fn set(expr: Expr) -> Vec<(String, String)> {
	let Expr::Update(update) = expr else {
		panic!("expected UPDATE, got {expr:?}");
	};
	let Some(Data::SetExpression(assignments)) = update.data else {
		panic!("expected SET");
	};
	assignments.iter().map(|a| (a.place.to_sql(), a.value.to_sql())).collect()
}

/// The `WHERE` clause of an `UPDATE` or a `DELETE`.
fn cond(expr: Expr) -> String {
	match expr {
		Expr::Update(update) => update.cond.expect("a WHERE clause").0.to_sql(),
		Expr::Delete(delete) => delete.cond.expect("a WHERE clause").0.to_sql(),
		other @ Expr::Literal(_)
		| other @ Expr::Param(_)
		| other @ Expr::Idiom(_)
		| other @ Expr::Table(_)
		| other @ Expr::Mock(_)
		| other @ Expr::Block(_)
		| other @ Expr::Constant(_)
		| other @ Expr::Prefix {
			..
		}
		| other @ Expr::Postfix {
			..
		}
		| other @ Expr::Binary {
			..
		}
		| other @ Expr::FunctionCall(_)
		| other @ Expr::Closure(_)
		| other @ Expr::Break
		| other @ Expr::Continue
		| other @ Expr::Return(_)
		| other @ Expr::Throw(_)
		| other @ Expr::IfElse(_)
		| other @ Expr::Select(_)
		| other @ Expr::Create(_)
		| other @ Expr::Upsert(_)
		| other @ Expr::Relate(_)
		| other @ Expr::Insert(_)
		| other @ Expr::Define(_)
		| other @ Expr::Remove(_)
		| other @ Expr::Rebuild(_)
		| other @ Expr::Alter(_)
		| other @ Expr::Info(_)
		| other @ Expr::Foreach(_)
		| other @ Expr::Let(_)
		| other @ Expr::Sleep(_)
		| other @ Expr::Explain {
			..
		} => panic!("expected UPDATE or DELETE, got {other:?}"),
	}
}

/// The items of the `SELECT` that is the source of an `INSERT`, and its `WHERE` clause.
fn source(expr: Expr) -> String {
	let Data::SingleExpression(Expr::Select(select)) = insert(expr).data else {
		panic!("expected a SELECT source");
	};
	let Fields::Select(fields) = &select.fields else {
		panic!("expected a projection list");
	};
	let mut text: Vec<String> = fields.iter().map(|f| f.to_sql()).collect();
	if let Some(cond) = &select.cond {
		text.push(format!("WHERE {}", cond.0.to_sql()));
	}
	text.join(" | ")
}

/// The projection of the `RETURNING` clause of a write.
fn returned(expr: Expr) -> String {
	let output = match expr {
		Expr::Update(update) => update.output,
		Expr::Delete(delete) => delete.output,
		Expr::Insert(insert) => insert.output,
		other @ Expr::Literal(_)
		| other @ Expr::Param(_)
		| other @ Expr::Idiom(_)
		| other @ Expr::Table(_)
		| other @ Expr::Mock(_)
		| other @ Expr::Block(_)
		| other @ Expr::Constant(_)
		| other @ Expr::Prefix {
			..
		}
		| other @ Expr::Postfix {
			..
		}
		| other @ Expr::Binary {
			..
		}
		| other @ Expr::FunctionCall(_)
		| other @ Expr::Closure(_)
		| other @ Expr::Break
		| other @ Expr::Continue
		| other @ Expr::Return(_)
		| other @ Expr::Throw(_)
		| other @ Expr::IfElse(_)
		| other @ Expr::Select(_)
		| other @ Expr::Create(_)
		| other @ Expr::Upsert(_)
		| other @ Expr::Relate(_)
		| other @ Expr::Define(_)
		| other @ Expr::Remove(_)
		| other @ Expr::Rebuild(_)
		| other @ Expr::Alter(_)
		| other @ Expr::Info(_)
		| other @ Expr::Foreach(_)
		| other @ Expr::Let(_)
		| other @ Expr::Sleep(_)
		| other @ Expr::Explain {
			..
		} => panic!("expected a write, got {other:?}"),
	};
	let Some(Output::Fields(fields)) = output else {
		panic!("expected a projection");
	};
	fields.to_sql()
}

/// The record of `table` the value `value` names when it is written to a link column.
fn written(table: &str, value: &str) -> String {
	format!(
		"IF {value} = '' OR !(type::is_int({value}) OR type::is_i32({value}) OR type::is_i16({value}) OR type::is_i8({value}) OR type::is_u32({value}) OR type::is_u16({value}) OR type::is_u8({value}) OR type::is_string({value}) OR type::is_uuid({value}) OR type::is_array({value}) OR type::is_object({value})) THEN {value} ELSE type::record('{table}', {value}) END"
	)
}

/// The record of `table` the value `value` names when it is compared with a link column.
fn compared(table: &str, value: &str) -> String {
	format!(
		"(IF {value} = '' OR !(type::is_int({value}) OR type::is_i32({value}) OR type::is_i16({value}) OR type::is_i8({value}) OR type::is_u32({value}) OR type::is_u16({value}) OR type::is_u8({value}) OR type::is_string({value}) OR type::is_uuid({value}) OR type::is_array({value}) OR type::is_object({value}) OR type::is_record({value})) THEN NULL ELSE type::record('{table}', {value}) END)"
	)
}

fn pair(column: &str, value: &str) -> (String, String) {
	(column.to_string(), value.to_string())
}

#[test]
fn an_int_literal_written_to_a_link_is_the_record_literal() {
	let values = row(pg("INSERT INTO emp (id, name, dept) VALUES (8, 'Sam', 1)"));
	assert_eq!(values, [pair("id", "8"), pair("name", "'Sam'"), pair("dept", "dept:1")]);
}

#[test]
fn a_string_literal_written_to_a_link_is_the_record_literal() {
	let values = row(pg("INSERT INTO emp (id, name, dept) VALUES (8, 'Sam', 'eng')"));
	assert_eq!(values[2], pair("dept", "dept:eng"));
}

#[test]
fn a_string_that_looks_like_a_record_id_is_a_string_key() {
	let values = row(pg("INSERT INTO emp (id, name, dept) VALUES (8, 'Sam', 'dept:1')"));
	assert_eq!(values[2], pair("dept", "dept:`dept:1`"));
}

#[test]
fn a_parameter_written_to_a_link_names_the_record_of_the_table_unless_it_is_a_record() {
	let values = row(pg("INSERT INTO emp (id, name, dept) VALUES (8, 'Sam', $1)"));
	assert_eq!(values[2], pair("dept", &written("dept", "$_1")));
	let values = row(my("INSERT INTO emp (id, name, dept) VALUES (8, 'Sam', ?)"));
	assert_eq!(values[2], pair("dept", &written("dept", "$_1")));
}

#[test]
fn an_expression_written_to_a_link_is_evaluated_once() {
	let values = row(pg("INSERT INTO emp (id, name, dept) VALUES (8, 'Sam', abs($1))"));
	assert_eq!(
		values[2].1,
		"{ LET $_link = { LET $__sql_binding_1 = [$_1]; IF $__sql_binding_1[0] = NULL OR $__sql_binding_1[0] = NONE THEN NULL ELSE math::abs($__sql_binding_1[0]) END; }; IF $_link = '' OR !(type::is_int($_link) OR type::is_i32($_link) OR type::is_i16($_link) OR type::is_i8($_link) OR type::is_u32($_link) OR type::is_u16($_link) OR type::is_u8($_link) OR type::is_string($_link) OR type::is_uuid($_link) OR type::is_array($_link) OR type::is_object($_link)) THEN $_link ELSE type::record('dept', $_link) END; }"
	);
}

#[test]
fn a_value_that_is_no_key_is_written_as_it_is() {
	for (value, rendered) in
		[("NULL", "NULL"), ("1.5", "1.5dec"), ("''", "''"), ("true", "true"), ("DEFAULT", "NONE")]
	{
		let values =
			row(pg(&format!("INSERT INTO emp (id, name, dept) VALUES (8, 'Sam', {value})")));
		assert_eq!(values[2], pair("dept", rendered), "{value}");
	}
}

#[test]
fn a_record_written_to_a_link_is_never_retargeted() {
	let wrapper = link_value("dept", Expr::Param(Param::from("v".to_string()))).to_sql();
	assert_eq!(
		wrapper,
		"IF $v = '' OR !(type::is_int($v) OR type::is_i32($v) OR type::is_i16($v) OR type::is_i8($v) OR type::is_u32($v) OR type::is_u16($v) OR type::is_u8($v) OR type::is_string($v) OR type::is_uuid($v) OR type::is_array($v) OR type::is_object($v)) THEN $v ELSE type::record('dept', $v) END"
	);
	assert!(!wrapper.contains("is_record"), "a record is left as it is: {wrapper}");
}

#[test]
fn a_value_is_converted_by_what_it_is_when_it_is_written() {
	let none = Expr::Literal(Literal::None);
	assert_eq!(link_value("dept", none.clone()), none);
	let null = Expr::Literal(Literal::Null);
	assert_eq!(link_value("dept", null.clone()), null);
	let record = Expr::Literal(Literal::RecordId(RecordIdLit {
		table: TableName::from("other"),
		key: RecordIdKeyLit::Number(9),
	}));
	assert_eq!(link_value("dept", record.clone()), record);
	assert_eq!(link_value("dept", Expr::Literal(Literal::Integer(3))).to_sql(), "dept:3");
	assert_eq!(link_value("dept", Expr::Literal(Literal::Bool(true))).to_sql(), "true");
}

#[test]
fn the_columns_of_an_insert_are_matched_by_name_not_by_position() {
	let values = row(pg("INSERT INTO emp (dept, name, id) VALUES (1, 'Sam', 8)"));
	assert_eq!(values, [pair("dept", "dept:1"), pair("name", "'Sam'"), pair("id", "8")]);
	let values = row(pg("INSERT INTO emp (mentor, dept) VALUES (7, 2)"));
	assert_eq!(values, [pair("mentor", "emp:7"), pair("dept", "dept:2")]);
}

#[test]
fn a_column_that_is_no_link_is_written_as_it_is() {
	let values = row(pg("INSERT INTO emp (id, name, age) VALUES (8, 'Sam', 1)"));
	assert_eq!(values, [pair("id", "8"), pair("name", "'Sam'"), pair("age", "1")]);
}

#[test]
fn a_table_the_catalog_does_not_describe_is_written_as_it_is() {
	let values = row(pg("INSERT INTO visitor (id, dept) VALUES (8, 1)"));
	assert_eq!(values, [pair("id", "8"), pair("dept", "1")]);
	let values = row(pg_in("INSERT INTO emp (id, dept) VALUES (8, 1)", &EmptyCatalog));
	assert_eq!(values, [pair("id", "8"), pair("dept", "1")]);
}

#[test]
fn the_id_column_is_never_a_link() {
	let catalog = MemoryCatalog::new().with_link("emp", "id", "dept");
	let values = row(pg_in("INSERT INTO emp (id, name) VALUES (8, 'Sam')", &catalog));
	assert_eq!(values[0], pair("id", "8"));
	let assignments = set(pg_in("UPDATE emp SET id = 5", &catalog));
	assert_eq!(assignments, [pair("id", "5")]);
}

#[test]
fn an_insert_select_converts_the_item_written_to_a_link_by_position() {
	let text = source(pg("INSERT INTO emp (id, name, dept) SELECT id, name, dept_id FROM staging"));
	assert_eq!(
		text,
		format!("record::id(id) AS id | name | ({}) AS dept", written("dept", "dept_id"))
	);
}

#[test]
fn an_insert_select_item_keeps_its_alias() {
	let text =
		source(pg("INSERT INTO emp (id, name, dept) SELECT id, name, dept_id AS foo FROM staging"));
	assert_eq!(
		text,
		format!("record::id(id) AS id | name | ({}) AS foo", written("dept", "dept_id"))
	);
}

#[test]
fn an_insert_select_with_a_wildcard_converts_nothing() {
	let text = source(pg("INSERT INTO emp (id, name, dept) SELECT * FROM staging"));
	assert_eq!(text, "* | record::id(id) AS id");
}

#[test]
fn the_where_clause_of_an_insert_select_compares_a_link_with_the_record() {
	let text = source(pg(
		"INSERT INTO emp (id, name, dept) SELECT id, name, dept FROM emp WHERE dept = 3",
	));
	assert!(text.ends_with("| WHERE dept = (dept:3)"), "{text}");
}

#[test]
fn a_mysql_insert_select_converts_the_item_written_to_a_link_by_position() {
	let expected =
		format!("record::id(id) AS id | name | ({}) AS dept", written("dept", "dept_id"));
	let text = source(my("INSERT INTO emp (id, name, dept) SELECT id, name, dept_id FROM staging"));
	assert_eq!(text, expected);
	let text =
		source(my("REPLACE INTO emp (id, name, dept) SELECT id, name, dept_id FROM staging"));
	assert_eq!(text, expected);
	let text = source(my("INSERT INTO emp (id, name, dept) SELECT * FROM staging"));
	assert_eq!(text, "* | record::id(id) AS id");
}

#[test]
fn on_conflict_do_update_converts_the_value_assigned_to_a_link() {
	let assignments = on_conflict(pg(
		"INSERT INTO emp (id, dept) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET dept = $1",
	));
	assert_eq!(assignments, [pair("dept", &written("dept", "$_1"))]);
	let assignments = on_conflict(pg(
		"INSERT INTO emp (id, dept) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET dept = 4, name = 'x'",
	));
	assert_eq!(assignments, [pair("dept", "dept:4"), pair("name", "'x'")]);
}

#[test]
fn an_excluded_link_keeps_the_record_it_already_is() {
	let assignments = on_conflict(pg(
		"INSERT INTO emp (id, dept) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET dept = EXCLUDED.dept, name = EXCLUDED.name",
	));
	assert_eq!(assignments[0], pair("dept", &written("dept", "$input.dept")));
	assert_eq!(assignments[1], pair("name", "$input.name"));
}

#[test]
fn on_duplicate_key_update_converts_the_value_assigned_to_a_link() {
	let assignments = on_conflict(my(
		"INSERT INTO emp (id, dept) VALUES (1, 2) ON DUPLICATE KEY UPDATE dept = ?, name = 'x'",
	));
	assert_eq!(assignments, [pair("dept", &written("dept", "$_1")), pair("name", "'x'")]);
	let assignments = on_conflict(my(
		"INSERT INTO emp (id, dept) VALUES (1, 2) ON DUPLICATE KEY UPDATE dept = VALUES(dept)",
	));
	assert_eq!(assignments, [pair("dept", &written("dept", "$input.dept"))]);
	let values = row(my("INSERT INTO emp (id, name, dept) VALUES (8, 'Sam', 1)"));
	assert_eq!(values[2], pair("dept", "dept:1"));
}

#[test]
fn replace_converts_the_values_written_to_links() {
	let sql = "REPLACE INTO emp (id, name, dept) VALUES (8, 'Sam', 1)";
	let values = row(my(sql));
	assert_eq!(values[2], pair("dept", "dept:1"));
	let replaced = on_conflict(my(sql));
	assert_eq!(
		replaced,
		[
			pair("id", "$input.id"),
			pair("name", "$input.name"),
			pair("dept", &written("dept", "$input.dept"))
		]
	);
}

#[test]
fn update_converts_the_value_assigned_to_a_link() {
	let assignments = set(pg("UPDATE emp SET dept = 2, name = 'x' WHERE id = 1"));
	assert_eq!(assignments, [pair("dept", "dept:2"), pair("name", "'x'")]);
	let assignments = set(pg("UPDATE emp SET dept = $1"));
	assert_eq!(assignments, [pair("dept", &written("dept", "$_1"))]);
	let assignments = set(my("UPDATE emp SET dept = 2, name = 'x'"));
	assert_eq!(assignments, [pair("dept", "dept:2"), pair("name", "'x'")]);
	let assignments = set(my("UPDATE emp SET dept = NULL"));
	assert_eq!(assignments, [pair("dept", "NULL")]);
}

#[test]
fn a_link_assigned_from_a_column_is_left_as_the_record_it_is() {
	let assignments = set(pg("UPDATE emp SET dept = mentor"));
	assert_eq!(assignments, [pair("dept", &written("dept", "mentor"))]);
}

#[test]
fn a_mysql_update_over_a_join_converts_nothing() {
	let stmts = surrealdb_mysql::parse_statements(
		"UPDATE emp JOIN dept ON emp.dept = dept.id SET dept = 2 WHERE emp.dept = 1",
	)
	.unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Update(s) = &stmts[0] else {
		panic!("expected Update");
	};
	assert!(matches!(
		lower_mysql_update(s, &catalog()),
		Err(crate::PlanError::UnsupportedStatement(_))
	));
}

#[test]
fn a_where_clause_compares_a_link_with_the_record() {
	for sql in [
		"UPDATE emp SET name = 'x' WHERE dept = 1",
		"UPDATE emp SET name = 'x' WHERE 1 = dept",
		"UPDATE emp SET name = 'x' WHERE emp.dept = 1",
		"DELETE FROM emp WHERE dept = 1",
	] {
		assert_eq!(cond(pg(sql)), "dept = (dept:1)");
	}
	check_fragments(
		&cond(pg("UPDATE emp SET name = 'x' WHERE dept = $1")),
		&["dept", "type::record('dept', $_1)", "$_1 ?? NULL", "THEN NULL"],
		&[],
	);
	assert_eq!(
		cond(my("DELETE FROM emp WHERE dept = ?")),
		format!("dept = {}", compared("dept", "$_1"))
	);
}

#[test]
fn a_where_clause_compares_a_link_that_is_not_equal_with_the_record() {
	for sql in [
		"UPDATE emp SET name = 'x' WHERE dept <> 1",
		"DELETE FROM emp WHERE dept <> 1",
		"UPDATE emp SET name = 'x' WHERE NOT (dept = 1) OR dept = 2",
	] {
		check_fragments(&cond(pg(sql)), &["dept:1", "THEN NULL"], &[]);
	}
	assert_eq!(cond(my("UPDATE emp SET name = 'x' WHERE dept != 1")), "dept != (dept:1)");
}

#[test]
fn a_where_clause_compares_a_link_in_a_list_with_the_records() {
	assert_eq!(cond(pg("DELETE FROM emp WHERE dept IN (1, 2)")), "dept INSIDE [dept:1, dept:2]");
	assert_eq!(cond(pg("DELETE FROM emp WHERE dept IN (1)")), "dept = (dept:1)");
	for sql in [
		"DELETE FROM emp WHERE dept NOT IN (1, 2)",
		"DELETE FROM emp WHERE dept NOT IN (1)",
		"DELETE FROM emp WHERE dept IN (1, $1)",
	] {
		check_fragments(&cond(pg(sql)), &["dept:1", "THEN NULL"], &[]);
	}
	assert_eq!(
		cond(my("DELETE FROM emp WHERE dept NOT IN (1, 2)")),
		"dept NOTINSIDE [dept:1, dept:2]"
	);
}

#[test]
fn a_where_clause_compares_a_link_with_a_column_by_the_record_the_column_names() {
	check_fragments(
		&cond(pg("DELETE FROM emp WHERE dept = age")),
		&["dept", "type::record('dept', age)", "age ?? NULL", "THEN NULL"],
		&[],
	);
}

#[test]
fn a_where_clause_compares_a_link_as_stored_with_the_id_it_links_to() {
	for sql in ["DELETE FROM emp WHERE mentor = id", "DELETE FROM emp WHERE id = mentor"] {
		let text = cond(pg(sql));
		check_fragments(&text, &["mentor", "id"], &["type::record", "record::id"]);
	}
	assert_eq!(cond(my("DELETE FROM emp WHERE mentor = id")), "mentor = id");
}

#[test]
fn a_where_clause_compares_a_link_with_the_id_of_another_table_by_its_key() {
	check_fragments(
		&cond(pg("DELETE FROM emp WHERE dept = id")),
		&["dept", "id", "type::record("],
		&[],
	);
}

#[test]
fn a_where_clause_leaves_a_comparison_that_names_no_link_as_it_was() {
	assert_eq!(cond(pg("DELETE FROM emp WHERE age = 1")), "age = 1");
	assert_eq!(
		cond(pg("DELETE FROM emp WHERE dept = NULL")),
		"IF dept = NULL OR dept = NONE OR NULL = NULL OR NULL = NONE THEN NULL ELSE dept = NULL END"
	);
	assert_eq!(cond(pg("DELETE FROM emp WHERE dept = 1.5")), "dept = 1.5dec");
	assert_eq!(cond(pg("DELETE FROM emp WHERE dept = ''")), "dept = ''");
	assert_eq!(cond(pg("DELETE FROM visitor WHERE dept = 1")), "dept = 1");
	assert_eq!(cond(pg("DELETE FROM emp WHERE id = 3")), "id = (emp:3)");
}

#[test]
fn the_projection_of_a_deleted_row_is_not_converted() {
	let Expr::Delete(delete) = pg("DELETE FROM emp WHERE dept = 1 RETURNING dept") else {
		panic!("expected DELETE");
	};
	let Output::Fields(fields) = delete.output.expect("an output") else {
		panic!("expected fields");
	};
	assert_eq!(fields.to_sql(), "$before.dept AS dept");
}

fn links_of(recorder: &RecordingCatalog) -> BTreeSet<(Option<String>, String, String)> {
	recorder.links_asked()
}

#[test]
fn an_insert_asks_the_catalog_about_each_assigned_column() {
	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_in(
		"INSERT INTO emp (id, name, dept) VALUES (1, 'a', 2) ON CONFLICT (id) DO UPDATE SET dept = EXCLUDED.dept, mentor = 3",
		&recorder,
	);
	assert_eq!(
		links_of(&recorder),
		asked(&[("emp", "id"), ("emp", "name"), ("emp", "dept"), ("emp", "mentor")])
	);

	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_in("INSERT INTO visitor (id, tag) VALUES (1, 'a')", &recorder);
	assert_eq!(links_of(&recorder), asked(&[("visitor", "id"), ("visitor", "tag")]));

	let recorder = RecordingCatalog::new(catalog());
	let _ = my_in("REPLACE INTO emp (id, dept) VALUES (1, 2)", &recorder);
	assert_eq!(links_of(&recorder), asked(&[("emp", "id"), ("emp", "dept")]));

	let recorder = RecordingCatalog::new(catalog());
	let _ = my_in(
		"INSERT INTO emp (id, dept) VALUES (1, 2) ON DUPLICATE KEY UPDATE name = 'x'",
		&recorder,
	);
	assert_eq!(links_of(&recorder), asked(&[("emp", "id"), ("emp", "dept"), ("emp", "name")]));
}

#[test]
fn an_insert_select_asks_the_catalog_about_each_column_even_with_a_wildcard() {
	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_in("INSERT INTO emp (id, dept) SELECT * FROM staging", &recorder);
	assert_eq!(links_of(&recorder), asked(&[("emp", "id"), ("emp", "dept")]));
}

#[test]
fn an_update_asks_the_catalog_about_each_assigned_column_and_each_compared_column() {
	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_in("UPDATE emp SET name = 'x', dept = 3 WHERE mentor = 1", &recorder);
	assert_eq!(links_of(&recorder), asked(&[("emp", "name"), ("emp", "dept"), ("emp", "mentor")]));

	let recorder = RecordingCatalog::new(catalog());
	let _ = my_in("UPDATE emp SET name = 'x' WHERE dept IN (1, 2)", &recorder);
	assert_eq!(links_of(&recorder), asked(&[("emp", "name"), ("emp", "dept")]));
}

#[test]
fn a_delete_asks_the_catalog_about_each_compared_column() {
	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_in("DELETE FROM emp WHERE dept = 1 AND age > 3", &recorder);
	assert!(links_of(&recorder).contains(&(None, "emp".to_string(), "dept".to_string())));

	let recorder = RecordingCatalog::new(catalog());
	let _ = my_in("DELETE FROM emp WHERE dept = age", &recorder);
	assert_eq!(links_of(&recorder), asked(&[("emp", "dept"), ("emp", "age")]));
}

#[test]
fn a_statement_over_a_schema_qualified_table_asks_about_its_schema() {
	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_in("UPDATE public.emp SET dept = 1", &recorder);
	assert_eq!(
		links_of(&recorder),
		[(Some("public".to_string()), "emp".to_string(), "dept".to_string())].into()
	);
}

#[test]
fn a_link_column_of_a_qualified_table_is_still_a_link() {
	let values = row(pg("INSERT INTO public.emp (id, name, dept) VALUES (8, 'Sam', 1)"));
	assert_eq!(values[2], pair("dept", "dept:1"));
	let values =
		row(pg("INSERT INTO \"public\".\"emp\" (id, name, dept) VALUES (8, 'Sam', 'eng')"));
	assert_eq!(values[2], pair("dept", "dept:eng"));
	let values = row(my("INSERT INTO shop.emp (id, name, dept) VALUES (8, 'Sam', 1)"));
	assert_eq!(values[2], pair("dept", "dept:1"));
	let values = row(my("REPLACE INTO `shop`.`emp` (id, name, dept) VALUES (8, 'Sam', 1)"));
	assert_eq!(values[2], pair("dept", "dept:1"));
	assert_eq!(set(pg("UPDATE public.emp SET dept = 1")), set(pg("UPDATE emp SET dept = 1")),);
	assert_eq!(set(my("UPDATE shop.emp SET dept = 1")), set(my("UPDATE emp SET dept = 1")),);
	assert_eq!(
		cond(pg("DELETE FROM public.emp WHERE dept = 1")),
		cond(pg("DELETE FROM emp WHERE dept = 1")),
	);
	assert_eq!(
		cond(my("DELETE FROM shop.emp WHERE dept IN (1, 2)")),
		cond(my("DELETE FROM emp WHERE dept IN (1, 2)")),
	);
	assert_eq!(
		on_conflict(pg(
			"INSERT INTO public.emp (id, dept) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET dept = 3"
		)),
		on_conflict(pg(
			"INSERT INTO emp (id, dept) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET dept = 3"
		)),
	);
}

#[test]
fn a_plain_field_that_differs_from_a_link_only_in_case_is_written_as_it_is() {
	let catalog = MemoryCatalog::new().with_link("emp", "Dept", "dept");
	let values = row(pg_in("INSERT INTO emp (id, dept) VALUES (8, 1)", &catalog));
	assert_eq!(values, [pair("id", "8"), pair("dept", "1")]);
	let values = row(pg_in("INSERT INTO emp (id, DEPT) VALUES (8, 1)", &catalog));
	assert_eq!(values, [pair("id", "8"), pair("dept", "1")]);
	let values = row(my_in("INSERT INTO emp (id, dept) VALUES (8, 1)", &catalog));
	assert_eq!(values, [pair("id", "8"), pair("dept", "1")]);
	let values = row(my_in("REPLACE INTO emp (id, dept) VALUES (8, 1)", &catalog));
	assert_eq!(values, [pair("id", "8"), pair("dept", "1")]);
	assert_eq!(set(pg_in("UPDATE emp SET dept = 1", &catalog)), [pair("dept", "1")]);
	assert_eq!(set(my_in("UPDATE emp SET dept = 1", &catalog)), [pair("dept", "1")]);
	assert_eq!(cond(pg_in("DELETE FROM emp WHERE dept = 1", &catalog)), "dept = 1");
	assert_eq!(cond(my_in("DELETE FROM emp WHERE dept = 1", &catalog)), "dept = 1");
	assert_eq!(
		on_conflict(pg_in(
			"INSERT INTO emp (id, dept) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET dept = 3",
			&catalog
		)),
		[pair("dept", "3")]
	);
}

#[test]
fn a_link_written_by_the_name_it_is_declared_by_is_converted() {
	let catalog =
		MemoryCatalog::new().with_link("emp", "Dept", "dept").with_link("emp", "DEPT", "region");
	let values = row(pg_in("INSERT INTO emp (id, \"Dept\") VALUES (8, 1)", &catalog));
	assert_eq!(values, [pair("id", "8"), pair("Dept", "dept:1")]);
	let values = row(pg_in("INSERT INTO emp (id, \"DEPT\") VALUES (8, 1)", &catalog));
	assert_eq!(values, [pair("id", "8"), pair("DEPT", "region:1")]);
	let values = row(pg_in("INSERT INTO emp (id, dept) VALUES (8, 1)", &catalog));
	assert_eq!(values, [pair("id", "8"), pair("dept", "1")]);
	assert_eq!(set(pg_in("UPDATE emp SET \"Dept\" = 1", &catalog)), [pair("Dept", "dept:1")]);
	let values = row(my_in("INSERT INTO emp (id, `Dept`) VALUES (8, 1)", &catalog));
	assert_eq!(values, [pair("id", "8"), pair("Dept", "dept:1")]);
	assert_eq!(set(my_in("UPDATE emp SET `DEPT` = 1", &catalog)), [pair("DEPT", "region:1")]);
}

#[test]
fn an_unquoted_postgres_name_is_written_as_the_lower_case_name_it_is_stored_under() {
	let values = row(pg("INSERT INTO emp (id, Dept) VALUES (8, 1)"));
	assert_eq!(values, [pair("id", "8"), pair("dept", "dept:1")]);
	let values = row(pg("INSERT INTO EMP (id, DEPT) VALUES (8, 1)"));
	assert_eq!(values, [pair("id", "8"), pair("dept", "dept:1")]);
}

#[test]
fn the_name_the_catalog_is_asked_about_is_the_name_the_column_is_written_by() {
	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_in("INSERT INTO EMP (id, Dept, \"Mentor\") VALUES (8, 1, 2)", &recorder);
	assert_eq!(recorder.links_asked(), asked(&[("emp", "id"), ("emp", "dept"), ("emp", "Mentor")]));

	let recorder = RecordingCatalog::new(catalog());
	let _ = my_in("INSERT INTO EMP (id, Dept) VALUES (8, 1)", &recorder);
	assert_eq!(recorder.links_asked(), asked(&[("EMP", "id"), ("EMP", "Dept")]));
}

/// `WHERE` clauses over `emp` in which a link column is an operand of an expression, with the text
/// of the clause that an `UPDATE` and a `DELETE` lower to. The `{k}` and `{m}` are the key of
/// `dept` and of `mentor`.
const KEYED: [(&str, &str); 14] = [
	("dept > 1", "{k} > 1"),
	("dept < 10", "{k} < 10"),
	("dept >= 1.5", "{k} >= 1.5dec"),
	("1.5 <= dept", "1.5dec <= {k}"),
	("dept > mentor", "{k} > {m}"),
	("dept BETWEEN 1 AND 3", "{k} >= 1 AND {k} <= 3"),
	("dept + 1 = 3", "{k} + 1 = 3"),
	("-dept = -2", "-{k} = -2"),
	("dept % 2 = 0", "{k} % 2 = 0"),
	(
		"abs(dept) = 2",
		"{ LET $__sql_binding_1 = [{k}]; IF $__sql_binding_1[0] = NULL OR $__sql_binding_1[0] = NONE THEN NULL ELSE math::abs($__sql_binding_1[0]) END; } = 2",
	),
	("mentor + dept = 3", "{m} + {k} = 3"),
	("age IN (dept, 3)", "age INSIDE [{k}, 3]"),
	("CASE WHEN dept > 1 THEN 1 ELSE 0 END = 1", "(IF {k} > 1 THEN 1 ELSE 0 END) = 1"),
	("COALESCE(dept, 0) = 2", "({k} ?? 0) = 2"),
];

fn keyed(text: &str) -> String {
	text.replace("{k}", KEY).replace("{m}", MENTOR_KEY)
}

/// Whether `text` reads the link column `dept` or `mentor` other than through its key.
fn reads_a_link_as_stored(text: &str) -> bool {
	let text = text.replace(KEY, "").replace(MENTOR_KEY, "");
	text.contains("dept") || text.contains("mentor")
}

#[test]
fn a_link_in_an_expression_of_a_where_clause_is_its_key() {
	for (clause, expected) in KEYED {
		for text in [
			cond(pg(&format!("UPDATE emp SET name = 'x' WHERE {clause}"))),
			cond(pg(&format!("DELETE FROM emp WHERE {clause}"))),
		] {
			assert!(text.contains(KEY) || text.contains(MENTOR_KEY), "{clause}: {text}");
			assert!(!reads_a_link_as_stored(&text), "{clause}: {text}");
		}
		let expected = keyed(expected);
		assert_eq!(cond(my(&format!("UPDATE emp SET name = 'x' WHERE {clause}"))), expected);
		assert_eq!(cond(my(&format!("DELETE FROM emp WHERE {clause}"))), expected);
	}
	check_fragments(
		&cond(pg("DELETE FROM emp WHERE dept NOT BETWEEN 1 AND 3")),
		&[KEY, "THEN NULL"],
		&[],
	);
	assert_eq!(cond(my("UPDATE emp AS e SET name = 'x' WHERE e.dept > 1")), format!("{KEY} > 1"));
}

#[test]
fn a_read_and_a_write_spell_the_key_of_a_link_alike() {
	for (clause, _) in KEYED {
		let writes = [
			cond(pg(&format!("UPDATE emp SET name = 'x' WHERE {clause}"))),
			cond(pg(&format!("DELETE FROM emp WHERE {clause}"))),
			cond(my(&format!("UPDATE emp SET name = 'x' WHERE {clause}"))),
		];
		let reads = [
			predicate(&pg_select(&format!("SELECT name FROM emp WHERE {clause}"))),
			predicate(&my_select(&format!("SELECT name FROM emp WHERE {clause}"))),
		];
		for text in writes.iter().chain(&reads) {
			assert!(text.contains(KEY) || text.contains(MENTOR_KEY), "{clause}: {text}");
			assert!(!reads_a_link_as_stored(text), "{clause}: {text}");
		}
	}
	for text in [
		cond(pg("DELETE FROM emp WHERE dept BETWEEN 1 AND 3")),
		predicate(&pg_select("SELECT name FROM emp WHERE dept BETWEEN 1 AND 3")),
	] {
		check_fragments(&text, &[KEY], &[]);
	}
	for item in ["abs(dept)", "upper(dept)"] {
		let read = projection(&pg_select(&format!("SELECT {item} AS n FROM emp")));
		let write = returned(pg(&format!("UPDATE emp SET name = 'x' RETURNING {item} AS n")));
		let function = if item.starts_with("abs") {
			"math::abs"
		} else {
			"string::uppercase"
		};
		for text in read.iter().chain(std::iter::once(&write)) {
			check_fragments(text, &[KEY, function, "THEN NULL"], &[]);
		}
	}
}

#[test]
fn a_link_that_stands_alone_in_a_write_is_the_link_as_stored() {
	assert_eq!(
		set(pg("UPDATE emp SET name = dept, note = emp.dept, age = (dept)")),
		[pair("name", "dept"), pair("note", "dept"), pair("age", "dept")]
	);
	assert_eq!(set(pg("UPDATE emp SET mentor = dept")), [pair("mentor", &written("emp", "dept"))]);
	assert_eq!(set(my("UPDATE emp SET name = dept")), [pair("name", "dept")]);
	assert_eq!(
		on_conflict(pg(
			"INSERT INTO emp (id, dept) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET name = dept, mentor = dept"
		)),
		[pair("name", "dept"), pair("mentor", &written("emp", "dept"))]
	);
	assert_eq!(
		on_conflict(my(
			"INSERT INTO emp (id, dept) VALUES (1, 2) ON DUPLICATE KEY UPDATE name = dept, mentor = dept"
		)),
		[pair("name", "dept"), pair("mentor", &written("emp", "dept"))]
	);
	assert_eq!(
		returned(pg(
			"UPDATE emp SET name = 'x' RETURNING dept, mentor AS m, emp.dept AS d, (dept) AS p"
		)),
		"dept, mentor AS m, dept AS d, dept AS p"
	);
	assert_eq!(returned(pg("INSERT INTO emp (id, dept) VALUES (1, 2) RETURNING dept")), "dept");
	assert_eq!(
		returned(pg("DELETE FROM emp WHERE id = 1 RETURNING dept, mentor AS m")),
		"$before.dept AS dept, $before.mentor AS m"
	);
	assert_eq!(cond(pg("DELETE FROM emp WHERE dept IS NULL")), "dept = NULL OR dept = NONE");
	assert_eq!(
		cond(pg("DELETE FROM emp WHERE emp.mentor IS NOT NULL")),
		"!(mentor = NULL OR mentor = NONE)"
	);
	assert_eq!(cond(pg("DELETE FROM emp WHERE (dept) IS NULL")), "dept = NULL OR dept = NONE");
	assert_eq!(cond(my("DELETE FROM emp WHERE dept IS NULL")), "dept = NULL");
	assert_eq!(cond(my("UPDATE emp SET name = 'x' WHERE mentor IS NOT NULL")), "mentor != NULL");
}

#[test]
fn a_link_in_an_expression_of_a_value_or_a_projection_is_its_key() {
	for text in [
		set(pg("UPDATE emp SET age = dept + 1"))[0].1.clone(),
		on_conflict(pg(
			"INSERT INTO emp (id, dept) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET age = dept * 2",
		))[0]
			.1
			.clone(),
		returned(pg("UPDATE emp SET name = 'x' RETURNING dept + 1 AS n")),
	] {
		check_fragments(&text, &[KEY, "THEN NULL"], &[]);
		assert!(!reads_a_link_as_stored(&text), "{text}");
	}
	let deleted = returned(pg("DELETE FROM emp WHERE id = 1 RETURNING dept + 1 AS n"));
	check_fragments(&deleted, &["record::id($before.dept)", "AS n", "THEN NULL"], &[]);
	assert_eq!(set(my("UPDATE emp SET age = dept + 1")), [pair("age", &format!("{KEY} + 1"))]);
}

#[test]
fn a_comparison_of_a_deleted_row_that_is_not_converted_compares_the_key() {
	check_fragments(
		&returned(pg("DELETE FROM emp WHERE id = 1 RETURNING dept > 1 AS n")),
		&["record::id($before.dept)", "AS n", "THEN NULL"],
		&[],
	);
}

#[test]
fn an_equality_of_a_link_is_converted_as_it_was() {
	check_fragments(
		&cond(pg("DELETE FROM emp WHERE dept = mentor")),
		&["type::record('dept', mentor)", "THEN NULL"],
		&[],
	);
	check_fragments(
		&cond(pg("DELETE FROM emp WHERE age <> dept")),
		&["type::record('dept', age)", "THEN NULL"],
		&[],
	);
	assert_eq!(
		cond(my("DELETE FROM emp WHERE dept IN (1, mentor)")),
		format!("dept INSIDE [dept:1, {}]", compared("dept", "mentor"))
	);
}

#[test]
fn a_column_that_is_no_link_is_read_as_it_is_in_an_expression() {
	for sql in ["DELETE FROM emp WHERE age + 1 > 3", "DELETE FROM visitor WHERE dept > 1"] {
		check_fragments(&cond(pg(sql)), &["THEN NULL"], &["record::id", "type::record"]);
	}
	check_fragments(
		&cond(pg("DELETE FROM emp WHERE id > 3")),
		&["record::id(id)", "THEN NULL"],
		&[],
	);
	for catalog in [MemoryCatalog::new(), MemoryCatalog::new().with_link("emp", "Dept", "dept")] {
		check_fragments(
			&cond(pg_in("DELETE FROM emp WHERE dept > 1", &catalog)),
			&["dept", "THEN NULL"],
			&["record::id", "type::record"],
		);
	}
}

#[test]
fn a_write_asks_the_catalog_about_each_column_an_expression_names() {
	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_in("UPDATE emp SET name = 'x' WHERE age + 1 > mentor", &recorder);
	assert_eq!(links_of(&recorder), asked(&[("emp", "name"), ("emp", "age"), ("emp", "mentor")]));

	let recorder = RecordingCatalog::new(catalog());
	let _ = my_in("DELETE FROM emp WHERE abs(age) = 2", &recorder);
	assert_eq!(links_of(&recorder), asked(&[("emp", "age")]));

	let recorder = RecordingCatalog::new(catalog());
	let _ = pg_in("DELETE FROM emp RETURNING age + 1 AS n", &recorder);
	assert_eq!(links_of(&recorder), asked(&[("emp", "age")]));
}

#[test]
fn a_quoted_upper_case_id_is_not_the_key_in_a_postgresql_write() {
	// The parser folds an unquoted `ID` to `id` and keeps a quoted `"ID"`, which is another column.
	let key = cond(pg("DELETE FROM emp WHERE id = 7"));
	assert!(key.contains("emp:7") || key.contains("record"), "{key}");
	assert_eq!(cond(pg("DELETE FROM emp WHERE ID = 7")), key);
	assert_eq!(cond(pg("DELETE FROM emp WHERE \"ID\" = 7")), "ID = 7");
	assert_eq!(cond(pg("DELETE FROM emp WHERE emp.\"ID\" = 7")), "ID = 7");
	assert_eq!(set(pg("UPDATE emp SET age = \"ID\"")), [pair("age", "ID")]);
	assert_ne!(set(pg("UPDATE emp SET age = id")), [pair("age", "id")]);
	assert_eq!(set(pg("UPDATE emp SET age = ID")), set(pg("UPDATE emp SET age = id")));
}

#[test]
fn an_id_is_the_key_whatever_its_case_in_a_mysql_write() {
	let key = cond(my("DELETE FROM emp WHERE id = 7"));
	assert!(key.contains("emp:7") || key.contains("record"), "{key}");
	for spelling in ["ID", "Id", "`ID`"] {
		assert_eq!(cond(my(&format!("DELETE FROM emp WHERE {spelling} = 7"))), key, "{spelling}");
	}
}

fn check_fragments(text: &str, required: &[&str], forbidden: &[&str]) {
	for fragment in required {
		assert!(text.contains(fragment), "missing {fragment}: {text}");
	}
	for fragment in forbidden {
		assert!(!text.contains(fragment), "unexpected {fragment}: {text}");
	}
}
