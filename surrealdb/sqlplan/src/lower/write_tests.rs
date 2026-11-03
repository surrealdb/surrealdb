// A catch-all arm in these tests reports the value a lowering produced instead of the one
// expected; naming every other variant would add nothing to that report.
#![allow(clippy::wildcard_enum_match_arm)]

#[allow(unused_imports)]
use surrealdb_expr::expr::data::Assignment;
#[allow(unused_imports)]
use surrealdb_expr::expr::statements::{DeleteStatement, InsertStatement, UpdateStatement};
use surrealdb_expr::expr::{Data, Expr, Field, Fields, Literal, Output, Part};
use surrealdb_types::ToSql;

use crate::catalog::EmptyCatalog;
use crate::lower::write::*;

/// The rendered projection of a statement's output.
fn output_sql(output: &Option<Output>) -> String {
	let Some(Output::Fields(fields)) = output else {
		panic!("expected Output::Fields, got {output:?}");
	};
	format!("RETURN {}", fields.to_sql())
}

fn pg_update(sql: &str) -> UpdateStatement {
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Update(update) = &stmts[0] else {
		panic!("Expected Update statement");
	};
	let Expr::Update(upd) = lower_pgsql_update(update, &EmptyCatalog).unwrap() else {
		panic!("Expected Expr::Update");
	};
	*upd
}

#[test]
fn a_literal_that_is_no_key_is_compared_with_the_key_in_the_condition_of_a_write() {
	for literal in ["3.0", "1.5", "true", "''", "NULL"] {
		for sql in [
			format!("UPDATE emp SET name = 'z' WHERE id = {literal}"),
			format!("UPDATE emp SET name = 'z' WHERE {literal} = id"),
		] {
			let text = where_sql(&pg_update(&sql).cond);
			assert!(text.contains("record::id(id)"), "{text}");
			assert!(!text.contains("type::record"), "{text}");
			if literal == "NULL" {
				assert!(text.contains("THEN NULL"), "{text}");
			}
		}
	}
	let text = where_sql(&pg_update("UPDATE emp SET name = 'z' WHERE id IN (1.5, 2)").cond);
	assert!(text.contains("record::id(id)") && text.contains("1.5dec"), "{text}");
}

fn pg_delete(sql: &str) -> Result<DeleteStatement, crate::PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Delete(delete) = &stmts[0] else {
		panic!("Expected Delete statement");
	};
	match lower_pgsql_delete(delete, &EmptyCatalog)? {
		Expr::Delete(del) => Ok(*del),
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
		| other @ Expr::Update(_)
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
		} => panic!("Expected Expr::Delete, got {other:?}"),
	}
}

fn pg_insert(sql: &str) -> InsertStatement {
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};
	let Expr::Insert(ins) = lower_pgsql_insert(insert, &EmptyCatalog).unwrap() else {
		panic!("Expected Expr::Insert");
	};
	*ins
}

fn my_update(sql: &str) -> UpdateStatement {
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Update(update) = &stmts[0] else {
		panic!("Expected Update statement");
	};
	let Expr::Update(upd) = lower_mysql_update(update, &EmptyCatalog).unwrap() else {
		panic!("Expected Expr::Update");
	};
	*upd
}

fn my_delete(sql: &str) -> DeleteStatement {
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Delete(delete) = &stmts[0] else {
		panic!("Expected Delete statement");
	};
	let Expr::Delete(del) = lower_mysql_delete(delete, &EmptyCatalog).unwrap() else {
		panic!("Expected Expr::Delete");
	};
	*del
}

fn my_insert(sql: &str) -> InsertStatement {
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};
	let Expr::Insert(ins) = lower_mysql_insert(insert, &EmptyCatalog).unwrap() else {
		panic!("Expected Expr::Insert");
	};
	*ins
}

fn where_sql(stmt_cond: &Option<surrealdb_expr::expr::Cond>) -> String {
	stmt_cond.as_ref().unwrap().0.to_sql()
}

const PARAM_RECORD: &str = "IF $_1 = '' OR !(type::is_int($_1) OR type::is_i32($_1) OR type::is_i16($_1) OR type::is_i8($_1) OR type::is_u32($_1) OR type::is_u16($_1) OR type::is_u8($_1) OR type::is_string($_1) OR type::is_uuid($_1) OR type::is_array($_1) OR type::is_object($_1) OR type::is_record($_1)) THEN NONE ELSE type::record('users', $_1) END";

// ============================================================================
// PostgreSQL DML Lowering Tests
// ============================================================================

#[test]
fn test_lower_pgsql_insert_values() {
	let sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	assert_eq!(stmts.len(), 1);

	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};

	let expr = lower_pgsql_insert(insert, &EmptyCatalog).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	assert_eq!(ins.into, Some(Expr::Table(surrealdb_strand::TableName::from("users"))));
	assert!(!ins.ignore);
	assert!(ins.update.is_none());
	assert_eq!(output_sql(&ins.output), "RETURN *, record::id(id) AS id");

	let Data::ValuesExpression(rows) = &ins.data else {
		panic!("Expected Data::ValuesExpression");
	};
	assert_eq!(rows.len(), 2);
	assert_eq!(rows[0].len(), 3);
	assert_eq!(rows[0][0].0.to_sql(), "id");
	assert_eq!(rows[0][0].1, Expr::Literal(Literal::Integer(1)));
	assert_eq!(rows[0][1].0.to_sql(), "name");
	assert_eq!(rows[0][1].1, Expr::Literal(Literal::String("Alice".into())));
}

#[test]
fn test_lower_pgsql_insert_default_values() {
	let sql = "INSERT INTO users DEFAULT VALUES";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};

	let expr = lower_pgsql_insert(insert, &EmptyCatalog).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	assert_eq!(ins.data, Data::SingleExpression(Expr::Literal(Literal::Object(Vec::new()))));
}

#[test]
fn test_lower_pgsql_insert_select() {
	let sql = "INSERT INTO users (id, name) SELECT id, name FROM temp_users WHERE active = true";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};

	let expr = lower_pgsql_insert(insert, &EmptyCatalog).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	let Data::SingleExpression(Expr::Select(sel)) = &ins.data else {
		panic!("Expected Data::SingleExpression(Expr::Select)");
	};
	assert_eq!(sel.what, vec![Expr::Table(surrealdb_strand::TableName::from("temp_users"))]);
	assert!(sel.cond.is_some());
}

#[test]
fn test_lower_pgsql_insert_on_conflict_do_nothing() {
	let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};

	let expr = lower_pgsql_insert(insert, &EmptyCatalog).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	assert!(ins.ignore);
	assert!(ins.update.is_none());
}

#[test]
fn test_lower_pgsql_insert_on_conflict_do_update() {
	let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};

	let expr = lower_pgsql_insert(insert, &EmptyCatalog).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	assert!(!ins.ignore);
	let Some(Data::UpdateExpression(assignments)) = &ins.update else {
		panic!("Expected Data::UpdateExpression");
	};
	assert_eq!(assignments.len(), 1);
	assert_eq!(assignments[0].place.to_sql(), "name");
	// EXCLUDED.name should map to $input.name
	let Expr::Idiom(idiom) = &assignments[0].value else {
		panic!("Expected Expr::Idiom");
	};
	assert_eq!(idiom.0.len(), 2);
	assert_eq!(
		idiom.0[0],
		Part::Start(Expr::Param(surrealdb_expr::expr::Param::from("input".to_string())))
	);
	assert_eq!(idiom.0[1], Part::Field("name".into()));
}

#[test]
fn test_lower_pgsql_insert_returning() {
	let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') RETURNING id, name AS user_name";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};

	let expr = lower_pgsql_insert(insert, &EmptyCatalog).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	let Some(Output::Fields(Fields::Select(fields))) = &ins.output else {
		panic!("Expected Output::Fields(Fields::Select)");
	};
	assert_eq!(fields.len(), 2);
	let Field::Single(s1) = &fields[0] else {
		panic!()
	};
	assert_eq!(s1.expr.to_sql(), "record::id(id)");
	assert_eq!(s1.alias.as_ref().unwrap().to_sql(), "id");
	let Field::Single(s2) = &fields[1] else {
		panic!()
	};
	assert_eq!(s2.expr.to_sql(), "name");
	assert_eq!(s2.alias.as_ref().unwrap().to_sql(), "user_name");
}

#[test]
fn test_lower_pgsql_insert_returning_wildcard() {
	let sql = "INSERT INTO users (id) VALUES (1) RETURNING *";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};

	let expr = lower_pgsql_insert(insert, &EmptyCatalog).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	assert_eq!(output_sql(&ins.output), "RETURN *, record::id(id) AS id");
}

#[test]
fn test_lower_pgsql_update() {
	let sql = "UPDATE users SET age = age + 1, name = 'Bob' WHERE id = 1 RETURNING *";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Update(update) = &stmts[0] else {
		panic!("Expected Update statement");
	};

	let expr = lower_pgsql_update(update, &EmptyCatalog).unwrap();
	let Expr::Update(upd) = expr else {
		panic!("Expected Expr::Update");
	};

	assert_eq!(upd.what, vec![Expr::Table(surrealdb_strand::TableName::from("users"))]);
	let Some(Data::SetExpression(assignments)) = &upd.data else {
		panic!("Expected SetExpression");
	};
	assert_eq!(assignments.len(), 2);
	assert_eq!(assignments[0].place.to_sql(), "age");
	assert_eq!(assignments[1].place.to_sql(), "name");
	assert_eq!(upd.cond.as_ref().unwrap().0.to_sql(), "id = (users:1)");
	assert_eq!(output_sql(&upd.output), "RETURN *, record::id(id) AS id");
}

#[test]
fn test_lower_pgsql_delete() {
	let sql = "DELETE FROM users WHERE age < 18 RETURNING id";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Delete(delete) = &stmts[0] else {
		panic!("Expected Delete statement");
	};

	let expr = lower_pgsql_delete(delete, &EmptyCatalog).unwrap();
	let Expr::Delete(del) = expr else {
		panic!("Expected Expr::Delete");
	};

	assert_eq!(del.what, vec![Expr::Table(surrealdb_strand::TableName::from("users"))]);
}

#[test]
fn test_lower_pgsql_truncate_of_several_tables() {
	let stmts =
		surrealdb_pgsql::parse_statements("TRUNCATE TABLE ONLY a, b, c RESTART IDENTITY").unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Truncate(truncate) = &stmts[0] else {
		panic!("Expected Truncate statement");
	};
	let Expr::Delete(del) = lower_pgsql_truncate(truncate).unwrap() else {
		panic!("Expected Expr::Delete");
	};
	let names: Vec<_> = ["a", "b", "c"]
		.iter()
		.map(|name| Expr::Table(surrealdb_strand::TableName::from(*name)))
		.collect();
	assert_eq!(del.what, names);
	assert!(del.cond.is_none());
	assert!(del.output.is_none());
}

#[test]
fn test_lower_pgsql_truncate() {
	let sql = "TRUNCATE TABLE users";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Truncate(truncate) = &stmts[0] else {
		panic!("Expected Truncate statement");
	};

	let expr = lower_pgsql_truncate(truncate).unwrap();
	let Expr::Delete(del) = expr else {
		panic!("Expected Expr::Delete");
	};

	assert_eq!(del.what, vec![Expr::Table(surrealdb_strand::TableName::from("users"))]);
	assert!(del.cond.is_none());
	assert!(del.output.is_none());
}

// ============================================================================
// MySQL DML Lowering Tests
// ============================================================================

#[test]
fn test_lower_mysql_insert_values() {
	let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};

	let expr = lower_mysql_insert(insert, &EmptyCatalog).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	assert_eq!(ins.into, Some(Expr::Table(surrealdb_strand::TableName::from("users"))));
	let Data::ValuesExpression(rows) = &ins.data else {
		panic!("Expected ValuesExpression");
	};
	assert_eq!(rows.len(), 2);
}

#[test]
fn test_lower_mysql_insert_ignore() {
	let sql = "INSERT IGNORE INTO users (id, name) VALUES (1, 'Alice')";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};

	let expr = lower_mysql_insert(insert, &EmptyCatalog).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	assert!(ins.ignore);
}

#[test]
fn test_lower_mysql_insert_on_duplicate_key_update() {
	let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON DUPLICATE KEY UPDATE name = VALUES(name)";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};

	let expr = lower_mysql_insert(insert, &EmptyCatalog).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	let Some(Data::UpdateExpression(assignments)) = &ins.update else {
		panic!("Expected UpdateExpression");
	};
	assert_eq!(assignments.len(), 1);
	assert_eq!(assignments[0].place.to_sql(), "name");
	// VALUES(name) should lower to $input.name
	let Expr::Idiom(idiom) = &assignments[0].value else {
		panic!("Expected Expr::Idiom");
	};
	assert_eq!(idiom.0.len(), 2);
	assert_eq!(
		idiom.0[0],
		Part::Start(Expr::Param(surrealdb_expr::expr::Param::from("input".to_string())))
	);
	assert_eq!(idiom.0[1], Part::Field("name".into()));
}

#[test]
fn test_lower_mysql_replace() {
	let sql = "REPLACE INTO users (id, name) VALUES (1, 'Alice')";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Replace(replace) = &stmts[0] else {
		panic!("Expected Replace statement");
	};

	let expr = lower_mysql_replace(replace, &EmptyCatalog).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	assert_eq!(ins.into, Some(Expr::Table(surrealdb_strand::TableName::from("users"))));
	let Some(Data::UpdateExpression(assignments)) = &ins.update else {
		panic!("Expected UpdateExpression");
	};
	assert_eq!(assignments.len(), 2);
	assert_eq!(assignments[0].place.to_sql(), "id");
	assert_eq!(assignments[1].place.to_sql(), "name");
}

#[test]
fn test_lower_mysql_update() {
	let sql = "UPDATE users SET age = 21, name = 'Alice' WHERE id = 1";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Update(update) = &stmts[0] else {
		panic!("Expected Update statement");
	};

	let expr = lower_mysql_update(update, &EmptyCatalog).unwrap();
	let Expr::Update(upd) = expr else {
		panic!("Expected Expr::Update");
	};

	assert_eq!(upd.what, vec![Expr::Table(surrealdb_strand::TableName::from("users"))]);
	let Some(Data::SetExpression(assignments)) = &upd.data else {
		panic!("Expected SetExpression");
	};
	assert_eq!(assignments.len(), 2);
	assert!(upd.cond.is_some());
}

#[test]
fn test_lower_mysql_delete() {
	let sql = "DELETE FROM users WHERE age > 65";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Delete(delete) = &stmts[0] else {
		panic!("Expected Delete statement");
	};

	let expr = lower_mysql_delete(delete, &EmptyCatalog).unwrap();
	let Expr::Delete(del) = expr else {
		panic!("Expected Expr::Delete");
	};

	assert_eq!(del.what, vec![Expr::Table(surrealdb_strand::TableName::from("users"))]);
	assert!(del.cond.is_some());
}

#[test]
fn test_lower_mysql_truncate() {
	let sql = "TRUNCATE TABLE users";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Truncate(truncate) = &stmts[0] else {
		panic!("Expected Truncate statement");
	};

	let expr = lower_mysql_truncate(truncate).unwrap();
	let Expr::Delete(del) = expr else {
		panic!("Expected Expr::Delete");
	};

	assert_eq!(del.what, vec![Expr::Table(surrealdb_strand::TableName::from("users"))]);
	assert!(del.cond.is_none());
	assert!(del.output.is_none());
}

// ============================================================================
// The `id` column of the target table
// ============================================================================

#[test]
fn pgsql_where_id_equal_to_a_literal_reads_the_record_by_its_id() {
	let upd = pg_update("UPDATE users SET name = 'x' WHERE id = 1");
	assert_eq!(where_sql(&upd.cond), "id = (users:1)");
	let del = pg_delete("DELETE FROM tag WHERE id = 'rust'").unwrap();
	assert_eq!(where_sql(&del.cond), "id = (tag:rust)");
	let del = pg_delete("DELETE FROM users WHERE 2 = id").unwrap();
	assert_eq!(where_sql(&del.cond), "id = (users:2)");
}

#[test]
fn pgsql_where_id_equal_to_a_parameter_reads_the_record_by_its_id() {
	let upd = pg_update("UPDATE users SET name = 'x' WHERE id = $1");
	assert_eq!(
		where_sql(&upd.cond),
		format!("id = ({})", PARAM_RECORD.replace("THEN NONE", "THEN $_1 ?? NULL"))
	);
}

#[test]
fn pgsql_where_id_in_literals_and_parameters_is_a_membership_of_records() {
	let del = pg_delete("DELETE FROM users WHERE id IN (1, 2, $1)").unwrap();
	assert_eq!(
		where_sql(&del.cond),
		format!(
			"id INSIDE [users:1, users:2, ({})]",
			PARAM_RECORD.replace("THEN NONE", "THEN $_1 ?? NULL")
		)
	);
}

#[test]
fn pgsql_where_id_in_one_key_is_the_lookup_of_its_record() {
	let del = pg_delete("DELETE FROM users WHERE id IN (7)").unwrap();
	assert_eq!(where_sql(&del.cond), "id = (users:7)");
}

#[test]
fn pgsql_where_id_otherwise_reads_the_key() {
	for predicate in [
		"id > 1",
		"id <> 1",
		"id = NULL",
		"id = 1.5",
		"id NOT IN (1, 2)",
		"users.id > 1",
		"id BETWEEN 1 AND 3",
	] {
		let del = pg_delete(&format!("DELETE FROM users WHERE {predicate}")).unwrap();
		let text = where_sql(&del.cond);
		assert!(
			text.contains("record::id(id)")
				|| ((predicate == "id <> 1" || predicate == "id NOT IN (1, 2)")
					&& text.contains("users:1")),
			"{predicate}: {text}"
		);
		assert!(!text.contains("type::record"), "{predicate}: {text}");
	}
}

#[test]
fn pgsql_a_column_path_ending_in_id_is_left_as_written() {
	let del = pg_delete("DELETE FROM users WHERE data.id = 1").unwrap();
	assert_eq!(where_sql(&del.cond), "data.id = 1");
}

#[test]
fn pgsql_set_right_hand_sides_read_the_key() {
	let upd = pg_update("UPDATE users SET n = id + 1 WHERE name = 'x'");
	let Some(Data::SetExpression(assignments)) = &upd.data else {
		panic!("Expected SetExpression");
	};
	assert_eq!(
		assignments[0].value.to_sql(),
		"{ LET $_sql_left = record::id(id); IF $_sql_left = NULL OR $_sql_left = NONE OR 1 = NULL OR 1 = NONE THEN NULL ELSE $_sql_left + 1 END; }"
	);
}

#[test]
fn pgsql_assigning_to_id_is_still_an_assignment_to_the_stored_id() {
	let upd = pg_update("UPDATE users SET id = 5 WHERE id = 1");
	let Some(Data::SetExpression(assignments)) = &upd.data else {
		panic!("Expected SetExpression");
	};
	assert_eq!(assignments[0].place.to_sql(), "id");
	assert_eq!(assignments[0].value.to_sql(), "5");
}

#[test]
fn pgsql_returning_id_is_the_key_of_the_written_row() {
	let upd = pg_update("UPDATE users SET name = 'x' WHERE id = 1 RETURNING id, name");
	assert_eq!(output_sql(&upd.output), "RETURN record::id(id) AS id, name");
	let ins = pg_insert("INSERT INTO users (id, name) VALUES (1, 'x') RETURNING users.id");
	assert_eq!(output_sql(&ins.output), "RETURN record::id(id) AS id");
}

#[test]
fn pgsql_a_returned_wildcard_is_followed_by_the_key() {
	let upd = pg_update("UPDATE users SET name = 'x' RETURNING *, name AS n");
	assert_eq!(output_sql(&upd.output), "RETURN *, record::id(id) AS id, name AS n");
}

#[test]
fn pgsql_a_deleted_row_is_read_from_before() {
	let del = pg_delete("DELETE FROM users WHERE id = 2 RETURNING id, name").unwrap();
	assert_eq!(where_sql(&del.cond), "id = (users:2)");
	assert_eq!(
		output_sql(&del.output),
		"RETURN record::id($before.id) AS id, $before.name AS name"
	);
	let del = pg_delete("DELETE FROM users WHERE id = 2").unwrap();
	assert_eq!(
		output_sql(&del.output),
		"RETURN VALUE object::extend($before, { id: record::id($before.id) })"
	);
	let del = pg_delete("DELETE FROM users WHERE id = 2 RETURNING *").unwrap();
	assert_eq!(
		output_sql(&del.output),
		"RETURN VALUE object::extend($before, { id: record::id($before.id) })"
	);
	assert!(pg_delete("DELETE FROM users RETURNING *, name").is_err());
}

#[test]
fn pgsql_on_conflict_reads_the_existing_key_and_the_proposed_row_as_given() {
	let ins = pg_insert(
		"INSERT INTO users (id, name) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET n = id, m = EXCLUDED.id RETURNING id",
	);
	let Some(Data::UpdateExpression(assignments)) = &ins.update else {
		panic!("Expected UpdateExpression");
	};
	assert_eq!(assignments[0].value.to_sql(), "record::id(id)");
	assert_eq!(assignments[1].value.to_sql(), "$input.id");
	assert_eq!(output_sql(&ins.output), "RETURN record::id(id) AS id");
	let Data::ValuesExpression(rows) = &ins.data else {
		panic!("Expected ValuesExpression");
	};
	assert_eq!(rows[0][0].0.to_sql(), "id");
	assert_eq!(rows[0][0].1, Expr::Literal(Literal::Integer(1)));
}

#[test]
fn pgsql_insert_select_reads_the_key_of_the_source_table() {
	let ins = pg_insert("INSERT INTO users (id, name) SELECT id, name FROM temp_users");
	let Data::SingleExpression(Expr::Select(sel)) = &ins.data else {
		panic!("Expected a SELECT source");
	};
	assert_eq!(sel.fields.to_sql(), "record::id(id) AS id, name");
	let ins = pg_insert("INSERT INTO users SELECT * FROM temp_users WHERE id = 3");
	let Data::SingleExpression(Expr::Select(sel)) = &ins.data else {
		panic!("Expected a SELECT source");
	};
	assert_eq!(sel.fields.to_sql(), "*, record::id(id) AS id");
	assert_eq!(where_sql(&sel.cond), "id = (temp_users:3)");
}

#[test]
fn mysql_where_id_reads_the_record_by_its_id() {
	let upd = my_update("UPDATE users SET name = 'x' WHERE id = 1");
	assert_eq!(where_sql(&upd.cond), "id = (users:1)");
	let upd = my_update("UPDATE users SET name = 'x' WHERE id IN (1, 2) AND name = 'y'");
	assert!(
		where_sql(&upd.cond).contains("id INSIDE [users:1, users:2]"),
		"{}",
		where_sql(&upd.cond)
	);
	let del = my_delete("DELETE FROM users WHERE id = ?");
	assert_eq!(where_sql(&del.cond), format!("id = ({PARAM_RECORD})"));
	let del = my_delete("DELETE FROM users WHERE id > 1");
	assert_eq!(where_sql(&del.cond), "record::id(id) > 1");
}

#[test]
fn mysql_echoes_a_written_row_with_the_key_and_a_deleted_row_from_before() {
	let ins = my_insert("INSERT INTO users (id, name) VALUES (1, 'x')");
	assert_eq!(output_sql(&ins.output), "RETURN *, record::id(id) AS id");
	let upd = my_update("UPDATE users SET name = 'x' WHERE id = 1");
	assert_eq!(output_sql(&upd.output), "RETURN *, record::id(id) AS id");
	let del = my_delete("DELETE FROM users WHERE id = 1");
	assert_eq!(
		output_sql(&del.output),
		"RETURN VALUE object::extend($before, { id: record::id($before.id) })"
	);
}

#[test]
fn mysql_on_duplicate_key_update_reads_the_existing_key() {
	let ins = my_insert(
		"INSERT INTO users (id, name) VALUES (1, 'x') ON DUPLICATE KEY UPDATE n = id, m = VALUES(id)",
	);
	let Some(Data::UpdateExpression(assignments)) = &ins.update else {
		panic!("Expected UpdateExpression");
	};
	assert_eq!(assignments[0].value.to_sql(), "record::id(id)");
	assert_eq!(assignments[1].value.to_sql(), "$input.id");
}

#[test]
fn mysql_a_multi_table_update_leaves_id_as_written() {
	let stmts = surrealdb_mysql::parse_statements(
		"UPDATE users u JOIN orders o ON u.id = o.user_id SET n = 1 WHERE u.id = 1",
	)
	.unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Update(update) = &stmts[0] else {
		panic!("Expected Update statement");
	};
	assert!(matches!(
		lower_mysql_update(update, &EmptyCatalog),
		Err(crate::PlanError::UnsupportedStatement(_))
	));
}

#[test]
fn mysql_update_and_delete_reject_order_by_and_limit() {
	for sql in [
		"UPDATE users SET n = 1 ORDER BY id LIMIT 1",
		"UPDATE users SET n = 1 LIMIT 5",
		"UPDATE users SET n = 1 ORDER BY id",
	] {
		let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
		let surrealdb_mysql::ast::stmt::Statement::Update(update) = &stmts[0] else {
			panic!("Expected Update statement");
		};
		assert!(
			matches!(
				lower_mysql_update(update, &EmptyCatalog),
				Err(crate::PlanError::UnsupportedStatement(_))
			),
			"{sql}"
		);
	}
	for sql in [
		"DELETE FROM users ORDER BY id LIMIT 1",
		"DELETE FROM users LIMIT 5",
		"DELETE FROM users ORDER BY id",
	] {
		let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
		let surrealdb_mysql::ast::stmt::Statement::Delete(delete) = &stmts[0] else {
			panic!("Expected Delete statement");
		};
		assert!(
			matches!(
				lower_mysql_delete(delete, &EmptyCatalog),
				Err(crate::PlanError::UnsupportedStatement(_))
			),
			"{sql}"
		);
	}
}

const PARENT_RECORD: &str = "IF parent = '' OR !(type::is_int(parent) OR type::is_i32(parent) OR type::is_i16(parent) OR type::is_i8(parent) OR type::is_u32(parent) OR type::is_u16(parent) OR type::is_u8(parent) OR type::is_string(parent) OR type::is_uuid(parent) OR type::is_array(parent) OR type::is_object(parent) OR type::is_record(parent)) THEN NONE ELSE type::record('users', parent) END";

#[test]
fn pgsql_where_id_equal_to_a_column_is_compared_with_the_record_the_column_names() {
	for sql in [
		"UPDATE users SET name = 'x' WHERE id = parent",
		"DELETE FROM users WHERE parent = id",
		"DELETE FROM users WHERE id <> parent",
	] {
		let text = if sql.starts_with("UPDATE") {
			where_sql(&pg_update(sql).cond)
		} else {
			where_sql(&pg_delete(sql).unwrap().cond)
		};
		assert!(text.contains("type::record('users', parent)"), "{text}");
		assert!(text.contains("parent ?? NULL"), "{text}");
	}
}

#[test]
fn pgsql_where_id_in_columns_and_keys_is_a_membership_of_the_records_they_name() {
	for sql in [
		"DELETE FROM users WHERE id IN (parent, 2)",
		"DELETE FROM users WHERE id NOT IN (parent, 2)",
		"DELETE FROM users WHERE id IN (parent)",
	] {
		let text = where_sql(&pg_delete(sql).unwrap().cond);
		assert!(text.contains("type::record('users', parent)"), "{text}");
		assert!(text.contains("parent ?? NULL"), "{text}");
		if sql.contains("NOT IN") {
			assert!(text.contains("THEN NULL"), "{text}");
		}
	}
}

#[test]
fn mysql_where_id_equal_to_a_column_is_compared_with_the_record_the_column_names() {
	let upd = my_update("UPDATE users SET name = 'x' WHERE id = parent");
	assert_eq!(where_sql(&upd.cond), format!("id = ({PARENT_RECORD})"));
	let del = my_delete("DELETE FROM users WHERE id <> parent AND id IN (parent, 3)");
	assert!(
		where_sql(&del.cond).contains(&format!("id != ({PARENT_RECORD})")),
		"{}",
		where_sql(&del.cond)
	);
	assert!(
		where_sql(&del.cond).contains(&format!("id INSIDE [({PARENT_RECORD}), users:3]")),
		"{}",
		where_sql(&del.cond)
	);
}

#[test]
fn pgsql_write_null_guards_evaluate_volatile_operands_once() {
	for (predicate, calls) in [
		("v <> random()", 1),
		("NOT (v = random())", 1),
		("v IN (random(), random())", 2),
		("v BETWEEN random() AND random()", 2),
	] {
		let text = where_sql(
			&pg_delete(&format!("DELETE FROM values_once WHERE {predicate}")).unwrap().cond,
		);
		assert_eq!(text.matches("`rand`::float()").count(), calls, "{predicate}: {text}");
	}
}

fn pg_insert_expr(sql: &str) -> Result<Expr, crate::PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Insert(insert) = &stmts[0] else {
		panic!("Expected Insert statement");
	};
	lower_pgsql_insert(insert, &EmptyCatalog)
}

#[test]
fn pgsql_on_conflict_do_update_where_updates_the_conflicting_row_only_when_the_condition_holds() {
	let expr = pg_insert_expr(
		"INSERT INTO users (id, name, age) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET age = \"public\".\"users\".\"age\" + EXCLUDED.age WHERE (\"public\".\"users\".\"id\" = $4 AND 1 = 1) RETURNING id, age",
	)
	.unwrap();
	let Expr::Block(block) = expr else {
		panic!("Expected a block");
	};
	assert_eq!(block.len(), 3);
	let Expr::Let(proposed) = &block[0] else {
		panic!("Expected LET");
	};
	assert_eq!(proposed.name.as_str(), "excluded");
	assert_eq!(proposed.what.to_sql(), "{ id: $_1, name: $_2, age: $_3 }");
	let Expr::Let(conflicting) = &block[1] else {
		panic!("Expected LET");
	};
	assert_eq!(conflicting.name.as_str(), "conflicting");
	let Expr::Select(lookup) = &conflicting.what else {
		panic!("Expected SELECT");
	};
	assert_eq!(lookup.fields.to_sql(), "VALUE id");
	let Expr::IfElse(branches) = &block[2] else {
		panic!("Expected IF");
	};
	let Expr::Insert(inserted) = &branches.exprs[0].1 else {
		panic!("Expected INSERT");
	};
	assert!(!inserted.ignore && inserted.update.is_none());
	let Some(Expr::Update(updated)) = &branches.close else {
		panic!("Expected UPDATE");
	};
	let Some(Data::SetExpression(assignments)) = &updated.data else {
		panic!("Expected SET");
	};
	let assigned = assignments[0].value.to_sql();
	assert!(assigned.contains("age + $_sql_right"), "{assigned}");
	assert!(assigned.contains("$excluded.age") && !assigned.contains("public"), "{assigned}");
	let matched = where_sql(&lookup.cond);
	let condition = where_sql(&updated.cond);
	assert!(condition.starts_with(&format!("{matched} AND ")), "{condition}");
	assert!(condition.contains("$_4"), "{condition}");
	assert_eq!(output_sql(&updated.output), output_sql(&inserted.output));
}

#[test]
fn pgsql_on_conflict_do_update_where_supports_an_alias_and_a_target_that_is_not_the_key() {
	let expr = pg_insert_expr(
		"INSERT INTO users AS u (id, email) VALUES (1, 'a') ON CONFLICT (email) DO UPDATE SET id = u.id WHERE u.id = EXCLUDED.id",
	)
	.unwrap();
	let Expr::Block(block) = expr else {
		panic!("Expected a block");
	};
	let Expr::IfElse(branches) = &block[2] else {
		panic!("Expected IF");
	};
	let Some(Expr::Update(updated)) = &branches.close else {
		panic!("Expected UPDATE");
	};
	let sql = where_sql(&updated.cond);
	assert!(sql.contains("$excluded.email") && !sql.contains("'a'"), "{sql}");
	assert!(sql.contains("$excluded.id"), "{sql}");
}

#[test]
fn pgsql_on_conflict_forms_without_a_lowering_are_unsupported() {
	for sql in [
		"INSERT INTO users (id) VALUES (1) ON CONFLICT (id) WHERE id > 0 DO NOTHING",
		"INSERT INTO users (id) VALUES (1) ON CONFLICT (id) WHERE id > 0 DO UPDATE SET id = 2",
		"INSERT INTO users (id) VALUES (1), (2) ON CONFLICT (id) DO UPDATE SET id = 2 WHERE id > 0",
		"INSERT INTO users (id) SELECT id FROM t ON CONFLICT (id) DO UPDATE SET id = 2 WHERE id > 0",
		"INSERT INTO users (id) VALUES (1) ON CONFLICT DO UPDATE SET id = 2 WHERE id > 0",
		"INSERT INTO users (name) VALUES ('a') ON CONFLICT (id) DO UPDATE SET name = 'b' WHERE id > 0",
	] {
		let err = pg_insert_expr(sql).unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{sql}: {err}");
	}
}

#[test]
fn pgsql_conditional_upsert_evaluates_each_proposed_value_once() {
	let expr = pg_insert_expr(
		"INSERT INTO users (id, name) VALUES (1, gen_random_uuid()) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id > 0",
	)
	.unwrap();
	let sql = expr.to_sql();
	assert_eq!(sql.matches("::uuid::v4").count(), 1, "{sql}");
}

#[test]
fn insert_select_rejects_complex_select_clauses() {
	for sql in [
		"INSERT INTO dst (id) SELECT id FROM src LIMIT 1",
		"INSERT INTO dst (id) SELECT id FROM src ORDER BY id",
		"INSERT INTO dst (id) SELECT id FROM src GROUP BY id",
		"INSERT INTO dst (id) SELECT DISTINCT id FROM src",
		"INSERT INTO dst (id) SELECT a.id FROM src a JOIN src b ON a.id = b.id",
	] {
		assert!(
			matches!(pg_insert_expr(sql), Err(crate::PlanError::UnsupportedStatement(_))),
			"pgsql: {sql}"
		);
		let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
		let surrealdb_mysql::ast::stmt::Statement::Insert(ins) = &stmts[0] else {
			panic!("Expected Insert");
		};
		assert!(
			matches!(
				lower_mysql_insert(ins, &EmptyCatalog),
				Err(crate::PlanError::UnsupportedStatement(_))
			),
			"mysql: {sql}"
		);
	}
}

#[test]
#[allow(clippy::wildcard_enum_match_arm)]
fn dml_rejects_cross_database_targets() {
	let catalog = crate::catalog::MemoryCatalog::new().with_database("db1");
	for sql in [
		"INSERT INTO db2.public.users (id) VALUES (1)",
		"INSERT INTO users (id) SELECT id FROM db2.public.src",
		"UPDATE db2.public.users SET name = 'a'",
		"DELETE FROM db2.public.users",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
		let res = match &stmts[0] {
			surrealdb_pgsql::ast::stmt::Statement::Insert(s) => lower_pgsql_insert(s, &catalog),
			surrealdb_pgsql::ast::stmt::Statement::Update(s) => lower_pgsql_update(s, &catalog),
			surrealdb_pgsql::ast::stmt::Statement::Delete(s) => lower_pgsql_delete(s, &catalog),
			_ => unreachable!(),
		};
		assert!(res.is_err(), "pgsql should reject cross-database target: {sql}");
	}
	for sql in [
		"INSERT INTO db2.users (id) VALUES (1)",
		"INSERT INTO db1.users (id) SELECT id FROM db2.src",
		"REPLACE INTO db2.users (id) VALUES (1)",
		"UPDATE db2.users SET name = 'a'",
		"DELETE FROM db2.users",
	] {
		let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
		let res = match &stmts[0] {
			surrealdb_mysql::ast::stmt::Statement::Insert(s) => lower_mysql_insert(s, &catalog),
			surrealdb_mysql::ast::stmt::Statement::Replace(s) => lower_mysql_replace(s, &catalog),
			surrealdb_mysql::ast::stmt::Statement::Update(s) => lower_mysql_update(s, &catalog),
			surrealdb_mysql::ast::stmt::Statement::Delete(s) => lower_mysql_delete(s, &catalog),
			_ => unreachable!(),
		};
		assert!(res.is_err(), "mysql should reject cross-database target: {sql}");
	}
}
