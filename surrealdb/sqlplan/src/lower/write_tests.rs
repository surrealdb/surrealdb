#[allow(unused_imports)]
use surrealdb_expr::expr::data::Assignment;
#[allow(unused_imports)]
use surrealdb_expr::expr::statements::{DeleteStatement, InsertStatement, UpdateStatement};
use surrealdb_expr::expr::{Data, Expr, Field, Fields, Literal, Output, Part};
use surrealdb_types::ToSql;

use crate::lower::write::*;

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

	let expr = lower_pgsql_insert(insert).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	assert_eq!(ins.into, Some(Expr::Table(surrealdb_strand::TableName::from("users"))));
	assert!(!ins.ignore);
	assert!(ins.update.is_none());
	assert!(ins.output.is_none());

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

	let expr = lower_pgsql_insert(insert).unwrap();
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

	let expr = lower_pgsql_insert(insert).unwrap();
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

	let expr = lower_pgsql_insert(insert).unwrap();
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

	let expr = lower_pgsql_insert(insert).unwrap();
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

	let expr = lower_pgsql_insert(insert).unwrap();
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
	assert_eq!(s1.expr.to_sql(), "id");
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

	let expr = lower_pgsql_insert(insert).unwrap();
	let Expr::Insert(ins) = expr else {
		panic!("Expected Expr::Insert");
	};

	assert_eq!(ins.output, Some(Output::Fields(Fields::all())));
}

#[test]
fn test_lower_pgsql_update() {
	let sql = "UPDATE users SET age = age + 1, name = 'Bob' WHERE id = 1 RETURNING *";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Update(update) = &stmts[0] else {
		panic!("Expected Update statement");
	};

	let expr = lower_pgsql_update(update).unwrap();
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
	assert!(upd.cond.is_some());
	assert_eq!(upd.output, Some(Output::Fields(Fields::all())));
}

#[test]
fn test_lower_pgsql_delete() {
	let sql = "DELETE FROM users WHERE age < 18 RETURNING id";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Delete(delete) = &stmts[0] else {
		panic!("Expected Delete statement");
	};

	let expr = lower_pgsql_delete(delete).unwrap();
	let Expr::Delete(del) = expr else {
		panic!("Expected Expr::Delete");
	};

	assert_eq!(del.what, vec![Expr::Table(surrealdb_strand::TableName::from("users"))]);
	assert!(del.cond.is_some());
	assert!(del.output.is_some());
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

	let expr = lower_mysql_insert(insert).unwrap();
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

	let expr = lower_mysql_insert(insert).unwrap();
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

	let expr = lower_mysql_insert(insert).unwrap();
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

	let expr = lower_mysql_replace(replace).unwrap();
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

	let expr = lower_mysql_update(update).unwrap();
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

	let expr = lower_mysql_delete(delete).unwrap();
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
}
