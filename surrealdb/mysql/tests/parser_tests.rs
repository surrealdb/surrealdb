//! Parser unit tests for surrealdb-mysql.

// A catch-all arm in these tests reports the node a parse produced instead of the one
// expected; naming every other variant of the AST would add nothing to that report.
#![allow(clippy::wildcard_enum_match_arm)]

use surrealdb_mysql::ast::expr::Expr;
use surrealdb_mysql::ast::literal::Literal;
use surrealdb_mysql::ast::operator::{BinaryOperator, UnaryOperator};
use surrealdb_mysql::ast::stmt::{SelectItem, Statement, VariableTarget};
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
fn test_parse_from_dual_is_a_select_without_from() {
	for sql in ["SELECT 1 FROM DUAL", "SELECT 1 FROM dual WHERE 1 = 1"] {
		let Statement::Select(sel) = parse_statement(sql).unwrap() else {
			panic!("Expected Select statement for {sql}");
		};
		assert!(sel.from.is_none(), "{sql} reads no table");
	}
	for sql in ["SELECT 1 FROM `dual`", "SELECT 1 FROM db.dual", "SELECT 1 FROM `db`.`dual`"] {
		let Statement::Select(sel) = parse_statement(sql).unwrap() else {
			panic!("Expected Select statement for {sql}");
		};
		assert!(sel.from.is_some(), "{sql} reads a table");
	}
	for sql in
		["SELECT 1 FROM DUAL AS d", "SELECT 1 FROM DUAL, t", "SELECT 1 FROM DUAL JOIN t ON 1 = 1"]
	{
		assert!(parse_statement(sql).is_err(), "{sql} is not valid MySQL");
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
fn test_parse_multi_word_and_serial_column_types() {
	let Statement::CreateTable(create) = parse_statement(
		"CREATE TABLE t (a NATIONAL CHARACTER VARYING(10), b LONG VARBINARY, c DOUBLE PRECISION, \
		 d SERIAL, e BIGINT UNSIGNED, f LONG VARCHAR, g CHARACTER VARYING(5) NOT NULL)",
	)
	.unwrap() else {
		panic!("expected CREATE TABLE");
	};
	let types: Vec<&str> = create.columns.iter().map(|c| c.data_type.as_str()).collect();
	assert_eq!(
		types,
		[
			"NATIONAL CHARACTER VARYING(10)",
			"LONG VARBINARY",
			"DOUBLE PRECISION",
			"SERIAL",
			"BIGINT UNSIGNED",
			"LONG VARCHAR",
			"CHARACTER VARYING(5)",
		]
	);
	// SERIAL is BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
	let serial = &create.columns[3];
	assert!(serial.not_null && serial.auto_increment && serial.unique);
	let plain = &create.columns[4];
	assert!(!plain.not_null && !plain.auto_increment && !plain.unique);
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

#[test]
fn test_parse_cte_statements() {
	let s = parse_statement("WITH cte AS (SELECT 1 AS a) SELECT a FROM cte").unwrap();
	if let Statement::Select(sel) = s {
		assert!(sel.with.is_some());
		let with = sel.with.unwrap();
		assert!(!with.recursive);
		assert_eq!(with.ctes.len(), 1);
		assert_eq!(with.ctes[0].name, "cte");
	} else {
		panic!("expected Statement::Select");
	}

	let s2 =
		parse_statement("WITH RECURSIVE cte(x, y) AS (SELECT 1, 2) SELECT * FROM cte").unwrap();
	if let Statement::Select(sel) = s2 {
		assert!(sel.with.is_some());
		let with = sel.with.unwrap();
		assert!(with.recursive);
		assert_eq!(with.ctes.len(), 1);
		assert_eq!(with.ctes[0].name, "cte");
		assert_eq!(with.ctes[0].columns, vec!["x", "y"]);
	} else {
		panic!("expected Statement::Select");
	}
}

#[test]
fn test_parse_window_function_and_named_window() {
	let s = parse_statement(
		"SELECT row_number() OVER (PARTITION BY dep ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM employees WINDOW w AS (PARTITION BY dep)",
	)
	.unwrap();
	if let Statement::Select(sel) = s {
		assert_eq!(sel.window.len(), 1);
		assert_eq!(sel.window[0].name, "w");
		assert_eq!(sel.window[0].spec.partition_by.len(), 1);
		assert_eq!(sel.projection.len(), 1);
		if let SelectItem::Expr {
			expr: Expr::FunctionCall {
				name,
				over,
				..
			},
			..
		} = &sel.projection[0]
		{
			assert_eq!(name, "row_number");
			assert!(over.is_some());
			let spec = over.as_ref().unwrap();
			assert_eq!(spec.partition_by.len(), 1);
			assert_eq!(spec.order_by.len(), 1);
			assert!(spec.frame.is_some());
		} else {
			panic!("expected FunctionCall projection with over clause");
		}
	} else {
		panic!("expected Statement::Select");
	}
}

#[test]
fn test_parse_quantified_comparison() {
	use surrealdb_mysql::ast::expr::Quantifier;

	let s = parse_statement("SELECT 1 WHERE x = ANY (SELECT y FROM t)").unwrap();
	if let Statement::Select(sel) = s {
		if let Some(Expr::Quantified {
			op,
			quantifier,
			..
		}) = sel.where_clause
		{
			assert_eq!(op, BinaryOperator::Equal);
			assert_eq!(quantifier, Quantifier::Any);
		} else {
			panic!("expected Expr::Quantified");
		}
	} else {
		panic!("expected Statement::Select");
	}

	let s = parse_statement("SELECT 1 WHERE x < ALL (SELECT y FROM t)").unwrap();
	if let Statement::Select(sel) = s {
		if let Some(Expr::Quantified {
			op,
			quantifier,
			..
		}) = sel.where_clause
		{
			assert_eq!(op, BinaryOperator::Less);
			assert_eq!(quantifier, Quantifier::All);
		} else {
			panic!("expected Expr::Quantified");
		}
	} else {
		panic!("expected Statement::Select");
	}
}

#[test]
fn test_parse_insert_and_replace_set_and_optional_into() {
	let s1 = parse_statement("INSERT users SET id = 1, name = 'Alice'").unwrap();
	if let Statement::Insert(ins) = s1 {
		assert_eq!(ins.table.parts, vec!["users"]);
		assert_eq!(ins.columns, vec!["id", "name"]);
		assert_eq!(ins.values.len(), 1);
		assert_eq!(ins.values[0].len(), 2);
	} else {
		panic!("expected Statement::Insert");
	}

	let s2 = parse_statement("INSERT INTO users SET id = 1").unwrap();
	if let Statement::Insert(ins) = s2 {
		assert_eq!(ins.columns, vec!["id"]);
		assert_eq!(ins.values.len(), 1);
	} else {
		panic!("expected Statement::Insert");
	}

	let s3 = parse_statement("REPLACE users SET id = 1, name = 'Bob'").unwrap();
	if let Statement::Replace(rep) = s3 {
		assert_eq!(rep.table.parts, vec!["users"]);
		assert_eq!(rep.columns, vec!["id", "name"]);
		assert_eq!(rep.values.len(), 1);
	} else {
		panic!("expected Statement::Replace");
	}
}

#[test]
fn test_parse_group_by_with_rollup() {
	let s =
		parse_statement("SELECT dep, SUM(salary) FROM employees GROUP BY dep WITH ROLLUP").unwrap();
	if let Statement::Select(sel) = s {
		assert!(sel.with_rollup);
		assert_eq!(sel.group_by.len(), 1);
	} else {
		panic!("expected Statement::Select with rollup");
	}
}

#[test]
fn test_parse_index_hints() {
	use surrealdb_mysql::ast::stmt::{IndexHintType, TableRef};

	let s = parse_statement("SELECT * FROM users USE INDEX (idx_name)").unwrap();
	if let Statement::Select(sel) = s {
		if let Some(TableRef::Table {
			index_hints,
			..
		}) = sel.from
		{
			assert_eq!(index_hints.len(), 1);
			assert_eq!(index_hints[0].hint_type, IndexHintType::Use);
			assert_eq!(index_hints[0].indexes, vec!["idx_name"]);
		} else {
			panic!("expected TableRef::Table with hints");
		}
	} else {
		panic!("expected Statement::Select");
	}

	let s2 = parse_statement("SELECT * FROM users FORCE INDEX (idx1, idx2) IGNORE INDEX (idx3)")
		.unwrap();
	if let Statement::Select(sel) = s2 {
		if let Some(TableRef::Table {
			index_hints,
			..
		}) = sel.from
		{
			assert_eq!(index_hints.len(), 2);
			assert_eq!(index_hints[0].hint_type, IndexHintType::Force);
			assert_eq!(index_hints[0].indexes, vec!["idx1", "idx2"]);
			assert_eq!(index_hints[1].hint_type, IndexHintType::Ignore);
			assert_eq!(index_hints[1].indexes, vec!["idx3"]);
		} else {
			panic!("expected TableRef::Table with multiple hints");
		}
	} else {
		panic!("expected Statement::Select");
	}
}

#[test]
fn test_parse_show_variants() {
	use surrealdb_mysql::ast::stmt::ShowStmt;

	let s = parse_statement("SHOW FULL TABLES").unwrap();
	assert!(matches!(
		s,
		Statement::Show(ShowStmt::Tables {
			full: true,
			..
		})
	));

	let s = parse_statement("SHOW FULL COLUMNS FROM users").unwrap();
	assert!(matches!(
		s,
		Statement::Show(ShowStmt::Columns {
			full: true,
			..
		})
	));

	let s = parse_statement("SHOW INDEX FROM users").unwrap();
	assert!(matches!(s, Statement::Show(ShowStmt::Index { .. })));

	let s = parse_statement("SHOW ENGINES").unwrap();
	assert!(matches!(s, Statement::Show(ShowStmt::Engines { .. })));

	let s = parse_statement("SHOW COLLATION LIKE 'utf8%'").unwrap();
	assert!(matches!(s, Statement::Show(ShowStmt::Collation { .. })));

	let s = parse_statement("SHOW CHARACTER SET").unwrap();
	assert!(matches!(s, Statement::Show(ShowStmt::Charset { .. })));
}

#[test]
fn test_parse_ddl_and_admin_extended() {
	let s = parse_statement("CREATE DATABASE IF NOT EXISTS mydb").unwrap();
	if let Statement::CreateDatabase(db) = s {
		assert_eq!(db.name, "mydb");
		assert!(db.if_not_exists);
	} else {
		panic!("expected CreateDatabase");
	}

	let s = parse_statement("DROP DATABASE IF EXISTS mydb").unwrap();
	if let Statement::DropDatabase(db) = s {
		assert_eq!(db.name, "mydb");
		assert!(db.if_exists);
	} else {
		panic!("expected DropDatabase");
	}

	let s = parse_statement("CREATE INDEX idx_user ON users (email, created_at)").unwrap();
	if let Statement::CreateIndex(idx) = s {
		assert_eq!(idx.name, "idx_user");
		assert_eq!(idx.columns, vec!["email", "created_at"]);
		assert!(!idx.unique);
	} else {
		panic!("expected CreateIndex");
	}

	let s = parse_statement("CREATE UNIQUE INDEX uq_email ON users (email)").unwrap();
	if let Statement::CreateIndex(idx) = s {
		assert_eq!(idx.name, "uq_email");
		assert!(idx.unique);
	} else {
		panic!("expected CreateIndex unique");
	}

	let s = parse_statement("DROP INDEX idx_user ON users").unwrap();
	if let Statement::DropIndex(idx) = s {
		assert_eq!(idx.name, "idx_user");
	} else {
		panic!("expected DropIndex");
	}

	let s = parse_statement("CREATE VIEW my_view AS SELECT id FROM t").unwrap();
	if let Statement::CreateView(v) = s {
		assert_eq!(v.name.parts, vec!["my_view"]);
		assert!(!v.or_replace);
	} else {
		panic!("expected CreateView");
	}

	let s = parse_statement("CREATE OR REPLACE VIEW my_view AS SELECT id FROM t").unwrap();
	if let Statement::CreateView(v) = s {
		assert!(v.or_replace);
	} else {
		panic!("expected CreateView or replace");
	}

	let s = parse_statement("DROP VIEW IF EXISTS v1, v2").unwrap();
	if let Statement::DropView(v) = s {
		assert!(v.if_exists);
		assert_eq!(v.names.len(), 2);
	} else {
		panic!("expected DropView");
	}

	let s = parse_statement(
		"ALTER TABLE users ADD COLUMN age INT, DROP COLUMN legacy, RENAME TO customers",
	)
	.unwrap();
	if let Statement::AlterTable(alt) = s {
		assert_eq!(alt.table.parts, vec!["users"]);
		assert_eq!(alt.actions.len(), 3);
	} else {
		panic!("expected AlterTable");
	}

	let s = parse_statement("CREATE USER IF NOT EXISTS 'admin'@'localhost' IDENTIFIED BY 'secret'")
		.unwrap();
	if let Statement::CreateUser(u) = s {
		assert_eq!(u.user, "admin");
		assert_eq!(u.host, Some("localhost".to_string()));
		assert_eq!(u.identified_by, Some("secret".to_string()));
		assert!(u.if_not_exists);
	} else {
		panic!("expected CreateUser");
	}

	let s = parse_statement("DROP USER IF EXISTS 'admin'@'localhost'").unwrap();
	if let Statement::DropUser(u) = s {
		assert!(u.if_exists);
		assert_eq!(u.users.len(), 1);
		assert_eq!(u.users[0].0, "admin");
		assert_eq!(u.users[0].1, Some("localhost".to_string()));
	} else {
		panic!("expected DropUser");
	}

	let s = parse_statement("GRANT ALL PRIVILEGES ON *.* TO 'admin'@'localhost'").unwrap();
	if let Statement::Grant(g) = s {
		assert_eq!(g.privileges, vec!["ALL"]);
		assert_eq!(g.user, "admin");
		assert_eq!(g.host, Some("localhost".to_string()));
	} else {
		panic!("expected Grant");
	}

	let s = parse_statement("REVOKE ALL PRIVILEGES ON *.* FROM 'admin'@'localhost'").unwrap();
	if let Statement::Revoke(r) = s {
		assert_eq!(r.privileges, vec!["ALL"]);
		assert_eq!(r.user, "admin");
		assert_eq!(r.host, Some("localhost".to_string()));
	} else {
		panic!("expected Revoke");
	}

	let s = parse_statement("LOCK TABLES t1 READ, t2 WRITE").unwrap();
	if let Statement::LockTables(l) = s {
		assert_eq!(l.locks.len(), 2);
	} else {
		panic!("expected LockTables");
	}

	let s = parse_statement("UNLOCK TABLES").unwrap();
	assert!(matches!(s, Statement::UnlockTables(_)));

	let s = parse_statement("PREPARE stmt1 FROM 'SELECT * FROM users WHERE id = ?'").unwrap();
	if let Statement::Prepare(p) = s {
		assert_eq!(p.name, "stmt1");
		assert_eq!(p.query, "SELECT * FROM users WHERE id = ?");
	} else {
		panic!("expected Prepare");
	}

	let s = parse_statement("EXECUTE stmt1 USING @val").unwrap();
	if let Statement::Execute(e) = s {
		assert_eq!(e.name, "stmt1");
		assert_eq!(e.using.len(), 1);
	} else {
		panic!("expected Execute");
	}

	let s = parse_statement("DEALLOCATE PREPARE stmt1").unwrap();
	if let Statement::Deallocate(d) = s {
		assert_eq!(d.name, "stmt1");
	} else {
		panic!("expected Deallocate");
	}
}

#[test]
fn test_sql_mode_parsing() {
	use surrealdb_mysql::SqlMode;

	let mode = SqlMode::from_mode_str("ANSI_QUOTES,PIPES_AS_CONCAT,NO_BACKSLASH_ESCAPES");
	assert!(mode.ansi_quotes);
	assert!(mode.pipes_as_concat);
	assert!(mode.no_backslash_escapes);
	assert!(!mode.ignore_space);

	// Combination preset ANSI
	let ansi_mode = SqlMode::from_mode_str("ANSI");
	assert!(ansi_mode.ansi_quotes);
	assert!(ansi_mode.pipes_as_concat);
	assert!(ansi_mode.real_as_float);

	// Preset TRADITIONAL
	let trad_mode = SqlMode::from_mode_str("TRADITIONAL");
	assert!(trad_mode.strict_trans_tables);
	assert!(trad_mode.strict_all_tables);
	assert!(trad_mode.no_zero_date);

	// Round-trip formatting
	let formatted = mode.format();
	let reparsed = SqlMode::from_mode_str(&formatted);
	assert_eq!(mode, reparsed);
}

#[test]
fn test_sql_mode_ansi_quotes_statements() {
	use surrealdb_mysql::ast::expr::Expr;
	use surrealdb_mysql::ast::literal::Literal;
	use surrealdb_mysql::ast::stmt::Statement;
	use surrealdb_mysql::parser::parse_statement_with_settings;
	use surrealdb_mysql::{ParserSettings, SqlMode};

	// Default mode: "id" in SELECT projection is parsed as string literal
	let stmt = parse_statement_with_settings(r#"SELECT "id""#, ParserSettings::default()).unwrap();
	if let Statement::Select(sel) = stmt {
		let SelectItem::Expr {
			expr,
			..
		} = &sel.projection[0]
		else {
			panic!("expected SelectItem::Expr");
		};
		assert!(matches!(expr, Expr::Literal(Literal::String(s), _) if s == "id"));
	}

	// ANSI_QUOTES mode: "id" and "users" are identifiers
	let settings = ParserSettings::default().with_sql_mode(SqlMode::empty().with_ansi_quotes(true));
	let stmt_ansi = parse_statement_with_settings(r#"SELECT "id" FROM "users""#, settings).unwrap();
	if let Statement::Select(sel) = stmt_ansi {
		let SelectItem::Expr {
			expr,
			..
		} = &sel.projection[0]
		else {
			panic!("expected SelectItem::Expr");
		};
		let Expr::ColumnRef(col) = expr else {
			panic!("expected ColumnRef for projection");
		};
		assert_eq!(col.fields, vec!["id"]);
	}
}

#[test]
fn test_sql_mode_pipes_as_concat() {
	use surrealdb_mysql::ast::expr::Expr;
	use surrealdb_mysql::ast::operator::BinaryOperator;
	use surrealdb_mysql::parser::parse_expr_with_settings;
	use surrealdb_mysql::{ParserSettings, SqlMode};

	// Default mode: || is logical OR
	let expr_default = parse_expr_with_settings("'a' || 'b'", ParserSettings::default()).unwrap();
	if let Expr::BinaryOp {
		op,
		..
	} = expr_default
	{
		assert_eq!(op, BinaryOperator::Or);
	} else {
		panic!("expected BinaryOp");
	}

	// PIPES_AS_CONCAT: || is Concat
	let settings =
		ParserSettings::default().with_sql_mode(SqlMode::empty().with_pipes_as_concat(true));
	let expr_concat = parse_expr_with_settings("'a' || 'b'", settings).unwrap();
	if let Expr::BinaryOp {
		op,
		..
	} = expr_concat
	{
		assert_eq!(op, BinaryOperator::Concat);
	} else {
		panic!("expected BinaryOp::Concat");
	}
}

#[test]
fn test_parse_charset_introduced_literals() {
	use surrealdb_mysql::ast::expr::Expr;
	use surrealdb_mysql::ast::literal::Literal;
	use surrealdb_mysql::ast::stmt::Statement;
	use surrealdb_mysql::parser::parse_statement;

	let stmt = parse_statement("SELECT _utf8mb4'hello', _binary 0x01AF").unwrap();
	if let Statement::Select(sel) = stmt {
		let SelectItem::Expr {
			expr,
			..
		} = &sel.projection[0]
		else {
			panic!("expected SelectItem::Expr");
		};
		let Expr::Literal(
			Literal::IntroducedString {
				charset,
				value,
			},
			_,
		) = expr
		else {
			panic!("expected IntroducedString, got {expr:?}");
		};
		assert_eq!(charset, "utf8mb4");
		assert_eq!(value, "hello");

		let SelectItem::Expr {
			expr,
			..
		} = &sel.projection[1]
		else {
			panic!("expected SelectItem::Expr");
		};
		let Expr::Literal(
			Literal::IntroducedHexString {
				charset,
				value,
			},
			_,
		) = expr
		else {
			panic!("expected IntroducedHexString, got {expr:?}");
		};
		assert_eq!(charset, "binary");
		assert_eq!(value.as_slice(), &[0x01, 0xAF]);
	}
}

#[test]
fn test_parse_executable_version_comments() {
	use surrealdb_mysql::ast::stmt::Statement;
	use surrealdb_mysql::parser::parse_statement;

	let stmt = parse_statement("/*!40101 SET NAMES utf8mb4 */;").unwrap();
	if let Statement::Set(s) = stmt {
		assert_eq!(s.assignments.len(), 1);
		assert_eq!(
			s.assignments[0].0,
			VariableTarget::System {
				name: "names".to_string(),
				is_global: false,
			}
		);
	} else {
		panic!("expected Set statement");
	}

	let stmt2 = parse_statement("SELECT /*!50000 1 + */ 2").unwrap();
	if let Statement::Select(sel) = stmt2 {
		assert_eq!(sel.projection.len(), 1);
	} else {
		panic!("expected Select statement");
	}
}

#[test]
fn test_a_decimal_integer_literal_that_does_not_fit_64_bits_is_a_decimal() {
	// The magnitude of the int8 minimum stays a double, so a sign can be folded into it.
	let e = parse_expr("9223372036854775808").expect("the int8 minimum magnitude");
	assert_eq!(e, Expr::Literal(Literal::Float(2f64.powi(63)), e.span()));
	for literal in ["18446744073709551616", "123456789012345678901234567890"] {
		let e = parse_expr(literal).expect(literal);
		assert_eq!(e, Expr::Literal(Literal::Decimal(literal.to_string()), e.span()));
	}
	assert!(parse_expr(&"9".repeat(400)).is_err(), "a literal beyond a double has no value");
}

#[test]
fn test_a_decimal_integer_literal_up_to_u64_max_is_exact_and_unsigned() {
	for (literal, value) in [
		("10000000000000000000", 10_000_000_000_000_000_000u64),
		("9223372036854775809", 9_223_372_036_854_775_809u64),
		("18446744073709551615", u64::MAX),
	] {
		let e = parse_expr(literal).expect(literal);
		assert_eq!(e, Expr::Literal(Literal::Unsigned(value), e.span()), "{literal}");
	}
}

#[test]
fn test_a_minus_applied_to_the_magnitude_of_the_int8_minimum_is_that_integer() {
	for sql in [
		"-9223372036854775808",
		"- 9223372036854775808",
		"-(9223372036854775808)",
		"-((9223372036854775808))",
		"-09223372036854775808",
	] {
		let e = parse_expr(sql).expect(sql);
		assert_eq!(e, Expr::Literal(Literal::Integer(i64::MIN), e.span()), "{sql}");
	}
}

#[test]
fn test_a_minus_applied_to_anything_else_is_not_folded_into_the_int8_minimum() {
	let two_pow_63 = 2f64.powi(63);
	for (sql, operator, operand) in [
		(
			"-9223372036854775809",
			UnaryOperator::Minus,
			Literal::Unsigned(9_223_372_036_854_775_809),
		),
		(
			"-9223372036854775808.0",
			UnaryOperator::Minus,
			Literal::Decimal("9223372036854775808.0".to_string()),
		),
		("-9223372036854775808e0", UnaryOperator::Minus, Literal::Float(two_pow_63)),
		("-9223372036854775807", UnaryOperator::Minus, Literal::Integer(i64::MAX)),
		("+9223372036854775808", UnaryOperator::Plus, Literal::Float(two_pow_63)),
		("- -9223372036854775808", UnaryOperator::Minus, Literal::Integer(i64::MIN)),
	] {
		let negation = parse_expr(sql).expect(sql);
		let Expr::UnaryOp {
			op,
			expr,
			..
		} = negation
		else {
			panic!("{sql}: expected a negation, got {negation:?}")
		};
		assert_eq!(op, operator, "{sql}");
		assert_eq!(*expr, Expr::Literal(operand, expr.span()), "{sql}");
	}
}

#[test]
fn test_the_int8_minimum_is_the_operand_of_a_comparison_and_of_arithmetic() {
	for (sql, operator) in [
		("x = -9223372036854775808", BinaryOperator::Equal),
		("x=-9223372036854775808", BinaryOperator::Equal),
		("x > -9223372036854775808", BinaryOperator::Greater),
		("x>-9223372036854775808", BinaryOperator::Greater),
		("x >= -9223372036854775808", BinaryOperator::GreaterEqual),
		("x <= -9223372036854775808", BinaryOperator::LessEqual),
		("x <=> -9223372036854775808", BinaryOperator::NullSafeEqual),
		("x - -9223372036854775808", BinaryOperator::Sub),
		("x * -9223372036854775808", BinaryOperator::Mul),
	] {
		let expr = parse_expr(sql).expect(sql);
		let Expr::BinaryOp {
			op,
			right,
			..
		} = expr
		else {
			panic!("{sql}: expected a binary operator, got {expr:?}")
		};
		assert_eq!(op, operator, "{sql}");
		assert_eq!(*right, Expr::Literal(Literal::Integer(i64::MIN), right.span()), "{sql}");
	}
	let quotient = parse_expr("-9223372036854775808 DIV -1").expect("a quotient");
	let Expr::BinaryOp {
		left,
		op: BinaryOperator::IntDiv,
		..
	} = quotient
	else {
		panic!("expected a quotient, got {quotient:?}")
	};
	assert_eq!(*left, Expr::Literal(Literal::Integer(i64::MIN), left.span()));
}

#[test]
fn test_parse_qualified_wildcard_and_a_column_named_star() {
	fn projection(sql: &str) -> Vec<SelectItem> {
		let statement = parse_statement(sql).expect("parse");
		let Statement::Select(select) = statement else {
			panic!("expected SELECT, got {statement:?}")
		};
		select.projection
	}

	let items = projection("SELECT t.*, u.name FROM t JOIN u");
	assert!(matches!(&items[0], SelectItem::QualifiedWildcard(parts, _) if *parts == ["t"]));
	assert!(matches!(&items[1], SelectItem::Expr { .. }));

	let items = projection("SELECT `t`.* FROM t");
	assert!(matches!(&items[0], SelectItem::QualifiedWildcard(parts, _) if *parts == ["t"]));

	let items = projection("SELECT db.t.* FROM db.t");
	assert!(
		matches!(&items[0], SelectItem::QualifiedWildcard(parts, _) if *parts == ["db", "t"]),
		"{items:?}"
	);

	// A quoted `*` is an identifier: the column of that name, not the wildcard.
	for sql in ["SELECT t.`*` FROM t", "SELECT `*` FROM t", "SELECT db.t.`*` FROM db.t"] {
		let items = projection(sql);
		assert!(matches!(&items[0], SelectItem::Expr { .. }), "{sql}: {items:?}");
	}
}

#[test]
fn test_keyword_function_forms_are_the_ordinary_calls() {
	for (sql, name, count) in [
		("EXTRACT(YEAR FROM ts)", "extract", 2),
		("POSITION('lo' IN 'Hello')", "position", 2),
		("SUBSTRING('Hello' FROM 2)", "substring", 2),
		("SUBSTRING('Hello' FROM 2 FOR 3)", "substring", 3),
		("MID('Hello', 2, 3)", "mid", 3),
		("DATE_ADD(d, INTERVAL 1 DAY)", "date_add", 2),
		("TIMESTAMPDIFF(DAY, a, b)", "timestampdiff", 3),
	] {
		let Expr::FunctionCall {
			name: parsed,
			args,
			..
		} = parse_expr(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
		else {
			panic!("{sql}: expected a function call");
		};
		assert_eq!((parsed.to_lowercase(), args.len()), (name.to_owned(), count), "{sql}");
	}
}
