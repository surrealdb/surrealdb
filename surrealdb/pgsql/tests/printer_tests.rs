//! Round-trip fidelity and formatting tests for PostgreSQL AST printer.
//!
//! Verifies:
//! 1. `parse(print(ast)) == ast` (round-trip semantic equivalence)
//! 2. `print(parse(print(ast))) == print(ast)` (formatting idempotence)

use surrealdb_pgsql::{ToPgSql, parse_expr, parse_statement};

fn assert_expr_roundtrip(sql: &str) {
	let mut ast1 = parse_expr(sql).unwrap_or_else(|e| panic!("failed to parse expr '{sql}': {e}"));
	let printed1 = ast1.to_pgsql_string();
	let mut ast2 = parse_expr(&printed1).unwrap_or_else(|e| {
		panic!("failed to reparse printed expr '{printed1}' (original: '{sql}'): {e}")
	});
	let printed2 = ast2.to_pgsql_string();

	assert_eq!(printed1, printed2, "idempotent printing failed for expr: {sql}");

	ast1.normalize_spans();
	ast2.normalize_spans();
	assert_eq!(ast1, ast2, "AST round-trip equality failed for expr: {sql}");
}

fn assert_stmt_roundtrip(sql: &str) {
	let mut ast1 =
		parse_statement(sql).unwrap_or_else(|e| panic!("failed to parse statement '{sql}': {e}"));
	let printed1 = ast1.to_pgsql_string();
	let mut ast2 = parse_statement(&printed1).unwrap_or_else(|e| {
		panic!("failed to reparse printed stmt '{printed1}' (original: '{sql}'): {e}")
	});
	let printed2 = ast2.to_pgsql_string();

	assert_eq!(printed1, printed2, "idempotent printing failed for statement: {sql}");

	ast1.normalize_spans();
	ast2.normalize_spans();
	assert_eq!(ast1, ast2, "AST round-trip equality failed for statement: {sql}");
}

#[test]
fn test_print_literals() {
	assert_expr_roundtrip("42");
	assert_expr_roundtrip("12.34");
	assert_expr_roundtrip("'hello world'");
	assert_expr_roundtrip("'hello ''escaped'' world'");
	assert_expr_roundtrip("TRUE");
	assert_expr_roundtrip("FALSE");
	assert_expr_roundtrip("NULL");
	assert_expr_roundtrip("B'1010'");
}

#[test]
fn test_print_precedence_and_associativity() {
	// Precedence preservation: addition inside multiplication requires parens
	assert_expr_roundtrip("(1 + 2) * 3");
	// Higher precedence inside lower precedence does not require parens
	assert_expr_roundtrip("1 + 2 * 3");
	// Right associativity of exponent: 2 ^ (3 ^ 4)
	assert_expr_roundtrip("2 ^ 3 ^ 4");
	// Left grouping of exponent requires parens: (2 ^ 3) ^ 4
	assert_expr_roundtrip("(2 ^ 3) ^ 4");
}

#[test]
fn test_print_operators_and_predicates() {
	assert_expr_roundtrip("a AND b OR c AND d");
	assert_expr_roundtrip("NOT x AND y");
	assert_expr_roundtrip("a IS NULL");
	assert_expr_roundtrip("b IS NOT NULL");
	assert_expr_roundtrip("c IS TRUE");
	assert_expr_roundtrip("d IS NOT FALSE");
	assert_expr_roundtrip("x IS DISTINCT FROM y");
	assert_expr_roundtrip("x IS NOT DISTINCT FROM y");
	assert_expr_roundtrip("x BETWEEN 1 AND 10");
	assert_expr_roundtrip("x NOT BETWEEN SYMMETRIC 10 AND 1");
	assert_expr_roundtrip("x IN (1, 2, 3)");
	assert_expr_roundtrip("x NOT IN (4, 5, 6)");
}

#[test]
fn test_print_complex_expressions() {
	assert_expr_roundtrip("count(*)");
	assert_expr_roundtrip("substring('hello', 1, 3)");
	assert_expr_roundtrip("count(DISTINCT id)");
	assert_expr_roundtrip("x::int4");
	assert_expr_roundtrip("(a + b)::text");
	assert_expr_roundtrip("arr[1]");
	assert_expr_roundtrip("arr[1:5]");
	assert_expr_roundtrip("CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END");
	assert_expr_roundtrip("CASE x WHEN 1 THEN 'one' END");
	assert_expr_roundtrip("ARRAY[1, 2, 3]");
	assert_expr_roundtrip("ROW(1, 'a', TRUE)");
}

#[test]
fn test_roundtrip_select_statements() {
	assert_stmt_roundtrip("SELECT 1");
	assert_stmt_roundtrip(
		"SELECT id, name AS user_name, age FROM users WHERE age >= 18 GROUP BY id, name HAVING count(*) > 1 ORDER BY age DESC NULLS LAST LIMIT 10 OFFSET 5",
	);
	assert_stmt_roundtrip("SELECT DISTINCT ON (dept) dept, salary FROM emp");
	assert_stmt_roundtrip(
		"SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id LEFT JOIN t3 USING (dept_id)",
	);
	assert_stmt_roundtrip("SELECT * FROM (SELECT id FROM users) AS u");
	assert_stmt_roundtrip("SELECT 1 UNION ALL SELECT 2");
}

#[test]
fn test_roundtrip_insert_statements() {
	assert_stmt_roundtrip("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')");
	assert_stmt_roundtrip(
		"INSERT INTO users (id, name) VALUES (1, 'alice') ON CONFLICT (id) DO UPDATE SET name = 'alice_new'",
	);
	assert_stmt_roundtrip("INSERT INTO users (id) VALUES (1) ON CONFLICT DO NOTHING RETURNING id");
}

#[test]
fn test_roundtrip_update_statements() {
	assert_stmt_roundtrip(
		"UPDATE users SET name = 'bob', updated_at = 123 WHERE id = 1 RETURNING *",
	);
}

#[test]
fn test_roundtrip_delete_statements() {
	assert_stmt_roundtrip("DELETE FROM users WHERE id = 1 RETURNING id");
	assert_stmt_roundtrip(
		"DELETE FROM users USING blacklisted WHERE users.email = blacklisted.email RETURNING users.id",
	);
}

#[test]
fn test_roundtrip_create_table_statements() {
	assert_stmt_roundtrip(
		"CREATE TEMPORARY TABLE IF NOT EXISTS users (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE, created_at INT DEFAULT 0, org_id INT REFERENCES orgs (id), CONSTRAINT fk_org FOREIGN KEY (org_id) REFERENCES orgs (id))",
	);
}

#[test]
fn test_roundtrip_drop_statements() {
	assert_stmt_roundtrip("DROP TABLE IF EXISTS users, profiles CASCADE");
}

#[test]
fn test_roundtrip_transactions() {
	assert_stmt_roundtrip("BEGIN");
	assert_stmt_roundtrip("COMMIT");
	assert_stmt_roundtrip("ROLLBACK");
	assert_stmt_roundtrip("SAVEPOINT sp1");
	assert_stmt_roundtrip("RELEASE SAVEPOINT sp1");
	assert_stmt_roundtrip("ROLLBACK TO SAVEPOINT sp1");
}

#[test]
fn test_roundtrip_misc_statements() {
	assert_stmt_roundtrip("SET timezone TO 'UTC'");
	assert_stmt_roundtrip("SHOW client_encoding");
	assert_stmt_roundtrip("RESET ALL");
	assert_stmt_roundtrip("EXPLAIN (analyze, verbose) SELECT 1");
}
