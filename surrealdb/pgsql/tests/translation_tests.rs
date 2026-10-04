//! Tests for the PostgreSQL to SurrealQL translation / lowering layer $\tau$ (Phase 7).

use surrealdb_pgsql::{ParamStyle, TranslationContext, translate_sql, translate_sql_with_context};

#[test]
fn test_translate_simple_select() {
	let sql = "SELECT 1;";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(surrealql, "RETURN [{ \"?column?\": 1 }];");

	let sql = "SELECT 1, 'hello', true, NULL;";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(
		surrealql,
		"RETURN [{ \"?column?_1\": 1, \"?column?_2\": 'hello', \"?column?_3\": true, \"?column?_4\": NONE }];"
	);
}

#[test]
fn test_translate_select_with_clauses() {
	let sql = "SELECT id, name AS user_name FROM users WHERE active = true ORDER BY id DESC LIMIT 10 OFFSET 20;";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(
		surrealql,
		"SELECT id, name AS user_name FROM users WHERE (active = true) ORDER BY id DESC LIMIT 10 START 20;"
	);
}

#[test]
fn test_translate_parameters() {
	let sql = "SELECT * FROM users WHERE id = $1 AND tenant_id = $2;";

	// Default style: $_1, $_2 (SurrealDB server PG wire convention)
	let res_default = translate_sql(sql).expect("translate");
	assert_eq!(res_default, "SELECT * FROM users WHERE ((id = $_1) AND (tenant_id = $_2));");

	// Verbatim style: $1, $2
	let ctx_verbatim = TranslationContext::new().with_param_style(ParamStyle::Verbatim);
	let res_verbatim = translate_sql_with_context(sql, &ctx_verbatim).expect("translate");
	assert_eq!(res_verbatim, "SELECT * FROM users WHERE ((id = $1) AND (tenant_id = $2));");

	// Named prefix: $p1, $p2
	let ctx_named =
		TranslationContext::new().with_param_style(ParamStyle::NamedPrefix("p".to_string()));
	let res_named = translate_sql_with_context(sql, &ctx_named).expect("translate");
	assert_eq!(res_named, "SELECT * FROM users WHERE ((id = $p1) AND (tenant_id = $p2));");
}

#[test]
fn test_translate_functions_and_builtins() {
	let sql =
		"SELECT now(), length(name), lower(name), upper(name), coalesce(bio, 'empty') FROM users;";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(
		surrealql,
		"SELECT time::now(), string::len(name), string::lowercase(name), string::uppercase(name), type::coalesce(bio, 'empty') FROM users;"
	);
}

#[test]
fn test_translate_introspection() {
	let sql = "SELECT current_database(), current_schema(), version();";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(
		surrealql,
		"RETURN [{ \"?column?_1\": db(), \"?column?_2\": 'public', \"?column?_3\": 'SurrealDB (PostgreSQL Frontend)' }];"
	);
}

#[test]
fn test_translate_operators() {
	let sql = "SELECT first_name || ' ' || last_name FROM users;";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(surrealql, "SELECT ((first_name + ' ') + last_name) FROM users;");

	let sql = "SELECT * FROM users WHERE name LIKE 'A%' AND email ILIKE '%@example.com';";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(
		surrealql,
		"SELECT * FROM users WHERE ((name ~ 'A%') AND (email ~* '%@example.com'));"
	);

	let sql = "SELECT * FROM users WHERE deleted_at IS NULL AND verified_at IS NOT NULL;";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(
		surrealql,
		"SELECT * FROM users WHERE ((deleted_at = NONE) AND (verified_at != NONE));"
	);
}

#[test]
fn test_translate_between_and_in() {
	let sql = "SELECT * FROM products WHERE price BETWEEN 10 AND 50 AND category_id IN (1, 2, 3);";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(
		surrealql,
		"SELECT * FROM products WHERE ((price BETWEEN 10 AND 50) AND (category_id IN [1, 2, 3]));"
	);
}

#[test]
fn test_translate_typecasts() {
	let sql = "SELECT id::int, active::bool, score::float, created_at::timestamptz FROM records;";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(
		surrealql,
		"SELECT <int> (id), <bool> (active), <float> (score), <datetime> (created_at) FROM records;"
	);
}

#[test]
fn test_translate_case_expression() {
	let sql = "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'F' END FROM students;";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(
		surrealql,
		"SELECT (IF (score >= 90) THEN 'A' ELSE IF (score >= 80) THEN 'B' ELSE 'F' END) FROM students;"
	);
}

#[test]
fn test_translate_dml_unsupported_for_typed_lowering() {
	let insert_sql =
		"INSERT INTO users (id, name, active) VALUES (1, 'Alice', true), (2, 'Bob', false);";
	assert!(matches!(
		translate_sql(insert_sql),
		Err(surrealdb_pgsql::TranslationError::Unsupported(_))
	));

	let update_sql = "UPDATE users SET name = 'Alice', updated_at = now() WHERE id = 1;";
	assert!(matches!(
		translate_sql(update_sql),
		Err(surrealdb_pgsql::TranslationError::Unsupported(_))
	));

	let delete_sql = "DELETE FROM users WHERE id = 1;";
	assert!(matches!(
		translate_sql(delete_sql),
		Err(surrealdb_pgsql::TranslationError::Unsupported(_))
	));

	let truncate_sql = "TRUNCATE TABLE users;";
	assert!(matches!(
		translate_sql(truncate_sql),
		Err(surrealdb_pgsql::TranslationError::Unsupported(_))
	));
}

#[test]
fn test_translate_transactions() {
	assert_eq!(translate_sql("BEGIN;").expect("translate"), "BEGIN TRANSACTION;");
	assert_eq!(translate_sql("COMMIT;").expect("translate"), "COMMIT TRANSACTION;");
	assert_eq!(translate_sql("ROLLBACK;").expect("translate"), "CANCEL TRANSACTION;");
}

#[test]
fn test_translate_create_and_drop_table() {
	let sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT);";
	let surrealql = translate_sql(sql).expect("translate");
	assert!(surrealql.contains("DEFINE TABLE users SCHEMAFULL;"));
	assert!(surrealql.contains("DEFINE FIELD id ON TABLE users TYPE int;"));
	assert!(surrealql.contains("DEFINE INDEX users_id_pk ON TABLE users COLUMNS id UNIQUE;"));
	assert!(
		surrealql.contains("DEFINE FIELD name ON TABLE users TYPE string ASSERT $value != NONE;")
	);
	assert!(surrealql.contains("DEFINE FIELD email ON TABLE users TYPE string;"));

	let sql = "DROP TABLE IF EXISTS users;";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(surrealql, "REMOVE TABLE IF EXISTS users;");
}

#[test]
fn test_translate_joins() {
	let sql = "SELECT u.name, o.total FROM users AS u JOIN orders AS o ON u.id = o.user_id;";
	let surrealql = translate_sql(sql).expect("translate");
	assert_eq!(
		surrealql,
		"SELECT u.name, o.total FROM users AS u JOIN orders AS o ON (u.id = o.user_id);"
	);
}
