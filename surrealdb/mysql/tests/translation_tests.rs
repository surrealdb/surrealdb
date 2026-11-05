//! Translation unit tests for surrealdb-mysql.

use surrealdb_mysql::translate_sql;

#[test]
fn test_translate_select() {
	let sql = "SELECT id, name FROM users WHERE age >= 18 ORDER BY id DESC LIMIT 10 OFFSET 5";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(
		surql,
		"SELECT id, name FROM users WHERE (age >= 18) ORDER BY id DESC LIMIT 10 START 5;"
	);
}

#[test]
fn test_translate_select_limit_comma() {
	let sql = "SELECT * FROM users LIMIT 5, 10";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "SELECT * FROM users LIMIT 10 START 5;");
}

#[test]
fn test_translate_variables_and_null_safe_equal() {
	let sql = "SELECT @user_var, @@time_zone, a <=> b FROM tbl";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "SELECT $user_var, $__time_zone, (a == b) FROM tbl;");
}

#[test]
fn test_translate_dml_unsupported_for_typed_lowering() {
	let insert_sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob') ON DUPLICATE KEY UPDATE name = 'Updated'";
	assert!(matches!(
		translate_sql(insert_sql),
		Err(surrealdb_mysql::TranslationError::Unsupported(_))
	));

	let replace_sql = "REPLACE INTO users (id, name) VALUES (1, 'Alice')";
	assert!(matches!(
		translate_sql(replace_sql),
		Err(surrealdb_mysql::TranslationError::Unsupported(_))
	));

	let update_sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
	assert!(matches!(
		translate_sql(update_sql),
		Err(surrealdb_mysql::TranslationError::Unsupported(_))
	));

	let delete_sql = "DELETE FROM users WHERE id = 1";
	assert!(matches!(
		translate_sql(delete_sql),
		Err(surrealdb_mysql::TranslationError::Unsupported(_))
	));

	let truncate_sql = "TRUNCATE TABLE users";
	assert!(matches!(
		translate_sql(truncate_sql),
		Err(surrealdb_mysql::TranslationError::Unsupported(_))
	));
}

#[test]
fn test_translate_functions() {
	let sql = "SELECT NOW(), UUID(), MD5('test'), IFNULL(col, 0) FROM tbl";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(
		surql,
		"SELECT time::now(), rand::uuid::v4(), crypto::md5('test'), (col ?? 0) FROM tbl;"
	);
}

#[test]
fn test_translate_use_and_show() {
	let sql = "USE testdb";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "USE DB testdb;");

	let sql = "SHOW TABLES";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(
		surql,
		"{ RETURN object::keys((INFO FOR DB).tables)\
		 .map(|$t| object::from_entries([['Tables_in_' + session::db(), $t]])); }"
	);

	let sql = "SHOW COLUMNS FROM users";
	let surql = translate_sql(sql).unwrap();
	assert!(surql.contains("INFO FOR TABLE `users` STRUCTURE"), "{surql}");
}

#[test]
fn test_translate_transactions() {
	let sql = "START TRANSACTION";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "BEGIN TRANSACTION;");

	let surql = translate_sql("START TRANSACTION READ ONLY").unwrap();
	assert_eq!(surql, "BEGIN TRANSACTION READ ONLY;");

	let surql = translate_sql("START TRANSACTION READ WRITE").unwrap();
	assert_eq!(surql, "BEGIN TRANSACTION;");

	let surql = translate_sql("BEGIN").unwrap();
	assert_eq!(surql, "BEGIN TRANSACTION;");

	let sql = "COMMIT";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "COMMIT TRANSACTION;");

	let sql = "ROLLBACK";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "CANCEL TRANSACTION;");
}

#[test]
fn test_translate_ddl_unsupported_for_typed_lowering() {
	let create_sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))";
	assert!(matches!(
		translate_sql(create_sql),
		Err(surrealdb_mysql::TranslationError::Unsupported(_))
	));

	let drop_sql = "DROP TABLE IF EXISTS users";
	assert!(matches!(
		translate_sql(drop_sql),
		Err(surrealdb_mysql::TranslationError::Unsupported(_))
	));
}

#[test]
fn test_a_decimal_beyond_the_fixed_decimal_translates_to_a_bigdecimal() {
	for (sql, expected) in [
		("SELECT 79228162514264337593543950335", "79228162514264337593543950335dec"),
		("SELECT 79228162514264337593543950336", "79228162514264337593543950336bigdec"),
		("SELECT 123456789012345678901234567890", "123456789012345678901234567890bigdec"),
		("SELECT 0.1234567890123456789012345678", "0.1234567890123456789012345678dec"),
		("SELECT 0.12345678901234567890123456789", "0.12345678901234567890123456789bigdec"),
		("SELECT 1.5", "1.5dec"),
	] {
		let translated = translate_sql(sql).expect(sql);
		assert!(translated.contains(&format!(" {expected}")), "{sql}: {translated}");
	}
}
