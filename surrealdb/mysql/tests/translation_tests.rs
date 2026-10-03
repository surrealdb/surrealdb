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
fn test_translate_insert() {
	let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob') ON DUPLICATE KEY UPDATE name = 'Updated'";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(
		surql,
		"INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob') ON DUPLICATE KEY UPDATE name = 'Updated';"
	);
}

#[test]
fn test_translate_replace() {
	let sql = "REPLACE INTO users (id, name) VALUES (1, 'Alice')";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "UPSERT INTO users (id, name) VALUES (1, 'Alice');");
}

#[test]
fn test_translate_update_delete() {
	let sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "UPDATE users SET name = 'Bob' WHERE (id == 1);");

	let sql = "DELETE FROM users WHERE id = 1";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "DELETE FROM users WHERE (id == 1);");
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
	assert_eq!(surql, "INFO FOR DB;");

	let sql = "SHOW COLUMNS FROM users";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "INFO FOR TABLE users;");
}

#[test]
fn test_translate_transactions() {
	let sql = "START TRANSACTION";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "BEGIN TRANSACTION;");

	let sql = "COMMIT";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "COMMIT TRANSACTION;");

	let sql = "ROLLBACK";
	let surql = translate_sql(sql).unwrap();
	assert_eq!(surql, "CANCEL TRANSACTION;");
}
