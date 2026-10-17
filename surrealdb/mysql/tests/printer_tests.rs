//! Printer unit tests and round-trip parsing tests for surrealdb-mysql.

use surrealdb_mysql::parser::parse_statement;
use surrealdb_mysql::printer::ToMySql;

#[test]
fn test_round_trip_select() {
	let sql = "SELECT `id`, `name` FROM `users` WHERE `age` >= 18 ORDER BY `id` DESC LIMIT 5, 10";
	let stmt = parse_statement(sql).unwrap();
	let printed = stmt.to_mysql_string();
	let stmt2 = parse_statement(&printed).unwrap();

	let mut s1 = stmt;
	let mut s2 = stmt2;
	s1.normalize_spans();
	s2.normalize_spans();
	assert_eq!(s1, s2);
}

#[test]
fn test_round_trip_insert() {
	let sql = "INSERT INTO `users` (`id`, `name`) VALUES (1, 'Alice'), (2, 'Bob') ON DUPLICATE KEY UPDATE `name` = 'Charlie'";
	let stmt = parse_statement(sql).unwrap();
	let printed = stmt.to_mysql_string();
	let stmt2 = parse_statement(&printed).unwrap();

	let mut s1 = stmt;
	let mut s2 = stmt2;
	s1.normalize_spans();
	s2.normalize_spans();
	assert_eq!(s1, s2);
}

#[test]
fn test_round_trip_update_delete() {
	let sql = "UPDATE `users` SET `name` = 'David' WHERE `id` = 3";
	let stmt = parse_statement(sql).unwrap();
	let printed = stmt.to_mysql_string();
	let stmt2 = parse_statement(&printed).unwrap();
	let mut s1 = stmt;
	let mut s2 = stmt2;
	s1.normalize_spans();
	s2.normalize_spans();
	assert_eq!(s1, s2);

	let sql = "DELETE FROM `users` WHERE `id` = 4";
	let stmt = parse_statement(sql).unwrap();
	let printed = stmt.to_mysql_string();
	let stmt2 = parse_statement(&printed).unwrap();
	let mut s1 = stmt;
	let mut s2 = stmt2;
	s1.normalize_spans();
	s2.normalize_spans();
	assert_eq!(s1, s2);
}

#[test]
fn test_round_trip_ddl_tx() {
	let sql =
		"CREATE TABLE IF NOT EXISTS `users` (`id` INT PRIMARY KEY, `name` VARCHAR(255) NOT NULL)";
	let stmt = parse_statement(sql).unwrap();
	let printed = stmt.to_mysql_string();
	let stmt2 = parse_statement(&printed).unwrap();
	let mut s1 = stmt;
	let mut s2 = stmt2;
	s1.normalize_spans();
	s2.normalize_spans();
	assert_eq!(s1, s2);

	let sql = "START TRANSACTION READ ONLY";
	let stmt = parse_statement(sql).unwrap();
	let printed = stmt.to_mysql_string();
	let stmt2 = parse_statement(&printed).unwrap();
	let mut s1 = stmt;
	let mut s2 = stmt2;
	s1.normalize_spans();
	s2.normalize_spans();
	assert_eq!(s1, s2);
}

#[test]
fn test_round_trip_window() {
	let sql = "SELECT row_number() OVER (PARTITION BY `dept` ORDER BY `salary` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM `employees` WINDOW `w` AS (PARTITION BY `dept`)";
	let stmt = parse_statement(sql).unwrap();
	let printed = stmt.to_mysql_string();
	let stmt2 = parse_statement(&printed).unwrap();
	let mut s1 = stmt;
	let mut s2 = stmt2;
	s1.normalize_spans();
	s2.normalize_spans();
	assert_eq!(s1, s2);
}

#[test]
fn test_round_trip_m01_extended() {
	let test_sqls = [
		"SELECT 1 WHERE `x` = ANY (SELECT `y` FROM `t`)",
		"SELECT 1 WHERE `x` < ALL (SELECT `y` FROM `t`)",
		"SELECT `dep`, SUM(`salary`) FROM `employees` GROUP BY `dep` WITH ROLLUP",
		"SELECT * FROM `users` USE INDEX (`idx1`)",
		"SHOW FULL TABLES",
		"SHOW FULL COLUMNS FROM `users`",
		"SHOW INDEX FROM `users`",
		"SHOW ENGINES",
		"SHOW COLLATION",
		"SHOW CHARACTER SET",
		"CREATE DATABASE IF NOT EXISTS `mydb`",
		"DROP DATABASE IF EXISTS `mydb`",
		"CREATE INDEX `idx_user` ON `users` (`email`, `created_at`)",
		"CREATE UNIQUE INDEX `uq_email` ON `users` (`email`)",
		"DROP INDEX `idx_user` ON `users`",
		"CREATE VIEW `my_view` AS SELECT `id` FROM `t`",
		"CREATE OR REPLACE VIEW `my_view` AS SELECT `id` FROM `t`",
		"DROP VIEW IF EXISTS `v1`, `v2`",
		"ALTER TABLE `users` ADD COLUMN `age` INT, DROP COLUMN `legacy`, RENAME TO `customers`",
		"CREATE USER IF NOT EXISTS 'admin'@'localhost' IDENTIFIED BY 'secret'",
		"DROP USER IF EXISTS 'admin'@'localhost'",
		"GRANT ALL ON *.* TO 'admin'@'localhost'",
		"REVOKE ALL ON *.* FROM 'admin'@'localhost'",
		"LOCK TABLES `t1` READ, `t2` WRITE",
		"UNLOCK TABLES",
		"PREPARE `stmt1` FROM 'SELECT * FROM users WHERE id = ?'",
		"EXECUTE `stmt1` USING @val",
		"DEALLOCATE PREPARE `stmt1`",
		"SELECT _utf8mb4'hello', _binary 0x01AF",
	];

	for sql in test_sqls {
		let stmt = parse_statement(sql)
			.unwrap_or_else(|e| panic!("Failed to parse initial SQL: {sql}, error: {e}"));
		let printed = stmt.to_mysql_string();
		let stmt2 = parse_statement(&printed)
			.unwrap_or_else(|e| panic!("Failed to parse printed SQL: {printed}, error: {e}"));
		let mut s1 = stmt;
		let mut s2 = stmt2;
		s1.normalize_spans();
		s2.normalize_spans();
		assert_eq!(s1, s2, "Mismatch on round-trip for: {sql}");
	}
}
