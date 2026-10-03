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
