//! The columns of the result a `SHOW` or `DESCRIBE` statement answers with, known before it runs.
//!
//! The rows of such a statement are objects, whose keys are sorted, so the columns of the result
//! set come from the statement: their names, their order and their types are MySQL's, and a
//! statement that has no rows to report still answers with a result set of its columns.

use surrealdb_mysql::translate::{ShowColumnKind, show_columns};
use surrealdb_sqlplan::{ResultColumn, ResultDescriptor, SqlType};

/// The descriptor of the result of `sql` when its first statement is a `SHOW`, `DESCRIBE` or
/// `EXPLAIN <table>`, and `None` otherwise. `database` is the session's database, which names the
/// `Tables_in_<database>` column of `SHOW TABLES`.
///
/// Only a statement that starts with one of those keywords is parsed, so the cost for any other
/// statement is the scan for its first word.
pub(crate) fn describe(sql: &str, database: &str) -> Option<ResultDescriptor> {
	if !starts_with_show_keyword(sql) {
		return None;
	}
	let stmts = surrealdb_mysql::parse_statements(sql).ok()?;
	let stmt = stmts.first()?;
	let columns = show_columns(stmt, database)?;
	Some(ResultDescriptor {
		columns: columns
			.into_iter()
			.map(|column| ResultColumn {
				name: column.name,
				ty: match column.kind {
					ShowColumnKind::Text => SqlType::Text,
					ShowColumnKind::Int => SqlType::Int4,
					ShowColumnKind::BigInt => SqlType::Int8,
				},
				..ResultColumn::default()
			})
			.collect(),
	})
}

/// Whether the first word of `sql`, after any leading whitespace and comments, is `SHOW`,
/// `DESCRIBE`, `DESC` or `EXPLAIN`.
fn starts_with_show_keyword(sql: &str) -> bool {
	let mut rest = sql;
	loop {
		rest = rest.trim_start();
		if let Some(comment) = rest.strip_prefix("/*") {
			let Some(end) = comment.find("*/") else {
				return false;
			};
			rest = &comment[end + 2..];
		} else if rest.starts_with("--") || rest.starts_with('#') {
			let Some(end) = rest.find('\n') else {
				return false;
			};
			rest = &rest[end + 1..];
		} else {
			break;
		}
	}
	let word_end = rest.find(|c: char| !c.is_ascii_alphabetic()).unwrap_or(rest.len());
	let word = &rest[..word_end];
	["SHOW", "DESCRIBE", "DESC", "EXPLAIN"].iter().any(|keyword| word.eq_ignore_ascii_case(keyword))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn names(sql: &str) -> Vec<String> {
		describe(sql, "shop")
			.unwrap_or_else(|| panic!("no descriptor for {sql}"))
			.columns
			.into_iter()
			.map(|column| column.name)
			.collect()
	}

	#[test]
	fn a_show_statement_is_described_in_mysqls_column_order() {
		assert_eq!(names("SHOW TABLES"), ["Tables_in_shop"]);
		assert_eq!(names("SHOW FULL TABLES"), ["Tables_in_shop", "Table_type"]);
		assert_eq!(names("DESCRIBE t"), ["Field", "Type", "Null", "Key", "Default", "Extra"]);
		assert_eq!(names("EXPLAIN t"), ["Field", "Type", "Null", "Key", "Default", "Extra"]);
		assert_eq!(names("SHOW WARNINGS"), ["Level", "Code", "Message"]);
	}

	#[test]
	fn a_column_of_integers_is_typed_as_one() {
		let descriptor = describe("SHOW INDEX FROM t", "shop").expect("descriptor");
		let ty = |name: &str| {
			descriptor.columns.iter().find(|column| column.name == name).map(|column| column.ty)
		};
		assert_eq!(ty("Table"), Some(SqlType::Text));
		assert_eq!(ty("Non_unique"), Some(SqlType::Int4));
		assert_eq!(ty("Cardinality"), Some(SqlType::Int8));
	}

	#[test]
	fn other_statements_have_no_show_descriptor() {
		for sql in ["SELECT 1", "INSERT INTO t VALUES (1)", "CREATE TABLE t (a INT)", ""] {
			assert!(describe(sql, "shop").is_none(), "{sql}");
		}
		// A `SHOW` that does not parse is the statement's own error to report.
		assert!(describe("SHOW NOTHING", "shop").is_none());
		// Only the first statement decides the columns.
		assert!(describe("SELECT 1; SHOW TABLES", "shop").is_none());
	}

	#[test]
	fn leading_comments_and_case_do_not_hide_a_show_statement() {
		assert!(starts_with_show_keyword("  /* tool */ show tables"));
		assert!(starts_with_show_keyword("-- tool\nSHOW TABLES"));
		assert!(starts_with_show_keyword("# tool\ndesc t"));
		assert!(!starts_with_show_keyword("/* unterminated SHOW TABLES"));
		assert!(!starts_with_show_keyword("SHOWN TABLES"));
		assert!(!starts_with_show_keyword("SELECT 'SHOW'"));
	}
}
