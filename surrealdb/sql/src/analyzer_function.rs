//! Helpers for analyzer `FUNCTION` clause references (`fn::` and `mod::`).

use common::fmt::EscapeKwFreeIdent;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

/// Returns the fully qualified function name for display in errors.
pub fn qualified_name(name: &str) -> String {
	if name.starts_with("mod::") || name.starts_with("fn::") {
		name.to_owned()
	} else {
		format!("fn::{name}")
	}
}

pub fn fmt_analyzer_function(f: &mut String, sql_fmt: SqlFormat, name: &str) {
	let (kind, path) = if let Some(rest) = name.strip_prefix("mod::") {
		("mod", rest)
	} else if let Some(rest) = name.strip_prefix("fn::") {
		("fn", rest)
	} else {
		("fn", name)
	};

	write_sql!(f, sql_fmt, " FUNCTION {kind}");
	for segment in path.split("::") {
		f.push_str("::");
		EscapeKwFreeIdent(segment).fmt_sql(f, sql_fmt);
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::SqlFormat;

	use super::*;

	#[test]
	fn qualified_name_legacy_fn_suffix() {
		assert_eq!(qualified_name("foo::bar"), "fn::foo::bar");
	}

	#[test]
	fn qualified_name_mod_prefix() {
		assert_eq!(qualified_name("mod::demo::alter"), "mod::demo::alter");
	}

	#[test]
	fn fmt_analyzer_function_fn() {
		let mut sql = String::new();
		fmt_analyzer_function(&mut sql, SqlFormat::SingleLine, "foo::bar");
		assert_eq!(sql, " FUNCTION fn::foo::bar");
	}

	#[test]
	fn fmt_analyzer_function_mod() {
		let mut sql = String::new();
		fmt_analyzer_function(&mut sql, SqlFormat::SingleLine, "mod::demo::alter");
		assert_eq!(sql, " FUNCTION mod::demo::alter");
	}
}
