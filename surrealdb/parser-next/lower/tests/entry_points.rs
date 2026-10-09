//! The single-value entry points: durations, datetimes, table and function
//! names.

use surrealdb_parser_next_lower::{
	ParserConfig, ParserSettings, datetime, duration, function_with_settings, table,
};

#[test]
fn durations() {
	assert_eq!(duration("1h30m").expect("valid").to_string(), "1h30m");
	assert_eq!(duration(" 2w ").expect("valid").to_string(), "2w");
	for bad in ["1h x", "1x", "99999999999999999999y", "'1h'", ""] {
		assert!(duration(bad).is_err(), "{bad:?}");
	}
}

#[test]
fn datetimes() {
	assert!(datetime("2024-01-02T03:04:05Z").is_ok());
	for bad in ["2024-13-01", "2024-01-02T03:04:05+25:00", "x"] {
		let err = datetime(bad).expect_err(bad).to_string();
		assert!(err.contains("datetime") || err.contains("Datetime"), "{bad:?}: {err}");
	}
}

#[test]
fn table_names() {
	assert_eq!(table("person").expect("valid").as_str(), "person");
	assert_eq!(table("`a b`").expect("valid").as_str(), "a b");
	// A keyword names a table, and anything after the name is ignored.
	assert_eq!(table("select").expect("valid").as_str(), "select");
	assert_eq!(table("person rest").expect("valid").as_str(), "person");
	assert!(table("1").is_err());
	assert!(table("").is_err());
}

#[test]
fn function_names() {
	let settings = ParserSettings::all_features(&ParserConfig::default());
	let name = |f: &str| function_with_settings(f, settings).map(|f| format!("{f:?}"));
	assert_eq!(name("string::len").expect("valid"), r#"Normal("string::len")"#);
	assert_eq!(name("fn::a::b").expect("valid"), r#"Custom("a::b")"#);
	assert_eq!(name("fn::`x y`").expect("valid"), r#"Custom("x y")"#);
	assert!(name("mod::m::f").is_ok());
	assert!(name("silo::o::p::<1.2.3>::f").is_ok());
	assert!(name("ml::model<1.2.3>").is_ok());
	assert_eq!(name("count rest").expect("valid"), r#"Normal("count")"#);
	assert!(name("fn").is_err());
	assert!(name("1").is_err());
	// Surrealism modules need the grammar enabled.
	assert!(function_with_settings("mod::m::f", ParserSettings::default()).is_err());
}

/// UTF-16 surrogate pairs in `\u` escapes are JSON; SurrealQL strings reject
/// them.
#[test]
fn surrogate_pairs_are_json_only() {
	let text = r#""\uD83D\uDE00""#;
	assert!(surrealdb_parser_next_lower::value(text).is_err());
	assert!(surrealdb_parser_next_lower::expr(text).is_err());
	let json = ParserSettings {
		json_string_escapes: true,
		..Default::default()
	};
	assert!(surrealdb_parser_next_lower::json_with_settings(text, json).is_ok());
}

/// Duration digits past `u64::MAX` are an error, however many there are.
#[test]
fn overlong_duration_digits_are_an_error() {
	let text = format!("{}y", "8".repeat(90));
	assert!(duration(&text).is_err());
	assert!(surrealdb_parser_next_lower::parse(&format!("RETURN {text};")).is_err());
}

/// A silo package and a file source lower as what they are, whichever
/// statements precede them.
#[test]
fn module_executables_lower_by_kind() {
	let src = include_str!("../../tests/corpus/module_executables.surql");
	let _ = surrealdb_parser_next_lower::parse(src);
	let ast = surrealdb_parser_next_lower::parse(
		"DEFINE MODULE mod::test FROM f\"bucket:/module.wasm\" UNSIGNED;\n\
		 DEFINE MODULE silo::acme::pkg::<1.0.0> UNSIGNED;",
	)
	.expect("both forms parse");
	assert_eq!(ast.expressions.len(), 2);
}

/// An escaped path segment is one segment, `::` and all.
#[test]
fn escaped_path_segments_may_hold_path_separators() {
	use surrealdb_types::ToSql;
	for text in ["REMOVE FUNCTION fn::`a::b`", "RETURN fn::`a::b`::c()"] {
		let once = surrealdb_parser_next_lower::parse(text).expect("valid").to_sql();
		let twice = surrealdb_parser_next_lower::parse(&once).expect("printed").to_sql();
		assert_eq!(once, twice, "{text}");
	}
	let name = function_with_settings("fn::`a::b`", ParserSettings::default())
		.map(|f| format!("{f:?}"))
		.expect("valid");
	assert_eq!(name, r#"Custom("a::b")"#);
}

/// A character that starts no token is rejected whole, wherever it is read.
#[test]
fn stray_non_ascii_characters_are_rejected() {
	for text in
		["ALTER SYSTEM DROP\u{0BAA}", "SELECT \u{0BAA} FROM t", "a.\u{0BAA}", "RETURN 1\u{0BAA}"]
	{
		assert!(surrealdb_parser_next_lower::parse(text).is_err(), "{text}");
	}
}
