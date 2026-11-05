//! A string literal keeps the backslash of `\%` and `\_` for the `LIKE` that reads it.

use surrealdb_mysql::lexer::token::TokenKind;
use surrealdb_mysql::lexer::tokenize;

fn strings(sql: &str) -> Vec<String> {
	tokenize(sql)
		.expect("tokenize")
		.into_iter()
		.filter_map(|token| {
			if let TokenKind::StringLiteral(text) = token.kind {
				Some(text)
			} else {
				None
			}
		})
		.collect()
}

#[test]
fn a_backslash_before_a_wildcard_stays_in_the_string() {
	assert_eq!(strings(r"'50\%' 'a\_b' '\%\_'"), [r"50\%", r"a\_b", r"\%\_"]);
	assert_eq!(strings(r#""50\%" "a\_b""#), [r"50\%", r"a\_b"]);
}

#[test]
fn every_other_escape_is_resolved() {
	assert_eq!(
		strings(r"'a\\b' 'a\qb' 'x\ny' 'x\0y' 'it\'s'"),
		[r"a\b", "aqb", "x\ny", "x\0y", "it's"]
	);
}

#[test]
fn an_escaped_backslash_before_a_wildcard_leaves_a_backslash_and_the_wildcard() {
	// `\\` is one backslash and the `%` after it is a character of its own, so the `LIKE` reads
	// `\%`, an escaped percent sign.
	assert_eq!(strings(r"'a\\%' 'a\\_'"), [r"a\%", r"a\_"]);
}
