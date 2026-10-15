//! SurrealQL rendering for the statement writers, through the repository's
//! own escapers so that what is written parses back as the same value.

use common::fmt::{EscapeKwFreeIdent, QuoteStr};
use surrealdb_types::ToSql as _;

/// A SurrealQL string literal.
pub fn quote(s: &str) -> String {
	QuoteStr(s).to_sql()
}

/// A path segment after `ext::` or `silo::`, escaped when it is not a plain
/// identifier, starts with a digit, or reads as a number (`NaN`,
/// `Infinity`).
pub fn segment(s: &str) -> String {
	EscapeKwFreeIdent(s).to_sql()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn quote_escapes() {
		assert_eq!(quote("plain"), "'plain'");
		assert_eq!(quote("it's"), r#""it's""#);
		for s in ["a\"b'c\\d", "x\ny\r\tz", "\0\u{8}\u{c}"] {
			let v = crate::core::syn::value(&quote(s)).expect("parses");
			assert_eq!(v, surrealdb_types::Value::String(s.into()), "{s:?}");
		}
	}

	#[test]
	fn segments_escape_what_is_not_plain() {
		assert_eq!(segment("color"), "color");
		for (s, out) in [("co-lor", "`co-lor`"), ("9color", "`9color`"), ("NaN", "`NaN`")] {
			assert_eq!(segment(s), out);
		}
	}
}
