//! Session state for MySQL user variables (`SET @a = ...`) and named prepared statements
//! (`PREPARE s FROM ...`, `EXECUTE s USING @a`).
//!
//! A user variable holds the text a later statement splices in for it. A reference is replaced by
//! that text before the statement is planned, so the planner only ever sees literals. A number is
//! spliced as written, any other value as a quoted string, and an unset variable as `NULL`.

use std::collections::HashMap;

use surrealdb_mysql::{Token, TokenKind};
use surrealdb_types::Value;

/// A value held in a user variable, stored as the SQL literal that stands for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UserValue(String);

impl UserValue {
	/// The literal for a value the engine returned and the MySQL text of it.
	pub(crate) fn from_result(value: &Value, text: Option<&str>) -> Self {
		let Some(text) = text else {
			return Self::null();
		};
		if matches!(value, Value::None | Value::Null) {
			Self::null()
		} else if matches!(value, Value::Number(_)) {
			Self(text.to_string())
		} else if let Value::Bytes(bytes) = value {
			let mut literal = String::with_capacity(bytes.len() * 2 + 3);
			literal.push_str("X'");
			for byte in bytes.iter() {
				literal.push_str(&format!("{byte:02X}"));
			}
			literal.push('\'');
			Self(literal)
		} else if let Value::Bool(b) = value {
			Self(
				if *b {
					"1"
				} else {
					"0"
				}
				.to_string(),
			)
		} else {
			Self(quote(text))
		}
	}

	pub(crate) fn null() -> Self {
		Self("NULL".to_string())
	}

	pub(crate) fn literal(&self) -> &str {
		&self.0
	}

	/// The string a `PREPARE ... FROM @var` reads its statement text from, if the variable holds
	/// one.
	pub(crate) fn as_statement_text(&self) -> Option<String> {
		unquote(&self.0)
	}
}

fn quote(text: &str) -> String {
	let mut out = String::with_capacity(text.len() + 2);
	out.push('\'');
	for c in text.chars() {
		match c {
			'\'' => out.push_str("''"),
			'\\' => out.push_str("\\\\"),
			'\0' => out.push_str("\\0"),
			c => out.push(c),
		}
	}
	out.push('\'');
	out
}

fn unquote(literal: &str) -> Option<String> {
	let inner = literal.strip_prefix('\'')?.strip_suffix('\'')?;
	let mut out = String::with_capacity(inner.len());
	let mut chars = inner.chars().peekable();
	while let Some(c) = chars.next() {
		match (c, chars.peek()) {
			('\'', Some('\'')) => {
				chars.next();
				out.push('\'');
			}
			('\\', Some('\\')) => {
				chars.next();
				out.push('\\');
			}
			('\\', Some('0')) => {
				chars.next();
				out.push('\0');
			}
			(c, _) => out.push(c),
		}
	}
	Some(out)
}

/// The user variables and named prepared statements of one connection. Names are case-insensitive.
#[derive(Debug, Default)]
pub(crate) struct UserState {
	variables: HashMap<String, UserValue>,
	statements: HashMap<String, String>,
}

impl UserState {
	pub(crate) fn set(&mut self, name: &str, value: UserValue) {
		self.variables.insert(name.to_ascii_lowercase(), value);
	}

	pub(crate) fn get(&self, name: &str) -> UserValue {
		self.variables.get(&name.to_ascii_lowercase()).cloned().unwrap_or_else(UserValue::null)
	}

	/// The variables as they are now, for [`UserState::restore_variables`] to return to.
	pub(crate) fn variables(&self) -> HashMap<String, UserValue> {
		self.variables.clone()
	}

	pub(crate) fn restore_variables(&mut self, variables: HashMap<String, UserValue>) {
		self.variables = variables;
	}

	pub(crate) fn prepare(&mut self, name: &str, sql: String) {
		self.statements.insert(name.to_ascii_lowercase(), sql);
	}

	pub(crate) fn statement(&self, name: &str) -> Option<&str> {
		self.statements.get(&name.to_ascii_lowercase()).map(String::as_str)
	}

	pub(crate) fn deallocate(&mut self, name: &str) -> bool {
		self.statements.remove(&name.to_ascii_lowercase()).is_some()
	}

	pub(crate) fn clear(&mut self) {
		self.variables.clear();
		self.statements.clear();
	}

	/// Replace each `@name` reference in `sql` with its literal. `None` when there is none.
	pub(crate) fn substitute(&self, sql: &str) -> Option<String> {
		if !sql.contains('@') {
			return None;
		}
		let tokens = surrealdb_mysql::tokenize(sql).ok()?;
		splice(sql, &tokens, |t| {
			if let TokenKind::UserVar(name) = &t.kind {
				Some(self.get(name).0)
			} else {
				None
			}
		})
	}
}

/// Replace each `?` placeholder in `sql` with the next literal. `None` when the count differs.
pub(crate) fn bind_placeholders(sql: &str, literals: &[String]) -> Option<String> {
	let tokens = surrealdb_mysql::tokenize(sql).ok()?;
	let count = tokens.iter().filter(|t| matches!(t.kind, TokenKind::Param(_))).count();
	if count != literals.len() {
		return None;
	}
	let mut next = literals.iter();
	let out = splice(sql, &tokens, |t| {
		if matches!(t.kind, TokenKind::Param(_)) {
			next.next().cloned()
		} else {
			None
		}
	});
	Some(out.unwrap_or_else(|| sql.to_string()))
}

fn splice(
	sql: &str,
	tokens: &[Token],
	mut replace: impl FnMut(&Token) -> Option<String>,
) -> Option<String> {
	let mut out = String::with_capacity(sql.len());
	let mut at = 0usize;
	let mut changed = false;
	for token in tokens {
		let start = token.span.start;
		let end = token.span.end;
		if start < at || end > sql.len() {
			continue;
		}
		if let Some(text) = replace(token) {
			out.push_str(&sql[at..start]);
			out.push_str(&text);
			at = end;
			changed = true;
		}
	}
	if !changed {
		return None;
	}
	out.push_str(&sql[at..]);
	Some(out)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn quoting_round_trips() {
		for text in ["plain", "it's", "back\\slash", ""] {
			assert_eq!(unquote(&quote(text)).as_deref(), Some(text));
		}
	}

	#[test]
	fn references_are_replaced_by_literals() {
		let mut state = UserState::default();
		state.set("A", UserValue("5".into()));
		state.set("s", UserValue(quote("x'y")));
		assert_eq!(state.substitute("select @a, @S, @unset").unwrap(), "select 5, 'x''y', NULL");
		assert_eq!(state.substitute("select 1"), None);
		assert_eq!(state.substitute("select '@a'"), None);
	}

	#[test]
	fn placeholders_are_bound_in_order() {
		let lits = vec!["5".to_string(), "'b'".to_string()];
		assert_eq!(
			bind_placeholders("select '?' from t where a > ? and b = ?", &lits).unwrap(),
			"select '?' from t where a > 5 and b = 'b'"
		);
		assert_eq!(bind_placeholders("select ?", &lits), None);
	}

	#[test]
	fn bytes_keep_their_value_as_a_hex_literal() {
		let value = Value::Bytes(surrealdb_types::Bytes::from(vec![0xFF, 0x00, 0x10]));
		assert_eq!(UserValue::from_result(&value, Some("lossy")).literal(), "X'FF0010'");
	}

	#[test]
	fn variables_return_to_a_snapshot() {
		let mut state = UserState::default();
		state.set("a", UserValue("1".into()));
		let before = state.variables();
		state.set("a", UserValue("2".into()));
		state.set("b", UserValue("3".into()));
		state.restore_variables(before);
		assert_eq!(state.get("a").literal(), "1");
		assert_eq!(state.get("b").literal(), "NULL");
	}

	#[test]
	fn statements_are_case_insensitive() {
		let mut state = UserState::default();
		state.prepare("S", "select 1".into());
		assert_eq!(state.statement("s"), Some("select 1"));
		assert!(state.deallocate("s"));
		assert!(!state.deallocate("s"));
	}
}
