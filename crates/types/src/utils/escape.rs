use std::str::Chars;

use crate::{SqlFormat, ToSql};

#[derive(Clone)]
pub struct Escape<'a> {
	chars: Chars<'a>,
	pending: Option<char>,
	escape_char: char,
}

impl<'a> Escape<'a> {
	pub fn escape_str(s: &'a str, escape_char: char) -> Self {
		Escape {
			chars: s.chars(),
			pending: None,
			escape_char,
		}
	}
}

impl Iterator for Escape<'_> {
	type Item = char;

	fn next(&mut self) -> Option<char> {
		if let Some(x) = self.pending.take() {
			return Some(x);
		}
		let next = self.chars.next()?;
		if next == self.escape_char || next == '\\' {
			self.pending = Some(next);
			return Some('\\');
		}
		Some(next)
	}
}

impl ToSql for Escape<'_> {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		for x in self.clone() {
			f.push(x);
		}
	}
}

pub struct QuoteStr<'a>(pub &'a str);
impl ToSql for QuoteStr<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let s = self.0;
		let quote = if s.contains('\'') {
			'\"'
		} else {
			'\''
		};

		f.push(quote);
		Escape::escape_str(s, quote).fmt_sql(f, fmt);
		f.push(quote);
	}
}

/// Escapes identifiers for use in SQON (SQL Object Notation).
pub struct EscapeSqonIdent<'a>(pub &'a str);
impl ToSql for EscapeSqonIdent<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let s = self.0;
		// Not a keyword, any non 'normal' characters or does it start with a digit?
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
		{
			f.push('`');
			Escape::escape_str(s, '`').fmt_sql(f, fmt);
			f.push('`');
		} else {
			f.push_str(s)
		}
	}
}

pub struct EscapeKey<'a>(pub &'a str);
impl ToSql for EscapeKey<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let s = self.0;
		// Any non 'normal' characters or does the key start with a digit?
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
		{
			f.push('\"');
			Escape::escape_str(s, '\"').fmt_sql(f, fmt);
			f.push('\"');
		}

		f.push_str(s)
	}
}

pub struct EscapeRid<'a>(pub &'a str);
impl ToSql for EscapeRid<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let s = self.0;
		// Any non 'normal' characters or are all character digits?
		if s.is_empty()
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| !s.contains(|x: char| !x.is_ascii_digit() && x != '_')
		{
			// Always use brackets for display (not backticks)
			f.push('⟨');
			Escape::escape_str(s, '⟩').fmt_sql(f, fmt);
			f.push('⟩');
		}

		f.push_str(s)
	}
}
