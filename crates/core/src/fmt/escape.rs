use std::fmt;
use std::str::Chars;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

// TODO: Remove duplicated code between sql and expr

#[derive(Clone)]
pub struct Escape<'a> {
	chars: Chars<'a>,
	pending: Option<char>,
	escape_char: char,
}

impl<'a> Escape<'a> {
	fn escape_str(s: &'a str, escape_char: char) -> Self {
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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
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

		write_sql!(f, fmt, "{}{}{}", quote, Escape::escape_str(s, quote), quote)
	}
}

/// Escapes identifiers which might be used in the same place as a keyword.
pub struct EscapeIdent<T>(pub T);
impl<T: AsRef<str>> ToSql for EscapeIdent<T> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let s = self.0.as_ref();
		if crate::syn::could_be_reserved_keyword(s) {
			write_sql!(f, fmt, "`{}`", Escape::escape_str(s, '`'));
		}
		EscapeKwFreeIdent(s).fmt_sql(f, fmt);
	}
}

pub struct EscapeKwIdent<'a>(pub &'a str, pub &'a [&'static str]);
impl ToSql for EscapeKwIdent<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if self.1.contains(&self.0) {
			write_sql!(f, fmt, "`{}`", Escape::escape_str(self.0, '`'));
		}
		EscapeKwFreeIdent(self.0).fmt_sql(f, fmt);
	}
}

/// Escapes identifiers which can never be used in the same place as a keyword.
///
/// Examples of this is a Param as '$' is in front of the identifier so it
/// cannot be an
pub struct EscapeKwFreeIdent<'a>(pub &'a str);
impl ToSql for EscapeKwFreeIdent<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let s = self.0;
		// Not a keyword, any non 'normal' characters or does it start with a digit?
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
		{
			write_sql!(f, fmt, "`{}`", Escape::escape_str(s, '`'));
		}
		f.push_str(s)
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
			write_sql!(f, fmt, "\"{}\"", Escape::escape_str(s, '\"'));
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
			return match *crate::cnf::ACCESSIBLE_OUTPUT {
				true => write_sql!(f, fmt, "`{}`", Escape::escape_str(s, '`')),
				false => write_sql!(f, fmt, "⟨{}⟩", Escape::escape_str(s, '⟩')),
			};
		}

		f.push_str(s)
	}
}
