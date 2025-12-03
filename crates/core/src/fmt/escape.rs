use std::str::Chars;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

// TODO: Remove duplicated code between sql and expr

#[derive(Clone)]
pub struct Escape<'a> {
	chars: Chars<'a>,
	pending_buffer: [char; 4],
	pending_len: u8,
	escape_char: char,
}

impl<'a> Escape<'a> {
	fn escape_str(s: &'a str, escape_char: char) -> Self {
		Escape {
			chars: s.chars(),
			pending_buffer: ['\0'; 4],
			pending_len: 0u8,
			escape_char,
		}
	}
}

impl Iterator for Escape<'_> {
	type Item = char;

	fn next(&mut self) -> Option<char> {
		if self.pending_len > 0 {
			self.pending_len -= 1;
			return Some(self.pending_buffer[self.pending_len as usize]);
		}
		let next = self.chars.next()?;
		if next == self.escape_char || next == '\\' {
			self.pending_buffer[0] = next;
			self.pending_len = 1;
			return Some('\\');
		}
		// Always escape backspace
		if next == '\u{8}' {
			self.pending_buffer[3] = 'u';
			self.pending_buffer[2] = '{';
			self.pending_buffer[1] = '8';
			self.pending_buffer[0] = '}';
			self.pending_len = 4;
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
		} else {
			EscapeKwFreeIdent(s).fmt_sql(f, fmt);
		}
	}
}

pub struct EscapeKwIdent<'a>(pub &'a str, pub &'a [&'static str]);
impl ToSql for EscapeKwIdent<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if self.1.iter().any(|x| x.eq_ignore_ascii_case(self.0)) {
			write_sql!(f, fmt, "`{}`", Escape::escape_str(self.0, '`'));
		} else {
			EscapeKwFreeIdent(self.0).fmt_sql(f, fmt);
		}
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
			|| s == "NaN"
			|| s == "Infinity"
		{
			write_sql!(f, fmt, "`{}`", Escape::escape_str(s, '`'));
		} else {
			f.push_str(s);
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
			write_sql!(f, fmt, "\"{}\"", Escape::escape_str(s, '\"'));
		} else {
			f.push_str(s);
		}
	}
}

pub struct EscapeRidKey<'a>(pub &'a str);
impl ToSql for EscapeRidKey<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let s = self.0;
		// Any non 'normal' characters or are all character digits?
		if s.is_empty()
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| !s.contains(|x: char| !x.is_ascii_digit() && x != '_')
		{
			if *crate::cnf::ACCESSIBLE_OUTPUT {
				write_sql!(f, fmt, "`{}`", Escape::escape_str(s, '`'));
			} else {
				write_sql!(f, fmt, "⟨{}⟩", Escape::escape_str(s, '⟩'));
			}
		} else {
			f.push_str(s)
		}
	}
}
