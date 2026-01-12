use std::fmt::{Display, Write};

use surrealdb_types::{SqlFormat, ToSql};

// TODO: Remove duplicated code between core and types

pub struct EscapeWriter<W> {
	escape_char: char,
	writer: W,
}

impl<'a> EscapeWriter<&'a mut String> {
	fn escape<D: Display + ?Sized>(into: &'a mut String, escape: char, display: &D) {
		Self {
			escape_char: escape,
			writer: into,
		}
		.write(display)
	}

	fn write<D: Display + ?Sized>(&mut self, display: &D) {
		let _ = self.write_fmt(format_args!("{display}"));
	}
}

impl<W: Write> Write for EscapeWriter<W> {
	fn write_str(&mut self, s: &str) -> std::fmt::Result {
		for c in s.chars() {
			self.write_char(c)?;
		}
		Ok(())
	}

	fn write_char(&mut self, c: char) -> std::fmt::Result {
		match c {
			'\0' => {
				self.writer.write_str("\\0")?;
			}
			'\r' => {
				self.writer.write_str("\\r")?;
			}
			'\t' => {
				self.writer.write_str("\\t")?;
			}
			'\n' => {
				self.writer.write_str("\\n")?;
			}
			// backspace
			'\x08' => {
				self.writer.write_str("\\u{8}")?;
			}
			// Form feed
			'\x0C' => {
				self.writer.write_str("\\f")?;
			}
			'\\' => {
				self.writer.write_str("\\\\")?;
			}
			x if x == self.escape_char => {
				self.writer.write_char('\\')?;
				self.writer.write_char(x)?;
			}
			_ => self.writer.write_char(c)?,
		}
		Ok(())
	}
}

pub struct QuoteStr<'a>(pub &'a str);
impl ToSql for QuoteStr<'_> {
	fn fmt_sql(&self, f: &mut String, _: SqlFormat) {
		let s = self.0;
		let quote = if s.contains('\'') {
			'\"'
		} else {
			'\''
		};

		f.push(quote);
		EscapeWriter::escape(f, quote, self.0);
		f.push(quote);
	}
}

/// Escapes identifiers which might be used in the same place as a keyword.
pub struct EscapeIdent<T>(pub T);
impl<T: AsRef<str>> ToSql for EscapeIdent<T> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let s = self.0.as_ref();
		if crate::syn::could_be_reserved_keyword(s) {
			f.push('`');
			EscapeWriter::escape(f, '`', self.0.as_ref());
			f.push('`');
		} else {
			EscapeKwFreeIdent(s).fmt_sql(f, fmt);
		}
	}
}

pub struct EscapeKwIdent<'a>(pub &'a str, pub &'a [&'static str]);
impl ToSql for EscapeKwIdent<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if self.1.iter().any(|x| x.eq_ignore_ascii_case(self.0)) {
			f.push('`');
			EscapeWriter::escape(f, '`', self.0);
			f.push('`');
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
	fn fmt_sql(&self, f: &mut String, _: SqlFormat) {
		let s = self.0;
		// Not a keyword, any non 'normal' characters or does it start with a digit?
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| s == "NaN"
			|| s == "Infinity"
		{
			f.push('`');
			EscapeWriter::escape(f, '`', self.0);
			f.push('`');
		} else {
			f.push_str(s);
		}
	}
}

pub struct EscapeKey<'a>(pub &'a str);
impl ToSql for EscapeKey<'_> {
	fn fmt_sql(&self, f: &mut String, _: SqlFormat) {
		let s = self.0;
		// Any non 'normal' characters or does the key start with a digit?
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| s == "NaN"
			|| s == "Infinity"
		{
			f.push('"');
			EscapeWriter::escape(f, '`', self.0);
			f.push('"');
		} else {
			f.push_str(s);
		}
	}
}

pub struct EscapeRidKey<'a>(pub &'a str);
impl ToSql for EscapeRidKey<'_> {
	fn fmt_sql(&self, f: &mut String, _: SqlFormat) {
		let s = self.0;
		// Any non 'normal' characters or are all character digits?
		if s.is_empty()
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| !s.contains(|x: char| !x.is_ascii_digit() && x != '_')
			|| s == "Infinity"
			|| s == "NaN"
		{
			f.push('`');
			EscapeWriter::escape(f, '`', self.0);
			f.push('`');
		} else {
			f.push_str(s)
		}
	}
}
