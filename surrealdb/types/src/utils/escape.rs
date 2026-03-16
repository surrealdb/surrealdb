use std::fmt::{Display, Write};

use crate::{SqlFormat, ToSql};

pub struct EscapeWriter<W> {
	escape_char: char,
	/// When true, newline characters are written literally instead of being
	/// escaped as `\n`. This produces more readable output while remaining
	/// valid SurrealQL (the parser accepts literal newlines in strings).
	preserve_newlines: bool,
	writer: W,
}

impl<'a> EscapeWriter<&'a mut String> {
	fn escape<D: Display + ?Sized>(into: &'a mut String, escape: char, display: &D) {
		Self {
			escape_char: escape,
			preserve_newlines: false,
			writer: into,
		}
		.write(display)
	}

	fn escape_pretty<D: Display + ?Sized>(into: &'a mut String, escape: char, display: &D) {
		Self {
			escape_char: escape,
			preserve_newlines: true,
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
			'\n' if self.preserve_newlines => {
				self.writer.write_char('\n')?;
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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let s = self.0;
		let quote = if s.contains('\'') {
			'\"'
		} else {
			'\''
		};

		f.push(quote);
		if fmt.is_pretty() {
			EscapeWriter::escape_pretty(f, quote, self.0);
		} else {
			EscapeWriter::escape(f, quote, self.0);
		}
		f.push(quote);
	}
}

/// Escapes identifiers for use in SQON (SQL Object Notation).
pub struct EscapeSqonIdent<'a>(pub &'a str);
impl ToSql for EscapeSqonIdent<'_> {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		let s = self.0;
		// Not a keyword, any non 'normal' characters or does it start with a digit?
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
		{
			f.push('`');
			EscapeWriter::escape(f, '`', self.0);
			f.push('`');
		} else {
			f.push_str(s)
		}
	}
}

pub struct EscapeObjectKey<'a>(pub &'a str);
impl ToSql for EscapeObjectKey<'_> {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		let s = self.0;
		// Any non 'normal' characters or does the key start with a digit?
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
		{
			f.push('\"');
			EscapeWriter::escape(f, '"', self.0);
			f.push('\"');
		} else {
			f.push_str(s)
		}
	}
}

pub struct EscapeRecordKey<'a>(pub &'a str);
impl ToSql for EscapeRecordKey<'_> {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		let s = self.0;
		// Any non 'normal' characters or are all character digits?
		if s.is_empty()
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| !s.contains(|x: char| !x.is_ascii_digit() && x != '_')
		{
			// Always use backticks for display (not brackets)
			f.push('`');
			EscapeWriter::escape(f, '`', self.0);
			f.push('`');
		} else {
			f.push_str(s)
		}
	}
}
