use std::fmt::{self, Write};

pub struct EscapeWriter<W> {
	escape_char: char,
	writer: W,
}

impl<W: fmt::Write> EscapeWriter<W> {
	fn escape<D: fmt::Display + ?Sized>(into: W, escape: char, display: &D) -> fmt::Result {
		Self {
			escape_char: escape,
			writer: into,
		}
		.write(display)
	}

	fn write<D: fmt::Display + ?Sized>(&mut self, display: &D) -> fmt::Result {
		self.write_fmt(format_args!("{display}"))
	}
}

impl<W: fmt::Write> fmt::Write for EscapeWriter<W> {
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
impl fmt::Display for QuoteStr<'_> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let s = self.0;
		let quote = if s.contains('\'') {
			'\"'
		} else {
			'\''
		};

		f.write_char(quote)?;
		EscapeWriter::escape(&mut *f, quote, self.0)?;
		f.write_char(quote)
	}
}

/// Escapes identifiers which might be used in the same place as a keyword.
pub struct EscapeIdent<T>(pub T);
impl<T: AsRef<str>> fmt::Display for EscapeIdent<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let s = self.0.as_ref();
		if crate::syn::could_be_reserved_keyword(s) {
			f.write_char('`')?;
			EscapeWriter::escape(&mut *f, '`', self.0.as_ref())?;
			f.write_char('`')
		} else {
			EscapeKwFreeIdent(s).fmt(f)
		}
	}
}

/// Escapes identifiers which can never be used in the same place as a keyword.
///
/// Examples of this is a Param as '$' is in front of the identifier so it
/// cannot be an
pub struct EscapeKwFreeIdent<'a>(pub &'a str);
impl fmt::Display for EscapeKwFreeIdent<'_> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let s = self.0;
		// Not a keyword, any non 'normal' characters or does it start with a digit?
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| s == "NaN"
			|| s == "Infinity"
		{
			write!(f, "`")?;
			EscapeWriter::escape(&mut *f, '`', self.0)?;
			write!(f, "`")
		} else {
			f.write_str(s)
		}
	}
}

pub struct EscapeKey<'a>(pub &'a str);
impl fmt::Display for EscapeKey<'_> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let s = self.0;
		// Any non 'normal' characters or does the key start with a digit?
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| s == "NaN"
			|| s == "Infinity"
		{
			write!(f, "\"")?;
			EscapeWriter::escape(&mut *f, '"', self.0)?;
			write!(f, "\"")
		} else {
			f.write_str(s)
		}
	}
}

pub struct EscapeRidKey<'a>(pub &'a str);
impl fmt::Display for EscapeRidKey<'_> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let s = self.0;
		// Any non 'normal' characters or are all character digits?
		if s.is_empty()
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| !s.contains(|x: char| !x.is_ascii_digit() && x != '_')
			|| s == "Infinity"
			|| s == "NaN"
		{
			write!(f, "`")?;
			EscapeWriter::escape(&mut *f, '`', self.0)?;
			write!(f, "`")
		} else {
			f.write_str(s)
		}
	}
}

pub struct EscapePath<'a>(pub &'a str);
impl fmt::Display for EscapePath<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for (idx, s) in self.0.split("::").enumerate() {
			if idx != 0 {
				f.write_str("::")?;
			}
			write!(f, "{}", EscapeKwFreeIdent(s))?
		}
		Ok(())
	}
}

pub struct EscapeFloat(pub f64);
impl fmt::Display for EscapeFloat {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if self.0.is_nan() {
			write!(f, "NaN")
		} else if self.0.is_infinite() {
			if self.0 < 0.0 {
				write!(f, "-Infinity")
			} else {
				write!(f, "Infinity")
			}
		} else {
			write!(f, "{}f", self.0)
		}
	}
}
