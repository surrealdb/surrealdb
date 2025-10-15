use std::fmt;
use std::str::Chars;

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

impl fmt::Display for Escape<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for x in self.clone() {
			fmt::Write::write_char(f, x)?;
		}
		Ok(())
	}
}

pub struct QuoteStr<'a>(pub &'a str);
impl fmt::Display for QuoteStr<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let s = self.0;
		let quote = if s.contains('\'') {
			'\"'
		} else {
			'\''
		};

		f.write_fmt(format_args!("{}{}{}", quote, Escape::escape_str(s, quote), quote))
	}
}

/// Escapes identifiers for use in SQON (SQL Object Notation).
pub struct EscapeSqonIdent<'a>(pub &'a str);
impl fmt::Display for EscapeSqonIdent<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let s = self.0;
		// Not a keyword, any non 'normal' characters or does it start with a digit?
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
		{
			return f.write_fmt(format_args!("`{}`", Escape::escape_str(s, '`')));
		}
		f.write_str(s)
	}
}

pub struct EscapeKey<'a>(pub &'a str);
impl fmt::Display for EscapeKey<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let s = self.0;
		// Any non 'normal' characters or does the key start with a digit?
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
		{
			return f.write_fmt(format_args!("\"{}\"", Escape::escape_str(s, '\"')));
		}

		f.write_str(s)
	}
}

pub struct EscapeRid<'a>(pub &'a str);
impl fmt::Display for EscapeRid<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let s = self.0;
		// Any non 'normal' characters or are all character digits?
		if s.is_empty()
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| !s.contains(|x: char| !x.is_ascii_digit() && x != '_')
		{
			// Always use brackets for display (not backticks)
			return f.write_fmt(format_args!("⟨{}⟩", Escape::escape_str(s, '⟩')));
		}

		f.write_str(s)
	}
}
