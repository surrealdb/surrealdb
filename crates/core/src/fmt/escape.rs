use std::fmt;
use std::str::Chars;

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

/// Escapes identifiers which might be used in the same place as a keyword.
pub struct EscapeIdent<'a>(pub &'a str);
impl fmt::Display for EscapeIdent<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let s = self.0;
		if crate::syn::could_be_reserved_keyword(s) {
			return f.write_fmt(format_args!("`{}`", Escape::escape_str(s, '`')));
		}
		EscapeKwFreeIdent(s).fmt(f)
	}
}

/// Escapes identifiers which can never be used in the same place as a keyword.
///
/// Examples of this is a Param as '$' is in front of the identifier so it
/// cannot be an
pub struct EscapeKwFreeIdent<'a>(pub &'a str);
impl fmt::Display for EscapeKwFreeIdent<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let s = self.0;
		// Not a keyword, any non 'normal' characters or does it start with a digit?
		if s.starts_with(|x: char| x.is_ascii_digit())
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
		if s.starts_with(|x: char| x.is_ascii_digit())
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
		if s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| !s.contains(|x: char| !x.is_ascii_digit() && x != '_')
		{
			return match *crate::cnf::ACCESSIBLE_OUTPUT {
				true => f.write_fmt(format_args!("`{}`", Escape::escape_str(s, '`'))),
				false => f.write_fmt(format_args!("⟨{}⟩", Escape::escape_str(s, '⟩'))),
			};
		}

		f.write_str(s)
	}
}
