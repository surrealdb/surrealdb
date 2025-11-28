use std::fmt;
use std::str::Chars;

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
pub struct EscapeIdent<T>(pub T);
impl<T: AsRef<str>> fmt::Display for EscapeIdent<T> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let s = self.0.as_ref();
		if crate::syn::could_be_reserved_keyword(s) {
			return f.write_fmt(format_args!("`{}`", Escape::escape_str(s, '`')));
		}
		EscapeKwFreeIdent(s).fmt(f)
	}
}

pub struct EscapeKwIdent<'a>(pub &'a str, pub &'a [&'static str]);
impl fmt::Display for EscapeKwIdent<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if self.1.iter().any(|x| x.eq_ignore_ascii_case(self.0)) {
			return f.write_fmt(format_args!("`{}`", Escape::escape_str(self.0, '`')));
		}
		EscapeKwFreeIdent(self.0).fmt(f)
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
		if s.is_empty()
			|| s.starts_with(|x: char| x.is_ascii_digit())
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| s == "NaN"
			|| s == "Infinity"
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

pub struct EscapeRidKey<'a>(pub &'a str);
impl fmt::Display for EscapeRidKey<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let s = self.0;
		// Any non 'normal' characters or are all character digits?
		if s.is_empty()
			|| s.contains(|x: char| !x.is_ascii_alphanumeric() && x != '_')
			|| !s.contains(|x: char| !x.is_ascii_digit() && x != '_')
		{
			if *crate::cnf::ACCESSIBLE_OUTPUT {
				f.write_fmt(format_args!("`{}`", Escape::escape_str(s, '`')))
			} else {
				f.write_fmt(format_args!("⟨{}⟩", Escape::escape_str(s, '⟩')))
			}
		} else {
			f.write_str(s)
		}
	}
}
