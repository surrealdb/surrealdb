use std::borrow::Cow;

const SINGLE: char = '\'';

const BRACKETL: char = '⟨';
const BRACKETR: char = '⟩';
const BRACKET_ESC: &str = r"\⟩";

const DOUBLE: char = '"';
const DOUBLE_ESC: &str = r#"\""#;

const BACKTICK: char = '`';
const BACKTICK_ESC: &str = r"\`";

/// Quotes a string with single or double quotes:
/// - cat -> 'cat'
/// - cat's -> "cat's"
/// - cat's "toy" -> "cat's \"toy\""
///
/// Escapes / as //
#[inline]
pub fn quote_str(s: &str) -> String {
	// Rough approximation of capacity, which may be exceeded
	// if things must be escaped.
	let mut ret = String::with_capacity(2 + s.len());

	fn escape_into(into: &mut String, s: &str, escape_double: bool) {
		// Based on internals of str::replace
		let mut last_end = 0;
		for (start, part) in s.match_indices(|c| c == '\\' || (c == DOUBLE && escape_double)) {
			into.push_str(&s[last_end..start]);
			into.push_str(if part == "\\" {
				"\\\\"
			} else {
				DOUBLE_ESC
			});
			last_end = start + part.len();
		}
		into.push_str(&s[last_end..s.len()]);
	}

	let quote = if s.contains(SINGLE) {
		DOUBLE
	} else {
		SINGLE
	};

	ret.push(quote);
	escape_into(&mut ret, s, quote == DOUBLE);
	ret.push(quote);
	ret
}

#[inline]
pub fn quote_plain_str(s: &str) -> String {
	quote_str(s)
}

#[inline]
/// Escapes a key if necessary
pub fn escape_key(s: &str) -> Cow<'_, str> {
	escape_normal(s, DOUBLE, DOUBLE, DOUBLE_ESC)
}

#[inline]
/// Escapes an id if necessary
pub fn escape_rid(s: &str) -> Cow<'_, str> {
	escape_full_numeric(s, BRACKETL, BRACKETR, BRACKET_ESC)
}

#[inline]
/// Escapes an ident if necessary
pub fn escape_ident(s: &str) -> Cow<'_, str> {
	if let Some(x) = escape_reserved_keyword(s) {
		return Cow::Owned(x);
	}
	escape_starts_numeric(s, BACKTICK, BACKTICK, BACKTICK_ESC)
}

#[inline]
pub fn escape_normal<'a>(s: &'a str, l: char, r: char, e: &str) -> Cow<'a, str> {
	// Is there no need to escape the value?
	if s.bytes().all(|x| x.is_ascii_alphanumeric() || x == b'_') {
		return Cow::Borrowed(s);
	}
	// Output the value
	Cow::Owned(format!("{l}{}{r}", s.replace(r, e)))
}

pub fn escape_reserved_keyword(s: &str) -> Option<String> {
	crate::syn::could_be_reserved_keyword(s).then(|| format!("`{}`", s))
}

#[inline]
pub fn escape_full_numeric<'a>(s: &'a str, l: char, r: char, e: &str) -> Cow<'a, str> {
	let mut numeric = true;
	// Loop over each character
	for x in s.bytes() {
		// Check if character is allowed
		if !(x.is_ascii_alphanumeric() || x == b'_') {
			return Cow::Owned(format!("{l}{}{r}", s.replace(r, e)));
		}
		// For every character, we need to check if it is a digit until we encounter a non-digit
		if numeric && !x.is_ascii_digit() {
			numeric = false;
		}
	}

	// If all characters are digits, then we need to escape the string
	if numeric {
		return Cow::Owned(format!("{l}{}{r}", s.replace(r, e)));
	}
	Cow::Borrowed(s)
}

#[inline]
pub fn escape_starts_numeric<'a>(s: &'a str, l: char, r: char, e: &str) -> Cow<'a, str> {
	// Loop over each character
	for (idx, x) in s.bytes().enumerate() {
		// the first character is not allowed to be a digit.
		if idx == 0 && x.is_ascii_digit() {
			return Cow::Owned(format!("{l}{}{r}", s.replace(r, e)));
		}
		// Check if character is allowed
		if !(x.is_ascii_alphanumeric() || x == b'_') {
			return Cow::Owned(format!("{l}{}{r}", s.replace(r, e)));
		}
	}
	Cow::Borrowed(s)
}
