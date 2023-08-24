use crate::sql::common::val_u8;
use nom::character::is_digit;
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
/// Escapes a key if necessary
pub fn escape_key(s: &str) -> Cow<'_, str> {
	escape_normal(s, DOUBLE, DOUBLE, DOUBLE_ESC)
}

#[inline]
/// Escapes an id if necessary
pub fn escape_rid(s: &str) -> Cow<'_, str> {
	escape_numeric(s, BRACKETL, BRACKETR, BRACKET_ESC)
}

#[inline]
/// Escapes an ident if necessary
pub fn escape_ident(s: &str) -> Cow<'_, str> {
	escape_numeric(s, BACKTICK, BACKTICK, BACKTICK_ESC)
}

#[inline]
pub fn escape_normal<'a>(s: &'a str, l: char, r: char, e: &str) -> Cow<'a, str> {
	// Loop over each character
	for x in s.bytes() {
		// Check if character is allowed
		if !val_u8(x) {
			return Cow::Owned(format!("{l}{}{r}", s.replace(r, e)));
		}
	}
	// Output the value
	Cow::Borrowed(s)
}

#[inline]
pub fn escape_numeric<'a>(s: &'a str, l: char, r: char, e: &str) -> Cow<'a, str> {
	// Presume this is numeric
	let mut numeric = true;
	// Loop over each character
	for x in s.bytes() {
		// Check if character is allowed
		if !val_u8(x) {
			return Cow::Owned(format!("{l}{}{r}", s.replace(r, e)));
		}
		// Check if character is non-numeric
		if !is_digit(x) {
			numeric = false;
		}
	}
	// Output the id value
	match numeric {
		// This is numeric so escape it
		true => Cow::Owned(format!("{l}{}{r}", s.replace(r, e))),
		// No need to escape the value
		_ => Cow::Borrowed(s),
	}
}
