use crate::sql::common::val_u8;
use std::borrow::Cow;

const BRACKET_L: char = '⟨';
const BRACKET_R: char = '⟩';

const DOUBLE: char = '"';
const DOUBLE_ESC: &str = r#"\""#;

const BACKTICK: char = '`';
const BACKTICK_ESC: &str = r#"\`"#;

#[inline]
pub fn escape_strand(s: &str) -> String {
	format!("{}{}{}", DOUBLE, s, DOUBLE)
}

#[inline]
pub fn escape_id(s: &str) -> Cow<'_, str> {
	for x in s.bytes() {
		if !val_u8(x) {
			return Cow::Owned(format!("{}{}{}", BRACKET_L, s, BRACKET_R));
		}
	}
	Cow::Borrowed(s)
}

#[inline]
pub fn escape_key(s: &str) -> Cow<'_, str> {
	for x in s.bytes() {
		if !val_u8(x) {
			return Cow::Owned(format!("{}{}{}", DOUBLE, s.replace(DOUBLE, DOUBLE_ESC), DOUBLE));
		}
	}
	Cow::Borrowed(s)
}

#[inline]
pub fn escape_ident(s: &str) -> Cow<'_, str> {
	for x in s.bytes() {
		if !val_u8(x) {
			return Cow::Owned(format!(
				"{}{}{}",
				BACKTICK,
				s.replace(BACKTICK, BACKTICK_ESC),
				BACKTICK
			));
		}
	}
	Cow::Borrowed(s)
}
