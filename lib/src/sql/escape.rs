use crate::sql::common::val_char;

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
pub fn escape_id(s: &str) -> String {
	for x in s.chars() {
		if !val_char(x) {
			return format!("{}{}{}", BRACKET_L, s, BRACKET_R);
		}
	}
	s.to_owned()
}

#[inline]
pub fn escape_key(s: &str) -> String {
	for x in s.chars() {
		if !val_char(x) {
			return format!("{}{}{}", DOUBLE, s.replace(DOUBLE, DOUBLE_ESC), DOUBLE);
		}
	}
	s.to_owned()
}

#[inline]
pub fn escape_ident(s: &str) -> String {
	for x in s.chars() {
		if !val_char(x) {
			return format!("{}{}{}", BACKTICK, s.replace(BACKTICK, BACKTICK_ESC), BACKTICK);
		}
	}
	s.to_owned()
}
