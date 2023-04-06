use crate::sql::common::val_char;
use crate::sql::error::IResult;
use crate::sql::escape::escape_ident;
use nom::branch::alt;
use nom::bytes::complete::escaped_transform;
use nom::bytes::complete::is_not;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_while1;
use nom::character::complete::char;
use nom::combinator::recognize;
use nom::combinator::value;
use nom::multi::separated_list1;
use nom::sequence::delimited;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

const BRACKET_L: char = '⟨';
const BRACKET_R: char = '⟩';
const BRACKET_END: &str = r#"⟩"#;

const BACKTICK: char = '`';
const BACKTICK_ESC: &str = r#"\`"#;

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Ident(pub String);

impl From<String> for Ident {
	fn from(v: String) -> Self {
		Self(v)
	}
}

impl From<&str> for Ident {
	fn from(v: &str) -> Self {
		Self::from(String::from(v))
	}
}

impl Deref for Ident {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Ident {
	/// Convert the Ident to a raw String
	pub fn to_raw(&self) -> String {
		self.0.to_string()
	}
	/// Returns a yield if an alias is specified
	pub(crate) fn is_id(&self) -> bool {
		self.0.as_str() == "id"
	}
}

impl Display for Ident {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&escape_ident(&self.0), f)
	}
}

pub fn ident(i: &str) -> IResult<&str, Ident> {
	let (i, v) = ident_raw(i)?;
	Ok((i, Ident::from(v)))
}

pub fn plain(i: &str) -> IResult<&str, Ident> {
	let (i, v) = take_while1(val_char)(i)?;
	Ok((i, Ident::from(v)))
}

pub fn multi(i: &str) -> IResult<&str, Ident> {
	let (i, v) = recognize(separated_list1(tag("::"), take_while1(val_char)))(i)?;
	Ok((i, Ident::from(v)))
}

pub fn ident_raw(i: &str) -> IResult<&str, String> {
	let (i, v) = alt((ident_default, ident_backtick, ident_brackets))(i)?;
	Ok((i, v))
}

fn ident_default(i: &str) -> IResult<&str, String> {
	let (i, v) = take_while1(val_char)(i)?;
	Ok((i, String::from(v)))
}

fn ident_backtick(i: &str) -> IResult<&str, String> {
	let (i, _) = char(BACKTICK)(i)?;
	let (i, v) = escaped_transform(
		is_not(BACKTICK_ESC),
		'\\',
		alt((
			value('\u{5c}', char('\\')),
			value('\u{60}', char('`')),
			value('\u{2f}', char('/')),
			value('\u{08}', char('b')),
			value('\u{0c}', char('f')),
			value('\u{0a}', char('n')),
			value('\u{0d}', char('r')),
			value('\u{09}', char('t')),
		)),
	)(i)?;
	let (i, _) = char(BACKTICK)(i)?;
	Ok((i, v))
}

fn ident_brackets(i: &str) -> IResult<&str, String> {
	let (i, v) = delimited(char(BRACKET_L), is_not(BRACKET_END), char(BRACKET_R))(i)?;
	Ok((i, String::from(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn ident_normal() {
		let sql = "test";
		let res = ident(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Ident::from("test"));
	}

	#[test]
	fn ident_quoted_backtick() {
		let sql = "`test`";
		let res = ident(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Ident::from("test"));
	}

	#[test]
	fn ident_quoted_brackets() {
		let sql = "⟨test⟩";
		let res = ident(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Ident::from("test"));
	}
}
