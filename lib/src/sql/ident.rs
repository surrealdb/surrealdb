use crate::sql::common::escape;
use crate::sql::common::val_char;
use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::bytes::complete::take_while1;
use nom::character::complete::char;
use nom::sequence::delimited;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::str;

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Ident(pub String);

impl From<String> for Ident {
	fn from(s: String) -> Self {
		Ident(s)
	}
}

impl<'a> From<&'a str> for Ident {
	fn from(i: &str) -> Ident {
		Ident(String::from(i))
	}
}

impl<'a> From<&'a String> for Ident {
	fn from(i: &String) -> Ident {
		Ident(String::from(i))
	}
}

impl Deref for Ident {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Ident {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", escape(&self.0, &val_char, "`"))
	}
}

pub fn ident(i: &str) -> IResult<&str, Ident> {
	let (i, v) = ident_raw(i)?;
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
	let (i, v) = delimited(char('`'), is_not("`"), char('`'))(i)?;
	Ok((i, String::from(v)))
}

fn ident_brackets(i: &str) -> IResult<&str, String> {
	let (i, v) = delimited(char('⟨'), is_not("⟩"), char('⟩'))(i)?;
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
