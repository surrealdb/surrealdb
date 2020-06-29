use crate::sql::common::escape;
use crate::sql::common::val_char;
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_while1;
use nom::sequence::delimited;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Ident {
	pub name: String,
}

impl<'a> From<&'a str> for Ident {
	fn from(i: &str) -> Ident {
		Ident {
			name: String::from(i),
		}
	}
}

impl fmt::Display for Ident {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", escape(&self.name, &val_char, "`"))
	}
}

pub fn ident(i: &str) -> IResult<&str, Ident> {
	let (i, v) = ident_raw(i)?;
	Ok((i, Ident::from(v)))
}

pub fn ident_raw(i: &str) -> IResult<&str, &str> {
	alt((ident_default, ident_backtick, ident_brackets))(i)
}

fn ident_default(i: &str) -> IResult<&str, &str> {
	take_while1(val_char)(i)
}

fn ident_backtick(i: &str) -> IResult<&str, &str> {
	delimited(tag("`"), is_not("`"), tag("`"))(i)
}

fn ident_brackets(i: &str) -> IResult<&str, &str> {
	delimited(tag("⟨"), is_not("⟩"), tag("⟩"))(i)
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
