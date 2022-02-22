use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::escaped;
use nom::bytes::complete::is_not;
use nom::bytes::complete::tag;
use nom::character::complete::one_of;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

const SINGLE: &str = r#"'"#;
const SINGLE_ESC: &str = r#"\'"#;

const DOUBLE: &str = r#"""#;
const DOUBLE_ESC: &str = r#"\""#;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Script {
	pub value: String,
}

impl From<String> for Script {
	fn from(s: String) -> Self {
		Script {
			value: s,
		}
	}
}

impl<'a> From<&'a str> for Script {
	fn from(s: &str) -> Self {
		Script {
			value: String::from(s),
		}
	}
}

impl Script {
	pub fn as_str(&self) -> &str {
		self.value.as_str()
	}
}

impl fmt::Display for Script {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "\"{}\"", self.value)
	}
}

pub fn script(i: &str) -> IResult<&str, Script> {
	let (i, v) = script_raw(i)?;
	Ok((i, Script::from(v)))
}

pub fn script_raw(i: &str) -> IResult<&str, String> {
	alt((script_single, script_double))(i)
}

fn script_single(i: &str) -> IResult<&str, String> {
	let (i, _) = tag(SINGLE)(i)?;
	let (i, v) = alt((escaped(is_not(SINGLE_ESC), '\\', one_of(SINGLE)), tag("")))(i)?;
	let (i, _) = tag(SINGLE)(i)?;
	Ok((i, String::from(v).replace(SINGLE_ESC, SINGLE)))
}

fn script_double(i: &str) -> IResult<&str, String> {
	let (i, _) = tag(DOUBLE)(i)?;
	let (i, v) = alt((escaped(is_not(DOUBLE_ESC), '\\', one_of(DOUBLE)), tag("")))(i)?;
	let (i, _) = tag(DOUBLE)(i)?;
	Ok((i, String::from(v).replace(DOUBLE_ESC, DOUBLE)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn script_empty() {
		let sql = r#""""#;
		let res = script(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#""""#, format!("{}", out));
		assert_eq!(out, Script::from(""));
	}

	#[test]
	fn script_single() {
		let sql = r#"'test'"#;
		let res = script(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#""test""#, format!("{}", out));
		assert_eq!(out, Script::from("test"));
	}

	#[test]
	fn script_double() {
		let sql = r#""test""#;
		let res = script(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#""test""#, format!("{}", out));
		assert_eq!(out, Script::from("test"));
	}

	#[test]
	fn script_quoted_single() {
		let sql = r#"'te\'st'"#;
		let res = script(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#""te'st""#, format!("{}", out));
		assert_eq!(out, Script::from(r#"te'st"#));
	}

	#[test]
	fn script_quoted_double() {
		let sql = r#""te\"st""#;
		let res = script(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#""te"st""#, format!("{}", out));
		assert_eq!(out, Script::from(r#"te"st"#));
	}
}
