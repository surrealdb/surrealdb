use crate::sql::comment::{block, slash};
use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::escaped;
use nom::bytes::complete::is_not;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::character::complete::multispace0;
use nom::combinator::recognize;
use nom::multi::many0;
use nom::multi::many1;
use nom::sequence::delimited;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

const SINGLE: char = '\'';
const SINGLE_ESC: &str = r#"\'"#;

const DOUBLE: char = '"';
const DOUBLE_ESC: &str = r#"\""#;

const BACKTICK: char = '`';
const BACKTICK_ESC: &str = r#"\`"#;

const OBJECT_BEG: char = '{';
const OBJECT_END: char = '}';

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Script(pub String);

impl From<String> for Script {
	fn from(s: String) -> Self {
		Self(s)
	}
}

impl From<&str> for Script {
	fn from(s: &str) -> Self {
		Self::from(String::from(s))
	}
}

impl Deref for Script {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Script {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

pub fn script(i: &str) -> IResult<&str, Script> {
	let (i, v) = script_raw(i)?;
	Ok((i, Script(String::from(v))))
}

fn script_raw(i: &str) -> IResult<&str, &str> {
	recognize(many0(alt((
		script_comment,
		script_object,
		script_string,
		script_maths,
		script_other,
	))))(i)
}

fn script_maths(i: &str) -> IResult<&str, &str> {
	recognize(tag("/"))(i)
}

fn script_other(i: &str) -> IResult<&str, &str> {
	recognize(many1(is_not("/{}`'\"")))(i)
}

fn script_comment(i: &str) -> IResult<&str, &str> {
	recognize(delimited(multispace0, many1(alt((block, slash))), multispace0))(i)
}

fn script_object(i: &str) -> IResult<&str, &str> {
	recognize(delimited(char(OBJECT_BEG), script_raw, char(OBJECT_END)))(i)
}

fn script_string(i: &str) -> IResult<&str, &str> {
	recognize(alt((
		|i| {
			let (i, _) = char(SINGLE)(i)?;
			let (i, _) = char(SINGLE)(i)?;
			Ok((i, ""))
		},
		|i| {
			let (i, _) = char(DOUBLE)(i)?;
			let (i, _) = char(DOUBLE)(i)?;
			Ok((i, ""))
		},
		|i| {
			let (i, _) = char(SINGLE)(i)?;
			let (i, v) = escaped(is_not(SINGLE_ESC), '\\', char(SINGLE))(i)?;
			let (i, _) = char(SINGLE)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, _) = char(DOUBLE)(i)?;
			let (i, v) = escaped(is_not(DOUBLE_ESC), '\\', char(DOUBLE))(i)?;
			let (i, _) = char(DOUBLE)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, _) = char(BACKTICK)(i)?;
			let (i, v) = escaped(is_not(BACKTICK_ESC), '\\', char(BACKTICK))(i)?;
			let (i, _) = char(BACKTICK)(i)?;
			Ok((i, v))
		},
	)))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn script_basic() {
		let sql = "return true;";
		let res = script(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("return true;", format!("{}", out));
		assert_eq!(out, Script::from("return true;"));
	}

	#[test]
	fn script_object() {
		let sql = "return { test: true, something: { other: true } };";
		let res = script(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("return { test: true, something: { other: true } };", format!("{}", out));
		assert_eq!(out, Script::from("return { test: true, something: { other: true } };"));
	}

	#[test]
	fn script_closure() {
		let sql = "return this.values.map(v => `This value is ${Number(v * 3)}`);";
		let res = script(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			"return this.values.map(v => `This value is ${Number(v * 3)}`);",
			format!("{}", out)
		);
		assert_eq!(
			out,
			Script::from("return this.values.map(v => `This value is ${Number(v * 3)}`);")
		);
	}

	#[test]
	fn script_complex() {
		let sql = r#"return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };"#;
		let res = script(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			r#"return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };"#,
			format!("{}", out)
		);
		assert_eq!(
			out,
			Script::from(
				r#"return { test: true, some: { object: "some text with uneven {{{ {} \" brackets", else: false } };"#
			)
		);
	}

	#[test]
	fn script_advanced() {
		let sql = r#"
			// {
			// }
			// {}
			// { }
			/* { */
			/* } */
			/* {} */
			/* { } */
			/* {{{ $ }} */
			/* /* /* /* */
			let x = {};
			let x = { };
			let x = '{';
			let x = "{";
			let x = '}';
			let x = "}";
			let x = '} } { {';
			let x = 3 / 4 * 2;
			let x = /* something */ 45 + 2;
		"#;
		let res = script(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
		assert_eq!(out, Script::from(sql));
	}
}
