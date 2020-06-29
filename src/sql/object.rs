use crate::sql::comment::mightbespace;
use crate::sql::common::{commas, escape, val_char};
use crate::sql::expression::{expression, Expression};
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_while1;
use nom::combinator::opt;
use nom::multi::separated_list;
use nom::sequence::delimited;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Object(Vec<(String, Expression)>);

impl fmt::Display for Object {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"{{ {} }}",
			self.0
				.iter()
				.map(|(ref k, ref v)| format!("{}: {}", escape(&k, &val_char, "\""), v))
				.collect::<Vec<_>>()
				.join(", ")
		)
	}
}

pub fn object(i: &str) -> IResult<&str, Object> {
	let (i, _) = tag("{")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list(commas, item)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(tag(","))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("}")(i)?;
	Ok((i, Object(v)))
}

fn item(i: &str) -> IResult<&str, (String, Expression)> {
	let (i, k) = key(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag(":")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = expression(i)?;
	Ok((i, (String::from(k), v)))
}

fn key(i: &str) -> IResult<&str, &str> {
	alt((key_none, key_single, key_double))(i)
}

fn key_none(i: &str) -> IResult<&str, &str> {
	take_while1(val_char)(i)
}

fn key_single(i: &str) -> IResult<&str, &str> {
	delimited(tag("\""), is_not("\""), tag("\""))(i)
}

fn key_double(i: &str) -> IResult<&str, &str> {
	delimited(tag("\'"), is_not("\'"), tag("\'"))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn object_normal() {
		let sql = "{one:1,two:2,tre:3}";
		let res = object(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, two: 2, tre: 3 }", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn object_commas() {
		let sql = "{one:1,two:2,tre:3,}";
		let res = object(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, two: 2, tre: 3 }", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn object_expression() {
		let sql = "{one:1,two:2,tre:3+1}";
		let res = object(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, two: 2, tre: 3 + 1 }", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}
}
