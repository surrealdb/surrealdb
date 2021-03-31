use crate::dbs;
use crate::dbs::Executor;
use crate::dbs::Runtime;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::common::{commas, escape, val_char};
use crate::sql::expression::expression;
use crate::sql::literal::Literal;
use crate::sql::value::Value;
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_while1;
use nom::combinator::opt;
use nom::multi::separated_list0;
use nom::sequence::delimited;
use nom::IResult;
use serde::ser::SerializeMap;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;

const NAME: &'static str = "Object";

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize)]
pub struct Object {
	pub value: Vec<(String, Value)>,
}

impl fmt::Display for Object {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"{{ {} }}",
			self.value
				.iter()
				.map(|(ref k, ref v)| format!("{}: {}", escape(&k, &val_char, "\""), v))
				.collect::<Vec<_>>()
				.join(", ")
		)
	}
}

impl dbs::Process for Object {
	fn process(
		&self,
		ctx: &Runtime,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error> {
		self.value
			.iter()
			.map(|(k, v)| match v.process(ctx, exe, doc) {
				Ok(v) => Ok((k.clone(), Value::from(v))),
				Err(e) => Err(e),
			})
			.collect::<Result<Vec<_>, _>>()
			.map(|v| {
				Literal::Object(Object {
					value: v,
				})
			})
	}
}

impl Serialize for Object {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if serializer.is_human_readable() {
			let mut map = serializer.serialize_map(Some(self.value.len()))?;
			for (ref k, ref v) in &self.value {
				map.serialize_key(k)?;
				map.serialize_value(v)?;
			}
			map.end()
		} else {
			let mut val = serializer.serialize_struct(NAME, 1)?;
			val.serialize_field("value", &self.value)?;
			val.end()
		}
	}
}

pub fn object(i: &str) -> IResult<&str, Object> {
	let (i, _) = tag("{")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list0(commas, item)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(tag(","))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("}")(i)?;
	Ok((
		i,
		Object {
			value: v,
		},
	))
}

fn item(i: &str) -> IResult<&str, (String, Value)> {
	let (i, k) = key(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag(":")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = expression(i)?;
	Ok((i, (String::from(k), Value::from(v))))
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
		assert_eq!(out.value.len(), 3);
	}

	#[test]
	fn object_commas() {
		let sql = "{one:1,two:2,tre:3,}";
		let res = object(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, two: 2, tre: 3 }", format!("{}", out));
		assert_eq!(out.value.len(), 3);
	}

	#[test]
	fn object_expression() {
		let sql = "{one:1,two:2,tre:3+1}";
		let res = object(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, two: 2, tre: 3 + 1 }", format!("{}", out));
		assert_eq!(out.value.len(), 3);
	}
}
