use crate::ctx::Parent;
use crate::dbs;
use crate::dbs::Executor;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::expression::expression;
use crate::sql::literal::Literal;
use crate::sql::value::Value;
use nom::bytes::complete::tag;
use nom::combinator::opt;
use nom::multi::separated_list0;
use nom::IResult;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;

const NAME: &'static str = "Array";

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize)]
pub struct Array {
	pub value: Vec<Value>,
}

impl fmt::Display for Array {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"[{}]",
			self.value.iter().map(|ref v| format!("{}", v)).collect::<Vec<_>>().join(", ")
		)
	}
}

impl dbs::Process for Array {
	fn process(
		&self,
		ctx: &Parent,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error> {
		self.value
			.iter()
			.map(|v| match v.process(ctx, exe, doc) {
				Ok(v) => Ok(Value::from(v)),
				Err(e) => Err(e),
			})
			.collect::<Result<Vec<_>, _>>()
			.map(|v| {
				Literal::Array(Array {
					value: v,
				})
			})
	}
}

impl Serialize for Array {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if serializer.is_human_readable() {
			serializer.serialize_some(&self.value)
		} else {
			let mut val = serializer.serialize_struct(NAME, 1)?;
			val.serialize_field("value", &self.value)?;
			val.end()
		}
	}
}

pub fn array(i: &str) -> IResult<&str, Array> {
	let (i, _) = tag("[")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list0(commas, item)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(tag(","))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("]")(i)?;
	Ok((
		i,
		Array {
			value: v,
		},
	))
}

fn item(i: &str) -> IResult<&str, Value> {
	let (i, v) = expression(i)?;
	Ok((i, Value::from(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn array_normal() {
		let sql = "[1,2,3]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3]", format!("{}", out));
		assert_eq!(out.value.len(), 3);
	}

	#[test]
	fn array_commas() {
		let sql = "[1,2,3,]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3]", format!("{}", out));
		assert_eq!(out.value.len(), 3);
	}

	#[test]
	fn array_expression() {
		let sql = "[1,2,3+1]";
		let res = array(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("[1, 2, 3 + 1]", format!("{}", out));
		assert_eq!(out.value.len(), 3);
	}
}
