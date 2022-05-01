use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::common::{commas, escape, val_char};
use crate::sql::error::IResult;
use crate::sql::operation::{Op, Operation};
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::bytes::complete::take_while1;
use nom::character::complete::char;
use nom::combinator::opt;
use nom::multi::separated_list0;
use nom::sequence::delimited;
use serde::ser::SerializeMap;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Deserialize)]
pub struct Object {
	pub value: BTreeMap<String, Value>,
}

impl From<BTreeMap<String, Value>> for Object {
	fn from(v: BTreeMap<String, Value>) -> Self {
		Object {
			value: v,
		}
	}
}

impl From<HashMap<String, Value>> for Object {
	fn from(v: HashMap<String, Value>) -> Self {
		Object {
			value: v.into_iter().collect(),
		}
	}
}

impl From<Operation> for Object {
	fn from(v: Operation) -> Self {
		Object {
			value: map! {
				String::from("op") => match v.op {
					Op::None => Value::from("none"),
					Op::Add => Value::from("add"),
					Op::Remove => Value::from("remove"),
					Op::Replace => Value::from("replace"),
					Op::Change => Value::from("change"),
				},
				String::from("path") => v.path.to_path().into(),
				String::from("value") => v.value,
			},
		}
	}
}

impl Object {
	pub fn remove(&mut self, key: &str) {
		self.value.remove(key);
	}
	pub fn insert(&mut self, key: &str, val: Value) {
		self.value.insert(key.to_owned(), val);
	}
	pub fn to_operation(&self) -> Result<Operation, Error> {
		match self.value.get("op") {
			Some(o) => match self.value.get("path") {
				Some(p) => Ok(Operation {
					op: o.into(),
					path: p.to_idiom(),
					value: match self.value.get("value") {
						Some(v) => v.clone(),
						None => Value::Null,
					},
				}),
				_ => Err(Error::InvalidPatch {
					message: String::from("'path' key missing"),
				}),
			},
			_ => Err(Error::InvalidPatch {
				message: String::from("'op' key missing"),
			}),
		}
	}
}

impl Object {
	pub(crate) async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		let mut x = BTreeMap::new();
		for (k, v) in &self.value {
			match v.compute(ctx, opt, txn, doc).await {
				Ok(v) => x.insert(k.clone(), v),
				Err(e) => return Err(e),
			};
		}
		Ok(Value::Object(Object {
			value: x,
		}))
	}
}

impl fmt::Display for Object {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"{{ {} }}",
			self.value
				.iter()
				.map(|(k, v)| format!("{}: {}", escape(k, &val_char, "\""), v))
				.collect::<Vec<_>>()
				.join(", ")
		)
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
			let mut val = serializer.serialize_struct("Object", 1)?;
			val.serialize_field("value", &self.value)?;
			val.end()
		}
	}
}

pub fn object(i: &str) -> IResult<&str, Object> {
	let (i, _) = char('{')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list0(commas, item)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(char(','))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('}')(i)?;
	Ok((
		i,
		Object {
			value: v.into_iter().collect(),
		},
	))
}

fn item(i: &str) -> IResult<&str, (String, Value)> {
	let (i, k) = key(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(':')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = value(i)?;
	Ok((i, (String::from(k), v)))
}

fn key(i: &str) -> IResult<&str, &str> {
	alt((key_none, key_single, key_double))(i)
}

fn key_none(i: &str) -> IResult<&str, &str> {
	take_while1(val_char)(i)
}

fn key_single(i: &str) -> IResult<&str, &str> {
	delimited(char('\''), is_not("\'"), char('\''))(i)
}

fn key_double(i: &str) -> IResult<&str, &str> {
	delimited(char('\"'), is_not("\""), char('\"'))(i)
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
		assert_eq!("{ one: 1, tre: 3, two: 2 }", format!("{}", out));
		assert_eq!(out.value.len(), 3);
	}

	#[test]
	fn object_commas() {
		let sql = "{one:1,two:2,tre:3,}";
		let res = object(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, tre: 3, two: 2 }", format!("{}", out));
		assert_eq!(out.value.len(), 3);
	}

	#[test]
	fn object_expression() {
		let sql = "{one:1,two:2,tre:3+1}";
		let res = object(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, tre: 3 + 1, two: 2 }", format!("{}", out));
		assert_eq!(out.value.len(), 3);
	}
}
