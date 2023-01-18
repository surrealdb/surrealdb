use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::common::{commas, val_char};
use crate::sql::error::IResult;
use crate::sql::escape::escape_key;
use crate::sql::fmt::Fmt;
use crate::sql::operation::{Op, Operation};
use crate::sql::serde::is_internal_serialization;
use crate::sql::thing::Thing;
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::bytes::complete::take_while1;
use nom::character::complete::char;
use nom::combinator::opt;
use nom::multi::separated_list0;
use nom::sequence::delimited;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::ops::Deref;
use std::ops::DerefMut;

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Deserialize, Hash)]
pub struct Object(pub BTreeMap<String, Value>);

impl From<BTreeMap<String, Value>> for Object {
	fn from(v: BTreeMap<String, Value>) -> Self {
		Self(v)
	}
}

impl From<HashMap<String, Value>> for Object {
	fn from(v: HashMap<String, Value>) -> Self {
		Self(v.into_iter().collect())
	}
}

impl From<Option<Self>> for Object {
	fn from(v: Option<Self>) -> Self {
		v.unwrap_or_default()
	}
}

impl From<Operation> for Object {
	fn from(v: Operation) -> Self {
		Self(map! {
			String::from("op") => match v.op {
				Op::None => Value::from("none"),
				Op::Add => Value::from("add"),
				Op::Remove => Value::from("remove"),
				Op::Replace => Value::from("replace"),
				Op::Change => Value::from("change"),
			},
			String::from("path") => v.path.to_path().into(),
			String::from("value") => v.value,
		})
	}
}

impl Deref for Object {
	type Target = BTreeMap<String, Value>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Object {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl IntoIterator for Object {
	type Item = (String, Value);
	type IntoIter = std::collections::btree_map::IntoIter<String, Value>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Object {
	/// Fetch the record id if there is one
	pub fn rid(&self) -> Option<Thing> {
		match self.get("id") {
			Some(Value::Thing(v)) => Some(v.clone()),
			_ => None,
		}
	}
	/// Convert this object to a diff-match-patch operation
	pub fn to_operation(&self) -> Result<Operation, Error> {
		match self.get("op") {
			Some(o) => match self.get("path") {
				Some(p) => Ok(Operation {
					op: o.into(),
					path: p.jsonpath(),
					value: match self.get("value") {
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
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		let mut x = BTreeMap::new();
		for (k, v) in self.iter() {
			match v.compute(ctx, opt, txn, doc).await {
				Ok(v) => x.insert(k.clone(), v),
				Err(e) => return Err(e),
			};
		}
		Ok(Value::Object(Object(x)))
	}
}

impl fmt::Display for Object {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"{{ {} }}",
			Fmt::comma_separated(
				self.0.iter().map(|args| Fmt::new(args, |(k, v), f| {
					write!(f, "{}: {}", escape_key(k), v)
				}))
			)
		)
	}
}

impl Serialize for Object {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			serializer.serialize_newtype_struct("Object", &self.0)
		} else {
			let mut map = serializer.serialize_map(Some(self.len()))?;
			for (ref k, ref v) in &self.0 {
				map.serialize_key(k)?;
				map.serialize_value(v)?;
			}
			map.end()
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
	Ok((i, Object(v.into_iter().collect())))
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
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn object_commas() {
		let sql = "{one:1,two:2,tre:3,}";
		let res = object(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, tre: 3, two: 2 }", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn object_expression() {
		let sql = "{one:1,two:2,tre:3+1}";
		let res = object(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, tre: 3 + 1, two: 2 }", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}
}
