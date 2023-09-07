use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::common::{closebraces, openbraces};
use crate::sql::common::{commas, val_char};
use crate::sql::error::{expected, IResult};
use crate::sql::escape::escape_key;
use crate::sql::fmt::{is_pretty, pretty_indent, Fmt, Pretty};
use crate::sql::operation::Operation;
use crate::sql::thing::Thing;
use crate::sql::util::expect_terminator;
use crate::sql::value::{value, Value};
use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::bytes::complete::take_while1;
use nom::character::complete::char;
use nom::combinator::{cut, opt};
use nom::sequence::delimited;
use nom::Err;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter, Write};
use std::ops::Deref;
use std::ops::DerefMut;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Object";

/// Invariant: Keys never contain NUL bytes.
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Object")]
#[revisioned(revision = 1)]
pub struct Object(#[serde(with = "no_nul_bytes_in_keys")] pub BTreeMap<String, Value>);

impl From<BTreeMap<String, Value>> for Object {
	fn from(v: BTreeMap<String, Value>) -> Self {
		Self(v)
	}
}

impl From<HashMap<&str, Value>> for Object {
	fn from(v: HashMap<&str, Value>) -> Self {
		Self(v.into_iter().map(|(key, val)| (key.to_string(), val)).collect())
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
		Self(match v {
			Operation::Add {
				path,
				value,
			} => map! {
				String::from("op") => Value::from("add"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
			Operation::Remove {
				path,
			} => map! {
				String::from("op") => Value::from("remove"),
				String::from("path") => path.to_path().into()
			},
			Operation::Replace {
				path,
				value,
			} => map! {
				String::from("op") => Value::from("replace"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
			Operation::Change {
				path,
				value,
			} => map! {
				String::from("op") => Value::from("change"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
			Operation::Copy {
				path,
				from,
			} => map! {
				String::from("op") => Value::from("copy"),
				String::from("path") => path.to_path().into(),
				String::from("from") => from.to_path().into()
			},
			Operation::Move {
				path,
				from,
			} => map! {
				String::from("op") => Value::from("move"),
				String::from("path") => path.to_path().into(),
				String::from("from") => from.to_path().into()
			},
			Operation::Test {
				path,
				value,
			} => map! {
				String::from("op") => Value::from("test"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
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
			Some(op_val) => match self.get("path") {
				Some(path_val) => {
					let path = path_val.jsonpath();

					let from =
						self.get("from").map(|value| value.jsonpath()).ok_or(Error::InvalidPatch {
							message: String::from("'from' key missing"),
						});

					let value = self.get("value").cloned().ok_or(Error::InvalidPatch {
						message: String::from("'value' key missing"),
					});

					match op_val.clone().as_string().as_str() {
						// Add operation
						"add" => Ok(Operation::Add {
							path,
							value: value?,
						}),
						// Remove operation
						"remove" => Ok(Operation::Remove {
							path,
						}),
						// Replace operation
						"replace" => Ok(Operation::Replace {
							path,
							value: value?,
						}),
						// Change operation
						"change" => Ok(Operation::Change {
							path,
							value: value?,
						}),
						// Copy operation
						"copy" => Ok(Operation::Copy {
							path,
							from: from?,
						}),
						// Move operation
						"move" => Ok(Operation::Move {
							path,
							from: from?,
						}),
						// Test operation
						"test" => Ok(Operation::Test {
							path,
							value: value?,
						}),
						unknown_op => Err(Error::InvalidPatch {
							message: format!("unknown op '{unknown_op}'"),
						}),
					}
				}
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
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
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

impl Display for Object {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		if is_pretty() {
			f.write_char('{')?;
		} else {
			f.write_str("{ ")?;
		}
		if !self.is_empty() {
			let indent = pretty_indent();
			write!(
				f,
				"{}",
				Fmt::pretty_comma_separated(
					self.0.iter().map(|args| Fmt::new(args, |(k, v), f| write!(
						f,
						"{}: {}",
						escape_key(k),
						v
					))),
				)
			)?;
			drop(indent);
		}
		if is_pretty() {
			f.write_char('}')
		} else {
			f.write_str(" }")
		}
	}
}

mod no_nul_bytes_in_keys {
	use serde::{
		de::{self, Visitor},
		ser::SerializeMap,
		Deserializer, Serializer,
	};
	use std::{collections::BTreeMap, fmt};

	use crate::sql::Value;

	pub(crate) fn serialize<S>(
		m: &BTreeMap<String, Value>,
		serializer: S,
	) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let mut s = serializer.serialize_map(Some(m.len()))?;
		for (k, v) in m {
			debug_assert!(!k.contains('\0'));
			s.serialize_entry(k, v)?;
		}
		s.end()
	}

	pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<String, Value>, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct NoNulBytesInKeysVisitor;

		impl<'de> Visitor<'de> for NoNulBytesInKeysVisitor {
			type Value = BTreeMap<String, Value>;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("a map without any NUL bytes in its keys")
			}

			fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
			where
				A: de::MapAccess<'de>,
			{
				let mut ret = BTreeMap::new();
				while let Some((k, v)) = map.next_entry()? {
					ret.insert(k, v);
				}
				Ok(ret)
			}
		}

		deserializer.deserialize_map(NoNulBytesInKeysVisitor)
	}
}

pub fn object(i: &str) -> IResult<&str, Object> {
	fn entry(i: &str) -> IResult<&str, (String, Value)> {
		let (i, k) = key(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = expected("`:`", char(':'))(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, v) = cut(value)(i)?;
		Ok((i, (String::from(k), v)))
	}

	let start = i;
	let (i, _) = openbraces(i)?;
	let (i, first) = match entry(i) {
		Ok(x) => x,
		Err(Err::Error(_)) => {
			let (i, _) = closebraces(i)?;
			return Ok((i, Object(BTreeMap::new())));
		}
		Err(Err::Failure(x)) => return Err(Err::Failure(x)),
		Err(Err::Incomplete(x)) => return Err(Err::Incomplete(x)),
	};

	let mut tree = BTreeMap::new();
	tree.insert(first.0, first.1);

	let mut input = i;
	while let (i, Some(_)) = opt(commas)(input)? {
		if let (i, Some(_)) = opt(closebraces)(i)? {
			return Ok((i, Object(tree)));
		}
		let (i, v) = cut(entry)(i)?;
		tree.insert(v.0, v.1);
		input = i
	}
	let (i, _) = expect_terminator(start, closebraces)(input)?;
	Ok((i, Object(tree)))
}

pub fn key(i: &str) -> IResult<&str, &str> {
	alt((key_none, key_single, key_double))(i)
}

fn key_none(i: &str) -> IResult<&str, &str> {
	take_while1(val_char)(i)
}

fn key_single(i: &str) -> IResult<&str, &str> {
	delimited(char('\''), is_not("\'\0"), char('\''))(i)
}

fn key_double(i: &str) -> IResult<&str, &str> {
	delimited(char('\"'), is_not("\"\0"), char('\"'))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn object_normal() {
		let sql = "{one:1,two:2,tre:3}";
		let res = object(sql);
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, tre: 3, two: 2 }", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn object_commas() {
		let sql = "{one:1,two:2,tre:3,}";
		let res = object(sql);
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, tre: 3, two: 2 }", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}

	#[test]
	fn object_expression() {
		let sql = "{one:1,two:2,tre:3+1}";
		let res = object(sql);
		let out = res.unwrap().1;
		assert_eq!("{ one: 1, tre: 3 + 1, two: 2 }", format!("{}", out));
		assert_eq!(out.0.len(), 3);
	}
}
