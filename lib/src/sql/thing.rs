use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::error::IResult;
use crate::sql::escape::escape_rid;
use crate::sql::id::{id, Id};
use crate::sql::ident::ident_raw;
use crate::sql::serde::is_internal_serialization;
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::character::complete::char;
use nom::sequence::delimited;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Deserialize, Store, Hash)]
pub struct Thing {
	pub tb: String,
	pub id: Id,
}

impl From<(String, Id)> for Thing {
	fn from((tb, id): (String, Id)) -> Self {
		Self {
			tb,
			id,
		}
	}
}

impl From<(String, String)> for Thing {
	fn from((tb, id): (String, String)) -> Self {
		Self::from((tb, Id::from(id)))
	}
}

impl From<(&str, &str)> for Thing {
	fn from((tb, id): (&str, &str)) -> Self {
		Self::from((tb.to_owned(), Id::from(id)))
	}
}

impl Thing {
	/// Convert the Thing to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
}

impl fmt::Display for Thing {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", escape_rid(&self.tb), self.id)
	}
}

impl Thing {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		Ok(Value::Thing(Thing {
			tb: self.tb.clone(),
			id: match &self.id {
				Id::Number(v) => Id::Number(*v),
				Id::String(v) => Id::String(v.clone()),
				Id::Object(v) => match v.compute(ctx, opt, txn, doc).await? {
					Value::Object(v) => Id::Object(v),
					_ => unreachable!(),
				},
				Id::Array(v) => match v.compute(ctx, opt, txn, doc).await? {
					Value::Array(v) => Id::Array(v),
					_ => unreachable!(),
				},
			},
		}))
	}
}

impl Serialize for Thing {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			let mut val = serializer.serialize_struct("Thing", 2)?;
			val.serialize_field("tb", &self.tb)?;
			val.serialize_field("id", &self.id)?;
			val.end()
		} else {
			let output = self.to_string();
			serializer.serialize_some(&output)
		}
	}
}

pub fn thing(i: &str) -> IResult<&str, Thing> {
	alt((thing_raw, thing_single, thing_double))(i)
}

fn thing_single(i: &str) -> IResult<&str, Thing> {
	delimited(char('\''), thing_raw, char('\''))(i)
}

fn thing_double(i: &str) -> IResult<&str, Thing> {
	delimited(char('\"'), thing_raw, char('\"'))(i)
}

fn thing_raw(i: &str) -> IResult<&str, Thing> {
	let (i, t) = ident_raw(i)?;
	let (i, _) = char(':')(i)?;
	let (i, v) = id(i)?;
	Ok((
		i,
		Thing {
			tb: t,
			id: v,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::array::Array;
	use crate::sql::object::Object;
	use crate::sql::value::Value;

	#[test]
	fn thing_normal() {
		let sql = "test:id";
		let res = thing(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}

	#[test]
	fn thing_integer() {
		let sql = "test:001";
		let res = thing(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test:1", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from(1),
			}
		);
	}

	#[test]
	fn thing_quoted_backtick() {
		let sql = "`test`:`id`";
		let res = thing(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}

	#[test]
	fn thing_quoted_brackets() {
		let sql = "⟨test⟩:⟨id⟩";
		let res = thing(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}

	#[test]
	fn thing_object() {
		let sql = "test:{ location: 'GBR', year: 2022 }";
		let res = thing(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test:{ location: 'GBR', year: 2022 }", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::Object(Object::from(map! {
					"location".to_string() => Value::from("GBR"),
					"year".to_string() => Value::from(2022),
				})),
			}
		);
	}

	#[test]
	fn thing_array() {
		let sql = "test:['GBR', 2022]";
		let res = thing(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test:['GBR', 2022]", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::Array(Array::from(vec![Value::from("GBR"), Value::from(2022)])),
			}
		);
	}
}
