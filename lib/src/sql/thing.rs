use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::error::IResult;
use crate::sql::escape::escape_rid;
use crate::sql::id::{id, Id};
use crate::sql::ident::ident_raw;
use crate::sql::strand::Strand;
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::combinator::map;
use nom::sequence::delimited;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Thing";

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[serde(rename = "$surrealdb::private::sql::Thing")]
pub struct Thing {
	pub tb: String,
	pub id: Id,
}

impl From<(&str, Id)> for Thing {
	fn from((tb, id): (&str, Id)) -> Self {
		Self {
			tb: tb.to_owned(),
			id,
		}
	}
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

impl FromStr for Thing {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::try_from(s)
	}
}

impl TryFrom<String> for Thing {
	type Error = ();
	fn try_from(v: String) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<Strand> for Thing {
	type Error = ();
	fn try_from(v: Strand) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<&str> for Thing {
	type Error = ();
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		match thing_raw(v) {
			Ok((_, v)) => Ok(v),
			_ => Err(()),
		}
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
			id: self.id.compute(ctx, opt, txn, doc).await?,
		}))
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
	let (i, v) = alt((
		map(tag("rand()"), |_| Id::rand()),
		map(tag("ulid()"), |_| Id::ulid()),
		map(tag("uuid()"), |_| Id::uuid()),
		id,
	))(i)?;
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
