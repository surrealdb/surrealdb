use crate::cnf::ID_CHARS;
use crate::sql::array::{array, Array};
use crate::sql::error::IResult;
use crate::sql::escape::escape_rid;
use crate::sql::ident::ident_raw;
use crate::sql::number::integer;
use crate::sql::object::{object, Object};
use crate::sql::strand::Strand;
use crate::sql::uuid::Uuid;
use crate::sql::value::Value;
use nanoid::nanoid;
use nom::branch::alt;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Id {
	Number(i64),
	String(String),
	Array(Array),
	Object(Object),
}

impl From<i64> for Id {
	fn from(v: i64) -> Self {
		Self::Number(v)
	}
}

impl From<i32> for Id {
	fn from(v: i32) -> Self {
		Self::Number(v as i64)
	}
}

impl From<u64> for Id {
	fn from(v: u64) -> Self {
		Self::Number(v as i64)
	}
}

impl From<String> for Id {
	fn from(v: String) -> Self {
		Self::String(v)
	}
}

impl From<Array> for Id {
	fn from(v: Array) -> Self {
		Self::Array(v)
	}
}

impl From<Object> for Id {
	fn from(v: Object) -> Self {
		Self::Object(v)
	}
}

impl From<Uuid> for Id {
	fn from(v: Uuid) -> Self {
		Self::String(v.to_raw())
	}
}

impl From<Strand> for Id {
	fn from(v: Strand) -> Self {
		Self::String(v.as_string())
	}
}

impl From<&str> for Id {
	fn from(v: &str) -> Self {
		Self::String(v.to_owned())
	}
}

impl From<Vec<Value>> for Id {
	fn from(v: Vec<Value>) -> Self {
		Id::Array(v.into())
	}
}

impl Id {
	pub fn rand() -> Self {
		Self::String(nanoid!(20, &ID_CHARS))
	}
	pub fn to_raw(&self) -> String {
		match self {
			Self::Number(v) => v.to_string(),
			Self::String(v) => v.to_string(),
			Self::Object(v) => v.to_string(),
			Self::Array(v) => v.to_string(),
		}
	}
}

impl Display for Id {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Number(v) => Display::fmt(v, f),
			Self::String(v) => Display::fmt(&escape_rid(v), f),
			Self::Object(v) => Display::fmt(v, f),
			Self::Array(v) => Display::fmt(v, f),
		}
	}
}

pub fn id(i: &str) -> IResult<&str, Id> {
	alt((
		map(integer, Id::Number),
		map(ident_raw, Id::String),
		map(object, Id::Object),
		map(array, Id::Array),
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn id_int() {
		let sql = "001";
		let res = id(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(Id::from(1), out);
		assert_eq!("1", format!("{}", out));
	}

	#[test]
	fn id_number() {
		let sql = "100";
		let res = id(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(Id::from(100), out);
		assert_eq!("100", format!("{}", out));
	}

	#[test]
	fn id_string() {
		let sql = "test";
		let res = id(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(Id::from("test"), out);
		assert_eq!("test", format!("{}", out));
	}

	#[test]
	fn id_numeric() {
		let sql = "⟨100⟩";
		let res = id(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(Id::from("100"), out);
		assert_eq!("⟨100⟩", format!("{}", out));
	}

	#[test]
	fn id_either() {
		let sql = "100test";
		let res = id(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(Id::from("100test"), out);
		assert_eq!("100test", format!("{}", out));
	}
}
