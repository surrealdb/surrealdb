use crate::cnf::ID_CHARS;
use crate::sql::array::{array, Array};
use crate::sql::error::IResult;
use crate::sql::escape::escape_id;
use crate::sql::ident::ident_raw;
use crate::sql::number::integer;
use crate::sql::object::{object, Object};
use crate::sql::strand::Strand;
use crate::sql::uuid::Uuid;
use nanoid::nanoid;
use nom::branch::alt;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Id {
	Number(i64),
	String(String),
	Array(Array),
	Object(Object),
}

impl From<i64> for Id {
	fn from(v: i64) -> Self {
		Id::Number(v)
	}
}

impl From<i32> for Id {
	fn from(v: i32) -> Self {
		Id::Number(v as i64)
	}
}

impl From<u64> for Id {
	fn from(v: u64) -> Self {
		Id::Number(v as i64)
	}
}

impl From<String> for Id {
	fn from(v: String) -> Self {
		Id::String(v)
	}
}

impl From<Array> for Id {
	fn from(v: Array) -> Self {
		Id::Array(v)
	}
}

impl From<Object> for Id {
	fn from(v: Object) -> Self {
		Id::Object(v)
	}
}

impl From<Uuid> for Id {
	fn from(v: Uuid) -> Self {
		Id::String(v.to_raw())
	}
}

impl From<Strand> for Id {
	fn from(v: Strand) -> Self {
		Id::String(v.as_string())
	}
}

impl From<&str> for Id {
	fn from(v: &str) -> Self {
		Id::String(v.to_owned())
	}
}

impl Id {
	pub fn rand() -> Id {
		Id::String(nanoid!(20, &ID_CHARS))
	}
	pub fn to_raw(&self) -> String {
		match self {
			Id::Number(v) => v.to_string(),
			Id::String(v) => v.to_string(),
			Id::Object(v) => v.to_string(),
			Id::Array(v) => v.to_string(),
		}
	}
}

impl fmt::Display for Id {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Id::Number(v) => write!(f, "{}", v),
			Id::String(v) => write!(f, "{}", escape_id(v)),
			Id::Object(v) => write!(f, "{}", v),
			Id::Array(v) => write!(f, "{}", v),
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
	}

	#[test]
	fn id_number() {
		let sql = "100";
		let res = id(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(Id::from(100), out);
	}

	#[test]
	fn id_string() {
		let sql = "test";
		let res = id(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(Id::from("test"), out);
	}

	#[test]
	fn id_either() {
		let sql = "100test";
		let res = id(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(Id::from("100test"), out);
	}
}
