use crate::cnf::ID_CHARS;
use crate::sql::error::IResult;
use crate::sql::escape::escape_id;
use crate::sql::ident::ident_raw;
use crate::sql::number::integer;
use nanoid::nanoid;
use nom::branch::alt;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Id {
	Number(i64),
	String(String),
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
		}
	}
}

impl fmt::Display for Id {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Id::Number(v) => write!(f, "{}", v),
			Id::String(v) => write!(f, "{}", escape_id(v)),
		}
	}
}

pub fn id(i: &str) -> IResult<&str, Id> {
	alt((map(integer, Id::Number), map(ident_raw, Id::String)))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

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
