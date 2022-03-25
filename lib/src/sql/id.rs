use crate::cnf::ID_CHARS;
use crate::sql::common::escape;
use crate::sql::common::val_char;
use crate::sql::error::IResult;
use crate::sql::ident::ident_raw;
use crate::sql::number::{number, Number};
use crate::sql::strand::Strand;
use nanoid::nanoid;
use nom::branch::alt;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Id {
	Number(Number),
	String(String),
}

impl From<Number> for Id {
	fn from(v: Number) -> Self {
		Id::Number(v)
	}
}

impl From<String> for Id {
	fn from(v: String) -> Self {
		Id::String(v)
	}
}

impl From<Strand> for Id {
	fn from(v: Strand) -> Self {
		Id::String(v.value)
	}
}

impl From<&str> for Id {
	fn from(v: &str) -> Self {
		Id::String(v.to_owned())
	}
}

impl From<u64> for Id {
	fn from(v: u64) -> Self {
		Id::Number(Number::from(v))
	}
}

impl Id {
	pub fn rand() -> Id {
		Id::String(nanoid!(20, &ID_CHARS))
	}
}

impl fmt::Display for Id {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Id::Number(v) => write!(f, "{}", v),
			Id::String(v) => write!(f, "{}", escape(v, &val_char, "`")),
		}
	}
}

pub fn id(i: &str) -> IResult<&str, Id> {
	alt((map(number, Id::Number), map(ident_raw, Id::String)))(i)
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
