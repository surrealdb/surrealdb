use crate::sql::error::IResult;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Base {
	Kv,
	Ns,
	Db,
}

impl Default for Base {
	fn default() -> Base {
		Base::Kv
	}
}

impl fmt::Display for Base {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Base::Ns => write!(f, "NAMESPACE"),
			Base::Db => write!(f, "DATABASE"),
			_ => write!(f, "KV"),
		}
	}
}

pub fn base(i: &str) -> IResult<&str, Base> {
	alt((
		map(tag_no_case("NAMESPACE"), |_| Base::Ns),
		map(tag_no_case("DATABASE"), |_| Base::Db),
		map(tag_no_case("NS"), |_| Base::Ns),
		map(tag_no_case("DB"), |_| Base::Db),
	))(i)
}
