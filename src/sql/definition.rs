use crate::sql::literal::{literal, Literal};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Definition {
	All,
	Void,
	Empty,
	Filled,
	Literal(Literal),
}

impl<'a> From<&'a str> for Definition {
	fn from(s: &str) -> Self {
		definition(s).unwrap().1
	}
}

impl From<Literal> for Definition {
	fn from(v: Literal) -> Self {
		Definition::Literal(v)
	}
}

impl fmt::Display for Definition {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Definition::All => write!(f, "*"),
			Definition::Void => write!(f, "VOID"),
			Definition::Empty => write!(f, "EMPTY"),
			Definition::Filled => write!(f, "FILLED"),
			Definition::Literal(v) => write!(f, "{}", v),
		}
	}
}

pub fn definition(i: &str) -> IResult<&str, Definition> {
	alt((
		map(tag_no_case("*"), |_| Definition::All),
		map(tag_no_case("VOID"), |_| Definition::Void),
		map(tag_no_case("EMPTY"), |_| Definition::Empty),
		map(tag_no_case("FILLED"), |_| Definition::Filled),
		map(tag_no_case("MISSING"), |_| Definition::Empty),
		map(literal, |v| Definition::Literal(v)),
	))(i)
}
