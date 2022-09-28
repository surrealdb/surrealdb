use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
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
	Sc(Ident),
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
			Base::Sc(sc) => write!(f, "SCOPE {}", sc),
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

pub fn base_or_scope(i: &str) -> IResult<&str, Base> {
	alt((
		map(tag_no_case("NAMESPACE"), |_| Base::Ns),
		map(tag_no_case("DATABASE"), |_| Base::Db),
		map(tag_no_case("NS"), |_| Base::Ns),
		map(tag_no_case("DB"), |_| Base::Db),
		|i| {
			let (i, _) = tag_no_case("SCOPE")(i)?;
			let (i, _) = shouldbespace(i)?;
			let (i, v) = ident(i)?;
			Ok((i, Base::Sc(v)))
		},
	))(i)
}
