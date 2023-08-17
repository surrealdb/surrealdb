use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
pub enum Base {
	Root,
	Ns,
	Db,
	Sc(Ident),
}

impl Default for Base {
	fn default() -> Self {
		Self::Root
	}
}

impl fmt::Display for Base {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Ns => f.write_str("NAMESPACE"),
			Self::Db => f.write_str("DATABASE"),
			Self::Sc(sc) => write!(f, "SCOPE {sc}"),
			Self::Root => f.write_str("ROOT"),
		}
	}
}

pub fn base(i: &str) -> IResult<&str, Base> {
	alt((
		map(tag_no_case("NAMESPACE"), |_| Base::Ns),
		map(tag_no_case("DATABASE"), |_| Base::Db),
		map(tag_no_case("ROOT"), |_| Base::Root),
		map(tag_no_case("NS"), |_| Base::Ns),
		map(tag_no_case("DB"), |_| Base::Db),
		map(tag_no_case("KV"), |_| Base::Root),
	))(i)
}

pub fn base_or_scope(i: &str) -> IResult<&str, Base> {
	alt((
		map(tag_no_case("NAMESPACE"), |_| Base::Ns),
		map(tag_no_case("DATABASE"), |_| Base::Db),
		map(tag_no_case("ROOT"), |_| Base::Root),
		map(tag_no_case("NS"), |_| Base::Ns),
		map(tag_no_case("DB"), |_| Base::Db),
		map(tag_no_case("KV"), |_| Base::Root),
		|i| {
			let (i, _) = tag_no_case("SCOPE")(i)?;
			let (i, _) = shouldbespace(i)?;
			let (i, v) = ident(i)?;
			Ok((i, Base::Sc(v)))
		},
	))(i)
}
