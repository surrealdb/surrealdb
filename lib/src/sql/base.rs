use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{cut, value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::error::expected;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
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
	expected(
		"a base, one of NAMESPACE, DATABASE, ROOT or KV",
		alt((
			value(Base::Ns, tag_no_case("NAMESPACE")),
			value(Base::Db, tag_no_case("DATABASE")),
			value(Base::Root, tag_no_case("ROOT")),
			value(Base::Ns, tag_no_case("NS")),
			value(Base::Db, tag_no_case("DB")),
			value(Base::Root, tag_no_case("KV")),
		)),
	)(i)
}

pub fn base_or_scope(i: &str) -> IResult<&str, Base> {
	alt((base, |i| {
		let (i, _) = tag_no_case("SCOPE")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, v) = cut(ident)(i)?;
		Ok((i, Base::Sc(v)))
	}))(i)
}
