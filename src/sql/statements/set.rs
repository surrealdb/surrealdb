use crate::ctx::Parent;
use crate::dbs;
use crate::dbs::Executor;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::expression::{expression, Expression};
use crate::sql::literal::Literal;
use crate::sql::param::{param, Param};
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SetStatement {
	pub name: Param,
	pub what: Expression,
}

impl fmt::Display for SetStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LET {} = {}", self.name, self.what)
	}
}

impl dbs::Process for SetStatement {
	fn process(
		&self,
		ctx: &Parent,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error> {
		todo!()
	}
}

pub fn set(i: &str) -> IResult<&str, SetStatement> {
	let (i, _) = tag_no_case("LET")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, n) = param(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("=")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, w) = expression(i)?;
	Ok((
		i,
		SetStatement {
			name: n,
			what: w,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn let_statement() {
		let sql = "LET $name = NULL";
		let res = set(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("LET $name = NULL", format!("{}", out));
	}
}
