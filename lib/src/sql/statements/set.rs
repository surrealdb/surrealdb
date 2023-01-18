use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::ident_raw;
use crate::sql::value::{value, Value};
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct SetStatement {
	pub name: String,
	pub what: Value,
}

impl SetStatement {
	pub(crate) fn writeable(&self) -> bool {
		self.what.writeable()
	}

	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		self.what.compute(ctx, opt, txn, doc).await
	}
}

impl fmt::Display for SetStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LET ${} = {}", self.name, self.what)
	}
}

pub fn set(i: &str) -> IResult<&str, SetStatement> {
	let (i, _) = tag_no_case("LET")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, n) = preceded(char('$'), ident_raw)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('=')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, w) = value(i)?;
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
