use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::strand::strand_raw;
use crate::sql::value::Value;
use derive::Store;
use nom::bytes::complete::tag_no_case;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct ThrowStatement(pub String);

impl ThrowStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		Err(Error::Thrown(self.0.to_owned()))
	}
}

impl fmt::Display for ThrowStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "THROW {}", self.0)
	}
}

pub fn throw(i: &str) -> IResult<&str, ThrowStatement> {
	let (i, _) = tag_no_case("THROW")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, e) = strand_raw(i)?;
	Ok((i, ThrowStatement(e)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn throw_basic() {
		let sql = "THROW 'Record does not exist'";
		let res = throw(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("THROW 'Record does not exist'", format!("{}", out))
	}
}
