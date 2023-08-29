use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::error::IResult;
use crate::sql::value::Value;
use derive::Store;
use nom::bytes::complete::tag_no_case;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct ContinueStatement;

impl ContinueStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		false
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		Err(Error::Continue)
	}
}

impl fmt::Display for ContinueStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("CONTINUE")
	}
}

pub fn r#continue(i: &str) -> IResult<&str, ContinueStatement> {
	let (i, _) = tag_no_case("CONTINUE")(i)?;
	Ok((i, ContinueStatement))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn continue_basic() {
		let sql = "CONTINUE";
		let res = r#continue(sql);
		let out = res.unwrap().1;
		assert_eq!("CONTINUE", format!("{}", out))
	}
}
