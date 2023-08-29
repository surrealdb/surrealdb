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
pub struct BreakStatement;

impl BreakStatement {
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
		Err(Error::Break)
	}
}

impl fmt::Display for BreakStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("BREAK")
	}
}

pub fn r#break(i: &str) -> IResult<&str, BreakStatement> {
	let (i, _) = tag_no_case("BREAK")(i)?;
	Ok((i, BreakStatement))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn break_basic() {
		let sql = "BREAK";
		let res = r#break(sql);
		let out = res.unwrap().1;
		assert_eq!("BREAK", format!("{}", out))
	}
}
