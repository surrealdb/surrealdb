use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::block::{block, Block};
use crate::sql::comment::{mightbespace, shouldbespace};
use crate::sql::error::IResult;
use crate::sql::param::{param, Param};
use crate::sql::value::{value, Value};
use derive::Store;
use nom::bytes::complete::tag_no_case;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct ForeachStatement {
	pub param: Param,
	pub range: Value,
	pub block: Block,
}

impl ForeachStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.range.writeable() || self.block.writeable()
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "FOR statements",
		})
	}
}

impl Display for ForeachStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FOR {} IN {} {}", self.param, self.range, self.block)
	}
}

pub fn foreach(i: &str) -> IResult<&str, ForeachStatement> {
	let (i, _) = tag_no_case("FOR")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, param) = param(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("IN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, range) = value(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, block) = block(i)?;
	Ok((
		i,
		ForeachStatement {
			param,
			range,
			block,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn foreach_statement_first() {
		let sql = "FOR $test IN [1, 2, 3, 4, 5] { UPDATE person:test SET scores += $test; }";
		let res = foreach(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
