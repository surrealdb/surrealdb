use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::kvs::transaction;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::value::Value;
use futures::lock::Mutex;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BeginStatement;

impl BeginStatement {
	pub async fn compute(
		&self,
		_ctx: &Runtime,
		_opt: &Options<'_>,
		exe: &mut Executor<'_>,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		match &exe.txn {
			Some(_) => Ok(Value::None),
			None => {
				let txn = transaction(true, false).await?;
				exe.txn = Some(Arc::new(Mutex::new(txn)));
				Ok(Value::None)
			}
		}
	}
}

impl fmt::Display for BeginStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "BEGIN TRANSACTION")
	}
}

pub fn begin(i: &str) -> IResult<&str, BeginStatement> {
	alt((begin_query, begin_basic))(i)
}

fn begin_basic(i: &str) -> IResult<&str, BeginStatement> {
	let (i, _) = tag_no_case("BEGIN")(i)?;
	Ok((i, BeginStatement))
}

fn begin_query(i: &str) -> IResult<&str, BeginStatement> {
	let (i, _) = tag_no_case("BEGIN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TRANSACTION"))))(i)?;
	Ok((i, BeginStatement))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn begin_basic() {
		let sql = "BEGIN";
		let res = begin(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("BEGIN TRANSACTION", format!("{}", out))
	}

	#[test]
	fn begin_query() {
		let sql = "BEGIN TRANSACTION";
		let res = begin(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("BEGIN TRANSACTION", format!("{}", out))
	}
}
