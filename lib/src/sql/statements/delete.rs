use crate::dbs::Iterator;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::error::IResult;
use crate::sql::output::{output, Output};
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::value::{whats, Value, Values};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeleteStatement {
	pub what: Values,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
}

impl DeleteStatement {
	pub async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::No)?;
		// Create a new iterator
		let mut i = Iterator::from(self);
		// Ensure futures are stored
		let opt = &opt.futures(false);
		// Loop over the delete targets
		for w in self.what.0.iter() {
			let v = w.compute(ctx, opt, txn, doc).await?;
			match v {
				Value::Table(_) => i.prepare(v),
				Value::Thing(_) => i.prepare(v),
				Value::Model(_) => i.prepare(v),
				Value::Array(_) => i.prepare(v),
				v => {
					return Err(Error::DeleteStatementError {
						value: v,
					})
				}
			};
		}
		// Output the results
		i.output(ctx, opt, txn).await
	}
}

impl fmt::Display for DeleteStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DELETE {}", self.what)?;
		if let Some(ref v) = self.cond {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.output {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {}", v)?
		}
		Ok(())
	}
}

pub fn delete(i: &str) -> IResult<&str, DeleteStatement> {
	let (i, _) = tag_no_case("DELETE")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("FROM"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = whats(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	Ok((
		i,
		DeleteStatement {
			what,
			cond,
			output,
			timeout,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn delete_statement() {
		let sql = "DELETE test";
		let res = delete(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("DELETE test", format!("{}", out))
	}
}
