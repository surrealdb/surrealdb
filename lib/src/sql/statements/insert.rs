use crate::dbs::Iterator;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::data::{single, update, values, Data};
use crate::sql::error::IResult;
use crate::sql::output::{output, Output};
use crate::sql::table::{table, Table};
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct InsertStatement {
	pub into: Table,
	pub data: Data,
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl InsertStatement {
	pub(crate) async fn compute(
		self: &Arc<Self>,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.check(Level::No)?;
		// Clone the statement
		let s = Arc::clone(self);
		// Create a new iterator
		let mut i = Iterator::from(s);
		// Ensure futures are stored
		let opt = &opt.futures(false);
		// Parse the expression
		match &self.data {
			Data::ValuesExpression(_) => {
				todo!() // TODO: loop over each
			}
			Data::SingleExpression(v) => {
				let v = v.compute(ctx, opt, txn, doc).await?;
				match v {
					Value::Array(v) => v.value.into_iter().for_each(|v| i.prepare(v)),
					Value::Object(_) => i.prepare(v),
					v => {
						return Err(Error::InsertStatement {
							value: v,
						})
					}
				}
			}
			_ => unreachable!(),
		}
		// Output the results
		i.output(ctx, opt, txn).await
	}
}

impl fmt::Display for InsertStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "INSERT")?;
		if self.ignore {
			write!(f, " IGNORE")?
		}
		write!(f, " INTO {} {}", self.into, self.data)?;
		if let Some(ref v) = self.output {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {}", v)?
		}
		if self.parallel {
			write!(f, " PARALLEL")?
		}
		Ok(())
	}
}

pub fn insert(i: &str) -> IResult<&str, InsertStatement> {
	let (i, _) = tag_no_case("INSERT")(i)?;
	let (i, ignore) = opt(preceded(shouldbespace, tag_no_case("IGNORE")))(i)?;
	let (i, _) = preceded(shouldbespace, tag_no_case("INTO"))(i)?;
	let (i, into) = preceded(shouldbespace, table)(i)?;
	let (i, data) = preceded(shouldbespace, alt((values, single)))(i)?;
	let (i, update) = opt(preceded(shouldbespace, update))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
	Ok((
		i,
		InsertStatement {
			into,
			data,
			ignore: ignore.is_some(),
			update,
			output,
			timeout,
			parallel: parallel.is_some(),
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn insert_statement_basic() {
		let sql = "INSERT INTO test (field) VALUES ($value)";
		let res = insert(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("INSERT INTO test (field) VALUES ($value)", format!("{}", out))
	}

	#[test]
	fn insert_statement_ignore() {
		let sql = "INSERT IGNORE INTO test (field) VALUES ($value)";
		let res = insert(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("INSERT IGNORE INTO test (field) VALUES ($value)", format!("{}", out))
	}
}
