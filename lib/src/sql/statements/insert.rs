use crate::ctx::Context;
use crate::dbs::Iterable;
use crate::dbs::Iterator;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Statement;
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
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
	pub(crate) fn writeable(&self) -> bool {
		true
	}

	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Selected DB?
		opt.needs(Level::Db)?;
		// Allowed to run?
		opt.check(Level::No)?;
		// Create a new iterator
		let mut i = Iterator::new();
		// Ensure futures are stored
		let opt = &opt.futures(false);
		// Parse the expression
		match &self.data {
			// Check if this is a traditional statement
			Data::ValuesExpression(v) => {
				for v in v {
					// Create a new empty base object
					let mut o = Value::base();
					// Set each field from the expression
					for (k, v) in v.iter() {
						let v = v.compute(ctx, opt, txn, None).await?;
						o.set(ctx, opt, txn, k, v).await?;
					}
					// Specify the new table record id
					let id = o.rid().generate(&self.into, true)?;
					// Pass the mergeable to the iterator
					i.ingest(Iterable::Mergeable(id, o));
				}
			}
			// Check if this is a modern statement
			Data::SingleExpression(v) => {
				let v = v.compute(ctx, opt, txn, doc).await?;
				match v {
					Value::Array(v) => {
						for v in v {
							// Specify the new table record id
							let id = v.rid().generate(&self.into, true)?;
							// Pass the mergeable to the iterator
							i.ingest(Iterable::Mergeable(id, v));
						}
					}
					Value::Object(_) => {
						// Specify the new table record id
						let id = v.rid().generate(&self.into, true)?;
						// Pass the mergeable to the iterator
						i.ingest(Iterable::Mergeable(id, v));
					}
					v => {
						return Err(Error::InsertStatement {
							value: v.to_string(),
						})
					}
				}
			}
			_ => unreachable!(),
		}
		// Assign the statement
		let stm = Statement::from(self);
		// Output the results
		i.output(ctx, opt, txn, &stm).await
	}
}

impl fmt::Display for InsertStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("INSERT")?;
		if self.ignore {
			f.write_str(" IGNORE")?
		}
		write!(f, " INTO {} {}", self.into, self.data)?;
		if let Some(ref v) = self.output {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {v}")?
		}
		if self.parallel {
			f.write_str(" PARALLEL")?
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
