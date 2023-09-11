use crate::ctx::Context;
use crate::dbs::Iterator;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::error::IResult;
use crate::sql::output::{output, Output};
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::value::{whats, Value, Values};
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 2)]
pub struct DeleteStatement {
	#[revision(start = 2)]
	pub only: bool,
	pub what: Values,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl DeleteStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Valid options?
		opt.valid_for_db()?;
		// Create a new iterator
		let mut i = Iterator::new();
		// Assign the statement
		let stm = Statement::from(self);
		// Ensure futures are stored
		let opt = &opt.new_with_futures(false).with_projections(false);
		// Loop over the delete targets
		for w in self.what.0.iter() {
			let v = w.compute(ctx, opt, txn, doc).await?;
			i.prepare(ctx, opt, txn, &stm, v).await.map_err(|e| match e {
				Error::InvalidStatementTarget {
					value: v,
				} => Error::DeleteStatement {
					value: v,
				},
				e => e,
			})?;
		}
		// Output the results
		match i.output(ctx, opt, txn, &stm).await? {
			// This is a single record result
			Value::Array(mut a) if self.only => match a.len() {
				// There was exactly one result
				1 => Ok(a.remove(0)),
				// There were no results
				_ => Err(Error::SingleOnlyOutput),
			},
			// This is standard query result
			v => Ok(v),
		}
	}
}

impl fmt::Display for DeleteStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DELETE")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {}", self.what)?;
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
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

pub fn delete(i: &str) -> IResult<&str, DeleteStatement> {
	let (i, _) = tag_no_case("DELETE")(i)?;
	let (i, _) = opt(preceded(shouldbespace, tag_no_case("FROM")))(i)?;
	let (i, only) = opt(preceded(shouldbespace, tag_no_case("ONLY")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = whats(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
	Ok((
		i,
		DeleteStatement {
			only: only.is_some(),
			what,
			cond,
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
	fn delete_statement() {
		let sql = "DELETE test";
		let res = delete(sql);
		let out = res.unwrap().1;
		assert_eq!("DELETE test", format!("{}", out))
	}
}
