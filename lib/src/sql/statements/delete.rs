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
use nom::combinator::cut;
use nom::combinator::opt;
use nom::sequence::preceded;
use nom::sequence::terminated;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DeleteStatement {
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
	/// Check if this statement is for a single record
	pub(crate) fn single(&self) -> bool {
		match self.what.len() {
			1 if self.what[0].is_object() => true,
			1 if self.what[0].is_thing() => true,
			_ => false,
		}
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
		i.output(ctx, opt, txn, &stm).await
	}
}

impl fmt::Display for DeleteStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DELETE {}", self.what)?;
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
	let (i, _) = shouldbespace(i)?;
	let (i, _) = opt(terminated(tag_no_case("FROM"), shouldbespace))(i)?;
	let (i, what) = whats(i)?;
	let (i, (cond, output, timeout, parallel)) = cut(|i| {
		let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
		let (i, output) = opt(preceded(shouldbespace, output))(i)?;
		let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
		let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
		Ok((i, (cond, output, timeout, parallel)))
	})(i)?;
	Ok((
		i,
		DeleteStatement {
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
