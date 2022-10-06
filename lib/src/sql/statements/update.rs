use crate::ctx::Context;
use crate::dbs::Iterable;
use crate::dbs::Iterator;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::data::{data, Data};
use crate::sql::error::IResult;
use crate::sql::output::{output, Output};
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::value::{whats, Value, Values};
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct UpdateStatement {
	pub what: Values,
	pub data: Option<Data>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl UpdateStatement {
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
		// Loop over the update targets
		for w in self.what.0.iter() {
			let v = w.compute(ctx, opt, txn, doc).await?;
			match v {
				Value::Table(v) => i.ingest(Iterable::Table(v)),
				Value::Thing(v) => i.ingest(Iterable::Thing(v)),
				Value::Range(v) => i.ingest(Iterable::Range(*v)),
				Value::Edges(v) => i.ingest(Iterable::Edges(*v)),
				Value::Model(v) => {
					for v in v {
						i.ingest(Iterable::Thing(v));
					}
				}
				Value::Array(v) => {
					for v in v {
						match v {
							Value::Table(v) => i.ingest(Iterable::Table(v)),
							Value::Thing(v) => i.ingest(Iterable::Thing(v)),
							Value::Edges(v) => i.ingest(Iterable::Edges(*v)),
							Value::Model(v) => {
								for v in v {
									i.ingest(Iterable::Thing(v));
								}
							}
							Value::Object(v) => match v.rid() {
								Some(v) => i.ingest(Iterable::Thing(v)),
								None => {
									return Err(Error::UpdateStatement {
										value: v.to_string(),
									})
								}
							},
							v => {
								return Err(Error::UpdateStatement {
									value: v.to_string(),
								})
							}
						};
					}
				}
				Value::Object(v) => match v.rid() {
					Some(v) => i.ingest(Iterable::Thing(v)),
					None => {
						return Err(Error::UpdateStatement {
							value: v.to_string(),
						})
					}
				},
				v => {
					return Err(Error::UpdateStatement {
						value: v.to_string(),
					})
				}
			};
		}
		// Assign the statement
		let stm = Statement::from(self);
		// Output the results
		i.output(ctx, opt, txn, &stm).await
	}
}

impl fmt::Display for UpdateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "UPDATE {}", self.what)?;
		if let Some(ref v) = self.data {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.cond {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.output {
			write!(f, " {}", v)?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {}", v)?
		}
		if self.parallel {
			f.write_str(" PARALLEL")?
		}
		Ok(())
	}
}

pub fn update(i: &str) -> IResult<&str, UpdateStatement> {
	let (i, _) = tag_no_case("UPDATE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = whats(i)?;
	let (i, data) = opt(preceded(shouldbespace, data))(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
	Ok((
		i,
		UpdateStatement {
			what,
			data,
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
	fn update_statement() {
		let sql = "UPDATE test";
		let res = update(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("UPDATE test", format!("{}", out))
	}
}
