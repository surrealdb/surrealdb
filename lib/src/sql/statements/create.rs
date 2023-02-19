use crate::ctx::Context;
use crate::dbs::Iterable;
use crate::dbs::Iterator;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct CreateStatement {
	pub what: Values,
	pub data: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl CreateStatement {
	pub(crate) fn writeable(&self) -> bool {
		true
	}

	pub(crate) fn single(&self) -> bool {
		match self.what.len() {
			1 if self.what[0].is_object() => true,
			1 if self.what[0].is_thing() => true,
			1 if self.what[0].is_table() => true,
			_ => false,
		}
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
		// Loop over the create targets
		for w in self.what.0.iter() {
			let v = w.compute(ctx, opt, txn, doc).await?;
			match v {
				Value::Table(v) => match &self.data {
					// There is a data clause so check for a record id
					Some(data) => match data.rid(ctx, opt, txn, &v).await {
						// There was a problem creating the record id
						Err(e) => return Err(e),
						// There is an id field so use the record id
						Ok(v) => i.ingest(Iterable::Thing(v)),
					},
					// There is no data clause so create a record id
					None => i.ingest(Iterable::Thing(v.generate())),
				},
				Value::Thing(v) => i.ingest(Iterable::Thing(v)),
				Value::Model(v) => {
					for v in v {
						i.ingest(Iterable::Thing(v));
					}
				}
				Value::Array(v) => {
					for v in v {
						match v {
							Value::Table(v) => i.ingest(Iterable::Thing(v.generate())),
							Value::Thing(v) => i.ingest(Iterable::Thing(v)),
							Value::Model(v) => {
								for v in v {
									i.ingest(Iterable::Thing(v));
								}
							}
							Value::Object(v) => match v.rid() {
								Some(v) => i.ingest(Iterable::Thing(v)),
								None => {
									return Err(Error::CreateStatement {
										value: v.to_string(),
									})
								}
							},
							v => {
								return Err(Error::CreateStatement {
									value: v.to_string(),
								})
							}
						};
					}
				}
				Value::Object(v) => match v.rid() {
					Some(v) => i.ingest(Iterable::Thing(v)),
					None => {
						return Err(Error::CreateStatement {
							value: v.to_string(),
						})
					}
				},
				v => {
					return Err(Error::CreateStatement {
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

impl fmt::Display for CreateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "CREATE {}", self.what)?;
		if let Some(ref v) = self.data {
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

pub fn create(i: &str) -> IResult<&str, CreateStatement> {
	let (i, _) = tag_no_case("CREATE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = whats(i)?;
	let (i, data) = opt(preceded(shouldbespace, data))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
	Ok((
		i,
		CreateStatement {
			what,
			data,
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
	fn create_statement() {
		let sql = "CREATE test";
		let res = create(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("CREATE test", format!("{}", out))
	}
}
