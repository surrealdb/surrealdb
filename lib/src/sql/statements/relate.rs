use crate::ctx::Context;
use crate::dbs::Iterable;
use crate::dbs::Iterator;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::array::array;
use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::data::{data, Data};
use crate::sql::error::IResult;
use crate::sql::output::{output, Output};
use crate::sql::param::plain as param;
use crate::sql::subquery::subquery;
use crate::sql::table::{table, Table};
use crate::sql::thing::thing;
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::map;
use nom::combinator::opt;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct RelateStatement {
	pub kind: Table,
	pub from: Value,
	pub with: Value,
	pub uniq: bool,
	pub data: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl RelateStatement {
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
		// Loop over the from targets
		let from = {
			let mut out = Vec::new();
			match self.from.compute(ctx, opt, txn, doc).await? {
				Value::Thing(v) => out.push(v),
				Value::Array(v) => {
					for v in v {
						match v {
							Value::Thing(v) => out.push(v),
							Value::Object(v) => match v.rid() {
								Some(v) => out.push(v),
								_ => {
									return Err(Error::RelateStatement {
										value: v.to_string(),
									})
								}
							},
							v => {
								return Err(Error::RelateStatement {
									value: v.to_string(),
								})
							}
						}
					}
				}
				Value::Object(v) => match v.rid() {
					Some(v) => out.push(v),
					None => {
						return Err(Error::RelateStatement {
							value: v.to_string(),
						})
					}
				},
				v => {
					return Err(Error::RelateStatement {
						value: v.to_string(),
					})
				}
			};
			// }
			out
		};
		// Loop over the with targets
		let with = {
			let mut out = Vec::new();
			match self.with.compute(ctx, opt, txn, doc).await? {
				Value::Thing(v) => out.push(v),
				Value::Array(v) => {
					for v in v {
						match v {
							Value::Thing(v) => out.push(v),
							Value::Object(v) => match v.rid() {
								Some(v) => out.push(v),
								None => {
									return Err(Error::RelateStatement {
										value: v.to_string(),
									})
								}
							},
							v => {
								return Err(Error::RelateStatement {
									value: v.to_string(),
								})
							}
						}
					}
				}
				Value::Object(v) => match v.rid() {
					Some(v) => out.push(v),
					None => {
						return Err(Error::RelateStatement {
							value: v.to_string(),
						})
					}
				},
				v => {
					return Err(Error::RelateStatement {
						value: v.to_string(),
					})
				}
			};
			out
		};
		//
		for f in from.iter() {
			for w in with.iter() {
				let f = f.clone();
				let w = w.clone();
				match &self.data {
					// There is a data clause so check for a record id
					Some(data) => match data.rid(ctx, opt, txn, &self.kind).await {
						// There was a problem creating the record id
						Err(e) => return Err(e),
						// There is an id field so use the record id
						Ok(t) => i.ingest(Iterable::Relatable(f, t, w)),
					},
					// There is no data clause so create a record id
					None => i.ingest(Iterable::Relatable(f, self.kind.generate(), w)),
				};
			}
		}
		// Assign the statement
		let stm = Statement::from(self);
		// Output the results
		i.output(ctx, opt, txn, &stm).await
	}
}

impl fmt::Display for RelateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RELATE {} -> {} -> {}", self.from, self.kind, self.with)?;
		if self.uniq {
			f.write_str(" UNIQUE")?
		}
		if let Some(ref v) = self.data {
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

pub fn relate(i: &str) -> IResult<&str, RelateStatement> {
	let (i, _) = tag_no_case("RELATE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, path) = alt((relate_o, relate_i))(i)?;
	let (i, uniq) = opt(preceded(shouldbespace, tag_no_case("UNIQUE")))(i)?;
	let (i, data) = opt(preceded(shouldbespace, data))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	let (i, parallel) = opt(preceded(shouldbespace, tag_no_case("PARALLEL")))(i)?;
	Ok((
		i,
		RelateStatement {
			kind: path.0,
			from: path.1,
			with: path.2,
			uniq: uniq.is_some(),
			data,
			output,
			timeout,
			parallel: parallel.is_some(),
		},
	))
}

fn relate_o(i: &str) -> IResult<&str, (Table, Value, Value)> {
	let (i, from) = alt((
		map(subquery, Value::from),
		map(array, Value::from),
		map(param, Value::from),
		map(thing, Value::from),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('-')(i)?;
	let (i, _) = char('>')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, kind) = table(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('-')(i)?;
	let (i, _) = char('>')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, with) = alt((
		map(subquery, Value::from),
		map(array, Value::from),
		map(param, Value::from),
		map(thing, Value::from),
	))(i)?;
	Ok((i, (kind, from, with)))
}

fn relate_i(i: &str) -> IResult<&str, (Table, Value, Value)> {
	let (i, with) = alt((
		map(subquery, Value::from),
		map(array, Value::from),
		map(param, Value::from),
		map(thing, Value::from),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('<')(i)?;
	let (i, _) = char('-')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, kind) = table(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('<')(i)?;
	let (i, _) = char('-')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, from) = alt((
		map(subquery, Value::from),
		map(array, Value::from),
		map(param, Value::from),
		map(thing, Value::from),
	))(i)?;
	Ok((i, (kind, from, with)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn relate_statement_in() {
		let sql = "RELATE animal:koala<-like<-person:tobie";
		let res = relate(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("RELATE person:tobie -> like -> animal:koala", format!("{}", out))
	}

	#[test]
	fn relate_statement_out() {
		let sql = "RELATE person:tobie->like->animal:koala";
		let res = relate(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("RELATE person:tobie -> like -> animal:koala", format!("{}", out))
	}

	#[test]
	fn relate_statement_params() {
		let sql = "RELATE $tobie->like->$koala";
		let res = relate(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("RELATE $tobie -> like -> $koala", format!("{}", out))
	}
}
