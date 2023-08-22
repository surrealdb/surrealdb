use crate::ctx::Context;
use crate::dbs::Iterator;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::{Iterable, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::array::array;
use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::data::{data, Data};
use crate::sql::error::IResult;
use crate::sql::output::{output, Output};
use crate::sql::param::param;
use crate::sql::subquery::subquery;
use crate::sql::table::table;
use crate::sql::thing::thing;
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::cut;
use nom::combinator::map;
use nom::combinator::opt;
use nom::sequence::preceded;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct RelateStatement {
	pub kind: Value,
	pub from: Value,
	pub with: Value,
	pub uniq: bool,
	pub data: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl RelateStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
	/// Check if this statement is for a single record
	pub(crate) fn single(&self) -> bool {
		match (&self.from, &self.with) {
			(v, w) if v.is_object() && w.is_object() => true,
			(v, w) if v.is_object() && w.is_thing() => true,
			(v, w) if v.is_thing() && w.is_object() => true,
			(v, w) if v.is_thing() && w.is_thing() => true,
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
		// Ensure futures are stored
		let opt = &opt.new_with_futures(false);
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
				match &self.kind {
					// The relation has a specific record id
					Value::Thing(id) => i.ingest(Iterable::Relatable(f, id.to_owned(), w)),
					// The relation does not have a specific record id
					Value::Table(tb) => match &self.data {
						// There is a data clause so check for a record id
						Some(data) => {
							let id = match data.rid(ctx, opt, txn).await? {
								Some(id) => id.generate(tb, false)?,
								None => tb.generate(),
							};
							i.ingest(Iterable::Relatable(f, id, w))
						}
						// There is no data clause so create a record id
						None => i.ingest(Iterable::Relatable(f, tb.generate(), w)),
					},
					// The relation can not be any other type
					_ => unreachable!(),
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

pub fn relate(i: &str) -> IResult<&str, RelateStatement> {
	let (i, _) = tag_no_case("RELATE")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, path) = relate_oi(i)?;
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
	})(i)
}

fn relate_oi(i: &str) -> IResult<&str, (Value, Value, Value)> {
	let (i, prefix) = alt((
		map(subquery, Value::from),
		map(array, Value::from),
		map(param, Value::from),
		map(thing, Value::from),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, is_o) = alt((map(tag("->"), |_| true), map(tag("<-"), |_| false)))(i)?;

	if is_o {
		let (i, (kind, with)) = relate_o(i)?;
		Ok((i, (kind, prefix, with)))
	} else {
		let (i, (kind, from)) = relate_i(i)?;
		Ok((i, (kind, from, prefix)))
	}
}

fn relate_o(i: &str) -> IResult<&str, (Value, Value)> {
	let (i, _) = mightbespace(i)?;
	let (i, kind) = alt((map(thing, Value::from), map(table, Value::from)))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("->")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, with) = alt((
		map(subquery, Value::from),
		map(array, Value::from),
		map(param, Value::from),
		map(thing, Value::from),
	))(i)?;
	Ok((i, (kind, with)))
}

fn relate_i(i: &str) -> IResult<&str, (Value, Value)> {
	let (i, _) = mightbespace(i)?;
	let (i, kind) = alt((map(thing, Value::from), map(table, Value::from)))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("<-")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, from) = alt((
		map(subquery, Value::from),
		map(array, Value::from),
		map(param, Value::from),
		map(thing, Value::from),
	))(i)?;
	Ok((i, (kind, from)))
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
