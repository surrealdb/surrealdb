use crate::dbs::Executor;
use crate::dbs::Iterator;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::data::{data, Data};
use crate::sql::error::IResult;
use crate::sql::output::{output, Output};
use crate::sql::table::{table, Table};
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::value::{whats, Value, Values};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelateStatement {
	pub kind: Table,
	pub from: Values,
	pub with: Values,
	pub uniq: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub data: Option<Data>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub output: Option<Output>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub timeout: Option<Timeout>,
}

impl RelateStatement {
	pub async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options<'_>,
		exe: &mut Executor,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		exe.check(opt, Level::No)?;
		// Create a new iterator
		let mut i = Iterator::new();
		// Ensure futures are stored
		let opt = &opt.futures(false);
		// Loop over the select targets
		for f in self.from.0.iter() {
			match f.compute(ctx, opt, exe, doc).await? {
				Value::Table(v) => {
					i.process_table(ctx, exe, v);
				}
				Value::Thing(v) => {
					i.process_thing(ctx, exe, v);
				}
				Value::Model(v) => {
					i.process_model(ctx, exe, v);
				}
				Value::Array(v) => {
					i.process_array(ctx, exe, v);
				}
				Value::Object(v) => {
					i.process_object(ctx, exe, v);
				}
				v => {
					return Err(Error::RelateStatementError {
						value: v,
					})
				}
			};
		}
		// Output the results
		i.output(ctx, exe)
	}
}

impl fmt::Display for RelateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RELATE {} -> {} -> {}", self.from, self.kind, self.with)?;
		if self.uniq {
			write!(f, " UNIQUE")?
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
		},
	))
}

fn relate_o(i: &str) -> IResult<&str, (Table, Values, Values)> {
	let (i, from) = whats(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("->")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, kind) = table(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("->")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, with) = whats(i)?;
	Ok((i, (kind, from, with)))
}

fn relate_i(i: &str) -> IResult<&str, (Table, Values, Values)> {
	let (i, with) = whats(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("<-")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, kind) = table(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = tag("<-")(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, from) = whats(i)?;
	Ok((i, (kind, from, with)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn relate_statement_in() {
		let sql = "RELATE person->like->animal";
		let res = relate(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("RELATE person -> like -> animal", format!("{}", out))
	}

	#[test]
	fn relate_statement_out() {
		let sql = "RELATE animal<-like<-person";
		let res = relate(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("RELATE person -> like -> animal", format!("{}", out))
	}

	#[test]
	fn relate_statement_thing() {
		let sql = "RELATE person:tobie->like->person:jaime";
		let res = relate(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("RELATE person:tobie -> like -> person:jaime", format!("{}", out))
	}
}
