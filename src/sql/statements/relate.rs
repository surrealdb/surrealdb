use crate::sql::comment::mightbespace;
use crate::sql::comment::shouldbespace;
use crate::sql::data::{data, Data};
use crate::sql::output::{output, Output};
use crate::sql::table::{table, Table};
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::what::{whats, Whats};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use nom::sequence::tuple;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RelateStatement {
	pub kind: Table,
	pub from: Whats,
	pub with: Whats,
	pub uniq: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub data: Option<Data>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub output: Option<Output>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub timeout: Option<Timeout>,
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
	let (i, uniq) = opt(tuple((shouldbespace, tag_no_case("UNIQUE"))))(i)?;
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

fn relate_o(i: &str) -> IResult<&str, (Table, Whats, Whats)> {
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

fn relate_i(i: &str) -> IResult<&str, (Table, Whats, Whats)> {
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
}
