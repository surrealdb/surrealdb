use crate::ctx::Parent;
use crate::dbs;
use crate::dbs::Executor;
use crate::dbs::Iterator;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::literal::{whats, Literal, Literals};
use crate::sql::output::{output, Output};
use crate::sql::timeout::{timeout, Timeout};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use nom::sequence::tuple;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeleteStatement {
	pub what: Literals,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub cond: Option<Cond>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub output: Option<Output>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub timeout: Option<Timeout>,
}

impl fmt::Display for DeleteStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DELETE {}", self.what)?;
		if let Some(ref v) = self.cond {
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

impl dbs::Process for DeleteStatement {
	fn process(
		&self,
		ctx: &Parent,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error> {
		// Create a new iterator
		let i = Iterator::new();
		// Loop over the select targets
		for w in self.what.to_owned() {
			match w.process(ctx, exe, doc)? {
				Literal::Table(_) => {
					i.process_table(ctx, exe);
				}
				Literal::Thing(_) => {
					i.process_thing(ctx, exe);
				}
				Literal::Model(_) => {
					i.process_model(ctx, exe);
				}
				Literal::Array(_) => {
					i.process_array(ctx, exe);
				}
				Literal::Object(_) => {
					i.process_object(ctx, exe);
				}
				_ => {
					todo!() // Return error
				}
			};
		}
		// Output the results
		i.output(ctx, exe)
	}
}

pub fn delete(i: &str) -> IResult<&str, DeleteStatement> {
	let (i, _) = tag_no_case("DELETE")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("FROM"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = whats(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	Ok((
		i,
		DeleteStatement {
			what,
			cond,
			output,
			timeout,
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
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("DELETE test", format!("{}", out))
	}
}
