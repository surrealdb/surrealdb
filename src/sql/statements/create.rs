use crate::dbs::Executor;
use crate::dbs::Iterator;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::data::{data, Data};
use crate::sql::error::IResult;
use crate::sql::output::{output, Output};
use crate::sql::timeout::{timeout, Timeout};
use crate::sql::value::{whats, Value, Values};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateStatement {
	pub what: Values,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub data: Option<Data>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub output: Option<Output>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub timeout: Option<Timeout>,
}

impl CreateStatement {
	pub async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options<'_>,
		exe: &Executor<'_>,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Allowed to run?
		exe.check(opt, Level::No)?;
		// Create a new iterator
		let mut i = Iterator::new();
		// Pass in statement config
		i.data = self.data.as_ref();
		// Ensure futures are stored
		let opt = &opt.futures(false);
		// Loop over the create targets
		for w in self.what.0.iter() {
			match w.compute(ctx, opt, exe, doc).await? {
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
					return Err(Error::CreateStatementError {
						value: v,
					})
				}
			};
		}
		// Output the results
		i.output(ctx, exe)
	}
}

impl fmt::Display for CreateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "CREATE {}", self.what)?;
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

pub fn create(i: &str) -> IResult<&str, CreateStatement> {
	let (i, _) = tag_no_case("CREATE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = whats(i)?;
	let (i, data) = opt(preceded(shouldbespace, data))(i)?;
	let (i, output) = opt(preceded(shouldbespace, output))(i)?;
	let (i, timeout) = opt(preceded(shouldbespace, timeout))(i)?;
	Ok((
		i,
		CreateStatement {
			what,
			data,
			output,
			timeout,
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
