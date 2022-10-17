use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::fetch::{fetch, Fetchs};
use crate::sql::value::{value, Value};
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store)]
pub struct OutputStatement {
	pub what: Value,
	pub fetch: Option<Fetchs>,
}

impl OutputStatement {
	pub(crate) fn writeable(&self) -> bool {
		self.what.writeable()
	}

	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Ensure futures are processed
		let opt = &opt.futures(true);
		// Process the output value
		let mut val = self.what.compute(ctx, opt, txn, doc).await?;
		// Fetch any
		if let Some(fetchs) = &self.fetch {
			for fetch in fetchs.iter() {
				val.fetch(ctx, opt, txn, fetch).await?;
			}
		}
		//
		Ok(val)
	}
}

impl fmt::Display for OutputStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RETURN {}", self.what)?;
		if let Some(ref v) = self.fetch {
			write!(f, " {}", v)?
		}
		Ok(())
	}
}

pub fn output(i: &str) -> IResult<&str, OutputStatement> {
	let (i, _) = tag_no_case("RETURN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = value(i)?;
	let (i, fetch) = opt(preceded(shouldbespace, fetch))(i)?;
	Ok((
		i,
		OutputStatement {
			what,
			fetch,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn output_statement() {
		let sql = "RETURN $param";
		let res = output(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("RETURN $param", format!("{}", out));
	}
}
