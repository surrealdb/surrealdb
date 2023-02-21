use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::block::{block, Block};
use crate::sql::comment::mightbespace;
use crate::sql::error::IResult;
use crate::sql::value::Value;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Future(pub Block);

impl From<Value> for Future {
	fn from(v: Value) -> Self {
		Future(Block::from(v))
	}
}

impl Future {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Prevent long future chains
		let opt = &opt.dive(1)?;
		// Process the future if enabled
		match opt.futures {
			true => self.0.compute(ctx, opt, txn, doc).await?.ok(),
			false => Ok(self.clone().into()),
		}
	}
}

impl fmt::Display for Future {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<future> {}", self.0)
	}
}

pub fn future(i: &str) -> IResult<&str, Future> {
	let (i, _) = char('<')(i)?;
	let (i, _) = tag("future")(i)?;
	let (i, _) = char('>')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = block(i)?;
	Ok((i, Future(v)))
}
#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::expression::Expression;
	use crate::sql::test::Parse;

	#[test]
	fn future_expression() {
		let sql = "<future> { 1.2345 + 5.4321 }";
		let res = future(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<future> { 1.2345 + 5.4321 }", format!("{}", out));
		assert_eq!(out, Future(Block::from(Value::from(Expression::parse("1.2345 + 5.4321")))));
	}
}
