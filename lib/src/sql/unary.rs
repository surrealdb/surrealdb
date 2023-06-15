use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::fnc;
use crate::sql::comment::mightbespace;
use crate::sql::error::IResult;
use crate::sql::operator::{self, Operator};
use crate::sql::value::{single, Value};
use async_recursion::async_recursion;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Unary";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Unary")]
pub struct Unary(pub Operator, pub Value);

impl PartialOrd for Unary {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl Unary {
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&'async_recursion Value>,
	) -> Result<Value, Error> {
		// Prevent long unary chains
		let opt = &opt.dive(1)?;
		// Compute the operand
		let operand = self.1.compute(ctx, opt, txn, doc).await?;
		match &self.0 {
			Operator::Neg => fnc::operate::neg(operand),
			Operator::Not => fnc::operate::not(operand),
			op => unreachable!("{op:?} is not a unary op"),
		}
	}
}

impl fmt::Display for Unary {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}{}", self.0, self.1)
	}
}

pub fn unary(i: &str) -> IResult<&str, Unary> {
	let (i, op) = operator::unary(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = single(i)?;
	Ok((i, Unary(op, v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn neg_var() {
		let sql = "-a";
		let res = unary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
		assert_eq!(out, Unary(Operator::Neg, Value::Strand("a".into()).to_idiom().into()));
	}

	#[test]
	fn not_var() {
		let sql = "!a";
		let res = unary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
		assert_eq!(out, Unary(Operator::Not, Value::Strand("a".into()).to_idiom().into()));
	}
}
