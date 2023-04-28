use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::value::Value;
use nom::character::complete::char;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Param";

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Param")]
pub struct Param(pub Ident);

impl From<Ident> for Param {
	fn from(v: Ident) -> Self {
		Self(v)
	}
}

impl From<String> for Param {
	fn from(v: String) -> Self {
		Self(v.into())
	}
}

impl From<&str> for Param {
	fn from(v: &str) -> Self {
		Self(v.into())
	}
}

impl Deref for Param {
	type Target = Ident;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Param {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Find the variable by name
		match self.as_str() {
			// This is a special param
			"this" | "self" => match doc {
				// The base document exists
				Some(v) => v.compute(ctx, opt, txn, doc).await,
				// The base document does not exist
				None => Ok(Value::None),
			},
			// This is a normal param
			v => match ctx.value(v) {
				// The param has been set locally
				Some(v) => v.compute(ctx, opt, txn, doc).await,
				// The param has not been set locally
				None => {
					// Clone transaction
					let run = txn.clone();
					// Claim transaction
					let mut run = run.lock().await;
					// Get the param definition
					let val = run.get_pa(opt.ns(), opt.db(), v).await;
					// Check if the param has been set globally
					match val {
						// The param has been set globally
						Ok(v) => Ok(v.value),
						// The param has not been set globally
						Err(_) => Ok(Value::None),
					}
				}
			},
		}
	}
}

impl fmt::Display for Param {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "${}", &self.0)
	}
}

pub fn param(i: &str) -> IResult<&str, Param> {
	let (i, _) = char('$')(i)?;
	let (i, v) = ident(i)?;
	Ok((i, Param::from(v)))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::test::Parse;

	#[test]
	fn param_normal() {
		let sql = "$test";
		let res = param(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("$test", format!("{}", out));
		assert_eq!(out, Param::parse("$test"));
	}

	#[test]
	fn param_longer() {
		let sql = "$test_and_deliver";
		let res = param(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("$test_and_deliver", format!("{}", out));
		assert_eq!(out, Param::parse("$test_and_deliver"));
	}
}
