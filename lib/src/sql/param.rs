use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::error::IResult;
use crate::sql::idiom;
use crate::sql::idiom::Idiom;
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::serde::is_internal_serialization;
use crate::sql::value::Value;
use nom::character::complete::char;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Param";

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize, Hash)]
pub struct Param(pub Idiom);

impl From<Idiom> for Param {
	fn from(p: Idiom) -> Self {
		Self(p)
	}
}

impl Deref for Param {
	type Target = Idiom;
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
		// Find a base variable by name
		match self.first() {
			// The first part will be a field
			Some(Part::Field(v)) => match v.as_str() {
				// This is a special param
				"this" | "self" => match doc {
					// The base document exists
					Some(v) => {
						// Get the path parts
						let pth: &[Part] = self;
						// Process the parameter value
						let res = v.compute(ctx, opt, txn, doc).await?;
						// Return the desired field
						res.get(ctx, opt, txn, pth.next()).await
					}
					// The base document does not exist
					None => Ok(Value::None),
				},
				// This is a normal param
				_ => match ctx.value(v) {
					// The param has been set locally
					Some(v) => {
						// Get the path parts
						let pth: &[Part] = self;
						// Process the parameter value
						let res = v.compute(ctx, opt, txn, doc).await?;
						// Return the desired field
						res.get(ctx, opt, txn, pth.next()).await
					}
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
			},
			_ => unreachable!(),
		}
	}
}

impl fmt::Display for Param {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "${}", &self.0)
	}
}

impl Serialize for Param {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			serializer.serialize_newtype_struct(TOKEN, &self.0)
		} else {
			serializer.serialize_none()
		}
	}
}

pub fn param(i: &str) -> IResult<&str, Param> {
	let (i, _) = char('$')(i)?;
	let (i, v) = idiom::param(i)?;
	Ok((i, Param::from(v)))
}

pub fn plain(i: &str) -> IResult<&str, Param> {
	let (i, _) = char('$')(i)?;
	let (i, v) = idiom::plain(i)?;
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

	#[test]
	fn param_embedded() {
		let sql = "$test.temporary[0].embedded";
		let res = param(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("$test.temporary[0].embedded", format!("{}", out));
		assert_eq!(out, Param::parse("$test.temporary[0].embedded"));
	}
}
