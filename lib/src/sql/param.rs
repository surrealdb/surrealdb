use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::error::IResult;
use crate::sql::idiom;
use crate::sql::idiom::Idiom;
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::value::Value;
use nom::character::complete::char;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Param {
	pub name: Idiom,
}

impl From<Idiom> for Param {
	fn from(p: Idiom) -> Param {
		Param {
			name: p,
		}
	}
}

impl Param {
	pub async fn compute(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&Value>,
	) -> Result<Value, Error> {
		// Find a base variable by name
		match self.name.parts.first() {
			// The first part will be a field
			Some(Part::Field(v)) => match ctx.value::<Value>(v.name.clone()) {
				// The base variable exists
				Some(v) => {
					// Get the path parts
					let pth: &[Part] = &self.name;
					// Process the paramater value
					let res = v.compute(ctx, opt, txn, doc).await?;
					// Return the desired field
					res.get(ctx, opt, txn, pth.next()).await
				}
				// The base variable does not exist
				None => Ok(Value::None),
			},
			_ => unreachable!(),
		}
	}
}

impl fmt::Display for Param {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "${}", &self.name)
	}
}

pub fn param(i: &str) -> IResult<&str, Param> {
	let (i, _) = char('$')(i)?;
	let (i, v) = idiom::param(i)?;
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
