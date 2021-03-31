use crate::dbs;
use crate::dbs::Executor;
use crate::dbs::Runtime;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::common::val_char;
use crate::sql::idiom::{idiom, Idiom};
use crate::sql::literal::Literal;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_while1;
use nom::IResult;
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

impl<'a> From<&'a str> for Param {
	fn from(p: &str) -> Param {
		Param {
			name: Idiom::from(p),
		}
	}
}

impl fmt::Display for Param {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "${}", &self.name)
	}
}

impl dbs::Process for Param {
	fn process(
		&self,
		ctx: &Runtime,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error> {
		// 1. Loop through the context variables
		// 2. Find a variable with the right name
		// 3. Process the variable value
		// 4. Return the processed value
		todo!()
	}
}

pub fn param(i: &str) -> IResult<&str, Param> {
	let (i, _) = tag("$")(i)?;
	let (i, v) = idiom(i)?;
	Ok((i, Param::from(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn param_normal() {
		let sql = "$test";
		let res = param(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("$test", format!("{}", out));
		assert_eq!(out, Param::from("test"));
	}

	#[test]
	fn param_longer() {
		let sql = "$test_and_deliver";
		let res = param(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("$test_and_deliver", format!("{}", out));
		assert_eq!(out, Param::from("test_and_deliver"));
	}

	#[test]
	fn param_embedded() {
		let sql = "$test.temporary[0].embedded";
		let res = param(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("$test.temporary[0].embedded", format!("{}", out));
		assert_eq!(out, Param::from("test.temporary[0].embedded"));
	}
}
