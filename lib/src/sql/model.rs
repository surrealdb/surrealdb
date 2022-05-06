use crate::sql::common::escape;
use crate::sql::common::take_u64;
use crate::sql::common::val_char;
use crate::sql::error::IResult;
use crate::sql::ident::ident_raw;
use nom::branch::alt;
use nom::character::complete::char;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Model {
	Count(String, u64),
	Range(String, u64, u64),
}

impl fmt::Display for Model {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Model::Count(tb, c) => {
				let t = escape(tb, &val_char, "`");
				write!(f, "|{}:{}|", t, c)
			}
			Model::Range(tb, b, e) => {
				let t = escape(tb, &val_char, "`");
				write!(f, "|{}:{}..{}|", t, b, e)
			}
		}
	}
}

pub fn model(i: &str) -> IResult<&str, Model> {
	alt((model_count, model_range))(i)
}

fn model_count(i: &str) -> IResult<&str, Model> {
	let (i, _) = char('|')(i)?;
	let (i, t) = ident_raw(i)?;
	let (i, _) = char(':')(i)?;
	let (i, c) = take_u64(i)?;
	let (i, _) = char('|')(i)?;
	Ok((i, Model::Count(t, c)))
}

fn model_range(i: &str) -> IResult<&str, Model> {
	let (i, _) = char('|')(i)?;
	let (i, t) = ident_raw(i)?;
	let (i, _) = char(':')(i)?;
	let (i, b) = take_u64(i)?;
	let (i, _) = char('.')(i)?;
	let (i, _) = char('.')(i)?;
	let (i, e) = take_u64(i)?;
	let (i, _) = char('|')(i)?;
	Ok((i, Model::Range(t, b, e)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn model_count() {
		let sql = "|test:1000|";
		let res = model(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("|test:1000|", format!("{}", out));
		assert_eq!(out, Model::Count(String::from("test"), 1000));
	}

	#[test]
	fn model_range() {
		let sql = "|test:1..1000|";
		let res = model(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("|test:1..1000|", format!("{}", out));
		assert_eq!(out, Model::Range(String::from("test"), 1, 1000));
	}
}
