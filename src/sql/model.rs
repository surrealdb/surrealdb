use crate::sql::common::escape;
use crate::sql::common::take_u64;
use crate::sql::common::val_char;
use crate::sql::ident::ident_raw;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Model {
	pub table: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub range: Option<(u64, u64)>,
}

impl fmt::Display for Model {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "|{}:", escape(&self.table, &val_char, "`"))?;
		if let Some(ref v) = self.count {
			write!(f, "{}", v)?
		}
		if let Some(ref v) = self.range {
			write!(f, "{}..{}", v.0, v.1)?
		}
		write!(f, "|")?;
		Ok(())
	}
}

pub fn model(i: &str) -> IResult<&str, Model> {
	alt((model_count, model_range))(i)
}

fn model_count(i: &str) -> IResult<&str, Model> {
	let (i, _) = tag("|")(i)?;
	let (i, t) = ident_raw(i)?;
	let (i, _) = tag(":")(i)?;
	let (i, c) = take_u64(i)?;
	let (i, _) = tag("|")(i)?;
	Ok((
		i,
		Model {
			table: String::from(t),
			count: Some(c),
			range: None,
		},
	))
}

fn model_range(i: &str) -> IResult<&str, Model> {
	let (i, _) = tag("|")(i)?;
	let (i, t) = ident_raw(i)?;
	let (i, _) = tag(":")(i)?;
	let (i, b) = take_u64(i)?;
	let (i, _) = tag("..")(i)?;
	let (i, e) = take_u64(i)?;
	let (i, _) = tag("|")(i)?;
	Ok((
		i,
		Model {
			table: String::from(t),
			count: None,
			range: Some((b, e)),
		},
	))
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
		assert_eq!(
			out,
			Model {
				table: String::from("test"),
				count: Some(1000),
				range: None,
			}
		);
	}

	#[test]
	fn model_range() {
		let sql = "|test:1..1000|";
		let res = model(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("|test:1..1000|", format!("{}", out));
		assert_eq!(
			out,
			Model {
				table: String::from("test"),
				count: None,
				range: Some((1, 1000)),
			}
		);
	}
}
