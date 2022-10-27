use crate::sql::error::IResult;
use crate::sql::id::{id, Id};
use crate::sql::ident::ident_raw;
use nom::character::complete::char;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Range {
	pub tb: String,
	pub beg: Id,
	pub end: Id,
}

impl fmt::Display for Range {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}..{}", self.tb, self.beg, self.end)
	}
}

pub fn range(i: &str) -> IResult<&str, Range> {
	let (i, tb) = ident_raw(i)?;
	let (i, _) = char(':')(i)?;
	let (i, beg) = id(i)?;
	let (i, _) = char('.')(i)?;
	let (i, _) = char('.')(i)?;
	let (i, end) = id(i)?;
	Ok((
		i,
		Range {
			tb,
			beg,
			end,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn range_int() {
		let sql = "person:1..100";
		let res = range(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#"person:1..100"#, format!("{}", out));
	}

	#[test]
	fn range_array() {
		let sql = "person:['USA', 10]..['USA', 100]";
		let res = range(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("person:['USA', 10]..['USA', 100]", format!("{}", out));
	}

	#[test]
	fn range_object() {
		let sql = "person:{ country: 'USA', position: 10 }..{ country: 'USA', position: 100 }";
		let res = range(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(
			"person:{ country: 'USA', position: 10 }..{ country: 'USA', position: 100 }",
			format!("{}", out)
		);
	}
}
