use std::ops::Bound;

use super::{
	super::{thing::id, IResult},
	ident_raw,
};
use crate::sql::Range;
use nom::{
	branch::alt,
	character::complete::char,
	combinator::{map, opt},
	sequence::{preceded, terminated},
};

pub fn range(i: &str) -> IResult<&str, Range> {
	let (i, tb) = ident_raw(i)?;
	let (i, _) = char(':')(i)?;
	let (i, beg) =
		opt(alt((map(terminated(id, char('>')), Bound::Excluded), map(id, Bound::Included))))(i)?;
	let (i, _) = char('.')(i)?;
	let (i, _) = char('.')(i)?;
	let (i, end) =
		opt(alt((map(preceded(char('='), id), Bound::Included), map(id, Bound::Excluded))))(i)?;
	Ok((
		i,
		Range {
			tb,
			beg: beg.unwrap_or(Bound::Unbounded),
			end: end.unwrap_or(Bound::Unbounded),
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
		let out = res.unwrap().1;
		assert_eq!(r#"person:1..100"#, format!("{}", out));
	}

	#[test]
	fn range_array() {
		let sql = "person:['USA', 10]..['USA', 100]";
		let res = range(sql);
		let out = res.unwrap().1;
		assert_eq!("person:['USA', 10]..['USA', 100]", format!("{}", out));
	}

	#[test]
	fn range_object() {
		let sql = "person:{ country: 'USA', position: 10 }..{ country: 'USA', position: 100 }";
		let res = range(sql);
		let out = res.unwrap().1;
		assert_eq!(
			"person:{ country: 'USA', position: 10 }..{ country: 'USA', position: 100 }",
			format!("{}", out)
		);
	}
}
