use super::super::{literal::ident_raw, IResult};
use crate::sql::Mock;
use nom::{
	branch::alt,
	character::complete::{char, u64},
	combinator::{map, value},
};

pub fn mock(i: &str) -> IResult<&str, Mock> {
	let (i, _) = char('|')(i)?;
	let (i, t) = ident_raw(i)?;
	let (i, _) = char(':')(i)?;
	let (i, c) = u64(i)?;
	let (i, e) = alt((value(None, char('|')), map(mock_range, Some)))(i)?;
	if let Some(e) = e {
		Ok((i, Mock::Range(t, c, e)))
	} else {
		Ok((i, Mock::Count(t, c)))
	}
}

fn mock_range(i: &str) -> IResult<&str, u64> {
	let (i, _) = char('.')(i)?;
	let (i, _) = char('.')(i)?;
	let (i, e) = u64(i)?;
	let (i, _) = char('|')(i)?;
	Ok((i, e))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn mock_count() {
		let sql = "|test:1000|";
		let res = mock(sql);
		let out = res.unwrap().1;
		assert_eq!("|test:1000|", format!("{}", out));
		assert_eq!(out, Mock::Count(String::from("test"), 1000));
	}

	#[test]
	fn mock_range() {
		let sql = "|test:1..1000|";
		let res = mock(sql);
		let out = res.unwrap().1;
		assert_eq!("|test:1..1000|", format!("{}", out));
		assert_eq!(out, Mock::Range(String::from("test"), 1, 1000));
	}
}
