use super::super::{IResult, ParseError};
use crate::sql::Regex;
use nom::{
	bytes::complete::{escaped, is_not},
	character::complete::{anychar, char},
};

pub fn regex(i: &str) -> IResult<&str, Regex> {
	let (i, _) = char('/')(i)?;
	let (i, v) = escaped(is_not("\\/"), '\\', anychar)(i)?;
	let (i, _) = char('/')(i)?;
	let regex = v.parse().map_err(|_| nom::Err::Error(ParseError::Base(v)))?;
	Ok((i, regex))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn regex_simple() {
		let sql = "/test/";
		let res = regex(sql);
		let out = res.unwrap().1;
		assert_eq!("/test/", format!("{}", out));
		assert_eq!(out, "test".parse().unwrap());
	}

	#[test]
	fn regex_complex() {
		let sql = r"/(?i)test\/[a-z]+\/\s\d\w{1}.*/";
		let res = regex(sql);
		let out = res.unwrap().1;
		assert_eq!(r"/(?i)test/[a-z]+/\s\d\w{1}.*/", format!("{}", out));
		assert_eq!(out, r"(?i)test/[a-z]+/\s\d\w{1}.*".parse().unwrap());
	}
}
