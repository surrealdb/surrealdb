use crate::sql::error::IResult;
use nom::bytes::complete::escaped;
use nom::bytes::complete::is_not;
use nom::bytes::complete::tag;
use nom::character::complete::one_of;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::str;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Regex {
	pub input: String,
	#[serde(skip)]
	pub value: Option<regex::Regex>,
}

impl<'a> From<&'a str> for Regex {
	fn from(r: &str) -> Regex {
		let r = r.replace("\\/", "/");
		let r = r.as_str();
		Regex {
			input: String::from(r),
			value: match regex::Regex::new(r) {
				Ok(v) => Some(v),
				Err(_) => None,
			},
		}
	}
}

impl PartialEq for Regex {
	fn eq(&self, other: &Self) -> bool {
		self.input == other.input
	}
}

impl PartialOrd for Regex {
	#[inline]
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.input.cmp(&other.input))
	}
}

impl fmt::Display for Regex {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "/{}/", &self.input)
	}
}

pub fn regex(i: &str) -> IResult<&str, Regex> {
	let (i, _) = tag("/")(i)?;
	let (i, v) = escaped(is_not("\\/"), '\\', one_of("/"))(i)?;
	let (i, _) = tag("/")(i)?;
	Ok((i, Regex::from(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn regex_simple() {
		let sql = "/test/";
		let res = regex(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("/test/", format!("{}", out));
		assert_eq!(out, Regex::from("test"));
	}

	#[test]
	fn regex_complex() {
		let sql = r"/test\/[a-z]+\/.*/";
		let res = regex(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r"/test/[a-z]+/.*/", format!("{}", out));
		assert_eq!(out, Regex::from("test/[a-z]+/.*"));
	}
}
