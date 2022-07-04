use crate::sql::error::IResult;
use crate::sql::escape::escape_strand;
use crate::sql::serde::is_internal_serialization;
use nom::branch::alt;
use nom::bytes::complete::escaped;
use nom::bytes::complete::is_not;
use nom::bytes::complete::tag;
use nom::character::complete::one_of;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops;
use std::ops::Deref;
use std::str;

const SINGLE: &str = r#"'"#;
const SINGLE_ESC: &str = r#"\'"#;

const DOUBLE: &str = r#"""#;
const DOUBLE_ESC: &str = r#"\""#;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize)]
pub struct Strand(pub String);

impl From<String> for Strand {
	fn from(s: String) -> Self {
		Strand(s)
	}
}

impl From<&str> for Strand {
	fn from(s: &str) -> Self {
		Strand(String::from(s))
	}
}

impl Deref for Strand {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Strand {
	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}
	pub fn as_string(self) -> String {
		self.0
	}
	pub fn to_raw(self) -> String {
		self.0
	}
}

impl fmt::Display for Strand {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", escape_strand(&self.0))
	}
}

impl Serialize for Strand {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			serializer.serialize_newtype_struct("Strand", &self.0)
		} else {
			serializer.serialize_some(&self.0)
		}
	}
}

impl ops::Add for Strand {
	type Output = Self;
	fn add(self, other: Self) -> Self {
		Strand::from(self.0 + &other.0)
	}
}

pub fn strand(i: &str) -> IResult<&str, Strand> {
	let (i, v) = strand_raw(i)?;
	Ok((i, Strand(v)))
}

pub fn strand_raw(i: &str) -> IResult<&str, String> {
	alt((strand_single, strand_double))(i)
}

fn strand_single(i: &str) -> IResult<&str, String> {
	let (i, _) = tag(SINGLE)(i)?;
	let (i, v) = alt((escaped(is_not(SINGLE_ESC), '\\', one_of(SINGLE)), tag("")))(i)?;
	let (i, _) = tag(SINGLE)(i)?;
	Ok((i, String::from(v).replace(SINGLE_ESC, SINGLE)))
}

fn strand_double(i: &str) -> IResult<&str, String> {
	let (i, _) = tag(DOUBLE)(i)?;
	let (i, v) = alt((escaped(is_not(DOUBLE_ESC), '\\', one_of(DOUBLE)), tag("")))(i)?;
	let (i, _) = tag(DOUBLE)(i)?;
	Ok((i, String::from(v).replace(DOUBLE_ESC, DOUBLE)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn strand_empty() {
		let sql = r#""""#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#""""#, format!("{}", out));
		assert_eq!(out, Strand::from(""));
	}

	#[test]
	fn strand_single() {
		let sql = r#"'test'"#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#""test""#, format!("{}", out));
		assert_eq!(out, Strand::from("test"));
	}

	#[test]
	fn strand_double() {
		let sql = r#""test""#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#""test""#, format!("{}", out));
		assert_eq!(out, Strand::from("test"));
	}

	#[test]
	fn strand_quoted_single() {
		let sql = r#"'te\'st'"#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#""te'st""#, format!("{}", out));
		assert_eq!(out, Strand::from(r#"te'st"#));
	}

	#[test]
	fn strand_quoted_double() {
		let sql = r#""te\"st""#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r#""te"st""#, format!("{}", out));
		assert_eq!(out, Strand::from(r#"te"st"#));
	}
}
