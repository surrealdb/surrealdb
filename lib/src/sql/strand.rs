use crate::sql::error::IResult;
use crate::sql::escape::escape_strand;
use crate::sql::serde::is_internal_serialization;
use nom::branch::alt;
use nom::bytes::complete::escaped_transform;
use nom::bytes::complete::is_not;
use nom::bytes::complete::take_while_m_n;
use nom::character::complete::char;
use nom::combinator::value;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops;
use std::ops::Deref;
use std::str;

const SINGLE: char = '\'';
const SINGLE_ESC: &str = r#"\'"#;

const DOUBLE: char = '"';
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
	alt((strand_blank, strand_single, strand_double))(i)
}

fn strand_blank(i: &str) -> IResult<&str, String> {
	alt((
		|i| {
			let (i, _) = char(SINGLE)(i)?;
			let (i, _) = char(SINGLE)(i)?;
			Ok((i, String::new()))
		},
		|i| {
			let (i, _) = char(DOUBLE)(i)?;
			let (i, _) = char(DOUBLE)(i)?;
			Ok((i, String::new()))
		},
	))(i)
}

fn strand_single(i: &str) -> IResult<&str, String> {
	let (i, _) = char(SINGLE)(i)?;
	let (i, v) = escaped_transform(
		is_not(SINGLE_ESC),
		'\\',
		alt((
			strand_unicode,
			value('\u{5c}', char('\\')),
			value('\u{27}', char('\'')),
			value('\u{2f}', char('/')),
			value('\u{08}', char('b')),
			value('\u{0c}', char('f')),
			value('\u{0a}', char('n')),
			value('\u{0d}', char('r')),
			value('\u{09}', char('t')),
		)),
	)(i)?;
	let (i, _) = char(SINGLE)(i)?;
	Ok((i, v))
}

fn strand_double(i: &str) -> IResult<&str, String> {
	let (i, _) = char(DOUBLE)(i)?;
	let (i, v) = escaped_transform(
		is_not(DOUBLE_ESC),
		'\\',
		alt((
			strand_unicode,
			value('\u{5c}', char('\\')),
			value('\u{22}', char('\"')),
			value('\u{2f}', char('/')),
			value('\u{08}', char('b')),
			value('\u{0c}', char('f')),
			value('\u{0a}', char('n')),
			value('\u{0d}', char('r')),
			value('\u{09}', char('t')),
		)),
	)(i)?;
	let (i, _) = char(DOUBLE)(i)?;
	Ok((i, v))
}

fn strand_unicode(i: &str) -> IResult<&str, char> {
	// Read the \u character
	let (i, _) = char('u')(i)?;
	// Let's read the next 4 ascii hexadecimal characters
	let (i, v) = take_while_m_n(1, 4, |c: char| c.is_ascii_hexdigit())(i)?;
	// We can convert this to u32 as we only have 4 chars
	let v = u32::from_str_radix(v, 16).unwrap();
	// We can convert this to char as we know it is valid
	let v = std::char::from_u32(v).unwrap();
	// Return the unicode char
	Ok((i, v))
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

	#[test]
	fn strand_quoted_escaped() {
		let sql = r#""te\"st\n\tand\bsome\u05d9""#;
		let res = strand(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("\"te\"st\n\tand\u{08}some\u{05d9}\"", format!("{}", out));
		assert_eq!(out, Strand::from("te\"st\n\tand\u{08}some\u{05d9}"));
	}
}
