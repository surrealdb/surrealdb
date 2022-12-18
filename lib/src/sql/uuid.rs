use crate::sql::common::is_hex;
use crate::sql::error::IResult;
use crate::sql::escape::escape_str;
use crate::sql::serde::is_internal_serialization;
use nom::branch::alt;
use nom::bytes::complete::take_while_m_n;
use nom::character::complete::char;
use nom::combinator::recognize;
use nom::sequence::delimited;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Deserialize, Hash)]
pub struct Uuid(pub uuid::Uuid);

impl From<&str> for Uuid {
	fn from(s: &str) -> Self {
		uuid::Uuid::try_parse(s).map(Self).unwrap_or_default()
	}
}

impl From<uuid::Uuid> for Uuid {
	fn from(v: uuid::Uuid) -> Self {
		Uuid(v)
	}
}

impl From<String> for Uuid {
	fn from(s: String) -> Self {
		Self::from(s.as_str())
	}
}

impl From<Uuid> for uuid::Uuid {
	fn from(s: Uuid) -> Self {
		s.0
	}
}

impl Deref for Uuid {
	type Target = uuid::Uuid;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Uuid {
	/// Generate a new V4 UUID
	pub fn new() -> Self {
		Self(uuid::Uuid::new_v4())
	}
	/// Generate a new V4 UUID
	pub fn new_v4() -> Self {
		Self(uuid::Uuid::new_v4())
	}
	/// Generate a new V7 UUID
	#[cfg(uuid_unstable)]
	pub fn new_v7() -> Self {
		Self(uuid::Uuid::now_v7())
	}
	/// Convert the Uuid to a raw String
	pub fn to_raw(&self) -> String {
		self.0.to_string()
	}
}

impl Display for Uuid {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&escape_str(&self.0.to_string()), f)
	}
}

impl Serialize for Uuid {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			serializer.serialize_newtype_struct("Uuid", &self.0)
		} else {
			serializer.serialize_some(&self.0)
		}
	}
}

pub fn uuid(i: &str) -> IResult<&str, Uuid> {
	alt((uuid_single, uuid_double))(i)
}

fn uuid_single(i: &str) -> IResult<&str, Uuid> {
	delimited(char('\''), uuid_raw, char('\''))(i)
}

fn uuid_double(i: &str) -> IResult<&str, Uuid> {
	delimited(char('\"'), uuid_raw, char('\"'))(i)
}

fn uuid_raw(i: &str) -> IResult<&str, Uuid> {
	let (i, v) = recognize(tuple((
		take_while_m_n(8, 8, is_hex),
		char('-'),
		take_while_m_n(4, 4, is_hex),
		char('-'),
		alt((
			char('1'),
			char('2'),
			char('3'),
			char('4'),
			char('5'),
			char('6'),
			char('7'),
			char('8'),
		)),
		take_while_m_n(3, 3, is_hex),
		char('-'),
		take_while_m_n(4, 4, is_hex),
		char('-'),
		take_while_m_n(12, 12, is_hex),
	)))(i)?;
	Ok((i, Uuid::from(v)))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn uuid_v1() {
		let sql = "e72bee20-f49b-11ec-b939-0242ac120002";
		let res = uuid_raw(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("'e72bee20-f49b-11ec-b939-0242ac120002'", format!("{}", out));
		assert_eq!(out, Uuid::from("e72bee20-f49b-11ec-b939-0242ac120002"));
	}

	#[test]
	fn uuid_v4() {
		let sql = "b19bc00b-aa98-486c-ae37-c8e1c54295b1";
		let res = uuid_raw(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("'b19bc00b-aa98-486c-ae37-c8e1c54295b1'", format!("{}", out));
		assert_eq!(out, Uuid::from("b19bc00b-aa98-486c-ae37-c8e1c54295b1"));
	}
}
