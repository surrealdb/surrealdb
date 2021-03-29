use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::bytes::complete::tag;
use nom::sequence::delimited;
use nom::IResult;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize)]
pub struct Strand {
	pub value: String,
}

impl From<String> for Strand {
	fn from(s: String) -> Self {
		Strand {
			value: s,
		}
	}
}

impl<'a> From<&'a str> for Strand {
	fn from(s: &str) -> Self {
		Strand {
			value: String::from(s),
		}
	}
}

impl fmt::Display for Strand {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "\"{}\"", self.value)
	}
}

impl Serialize for Strand {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if serializer.is_human_readable() {
			serializer.serialize_some(&self.value)
		} else {
			let mut val = serializer.serialize_struct("Strand", 1)?;
			val.serialize_field("value", &self.value)?;
			val.end()
		}
	}
}

pub fn strand(i: &str) -> IResult<&str, Strand> {
	let (i, v) = strand_raw(i)?;
	Ok((i, Strand::from(v)))
}

pub fn strand_raw(i: &str) -> IResult<&str, &str> {
	alt((strand_single, strand_double))(i)
}

fn strand_single(i: &str) -> IResult<&str, &str> {
	delimited(tag("\'"), is_not("\'"), tag("\'"))(i)
}

fn strand_double(i: &str) -> IResult<&str, &str> {
	delimited(tag("\""), is_not("\""), tag("\""))(i)
}
