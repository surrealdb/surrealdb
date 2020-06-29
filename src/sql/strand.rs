use nom::branch::alt;
use nom::bytes::complete::is_not;
use nom::bytes::complete::tag;
use nom::sequence::delimited;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Strand {
	pub value: String,
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
