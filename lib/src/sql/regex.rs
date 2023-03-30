use crate::sql::error::IResult;
use crate::sql::serde::is_internal_serialization;
use nom::bytes::complete::escaped;
use nom::bytes::complete::is_not;
use nom::character::complete::anychar;
use nom::character::complete::char;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Regex";

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Deserialize, Hash)]
pub struct Regex(pub(super) String);

impl From<&str> for Regex {
	fn from(r: &str) -> Self {
		Self(r.replace("\\/", "/"))
	}
}

impl Deref for Regex {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Regex {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "/{}/", &self.0)
	}
}

impl Regex {
	pub fn regex(&self) -> Option<regex::Regex> {
		regex::Regex::new(&self.0).ok()
	}
}

impl Serialize for Regex {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			serializer.serialize_newtype_struct(TOKEN, &self.0)
		} else {
			serializer.serialize_none()
		}
	}
}

pub fn regex(i: &str) -> IResult<&str, Regex> {
	let (i, _) = char('/')(i)?;
	let (i, v) = escaped(is_not("\\/"), '\\', anychar)(i)?;
	let (i, _) = char('/')(i)?;
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
		let sql = r"/(?i)test\/[a-z]+\/\s\d\w{1}.*/";
		let res = regex(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r"/(?i)test/[a-z]+/\s\d\w{1}.*/", format!("{}", out));
		assert_eq!(out, Regex::from(r"(?i)test/[a-z]+/\s\d\w{1}.*"));
	}
}
