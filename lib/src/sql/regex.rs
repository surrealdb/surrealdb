use crate::sql::error::IResult;
use nom::bytes::complete::escaped;
use nom::bytes::complete::is_not;
use nom::character::complete::anychar;
use nom::character::complete::char;
use serde::{
	de::{self, Visitor},
	Deserialize, Deserializer, Serialize, Serializer,
};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str;
use std::str::FromStr;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Regex";

#[derive(Clone)]
pub struct Regex(pub(super) regex::Regex);

impl Regex {
	// Deref would expose `regex::Regex::as_str` which wouldn't have the '/' delimiters.
	pub fn regex(&self) -> &regex::Regex {
		&self.0
	}
}

impl FromStr for Regex {
	type Err = <regex::Regex as FromStr>::Err;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		regex::Regex::new(&s.replace("\\/", "/")).map(Self)
	}
}

impl PartialEq for Regex {
	fn eq(&self, other: &Self) -> bool {
		self.0.as_str().eq(other.0.as_str())
	}
}

impl Eq for Regex {}

impl Ord for Regex {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.as_str().cmp(other.0.as_str())
	}
}

impl PartialOrd for Regex {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Hash for Regex {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.as_str().hash(state);
	}
}

impl Debug for Regex {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(self, f)
	}
}

impl Display for Regex {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "/{}/", &self.0)
	}
}

impl Serialize for Regex {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_newtype_struct(TOKEN, self.0.as_str())
	}
}

impl<'de> Deserialize<'de> for Regex {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct RegexVisitor;

		impl<'de> Visitor<'de> for RegexVisitor {
			type Value = Regex;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("a regex str")
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				Regex::from_str(value).map_err(|_| de::Error::custom("invalid regex"))
			}
		}

		deserializer.deserialize_str(RegexVisitor)
	}
}

pub fn regex(i: &str) -> IResult<&str, Regex> {
	let (i, _) = char('/')(i)?;
	let (i, v) = escaped(is_not("\\/"), '\\', anychar)(i)?;
	let (i, _) = char('/')(i)?;
	let regex = v.parse().map_err(|_| nom::Err::Error(crate::sql::Error::Parser(v)))?;
	Ok((i, regex))
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
		assert_eq!(out, "test".parse().unwrap());
	}

	#[test]
	fn regex_complex() {
		let sql = r"/(?i)test\/[a-z]+\/\s\d\w{1}.*/";
		let res = regex(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(r"/(?i)test/[a-z]+/\s\d\w{1}.*/", format!("{}", out));
		assert_eq!(out, r"(?i)test/[a-z]+/\s\d\w{1}.*".parse().unwrap());
	}
}
