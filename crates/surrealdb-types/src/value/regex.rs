use std::cmp::Ordering;
use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use regex::RegexBuilder;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub(crate) const REGEX_TOKEN: &str = "$surrealdb::public::Regex";

/// Represents a regular expression in SurrealDB
///
/// A regular expression is a pattern used for matching strings.
/// This type wraps the `regex::Regex` type and provides custom serialization/deserialization.
#[derive(Clone)]
pub struct Regex(pub regex::Regex);

impl Regex {
	/// Returns a reference to the underlying regex
	///
	/// Note: This method returns the regex without the '/' delimiters that are used in display.
	pub fn regex(&self) -> &regex::Regex {
		&self.0
	}
}

impl FromStr for Regex {
	type Err = <regex::Regex as FromStr>::Err;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.contains('\0') {
			Err(regex::Error::Syntax("regex contained NUL byte".to_owned()))
		} else {
			Ok(Regex(RegexBuilder::new(&s.replace("\\/", "/")).build()?))
		}
	}
}

impl PartialEq for Regex {
	fn eq(&self, other: &Self) -> bool {
		let str_left = self.0.as_str();
		let str_right = other.0.as_str();
		str_left == str_right
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
		let t = self.0.to_string().replace('/', "\\/");
		write!(f, "/{}/", &t)
	}
}

impl Serialize for Regex {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_newtype_struct(REGEX_TOKEN, self.0.as_str())
	}
}

impl<'de> Deserialize<'de> for Regex {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct RegexNewtypeVisitor;

		impl<'de> Visitor<'de> for RegexNewtypeVisitor {
			type Value = Regex;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("a regex newtype")
			}

			fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
			where
				D: Deserializer<'de>,
			{
				struct RegexVisitor;

				impl Visitor<'_> for RegexVisitor {
					type Value = Regex;

					fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
						formatter.write_str("a regex str")
					}

					fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
					where
						E: de::Error,
					{
						regex::Regex::from_str(value)
							.map(Regex)
							.map_err(|_| de::Error::custom("invalid regex"))
					}
				}

				deserializer.deserialize_str(RegexVisitor)
			}
		}

		deserializer.deserialize_newtype_struct(REGEX_TOKEN, RegexNewtypeVisitor)
	}
}
