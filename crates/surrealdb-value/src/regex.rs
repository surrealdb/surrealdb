use std::cmp::Ordering;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str;
use std::str::FromStr;
use std::sync::LazyLock;

use quick_cache::sync::{Cache, GuardResult};
use regex::RegexBuilder;
use revision::revisioned;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::cnf::{REGEX_CACHE_SIZE, REGEX_SIZE_LIMIT};

pub(crate) const REGEX_TOKEN: &str = "$surrealdb::private::Regex";

#[revisioned(revision = 1)]
#[derive(Clone)]
pub struct Regex(pub regex::Regex);

impl Regex {
	// Deref would expose `regex::Regex::as_str` which wouldn't have the '/'
	// delimiters.
	pub fn regex(&self) -> &regex::Regex {
		&self.0
	}
}

pub(crate) fn regex_new(str: &str) -> Result<regex::Regex, regex::Error> {
	static REGEX_CACHE: LazyLock<Cache<String, regex::Regex>> =
		LazyLock::new(|| Cache::new(REGEX_CACHE_SIZE.max(10)));
	match REGEX_CACHE.get_value_or_guard(str, None) {
		GuardResult::Value(v) => Ok(v),
		GuardResult::Guard(g) => {
			let re = RegexBuilder::new(str).size_limit(*REGEX_SIZE_LIMIT).build()?;
			g.insert(re.clone()).ok();
			Ok(re)
		}
		GuardResult::Timeout => {
			warn!("Regex cache timeout");
			RegexBuilder::new(str).size_limit(*REGEX_SIZE_LIMIT).build()
		}
	}
}

impl FromStr for Regex {
	type Err = <regex::Regex as FromStr>::Err;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.contains('\0') {
			Err(regex::Error::Syntax("regex contained NUL byte".to_owned()))
		} else {
			regex_new(&s.replace("\\/", "/")).map(Self)
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
		Display::fmt(self, f)
	}
}

impl Display for Regex {
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
						Regex::from_str(value).map_err(|_| de::Error::custom("invalid regex"))
					}
				}

				deserializer.deserialize_str(RegexVisitor)
			}
		}

		deserializer.deserialize_newtype_struct(REGEX_TOKEN, RegexNewtypeVisitor)
	}
}

#[cfg(test)]
mod tests {
	use super::regex_new;
	#[test]
	fn regex_compile_limit() {
		match regex_new("^(a|b|c){1000000}") {
			Err(e) => {
				assert!(matches!(e, regex::Error::CompiledTooBig(10_485_760)), "{e}");
			}
			Ok(_) => panic!("regex should have failed"),
		}
	}
}
