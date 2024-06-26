use once_cell::sync::Lazy;
use quick_cache::sync::{Cache, GuardResult};
use revision::revisioned;
use serde::{
	de::{self, Visitor},
	Deserialize, Deserializer, Serialize, Serializer,
};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::{env, str};

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Regex";

#[revisioned(revision = 1)]
#[derive(Clone)]
#[non_exhaustive]
pub struct Regex(pub regex::Regex);

impl Regex {
	// Deref would expose `regex::Regex::as_str` which wouldn't have the '/' delimiters.
	pub fn regex(&self) -> &regex::Regex {
		&self.0
	}
}

fn regex_new(str: &str) -> Result<regex::Regex, regex::Error> {
	static REGEX_CACHE: Lazy<Cache<String, regex::Regex>> = Lazy::new(|| {
		let cache_size: usize = env::var("SURREAL_REGEX_CACHE_SIZE")
			.map_or(1000, |v| v.parse().unwrap_or(1000))
			.max(10); // The minimum cache size is 10
		Cache::new(cache_size)
	});
	match REGEX_CACHE.get_value_or_guard(str, None) {
		GuardResult::Value(v) => Ok(v),
		GuardResult::Guard(g) => {
			let re = regex::Regex::new(str)?;
			g.insert(re.clone()).ok();
			Ok(re)
		}
		GuardResult::Timeout => {
			warn!("Regex cache timeout");
			regex::Regex::new(str)
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
		serializer.serialize_newtype_struct(TOKEN, self.0.as_str())
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

		deserializer.deserialize_newtype_struct(TOKEN, RegexNewtypeVisitor)
	}
}
