use std::cmp::Ordering;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str;
use std::str::FromStr;
use std::sync::LazyLock;

use quick_cache::sync::{Cache, GuardResult};
use revision::{
	BorrowedReader, DeserializeRevisioned, LeafWalker, Revisioned, SerializeRevisioned,
	SkipRevisioned, WalkRevisioned,
};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use storekey::{BorrowDecode, Encode};
use surrealdb_cnf::{REGEX_CACHE_SIZE, REGEX_SIZE_LIMIT};
use surrealdb_strand::Strand;
use surrealdb_types::regex_engine::{self, RegexError};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

pub const REGEX_TOKEN: &str = "$surrealdb::private::Regex";

#[derive(Clone)]
pub struct Regex(pub regex_engine::Regex);

// Stored as a revision-1 struct holding the pattern string: the `u16` revision
// then the pattern's length-prefixed bytes. Decoding compiles the pattern with
// the engine's default size limit, so a pattern this build cannot compile
// fails to decode.
impl Revisioned for Regex {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for Regex {
	fn serialize_revisioned<W: std::io::Write>(&self, w: &mut W) -> Result<(), revision::Error> {
		Self::revision().serialize_revisioned(w)?;
		self.0.as_str().serialize_revisioned(w)
	}
}

impl DeserializeRevisioned for Regex {
	fn deserialize_revisioned<R: std::io::Read>(r: &mut R) -> Result<Self, revision::Error> {
		match u16::deserialize_revisioned(r)? {
			1 => {
				// Strand reads a claimed length without allocating it up front.
				let s = Strand::deserialize_revisioned(r)?;
				regex_engine::compile(s.as_str(), None)
					.map(Self)
					.map_err(|e| revision::Error::Deserialize(format!("invalid regex: {e}")))
			}
			x => Err(revision::Error::Deserialize(format!(
				"Invalid revision `{x}` for type `Regex`"
			))),
		}
	}
}

impl SkipRevisioned for Regex {
	fn skip_revisioned<R: std::io::Read>(r: &mut R) -> Result<(), revision::Error> {
		u16::skip_revisioned(r)?;
		String::skip_revisioned(r)
	}

	#[inline]
	fn skip_revisioned_slice(
		reader: &mut revision::SliceReader<'_>,
	) -> Result<(), revision::Error> {
		u16::skip_revisioned_slice(reader)?;
		String::skip_revisioned_slice(reader)
	}
}

impl WalkRevisioned for Regex {
	type Walker<'r, R: BorrowedReader + 'r> = LeafWalker<'r, Regex, R>;

	fn walk_revisioned<'r, R: BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(LeafWalker::new(reader))
	}
}

impl Regex {
	// Deref would expose the engine's `as_str`, which wouldn't have the '/'
	// delimiters.
	pub fn inner(&self) -> &regex_engine::Regex {
		&self.0
	}
}

pub fn regex_new(str: &str) -> Result<regex_engine::Regex, RegexError> {
	static REGEX_CACHE: LazyLock<Cache<String, regex_engine::Regex>> =
		LazyLock::new(|| Cache::new(REGEX_CACHE_SIZE.max(10)));
	match REGEX_CACHE.get_value_or_guard(str, None) {
		GuardResult::Value(v) => Ok(v),
		GuardResult::Guard(g) => {
			let re = regex_engine::compile(str, Some(*REGEX_SIZE_LIMIT))?;
			g.insert(re.clone()).ok();
			Ok(re)
		}
		GuardResult::Timeout => {
			warn!("Regex cache timeout");
			regex_engine::compile(str, Some(*REGEX_SIZE_LIMIT))
		}
	}
}

impl FromStr for Regex {
	type Err = RegexError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		regex_new(&s.replace("\\/", "/")).map(Self)
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
		std::fmt::Display::fmt(&self.0, f)
	}
}

impl ToSql for Regex {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let t = self.0.to_string().replace('/', "\\/");
		write_sql!(f, sql_fmt, "/{}/", &t)
	}
}

impl From<regex_engine::Regex> for Regex {
	fn from(v: regex_engine::Regex) -> Self {
		Self(v)
	}
}

impl From<Regex> for regex_engine::Regex {
	fn from(v: Regex) -> Self {
		v.0
	}
}

impl From<surrealdb_types::Regex> for Regex {
	fn from(v: surrealdb_types::Regex) -> Self {
		Self(v.into_inner())
	}
}

impl From<Regex> for surrealdb_types::Regex {
	fn from(x: Regex) -> Self {
		surrealdb_types::Regex::from(x.0)
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
						Regex::from_str(value)
							.map_err(|e| de::Error::custom(format_args!("invalid regex: {e}")))
					}
				}

				deserializer.deserialize_str(RegexVisitor)
			}
		}

		deserializer.deserialize_newtype_struct(REGEX_TOKEN, RegexNewtypeVisitor)
	}
}

impl<F> Encode<F> for Regex {
	fn encode<W: std::io::Write>(
		&self,
		_: &mut storekey::Writer<W>,
	) -> Result<(), storekey::EncodeError> {
		Err(storekey::EncodeError::message("Regex cannot be encoded"))
	}
}

impl<'de, F> BorrowDecode<'de, F> for Regex {
	fn borrow_decode(_: &mut storekey::BorrowReader<'de>) -> Result<Self, storekey::DecodeError> {
		Err(storekey::DecodeError::message("Regex cannot be decoded"))
	}
}

#[cfg(test)]
mod tests {
	use revision::{SerializeRevisioned, SkipRevisioned};

	use super::{Regex, regex_new};

	#[test]
	fn slice_skip_agrees_with_reader_skip() {
		let regex = Regex(regex_new("^a+b$").unwrap());
		let mut bytes = Vec::new();
		regex.serialize_revisioned(&mut bytes).unwrap();
		assert_eq!(revision::skip_slice::<Regex>(&bytes).unwrap(), bytes.len());
		let cut = &bytes[..bytes.len() - 1];
		assert!(revision::skip_slice::<Regex>(cut).is_err());
		assert!(Regex::skip_revisioned(&mut &cut[..]).is_err());
	}

	#[test]
	fn regex_compile_limit() {
		match regex_new("^(a|b|c){1000000}") {
			Err(e) => {
				assert!(e.message().contains("size limit"), "{e}");
			}
			Ok(_) => panic!("regex should have failed"),
		}
	}
}
