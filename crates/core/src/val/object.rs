use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display, Formatter, Write};
use std::ops::{Deref, DerefMut};

use anyhow::Result;
use http::{HeaderMap, HeaderName, HeaderValue};
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::err::Error;
use crate::expr::escape::EscapeKey;
use crate::expr::fmt::{Fmt, Pretty, is_pretty, pretty_indent};
use crate::expr::literal::ObjectEntry;
use crate::val::{RecordId, Value};

/// Invariant: Keys never contain NUL bytes.
/// TODO: Null byte validity
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::Object")]
pub struct Object(#[serde(with = "no_nul_bytes_in_keys")] pub BTreeMap<String, Value>);

impl From<BTreeMap<&str, Value>> for Object {
	fn from(v: BTreeMap<&str, Value>) -> Self {
		Self(v.into_iter().map(|(key, val)| (key.to_string(), val)).collect())
	}
}

impl From<BTreeMap<String, Value>> for Object {
	fn from(v: BTreeMap<String, Value>) -> Self {
		Self(v)
	}
}

impl FromIterator<(String, Value)> for Object {
	fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
		Self(BTreeMap::from_iter(iter))
	}
}

impl From<BTreeMap<String, String>> for Object {
	fn from(v: BTreeMap<String, String>) -> Self {
		Self(v.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
	}
}

impl From<HashMap<&str, Value>> for Object {
	fn from(v: HashMap<&str, Value>) -> Self {
		Self(v.into_iter().map(|(key, val)| (key.to_string(), val)).collect())
	}
}

impl From<HashMap<String, Value>> for Object {
	fn from(v: HashMap<String, Value>) -> Self {
		Self(v.into_iter().collect())
	}
}

impl From<Option<Self>> for Object {
	fn from(v: Option<Self>) -> Self {
		v.unwrap_or_default()
	}
}

impl Deref for Object {
	type Target = BTreeMap<String, Value>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Object {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl IntoIterator for Object {
	type Item = (String, Value);
	type IntoIter = std::collections::btree_map::IntoIter<String, Value>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl TryInto<BTreeMap<String, String>> for Object {
	type Error = Error;
	fn try_into(self) -> Result<BTreeMap<String, String>, Self::Error> {
		self.into_iter().map(|(k, v)| Ok((k, v.coerce_to()?))).collect()
	}
}

impl TryInto<HeaderMap> for Object {
	type Error = Error;
	fn try_into(self) -> Result<HeaderMap, Self::Error> {
		let mut headermap = HeaderMap::new();
		for (k, v) in self.into_iter() {
			let k: HeaderName = k.parse()?;
			let v: HeaderValue = v.coerce_to::<String>()?.parse()?;
			headermap.insert(k, v);
		}

		Ok(headermap)
	}
}

impl Object {
	pub fn new() -> Self {
		Object(BTreeMap::new())
	}

	/// Fetch the record id if there is one
	pub fn rid(&self) -> Option<RecordId> {
		match self.get("id") {
			Some(Value::RecordId(v)) => Some(v.clone()),
			_ => None,
		}
	}

	pub fn into_literal(self) -> Vec<ObjectEntry> {
		self.0
			.into_iter()
			.map(|(k, v)| ObjectEntry {
				key: k,
				value: v.into_literal(),
			})
			.collect()
	}

	pub(crate) fn display<V: Display>(f: &mut Formatter, o: &BTreeMap<String, V>) -> fmt::Result {
		let mut f = Pretty::from(f);
		if is_pretty() {
			f.write_char('{')?;
		} else {
			f.write_str("{ ")?;
		}
		if !o.is_empty() {
			let indent = pretty_indent();
			write!(
				f,
				"{}",
				Fmt::pretty_comma_separated(
					o.iter().map(|args| Fmt::new(args, |(k, v), f| write!(
						f,
						"{}: {}",
						EscapeKey(k),
						v
					))),
				)
			)?;
			drop(indent);
		}
		if is_pretty() {
			f.write_char('}')
		} else {
			f.write_str(" }")
		}
	}
}

impl std::ops::Add for Object {
	type Output = Self;

	fn add(self, rhs: Self) -> Self::Output {
		let mut lhs = self;
		lhs.0.extend(rhs.0);
		lhs
	}
}

impl Display for Object {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Object::display(f, &self.0)
	}
}

mod no_nul_bytes_in_keys {
	use std::collections::BTreeMap;
	use std::fmt;

	use serde::de::{self, Visitor};
	use serde::ser::SerializeMap;
	use serde::{Deserializer, Serializer};

	use crate::val::Value;

	pub(crate) fn serialize<S>(
		m: &BTreeMap<String, Value>,
		serializer: S,
	) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let mut s = serializer.serialize_map(Some(m.len()))?;
		for (k, v) in m {
			debug_assert!(!k.contains('\0'));
			s.serialize_entry(k, v)?;
		}
		s.end()
	}

	pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<String, Value>, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct NoNulBytesInKeysVisitor;

		impl<'de> Visitor<'de> for NoNulBytesInKeysVisitor {
			type Value = BTreeMap<String, Value>;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("a map without any NUL bytes in its keys")
			}

			fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
			where
				A: de::MapAccess<'de>,
			{
				let mut ret = BTreeMap::new();
				while let Some((k, v)) = map.next_entry()? {
					ret.insert(k, v);
				}
				Ok(ret)
			}
		}

		deserializer.deserialize_map(NoNulBytesInKeysVisitor)
	}
}
