use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display, Formatter, Write};
use std::ops::{Deref, DerefMut};

use anyhow::Result;
use http::{HeaderMap, HeaderName, HeaderValue};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};

use crate::err::Error;
use crate::expr::literal::ObjectEntry;
use crate::fmt::{EscapeKey, Fmt, Pretty, is_pretty, pretty_indent};
use crate::val::{IndexFormat, RecordId, Value};

/// Invariant: Keys never contain NUL bytes.
#[revisioned(revision = 1)]
#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Ord,
	PartialEq,
	PartialOrd,
	Serialize,
	Deserialize,
	Hash,
	Encode,
	BorrowDecode,
)]
#[serde(rename = "$surrealdb::private::Object")]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub(crate) struct Object(pub(crate) BTreeMap<String, Value>);

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

impl TryFrom<Object> for crate::types::PublicObject {
	type Error = anyhow::Error;

	fn try_from(s: Object) -> Result<Self, Self::Error> {
		s.0.into_iter()
			.map(|(k, v)| crate::types::PublicValue::try_from(v).map(|v| (k, v)))
			.collect()
	}
}

impl From<crate::types::PublicObject> for Object {
	fn from(s: crate::types::PublicObject) -> Self {
		s.into_iter().map(|(k, v)| (k, Value::from(v))).collect()
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

impl surrealdb_types::ToSql for Object {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		use surrealdb_types::write_sql;

		use crate::fmt::EscapeKey;

		if self.is_empty() {
			return f.push_str("{  }");
		}

		if fmt.is_pretty() {
			f.push('{');
		} else {
			f.push_str("{ ");
		}

		if !self.is_empty() {
			let inner_fmt = fmt.increment();
			if fmt.is_pretty() {
				f.push('\n');
				inner_fmt.write_indent(f);
			}
			for (i, (key, value)) in self.0.iter().enumerate() {
				if i > 0 {
					inner_fmt.write_separator(f);
				}
				write_sql!(f, "{}: ", EscapeKey(key));
				value.fmt_sql(f, inner_fmt);
			}
			if fmt.is_pretty() {
				f.push('\n');
				fmt.write_indent(f);
			}
		}

		if fmt.is_pretty() {
			f.push('}');
		} else {
			f.push_str(" }");
		}
	}
}
