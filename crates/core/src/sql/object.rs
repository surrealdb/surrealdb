use crate::err::Error;
use crate::sql::{
	Operation, SqlValue, Thing,
	escape::EscapeKey,
	fmt::{Fmt, Pretty, is_pretty, pretty_indent},
};
use anyhow::{Result, bail};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter, Write};
use std::ops::Deref;
use std::ops::DerefMut;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Object";

/// Invariant: Keys never contain NUL bytes.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Object")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Object(#[serde(with = "no_nul_bytes_in_keys")] pub BTreeMap<String, SqlValue>);

impl From<BTreeMap<&str, SqlValue>> for Object {
	fn from(v: BTreeMap<&str, SqlValue>) -> Self {
		Self(v.into_iter().map(|(key, val)| (key.to_string(), val)).collect())
	}
}

impl From<BTreeMap<String, SqlValue>> for Object {
	fn from(v: BTreeMap<String, SqlValue>) -> Self {
		Self(v)
	}
}

impl FromIterator<(String, SqlValue)> for Object {
	fn from_iter<T: IntoIterator<Item = (String, SqlValue)>>(iter: T) -> Self {
		Self(BTreeMap::from_iter(iter))
	}
}

impl From<BTreeMap<String, String>> for Object {
	fn from(v: BTreeMap<String, String>) -> Self {
		Self(v.into_iter().map(|(k, v)| (k, SqlValue::from(v))).collect())
	}
}

impl From<HashMap<&str, SqlValue>> for Object {
	fn from(v: HashMap<&str, SqlValue>) -> Self {
		Self(v.into_iter().map(|(key, val)| (key.to_string(), val)).collect())
	}
}

impl From<HashMap<String, SqlValue>> for Object {
	fn from(v: HashMap<String, SqlValue>) -> Self {
		Self(v.into_iter().collect())
	}
}

impl From<Option<Self>> for Object {
	fn from(v: Option<Self>) -> Self {
		v.unwrap_or_default()
	}
}

impl From<Operation> for Object {
	fn from(v: Operation) -> Self {
		Self(match v {
			Operation::Add {
				path,
				value,
			} => map! {
				String::from("op") => SqlValue::from("add"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
			Operation::Remove {
				path,
			} => map! {
				String::from("op") => SqlValue::from("remove"),
				String::from("path") => path.to_path().into()
			},
			Operation::Replace {
				path,
				value,
			} => map! {
				String::from("op") => SqlValue::from("replace"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
			Operation::Change {
				path,
				value,
			} => map! {
				String::from("op") => SqlValue::from("change"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
			Operation::Copy {
				path,
				from,
			} => map! {
				String::from("op") => SqlValue::from("copy"),
				String::from("path") => path.to_path().into(),
				String::from("from") => from.to_path().into()
			},
			Operation::Move {
				path,
				from,
			} => map! {
				String::from("op") => SqlValue::from("move"),
				String::from("path") => path.to_path().into(),
				String::from("from") => from.to_path().into()
			},
			Operation::Test {
				path,
				value,
			} => map! {
				String::from("op") => SqlValue::from("test"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
		})
	}
}

impl From<Object> for crate::expr::Object {
	fn from(v: Object) -> Self {
		crate::expr::Object(v.0.into_iter().map(|(k, v)| (k, v.into())).collect())
	}
}

impl From<crate::expr::Object> for Object {
	fn from(v: crate::expr::Object) -> Self {
		Object(v.0.into_iter().map(|(k, v)| (k, v.into())).collect())
	}
}

impl Deref for Object {
	type Target = BTreeMap<String, SqlValue>;
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
	type Item = (String, SqlValue);
	type IntoIter = std::collections::btree_map::IntoIter<String, SqlValue>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Object {
	/// Fetch the record id if there is one
	pub fn rid(&self) -> Option<Thing> {
		match self.get("id") {
			Some(SqlValue::Thing(v)) => Some(v.clone()),
			_ => None,
		}
	}
	/// Convert this object to a diff-match-patch operation
	pub fn to_operation(&self) -> Result<Operation> {
		let Some(op_val) = self.get("op") else {
			bail!(Error::InvalidPatch {
				message: String::from("'op' key missing"),
			})
		};
		let Some(path_val) = self.get("path") else {
			bail!(Error::InvalidPatch {
				message: String::from("'path' key missing"),
			})
		};
		let path = path_val.jsonpath();

		let from =
			self.get("from").map(|value| value.jsonpath()).ok_or_else(|| Error::InvalidPatch {
				message: String::from("'from' key missing"),
			});

		let value = self.get("value").cloned().ok_or_else(|| Error::InvalidPatch {
			message: String::from("'value' key missing"),
		});

		match op_val.clone().as_string().as_str() {
			// Add operation
			"add" => Ok(Operation::Add {
				path,
				value: value?,
			}),
			// Remove operation
			"remove" => Ok(Operation::Remove {
				path,
			}),
			// Replace operation
			"replace" => Ok(Operation::Replace {
				path,
				value: value?,
			}),
			// Change operation
			"change" => Ok(Operation::Change {
				path,
				value: value?,
			}),
			// Copy operation
			"copy" => Ok(Operation::Copy {
				path,
				from: from?,
			}),
			// Move operation
			"move" => Ok(Operation::Move {
				path,
				from: from?,
			}),
			// Test operation
			"test" => Ok(Operation::Test {
				path,
				value: value?,
			}),
			unknown_op => Err(anyhow::Error::new(Error::InvalidPatch {
				message: format!("unknown op '{unknown_op}'"),
			})),
		}
	}

	/// Checks whether all object values are static values
	pub(crate) fn is_static(&self) -> bool {
		self.values().all(SqlValue::is_static)
	}

	/// Validate that a Object contains only computed Values
	pub(crate) fn validate_computed(&self) -> Result<()> {
		self.values().try_for_each(|v| v.validate_computed())
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
		let mut f = Pretty::from(f);
		if is_pretty() {
			f.write_char('{')?;
		} else {
			f.write_str("{ ")?;
		}
		if !self.is_empty() {
			let indent = pretty_indent();
			write!(
				f,
				"{}",
				Fmt::pretty_comma_separated(
					self.0.iter().map(|args| Fmt::new(args, |(k, v), f| write!(
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

mod no_nul_bytes_in_keys {
	use serde::{
		Deserializer, Serializer,
		de::{self, Visitor},
		ser::SerializeMap,
	};
	use std::{collections::BTreeMap, fmt};

	use crate::sql::SqlValue;

	pub(crate) fn serialize<S>(
		m: &BTreeMap<String, SqlValue>,
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

	pub(crate) fn deserialize<'de, D>(
		deserializer: D,
	) -> Result<BTreeMap<String, SqlValue>, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct NoNulBytesInKeysVisitor;

		impl<'de> Visitor<'de> for NoNulBytesInKeysVisitor {
			type Value = BTreeMap<String, SqlValue>;

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
