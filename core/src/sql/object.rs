use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{
	escape::escape_key,
	fmt::{is_pretty, pretty_indent, Fmt, Pretty},
	Operation, Thing, Value,
};
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

impl From<Operation> for Object {
	fn from(v: Operation) -> Self {
		Self(match v {
			Operation::Add {
				path,
				value,
			} => map! {
				String::from("op") => Value::from("add"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
			Operation::Remove {
				path,
			} => map! {
				String::from("op") => Value::from("remove"),
				String::from("path") => path.to_path().into()
			},
			Operation::Replace {
				path,
				value,
			} => map! {
				String::from("op") => Value::from("replace"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
			Operation::Change {
				path,
				value,
			} => map! {
				String::from("op") => Value::from("change"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
			Operation::Copy {
				path,
				from,
			} => map! {
				String::from("op") => Value::from("copy"),
				String::from("path") => path.to_path().into(),
				String::from("from") => from.to_path().into()
			},
			Operation::Move {
				path,
				from,
			} => map! {
				String::from("op") => Value::from("move"),
				String::from("path") => path.to_path().into(),
				String::from("from") => from.to_path().into()
			},
			Operation::Test {
				path,
				value,
			} => map! {
				String::from("op") => Value::from("test"),
				String::from("path") => path.to_path().into(),
				String::from("value") => value
			},
		})
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

impl Object {
	/// Fetch the record id if there is one
	pub fn rid(&self) -> Option<Thing> {
		match self.get("id") {
			Some(Value::Thing(v)) => Some(v.clone()),
			_ => None,
		}
	}
	/// Convert this object to a diff-match-patch operation
	pub fn to_operation(&self) -> Result<Operation, Error> {
		match self.get("op") {
			Some(op_val) => match self.get("path") {
				Some(path_val) => {
					let path = path_val.jsonpath();

					let from =
						self.get("from").map(|value| value.jsonpath()).ok_or(Error::InvalidPatch {
							message: String::from("'from' key missing"),
						});

					let value = self.get("value").cloned().ok_or(Error::InvalidPatch {
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
						unknown_op => Err(Error::InvalidPatch {
							message: format!("unknown op '{unknown_op}'"),
						}),
					}
				}
				_ => Err(Error::InvalidPatch {
					message: String::from("'path' key missing"),
				}),
			},
			_ => Err(Error::InvalidPatch {
				message: String::from("'op' key missing"),
			}),
		}
	}
}

impl Object {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		let mut x = BTreeMap::new();
		for (k, v) in self.iter() {
			match v.compute(ctx, opt, txn, doc).await {
				Ok(v) => x.insert(k.clone(), v),
				Err(e) => return Err(e),
			};
		}
		Ok(Value::Object(Object(x)))
	}

	pub(crate) fn is_static(&self) -> bool {
		self.values().all(Value::is_static)
	}

	/// Validate that a Object contains only computed Values
	pub(crate) fn validate_computed(&self) -> Result<(), Error> {
		self.values().try_for_each(|v| v.validate_computed())
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
						escape_key(k),
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
		de::{self, Visitor},
		ser::SerializeMap,
		Deserializer, Serializer,
	};
	use std::{collections::BTreeMap, fmt};

	use crate::sql::Value;

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
