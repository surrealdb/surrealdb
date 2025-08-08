use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use revision::{Revisioned, revisioned};

use crate::{
	expr::{Value, statements::info::InfoStructure},
	kvs::impl_kv_value_revisioned,
	sql::ToSql,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct NamespaceId(pub u32);

impl_kv_value_revisioned!(NamespaceId);

impl Revisioned for NamespaceId {
	fn revision() -> u16 {
		1
	}

	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		self.0.serialize_revisioned(writer)
	}

	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		Revisioned::deserialize_revisioned(reader).map(NamespaceId)
	}
}

impl Display for NamespaceId {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl From<u32> for NamespaceId {
	fn from(value: u32) -> Self {
		Self(value)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct NamespaceDefinition {
	pub namespace_id: NamespaceId,
	pub name: String,
	pub comment: Option<String>,
}
impl_kv_value_revisioned!(NamespaceDefinition);

impl ToSql for NamespaceDefinition {
	fn to_sql(&self) -> String {
		let mut out = String::new();
		out.push_str(&format!("DEFINE NAMESPACE {}", self.name));
		if let Some(comment) = &self.comment {
			out.push_str(&format!(" COMMENT {comment}"));
		}
		out
	}
}

impl InfoStructure for NamespaceDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
