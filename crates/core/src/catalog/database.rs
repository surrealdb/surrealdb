use std::fmt::{Display, Formatter};

use revision::{Revisioned, revisioned};
use serde::{Deserialize, Serialize};

use crate::{
	catalog::NamespaceId,
	expr::{ChangeFeed, Value, statements::info::InfoStructure},
	kvs::impl_kv_value_revisioned,
	sql::ToSql,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct DatabaseId(pub u32);

impl_kv_value_revisioned!(DatabaseId);

impl Revisioned for DatabaseId {
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
		Revisioned::deserialize_revisioned(reader).map(DatabaseId)
	}
}

impl Display for DatabaseId {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DatabaseDefinition {
	pub namespace_id: NamespaceId,
	pub database_id: DatabaseId,
	pub name: String,
	pub comment: Option<String>,
	pub changefeed: Option<ChangeFeed>,
}
impl_kv_value_revisioned!(DatabaseDefinition);

impl ToSql for DatabaseDefinition {
	fn to_sql(&self) -> String {
		let mut s = String::new();
		s.push_str(&format!("DEFINE DATABASE {}", self.name));
		if let Some(comment) = &self.comment {
			s.push_str(&format!(" COMMENT {}", comment));
		}
		s
	}
}

impl InfoStructure for DatabaseDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
