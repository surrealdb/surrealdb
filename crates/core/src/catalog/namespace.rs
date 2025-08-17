use std::fmt::{Display, Formatter};

use revision::{Revisioned, revisioned};
use serde::{Deserialize, Serialize};

use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::DefineNamespaceStatement;
use crate::sql::{Ident, ToSql};
use crate::val::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
		NamespaceId(value)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct NamespaceDefinition {
	pub namespace_id: NamespaceId,
	pub name: String,
	pub comment: Option<String>,
}
impl_kv_value_revisioned!(NamespaceDefinition);

impl NamespaceDefinition {
	fn to_sql_definition(&self) -> DefineNamespaceStatement {
		DefineNamespaceStatement {
			// SAFETY: we know the name is valid because it was validated when the namespace was
			// created.
			name: unsafe { Ident::new_unchecked(self.name.clone()) },
			comment: self.comment.clone().map(|v| v.into()),
			..Default::default()
		}
	}
}

impl ToSql for NamespaceDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
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
