use std::fmt::{Display, Formatter};

use revision::{revisioned, Revisioned};
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};

use crate::catalog::NamespaceId;
use crate::expr::statements::info::InfoStructure;
use crate::expr::ChangeFeed;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineDatabaseStatement;
use crate::sql::ToSql;
use crate::val::Value;

#[derive(
	Debug,
	Clone,
	Copy,
	PartialEq,
	Eq,
	PartialOrd,
	Ord,
	Hash,
	Serialize,
	Deserialize,
	Encode,
	BorrowDecode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl From<u32> for DatabaseId {
	fn from(value: u32) -> Self {
		Self(value)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DatabaseDefinition {
	pub namespace_id: NamespaceId,
	pub database_id: DatabaseId,
	pub name: String,
	pub comment: Option<String>,
	pub changefeed: Option<ChangeFeed>,
}
impl_kv_value_revisioned!(DatabaseDefinition);

impl DatabaseDefinition {
	pub fn to_sql_definition(&self) -> DefineDatabaseStatement {
		DefineDatabaseStatement {
			name: self.name.clone(),
			comment: self.comment.clone(),
			changefeed: self.changefeed.map(|x| x.into()),

			..Default::default()
		}
	}
}

impl ToSql for DatabaseDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
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
