use std::fmt::{Display, Formatter};

use revision::{
	DeserializeRevisioned, Revisioned, SerializeRevisioned, SkipRevisioned, revisioned,
};
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::DefineNamespaceStatement;
use crate::sql::{Expr, Literal};
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
#[repr(transparent)]
pub struct NamespaceId(pub u32);

impl_kv_value_revisioned!(NamespaceId);

impl Revisioned for NamespaceId {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for NamespaceId {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&self.0, writer)
	}
}

impl DeserializeRevisioned for NamespaceId {
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		DeserializeRevisioned::deserialize_revisioned(reader).map(NamespaceId)
	}
}

impl SkipRevisioned for NamespaceId {
	#[inline]
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		<u32 as SkipRevisioned>::skip_revisioned(reader)
	}
}

impl revision::WalkRevisioned for NamespaceId {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, NamespaceId, R>;

	#[inline]
	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
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
	pub name: Strand,
	pub comment: Option<String>,
}
impl_kv_value_revisioned!(NamespaceDefinition);

impl NamespaceDefinition {
	fn to_sql_definition(&self) -> DefineNamespaceStatement {
		DefineNamespaceStatement {
			name: crate::sql::Expr::Idiom(crate::sql::Idiom::field(self.name.clone())),
			comment: self
				.comment
				.clone()
				.map(|v| Expr::Literal(Literal::String(v.into())))
				.unwrap_or(Expr::Literal(Literal::None)),
			..Default::default()
		}
	}
}

impl ToSql for NamespaceDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

impl InfoStructure for NamespaceDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.into(),
			"comment", if let Some(v) = self.comment => v.into(),
			"id" => self.namespace_id.0.into(),
		})
	}
}
