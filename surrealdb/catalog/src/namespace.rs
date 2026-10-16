use std::fmt::{Display, Formatter};

use revision::{
	DeserializeRevisioned, Revisioned, SerializeRevisioned, SkipRevisioned, revisioned,
};
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::statements::DefineNamespaceStatement;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Literal};
use crate::impl_kv_value_catalog;
use crate::stored::{Stored, impl_stored_id};
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
impl_stored_id!(NamespaceId);

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

	#[inline]
	fn skip_revisioned_slice(
		reader: &mut revision::SliceReader<'_>,
	) -> Result<(), revision::Error> {
		<u32 as SkipRevisioned>::skip_revisioned_slice(reader)
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
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash, Stored)]
#[stored(since = "4.0")]
pub struct NamespaceDefinition {
	#[stored(n = 2)]
	pub namespace_id: NamespaceId,
	#[stored(n = 4)]
	pub name: Strand,
	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
}
impl_kv_value_catalog!(NamespaceDefinition);

impl NamespaceDefinition {
	fn to_runtime_definition(&self) -> DefineNamespaceStatement {
		DefineNamespaceStatement {
			name: crate::expr::Expr::Idiom(crate::expr::Idiom::field(self.name.clone())),
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
		self.to_runtime_definition().fmt_sql(f, fmt)
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
