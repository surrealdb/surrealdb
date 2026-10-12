use std::fmt::{Display, Formatter};

use revision::{
	DeserializeRevisioned, Revisioned, SerializeRevisioned, SkipRevisioned, revisioned,
};
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::NamespaceId;
use crate::expr::statements::define::DefineDatabaseStatement;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{ChangeFeed, Expr, Idiom, Literal};
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
pub struct DatabaseId(pub u32);

impl_kv_value_revisioned!(DatabaseId);
impl_stored_id!(DatabaseId);

impl Revisioned for DatabaseId {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for DatabaseId {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&self.0, writer)
	}
}

impl DeserializeRevisioned for DatabaseId {
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		DeserializeRevisioned::deserialize_revisioned(reader).map(DatabaseId)
	}
}

impl SkipRevisioned for DatabaseId {
	#[inline]
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		<u32 as SkipRevisioned>::skip_revisioned(reader)
	}
}

impl revision::WalkRevisioned for DatabaseId {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, DatabaseId, R>;

	#[inline]
	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
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
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct DatabaseDefinition {
	#[stored(n = 2)]
	pub namespace_id: NamespaceId,
	#[stored(n = 4)]
	pub database_id: DatabaseId,
	#[stored(n = 6)]
	pub name: Strand,
	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
	#[stored(n = 8)]
	pub changefeed: Option<ChangeFeed>,
	#[stored(n = 10)]
	pub strict: bool,
}
impl_kv_value_catalog!(DatabaseDefinition);

impl DatabaseDefinition {
	fn to_runtime_definition(&self) -> DefineDatabaseStatement {
		DefineDatabaseStatement {
			name: Expr::Idiom(Idiom::field(self.name.clone())),
			strict: self.strict,
			comment: self
				.comment
				.clone()
				.map(|v| Expr::Literal(Literal::String(v.into())))
				.unwrap_or(Expr::Literal(Literal::None)),
			changefeed: self.changefeed,
			..Default::default()
		}
	}
}

impl ToSql for DatabaseDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition().fmt_sql(f, fmt)
	}
}

impl InfoStructure for DatabaseDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.into(),
			"comment", if let Some(v) = self.comment => v.into(),
			"id" => self.database_id.0.into(),
		})
	}
}
