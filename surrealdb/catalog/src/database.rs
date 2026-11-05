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
use crate::stored::{SchemaError, Stored, StoredField, impl_stored_id};
use crate::val::Value;
use crate::val::stored::schema::Unrevisioned;
use crate::val::stored::{Encoder, Item};

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

	#[inline]
	fn skip_revisioned_slice(
		reader: &mut revision::SliceReader<'_>,
	) -> Result<(), revision::Error> {
		<u32 as SkipRevisioned>::skip_revisioned_slice(reader)
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

/// A database's compatibility level: how strictly its `SCHEMAFULL` tables hold their stored rows
/// to their field definitions.
///
/// At [`Self::LEVEL_3`] a table accepts any schema change, as the 3.x releases did. At
/// [`Self::LEVEL_4`] a table refuses a schema change its stored rows might not satisfy unless the
/// change carries `MIGRATE EXISTING ROWS` ([`Self::enforces_migrations`]). The level is stored as
/// the `u8` it holds, and 3 is the level a definition that records none has.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DatabaseCompatibility(u8);

impl DatabaseCompatibility {
	/// The level of a database written before databases held one: its tables accept any schema
	/// change.
	pub const LEVEL_3: Self = Self(3);

	/// The level at which a `SCHEMAFULL` table enforces migrations.
	pub const LEVEL_4: Self = Self(4);

	/// The level `level` names, or `None` for a number that is no compatibility level.
	pub const fn new(level: u8) -> Option<Self> {
		match level {
			3 | 4 => Some(Self(level)),
			_ => None,
		}
	}

	/// The level as a number.
	pub const fn get(self) -> u8 {
		self.0
	}

	/// Whether the `SCHEMAFULL` tables of a database at this level enforce migrations: a schema
	/// change their stored rows might not satisfy is refused unless it carries `MIGRATE EXISTING
	/// ROWS`, and a field's removal removes its stored values.
	pub const fn enforces_migrations(self) -> bool {
		self.0 >= Self::LEVEL_4.0
	}
}

impl Default for DatabaseCompatibility {
	fn default() -> Self {
		Self::LEVEL_3
	}
}

impl Display for DatabaseCompatibility {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(&self.0, f)
	}
}

/// A level is stored as the `u8` it holds. A stored number that is no level is refused as out of
/// range, so a reader never meets a level it does not know how to apply.
impl StoredField for DatabaseCompatibility {
	fn encode_item(&self, enc: &mut Encoder, prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		<u8 as StoredField>::encode_item(&self.0, enc, prior)
	}

	fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
		let level = <u8 as StoredField>::decode_item(item, depth)?;
		Self::new(level).ok_or_else(SchemaError::out_of_range)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		<u8 as StoredField>::into_vm_value(self.0)
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		let level = <u8 as StoredField>::from_vm_value(value)?;
		Self::new(level).ok_or_else(|| SchemaError::vm("int:u8", "a compatibility level of 3 or 4"))
	}

	fn write_type_name(out: &mut String) {
		<u8 as StoredField>::write_type_name(out);
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
	/// The database's compatibility level. The `revision` encoding leaves it out, so a
	/// definition written in it has the level a definition that records none has, and a
	/// definition at another level is written only in the stored grammar.
	#[stored(n = 12)]
	pub compatibility: Unrevisioned<DatabaseCompatibility>,
}
impl_kv_value_catalog!(DatabaseDefinition);

impl DatabaseDefinition {
	/// Whether the `SCHEMAFULL` tables of this database enforce migrations
	/// ([`DatabaseCompatibility::enforces_migrations`]).
	pub fn enforces_migrations(&self) -> bool {
		self.compatibility.enforces_migrations()
	}

	fn to_runtime_definition(&self) -> DefineDatabaseStatement {
		DefineDatabaseStatement {
			name: Expr::Idiom(Idiom::field(self.name.clone())),
			strict: self.strict,
			// Always written: a definition without the clause takes the store's default, which
			// another store need not share.
			compatibility: Some(self.compatibility.get()),
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
			"compatibility" => Value::from(i64::from(self.compatibility.get())),
			"id" => self.database_id.0.into(),
		})
	}
}
