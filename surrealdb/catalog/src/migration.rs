//! Field migrations: the step a generation's descriptor carries, and the permanent record of a
//! named migration a table has run.
//!
//! `MIGRATE EXISTING ROWS` mints a generation whose [`LayoutDescriptor`](crate::LayoutDescriptor)
//! holds one [`Migration`]. A row stamped with an older generation of the table's ring passes
//! through the step of every later generation, oldest first, when it is read, so the step is
//! part of the generation's immutable definition and is retired with it when the ring's floor
//! moves past it.
//!
//! A step is self-contained. It holds the path it changes and everything a replayed value must
//! pass, as the catalog stood when the step was minted, so replaying it never consults the
//! current catalog.
//!
//! A migration's name outlives its descriptor. [`MigrationRun`] is the table's permanent record
//! that the name has run, so restating the statement after the generation is retired does not
//! run it again.

use surrealdb_kvs::value::KVValue;
use surrealdb_kvs::{EncodeContext, Encoded};

use crate::generation::Generation;
use crate::stored::{EntryError, Stored, StoredType, canonical, decode_entry, encode_stored};
use crate::val::Value;
use crate::{Error, ExprText, IdiomText, KindText};

/// The migration a generation carries: a name, unique per table, and what the migration does to
/// a row stamped before it.
#[derive(Clone, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0")]
pub struct Migration {
	/// The name the migration was written under. A migration restated under a name the table has
	/// run is not run again.
	#[stored(n = 2)]
	pub name: String,
	/// What the migration does to a row older than its generation.
	#[stored(n = 4)]
	pub step: MigrationStep,
}

impl Migration {
	/// The field the migration changes, as the path it was written under. `None` for a step that
	/// changes no single field.
	pub fn field(&self) -> Option<&str> {
		match &self.step {
			MigrationStep::Rewrite {
				path,
				..
			}
			| MigrationStep::Remove {
				path,
			} => Some(path.as_str()),
			MigrationStep::RemoveUndeclared {
				..
			} => None,
		}
	}
}

/// What a migration does to a row older than it.
#[derive(Clone, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0")]
pub enum MigrationStep {
	/// Rewrite the value at `path` through `using`, falling back to `fallback` when the result
	/// fails `test`.
	#[stored(n = 1)]
	Rewrite {
		/// The field path the step changes.
		#[stored(n = 2)]
		path: IdiomText,
		/// The transform, over `$value`.
		#[stored(n = 4)]
		using: ExprText,
		/// The value stored when the transform fails or its result fails `test`; checked against
		/// `test` when the migration was defined. `None` makes such a row's read fail.
		#[stored(n = 6)]
		fallback: Option<Value>,
		/// What a rewritten value must pass.
		#[stored(n = 8)]
		test: MigrationTest,
	},
	/// Remove the value at `path`.
	#[stored(n = 2)]
	Remove {
		/// The field path the step removes.
		#[stored(n = 2)]
		path: IdiomText,
	},
	/// Remove every top-level field the table did not declare when the step was minted
	/// (`REMOVE UNDECLARED FIELDS`), other than the record's own keys `id`, `in` and `out`.
	#[stored(n = 3)]
	RemoveUndeclared {
		/// The top-level field names declared when the step was minted.
		#[stored(n = 2)]
		declared: Vec<String>,
	},
}

/// The whole test a rewritten value must pass, as the catalog stood when its step was minted.
#[derive(Clone, Debug, Default, PartialEq, Eq, Stored)]
#[stored(since = "4.0")]
pub struct MigrationTest {
	/// The field's `TYPE`, with the definitions beneath it folded in.
	#[stored(n = 2)]
	pub kind: Option<KindText>,
	/// Whether the table was `SCHEMAFULL`.
	#[stored(n = 4)]
	pub schemafull: bool,
	/// Whether the field was `FLEXIBLE`.
	#[stored(n = 6)]
	pub flexible: bool,
	/// The field's `ASSERT`, over `$value`, when the step applies it.
	#[stored(n = 8)]
	pub assert: Option<ExprText>,
}

/// The permanent record that a table ran the migration whose name keys it.
///
/// It is written by the transaction that mints the migration's generation and read only to
/// refuse a second run of the name and to list the migrations a table has run.
#[derive(Clone, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0")]
pub struct MigrationRun {
	/// The generation the migration minted. The generation's descriptor is gone once the table's
	/// floor has moved past it.
	#[stored(n = 2)]
	pub generation: Generation,
	/// The field the migration changed, as the path it was written under; absent for a
	/// migration that changes no single field.
	#[stored(n = 4)]
	pub field: Option<String>,
}

/// Written and read at the store's feature level.
impl KVValue for MigrationRun {
	type KeyContext = ();

	const LEVELLED: bool = true;

	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		Err(Error::Entry(EntryError::Unlevelled {
			ty: <Self as StoredType>::SCHEMA.name,
		})
		.into())
	}

	fn kv_encode_value_at(&self, ctx: &EncodeContext<'_>) -> anyhow::Result<Encoded> {
		Ok(encode_stored(self, ctx).map_err(Error::Entry)?)
	}

	fn kv_canonical(&self) -> anyhow::Result<Vec<u8>> {
		Ok(canonical(self).map_err(Error::Entry)?)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(decode_entry::<Self>(bytes).map_err(Error::Entry)?)
	}
}
