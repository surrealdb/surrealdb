use anyhow::Result;
use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::val::Value;

mod database;
mod field;
mod index;
mod namespace;
mod sequence;
mod system;
mod table;

pub(crate) use database::AlterDatabaseStatement;
pub(crate) use field::{AlterDefault, AlterFieldStatement};
pub(crate) use index::AlterIndexStatement;
pub(crate) use namespace::AlterNamespaceStatement;
pub(crate) use sequence::AlterSequenceStatement;
pub(crate) use system::AlterSystemStatement;
pub(crate) use table::AlterTableStatement;
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
/// Helper to express a tri‑state alteration:
/// - `None`: leave the current value unchanged
/// - `Set(T)`: set/replace the current value to `T`
/// - `Drop`: remove/clear the current value
pub(crate) enum AlterKind<T> {
	#[default]
	None,
	Set(T),
	Drop,
}

impl<T: Revisioned> Revisioned for AlterKind<T> {
	fn revision() -> u16 {
		1
	}
}

impl<T: Revisioned + SerializeRevisioned> SerializeRevisioned for AlterKind<T> {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		w: &mut W,
	) -> std::result::Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&Self::revision(), w)?;
		match self {
			AlterKind::None => SerializeRevisioned::serialize_revisioned(&0u32, w)?,
			AlterKind::Set(x) => {
				SerializeRevisioned::serialize_revisioned(&1u32, w)?;
				SerializeRevisioned::serialize_revisioned(x, w)?;
			}
			AlterKind::Drop => {
				SerializeRevisioned::serialize_revisioned(&2u32, w)?;
			}
		}
		Ok(())
	}
}

impl<T: Revisioned + DeserializeRevisioned> DeserializeRevisioned for AlterKind<T> {
	fn deserialize_revisioned<R: std::io::Read>(
		r: &mut R,
	) -> std::result::Result<Self, revision::Error>
	where
		Self: Sized,
	{
		match DeserializeRevisioned::deserialize_revisioned(r)? {
			1u16 => {
				let variant: u32 = DeserializeRevisioned::deserialize_revisioned(r)?;
				match variant {
					0 => Ok(AlterKind::None),
					1 => Ok(AlterKind::Set(DeserializeRevisioned::deserialize_revisioned(r)?)),
					2 => Ok(AlterKind::Drop),
					x => Err(revision::Error::Deserialize(format!(
						"Unknown variant `{x}` for AlterKind"
					))),
				}
			}
			x => Err(revision::Error::Deserialize(format!("Unknown revision `{x}` for AlterKind"))),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
/// Execution‑time representation of all `ALTER` statements.
///
/// Variants map to specific resources and delegate execution to their
/// corresponding implementations.
pub(crate) enum AlterStatement {
	System(AlterSystemStatement),
	Namespace(AlterNamespaceStatement),
	Database(AlterDatabaseStatement),
	Table(AlterTableStatement),
	Index(AlterIndexStatement),
	Sequence(AlterSequenceStatement),
	Field(AlterFieldStatement),
}

impl AlterStatement {
	/// Executes this statement, returning a simple value.
	///
	/// All `ALTER` statements currently return `Value::None` on success and may
	/// perform side effects such as storage compaction or metadata updates.
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		match self {
			Self::System(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Namespace(v) => v.compute(ctx, opt).await,
			Self::Database(v) => v.compute(ctx, opt).await,
			Self::Table(v) => v.compute(ctx, opt).await,
			Self::Index(v) => v.compute(ctx, opt).await,
			Self::Sequence(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Field(v) => v.compute(ctx, opt).await,
		}
	}
}

impl ToSql for AlterStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::System(v) => v.fmt_sql(f, fmt),
			Self::Namespace(v) => v.fmt_sql(f, fmt),
			Self::Database(v) => v.fmt_sql(f, fmt),
			Self::Table(v) => v.fmt_sql(f, fmt),
			Self::Index(v) => v.fmt_sql(f, fmt),
			Self::Sequence(v) => v.fmt_sql(f, fmt),
			Self::Field(v) => v.fmt_sql(f, fmt),
		}
	}
}
