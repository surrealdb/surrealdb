use anyhow::Result;
use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::val::Value;

mod access;
mod analyzer;
mod api;
mod bucket;
mod config;
mod database;
mod event;
mod field;
mod function;
mod index;
mod module;
mod namespace;
mod param;
mod sequence;
mod system;
mod table;
mod user;

pub(crate) use access::AlterAccessStatement;
pub(crate) use analyzer::AlterAnalyzerStatement;
pub(crate) use api::{AlterApiClause, AlterApiStatement};
pub(crate) use bucket::AlterBucketStatement;
pub(crate) use config::AlterConfigStatement;
pub(crate) use database::AlterDatabaseStatement;
pub(crate) use event::AlterEventStatement;
pub(crate) use field::{AlterDefault, AlterFieldStatement};
pub(crate) use function::AlterFunctionStatement;
pub(crate) use index::AlterIndexStatement;
pub(crate) use module::AlterModuleStatement;
pub(crate) use namespace::AlterNamespaceStatement;
pub(crate) use param::AlterParamStatement;
pub(crate) use sequence::AlterSequenceStatement;
pub(crate) use system::AlterSystemStatement;
pub(crate) use table::AlterTableStatement;
pub(crate) use user::AlterUserStatement;
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
	Api(AlterApiStatement),
	Event(AlterEventStatement),
	Index(AlterIndexStatement),
	Sequence(AlterSequenceStatement),
	Field(AlterFieldStatement),
	Param(AlterParamStatement),
	Bucket(AlterBucketStatement),
	Config(AlterConfigStatement),
	Analyzer(AlterAnalyzerStatement),
	Function(AlterFunctionStatement),
	User(AlterUserStatement),
	Access(AlterAccessStatement),
	Module(AlterModuleStatement),
}

impl AlterStatement {
	/// Executes this statement, returning a simple value.
	///
	/// All `ALTER` statements currently return `Value::None` on success and may
	/// perform side effects such as storage compaction or metadata updates.
	#[instrument(level = "trace", name = "AlterStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		match self {
			Self::System(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Namespace(v) => v.compute(ctx, opt).await,
			Self::Database(v) => v.compute(ctx, opt).await,
			Self::Table(v) => v.compute(ctx, opt).await,
			Self::Api(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Event(v) => v.compute(ctx, opt).await,
			Self::Index(v) => v.compute(ctx, opt).await,
			Self::Sequence(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Field(v) => v.compute(ctx, opt).await,
			Self::Param(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Bucket(v) => v.compute(ctx, opt).await,
			Self::Config(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Analyzer(v) => v.compute(ctx, opt).await,
			Self::Function(v) => v.compute(ctx, opt).await,
			Self::User(v) => v.compute(ctx, opt).await,
			Self::Access(v) => v.compute(ctx, opt).await,
			Self::Module(v) => v.compute(ctx, opt).await,
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
			Self::Api(v) => v.fmt_sql(f, fmt),
			Self::Event(v) => v.fmt_sql(f, fmt),
			Self::Index(v) => v.fmt_sql(f, fmt),
			Self::Sequence(v) => v.fmt_sql(f, fmt),
			Self::Field(v) => v.fmt_sql(f, fmt),
			Self::Param(v) => v.fmt_sql(f, fmt),
			Self::Bucket(v) => v.fmt_sql(f, fmt),
			Self::Config(v) => v.fmt_sql(f, fmt),
			Self::Analyzer(v) => v.fmt_sql(f, fmt),
			Self::Function(v) => v.fmt_sql(f, fmt),
			Self::User(v) => v.fmt_sql(f, fmt),
			Self::Access(v) => v.fmt_sql(f, fmt),
			Self::Module(v) => v.fmt_sql(f, fmt),
		}
	}
}
