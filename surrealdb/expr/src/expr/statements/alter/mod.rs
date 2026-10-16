use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use surrealdb_types::{SqlFormat, ToSql};

pub mod access;
pub mod analyzer;
pub mod api;
pub mod bucket;
pub mod config;
pub mod database;
pub mod event;
pub mod field;
pub mod function;
pub mod index;
pub mod module;
pub mod namespace;
pub mod param;
pub mod sequence;
pub mod system;
pub mod table;
pub mod user;

pub use access::AlterAccessStatement;
pub use analyzer::AlterAnalyzerStatement;
pub use api::{AlterApiClause, AlterApiStatement};
pub use bucket::AlterBucketStatement;
pub use config::AlterConfigStatement;
pub use database::AlterDatabaseStatement;
pub use event::AlterEventStatement;
pub use field::{AlterDefault, AlterFieldStatement};
pub use function::AlterFunctionStatement;
pub use index::AlterIndexStatement;
pub use module::AlterModuleStatement;
pub use namespace::AlterNamespaceStatement;
pub use param::AlterParamStatement;
pub use sequence::AlterSequenceStatement;
pub use system::AlterSystemStatement;
pub use table::AlterTableStatement;
pub use user::AlterUserStatement;
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
/// Helper to express a tri‑state alteration:
/// - `None`: leave the current value unchanged
/// - `Set(T)`: set/replace the current value to `T`
/// - `Drop`: remove/clear the current value
pub enum AlterKind<T> {
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

#[derive(Debug, Eq, PartialEq, Hash)]
/// Execution‑time representation of all `ALTER` statements.
///
/// Variants map to specific resources and delegate execution to their
/// corresponding implementations.
pub enum AlterStatement {
	System(AlterSystemStatement),
	Namespace(AlterNamespaceStatement),
	Database(AlterDatabaseStatement),
	Table(AlterTableStatement),
	Api(AlterApiStatement),
	Event(AlterEventStatement),
	Index(AlterIndexStatement),
	Sequence(AlterSequenceStatement),
	Field(Box<AlterFieldStatement>),
	Param(AlterParamStatement),
	Bucket(AlterBucketStatement),
	Config(AlterConfigStatement),
	Analyzer(AlterAnalyzerStatement),
	Function(AlterFunctionStatement),
	User(AlterUserStatement),
	Access(AlterAccessStatement),
	Module(AlterModuleStatement),
}

/// Written out and kept out of line so one copy serves every crate that clones an `AlterStatement`.
impl Clone for AlterStatement {
	#[inline(never)]
	fn clone(&self) -> Self {
		match self {
			Self::System(v) => Self::System(v.clone()),
			Self::Namespace(v) => Self::Namespace(v.clone()),
			Self::Database(v) => Self::Database(v.clone()),
			Self::Table(v) => Self::Table(v.clone()),
			Self::Api(v) => Self::Api(v.clone()),
			Self::Event(v) => Self::Event(v.clone()),
			Self::Index(v) => Self::Index(v.clone()),
			Self::Sequence(v) => Self::Sequence(v.clone()),
			Self::Field(v) => Self::Field(v.clone()),
			Self::Param(v) => Self::Param(v.clone()),
			Self::Bucket(v) => Self::Bucket(v.clone()),
			Self::Config(v) => Self::Config(v.clone()),
			Self::Analyzer(v) => Self::Analyzer(v.clone()),
			Self::Function(v) => Self::Function(v.clone()),
			Self::User(v) => Self::User(v.clone()),
			Self::Access(v) => Self::Access(v.clone()),
			Self::Module(v) => Self::Module(v.clone()),
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
