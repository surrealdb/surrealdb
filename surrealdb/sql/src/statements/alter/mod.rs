pub mod access;
pub mod analyzer;
pub mod api;
pub mod bucket;
pub mod config;
pub mod event;
pub mod field;
pub mod function;
pub mod module;
use surrealdb_types::{SqlFormat, ToSql};
pub mod database;
pub mod index;
pub mod param;
pub mod sequence;
pub mod user;

pub mod namespace;
pub mod system;
pub mod table;

pub use access::AlterAccessStatement;
pub use analyzer::AlterAnalyzerStatement;
pub use api::{AlterApiClause, AlterApiStatement};
pub use bucket::AlterBucketStatement;
pub use config::AlterConfigStatement;
pub use database::AlterDatabaseStatement;
pub use event::AlterEventStatement;
pub use field::AlterFieldStatement;
pub use function::AlterFunctionStatement;
pub use index::AlterIndexStatement;
pub use module::AlterModuleStatement;
pub use namespace::AlterNamespaceStatement;
pub use param::AlterParamStatement;
pub use sequence::AlterSequenceStatement;
pub use system::AlterSystemStatement;
pub use table::AlterTableStatement;
pub use user::AlterUserStatement;

#[derive(Clone, Debug, Eq, PartialEq, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// Tri‑state alteration helper used across `ALTER` AST nodes.
///
/// - `None`: leave the current value unchanged
/// - `Set(T)`: set/replace the current value to `T`
/// - `Drop`: remove/clear the current value
pub enum AlterKind<T> {
	#[default]
	None,
	Set(T),
	Drop,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// SQL AST for `ALTER` statements. Variants mirror specific resources.
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
