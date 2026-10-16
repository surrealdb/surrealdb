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
pub mod model;
pub mod module;
pub mod namespace;
pub mod param;
pub mod sequence;
pub mod table;
pub mod user;

pub use access::DefineAccessStatement;
pub use analyzer::DefineAnalyzerStatement;
pub use api::{ApiAction, DefineApiStatement};
pub use bucket::DefineBucketStatement;
pub use config::DefineConfigStatement;
pub use database::DefineDatabaseStatement;
pub use event::DefineEventStatement;
pub use field::{DefineDefault, DefineFieldStatement, MigrateClause, kind_contains_object};
pub use function::DefineFunctionStatement;
pub use index::DefineIndexStatement;
pub use model::DefineModelStatement;
pub use module::DefineModuleStatement;
pub use namespace::DefineNamespaceStatement;
pub use param::DefineParamStatement;
pub use sequence::DefineSequenceStatement;
pub use table::DefineTableStatement;
pub use user::DefineUserStatement;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub enum DefineKind {
	#[default]
	Default,
	Overwrite,
	IfNotExists,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum DefineStatement {
	Namespace(DefineNamespaceStatement),
	Database(DefineDatabaseStatement),
	Function(DefineFunctionStatement),
	Analyzer(DefineAnalyzerStatement),
	Param(DefineParamStatement),
	Table(DefineTableStatement),
	Event(DefineEventStatement),
	/// Boxed because this is the largest variant; keeping it out of line
	/// bounds every `DefineStatement` allocation.
	Field(Box<DefineFieldStatement>),
	Index(DefineIndexStatement),
	User(DefineUserStatement),
	Model(DefineModelStatement),
	Access(DefineAccessStatement),
	Config(DefineConfigStatement),
	Api(DefineApiStatement),
	Bucket(DefineBucketStatement),
	Sequence(DefineSequenceStatement),
	Module(DefineModuleStatement),
}

/// Written out and kept out of line so one copy serves every crate that clones a `DefineStatement`.
impl Clone for DefineStatement {
	#[inline(never)]
	fn clone(&self) -> Self {
		match self {
			Self::Namespace(v) => Self::Namespace(v.clone()),
			Self::Database(v) => Self::Database(v.clone()),
			Self::Function(v) => Self::Function(v.clone()),
			Self::Analyzer(v) => Self::Analyzer(v.clone()),
			Self::Param(v) => Self::Param(v.clone()),
			Self::Table(v) => Self::Table(v.clone()),
			Self::Event(v) => Self::Event(v.clone()),
			Self::Field(v) => Self::Field(v.clone()),
			Self::Index(v) => Self::Index(v.clone()),
			Self::User(v) => Self::User(v.clone()),
			Self::Model(v) => Self::Model(v.clone()),
			Self::Access(v) => Self::Access(v.clone()),
			Self::Config(v) => Self::Config(v.clone()),
			Self::Api(v) => Self::Api(v.clone()),
			Self::Bucket(v) => Self::Bucket(v.clone()),
			Self::Sequence(v) => Self::Sequence(v.clone()),
			Self::Module(v) => Self::Module(v.clone()),
		}
	}
}

impl ToSql for DefineStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Namespace(v) => v.fmt_sql(f, fmt),
			Self::Database(v) => v.fmt_sql(f, fmt),
			Self::Function(v) => v.fmt_sql(f, fmt),
			Self::User(v) => v.fmt_sql(f, fmt),
			Self::Param(v) => v.fmt_sql(f, fmt),
			Self::Table(v) => v.fmt_sql(f, fmt),
			Self::Event(v) => v.fmt_sql(f, fmt),
			Self::Field(v) => v.fmt_sql(f, fmt),
			Self::Index(v) => v.fmt_sql(f, fmt),
			Self::Analyzer(v) => v.fmt_sql(f, fmt),
			Self::Model(v) => v.fmt_sql(f, fmt),
			Self::Access(v) => v.fmt_sql(f, fmt),
			Self::Config(v) => v.fmt_sql(f, fmt),
			Self::Api(v) => v.fmt_sql(f, fmt),
			Self::Bucket(v) => v.fmt_sql(f, fmt),
			Self::Sequence(v) => v.fmt_sql(f, fmt),
			Self::Module(v) => v.fmt_sql(f, fmt),
		}
	}
}
