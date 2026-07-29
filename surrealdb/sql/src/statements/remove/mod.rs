pub mod access;
use surrealdb_types::{SqlFormat, ToSql};
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

pub use access::RemoveAccessStatement;
pub use analyzer::RemoveAnalyzerStatement;
pub use api::RemoveApiStatement;
pub use bucket::RemoveBucketStatement;
pub use config::{RemoveConfigKind, RemoveConfigStatement};
pub use database::RemoveDatabaseStatement;
pub use event::RemoveEventStatement;
pub use field::RemoveFieldStatement;
pub use function::RemoveFunctionStatement;
pub use index::RemoveIndexStatement;
pub use model::RemoveModelStatement;
pub use module::RemoveModuleStatement;
pub use namespace::RemoveNamespaceStatement;
pub use param::RemoveParamStatement;
pub use sequence::RemoveSequenceStatement;
pub use table::RemoveTableStatement;
pub use user::RemoveUserStatement;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum RemoveStatement {
	Namespace(RemoveNamespaceStatement),
	Database(RemoveDatabaseStatement),
	Function(RemoveFunctionStatement),
	Analyzer(RemoveAnalyzerStatement),
	Access(RemoveAccessStatement),
	Param(RemoveParamStatement),
	Table(RemoveTableStatement),
	Event(RemoveEventStatement),
	Field(RemoveFieldStatement),
	Index(RemoveIndexStatement),
	User(RemoveUserStatement),
	#[cfg_attr(feature = "arbitrary", arbitrary(skip))]
	Model(RemoveModelStatement),
	Api(RemoveApiStatement),
	Bucket(RemoveBucketStatement),
	Sequence(RemoveSequenceStatement),
	Module(RemoveModuleStatement),
	Config(RemoveConfigStatement),
}

impl ToSql for RemoveStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Namespace(v) => v.fmt_sql(f, fmt),
			Self::Database(v) => v.fmt_sql(f, fmt),
			Self::Function(v) => v.fmt_sql(f, fmt),
			Self::Access(v) => v.fmt_sql(f, fmt),
			Self::Param(v) => v.fmt_sql(f, fmt),
			Self::Table(v) => v.fmt_sql(f, fmt),
			Self::Event(v) => v.fmt_sql(f, fmt),
			Self::Field(v) => v.fmt_sql(f, fmt),
			Self::Index(v) => v.fmt_sql(f, fmt),
			Self::Analyzer(v) => v.fmt_sql(f, fmt),
			Self::User(v) => v.fmt_sql(f, fmt),
			Self::Model(v) => v.fmt_sql(f, fmt),
			Self::Api(v) => v.fmt_sql(f, fmt),
			Self::Bucket(v) => v.fmt_sql(f, fmt),
			Self::Sequence(v) => v.fmt_sql(f, fmt),
			Self::Module(v) => v.fmt_sql(f, fmt),
			Self::Config(v) => v.fmt_sql(f, fmt),
		}
	}
}
