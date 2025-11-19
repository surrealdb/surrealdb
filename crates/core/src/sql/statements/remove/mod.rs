mod access;
use surrealdb_types::{SqlFormat, ToSql};
mod analyzer;
mod api;
mod bucket;
mod database;
mod event;
mod field;
mod function;
mod index;
mod model;
mod module;
mod namespace;
mod param;
mod sequence;
mod table;
mod user;

pub(crate) use access::RemoveAccessStatement;
pub(crate) use analyzer::RemoveAnalyzerStatement;
pub(crate) use api::RemoveApiStatement;
pub(crate) use bucket::RemoveBucketStatement;
pub(crate) use database::RemoveDatabaseStatement;
pub(crate) use event::RemoveEventStatement;
pub(crate) use field::RemoveFieldStatement;
pub(crate) use function::RemoveFunctionStatement;
pub(crate) use index::RemoveIndexStatement;
pub(crate) use model::RemoveModelStatement;
pub(crate) use module::RemoveModuleStatement;
pub(crate) use namespace::RemoveNamespaceStatement;
pub(crate) use param::RemoveParamStatement;
pub(crate) use sequence::RemoveSequenceStatement;
pub(crate) use table::RemoveTableStatement;
pub(crate) use user::RemoveUserStatement;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum RemoveStatement {
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
	Model(RemoveModelStatement),
	Api(RemoveApiStatement),
	Bucket(RemoveBucketStatement),
	Sequence(RemoveSequenceStatement),
	Module(RemoveModuleStatement),
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
		}
	}
}

impl From<RemoveStatement> for crate::expr::statements::RemoveStatement {
	fn from(v: RemoveStatement) -> Self {
		match v {
			RemoveStatement::Namespace(v) => Self::Namespace(v.into()),
			RemoveStatement::Database(v) => Self::Database(v.into()),
			RemoveStatement::Function(v) => Self::Function(v.into()),
			RemoveStatement::Analyzer(v) => Self::Analyzer(v.into()),
			RemoveStatement::Access(v) => Self::Access(v.into()),
			RemoveStatement::Param(v) => Self::Param(v.into()),
			RemoveStatement::Table(v) => Self::Table(v.into()),
			RemoveStatement::Event(v) => Self::Event(v.into()),
			RemoveStatement::Field(v) => Self::Field(v.into()),
			RemoveStatement::Index(v) => Self::Index(v.into()),
			RemoveStatement::User(v) => Self::User(v.into()),
			RemoveStatement::Model(v) => Self::Model(v.into()),
			RemoveStatement::Api(v) => Self::Api(v.into()),
			RemoveStatement::Bucket(v) => Self::Bucket(v.into()),
			RemoveStatement::Sequence(v) => Self::Sequence(v.into()),
			RemoveStatement::Module(v) => Self::Module(v.into()),
		}
	}
}

impl From<crate::expr::statements::RemoveStatement> for RemoveStatement {
	fn from(v: crate::expr::statements::RemoveStatement) -> Self {
		match v {
			crate::expr::statements::RemoveStatement::Namespace(v) => Self::Namespace(v.into()),
			crate::expr::statements::RemoveStatement::Database(v) => Self::Database(v.into()),
			crate::expr::statements::RemoveStatement::Function(v) => Self::Function(v.into()),
			crate::expr::statements::RemoveStatement::Analyzer(v) => Self::Analyzer(v.into()),
			crate::expr::statements::RemoveStatement::Access(v) => Self::Access(v.into()),
			crate::expr::statements::RemoveStatement::Param(v) => Self::Param(v.into()),
			crate::expr::statements::RemoveStatement::Table(v) => Self::Table(v.into()),
			crate::expr::statements::RemoveStatement::Event(v) => Self::Event(v.into()),
			crate::expr::statements::RemoveStatement::Field(v) => Self::Field(v.into()),
			crate::expr::statements::RemoveStatement::Index(v) => Self::Index(v.into()),
			crate::expr::statements::RemoveStatement::User(v) => Self::User(v.into()),
			crate::expr::statements::RemoveStatement::Model(v) => Self::Model(v.into()),
			crate::expr::statements::RemoveStatement::Api(v) => Self::Api(v.into()),
			crate::expr::statements::RemoveStatement::Bucket(v) => Self::Bucket(v.into()),
			crate::expr::statements::RemoveStatement::Sequence(v) => Self::Sequence(v.into()),
			crate::expr::statements::RemoveStatement::Module(v) => Self::Module(v.into()),
		}
	}
}
