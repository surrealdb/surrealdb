mod access;
mod analyzer;
mod api;
mod bucket;
pub mod config;
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
pub mod user;

pub(crate) use access::DefineAccessStatement;
pub(crate) use analyzer::DefineAnalyzerStatement;
pub(crate) use api::{ApiAction, DefineApiStatement};
pub(crate) use bucket::DefineBucketStatement;
pub(crate) use config::DefineConfigStatement;
pub(crate) use database::DefineDatabaseStatement;
pub(crate) use event::DefineEventStatement;
pub(crate) use field::{DefineDefault, DefineFieldStatement};
pub(crate) use function::DefineFunctionStatement;
pub(crate) use index::DefineIndexStatement;
pub(crate) use model::DefineModelStatement;
pub(crate) use module::DefineModuleStatement;
pub(crate) use namespace::DefineNamespaceStatement;
pub(crate) use param::DefineParamStatement;
pub(crate) use sequence::DefineSequenceStatement;
use surrealdb_types::{SqlFormat, ToSql};
pub(crate) use table::DefineTableStatement;
pub(crate) use user::DefineUserStatement;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum DefineKind {
	#[default]
	Default,
	Overwrite,
	IfNotExists,
}

impl From<crate::expr::statements::define::DefineKind> for DefineKind {
	fn from(value: crate::expr::statements::define::DefineKind) -> Self {
		match value {
			crate::expr::statements::define::DefineKind::Default => DefineKind::Default,
			crate::expr::statements::define::DefineKind::Overwrite => DefineKind::Overwrite,
			crate::expr::statements::define::DefineKind::IfNotExists => DefineKind::IfNotExists,
		}
	}
}

impl From<DefineKind> for crate::expr::statements::define::DefineKind {
	fn from(value: DefineKind) -> Self {
		match value {
			DefineKind::Default => crate::expr::statements::define::DefineKind::Default,
			DefineKind::Overwrite => crate::expr::statements::define::DefineKind::Overwrite,
			DefineKind::IfNotExists => crate::expr::statements::define::DefineKind::IfNotExists,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum DefineStatement {
	Namespace(DefineNamespaceStatement),
	Database(DefineDatabaseStatement),
	Function(DefineFunctionStatement),
	Analyzer(DefineAnalyzerStatement),
	Param(DefineParamStatement),
	Table(DefineTableStatement),
	Event(DefineEventStatement),
	Field(DefineFieldStatement),
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

impl From<DefineStatement> for crate::expr::statements::DefineStatement {
	fn from(v: DefineStatement) -> Self {
		match v {
			DefineStatement::Namespace(v) => Self::Namespace(v.into()),
			DefineStatement::Database(v) => Self::Database(v.into()),
			DefineStatement::Function(v) => Self::Function(v.into()),
			DefineStatement::Analyzer(v) => Self::Analyzer(v.into()),
			DefineStatement::Param(v) => Self::Param(v.into()),
			DefineStatement::Table(v) => Self::Table(v.into()),
			DefineStatement::Event(v) => Self::Event(v.into()),
			DefineStatement::Field(v) => Self::Field(v.into()),
			DefineStatement::Index(v) => Self::Index(v.into()),
			DefineStatement::User(v) => Self::User(v.into()),
			DefineStatement::Model(v) => Self::Model(v.into()),
			DefineStatement::Access(v) => Self::Access(v.into()),
			DefineStatement::Config(v) => Self::Config(v.into()),
			DefineStatement::Api(v) => Self::Api(v.into()),
			DefineStatement::Bucket(v) => Self::Bucket(v.into()),
			DefineStatement::Sequence(v) => Self::Sequence(v.into()),
			DefineStatement::Module(v) => Self::Module(v.into()),
		}
	}
}

impl From<crate::expr::statements::DefineStatement> for DefineStatement {
	fn from(v: crate::expr::statements::DefineStatement) -> Self {
		match v {
			crate::expr::statements::DefineStatement::Namespace(v) => Self::Namespace(v.into()),
			crate::expr::statements::DefineStatement::Database(v) => Self::Database(v.into()),
			crate::expr::statements::DefineStatement::Function(v) => Self::Function(v.into()),
			crate::expr::statements::DefineStatement::Analyzer(v) => Self::Analyzer(v.into()),
			crate::expr::statements::DefineStatement::Param(v) => Self::Param(v.into()),
			crate::expr::statements::DefineStatement::Table(v) => Self::Table(v.into()),
			crate::expr::statements::DefineStatement::Event(v) => Self::Event(v.into()),
			crate::expr::statements::DefineStatement::Field(v) => Self::Field(v.into()),
			crate::expr::statements::DefineStatement::Index(v) => Self::Index(v.into()),
			crate::expr::statements::DefineStatement::User(v) => Self::User(v.into()),
			crate::expr::statements::DefineStatement::Model(v) => Self::Model(v.into()),
			crate::expr::statements::DefineStatement::Access(v) => Self::Access(v.into()),
			crate::expr::statements::DefineStatement::Config(v) => Self::Config(v.into()),
			crate::expr::statements::DefineStatement::Api(v) => Self::Api(v.into()),
			crate::expr::statements::DefineStatement::Bucket(v) => Self::Bucket(v.into()),
			crate::expr::statements::DefineStatement::Sequence(v) => Self::Sequence(v.into()),
			crate::expr::statements::DefineStatement::Module(v) => Self::Module(v.into()),
		}
	}
}
