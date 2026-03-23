mod access;
mod analyzer;
mod api;
mod bucket;
mod config;
mod event;
pub mod field;
mod function;
mod module;
use surrealdb_types::{SqlFormat, ToSql};
mod database;
mod index;
mod param;
mod sequence;
mod user;

mod namespace;
mod system;
mod table;

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

impl<A, B> From<AlterKind<A>> for crate::expr::statements::alter::AlterKind<B>
where
	B: From<A>,
{
	fn from(value: AlterKind<A>) -> Self {
		match value {
			AlterKind::Set(a) => crate::expr::statements::alter::AlterKind::Set(a.into()),
			AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
			AlterKind::None => crate::expr::statements::alter::AlterKind::None,
		}
	}
}

impl<A, B> From<crate::expr::statements::alter::AlterKind<A>> for AlterKind<B>
where
	B: From<A>,
{
	fn from(value: crate::expr::statements::alter::AlterKind<A>) -> Self {
		match value {
			crate::expr::statements::alter::AlterKind::Set(a) => AlterKind::Set(a.into()),
			crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
			crate::expr::statements::alter::AlterKind::None => AlterKind::None,
		}
	}
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

impl From<AlterStatement> for crate::expr::statements::AlterStatement {
	fn from(v: AlterStatement) -> Self {
		match v {
			AlterStatement::System(v) => Self::System(v.into()),
			AlterStatement::Namespace(v) => Self::Namespace(v.into()),
			AlterStatement::Database(v) => Self::Database(v.into()),
			AlterStatement::Table(v) => Self::Table(v.into()),
			AlterStatement::Api(v) => Self::Api(v.into()),
			AlterStatement::Event(v) => Self::Event(v.into()),
			AlterStatement::Index(v) => Self::Index(v.into()),
			AlterStatement::Sequence(v) => Self::Sequence(v.into()),
			AlterStatement::Field(v) => Self::Field(v.into()),
			AlterStatement::Param(v) => Self::Param(v.into()),
			AlterStatement::Bucket(v) => Self::Bucket(v.into()),
			AlterStatement::Config(v) => Self::Config(v.into()),
			AlterStatement::Analyzer(v) => Self::Analyzer(v.into()),
			AlterStatement::Function(v) => Self::Function(v.into()),
			AlterStatement::User(v) => Self::User(v.into()),
			AlterStatement::Access(v) => Self::Access(v.into()),
			AlterStatement::Module(v) => Self::Module(v.into()),
		}
	}
}

impl From<crate::expr::statements::AlterStatement> for AlterStatement {
	fn from(v: crate::expr::statements::AlterStatement) -> Self {
		match v {
			crate::expr::statements::AlterStatement::System(v) => Self::System(v.into()),
			crate::expr::statements::AlterStatement::Namespace(v) => Self::Namespace(v.into()),
			crate::expr::statements::AlterStatement::Database(v) => Self::Database(v.into()),
			crate::expr::statements::AlterStatement::Table(v) => Self::Table(v.into()),
			crate::expr::statements::AlterStatement::Api(v) => Self::Api(v.into()),
			crate::expr::statements::AlterStatement::Event(v) => Self::Event(v.into()),
			crate::expr::statements::AlterStatement::Index(v) => Self::Index(v.into()),
			crate::expr::statements::AlterStatement::Sequence(v) => Self::Sequence(v.into()),
			crate::expr::statements::AlterStatement::Field(v) => Self::Field(v.into()),
			crate::expr::statements::AlterStatement::Param(v) => Self::Param(v.into()),
			crate::expr::statements::AlterStatement::Bucket(v) => Self::Bucket(v.into()),
			crate::expr::statements::AlterStatement::Config(v) => Self::Config(v.into()),
			crate::expr::statements::AlterStatement::Analyzer(v) => Self::Analyzer(v.into()),
			crate::expr::statements::AlterStatement::Function(v) => Self::Function(v.into()),
			crate::expr::statements::AlterStatement::User(v) => Self::User(v.into()),
			crate::expr::statements::AlterStatement::Access(v) => Self::Access(v.into()),
			crate::expr::statements::AlterStatement::Module(v) => Self::Module(v.into()),
		}
	}
}
