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
mod namespace;
mod param;
mod sequence;
mod table;
pub mod user;

use std::fmt::{self, Display};

pub use access::DefineAccessStatement;
pub use analyzer::DefineAnalyzerStatement;
pub use api::{ApiAction, DefineApiStatement};
pub use bucket::DefineBucketStatement;
pub use config::DefineConfigStatement;
pub use database::DefineDatabaseStatement;
pub use event::DefineEventStatement;
pub use field::{DefineDefault, DefineFieldStatement};
pub use function::DefineFunctionStatement;
pub use index::DefineIndexStatement;
pub use model::DefineModelStatement;
pub use namespace::DefineNamespaceStatement;
pub use param::DefineParamStatement;
pub use sequence::DefineSequenceStatement;
pub use table::DefineTableStatement;
pub use user::DefineUserStatement;

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
pub enum DefineStatement {
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
}

impl Display for DefineStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Namespace(v) => Display::fmt(v, f),
			Self::Database(v) => Display::fmt(v, f),
			Self::Function(v) => Display::fmt(v, f),
			Self::User(v) => Display::fmt(v, f),
			Self::Param(v) => Display::fmt(v, f),
			Self::Table(v) => Display::fmt(v, f),
			Self::Event(v) => Display::fmt(v, f),
			Self::Field(v) => Display::fmt(v, f),
			Self::Index(v) => Display::fmt(v, f),
			Self::Analyzer(v) => Display::fmt(v, f),
			Self::Model(v) => Display::fmt(v, f),
			Self::Access(v) => Display::fmt(v, f),
			Self::Config(v) => Display::fmt(v, f),
			Self::Api(v) => Display::fmt(v, f),
			Self::Bucket(v) => Display::fmt(v, f),
			Self::Sequence(v) => Display::fmt(v, f),
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
		}
	}
}
