mod access;
mod analyzer;
mod bucket;
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
mod user;

use std::fmt::{self, Display, Formatter};

pub use access::RemoveAccessStatement;
pub use analyzer::RemoveAnalyzerStatement;
pub use bucket::RemoveBucketStatement;
pub use database::RemoveDatabaseStatement;
pub use event::RemoveEventStatement;
pub use field::RemoveFieldStatement;
pub use function::RemoveFunctionStatement;
pub use index::RemoveIndexStatement;
pub use model::RemoveModelStatement;
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
	Model(RemoveModelStatement),
	Bucket(RemoveBucketStatement),
	Sequence(RemoveSequenceStatement),
}

impl Display for RemoveStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Namespace(v) => Display::fmt(v, f),
			Self::Database(v) => Display::fmt(v, f),
			Self::Function(v) => Display::fmt(v, f),
			Self::Access(v) => Display::fmt(v, f),
			Self::Param(v) => Display::fmt(v, f),
			Self::Table(v) => Display::fmt(v, f),
			Self::Event(v) => Display::fmt(v, f),
			Self::Field(v) => Display::fmt(v, f),
			Self::Index(v) => Display::fmt(v, f),
			Self::Analyzer(v) => Display::fmt(v, f),
			Self::User(v) => Display::fmt(v, f),
			Self::Model(v) => Display::fmt(v, f),
			Self::Bucket(v) => Display::fmt(v, f),
			Self::Sequence(v) => Display::fmt(v, f),
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
			RemoveStatement::Bucket(v) => Self::Bucket(v.into()),
			RemoveStatement::Sequence(v) => Self::Sequence(v.into()),
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
			crate::expr::statements::RemoveStatement::Bucket(v) => Self::Bucket(v.into()),
			crate::expr::statements::RemoveStatement::Sequence(v) => Self::Sequence(v.into()),
		}
	}
}
