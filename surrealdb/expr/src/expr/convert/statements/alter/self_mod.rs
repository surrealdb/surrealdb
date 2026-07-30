//! `sql` -> `expr` conversions declared directly in [`crate::sql::statements::alter`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::*;

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
			AlterStatement::Field(v) => Self::Field(Box::new((*v).into())),
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
			crate::expr::statements::AlterStatement::Field(v) => Self::Field(Box::new((*v).into())),
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
