//! `sql` -> `expr` conversions declared directly in [`crate::sql::statements::define`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::*;

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
			DefineStatement::Field(v) => Self::Field(Box::new((*v).into())),
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
			crate::expr::statements::DefineStatement::Field(v) => {
				Self::Field(Box::new((*v).into()))
			}
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
