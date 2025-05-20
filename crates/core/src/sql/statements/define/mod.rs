mod access;
mod analyzer;
mod api;
mod bucket;
pub mod config;
mod database;
mod deprecated;
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

pub use access::DefineAccessStatement;
pub use analyzer::DefineAnalyzerStatement;
pub use api::DefineApiStatement;
pub use bucket::DefineBucketStatement;
pub use config::DefineConfigStatement;
pub use database::DefineDatabaseStatement;
pub use event::DefineEventStatement;
pub use field::DefineFieldStatement;
pub use function::DefineFunctionStatement;
pub use index::DefineIndexStatement;
pub use model::DefineModelStatement;
pub use namespace::DefineNamespaceStatement;
pub use param::DefineParamStatement;
pub use sequence::DefineSequenceStatement;
pub use table::DefineTableStatement;
pub use user::DefineUserStatement;

pub use deprecated::scope::DefineScopeStatement;
pub use deprecated::token::DefineTokenStatement;

pub use api::ApiAction;
pub use api::ApiDefinition;

pub use bucket::BucketDefinition;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::sql::value::SqlValue;
use anyhow::Result;

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 5)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum DefineStatement {
	Namespace(DefineNamespaceStatement),
	Database(DefineDatabaseStatement),
	Function(DefineFunctionStatement),
	Analyzer(DefineAnalyzerStatement),
	#[revision(
		end = 2,
		convert_fn = "convert_token_to_access",
		fields_name = "DefineTokenStatementFields"
	)]
	Token(DefineTokenStatement),
	#[revision(
		end = 2,
		convert_fn = "convert_scope_to_access",
		fields_name = "DefineScopeStatementFields"
	)]
	Scope(DefineScopeStatement),
	Param(DefineParamStatement),
	Table(DefineTableStatement),
	Event(DefineEventStatement),
	Field(DefineFieldStatement),
	Index(DefineIndexStatement),
	User(DefineUserStatement),
	Model(DefineModelStatement),
	#[revision(start = 2)]
	Access(DefineAccessStatement),
	Config(DefineConfigStatement),
	#[revision(start = 3)]
	Api(DefineApiStatement),
	#[revision(start = 4)]
	Bucket(DefineBucketStatement),
	#[revision(start = 5)]
	Sequence(DefineSequenceStatement),
}

// Revision implementations
impl DefineStatement {
	fn convert_token_to_access(
		fields: DefineTokenStatementFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(DefineStatement::Access(fields.0.into()))
	}

	fn convert_scope_to_access(
		fields: DefineScopeStatementFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(DefineStatement::Access(fields.0.into()))
	}
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

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::Ident;

	#[test]
	fn check_define_serialize() {
		let stm = DefineStatement::Namespace(DefineNamespaceStatement {
			name: Ident::from("test"),
			..Default::default()
		});
		let enc: Vec<u8> = revision::to_vec(&stm).unwrap();
		assert_eq!(13, enc.len());
	}
}
