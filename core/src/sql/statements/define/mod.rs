mod access;
mod analyzer;
mod database;
mod deprecated;
mod event;
mod field;
mod function;
mod index;
mod model;
mod namespace;
mod param;
mod table;
mod user;

pub use access::DefineAccessStatement;
pub use analyzer::DefineAnalyzerStatement;
pub use database::DefineDatabaseStatement;
pub use event::DefineEventStatement;
pub use field::DefineFieldStatement;
pub use function::DefineFunctionStatement;
pub use index::DefineIndexStatement;
pub use model::DefineModelStatement;
pub use namespace::DefineNamespaceStatement;
pub use param::DefineParamStatement;
pub use table::DefineTableStatement;
pub use user::DefineUserStatement;

use deprecated::scope::DefineScopeStatement;
use deprecated::token::DefineTokenStatement;

use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::access::AccessDuration;
use crate::sql::access_type::{JwtAccessIssue, JwtAccessVerify, JwtAccessVerifyKey};
use crate::sql::value::Value;
use crate::sql::{Algorithm, Base, JwtAccess, RecordAccess};
use crate::{ctx::Context, sql::AccessType};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
}

// Revision implementations
impl DefineStatement {
	fn convert_token_to_access(
		fields: DefineTokenStatementFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(DefineStatement::Access(DefineAccessStatement {
			name: fields.0.name,
			base: fields.0.base,
			comment: fields.0.comment,
			if_not_exists: fields.0.if_not_exists,
			kind: AccessType::Jwt(JwtAccess {
				issue: None,
				verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
					alg: fields.0.kind,
					key: fields.0.code,
				}),
			}),
			// unused fields
			authenticate: None,
			duration: AccessDuration::default(),
			overwrite: false,
		}))
	}

	fn convert_scope_to_access(
		fields: DefineScopeStatementFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(DefineStatement::Access(DefineAccessStatement {
			name: fields.0.name,
			base: Base::Db,
			comment: fields.0.comment,
			if_not_exists: fields.0.if_not_exists,
			kind: AccessType::Record(RecordAccess {
				signup: fields.0.signup,
				signin: fields.0.signin,
				jwt: JwtAccess {
					issue: Some(JwtAccessIssue {
						alg: Algorithm::Hs512,
						key: fields.0.code.clone(),
					}),
					verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
						alg: Algorithm::Hs512,
						key: fields.0.code,
					}),
				},
			}),
			// unused fields
			authenticate: None,
			duration: AccessDuration {
				session: fields.0.session,
				..AccessDuration::default()
			},
			overwrite: false,
		}))
	}
}

impl DefineStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		match self {
			Self::Namespace(ref v) => v.compute(ctx, opt, doc).await,
			Self::Database(ref v) => v.compute(ctx, opt, doc).await,
			Self::Function(ref v) => v.compute(ctx, opt, doc).await,
			Self::Param(ref v) => v.compute(stk, ctx, opt, doc).await,
			Self::Table(ref v) => v.compute(stk, ctx, opt, doc).await,
			Self::Event(ref v) => v.compute(ctx, opt, doc).await,
			Self::Field(ref v) => v.compute(ctx, opt, doc).await,
			Self::Index(ref v) => v.compute(stk, ctx, opt, doc).await,
			Self::Analyzer(ref v) => v.compute(ctx, opt, doc).await,
			Self::User(ref v) => v.compute(ctx, opt, doc).await,
			Self::Model(ref v) => v.compute(ctx, opt, doc).await,
			Self::Access(ref v) => v.compute(ctx, opt, doc).await,
		}
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
		let enc: Vec<u8> = stm.into();
		assert_eq!(13, enc.len());
	}
}
