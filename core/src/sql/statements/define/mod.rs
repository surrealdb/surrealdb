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

#[doc(hidden)]
pub use deprecated::scope::DefineScopeStatement;
#[doc(hidden)]
pub use deprecated::token::DefineTokenStatement;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::value::Value;
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
		Ok(DefineStatement::Access(fields.0.into()))
	}

	fn convert_scope_to_access(
		fields: DefineScopeStatementFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(DefineStatement::Access(fields.0.into()))
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
