mod analyzer;
mod database;
mod event;
mod field;
mod function;
mod index;
mod model;
mod namespace;
mod param;
mod scope;
mod table;
mod token;
mod user;

pub use analyzer::DefineAnalyzerStatement;
pub use database::DefineDatabaseStatement;
pub use event::DefineEventStatement;
pub use field::DefineFieldStatement;
pub use function::DefineFunctionStatement;
pub use index::DefineIndexStatement;
pub use model::DefineModelStatement;
pub use namespace::DefineNamespaceStatement;
pub use param::DefineParamStatement;
pub use scope::DefineScopeStatement;
pub use table::DefineTableStatement;
pub use token::DefineTokenStatement;
pub use user::DefineUserStatement;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::value::Value;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
#[non_exhaustive]
pub enum DefineStatement {
	Namespace(DefineNamespaceStatement),
	Database(DefineDatabaseStatement),
	Function(DefineFunctionStatement),
	Analyzer(DefineAnalyzerStatement),
	Token(DefineTokenStatement),
	Scope(DefineScopeStatement),
	Param(DefineParamStatement),
	Table(DefineTableStatement),
	Event(DefineEventStatement),
	Field(DefineFieldStatement),
	Index(DefineIndexStatement),
	User(DefineUserStatement),
	Model(DefineModelStatement),
}

impl DefineStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match self {
			Self::Namespace(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Database(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Function(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Token(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Scope(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Param(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Table(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Event(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Field(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Index(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Analyzer(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::User(ref v) => v.compute(ctx, opt, txn, doc).await,
			Self::Model(ref v) => v.compute(ctx, opt, txn, doc).await,
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
			Self::Token(v) => Display::fmt(v, f),
			Self::Scope(v) => Display::fmt(v, f),
			Self::Param(v) => Display::fmt(v, f),
			Self::Table(v) => Display::fmt(v, f),
			Self::Event(v) => Display::fmt(v, f),
			Self::Field(v) => Display::fmt(v, f),
			Self::Index(v) => Display::fmt(v, f),
			Self::Analyzer(v) => Display::fmt(v, f),
			Self::Model(v) => Display::fmt(v, f),
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
		assert_eq!(12, enc.len());
	}
}
