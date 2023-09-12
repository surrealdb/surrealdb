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

pub use analyzer::{analyzer, DefineAnalyzerStatement};
pub use database::{database, DefineDatabaseStatement};
pub use event::{event, DefineEventStatement};
pub use field::{field, DefineFieldStatement};
pub use function::{function, DefineFunctionStatement};
pub use index::{index, DefineIndexStatement};
pub use model::DefineModelStatement;
pub use namespace::{namespace, DefineNamespaceStatement};
use nom::bytes::complete::tag_no_case;
pub use param::{param, DefineParamStatement};
pub use scope::{scope, DefineScopeStatement};
pub use table::{table, DefineTableStatement};
pub use token::{token, DefineTokenStatement};
pub use user::{user, DefineUserStatement};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::combinator::map;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
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
	MlModel(DefineModelStatement),
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
			Self::MlModel(ref v) => v.compute(ctx, opt, txn, doc).await,
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
			Self::MlModel(v) => Display::fmt(v, f),
		}
	}
}

pub fn define(i: &str) -> IResult<&str, DefineStatement> {
	let (i, _) = tag_no_case("DEFINE")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((
		map(namespace, DefineStatement::Namespace),
		map(database, DefineStatement::Database),
		map(function, DefineStatement::Function),
		map(user, DefineStatement::User),
		map(token, DefineStatement::Token),
		map(scope, DefineStatement::Scope),
		map(param, DefineStatement::Param),
		map(table, DefineStatement::Table),
		map(event, DefineStatement::Event),
		map(field, DefineStatement::Field),
		map(index, DefineStatement::Index),
		map(analyzer, DefineStatement::Analyzer),
	))(i)
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
		let enc: Vec<u8> = stm.try_into().unwrap();
		assert_eq!(11, enc.len());
	}
}
