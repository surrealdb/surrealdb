mod analyzer;
mod database;
mod event;
mod field;
mod function;
mod index;
mod namespace;
mod param;
mod scope;
mod table;
mod token;
mod user;

pub use analyzer::{analyzer, RemoveAnalyzerStatement};
pub use database::{database, RemoveDatabaseStatement};
pub use event::{event, RemoveEventStatement};
pub use field::{field, RemoveFieldStatement};
pub use function::{function, RemoveFunctionStatement};
pub use index::{index, RemoveIndexStatement};
pub use namespace::{namespace, RemoveNamespaceStatement};
use nom::bytes::complete::tag_no_case;
pub use param::{param, RemoveParamStatement};
pub use scope::{scope, RemoveScopeStatement};
pub use table::{table, RemoveTableStatement};
pub use token::{token, RemoveTokenStatement};
pub use user::{user, RemoveUserStatement};

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
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub enum RemoveStatement {
	Namespace(RemoveNamespaceStatement),
	Database(RemoveDatabaseStatement),
	Function(RemoveFunctionStatement),
	Analyzer(RemoveAnalyzerStatement),
	Token(RemoveTokenStatement),
	Scope(RemoveScopeStatement),
	Param(RemoveParamStatement),
	Table(RemoveTableStatement),
	Event(RemoveEventStatement),
	Field(RemoveFieldStatement),
	Index(RemoveIndexStatement),
	User(RemoveUserStatement),
}

impl RemoveStatement {
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
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match self {
			Self::Namespace(ref v) => v.compute(ctx, opt, txn).await,
			Self::Database(ref v) => v.compute(ctx, opt, txn).await,
			Self::Function(ref v) => v.compute(ctx, opt, txn).await,
			Self::Token(ref v) => v.compute(ctx, opt, txn).await,
			Self::Scope(ref v) => v.compute(ctx, opt, txn).await,
			Self::Param(ref v) => v.compute(ctx, opt, txn).await,
			Self::Table(ref v) => v.compute(ctx, opt, txn).await,
			Self::Event(ref v) => v.compute(ctx, opt, txn).await,
			Self::Field(ref v) => v.compute(ctx, opt, txn).await,
			Self::Index(ref v) => v.compute(ctx, opt, txn).await,
			Self::Analyzer(ref v) => v.compute(ctx, opt, txn).await,
			Self::User(ref v) => v.compute(ctx, opt, txn).await,
		}
	}
}

impl Display for RemoveStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Namespace(v) => Display::fmt(v, f),
			Self::Database(v) => Display::fmt(v, f),
			Self::Function(v) => Display::fmt(v, f),
			Self::Token(v) => Display::fmt(v, f),
			Self::Scope(v) => Display::fmt(v, f),
			Self::Param(v) => Display::fmt(v, f),
			Self::Table(v) => Display::fmt(v, f),
			Self::Event(v) => Display::fmt(v, f),
			Self::Field(v) => Display::fmt(v, f),
			Self::Index(v) => Display::fmt(v, f),
			Self::Analyzer(v) => Display::fmt(v, f),
			Self::User(v) => Display::fmt(v, f),
		}
	}
}

pub fn remove(i: &str) -> IResult<&str, RemoveStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	alt((
		map(namespace, RemoveStatement::Namespace),
		map(database, RemoveStatement::Database),
		map(function, RemoveStatement::Function),
		map(token, RemoveStatement::Token),
		map(scope, RemoveStatement::Scope),
		map(param, RemoveStatement::Param),
		map(table, RemoveStatement::Table),
		map(event, RemoveStatement::Event),
		map(field, RemoveStatement::Field),
		map(index, RemoveStatement::Index),
		map(analyzer, RemoveStatement::Analyzer),
		map(user, RemoveStatement::User),
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::Ident;

	#[test]
	fn check_remove_serialize() {
		let stm = RemoveStatement::Namespace(RemoveNamespaceStatement {
			name: Ident::from("test"),
		});
		let enc: Vec<u8> = stm.try_into().unwrap();
		assert_eq!(9, enc.len());
	}
}
