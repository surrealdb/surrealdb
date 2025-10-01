mod access;
mod analyzer;
mod api;
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

pub(crate) use access::RemoveAccessStatement;
pub(crate) use analyzer::RemoveAnalyzerStatement;
use anyhow::Result;
pub(crate) use api::RemoveApiStatement;
pub(crate) use bucket::RemoveBucketStatement;
pub(crate) use database::RemoveDatabaseStatement;
pub(crate) use event::RemoveEventStatement;
pub(crate) use field::RemoveFieldStatement;
pub(crate) use function::RemoveFunctionStatement;
pub(crate) use index::RemoveIndexStatement;
pub(crate) use model::RemoveModelStatement;
pub(crate) use namespace::RemoveNamespaceStatement;
pub(crate) use param::RemoveParamStatement;
use reblessive::tree::Stk;
pub(crate) use sequence::RemoveSequenceStatement;
pub(crate) use table::RemoveTableStatement;
pub(crate) use user::RemoveUserStatement;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::expression::VisitExpression;
use crate::expr::{Expr, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum RemoveStatement {
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
	Api(RemoveApiStatement),
	Bucket(RemoveBucketStatement),
	Sequence(RemoveSequenceStatement),
}

impl RemoveStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		match self {
			Self::Namespace(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Database(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Function(v) => v.compute(ctx, opt).await,
			Self::Access(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Param(v) => v.compute(ctx, opt).await,
			Self::Table(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Event(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Field(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Index(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Analyzer(v) => v.compute(stk, ctx, opt, doc).await,
			Self::User(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Model(v) => v.compute(ctx, opt).await,
			Self::Api(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Bucket(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Sequence(v) => v.compute(stk, ctx, opt, doc).await,
		}
	}
}

impl VisitExpression for RemoveStatement {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		match self {
			RemoveStatement::Namespace(namespace) => namespace.visit(visitor),
			RemoveStatement::Database(database) => database.visit(visitor),
			RemoveStatement::Analyzer(analyzer) => analyzer.visit(visitor),
			RemoveStatement::Access(access) => access.visit(visitor),
			RemoveStatement::Table(table) => table.visit(visitor),
			RemoveStatement::Event(event) => event.visit(visitor),
			RemoveStatement::Field(field) => field.visit(visitor),
			RemoveStatement::Index(index) => index.visit(visitor),
			RemoveStatement::User(user) => user.visit(visitor),
			RemoveStatement::Api(api) => api.visit(visitor),
			RemoveStatement::Bucket(bucket) => bucket.visit(visitor),
			RemoveStatement::Sequence(sequence) => sequence.visit(visitor),
			RemoveStatement::Model(_)
			| RemoveStatement::Function(_)
			| RemoveStatement::Param(_) => {}
		}
	}
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
			Self::Api(v) => Display::fmt(v, f),
			Self::Bucket(v) => Display::fmt(v, f),
			Self::Sequence(v) => Display::fmt(v, f),
		}
	}
}
