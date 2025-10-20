mod access;
mod analyzer;
mod api;
mod bucket;
pub mod config;
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

pub(crate) use access::DefineAccessStatement;
pub(crate) use analyzer::DefineAnalyzerStatement;
use anyhow::Result;
pub(crate) use api::{ApiAction, DefineApiStatement};
pub(crate) use bucket::DefineBucketStatement;
pub(crate) use config::DefineConfigStatement;
pub(crate) use database::DefineDatabaseStatement;
pub(crate) use event::DefineEventStatement;
pub(crate) use field::{DefineDefault, DefineFieldStatement};
pub(crate) use function::DefineFunctionStatement;
pub(crate) use index::DefineIndexStatement;
pub(in crate::expr::statements) use index::run_indexing;
pub(crate) use model::DefineModelStatement;
pub(crate) use namespace::DefineNamespaceStatement;
pub(crate) use param::DefineParamStatement;
use reblessive::tree::Stk;
pub(crate) use sequence::DefineSequenceStatement;
pub(crate) use table::DefineTableStatement;
pub(crate) use user::DefineUserStatement;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Expr;
use crate::expr::expression::VisitExpression;
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub enum DefineKind {
	#[default]
	Default,
	Overwrite,
	IfNotExists,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum DefineStatement {
	Namespace(DefineNamespaceStatement),
	Database(DefineDatabaseStatement),
	Function(DefineFunctionStatement),
	Analyzer(DefineAnalyzerStatement),
	Param(DefineParamStatement),
	Table(DefineTableStatement),
	Event(DefineEventStatement),
	Field(DefineFieldStatement),
	Index(DefineIndexStatement),
	User(DefineUserStatement),
	Model(DefineModelStatement),
	Access(DefineAccessStatement),
	Config(DefineConfigStatement),
	Api(DefineApiStatement),
	Bucket(DefineBucketStatement),
	Sequence(DefineSequenceStatement),
}

impl DefineStatement {
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
			Self::Function(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Param(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Table(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Event(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Field(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Index(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Analyzer(v) => v.compute(stk, ctx, opt, doc).await,
			Self::User(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Model(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Access(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Config(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Api(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Bucket(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Sequence(v) => v.compute(stk, ctx, opt, doc).await,
		}
	}
}

impl VisitExpression for DefineStatement {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		match self {
			DefineStatement::Namespace(namespace) => namespace.visit(visitor),
			DefineStatement::Database(database) => database.visit(visitor),
			DefineStatement::Function(function) => function.visit(visitor),
			DefineStatement::Analyzer(analyzer) => analyzer.visit(visitor),
			DefineStatement::Param(param) => param.visit(visitor),
			DefineStatement::Table(table) => table.visit(visitor),
			DefineStatement::Event(event) => event.visit(visitor),
			DefineStatement::Field(field) => field.visit(visitor),
			DefineStatement::Index(index) => index.visit(visitor),
			DefineStatement::User(user) => user.visit(visitor),
			DefineStatement::Model(model) => model.visit(visitor),
			DefineStatement::Access(access) => access.visit(visitor),
			DefineStatement::Config(_) => {}
			DefineStatement::Api(api) => api.visit(visitor),
			DefineStatement::Bucket(bucket) => bucket.visit(visitor),
			DefineStatement::Sequence(sequence) => sequence.visit(visitor),
		}
	}
}

impl std::fmt::Display for DefineStatement {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		use surrealdb_types::ToSql;
		write!(f, "{}", self.to_sql())
	}
}

impl surrealdb_types::ToSql for DefineStatement {
	fn fmt_sql(&self, f: &mut String, pretty: PrettyMode) {
		match self {
			Self::Namespace(v) => v.fmt_sql(f, pretty),
			Self::Database(v) => v.fmt_sql(f, pretty),
			Self::Function(v) => v.fmt_sql(f, pretty),
			Self::User(v) => v.fmt_sql(f, pretty),
			Self::Param(v) => v.fmt_sql(f, pretty),
			Self::Table(v) => v.fmt_sql(f, pretty),
			Self::Event(v) => v.fmt_sql(f, pretty),
			Self::Field(v) => v.fmt_sql(f, pretty),
			Self::Index(v) => v.fmt_sql(f, pretty),
			Self::Analyzer(v) => v.fmt_sql(f, pretty),
			Self::Model(v) => v.fmt_sql(f, pretty),
			Self::Access(v) => v.fmt_sql(f, pretty),
			Self::Config(v) => v.fmt_sql(f, pretty),
			Self::Api(v) => v.fmt_sql(f, pretty),
			Self::Bucket(v) => v.fmt_sql(f, pretty),
			Self::Sequence(v) => v.fmt_sql(f, pretty),
		}
	}
}
