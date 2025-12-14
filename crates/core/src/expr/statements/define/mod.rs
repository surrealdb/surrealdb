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
mod module;
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
pub(crate) use module::DefineModuleStatement;
pub(crate) use namespace::DefineNamespaceStatement;
pub(crate) use param::DefineParamStatement;
use reblessive::tree::Stk;
pub(crate) use sequence::DefineSequenceStatement;
pub(crate) use table::DefineTableStatement;
pub(crate) use user::DefineUserStatement;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
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
	Module(DefineModuleStatement),
}

impl DefineStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
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
			Self::Module(v) => v.compute(stk, ctx, opt, doc).await,
		}
	}
}
