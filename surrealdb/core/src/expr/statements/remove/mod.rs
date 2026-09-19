mod access;
mod analyzer;
mod api;
mod bucket;
mod config;
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

pub(crate) use access::RemoveAccessStatement;
pub(crate) use analyzer::RemoveAnalyzerStatement;
use anyhow::Result;
pub(crate) use api::RemoveApiStatement;
pub(crate) use bucket::RemoveBucketStatement;
pub(crate) use config::RemoveConfigStatement;
pub(crate) use database::RemoveDatabaseStatement;
pub(crate) use event::RemoveEventStatement;
pub(crate) use field::RemoveFieldStatement;
pub(crate) use function::RemoveFunctionStatement;
pub(crate) use index::RemoveIndexStatement;
pub(crate) use model::RemoveModelStatement;
pub(crate) use module::RemoveModuleStatement;
pub(crate) use namespace::RemoveNamespaceStatement;
pub(crate) use param::RemoveParamStatement;
use reblessive::tree::Stk;
pub(crate) use sequence::RemoveSequenceStatement;
pub(crate) use table::RemoveTableStatement;
pub(crate) use user::RemoveUserStatement;

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::{Options, RoutedNotification};
use crate::doc::CursorDoc;
use crate::expr::Value;
use crate::kvs::Transaction;
use crate::kvs::index::retire_durable_index;
use crate::types::{PublicAction, PublicNotification, PublicValue};

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
	Module(RemoveModuleStatement),
	Config(RemoveConfigStatement),
}

impl RemoveStatement {
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
			Self::Module(v) => v.compute(ctx, opt).await,
			Self::Config(v) => v.compute(ctx, opt).await,
		}
	}
}

/// Tell every subscriber under `ns`/`db` that its live query is gone.
///
/// `REMOVE TABLE` announces this for the one table it destroys. Removing a
/// database or namespace destroys the same subscriptions without going through
/// that statement, so it has to walk to them itself — otherwise the
/// subscriber is never told and its stream stays open forever.
///
/// The walk is skipped entirely when nothing is listening, and otherwise
/// mirrors the table iteration [`retire_database_indexes`] already performs.
async fn kill_database_lives(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
) -> Result<()> {
	let Some(sender) = ctx.broker() else {
		return Ok(());
	};
	for tb in txn.all_tb(ns, db, None).await?.iter() {
		for lv in txn.all_tb_lives(ns, db, &tb.name, None).await?.iter() {
			sender
				.send(RoutedNotification::new(
					lv.node,
					PublicNotification::new(
						lv.id.into(),
						lv.session_id(),
						PublicAction::Killed,
						PublicValue::None,
						PublicValue::None,
					),
				))
				.await;
		}
	}
	Ok(())
}

/// Tell every subscriber under `ns` that its live query is gone.
async fn kill_namespace_lives(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
) -> Result<()> {
	if ctx.broker().is_none() {
		return Ok(());
	}
	for db in txn.all_db(ns, None).await?.iter() {
		kill_database_lives(ctx, txn, ns, db.database_id).await?;
	}
	Ok(())
}

async fn retire_namespace_indexes(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
) -> Result<()> {
	for db in txn.all_db(ns, None).await?.iter() {
		retire_database_indexes(ctx, txn, ns, db.database_id).await?;
	}
	Ok(())
}

async fn retire_database_indexes(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
) -> Result<()> {
	for tb in txn.all_tb(ns, db, None).await?.iter() {
		retire_table_indexes(ctx, txn, ns, db, tb).await?;
	}
	Ok(())
}

async fn retire_table_indexes(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableDefinition,
) -> Result<()> {
	let index_builder = ctx.get_index_builder().cloned();
	for ix in txn.all_tb_indexes(ns, db, &tb.name, None).await?.iter() {
		// Local index wrappers can be evicted immediately, but the builder task
		// is process memory and must only be aborted after this transaction commits.
		ctx.get_index_stores().index_removed(ns, db, tb, ix).await?;
		if let Some(index_builder) = &index_builder {
			txn.register_index_builder_abort_after_commit(
				index_builder.clone(),
				ns,
				db,
				tb.name.clone(),
				ix.index_id,
			)
			.await;
		}
		retire_durable_index(txn, ns, db, &tb.name, ix.index_id).await?;
	}
	Ok(())
}
