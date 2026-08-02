pub(crate) mod access;
pub(crate) mod analyzer;
pub(crate) mod api;
pub(crate) mod bucket;
pub(crate) mod config;
pub(crate) mod database;
pub(crate) mod event;
pub(crate) mod field;
pub(crate) mod function;
pub(crate) mod index;
pub(crate) mod model;
pub(crate) mod module;
pub(crate) mod namespace;
pub(crate) mod param;
pub(crate) mod sequence;
pub(crate) mod table;
pub(crate) mod user;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::remove::RemoveStatement;
use crate::kvs::Transaction;
use crate::kvs::index::{AbortLocalBuild, retire_durable_index};
use crate::val::Value;

/// Process this type returning a computed simple Value
pub(crate) async fn remove_statement_compute(
	this: &RemoveStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	match this {
		RemoveStatement::Namespace(v) => {
			crate::legacy::remove_namespace_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::Database(v) => {
			crate::legacy::remove_database_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::Function(v) => {
			crate::legacy::remove_function_statement_compute(v, ctx, opt).await
		}
		RemoveStatement::Access(v) => {
			crate::legacy::remove_access_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::Param(v) => {
			crate::legacy::remove_param_statement_compute(v, ctx, opt).await
		}
		RemoveStatement::Table(v) => {
			crate::legacy::remove_table_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::Event(v) => {
			crate::legacy::remove_event_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::Field(v) => {
			crate::legacy::remove_field_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::Index(v) => {
			crate::legacy::remove_index_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::Analyzer(v) => {
			crate::legacy::remove_analyzer_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::User(v) => {
			crate::legacy::remove_user_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::Model(v) => {
			crate::legacy::remove_model_statement_compute(v, ctx, opt).await
		}
		RemoveStatement::Api(v) => {
			crate::legacy::remove_api_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::Bucket(v) => {
			crate::legacy::remove_bucket_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::Sequence(v) => {
			crate::legacy::remove_sequence_statement_compute(v, stk, ctx, opt, doc).await
		}
		RemoveStatement::Module(v) => {
			crate::legacy::remove_module_statement_compute(v, ctx, opt).await
		}
		RemoveStatement::Config(v) => {
			crate::legacy::remove_config_statement_compute(v, ctx, opt).await
		}
	}
}

pub(crate) async fn retire_namespace_indexes(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
) -> Result<()> {
	for db in txn.all_db(ns, None).await?.iter() {
		retire_database_indexes(ctx, txn, ns, db.database_id).await?;
	}
	Ok(())
}

pub(crate) async fn retire_database_indexes(
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

pub(crate) async fn retire_table_indexes(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableDefinition,
) -> Result<()> {
	let index_builder = ctx.get_index_builder().cloned();
	let tb_name = tb.name.clone();
	for ix in txn.all_tb_indexes(ns, db, &tb_name, None).await?.iter() {
		// Local index wrappers can be evicted immediately, but the builder task
		// is process memory and must only be aborted after this transaction commits.
		ctx.get_index_stores().index_removed(ns, db, tb, ix).await?;
		if let Some(index_builder) = &index_builder {
			txn.on_commit(AbortLocalBuild::boxed(
				index_builder.clone(),
				ns,
				db,
				tb_name.clone(),
				ix.index_id,
			))
			.await;
		}
		retire_durable_index(txn, ns, db, &tb_name, ix.index_id).await?;
	}
	Ok(())
}
