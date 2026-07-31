use std::borrow::Cow;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog;
use crate::catalog::Error;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::analyzer::DefineAnalyzerStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::AnalyzerKey;
use crate::legacy::expr_to_ident;
use crate::val::Value;

pub(crate) async fn define_analyzer_statement_to_definition(
	this: &DefineAnalyzerStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<catalog::AnalyzerDefinition> {
	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	Ok(catalog::AnalyzerDefinition {
		name: expr_to_ident(stk, ctx, opt, doc, &this.name, "analyzer name").await?.into(),
		function: this.function.clone(),
		tokenizers: this.tokenizers.clone(),
		filters: this.filters.clone(),
		comment,
	})
}

#[instrument(level = "trace", name = "DefineAnalyzerStatement::compute", skip_all)]
pub(crate) async fn define_analyzer_statement_compute(
	this: &DefineAnalyzerStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Analyzer, Base::Db)?;
	// Compute the definition
	let definition =
		crate::legacy::define_analyzer_statement_to_definition(this, stk, ctx, opt, doc).await?;
	// Fetch the transaction
	let txn = ctx.tx();
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	// Check if the definition exists
	if txn.get_db_analyzer(ns, db, definition.name.as_str(), None).await.is_ok() {
		match this.kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::AzAlreadyExists {
						name: definition.name.to_string(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}
	// Process the statement
	let key = AnalyzerKey {
		ns,
		db,
		az: Cow::Borrowed(definition.name.as_str()),
	};
	ctx.get_index_stores().mappers().load(&definition, &ctx.config.idx.file_allowlist).await?;
	txn.set_key(&key, &definition).await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
