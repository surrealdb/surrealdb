use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_strand::Strand;

use crate::catalog::providers::NamespaceProvider;
use crate::catalog::{Error, NamespaceDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::namespace::DefineNamespaceStatement;
use crate::iam::{Action, ResourceKind};
use crate::legacy::expr_to_ident;
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineNamespaceStatement::compute", skip_all)]
pub(crate) async fn define_namespace_statement_compute(
	this: &DefineNamespaceStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Namespace, Base::Root)?;
	// Fetch the transaction
	let txn = ctx.tx();
	// Process the name
	let name: Strand =
		expr_to_ident(stk, ctx, opt, doc, &this.name, "namespace name").await?.into();

	// Check if the definition exists
	let namespace_id = if let Some(ns) = txn.get_ns_by_name(name.as_str(), None).await? {
		match this.kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::NsAlreadyExists {
						name: name.to_string(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
		ns.namespace_id
	} else {
		ctx.try_get_sequences()?.next_namespace_id(Some(ctx)).await?
	};

	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;
	// Process the statement
	let ns_def = NamespaceDefinition {
		namespace_id,
		name,
		comment,
	};
	txn.put_ns(ns_def).await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
