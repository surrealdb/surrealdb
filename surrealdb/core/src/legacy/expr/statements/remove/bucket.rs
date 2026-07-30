use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::Error;
use crate::catalog::providers::BucketProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::remove::bucket::RemoveBucketStatement;
use crate::iam::{Action, ResourceKind};
use crate::legacy::expr_to_ident;
use crate::val::Value;

/// Process this type returning a computed simple Value
pub(crate) async fn remove_bucket_statement_compute(
	this: &RemoveBucketStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Bucket, Base::Db)?;
	// Compute the name
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "bucket name").await?;
	// Get the transaction
	let txn = ctx.tx();
	// Get the definition
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let Some(bu) = txn.get_db_bucket(ns, db, &name, None).await? else {
		if this.if_exists {
			return Ok(Value::None);
		} else {
			return Err(Error::BuNotFound {
				name,
			}
			.into());
		}
	};

	// Delete the definition
	let key = crate::key::database::bu::BucketKey {
		prefix: crate::key::database::all::DatabaseRoot {
			ns,
			db,
		},
		bu: std::borrow::Cow::Borrowed(&bu.name),
	};
	txn.del_key(&key).await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
