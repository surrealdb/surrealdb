use std::borrow::Cow;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::buc::Error as BucError;
use crate::catalog::providers::BucketProvider;
use crate::catalog::{BucketDefinition, Error as CatalogError};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::exec::Error as ExecError;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::bucket::DefineBucketStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::database::all::DatabaseRoot;
use crate::legacy::expr_to_ident;
use crate::val::Value;

#[instrument(level = "trace", name = "DefineBucketStatement::compute", skip_all)]
pub(crate) async fn define_bucket_statement_compute(
	this: &DefineBucketStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Bucket, Base::Db)?;
	// Process the name
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "bucket name").await?;
	// A PERMISSIONS clause must not perform writes (GHSA-66r2-5gwj-gxm2).
	if this.permissions.has_direct_write() {
		bail!(ExecError::PermissionClauseNotReadonly {
			kind: "bucket",
			name: name.clone(),
		});
	}
	// Fetch the transaction
	let txn = ctx.tx();
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	// Check if the definition exists
	if let Some(bucket) = txn.get_db_bucket(ns, db, &name, None).await? {
		match this.kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(CatalogError::BuAlreadyExists {
						value: bucket.name.to_string(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => {
				return Ok(Value::None);
			}
		}
	}
	// Process the backend input
	let backend = if let Some(ref url) = this.backend {
		Some(
			stk.run(|stk| crate::legacy::expr_compute(url, stk, ctx, opt, doc))
				.await
				.catch_return()?
				.coerce_to::<String>()?,
		)
	} else {
		None
	};

	// Create and cache a new backend
	if let Some(buckets) = ctx.get_buckets() {
		buckets.new_backend(ns, db, &name, this.readonly, backend.as_deref()).await?;
	} else {
		bail!(BucError::BucketUnavailable(name));
	}

	// Process the statement
	let key = crate::key::database::bu::BucketKey {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		bu: Cow::Borrowed(&name),
	};

	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	let ap = BucketDefinition {
		id: None,
		name: name.clone().into(),
		backend: backend.map(|s| s.into()),
		permissions: this.permissions.clone(),
		readonly: this.readonly,
		comment,
	};
	txn.set_key(&key, &ap.to_stored()).await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
