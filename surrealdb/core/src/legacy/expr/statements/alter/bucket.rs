use std::borrow::Cow;
use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use tracing::instrument;

use crate::catalog::Error;
use crate::catalog::providers::BucketProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::bucket::AlterBucketStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::database::all::DatabaseRoot;
use crate::legacy::expr_to_ident;
use crate::val::Value;

#[instrument(level = "trace", name = "AlterBucketStatement::compute", skip_all)]
pub(crate) async fn alter_bucket_statement_compute(
	this: &AlterBucketStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Bucket, Base::Db)?;
	let (_, _) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let txn = ctx.tx();
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "bucket name").await?;

	let mut bu = match txn.get_db_bucket(ns, db, &name, None).await? {
		Some(v) => v.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(Error::BuNotFound {
				name: name.clone(),
			}
			.into());
		}
	};

	match this.backend {
		AlterKind::Set(ref v) => bu.backend = Some(v.into()),
		AlterKind::Drop => bu.backend = None,
		AlterKind::None => {}
	}

	if let Some(ref p) = this.permissions {
		bu.permissions = p.clone();
	}

	match this.readonly {
		AlterKind::Set(_) => bu.readonly = true,
		AlterKind::Drop => bu.readonly = false,
		AlterKind::None => {}
	}

	match this.comment {
		AlterKind::Set(ref v) => bu.comment = Some(v.clone()),
		AlterKind::Drop => bu.comment = None,
		AlterKind::None => {}
	}

	let key = crate::key::database::bu::BucketKey {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		bu: Cow::Borrowed(&name),
	};
	txn.set_key(&key, &bu.to_stored()).await?;
	txn.clear_cache();
	Ok(Value::None)
}
