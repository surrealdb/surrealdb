use std::borrow::Cow;
use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::param::AlterParamStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::ParamKey;
use crate::val::Value;

#[instrument(level = "trace", name = "AlterParamStatement::compute", skip_all)]
pub(crate) async fn alter_param_statement_compute(
	this: &AlterParamStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Parameter, Base::Db)?;
	let (_, _) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let txn = ctx.tx();

	let mut pa = match txn.get_db_param(ns, db, &this.name, None).await {
		Ok(v) => v.deref().clone(),
		Err(e) => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	if let Some(ref v) = this.value {
		pa.value = stk
			.run(|stk| crate::legacy::expr_compute(v, stk, ctx, opt, doc))
			.await
			.catch_return()?;
		// SECURITY: an altered param value persists in a plain revisioned encode with
		// no depth guard of its own and is rendered back recursively by INFO/EXPORT,
		// exactly like a value set by DEFINE PARAM, so bound its nesting here too. `$p`
		// in the new value resolves to the current stored value, so without this
		// `ALTER PARAM $p VALUE [$p]` repeated across statements would grow the stored
		// value one level per statement until it aborts the process on read-back.
		crate::val::check_value_depth(&pa.value)?;
	}

	match this.comment {
		AlterKind::Set(ref v) => pa.comment = Some(v.clone()),
		AlterKind::Drop => pa.comment = None,
		AlterKind::None => {}
	}

	if let Some(ref p) = this.permissions {
		pa.permissions = p.clone();
	}

	let key = ParamKey {
		ns,
		db,
		pa: Cow::Borrowed(&this.name),
	};
	txn.set_key(&key, &pa.to_stored()).await?;
	txn.clear_cache();
	Ok(Value::None)
}
