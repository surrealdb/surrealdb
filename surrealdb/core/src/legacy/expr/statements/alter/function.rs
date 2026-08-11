use std::borrow::Cow;
use std::ops::Deref;

use anyhow::Result;

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::function::AlterFunctionStatement;
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::key::schema::FunctionKey;
use crate::val::Value;

#[instrument(level = "trace", name = "AlterFunctionStatement::compute", skip_all)]
pub(crate) async fn alter_function_statement_compute(
	this: &AlterFunctionStatement,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Function, Base::Db)?;
	let (_, _) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let txn = ctx.tx();

	let mut fc = match txn.get_db_function(ns, db, &this.name, None).await {
		Ok(v) => v.deref().clone(),
		Err(e) => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	match this.args {
		AlterKind::Set(ref v) => fc.args = v.clone(),
		AlterKind::Drop => fc.args = vec![],
		AlterKind::None => {}
	}

	match this.block {
		AlterKind::Set(ref v) => fc.block = v.clone(),
		AlterKind::Drop => {}
		AlterKind::None => {}
	}

	match this.comment {
		AlterKind::Set(ref v) => fc.comment = Some(v.clone()),
		AlterKind::Drop => fc.comment = None,
		AlterKind::None => {}
	}

	if let Some(ref p) = this.permissions {
		fc.permissions = p.clone();
	}

	match this.returns {
		AlterKind::Set(ref v) => fc.returns = Some(v.clone()),
		AlterKind::Drop => fc.returns = None,
		AlterKind::None => {}
	}

	// Recompute auth_limit from the current principal to prevent privilege escalation
	fc.auth_limit = AuthLimit::new_from_auth(&opt.auth).into();

	// The assembled definition must satisfy the same read-only rules DEFINE
	// enforces, since ALTER stores the same shape: no writing guard
	// (GHSA-66r2-5gwj-gxm2), directly or through a function call, and no
	// writing body while read-only consumers depend on this name.
	if fc.permissions.has_direct_write() {
		anyhow::bail!(crate::exec::Error::PermissionClauseNotReadonly {
			kind: "function",
			name: format!("fn::{}", this.name),
		});
	}
	crate::fnc::mutability::ensure_guards_call_read_only(
		ctx,
		opt,
		"function",
		format!("fn::{}", this.name),
		[&fc.permissions],
	)
	.await?;
	crate::fnc::mutability::ensure_function_stays_read_only_for_consumers(
		ctx,
		opt,
		&this.name,
		&fc.block,
		&fc.permissions,
	)
	.await?;

	let key = FunctionKey {
		ns,
		db,
		fc: Cow::Borrowed(&this.name),
	};
	txn.set_key(&key, &fc.to_stored()).await?;
	txn.clear_cache();
	Ok(Value::None)
}
