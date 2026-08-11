use std::ops::Deref;

use anyhow::Result;

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::module::AlterModuleStatement;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[instrument(level = "trace", name = "AlterModuleStatement::compute", skip_all)]
pub(crate) async fn alter_module_statement_compute(
	this: &AlterModuleStatement,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Module, Base::Db)?;
	let (_, _) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let txn = ctx.tx();

	let storage_name = this.name.get_storage_name();
	let mut md = match txn.get_db_module(ns, db, &storage_name, None).await {
		Ok(v) => v.deref().clone(),
		Err(e) => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	match this.comment {
		AlterKind::Set(ref v) => md.comment = Some(v.clone()),
		AlterKind::Drop => md.comment = None,
		AlterKind::None => {}
	}

	if let Some(ref p) = this.permissions {
		md.permissions = p.clone();
	}

	// ALTER stores the same shape DEFINE does, so the assembled definition must
	// satisfy the same read-only rules: no permission guard that modifies data
	// (GHSA-66r2-5gwj-gxm2), directly or through a function call.
	if md.permissions.has_direct_write() {
		anyhow::bail!(crate::exec::Error::PermissionClauseNotReadonly {
			kind: "module",
			name: storage_name.clone(),
		});
	}
	crate::fnc::mutability::ensure_guards_call_read_only(
		ctx,
		opt,
		"module",
		storage_name.clone(),
		[&md.permissions],
	)
	.await?;

	txn.put_db_module(ns, db, &md).await?;
	txn.clear_cache();
	Ok(Value::None)
}
