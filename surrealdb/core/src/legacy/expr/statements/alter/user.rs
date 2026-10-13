use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use tracing::instrument;

use crate::catalog;
use crate::catalog::Error;
use crate::catalog::providers::UserProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::user::AlterUserStatement;
use crate::iam::{Action, ResourceKind};
use crate::legacy::expr_to_ident;
use crate::val::Value;

#[instrument(level = "trace", name = "AlterUserStatement::compute", skip_all)]
pub(crate) async fn alter_user_statement_compute(
	this: &AlterUserStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Actor, this.base)?;
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "user name").await?;

	match this.base {
		Base::Root => crate::legacy::alter_user_statement_compute_root(this, ctx, &name).await,
		Base::Ns => crate::legacy::alter_user_statement_compute_ns(this, ctx, opt, &name).await,
		Base::Db => crate::legacy::alter_user_statement_compute_db(this, ctx, opt, &name).await,
	}
}

pub(crate) async fn alter_user_statement_compute_root(
	this: &AlterUserStatement,
	ctx: &FrozenContext,
	name: &str,
) -> Result<Value> {
	let txn = ctx.tx();
	let mut user = match txn.get_root_user(name, None).await? {
		Some(v) => v.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(Error::UserRootNotFound {
				name: name.to_owned(),
			}
			.into());
		}
	};
	crate::legacy::alter_user_statement_apply(this, &mut user);
	txn.put_root_user(&user).await?;
	txn.clear_cache();
	Ok(Value::None)
}

pub(crate) async fn alter_user_statement_compute_ns(
	this: &AlterUserStatement,
	ctx: &FrozenContext,
	opt: &Options,
	name: &str,
) -> Result<Value> {
	let txn = ctx.tx();
	let ns = ctx.get_ns_id(opt).await?;
	let ns_name = opt.ns()?;
	let mut user = match txn.get_ns_user(ns, name, None).await? {
		Some(v) => v.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(Error::UserNsNotFound {
				name: name.to_owned(),
				ns: ns_name.to_string(),
			}
			.into());
		}
	};
	crate::legacy::alter_user_statement_apply(this, &mut user);
	txn.put_ns_user(ns, &user).await?;
	txn.clear_cache();
	Ok(Value::None)
}

pub(crate) async fn alter_user_statement_compute_db(
	this: &AlterUserStatement,
	ctx: &FrozenContext,
	opt: &Options,
	name: &str,
) -> Result<Value> {
	let txn = ctx.tx();
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let (ns_name, db_name) = opt.ns_db()?;
	let mut user = match txn.get_db_user(ns, db, name, None).await? {
		Some(v) => v.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(Error::UserDbNotFound {
				name: name.to_owned(),
				ns: ns_name.to_string(),
				db: db_name.to_string(),
			}
			.into());
		}
	};
	crate::legacy::alter_user_statement_apply(this, &mut user);
	txn.put_db_user(ns, db, &user).await?;
	txn.clear_cache();
	Ok(Value::None)
}

pub(crate) fn alter_user_statement_apply(
	this: &AlterUserStatement,
	user: &mut catalog::UserDefinition,
) {
	if let Some(ref h) = this.hash {
		user.hash.clone_from(h);
	}
	if let Some(ref s) = this.scram {
		user.scram.clone_from(s);
	}
	match this.roles {
		AlterKind::Set(ref v) => user.roles.clone_from(v),
		AlterKind::Drop => user.roles = vec![],
		AlterKind::None => {}
	}
	match this.token_duration {
		AlterKind::Set(v) => user.token_duration = v,
		AlterKind::Drop => user.token_duration = None,
		AlterKind::None => {}
	}
	match this.session_duration {
		AlterKind::Set(v) => user.session_duration = v,
		AlterKind::Drop => user.session_duration = None,
		AlterKind::None => {}
	}
	match this.comment {
		AlterKind::Set(ref v) => user.comment = Some(v.clone()),
		AlterKind::Drop => user.comment = None,
		AlterKind::None => {}
	}
}
