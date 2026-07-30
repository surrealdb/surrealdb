use std::borrow::Cow;
use std::ops::Deref;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use tracing::instrument;

use crate::catalog;
use crate::catalog::Error as CatalogError;
use crate::catalog::providers::AuthorisationProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::Error as ExecError;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::access::AlterAccessStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::database::all::DatabaseRoot;
use crate::legacy::expr_to_ident;
use crate::val::Value;

#[instrument(level = "trace", name = "AlterAccessStatement::compute", skip_all)]
pub(crate) async fn alter_access_statement_compute(
	this: &AlterAccessStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Access, this.base)?;
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "access name").await?;

	match this.base {
		Base::Root => crate::legacy::alter_access_statement_compute_root(this, ctx, &name).await,
		Base::Ns => crate::legacy::alter_access_statement_compute_ns(this, ctx, opt, &name).await,
		Base::Db => crate::legacy::alter_access_statement_compute_db(this, ctx, opt, &name).await,
	}
}

pub(crate) async fn alter_access_statement_compute_root(
	this: &AlterAccessStatement,
	ctx: &FrozenContext,
	name: &str,
) -> Result<Value> {
	let txn = ctx.tx();
	let mut ac = match txn.get_root_access(name, None).await? {
		Some(v) => v.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(CatalogError::AccessRootNotFound {
				ac: name.to_owned(),
			}
			.into());
		}
	};
	crate::legacy::alter_access_statement_apply(this, &mut ac)?;
	let ac = ac.to_stored();
	let key = crate::key::root::ac::AccessKey {
		ac: Cow::Borrowed(name),
	};
	txn.set_key(&key, &ac).await?;
	txn.clear_cache();
	Ok(Value::None)
}

pub(crate) async fn alter_access_statement_compute_ns(
	this: &AlterAccessStatement,
	ctx: &FrozenContext,
	opt: &Options,
	name: &str,
) -> Result<Value> {
	let txn = ctx.tx();
	let ns = ctx.get_ns_id(opt).await?;
	let mut ac = match txn.get_ns_access(ns, name, None).await? {
		Some(v) => v.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(CatalogError::AccessNsNotFound {
				ac: name.to_owned(),
				ns: opt.ns()?.to_string(),
			}
			.into());
		}
	};
	crate::legacy::alter_access_statement_apply(this, &mut ac)?;
	let ac = ac.to_stored();
	let key = crate::key::namespace::ac::AccessKey {
		ns,
		ac: Cow::Borrowed(name),
	};
	txn.set_key(&key, &ac).await?;
	txn.clear_cache();
	Ok(Value::None)
}

pub(crate) async fn alter_access_statement_compute_db(
	this: &AlterAccessStatement,
	ctx: &FrozenContext,
	opt: &Options,
	name: &str,
) -> Result<Value> {
	let txn = ctx.tx();
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let mut ac = match txn.get_db_access(ns, db, name, None).await? {
		Some(v) => v.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			}
			let (ns_name, db_name) = opt.ns_db()?;
			return Err(CatalogError::AccessDbNotFound {
				ac: name.to_owned(),
				ns: ns_name.to_string(),
				db: db_name.to_string(),
			}
			.into());
		}
	};
	crate::legacy::alter_access_statement_apply(this, &mut ac)?;
	let ac = ac.to_stored();

	let key = crate::key::database::ac::AccessKey {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		ac: Cow::Borrowed(name),
	};
	txn.set_key(&key, &ac).await?;
	txn.clear_cache();
	Ok(Value::None)
}

pub(crate) fn alter_access_statement_apply(
	this: &AlterAccessStatement,
	ac: &mut catalog::AccessDefinition,
) -> Result<()> {
	match this.authenticate {
		AlterKind::Set(ref v) => ac.authenticate = Some(v.clone()),
		AlterKind::Drop => ac.authenticate = None,
		AlterKind::None => {}
	}
	match this.grant_duration {
		AlterKind::Set(v) => ac.grant_duration = v,
		AlterKind::Drop => ac.grant_duration = None,
		AlterKind::None => {}
	}
	match this.token_duration {
		AlterKind::Set(v) => ac.token_duration = v,
		AlterKind::Drop => ac.token_duration = None,
		AlterKind::None => {}
	}
	match this.session_duration {
		AlterKind::Set(v) => ac.session_duration = v,
		AlterKind::Drop => ac.session_duration = None,
		AlterKind::None => {}
	}
	match this.comment {
		AlterKind::Set(ref v) => ac.comment = Some(v.clone()),
		AlterKind::Drop => ac.comment = None,
		AlterKind::None => {}
	}
	// Mirrors the check in `DefineAccessStatement::to_definition`: tokens
	// issued by record-access methods must expire, so reject ALTER paths
	// that would leave the resulting `token_duration` as NONE.
	if matches!(ac.access_type, catalog::AccessType::Record(_)) && ac.token_duration.is_none() {
		bail!(ExecError::AccessRecordTokenDurationRequired);
	}
	Ok(())
}
