use std::borrow::Cow;
use std::ops::Deref;

use anyhow::{Result, ensure};
use reblessive::tree::Stk;
use tracing::instrument;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{self, Error as CatalogError};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::Error as ExecError;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::field::{AlterDefault, AlterFieldStatement};
use crate::expr::statements::define::kind_contains_object;
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::key::schema::FieldKey;
use crate::legacy::{expr_to_ident, expr_to_idiom};
use crate::val::{TableName, Value};

#[instrument(level = "trace", name = "AlterFieldStatement::compute", skip_all)]
pub(crate) async fn alter_field_statement_compute(
	this: &AlterFieldStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Field, Base::Db)?;
	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	// Fetch the transaction
	let txn = ctx.tx();
	let idiom = expr_to_idiom(stk, ctx, opt, doc, &this.name, "field name").await?;
	let name = idiom.to_raw_string();
	let what = TableName::new(expr_to_ident(stk, ctx, opt, doc, &this.what, "table name").await?);
	// Get the table definition
	let mut df = match txn.get_tb_field(ns, db, &what, &name, None).await? {
		Some(tb) => tb.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			}

			return Err(CatalogError::FdNotFound {
				name,
			}
			.into());
		}
	};

	// Snapshot the definition before mutating it so we can tell which
	// reference target tables the change drops.
	let old_definition = df.clone();

	match this.kind {
		AlterKind::Set(ref k) => df.field_kind = Some(k.clone()),
		AlterKind::Drop => df.field_kind = None,
		AlterKind::None => {}
	}
	match this.flexible {
		AlterKind::Set(_) => df.flexible = true,
		AlterKind::Drop => df.flexible = false,
		AlterKind::None => {}
	}

	match this.readonly {
		AlterKind::Set(_) => df.readonly = true,
		AlterKind::Drop => df.readonly = false,
		AlterKind::None => {}
	}

	match this.value {
		AlterKind::Set(ref k) => df.value = Some(k.clone()),
		AlterKind::Drop => df.value = None,
		AlterKind::None => {}
	}

	match this.assert {
		AlterKind::Set(ref k) => df.assert = Some(k.clone()),
		AlterKind::Drop => df.assert = None,
		AlterKind::None => {}
	}

	match this.default {
		AlterDefault::None => {}
		AlterDefault::Drop => df.default = catalog::DefineDefault::None,
		AlterDefault::Always(ref expr) => df.default = catalog::DefineDefault::Always(expr.clone()),
		AlterDefault::Set(ref expr) => df.default = catalog::DefineDefault::Set(expr.clone()),
	}

	if let Some(permissions) = &this.permissions {
		df.select_permission = permissions.select.clone();
		df.create_permission = permissions.create.clone();
		df.update_permission = permissions.update.clone();
	}

	match this.comment {
		AlterKind::Set(ref k) => df.comment = Some(k.clone()),
		AlterKind::Drop => df.comment = None,
		AlterKind::None => {}
	}

	match this.reference {
		AlterKind::Set(ref k) => df.reference = Some(k.clone()),
		AlterKind::Drop => df.reference = None,
		AlterKind::None => {}
	}

	// Recompute auth_limit from the current principal to prevent privilege escalation
	df.auth_limit = AuthLimit::new_from_auth(opt.auth.as_ref()).into();

	// The `id` field forbids the same clauses on ALTER as on DEFINE — VALUE,
	// REFERENCE, COMPUTED, DEFAULT ALWAYS, READONLY, FLEXIBLE, and non-key
	// TYPEs — validated against the fully-resolved definition. Without this,
	// ALTER FIELD silently bypassed the restrictions DEFINE FIELD enforces.
	crate::legacy::expr::statements::define::field::validate_id_field_restrictions(&df)?;

	if df.flexible {
		ensure!(
			df.field_kind.as_ref().is_some_and(kind_contains_object),
			ExecError::Thrown("FLEXIBLE can only be used with types containing object".into())
		);
		let Some(tb) = txn.get_tb(ns, db, &what, None).await? else {
			return Err(CatalogError::TbNotFound {
				name: what.clone(),
			}
			.into());
		};
		ensure!(
			tb.schemafull,
			ExecError::Thrown("FLEXIBLE can only be used in SCHEMAFULL tables".into())
		);
	}

	// ALTER stores the same shape DEFINE does, so the assembled definition must
	// satisfy the same read-only rules: the SELECT guard never modifies data
	// (GHSA-66r2-5gwj-gxm2), and the create/update guards only when the
	// `mutable_permissions` capability is enabled.
	crate::fnc::mutability::ensure_permission_clauses_read_only(
		ctx,
		opt,
		"field",
		name.clone(),
		[&df.select_permission],
		[&df.create_permission, &df.update_permission],
	)
	.await?;

	let key = FieldKey {
		ns,
		db,
		tb: Cow::Borrowed(&what),
		fd: Cow::Borrowed(&name),
	};
	txn.set_key(&key, &df.to_stored()).await?;
	// Dropping the REFERENCE clause or narrowing/changing the record kind can
	// strand reference keys under target tables the field no longer
	// references. Purge them so the DELETE reference-purge gate stays sound.
	// Skipped during import, which restores reference keys verbatim.
	if !opt.import {
		crate::legacy::expr::statements::define::field::purge_dropped_reference_keys(
			&txn,
			ns,
			db,
			&what,
			&old_definition,
			Some(&df),
		)
		.await?;
	}
	// Refresh the table cache
	let Some(tb) = txn.get_tb(ns, db, &what, None).await? else {
		return Err(CatalogError::TbNotFound {
			name: what.clone(),
		}
		.into());
	};
	txn.put_tb(
		ns_name,
		db_name,
		&catalog::TableDefinition {
			cache_fields_ts: Uuid::now_v7(),
			..(*tb).clone()
		},
	)
	.await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
