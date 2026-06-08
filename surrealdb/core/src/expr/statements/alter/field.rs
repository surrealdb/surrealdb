use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};
use tracing::instrument;
use uuid::Uuid;

use super::AlterKind;
use crate::catalog::providers::TableProvider;
use crate::catalog::{self, Permission, Permissions, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::{expr_to_ident, expr_to_idiom};
use crate::expr::reference::Reference;
use crate::expr::{Base, Expr, Kind, Literal};
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::val::{TableName, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum AlterDefault {
	#[default]
	None,
	Drop,
	Always(Expr),
	Set(Expr),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AlterFieldStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
	pub kind: AlterKind<Kind>,
	pub flexible: AlterKind<()>,
	pub readonly: AlterKind<()>,
	pub value: AlterKind<Expr>,
	pub assert: AlterKind<Expr>,
	pub default: AlterDefault,
	pub permissions: Option<Permissions>,
	pub comment: AlterKind<String>,
	pub reference: AlterKind<Reference>,
}

impl Default for AlterFieldStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			if_exists: false,
			kind: AlterKind::None,
			flexible: AlterKind::None,
			readonly: AlterKind::None,
			value: AlterKind::None,
			assert: AlterKind::None,
			default: AlterDefault::None,
			permissions: None,
			comment: AlterKind::None,
			reference: AlterKind::None,
		}
	}
}

impl AlterFieldStatement {
	#[instrument(level = "trace", name = "AlterFieldStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
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
		let idiom = expr_to_idiom(stk, ctx, opt, doc, &self.name, "field name").await?;
		let name = idiom.to_raw_string();
		let what =
			TableName::new(expr_to_ident(stk, ctx, opt, doc, &self.what, "table name").await?);
		// Get the table definition
		let mut df = match txn.get_tb_field(ns, db, &what, &name, None).await? {
			Some(tb) => tb.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}

				return Err(Error::FdNotFound {
					name,
				}
				.into());
			}
		};

		match self.kind {
			AlterKind::Set(ref k) => df.field_kind = Some(k.clone()),
			AlterKind::Drop => df.field_kind = None,
			AlterKind::None => {}
		}
		match self.flexible {
			AlterKind::Set(_) => df.flexible = true,
			AlterKind::Drop => df.flexible = false,
			AlterKind::None => {}
		}

		match self.readonly {
			AlterKind::Set(_) => df.readonly = true,
			AlterKind::Drop => df.readonly = false,
			AlterKind::None => {}
		}

		match self.value {
			AlterKind::Set(ref k) => df.value = Some(k.clone()),
			AlterKind::Drop => df.value = None,
			AlterKind::None => {}
		}

		match self.assert {
			AlterKind::Set(ref k) => df.assert = Some(k.clone()),
			AlterKind::Drop => df.assert = None,
			AlterKind::None => {}
		}

		match self.default {
			AlterDefault::None => {}
			AlterDefault::Drop => df.default = catalog::DefineDefault::None,
			AlterDefault::Always(ref expr) => {
				df.default = catalog::DefineDefault::Always(expr.clone())
			}
			AlterDefault::Set(ref expr) => df.default = catalog::DefineDefault::Set(expr.clone()),
		}

		fn convert_permission(perm: &Permission) -> catalog::Permission {
			match perm {
				Permission::None => catalog::Permission::None,
				Permission::Full => catalog::Permission::Full,
				Permission::Specific(expr) => catalog::Permission::Specific(expr.clone()),
			}
		}

		if let Some(permissions) = &self.permissions {
			df.select_permission = convert_permission(&permissions.select);
			df.create_permission = convert_permission(&permissions.create);
			df.update_permission = convert_permission(&permissions.update);
		}

		match self.comment {
			AlterKind::Set(ref k) => df.comment = Some(k.clone()),
			AlterKind::Drop => df.comment = None,
			AlterKind::None => {}
		}

		match self.reference {
			AlterKind::Set(ref k) => df.reference = Some(k.clone()),
			AlterKind::Drop => df.reference = None,
			AlterKind::None => {}
		}

		// Recompute auth_limit from the current principal to prevent privilege escalation
		df.auth_limit = AuthLimit::new_from_auth(opt.auth.as_ref()).into();

		let key = crate::key::table::fd::new(ns, db, &what, &name);
		txn.set(&key, &df).await?;
		// Refresh the table cache
		let Some(tb) = txn.get_tb(ns, db, &what, None).await? else {
			return Err(Error::TbNotFound {
				name: what.clone(),
			}
			.into());
		};
		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_fields_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for AlterFieldStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::field::AlterFieldStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
