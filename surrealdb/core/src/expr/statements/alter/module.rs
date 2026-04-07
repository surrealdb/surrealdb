use std::ops::Deref;

use anyhow::Result;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{ModuleName, Permission};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AlterModuleStatement {
	pub name: ModuleName,
	pub if_exists: bool,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
}

impl AlterModuleStatement {
	#[instrument(level = "trace", name = "AlterModuleStatement::compute", skip_all)]
	pub(crate) async fn compute(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		opt.is_allowed(Action::Edit, ResourceKind::Module, &Base::Db)?;
		let (_, _) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let txn = ctx.tx();

		let storage_name = self.name.get_storage_name();
		let mut md = match txn.get_db_module(ns, db, &storage_name).await {
			Ok(v) => v.deref().clone(),
			Err(e) => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(e);
			}
		};

		match self.comment {
			AlterKind::Set(ref v) => md.comment = Some(v.clone()),
			AlterKind::Drop => md.comment = None,
			AlterKind::None => {}
		}

		if let Some(ref p) = self.permissions {
			md.permissions = p.clone();
		}

		txn.put_db_module(ns, db, &md).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}

impl ToSql for AlterModuleStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterModuleStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
