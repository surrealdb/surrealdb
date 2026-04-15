use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::catalog::Permission;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Base, Expr, FlowResultExt};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterParamStatement {
	pub name: String,
	pub if_exists: bool,
	pub value: Option<Expr>,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
}

impl AlterParamStatement {
	#[instrument(level = "trace", name = "AlterParamStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		opt.is_allowed(Action::Edit, ResourceKind::Parameter, &Base::Db)?;
		let (_, _) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let txn = ctx.tx();

		let mut pa = match txn.get_db_param(ns, db, &self.name, None).await {
			Ok(v) => v.deref().clone(),
			Err(e) => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(e);
			}
		};

		if let Some(ref v) = self.value {
			pa.value = stk.run(|stk| v.compute(stk, ctx, opt, doc)).await.catch_return()?;
		}

		match self.comment {
			AlterKind::Set(ref v) => pa.comment = Some(v.clone()),
			AlterKind::Drop => pa.comment = None,
			AlterKind::None => {}
		}

		if let Some(ref p) = self.permissions {
			pa.permissions = p.clone();
		}

		let key = crate::key::database::pa::new(ns, db, &self.name);
		txn.set(&key, &pa).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}

impl ToSql for AlterParamStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterParamStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
