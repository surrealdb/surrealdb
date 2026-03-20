use std::ops::Deref;

use anyhow::Result;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::catalog::Permission;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::{Base, Block, Kind};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterFunctionStatement {
	pub name: String,
	pub if_exists: bool,
	pub args: AlterKind<Vec<(String, Kind)>>,
	pub block: AlterKind<Block>,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub returns: AlterKind<Kind>,
}

impl AlterFunctionStatement {
	#[instrument(level = "trace", name = "AlterFunctionStatement::compute", skip_all)]
	pub(crate) async fn compute(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		opt.is_allowed(Action::Edit, ResourceKind::Function, &Base::Db)?;
		let (_, _) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let txn = ctx.tx();

		let mut fc = match txn.get_db_function(ns, db, &self.name).await {
			Ok(v) => v.deref().clone(),
			Err(e) => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(e);
			}
		};

		match self.args {
			AlterKind::Set(ref v) => fc.args.clone_from(v),
			AlterKind::Drop => fc.args = vec![],
			AlterKind::None => {}
		}

		match self.block {
			AlterKind::Set(ref v) => fc.block = v.clone(),
			AlterKind::Drop => {}
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref v) => fc.comment = Some(v.clone()),
			AlterKind::Drop => fc.comment = None,
			AlterKind::None => {}
		}

		if let Some(ref p) = self.permissions {
			fc.permissions = p.clone();
		}

		match self.returns {
			AlterKind::Set(ref v) => fc.returns = Some(v.clone()),
			AlterKind::Drop => fc.returns = None,
			AlterKind::None => {}
		}

		let key = crate::key::database::fc::new(ns, db, &self.name);
		txn.set(&key, &fc, None).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}

impl ToSql for AlterFunctionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterFunctionStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
