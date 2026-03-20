use std::ops::Deref;

use anyhow::Result;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::catalog::Permission;
use crate::catalog::providers::BucketProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterBucketStatement {
	pub name: String,
	pub if_exists: bool,
	pub backend: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub readonly: AlterKind<()>,
	pub comment: AlterKind<String>,
}

impl AlterBucketStatement {
	#[instrument(level = "trace", name = "AlterBucketStatement::compute", skip_all)]
	pub(crate) async fn compute(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		opt.is_allowed(Action::Edit, ResourceKind::Bucket, &Base::Db)?;
		let (_, _) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let txn = ctx.tx();

		let mut bu = match txn.get_db_bucket(ns, db, &self.name).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::BuNotFound {
					name: self.name.clone(),
				}
				.into());
			}
		};

		match self.backend {
			AlterKind::Set(ref v) => bu.backend = Some(v.clone()),
			AlterKind::Drop => bu.backend = None,
			AlterKind::None => {}
		}

		if let Some(ref p) = self.permissions {
			bu.permissions = p.clone();
		}

		match self.readonly {
			AlterKind::Set(_) => bu.readonly = true,
			AlterKind::Drop => bu.readonly = false,
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref v) => bu.comment = Some(v.clone()),
			AlterKind::Drop => bu.comment = None,
			AlterKind::None => {}
		}

		let key = crate::key::database::bu::new(ns, db, &self.name);
		txn.set(&key, &bu, None).await?;
		txn.clear_cache();
		Ok(Value::None)
	}
}

impl ToSql for AlterBucketStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterBucketStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
