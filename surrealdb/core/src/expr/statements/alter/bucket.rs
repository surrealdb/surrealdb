use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};
use tracing::instrument;

use super::AlterKind;
use crate::catalog::Permission;
use crate::catalog::providers::BucketProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AlterBucketStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub backend: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub readonly: AlterKind<()>,
	pub comment: AlterKind<String>,
}

impl Default for AlterBucketStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			backend: AlterKind::None,
			permissions: None,
			readonly: AlterKind::None,
			comment: AlterKind::None,
		}
	}
}

impl AlterBucketStatement {
	#[instrument(level = "trace", name = "AlterBucketStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Bucket, Base::Db)?;
		let (_, _) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let txn = ctx.tx();
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "bucket name").await?;

		let mut bu = match txn.get_db_bucket(ns, db, &name, None).await? {
			Some(v) => v.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::BuNotFound {
					name: name.clone(),
				}
				.into());
			}
		};

		match self.backend {
			AlterKind::Set(ref v) => bu.backend = Some(v.into()),
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

		let key = crate::key::database::bu::new(ns, db, &name);
		txn.set(&key, &bu).await?;
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
