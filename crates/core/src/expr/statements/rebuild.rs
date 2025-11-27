use std::fmt;
use std::fmt::{Display, Formatter};

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::providers::TableProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::Base;
use crate::expr::statements::define::run_indexing;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum RebuildStatement {
	Index(RebuildIndexStatement),
}

impl RebuildStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		match self {
			Self::Index(s) => s.compute(ctx, opt).await,
		}
	}
}

impl ToSql for RebuildStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::rebuild::RebuildStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct RebuildIndexStatement {
	pub name: String,
	pub what: String,
	pub if_exists: bool,
	pub concurrently: bool,
}

impl RebuildIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
		// Get the index definition
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let res = ctx.tx().get_tb_index(ns, db, &self.what, &self.name).await?;
		let ix = match res {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::IxNotFound {
						name: self.name.clone(),
					}
					.into());
				}
			}
		};
		let tb = ctx.tx().expect_tb(ns, db, &self.what).await?;

		// Rebuild the index
		run_indexing(ctx, opt, tb.table_id, ix, !self.concurrently).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for RebuildIndexStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::rebuild::RebuildIndexStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
