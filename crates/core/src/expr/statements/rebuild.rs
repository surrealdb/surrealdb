use std::fmt;
use std::fmt::{Display, Formatter};

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::TableProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::Base;
use crate::expr::statements::define::run_indexing;
use crate::fmt::EscapeIdent;
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

impl Display for RebuildStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Index(v) => Display::fmt(v, f),
		}
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

impl Display for RebuildIndexStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REBUILD INDEX")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", EscapeIdent(&self.name), EscapeIdent(&self.what))?;
		if self.concurrently {
			write!(f, " CONCURRENTLY")?
		}
		Ok(())
	}
}
