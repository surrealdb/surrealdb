use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::ident::Ident;
use crate::sql::statements::RemoveIndexStatement;
use crate::sql::value::Value;
use crate::sql::Base;
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum RebuildStatement {
	Index(RebuildIndexStatement),
}

impl RebuildStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		match self {
			Self::Index(s) => s.compute(stk, ctx, opt, doc).await,
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RebuildIndexStatement {
	pub name: Ident,
	pub what: Ident,
	pub if_exists: bool,
}

impl RebuildIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
			// Get the index definition
			let ix = ctx.tx().get_tb_index(opt.ns()?, opt.db()?, &self.what, &self.name).await?;
			// Create the remove statement
			let stm = RemoveIndexStatement {
				name: self.name.clone(),
				what: self.what.clone(),
				if_exists: false,
			};
			// Execute the delete statement
			stm.compute(ctx, opt).await?;
			// Rebuild the index
			ix.compute(stk, ctx, opt, doc).await?;
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::IxNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

impl Display for RebuildIndexStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REBUILD INDEX")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}
