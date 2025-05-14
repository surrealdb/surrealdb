use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::block::Block;
use crate::expr::value::Value;
use crate::{ctx::Context, dbs::Futures};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::FlowResultExt as _;

pub(crate) const TOKEN: &str = "$surrealdb::private::expr::Future";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::expr::Future")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Future(pub Block);

impl From<Value> for Future {
	fn from(v: Value) -> Self {
		Future(Block::from(v))
	}
}

impl Future {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Process the future if enabled
		match opt.futures {
			Futures::Enabled => {
				stk.run(|stk| self.0.compute(stk, ctx, opt, doc)).await.catch_return()?.ok()
			}
			_ => Ok(self.clone().into()),
		}
	}
}

crate::expr::impl_display_from_sql!(Future);

impl crate::expr::DisplaySql for Future {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<future> {}", self.0)
	}
}
