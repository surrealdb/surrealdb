use crate::ctx::Context;
use crate::dbs::{Futures, Options};
use crate::doc::CursorDoc;
use crate::expr::block::Block;
use crate::val::Value;
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::FlowResultExt as _;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Future";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::sql::Future")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Future(pub Block);

impl From<Value> for Future {
	fn from(v: Value) -> Self {
		Future(Block::from(v))
	}
}

impl Future {
	pub fn read_only(&self) -> bool {
		self.0.read_only()
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Process the future if enabled
		match opt.futures {
			Futures::Enabled => {
				stk.run(|stk| self.0.compute(stk, ctx, opt, doc)).await.catch_return()?.ok()
			}
			_ => Ok(self.clone().into()),
		}
	}
}

impl fmt::Display for Future {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<future> {}", self.0)
	}
}
