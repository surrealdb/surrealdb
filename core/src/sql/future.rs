use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::block::Block;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Future";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
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
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Process the future if enabled
		match opt.futures {
			true => stk.run(|stk| self.0.compute(stk, ctx, opt, doc)).await?.ok(),
			false => Ok(self.clone().into()),
		}
	}
}

impl fmt::Display for Future {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<future> {}", self.0)
	}
}
