use super::FlowResultExt;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::Expr;
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Version(pub Expr);

impl Version {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<u64> {
		match self.0.compute(stk, ctx, opt, doc).await.catch_return()? {
			Value::Datetime(v) => match v.to_u64() {
				Some(ts) => Ok(ts),
				_ => Err(anyhow::Error::new(Error::unreachable(
					"Failed to convert datetime to timestamp",
				))),
			},
			found => Err(anyhow::Error::new(Error::InvalidVersion {
				found,
			})),
		}
	}
}

impl fmt::Display for Version {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "VERSION {}", self.0)
	}
}
