use std::fmt;

use anyhow::Result;
use reblessive::tree::Stk;

use super::FlowResultExt as _;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::Expr;
use crate::val::{Number, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Limit(pub Expr);

impl Limit {
	pub(crate) async fn process(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<u32> {
		match stk.run(|stk| self.0.compute(stk, ctx, opt, doc)).await.catch_return() {
			// This is a valid limiting number
			Ok(Value::Number(Number::Int(v))) if v >= 0 => {
				if v > u32::MAX as i64 {
					Err(anyhow::Error::new(Error::InvalidLimit {
						value: v.to_string(),
					}))
				} else {
					Ok(v as u32)
				}
			}
			// An invalid value was specified
			Ok(v) => Err(anyhow::Error::new(Error::InvalidLimit {
				value: v.as_raw_string(),
			})),
			// A different error occurred
			Err(e) => Err(e),
		}
	}
}

impl fmt::Display for Limit {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LIMIT {}", self.0)
	}
}
