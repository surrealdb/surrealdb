use std::fmt;

use anyhow::Result;
use reblessive::tree::Stk;

use super::FlowResultExt as _;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::Expr;
use crate::expr::expression::VisitExpression;
use crate::val::{Number, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Start(pub(crate) Expr);

impl Start {
	pub(crate) async fn process(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<u32> {
		match stk.run(|stk| self.0.compute(stk, ctx, opt, doc)).await.catch_return() {
			// This is a valid starting number
			Ok(Value::Number(Number::Int(v))) if v >= 0 => {
				if v > u32::MAX as i64 {
					Err(anyhow::Error::new(Error::InvalidStart {
						value: v.to_string(),
					}))
				} else {
					Ok(v as u32)
				}
			}
			// An invalid value was specified
			Ok(v) => Err(anyhow::Error::new(Error::InvalidStart {
				value: v.into_raw_string(),
			})),
			// A different error occurred
			Err(e) => Err(e),
		}
	}
}

impl VisitExpression for Start {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.0.visit(visitor);
	}
}

impl fmt::Display for Start {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "START {}", self.0)
	}
}
