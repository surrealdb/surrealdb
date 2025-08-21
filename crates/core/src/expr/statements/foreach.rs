use std::fmt::{self, Display};

use reblessive::tree::Stk;

use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Block, ControlFlow, Expr, FlowResult, Param, Value};
use crate::val::range::IntegerRangeIter;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ForeachStatement {
	pub param: Param,
	pub range: Expr,
	pub block: Block,
}

enum ForeachIter {
	Array(std::vec::IntoIter<Value>),
	Range(std::iter::Map<IntegerRangeIter, fn(i64) -> Value>),
}

impl Iterator for ForeachIter {
	type Item = Value;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			ForeachIter::Array(iter) => iter.next(),
			ForeachIter::Range(iter) => iter.next(),
		}
	}
}

impl ForeachStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		self.range.read_only() && self.block.read_only()
	}
	/// Process this type returning a computed simple Value
	///
	/// Was marked recursive
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		// Check the loop data
		let data = stk.run(|stk| self.range.compute(stk, ctx, opt, doc)).await?;
		let iter = match data {
			Value::Array(arr) => ForeachIter::Array(arr.into_iter()),
			Value::Range(r) => {
				let r =
					r.coerce_to_typed::<i64>().map_err(Error::from).map_err(anyhow::Error::new)?;
				ForeachIter::Range(r.iter().map(Value::from))
			}

			v => {
				return Err(ControlFlow::from(anyhow::Error::new(Error::InvalidStatementTarget {
					value: v.to_string(),
				})));
			}
		};

		// Loop over the values
		for v in iter {
			if ctx.is_timedout().await? {
				return Err(ControlFlow::from(anyhow::Error::new(Error::QueryTimedout)));
			}
			// Duplicate context
			let ctx = MutableContext::new(ctx).freeze();
			// Set the current parameter
			let key = self.param.clone().ident().into_string();
			let mut ctx = MutableContext::unfreeze(ctx)?;
			ctx.add_value(key, v.into());
			let mut ctx = Some(ctx.freeze());
			// Loop over the code block statements
			for v in self.block.iter() {
				// Compute each block entry
				let res = match v {
					Expr::Let(x) => x.compute(stk, &mut ctx, opt, doc).await,
					v => stk.run(|stk| v.compute(stk, ctx.as_ref().unwrap(), opt, doc)).await,
				};
				// Catch any special errors
				match res {
					Err(ControlFlow::Continue) => break,
					Err(ControlFlow::Break) => return Ok(Value::None),
					Err(err) => return Err(err),
					_ => (),
				};
			}
			// Cooperatively yield if the task has been running for too long.
			yield_now!();
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for ForeachStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FOR {} IN {} {}", self.param, self.range, self.block)
	}
}
