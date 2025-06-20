use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Block, Param, Value};
use crate::expr::{ControlFlow, Expr, FlowResult};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ForeachStatement {
	pub param: Param,
	pub range: Expr,
	pub block: Block,
}

enum ForeachIter {
	Array(std::vec::IntoIter<Value>),
	Range(std::iter::Map<TypedRange<i64>, fn(i64) -> Value>),
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
		let data = self.range.compute(stk, ctx, opt, doc).await?;
		let iter = match data {
			Value::Array(arr) => ForeachIter::Array(arr.into_iter()),
			Value::Range(r) => {
				let r =
					r.coerce_to_typed::<i64>().map_err(Error::from).map_err(anyhow::Error::new)?;
				ForeachIter::Range(r.map(Value::from))
			}
			Value::Future(fut) => {
				let result = fut.compute(stk, ctx, opt, doc).await?;

				// We only accept arrays as output of a future in a foreach statement.
				match result {
					Value::Array(arr) => ForeachIter::Array(arr.into_iter()),
					v => {
						return Err(ControlFlow::from(anyhow::Error::new(
							Error::InvalidStatementTarget {
								value: v.to_string(),
							},
						)));
					}
				}
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
			let key = self.param.0.to_raw();
			let val = stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await?;
			let mut ctx = MutableContext::unfreeze(ctx)?;
			ctx.add_value(key, val.into());
			let mut ctx = ctx.freeze();
			// Loop over the code block statements
			for v in self.block.iter() {
				// Compute each block entry
				let res = match v {
					Entry::Set(v) => {
						let val = stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await?;
						let mut c = MutableContext::unfreeze(ctx)?;
						c.add_value(v.name.clone(), val.into());
						ctx = c.freeze();
						Ok(Value::None)
					}
					Entry::Value(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Break(v) => v.compute(&ctx, opt, doc).await,
					Entry::Continue(v) => v.compute(&ctx, opt, doc).await,
					Entry::Foreach(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Ifelse(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Select(v) => Ok(stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await?),
					Entry::Create(v) => Ok(stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await?),
					Entry::Upsert(v) => Ok(stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await?),
					Entry::Update(v) => Ok(stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await?),
					Entry::Delete(v) => Ok(stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await?),
					Entry::Relate(v) => Ok(stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await?),
					Entry::Insert(v) => Ok(stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await?),
					Entry::Define(v) => Ok(v.compute(stk, &ctx, opt, doc).await?),
					Entry::Alter(v) => Ok(v.compute(stk, &ctx, opt, doc).await?),
					Entry::Rebuild(v) => Ok(v.compute(stk, &ctx, opt, doc).await?),
					Entry::Remove(v) => Ok(v.compute(&ctx, opt, doc).await?),
					Entry::Info(v) => Ok(v.compute(stk, &ctx, opt, doc).await?),
					Entry::Output(v) => {
						return stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await;
					}
					Entry::Throw(v) => return stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
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
