use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{block::Entry, Block, Param, Value};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ForeachStatement {
	pub param: Param,
	pub range: Value,
	pub block: Block,
}

enum ForeachIter {
	Array(std::vec::IntoIter<Value>),
	Range(std::iter::Map<std::ops::Range<i64>, fn(i64) -> Value>),
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
	pub(crate) fn writeable(&self) -> bool {
		self.range.writeable() || self.block.writeable()
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
	) -> Result<Value, Error> {
		// Check the loop data
		let data = self.range.compute(stk, ctx, opt, doc).await?;
		let iter = match data {
			Value::Array(arr) => ForeachIter::Array(arr.into_iter()),
			Value::Range(r) => {
				let r: std::ops::Range<i64> = r.deref().to_owned().try_into()?;
				ForeachIter::Range(r.map(Value::from))
			}

			v => {
				return Err(Error::InvalidStatementTarget {
					value: v.to_string(),
				})
			}
		};

		// Loop over the values
		'foreach: for v in iter {
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
						c.add_value(v.name.to_owned(), val.into());
						ctx = c.freeze();
						Ok(Value::None)
					}
					Entry::Value(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Break(v) => v.compute(&ctx, opt, doc).await,
					Entry::Continue(v) => v.compute(&ctx, opt, doc).await,
					Entry::Foreach(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Ifelse(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Select(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Create(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Upsert(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Update(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Delete(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Relate(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Insert(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
					Entry::Define(v) => v.compute(stk, &ctx, opt, doc).await,
					Entry::Alter(v) => v.compute(stk, &ctx, opt, doc).await,
					Entry::Rebuild(v) => v.compute(stk, &ctx, opt, doc).await,
					Entry::Remove(v) => v.compute(&ctx, opt, doc).await,
					Entry::Output(v) => {
						return stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await;
					}
					Entry::Throw(v) => {
						return stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await;
					}
				};
				// Catch any special errors
				match res {
					Err(Error::Continue) => continue 'foreach,
					Err(Error::Break) => return Ok(Value::None),
					Err(err) => return Err(err),
					_ => (),
				};
			}
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
