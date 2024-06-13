use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{block::Entry, Block, Param, Value};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ForeachStatement {
	pub param: Param,
	pub range: Value,
	pub block: Block,
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
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Check the loop data
		match &self.range.compute(stk, ctx, opt, doc).await? {
			Value::Array(arr) => {
				// Loop over the values
				'foreach: for v in arr.iter() {
					// Duplicate context
					let mut ctx = Context::new(ctx);
					// Set the current parameter
					let key = self.param.0.to_raw();
					let val = stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await?;
					ctx.add_value(key, val);
					// Loop over the code block statements
					for v in self.block.iter() {
						// Compute each block entry
						let res = match v {
							Entry::Set(v) => {
								let val = stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await?;
								ctx.add_value(v.name.to_owned(), val);
								Ok(Value::None)
							}
							Entry::Value(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
							Entry::Break(v) => v.compute(&ctx, opt, doc).await,
							Entry::Continue(v) => v.compute(&ctx, opt, doc).await,
							Entry::Foreach(v) => {
								stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await
							}
							Entry::Ifelse(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
							Entry::Select(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
							Entry::Create(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
							Entry::Upsert(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
							Entry::Update(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
							Entry::Delete(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
							Entry::Relate(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
							Entry::Insert(v) => stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await,
							Entry::Define(v) => v.compute(stk, &ctx, opt, doc).await,
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
			v => Err(Error::InvalidStatementTarget {
				value: v.to_string(),
			}),
		}
	}
}

impl Display for ForeachStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FOR {} IN {} {}", self.param, self.range, self.block)
	}
}
