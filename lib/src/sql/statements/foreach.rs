use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::block::{block, Block, Entry};
use crate::sql::comment::{mightbespace, shouldbespace};
use crate::sql::error::{expect_tag_no_case, IResult};
use crate::sql::param::{param, Param};
use crate::sql::value::{value, Value};
use async_recursion::async_recursion;
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
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
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&'async_recursion CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Check the loop data
		match &self.range.compute(ctx, opt, txn, doc).await? {
			Value::Array(arr) => {
				// Loop over the values
				'foreach: for v in arr.iter() {
					// Duplicate context
					let mut ctx = Context::new(ctx);
					// Set the current parameter
					let key = self.param.0.to_raw();
					let val = v.compute(&ctx, opt, txn, doc).await?;
					ctx.add_value(key, val);
					// Loop over the code block statements
					for v in self.block.iter() {
						// Compute each block entry
						let res = match v {
							Entry::Set(v) => {
								let val = v.compute(&ctx, opt, txn, doc).await?;
								ctx.add_value(v.name.to_owned(), val);
								Ok(Value::None)
							}
							Entry::Value(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Break(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Continue(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Foreach(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Ifelse(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Select(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Create(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Update(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Delete(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Relate(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Insert(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Define(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Remove(v) => v.compute(&ctx, opt, txn, doc).await,
							Entry::Output(v) => {
								return v.compute(&ctx, opt, txn, doc).await;
							}
							Entry::Throw(v) => {
								return v.compute(&ctx, opt, txn, doc).await;
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

pub fn foreach(i: &str) -> IResult<&str, ForeachStatement> {
	let (i, _) = tag_no_case("FOR")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, param) = param(i)?;
	let (i, (range, block)) = cut(|i| {
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("IN")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, range) = value(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, block) = block(i)?;
		Ok((i, (range, block)))
	})(i)?;
	Ok((
		i,
		ForeachStatement {
			param,
			range,
			block,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn foreach_statement_first() {
		let sql = "FOR $test IN [1, 2, 3, 4, 5] { UPDATE person:test SET scores += $test; }";
		let res = foreach(sql);
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out))
	}
}
