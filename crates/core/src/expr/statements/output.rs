use std::fmt;

use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::fetch::Fetchs;
use crate::expr::{ControlFlow, Expr, FlowResult};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct OutputStatement {
	pub what: Expr,
	pub fetch: Option<Fetchs>,
}

impl OutputStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		self.what.read_only()
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		// Process the output value
		let mut value = stk.run(|stk| self.what.compute(stk, ctx, opt, doc)).await?;
		// Fetch any
		if let Some(fetchs) = &self.fetch {
			let mut idioms = Vec::with_capacity(fetchs.0.len());
			for fetch in fetchs.iter() {
				fetch.compute(stk, ctx, opt, &mut idioms).await?
			}
			for i in &idioms {
				value.fetch(stk, ctx, opt, i).await?;
			}
		}
		//
		Err(ControlFlow::Return(value))
	}
}

impl fmt::Display for OutputStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RETURN {}", self.what)?;
		if let Some(ref v) = self.fetch {
			write!(f, " {v}")?
		}
		Ok(())
	}
}
