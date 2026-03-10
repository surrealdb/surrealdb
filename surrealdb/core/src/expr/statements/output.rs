use std::collections::BTreeSet;

use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::fetch::Fetchs;
use crate::expr::{ControlFlow, Expr, FlowResult};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct OutputStatement {
	pub what: Expr,
	pub fetch: Option<Fetchs>,
}

impl OutputStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		self.what.read_only()
	}

	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "OutputStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		// Process the output value
		let mut value = stk.run(|stk| self.what.compute(stk, ctx, opt, doc)).await?;
		// Fetch any
		if let Some(fetchs) = &self.fetch {
			let mut idioms = BTreeSet::new();
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
