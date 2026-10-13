use std::collections::BTreeSet;

use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::output::OutputStatement;
use crate::expr::{ControlFlow, FlowResult};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "OutputStatement::compute", skip_all)]
pub(crate) async fn output_statement_compute(
	this: &OutputStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	// Process the output value
	let mut value =
		stk.run(|stk| crate::legacy::expr_compute(&this.what, stk, ctx, opt, doc)).await?;
	// Fetch any
	if let Some(fetchs) = &this.fetch {
		let mut idioms = BTreeSet::new();
		for fetch in fetchs.iter() {
			crate::legacy::fetch_compute(fetch, stk, ctx, opt, &mut idioms).await?
		}
		for i in &idioms {
			crate::legacy::value_fetch(&mut value, stk, ctx, opt, i).await?;
		}
	}
	//
	Err(ControlFlow::Return(value))
}
