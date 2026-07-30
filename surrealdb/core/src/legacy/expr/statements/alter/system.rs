use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::system::AlterSystemStatement;
use crate::iam::{Action, ResourceKind};
use crate::val::{Duration, Value};

pub(crate) async fn alter_system_statement_compute(
	this: &AlterSystemStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> anyhow::Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Any, Base::Root)?;
	// Are we doing compaction?
	if this.compact {
		ctx.tx().compact_all().await?;
	}
	match &this.query_timeout {
		AlterKind::None => {}
		AlterKind::Set(timeout) => {
			let timeout = stk
				.run(|stk| crate::legacy::expr_compute(timeout, stk, ctx, opt, doc))
				.await
				.catch_return()?
				.cast_to::<Duration>()?;
			ctx.dynamic_configuration().set_query_timeout(Some(timeout.0));
		}
		AlterKind::Drop => {
			ctx.dynamic_configuration().set_query_timeout(None);
		}
	}
	Ok(Value::None)
}
