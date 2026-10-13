use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::show::ShowStatement;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "ShowStatement::compute", skip_all)]
pub(crate) async fn show_statement_compute(
	this: &ShowStatement,
	ctx: &FrozenContext,
	opt: &Options,
	_doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::View, ResourceKind::Table, Base::Db)?;
	// Get the transaction
	let txn = ctx.tx();
	// Process the show query
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let r =
		crate::cf::read(&txn, ns, db, this.table.as_ref(), this.since.clone(), this.limit).await?;
	// Return the changes
	let a = r.iter().cloned().map(|x| x.into_value()).collect::<Result<Vec<Value>>>()?;
	Ok(a.into())
}
