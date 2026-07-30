use std::borrow::Cow;
use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::FrozenContext;
use crate::dbs::{Iterator, Options, Statement};
use crate::doc::{CursorDoc, NsDbCtx};
use crate::exec::Error as ExecError;
use crate::expr::statements::upsert::UpsertStatement;
use crate::idx::planner::{QueryPlanner, RecordStrategy, StatementContext};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "UpsertStatement::compute", skip_all)]
pub(crate) async fn upsert_statement_compute(
	this: &UpsertStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Valid options?
	opt.valid_for_db()?;
	// Create a new iterator
	let mut iterator = Iterator::new();

	// Assign the statement
	let stm = Statement::from(this);
	// Check if there is a timeout
	let ctx = stm.setup_timeout(stk, ctx, opt, doc).await?;

	// Get a query planner
	let mut planner = QueryPlanner::new();

	let stm_ctx = StatementContext::new(&ctx, opt, &stm)?;

	let txn = ctx.tx();
	let ns = txn.expect_ns_by_name(opt.ns()?).await?;
	let db = txn.expect_db_by_name(opt.ns()?, opt.db()?).await?;
	let doc_ctx = NsDbCtx {
		ns: Arc::clone(&ns),
		db: Arc::clone(&db),
	};

	let prepare_ctx: Cow<'_, FrozenContext> = CursorDoc::with_parent_ctx(&ctx, doc);

	// Loop over the upsert targets
	for w in this.what.iter() {
		iterator
			.prepare(stk, prepare_ctx.as_ref(), opt, doc, &mut planner, &stm_ctx, &doc_ctx, w)
			.await
			// `prepare` rejects a target generically; name the statement that
			// rejected it.
			.map_err(|e| match crate::err::exec_error(&e) {
				Some(ExecError::InvalidStatementTarget {
					value,
				}) => anyhow::Error::new(ExecError::UpsertStatement {
					value: value.clone(),
				}),
				_ => e,
			})?;
	}
	CursorDoc::update_parent(prepare_ctx.as_ref(), None, async |ctx| {
		// Attach the query planner to the context
		let ctx = stm.setup_query_planner(planner, ctx);

		// Ensure the database exists.
		ctx.get_db(opt).await?;

		// Process the statement
		let res = iterator.output(stk, &ctx, opt, &stm, RecordStrategy::KeysAndValues).await?;
		// Catch statement timeout
		ctx.expect_not_timedout().await?;
		// Output the results
		match res {
			// This is a single record result
			Value::Array(mut a) if this.only => match a.len() {
				// There was exactly one result
				1 => Ok(a.remove(0)),
				// No results (e.g. record did not exist): return None for backwards
				// compatibility with clients that expect a single value.
				0 => Ok(Value::None),
				// There were no results
				_ => Err(anyhow::Error::new(ExecError::SingleOnlyOutput)),
			},
			// This is standard query result
			v => Ok(v),
		}
	})
	.await
}
