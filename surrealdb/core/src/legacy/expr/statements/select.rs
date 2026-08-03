use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Result, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::FrozenContext;
use crate::dbs::{Iterator, Options, Statement};
use crate::doc::{CursorDoc, NsDbCtx};
use crate::exe::FlowResultExt as _;
use crate::exec::Error as ExecError;
use crate::expr::order::Ordering;
use crate::expr::statements::select::SelectStatement;
use crate::expr::{Expr, Field, Fields, Idiom};
use crate::idx::planner::{QueryPlanner, RecordStrategy, StatementContext};
use crate::val::{Datetime, Value};

/// Return the first `ORDER BY` idiom this statement sorts on that the
/// projection does not carry, or `None` when every sort key survives
/// projection.
///
/// `SELECT *` carries every field, and `SELECT VALUE` has its projection
/// deferred until after the sort, so both are always covered.
fn uncovered_order_idiom(stm: &SelectStatement) -> Option<String> {
	let Some(Ordering::Order(orders)) = &stm.order else {
		return None;
	};
	let Fields::Select(fields) = &stm.fields else {
		return None;
	};
	if stm.fields.has_all_selection() {
		return None;
	}

	let covered = |idiom: &Idiom| {
		fields.iter().any(|field| {
			let Field::Single(selector) = field else {
				// `Field::All` is handled by `has_all_selection` above.
				return true;
			};
			if selector.alias.as_ref().is_some_and(|alias| alias == idiom) {
				return true;
			}
			match &selector.expr {
				Expr::Idiom(x) => x == idiom,
				v => v.to_idiom() == *idiom,
			}
		})
	};

	orders.iter().find(|order| !covered(&order.value)).map(|order| order.value.to_sql())
}

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "SelectStatement::compute", skip_all)]
pub(crate) async fn select_statement_compute(
	this: &SelectStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	parent_doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Valid options?
	opt.valid_for_db()?;
	// Assign the statement
	let stm = Statement::from_select(stk, ctx, opt, parent_doc, this).await?;
	// Create a new iterator
	let mut iterator = Iterator::new();
	// Ensure futures are stored and the version is set if specified

	let ts_impl = ctx.tx().timestamp_impl();
	let version = stk
		.run(|stk| crate::legacy::expr_compute(&this.version, stk, ctx, opt, parent_doc))
		.await
		.catch_return()?
		.cast_to::<Option<Datetime>>()?
		.map(|x| x.to_version_stamp(ts_impl.as_ref()))
		.transpose()?;
	let opt = Arc::new(opt.clone().with_version(version.or(opt.version)));

	// Extract the limits
	iterator.setup_limit(stk, ctx, &opt, &stm).await?;
	// Fail for multiple targets without a limit
	ensure!(
		!this.only || iterator.is_limit_one_or_zero() || this.what.len() <= 1,
		ExecError::SingleOnlyOutput
	);
	// Check if there is a timeout
	// This is calculated on the parent doc
	let ctx = stm.setup_timeout(stk, ctx, &opt, parent_doc).await?;

	// Get a query planner
	let mut planner = QueryPlanner::new();

	let stm_ctx = StatementContext::new(&ctx, &opt, &stm)?;

	let txn = ctx.tx();
	let ns = txn.expect_ns_by_name(opt.ns()?).await?;
	let db = txn.expect_db_by_name(opt.ns()?, opt.db()?).await?;
	let doc_ctx = NsDbCtx {
		ns: Arc::clone(&ns),
		db: Arc::clone(&db),
	};

	// The legacy pipeline projects each document during iteration and only
	// defers projection for `SELECT VALUE`, so a sort key outside the
	// projection is already gone by the time `Results::sort` runs and the
	// rows would come back in scan order. The streaming executor sorts the
	// full record before projecting and has no such limit; reject the shape
	// here rather than return a silently mis-ordered result.
	if let Some(idiom) = uncovered_order_idiom(this) {
		return Err(anyhow::Error::new(ExecError::Query {
			message: format!(
				"Cannot ORDER BY `{idiom}` because it is not in the statement selection. \
				 Either add it to the selection, or use a planner strategy other than \
				 'compute-only'."
			),
		}));
	}

	// Reject VERSION with subquery sources
	if opt.version.is_some() {
		for w in this.what.iter() {
			if matches!(w, Expr::Select(_)) {
				return Err(anyhow::Error::new(ExecError::Query {
					message: "VERSION clause cannot be used with a subquery source. \
								  Place the VERSION clause inside the subquery instead."
						.to_string(),
				}));
			}
		}
	}

	// `$parent` must be visible while planning FROM targets (e.g. `FROM
	// $parent->edge`), not only during output — same binding as
	// `CursorDoc::update_parent`.
	let prepare_ctx: Cow<'_, FrozenContext> = CursorDoc::with_parent_ctx(&ctx, parent_doc);

	// Loop over the select targets
	for w in this.what.iter() {
		// The target is also calculated on the parent doc
		iterator
			.prepare(
				stk,
				prepare_ctx.as_ref(),
				&opt,
				parent_doc,
				&mut planner,
				&stm_ctx,
				&doc_ctx,
				w,
			)
			.await?;
	}

	// Reuse `prepare_ctx` so we do not clone the parent document again inside
	// `update_parent` (see `CursorDoc::with_parent_ctx`).
	CursorDoc::update_parent(prepare_ctx.as_ref(), None, async |ctx| {
		// Attach the query planner to the context
		let ctx = stm.setup_query_planner(planner, ctx);
		// Process the statement
		let res =
			iterator.output(stk, ctx.as_ref(), &opt, &stm, RecordStrategy::KeysAndValues).await?;
		// Catch statement timeout
		ctx.expect_not_timedout().await?;

		if this.only {
			match res {
				Value::Array(mut array) => {
					if array.is_empty() {
						Ok(Value::None)
					} else {
						ensure!(array.len() == 1, ExecError::SingleOnlyOutput);
						Ok(array.0.pop().expect("array has exactly one element"))
					}
				}
				x => Ok(x),
			}
		} else {
			Ok(res)
		}
	})
	.await
}
