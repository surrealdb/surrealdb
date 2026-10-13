use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::processor::RelateThrough;
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::{CursorDoc, DocumentContext, NsDbCtx};
use crate::exe::FlowResultExt as _;
use crate::exec::Error as ExecError;
use crate::expr::statements::relate::RelateStatement;
use crate::idx::planner::RecordStrategy;
use crate::val::{Duration, Value};

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "RelateStatement::compute", skip_all)]
pub(crate) async fn relate_statement_compute(
	this: &RelateStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Valid options?
	opt.valid_for_db()?;
	// Create a new iterator
	let mut iterator = Iterator::new();
	// Check if there is a timeout
	let ctx_store: FrozenContext;
	let ctx = match stk
		.run(|stk| crate::legacy::expr_compute(&this.timeout, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to::<Option<Duration>>()?
	{
		Some(timeout) => {
			let mut new_ctx = Context::new_child(ctx);
			new_ctx.add_timeout(timeout.0)?;
			ctx_store = new_ctx.freeze();
			&ctx_store
		}
		None => ctx,
	};
	// Loop over the from targets
	let from = {
		let mut out = Vec::new();
		match stk
			.run(|stk| crate::legacy::expr_compute(&this.from, stk, ctx, opt, doc))
			.await
			.catch_return()?
		{
			Value::RecordId(v) => out.push(v),
			Value::Array(v) => {
				for v in v {
					match v {
						Value::RecordId(v) => out.push(v),
						Value::Object(v) => match v.rid() {
							Some(v) => out.push(v),
							_ => {
								bail!(ExecError::RelateStatementIn {
									value: v.to_sql(),
								})
							}
						},
						v => {
							bail!(ExecError::RelateStatementIn {
								value: v.to_sql(),
							})
						}
					}
				}
			}
			Value::Object(v) => match v.rid() {
				Some(v) => out.push(v),
				None => {
					bail!(ExecError::RelateStatementIn {
						value: v.to_sql(),
					})
				}
			},
			v => {
				bail!(ExecError::RelateStatementIn {
					value: v.to_sql(),
				})
			}
		};
		// }
		out
	};
	// Loop over the with targets
	let to = {
		let mut out = Vec::new();
		match stk
			.run(|stk| crate::legacy::expr_compute(&this.to, stk, ctx, opt, doc))
			.await
			.catch_return()?
		{
			Value::RecordId(v) => out.push(v),
			Value::Array(v) => {
				for v in v {
					match v {
						Value::RecordId(v) => out.push(v),
						Value::Object(v) => match v.rid() {
							Some(v) => out.push(v),
							None => {
								bail!(ExecError::RelateStatementId {
									value: v.to_sql(),
								})
							}
						},
						v => {
							bail!(ExecError::RelateStatementId {
								value: v.to_sql(),
							})
						}
					}
				}
			}
			Value::Object(v) => match v.rid() {
				Some(v) => out.push(v),
				None => {
					bail!(ExecError::RelateStatementId {
						value: v.to_sql(),
					})
				}
			},
			v => {
				bail!(ExecError::RelateStatementId {
					value: v.to_sql(),
				})
			}
		};
		out
	};

	let txn = ctx.tx();
	let ns = txn.expect_ns_by_name(opt.ns()?).await?;
	let db = txn.expect_db_by_name(opt.ns()?, opt.db()?).await?;

	//
	for f in from.iter() {
		for t in to.iter() {
			let through = stk
				.run(|stk| crate::legacy::expr_compute(&this.through, stk, ctx, opt, doc))
				.await
				.catch_return()?;
			let through = RelateThrough::try_from(through)?;

			// Get the table name from the through part (where the relation record is stored)
			let through_table = match &through {
				RelateThrough::Table(tb) => tb,
				RelateThrough::RecordId(rid) => &rid.table,
			};

			// Auto-create the through table if it doesn't exist
			let tb =
				txn.get_or_add_tb(Some(ctx), opt.ns()?, opt.db()?, through_table, None).await?;
			let parent = NsDbCtx {
				ns: Arc::clone(&ns),
				db: Arc::clone(&db),
			};
			let doc_ctx =
				DocumentContext::initialise(ctx, &parent, tb, through_table, opt.version, true)
					.await?;

			iterator.ingest(Iterable::Relatable(doc_ctx, f.clone(), through, t.clone(), None));
		}
	}

	// Assign the statement
	let stm = Statement::from(this);

	CursorDoc::update_parent(ctx, doc, async |ctx| {
		// Process the statement
		let res =
			iterator.output(stk, ctx.as_ref(), opt, &stm, RecordStrategy::KeysAndValues).await?;
		// Catch statement timeout
		ctx.expect_not_timedout().await?;
		// Output the results
		match res {
			// This is a single record result
			Value::Array(mut a) if this.only => match a.len() {
				// There was exactly one result
				1 => Ok(a.0.pop().expect("array has exactly one element")),
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
