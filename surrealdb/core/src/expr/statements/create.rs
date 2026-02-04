use std::sync::Arc;

use anyhow::Result;
use priority_lfu::DeepSizeOf;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::FrozenContext;
use crate::dbs::{Iterator, Options, Statement};
use crate::doc::{CursorDoc, NsDbCtx};
use crate::err::Error;
use crate::expr::{Data, Expr, Literal, Output};
use crate::idx::planner::{QueryPlanner, RecordStrategy, StatementContext};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
pub(crate) struct CreateStatement {
	// A keyword modifier indicating if we are expecting a single result or several
	pub only: bool,
	// Where we are creating (i.e. table, or record ID)
	pub(crate) what: Vec<Expr>,
	// The data associated with the record being created
	pub(crate) data: Option<Data>,
	//  What the result of the statement should resemble (i.e. Diff or no result etc).
	pub(crate) output: Option<Output>,
	// The timeout for the statement
	pub timeout: Expr,
}

impl Default for CreateStatement {
	fn default() -> Self {
		Self {
			only: Default::default(),
			what: Default::default(),
			data: Default::default(),
			output: Default::default(),
			timeout: Expr::Literal(Literal::None),
		}
	}
}

impl CreateStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "CreateStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
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
		let stm = Statement::from(self);
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

		// Loop over the create targets
		for w in self.what.iter() {
			iterator
				.prepare(stk, &ctx, opt, doc, &mut planner, &stm_ctx, &doc_ctx, w)
				.await
				.map_err(|e| {
					// double match to avoid allocation
					if matches!(e.downcast_ref(), Some(Error::InvalidStatementTarget { .. })) {
						let Ok(Error::InvalidStatementTarget {
							value,
						}) = e.downcast()
						else {
							unreachable!()
						};
						anyhow::Error::new(Error::CreateStatement {
							value,
						})
					} else {
						e
					}
				})?;
		}
		// Attach the query planner to the context
		let ctx = stm.setup_query_planner(planner, ctx);

		// Ensure the database exists.
		// ctx.get_db(opt).await?;

		CursorDoc::update_parent(&ctx, doc, async |ctx| {
			// Process the statement
			let res = iterator.output(stk, &ctx, opt, &stm, RecordStrategy::KeysAndValues).await?;
			// Catch statement timeout
			ctx.expect_not_timedout().await?;
			// Output the results
			match res {
				// This is a single record result
				Value::Array(mut a) if self.only => match a.len() {
					// There was exactly one result
					1 => Ok(a.remove(0)),
					// There were no results
					_ => Err(anyhow::Error::new(Error::SingleOnlyOutput)),
				},
				// This is standard query result
				v => Ok(v),
			}
		})
		.await
	}
}

impl ToSql for CreateStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::create::CreateStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
