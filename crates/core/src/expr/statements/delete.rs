use anyhow::{Result, ensure};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::dbs::{Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Cond, Explain, Expr, Literal, Output, With};
use crate::idx::planner::{QueryPlanner, RecordStrategy, StatementContext};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DeleteStatement {
	pub only: bool,
	pub what: Vec<Expr>,
	pub with: Option<With>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Expr,
	pub parallel: bool,
	pub explain: Option<Explain>,
}

impl Default for DeleteStatement {
	fn default() -> Self {
		Self {
			only: Default::default(),
			what: Default::default(),
			with: Default::default(),
			cond: Default::default(),
			output: Default::default(),
			timeout: Expr::Literal(Literal::None),
			parallel: Default::default(),
			explain: Default::default(),
		}
	}
}

impl DeleteStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "DeleteStatement::compute", skip_all)]
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
		let mut i = Iterator::new();
		// Assign the statement
		let stm = Statement::from(self);
		// Check if there is a timeout
		let ctx = stm.setup_timeout(stk, ctx, opt, doc).await?;

		// Get a query planner
		let mut planner = QueryPlanner::new();

		let stm_ctx = StatementContext::new(&ctx, opt, &stm)?;
		// Loop over the delete targets
		for w in self.what.iter() {
			i.prepare(stk, &ctx, opt, doc, &mut planner, &stm_ctx, w).await.map_err(|e| {
				if matches!(e.downcast_ref(), Some(Error::InvalidStatementTarget { .. })) {
					let Ok(Error::InvalidStatementTarget {
						value,
					}) = e.downcast()
					else {
						unreachable!()
					};
					anyhow::Error::new(Error::DeleteStatement {
						value,
					})
				} else {
					e
				}
			})?;
		}
		CursorDoc::update_parent(&ctx, doc, async |ctx| {
			// Attach the query planner to the context
			let ctx = stm.setup_query_planner(planner, ctx);

			// Process the statement
			let res = i.output(stk, &ctx, opt, &stm, RecordStrategy::KeysAndValues).await?;
			// Catch statement timeout
			ensure!(!ctx.is_timedout().await?, Error::QueryTimedout);
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

impl ToSql for DeleteStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::delete::DeleteStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
