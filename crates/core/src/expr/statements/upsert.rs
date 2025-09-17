use std::fmt;

use anyhow::{Result, ensure};
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::{Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::fmt::Fmt;
use crate::expr::{Cond, Data, Explain, Expr, Output, Timeout, With};
use crate::idx::planner::{QueryPlanner, RecordStrategy, StatementContext};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Default, Hash)]
pub struct UpsertStatement {
	pub only: bool,
	pub what: Vec<Expr>,
	pub with: Option<With>,
	pub data: Option<Data>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	pub explain: Option<Explain>,
}

impl UpsertStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
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
		let ctx = stm.setup_timeout(ctx)?;

		// Get a query planner
		let mut planner = QueryPlanner::new();

		let stm_ctx = StatementContext::new(&ctx, opt, &stm)?;
		// Loop over the upsert targets
		for w in self.what.iter() {
			i.prepare(stk, &ctx, opt, doc, &mut planner, &stm_ctx, w).await.map_err(|e| {
				if matches!(e.downcast_ref(), Some(Error::InvalidStatementTarget { .. })) {
					let Ok(Error::InvalidStatementTarget {
						value,
					}) = e.downcast()
					else {
						unreachable!()
					};
					anyhow::Error::new(Error::UpsertStatement {
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
		ctx.get_db(opt).await?;

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
	}
}

impl fmt::Display for UpsertStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "UPSERT")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {}", Fmt::comma_separated(self.what.iter()))?;
		if let Some(ref v) = self.with {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.data {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.output {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {v}")?
		}
		if self.parallel {
			f.write_str(" PARALLEL")?
		}
		if let Some(ref v) = self.explain {
			write!(f, " {v}")?
		}
		Ok(())
	}
}
