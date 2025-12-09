use anyhow::{Result, ensure};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::dbs::{Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Data, Expr, FlowResultExt as _, Literal, Output};
use crate::idx::planner::{QueryPlanner, RecordStrategy, StatementContext};
use crate::val::{Datetime, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
	// If the statement should be run in parallel
	pub parallel: bool,
	// Version as nanosecond timestamp passed down to Datastore
	pub(crate) version: Expr,
}

impl Default for CreateStatement {
	fn default() -> Self {
		Self {
			only: Default::default(),
			what: Default::default(),
			data: Default::default(),
			output: Default::default(),
			timeout: Expr::Literal(Literal::None),
			parallel: Default::default(),
			version: Expr::Literal(Literal::None),
		}
	}
}

impl CreateStatement {
	/// Process this type returning a computed simple Value
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
		// Propagate the version to the underlying datastore
		let version = stk
			.run(|stk| self.version.compute(stk, ctx, opt, doc))
			.await
			.catch_return()?
			.cast_to::<Option<Datetime>>()?
			.map(|x| x.to_version_stamp())
			.transpose()?;
		let opt = &opt.clone().with_version(version);
		// Check if there is a timeout
		let ctx = stm.setup_timeout(stk, ctx, opt, doc).await?;

		// Get a query planner
		let mut planner = QueryPlanner::new();

		let stm_ctx = StatementContext::new(&ctx, opt, &stm)?;
		// Loop over the create targets
		for w in self.what.iter() {
			i.prepare(stk, &ctx, opt, doc, &mut planner, &stm_ctx, w).await.map_err(|e| {
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
		ctx.get_db(opt).await?;

		CursorDoc::update_parent(&ctx, doc, async |ctx| {
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

impl ToSql for CreateStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::create::CreateStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
