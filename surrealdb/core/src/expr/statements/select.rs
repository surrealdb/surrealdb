use std::sync::Arc;

use anyhow::{Result, ensure};
use reblessive::tree::Stk;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::FrozenContext;
use crate::dbs::{Iterator, Options, Statement};
use crate::doc::{CursorDoc, NsDbCtx};
use crate::err::Error;
use crate::expr::order::Ordering;
use crate::expr::{
	Cond, Explain, Expr, Fetchs, Fields, FlowResultExt as _, Groups, Limit, Splits, Start, With,
};
use crate::idx::planner::{QueryPlanner, RecordStrategy, StatementContext};
use crate::val::{Datetime, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SelectStatement {
	/// The fields to extract from the records.
	///
	/// The foo,bar part in `SELECT foo,bar FROM baz`.
	pub fields: Fields,
	/// The fields to omit from the records. This is used to exclude fields from wildcard
	/// selection.
	///
	/// The OMIT foo,bar part in `SELECT foo,bar OMIT baz,qux FROM baz`.
	pub omit: Vec<Expr>,
	/// Whether to only return a single record.
	///
	/// The ONLY part in `SELECT * FROM ONLY foo`.
	pub only: bool,
	/// The expressions (tables, record IDs, arrays, etc) to select from.
	///
	/// The baz part in SELECT foo,bar FROM baz.
	pub what: Vec<Expr>,
	pub with: Option<With>,
	/// The WHERE clause.
	pub cond: Option<Cond>,
	/// The SPLIT clause.
	///
	/// This is used to produce the cartesian product of the values in split fields.
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	pub order: Option<Ordering>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub fetch: Option<Fetchs>,
	pub version: Expr,
	pub timeout: Expr,
	pub explain: Option<Explain>,
	pub tempfiles: bool,
}

impl SelectStatement {
	/// Check if computing this type can be done on a read only transaction.
	pub(crate) fn read_only(&self) -> bool {
		self.fields.read_only()
			&& self.what.iter().all(|v| v.read_only())
			&& self.cond.as_ref().map(|x| x.0.read_only()).unwrap_or(true)
	}

	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "SelectStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		parent_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Valid options?
		opt.valid_for_db()?;
		// Assign the statement
		let stm = Statement::from_select(stk, ctx, opt, parent_doc, self).await?;
		// Create a new iterator
		let mut iterator = Iterator::new();
		// Ensure futures are stored and the version is set if specified

		let version = stk
			.run(|stk| self.version.compute(stk, ctx, opt, parent_doc))
			.await
			.catch_return()?
			.cast_to::<Option<Datetime>>()?
			.map(|x| x.to_version_stamp())
			.transpose()?;
		let opt = Arc::new(opt.clone().with_version(version));

		// Extract the limits
		iterator.setup_limit(stk, ctx, &opt, &stm).await?;
		// Fail for multiple targets without a limit
		ensure!(
			!self.only || iterator.is_limit_one_or_zero() || self.what.len() <= 1,
			Error::SingleOnlyOutput
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

		// Loop over the select targets
		for w in self.what.iter() {
			// The target is also calculated on the parent doc
			iterator
				.prepare(stk, &ctx, &opt, parent_doc, &mut planner, &stm_ctx, &doc_ctx, w)
				.await?;
		}

		CursorDoc::update_parent(&ctx, parent_doc, async |ctx| {
			// Attach the query planner to the context
			let ctx = stm.setup_query_planner(planner, ctx);
			// Process the statement
			let res = iterator
				.output(stk, ctx.as_ref(), &opt, &stm, RecordStrategy::KeysAndValues)
				.await?;
			// Catch statement timeout
			ctx.expect_not_timedout().await?;

			if self.only {
				match res {
					Value::Array(mut array) => {
						if array.is_empty() {
							Ok(Value::None)
						} else {
							ensure!(array.len() == 1, Error::SingleOnlyOutput);
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
}
