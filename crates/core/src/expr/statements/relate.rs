use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::{CursorDoc, NsDbTbCtx};
use crate::err::Error;
use crate::expr::{Data, Expr, FlowResultExt as _, Output, Value};
use crate::idx::planner::RecordStrategy;
use crate::val::{Duration, RecordId, RecordIdKey, TableName};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RelateStatement {
	pub only: bool,
	/// The expression resulting in the table through which we create a relation
	pub through: Expr,
	/// The expression the relation is from
	pub from: Expr,
	/// The expression the relation targets.
	pub to: Expr,
	pub uniq: bool,
	pub data: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Expr,
	pub parallel: bool,
}

impl RelateStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "RelateStatement::compute", skip_all)]
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
		// Check if there is a timeout
		let ctx_store: FrozenContext;
		let ctx = match stk
			.run(|stk| self.timeout.compute(stk, ctx, opt, doc))
			.await
			.catch_return()?
			.cast_to::<Option<Duration>>()?
		{
			Some(timeout) => {
				let mut new_ctx = Context::new(ctx);
				new_ctx.add_timeout(timeout.0)?;
				ctx_store = new_ctx.freeze();
				&ctx_store
			}
			None => ctx,
		};
		// Loop over the from targets
		let from = {
			let mut out = Vec::new();
			match stk.run(|stk| self.from.compute(stk, ctx, opt, doc)).await.catch_return()? {
				Value::RecordId(v) => out.push(v),
				Value::Array(v) => {
					for v in v {
						match v {
							Value::RecordId(v) => out.push(v),
							Value::Object(v) => match v.rid() {
								Some(v) => out.push(v),
								_ => {
									bail!(Error::RelateStatementIn {
										value: v.to_sql(),
									})
								}
							},
							v => {
								bail!(Error::RelateStatementIn {
									value: v.to_sql(),
								})
							}
						}
					}
				}
				Value::Object(v) => match v.rid() {
					Some(v) => out.push(v),
					None => {
						bail!(Error::RelateStatementIn {
							value: v.to_sql(),
						})
					}
				},
				v => {
					bail!(Error::RelateStatementIn {
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
			match stk.run(|stk| self.to.compute(stk, ctx, opt, doc)).await.catch_return()? {
				Value::RecordId(v) => out.push(v),
				Value::Array(v) => {
					for v in v {
						match v {
							Value::RecordId(v) => out.push(v),
							Value::Object(v) => match v.rid() {
								Some(v) => out.push(v),
								None => {
									bail!(Error::RelateStatementId {
										value: v.to_sql(),
									})
								}
							},
							v => {
								bail!(Error::RelateStatementId {
									value: v.to_sql(),
								})
							}
						}
					}
				}
				Value::Object(v) => match v.rid() {
					Some(v) => out.push(v),
					None => {
						bail!(Error::RelateStatementId {
							value: v.to_sql(),
						})
					}
				},
				v => {
					bail!(Error::RelateStatementId {
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
				let through =
					stk.run(|stk| self.through.compute(stk, ctx, opt, doc)).await.catch_return()?;
				let through = RelateThrough::try_from(through)?;

				// Get the table name from the through part (where the relation record is stored)
				let through_table = match &through {
					RelateThrough::Table(tb) => tb,
					RelateThrough::RecordId(rid) => &rid.table,
				};

				// Auto-create the through table if it doesn't exist
				let tb = txn.get_or_add_tb(Some(ctx), opt.ns()?, opt.db()?, through_table).await?;
				let fields = txn
					.all_tb_fields(ns.namespace_id, db.database_id, through_table, opt.version)
					.await?;
				let doc_ctx = NsDbTbCtx {
					ns: Arc::clone(&ns),
					db: Arc::clone(&db),
					tb,
					fields,
				};

				i.ingest(Iterable::Relatable(doc_ctx, f.clone(), through, t.clone(), None));
			}
		}

		// Assign the statement
		let stm = Statement::from(self);

		CursorDoc::update_parent(ctx, doc, async |ctx| {
			// Process the statement
			let res = i.output(stk, ctx.as_ref(), opt, &stm, RecordStrategy::KeysAndValues).await?;
			// Catch statement timeout
			ensure!(!ctx.is_timedout().await?, Error::QueryTimedout);
			// Output the results
			match res {
				// This is a single record result
				Value::Array(mut a) if self.only => match a.len() {
					// There was exactly one result
					1 => Ok(a.0.pop().expect("array has exactly one element")),
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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum RelateThrough {
	RecordId(RecordId),
	Table(TableName),
}

impl From<(TableName, Option<RecordIdKey>)> for RelateThrough {
	fn from((table, id): (TableName, Option<RecordIdKey>)) -> Self {
		if let Some(id) = id {
			RelateThrough::RecordId(RecordId::new(table, id))
		} else {
			RelateThrough::Table(table)
		}
	}
}

impl TryFrom<Value> for RelateThrough {
	type Error = anyhow::Error;
	fn try_from(value: Value) -> Result<Self> {
		match value {
			Value::RecordId(id) => Ok(RelateThrough::RecordId(id)),
			Value::Table(table) => Ok(RelateThrough::Table(table)),
			_ => bail!(Error::RelateStatementOut {
				value: value.to_sql()
			}),
		}
	}
}

impl From<RelateThrough> for Value {
	fn from(v: RelateThrough) -> Self {
		match v {
			RelateThrough::RecordId(id) => Value::RecordId(id),
			RelateThrough::Table(table) => Value::Table(table),
		}
	}
}
