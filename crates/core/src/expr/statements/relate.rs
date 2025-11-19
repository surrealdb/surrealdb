use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;

use crate::ctx::{Context, MutableContext};
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Data, Expr, FlowResultExt as _, Output, Timeout, Value};
use crate::idx::planner::RecordStrategy;
use crate::val::{RecordId, RecordIdKey, Table};

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
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl RelateStatement {
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
		// Check if there is a timeout
		let ctx = match self.timeout.as_ref() {
			Some(timeout) => {
				let x = timeout.compute(stk, ctx, opt, doc).await?.0;
				let mut ctx = MutableContext::new(ctx);
				ctx.add_timeout(x)?;
				ctx.freeze()
			}
			None => ctx.clone(),
		};
		// Loop over the from targets
		let from = {
			let mut out = Vec::new();
			match stk.run(|stk| self.from.compute(stk, &ctx, opt, doc)).await.catch_return()? {
				Value::RecordId(v) => out.push(v),
				Value::Array(v) => {
					for v in v {
						match v {
							Value::RecordId(v) => out.push(v),
							Value::Object(v) => match v.rid() {
								Some(v) => out.push(v),
								_ => {
									bail!(Error::RelateStatementIn {
										value: v.to_string(),
									})
								}
							},
							v => {
								bail!(Error::RelateStatementIn {
									value: v.to_string(),
								})
							}
						}
					}
				}
				Value::Object(v) => match v.rid() {
					Some(v) => out.push(v),
					None => {
						bail!(Error::RelateStatementIn {
							value: v.to_string(),
						})
					}
				},
				v => {
					bail!(Error::RelateStatementIn {
						value: v.to_string(),
					})
				}
			};
			// }
			out
		};
		// Loop over the with targets
		let to = {
			let mut out = Vec::new();
			match stk.run(|stk| self.to.compute(stk, &ctx, opt, doc)).await.catch_return()? {
				Value::RecordId(v) => out.push(v),
				Value::Array(v) => {
					for v in v {
						match v {
							Value::RecordId(v) => out.push(v),
							Value::Object(v) => match v.rid() {
								Some(v) => out.push(v),
								None => {
									bail!(Error::RelateStatementId {
										value: v.to_string(),
									})
								}
							},
							v => {
								bail!(Error::RelateStatementId {
									value: v.to_string(),
								})
							}
						}
					}
				}
				Value::Object(v) => match v.rid() {
					Some(v) => out.push(v),
					None => {
						bail!(Error::RelateStatementId {
							value: v.to_string(),
						})
					}
				},
				v => {
					bail!(Error::RelateStatementId {
						value: v.to_string(),
					})
				}
			};
			out
		};
		//
		for f in from.iter() {
			for t in to.iter() {
				let through = stk
					.run(|stk| self.through.compute(stk, &ctx, opt, doc))
					.await
					.catch_return()?;

				let through = RelateThrough::try_from(through)?;
				i.ingest(Iterable::Relatable(f.clone(), through, t.clone(), None));
			}
		}

		// Assign the statement
		let stm = Statement::from(self);

		CursorDoc::update_parent(&ctx, doc, async |ctx| {
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
	Table(String),
}

impl From<(String, Option<RecordIdKey>)> for RelateThrough {
	fn from((table, id): (String, Option<RecordIdKey>)) -> Self {
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
			Value::Table(table) => Ok(RelateThrough::Table(table.into_string())),
			_ => bail!(Error::RelateStatementOut {
				value: value.to_string()
			}),
		}
	}
}

impl From<RelateThrough> for Value {
	fn from(v: RelateThrough) -> Self {
		match v {
			RelateThrough::RecordId(id) => Value::RecordId(id),
			RelateThrough::Table(table) => Value::Table(Table::new(table)),
		}
	}
}
