use std::fmt;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;

use crate::ctx::{Context, MutableContext};
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Data, Expr, FlowResultExt as _, Output, Timeout, Value};
use crate::idx::planner::RecordStrategy;
use crate::val::RecordId;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RelateStatement {
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
				let mut ctx = MutableContext::new(ctx);
				ctx.add_timeout(*timeout.0)?;
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
				match stk
					.run(|stk| self.through.compute(stk, &ctx, opt, doc))
					.await
					.catch_return()?
				{
					// The relation has a specific record id
					Value::RecordId(id) => {
						i.ingest(Iterable::Relatable(f.clone(), id.clone(), t.clone(), None))
					}
					// The relation does not have a specific record id
					Value::Table(tb) => match self.data {
						// There is a data clause so check for a record id
						Some(ref data) => {
							let id = match data.rid(stk, &ctx, opt).await? {
								Value::None => RecordId::random_for_table(tb.into_string()),
								id => id.generate(tb.into_strand(), false)?,
							};
							i.ingest(Iterable::Relatable(f.clone(), id, t.clone(), None))
						}
						// There is no data clause so create a record id
						None => i.ingest(Iterable::Relatable(
							f.clone(),
							RecordId::random_for_table(tb.into_string()),
							t.clone(),
							None,
						)),
					},
					// The relation can not be any other type
					v => {
						bail!(Error::RelateStatementOut {
							value: v.to_string(),
						})
					}
				};
			}
		}

		// Assign the statement
		let stm = Statement::from(self);

		// Process the statement
		let res = i.output(stk, &ctx, opt, &stm, RecordStrategy::KeysAndValues).await?;
		// Catch statement timeout
		ensure!(!ctx.is_timedout().await?, Error::QueryTimedout);
		// Output the results
		match res {
			// This is a single record result
			Value::Array(mut a) if self.only => match a.len() {
				// There was exactly one result
				1 => Ok(a.0.pop().unwrap()),
				// There were no results
				_ => Err(anyhow::Error::new(Error::SingleOnlyOutput)),
			},
			// This is standard query result
			v => Ok(v),
		}
	}
}

impl fmt::Display for RelateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "RELATE")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {} -> {} -> {}", self.from, self.through, self.to)?;
		if self.uniq {
			f.write_str(" UNIQUE")?
		}
		if let Some(ref v) = self.data {
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
		Ok(())
	}
}
