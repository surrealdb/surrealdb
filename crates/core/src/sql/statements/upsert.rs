use crate::ctx::Context;
use crate::dbs::{Iterable, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::planner::{QueryPlanner, RecordStrategy, StatementContext};
use crate::sql::{Cond, Data, Explain, Id, Output, Table, Thing, Timeout, Value, Values, With};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::Iterator;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct UpsertStatement {
	pub only: bool,
	pub what: Values,
	#[revision(start = 2)]
	pub with: Option<With>,
	pub data: Option<Data>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	#[revision(start = 2)]
	pub explain: Option<Explain>,
}

impl UpsertStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Valid options?
		opt.valid_for_db()?;
		// Create a new iterator
		let mut i = crate::dbs::Iterator::new();
		// Ensure futures are stored
		let opt = &opt.new_with_futures(false);
		// Assign the statement
		let stm = Statement::from(self);
		// Check if there is a timeout
		let ctx = stm.setup_timeout(ctx)?;

		let mut is_bulk = false;
		if let Some(Data::ContentExpression(v)) | Some(Data::MergeExpression(v)) = &self.data {
			if let Ok(tables) = self.get_tables(stk, &ctx, opt, doc).await {
				match v.compute(stk, &ctx, opt, doc).await? {
					Value::Array(v) => {
						for v in v {
							iterable(&mut i, &tables, &v)?;
						}
						is_bulk = true;
					}
					Value::Object(_) if !matches!(v.rid(), Value::None) => {
						iterable(&mut i, &tables, v)?;
						is_bulk = true;
					}
					_ => {}
				}
			}
		}

		// Get a query planner
		let mut planner = QueryPlanner::new();
		let stm_ctx = StatementContext::new(&ctx, opt, &stm)?;

		if !is_bulk {
			// Loop over the upsert targets
			for w in self.what.0.iter() {
				let v = w.compute(stk, &ctx, opt, doc).await?;
				i.prepare(stk, &mut planner, &stm_ctx, v).await.map_err(|e| match e {
					Error::InvalidStatementTarget {
						value: v,
					} => Error::UpsertStatement {
						value: v,
					},
					e => e,
				})?;
			}
			//Check for update data
			if let Some(v) = &self.data {
				match v {
					Data::ContentExpression(v) => {
						let v = v.compute(stk, &ctx, opt, doc).await?;
						if !matches!(v, Value::Object(_)) {
							return Err(Error::InvalidContent {
								value: v,
							});
						}
					}
					Data::MergeExpression(v) => {
						let v = v.compute(stk, &ctx, opt, doc).await?;
						if !matches!(v, Value::Object(_)) {
							return Err(Error::InvalidMerge {
								value: v,
							});
						}
					}
					_ => {}
				};
			};
		}
		// Attach the query planner to the context
		let ctx = stm.setup_query_planner(planner, ctx);
		// Process the statement
		let res = i.output(stk, &ctx, opt, &stm, RecordStrategy::KeysAndValues).await?;
		// Catch statement timeout
		if ctx.is_timedout() {
			return Err(Error::QueryTimedout);
		}
		// Output the results
		match res {
			// This is a single record result
			Value::Array(mut a) if self.only => match a.len() {
				// There was exactly one result
				1 => Ok(a.remove(0)),
				// There were no results
				_ => Err(Error::SingleOnlyOutput),
			},
			// This is standard query result
			v => Ok(v),
		}
	}

	async fn get_tables(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Vec<Table>, Error> {
		let mut accum = vec![];
		for w in self.what.0.iter() {
			if let Value::Table(t) = w.compute(stk, ctx, opt, doc).await? {
				accum.push(t);
			} else {
				return Err(Error::UpsertStatement {
					value: "Targets contains Thing".to_string(),
				});
			}
		}
		Ok(accum)
	}
}

impl fmt::Display for UpsertStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "UPSERT")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {}", self.what)?;
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

fn gen_id(id: Value, tables: &[Table], selected: &Table) -> Result<Option<Thing>, Error> {
	match id {
		Value::Thing(v) => match v {
			Thing {
				id: Id::Generate(_),
				..
			} => Err(Error::UpsertStatementId {
				value: v.to_string(),
			}),
			Thing {
				tb,
				..
			} if tb != selected.0 => {
				if tables.iter().any(|x| x.0 == tb) {
					Ok(None)
				} else {
					Err(Error::UpsertStatementId {
						value: tb.to_string(),
					})
				}
			}
			v => Ok(Some(v)),
		},
		Value::None => Err(Error::UpsertStatementId {
			value: "not specified".to_string(),
		}),
		v => v.generate(selected, false).map(Into::into),
	}
}

fn iterable(i: &mut crate::dbs::Iterator, tables: &[Table], v: &Value) -> Result<(), Error> {
	let id = v.rid();
	for table in tables.iter() {
		let Some(id) = gen_id(id.clone(), tables, table)? else {
			continue;
		};
		i.ingest(Iterable::Mergeable(id, v.clone()));
	}
	Ok(())
}
