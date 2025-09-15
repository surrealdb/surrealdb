use std::fmt;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;

use crate::ctx::{Context, MutableContext};
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::paths::{IN, OUT};
use crate::expr::{Data, Expr, FlowResultExt as _, Output, Timeout, Value};
use crate::idx::planner::RecordStrategy;
use crate::val::{Datetime, RecordId, Table};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct InsertStatement {
	pub into: Option<Expr>,
	pub data: Data,
	/// Does the statement have the ignore clause.
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	pub relation: bool,
	pub version: Option<Expr>,
}

impl InsertStatement {
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
		// Propagate the version to the underlying datastore
		let version = match &self.version {
			Some(v) => Some(
				stk.run(|stk| v.compute(stk, ctx, opt, doc))
					.await
					.catch_return()?
					.cast_to::<Datetime>()?
					.to_version_stamp()?,
			),
			_ => None,
		};
		let opt = &opt.clone().with_version(version);
		// Check if there is a timeout
		let ctx = match self.timeout.as_ref() {
			Some(timeout) => {
				let mut ctx = MutableContext::new(ctx);
				ctx.add_timeout(*timeout.0)?;
				ctx.freeze()
			}
			None => ctx.clone(),
		};
		// Parse the INTO expression
		let into = match &self.into {
			None => None,
			Some(into) => {
				match stk.run(|stk| into.compute(stk, &ctx, opt, doc)).await.catch_return()? {
					Value::Table(into) => Some(into),
					v => {
						bail!(Error::InsertStatement {
							value: v.to_string(),
						})
					}
				}
			}
		};

		// Parse the data expression
		match &self.data {
			// Check if this is a traditional statement
			Data::ValuesExpression(v) => {
				for v in v {
					// Create a new empty base object
					let mut o = Value::empty_object();
					// Set each field from the expression
					for (k, v) in v.iter() {
						let v =
							stk.run(|stk| v.compute(stk, &ctx, opt, None)).await.catch_return()?;
						o.set(stk, &ctx, opt, k, v).await?;
					}
					// Specify the new table record id
					let id = gen_id(&o, &into)?;
					// Pass the value to the iterator
					i.ingest(iterable(id, o, self.relation)?)
				}
			}
			// Check if this is a modern statement
			Data::SingleExpression(v) => {
				let v = stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await.catch_return()?;
				match v {
					Value::Array(v) => {
						for v in v {
							// Specify the new table record id
							let id = gen_id(&v, &into)?;
							// Pass the value to the iterator
							i.ingest(iterable(id, v, self.relation)?)
						}
					}
					Value::Object(_) => {
						// Specify the new table record id
						let id = gen_id(&v, &into)?;
						// Pass the value to the iterator
						i.ingest(iterable(id, v, self.relation)?)
					}
					v => {
						bail!(Error::InsertStatement {
							value: v.to_string(),
						})
					}
				}
			}
			v => fail!("Unknown data clause type in INSERT statement: {v:?}"),
		}
		// Assign the statement
		let stm = Statement::from(self);

		// Ensure the database exists.
		ctx.get_db(opt).await?;

		// Process the statement
		let res = i.output(stk, &ctx, opt, &stm, RecordStrategy::KeysAndValues).await?;
		// Catch statement timeout
		ensure!(!ctx.is_timedout().await?, Error::QueryTimedout);
		// Output the results
		Ok(res)
	}
}

impl fmt::Display for InsertStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("INSERT")?;
		if self.relation {
			f.write_str(" RELATION")?
		}
		if self.ignore {
			f.write_str(" IGNORE")?
		}
		if let Some(into) = &self.into {
			write!(f, " INTO {}", into)?;
		}
		write!(f, " {}", self.data)?;
		if let Some(ref v) = self.update {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.output {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.version {
			write!(f, "VERSION {v}")?
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

fn iterable(id: RecordId, v: Value, relation: bool) -> Result<Iterable> {
	if relation {
		let f = match v.pick(&*IN) {
			Value::RecordId(v) => v,
			v => {
				bail!(Error::InsertStatementIn {
					value: v.to_string(),
				})
			}
		};
		let w = match v.pick(&*OUT) {
			Value::RecordId(v) => v,
			v => {
				bail!(Error::InsertStatementOut {
					value: v.to_string(),
				})
			}
		};
		Ok(Iterable::Relatable(f, id, w, Some(v)))
	} else {
		Ok(Iterable::Mergeable(id, v))
	}
}

fn gen_id(v: &Value, into: &Option<Table>) -> Result<RecordId> {
	match into {
		Some(into) => v.rid().generate(into.clone().into_strand(), true),
		None => match v.rid() {
			Value::RecordId(v) => Ok(v),
			v => Err(anyhow::Error::new(Error::InsertStatementId {
				value: v.to_string(),
			})),
		},
	}
}
