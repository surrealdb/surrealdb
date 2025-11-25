use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::ctx::{Context, MutableContext};
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::paths::{IN, OUT};
use crate::expr::statements::relate::RelateThrough;
use crate::expr::{Data, Expr, FlowResultExt as _, Output, Timeout, Value};
use crate::idx::planner::RecordStrategy;
use crate::val::{Datetime, RecordIdKey, Table};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct InsertStatement {
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
				let x = timeout.compute(stk, ctx, opt, doc).await?.0;
				let mut ctx = MutableContext::new(ctx);
				ctx.add_timeout(x)?;
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
							value: v.to_sql(),
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
					let (tb, id) = extract_tb_id(&o, &into)?;
					// Pass the value to the iterator
					i.ingest(iterable(tb, id, o, self.relation)?)
				}
			}
			// Check if this is a modern statement
			Data::SingleExpression(v) => {
				let v = stk.run(|stk| v.compute(stk, &ctx, opt, doc)).await.catch_return()?;
				match v {
					Value::Array(v) => {
						for v in v {
							// Specify the new table record id
							let (tb, id) = extract_tb_id(&v, &into)?;
							// Pass the value to the iterator
							i.ingest(iterable(tb, id, v, self.relation)?)
						}
					}
					Value::Object(_) => {
						// Specify the new table record id
						let (tb, id) = extract_tb_id(&v, &into)?;
						// Pass the value to the iterator
						i.ingest(iterable(tb, id, v, self.relation)?)
					}
					v => {
						bail!(Error::InsertStatement {
							value: v.to_sql(),
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

		CursorDoc::update_parent(&ctx, doc, async |ctx| {
			// Process the statement
			let res = i.output(stk, &ctx, opt, &stm, RecordStrategy::KeysAndValues).await?;
			// Catch statement timeout
			ensure!(!ctx.is_timedout().await?, Error::QueryTimedout);
			// Output the results
			Ok(res)
		})
		.await
	}
}

fn iterable(tb: String, id: Option<RecordIdKey>, v: Value, relation: bool) -> Result<Iterable> {
	if relation {
		let f = match v.pick(&*IN) {
			Value::RecordId(v) => v,
			v => {
				bail!(Error::InsertStatementIn {
					value: v.to_sql(),
				})
			}
		};
		let w = match v.pick(&*OUT) {
			Value::RecordId(v) => v,
			v => {
				bail!(Error::InsertStatementOut {
					value: v.to_sql(),
				})
			}
		};
		// TODO(micha): Support table relations too?
		Ok(Iterable::Relatable(f, RelateThrough::from((tb, id)), w, Some(v)))
	} else {
		Ok(Iterable::Mergeable(tb, id, v))
	}
}

fn extract_tb_id(v: &Value, into: &Option<Table>) -> Result<(String, Option<RecordIdKey>)> {
	if let Some(tb) = into {
		let id = match v.rid() {
			// There is a floating point number for the id field
			Value::Number(id) if id.is_float() => Some(RecordIdKey::Number(id.as_int())),
			// There is an integer number for the id field
			Value::Number(id) if id.is_int() => Some(RecordIdKey::Number(id.as_int())),
			// There is a string for the id field
			Value::String(id) if !id.is_empty() => Some(id.into()),
			// There is an object for the id field
			Value::Object(id) => Some(id.into()),
			// There is an array for the id field
			Value::Array(id) => Some(id.into()),
			// There is a UUID for the id field
			Value::Uuid(id) => Some(id.into()),
			// There is a record id defined
			Value::RecordId(id) => Some(id.key),
			// There is no record id field
			Value::None => None,
			// Any other value cannot be converted to a record id key
			v => {
				bail!(Error::InsertStatementId {
					value: v.to_sql(),
				});
			}
		};
		Ok((tb.clone().into_string(), id))
	} else {
		let v = v.rid();
		if let Value::RecordId(rid) = v {
			Ok((rid.table, Some(rid.key)))
		} else {
			bail!(Error::InsertStatementId {
				value: v.to_sql(),
			});
		}
	}
}
