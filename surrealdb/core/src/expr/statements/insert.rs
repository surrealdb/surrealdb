use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Iterable, Iterator, Options, Statement};
use crate::doc::{CursorDoc, NsDbTbCtx};
use crate::err::Error;
use crate::expr::paths::{IN, OUT};
use crate::expr::statements::relate::RelateThrough;
use crate::expr::{Data, Expr, FlowResultExt as _, Output, Value};
use crate::idx::planner::RecordStrategy;
use crate::val::{Duration, RecordIdKey, TableName};

#[derive(Clone, Debug, Eq, PartialEq, Hash, priority_lfu::DeepSizeOf)]
pub(crate) struct InsertStatement {
	pub into: Option<Expr>,
	pub data: Data,
	/// Does the statement have the ignore clause.
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Expr,
	pub relation: bool,
}

impl InsertStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "InsertStatement::compute", skip_all)]
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
		let mut iterator = Iterator::new();
		// Check if there is a timeout
		let ctx_store;
		let ctx = match stk
			.run(|stk| self.timeout.compute(stk, ctx, opt, doc))
			.await
			.catch_return()?
			.cast_to::<Option<Duration>>()?
		{
			Some(timeout) => {
				let mut ctx = Context::new(ctx);
				ctx.add_timeout(timeout.0)?;
				ctx_store = ctx.freeze();
				&ctx_store
			}
			None => ctx,
		};
		// Parse the INTO expression
		let tb = match &self.into {
			Some(into) => {
				match stk.run(|stk| into.compute(stk, ctx, opt, doc)).await.catch_return()? {
					Value::Table(into) => Some(into),
					Value::String(into) => Some(TableName::new(into)),
					_ => {
						return Err(Error::InsertStatement {
							value: into.to_sql(),
						}
						.into());
					}
				}
			}
			None => None,
		};

		let txn = ctx.tx();
		// let ns = txn.expect_ns_by_name(opt.ns()?).await?;
		// let db = txn.expect_db_by_name(opt.ns()?, opt.db()?).await?;
		// let tb_def = txn.expect_tb_by_name(opt.ns()?, opt.db()?, &tb).await?;
		// let fields = txn.all_tb_fields(ns.namespace_id, db.database_id, &tb, opt.version).await?;

		// let doc_ctx = NsDbTbCtx {
		// 	ns,
		// 	db,
		// 	tb: tb_def,
		// 	fields,
		// };

		let ns = ctx.tx().expect_ns_by_name(opt.ns()?).await?;
		let db = ctx.tx().expect_db_by_name(opt.ns()?, opt.db()?).await?;

		let mut doc_ctx = None;
		if let Some(tb) = &tb {
			let tb_def = ctx.tx().get_or_add_tb(Some(ctx), &ns.name, &db.name, tb).await?;
			let fields =
				ctx.tx().all_tb_fields(ns.namespace_id, db.database_id, tb, opt.version).await?;
			doc_ctx = Some(NsDbTbCtx {
				ns: Arc::clone(&ns),
				db: Arc::clone(&db),
				tb: tb_def,
				fields,
			});
		}

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
							stk.run(|stk| v.compute(stk, ctx, opt, None)).await.catch_return()?;
						o.set(stk, ctx, opt, k, v).await?;
					}
					// Specify the new table record id
					let (tb, id) = extract_table_and_rid_key(&o, &tb)?;

					doc_ctx = match doc_ctx {
						Some(doc_ctx) if doc_ctx.tb.name == tb => Some(doc_ctx),
						Some(_) | None => {
							let tb_def =
								txn.get_or_add_tb(Some(ctx), &ns.name, &db.name, &tb).await?;
							let fields = txn
								.all_tb_fields(ns.namespace_id, db.database_id, &tb, opt.version)
								.await?;
							Some(NsDbTbCtx {
								ns: Arc::clone(&ns),
								db: Arc::clone(&db),
								tb: tb_def,
								fields,
							})
						}
					};

					// Pass the value to the iterator
					iterator.ingest(iterable(
						doc_ctx.clone().expect("doc_ctx must be set at this point"),
						tb.clone(),
						id,
						o,
						self.relation,
					)?)
				}
			}
			// Check if this is a modern statement
			Data::SingleExpression(v) => {
				let v = stk.run(|stk| v.compute(stk, ctx, opt, doc)).await.catch_return()?;
				match v {
					Value::Array(v) => {
						for v in v {
							// Specify the new table record id
							let (tb, id) = extract_table_and_rid_key(&v, &tb)?;

							doc_ctx = match doc_ctx {
								Some(doc_ctx) if doc_ctx.tb.name == tb => Some(doc_ctx),
								Some(_) | None => {
									let tb_def = txn
										.get_or_add_tb(Some(ctx), &ns.name, &db.name, &tb)
										.await?;
									let fields = txn
										.all_tb_fields(
											ns.namespace_id,
											db.database_id,
											&tb,
											opt.version,
										)
										.await?;
									Some(NsDbTbCtx {
										ns: Arc::clone(&ns),
										db: Arc::clone(&db),
										tb: tb_def,
										fields,
									})
								}
							};

							// Pass the value to the iterator
							iterator.ingest(iterable(
								doc_ctx.clone().expect("doc_ctx must be set at this point"),
								tb.clone(),
								id,
								v,
								self.relation,
							)?)
						}
					}
					Value::Object(_) => {
						// Specify the new table record id
						let (tb, id) = extract_table_and_rid_key(&v, &tb)?;

						doc_ctx = match doc_ctx {
							Some(doc_ctx) if doc_ctx.tb.name == tb => Some(doc_ctx),
							Some(_) | None => {
								let tb_def =
									txn.get_or_add_tb(Some(ctx), &ns.name, &db.name, &tb).await?;
								let fields = txn
									.all_tb_fields(
										ns.namespace_id,
										db.database_id,
										&tb,
										opt.version,
									)
									.await?;
								Some(NsDbTbCtx {
									ns: Arc::clone(&ns),
									db: Arc::clone(&db),
									tb: tb_def,
									fields,
								})
							}
						};

						// Pass the value to the iterator
						iterator.ingest(iterable(
							doc_ctx.clone().expect("doc_ctx must be set at this point"),
							tb.clone(),
							id,
							v,
							self.relation,
						)?)
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

		CursorDoc::update_parent(ctx, doc, async |ctx| {
			// Process the statement
			let res = iterator.output(stk, &ctx, opt, &stm, RecordStrategy::KeysAndValues).await?;
			// Catch statement timeout
			ctx.expect_not_timedout().await?;
			// Output the results
			Ok(res)
		})
		.await
	}
}

impl ToSql for InsertStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::insert::InsertStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}

fn iterable(
	doc_ctx: NsDbTbCtx,
	tb: TableName,
	id: Option<RecordIdKey>,
	v: Value,
	relation: bool,
) -> Result<Iterable> {
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
		// INSERT RELATION INTO likes (id, in, out, desc) VALUES (1, person:1, person:2, 'Somewhat
		// likes'), (2, person:2, person:3, 'Really likes') f: person:1
		// w: person:2
		// v: { desc: 'Somewhat likes' }
		Ok(Iterable::Relatable(doc_ctx, f, RelateThrough::from((tb, id)), w, Some(v)))
	} else {
		// INSERT INTO person (id, name) VALUES (1, 'John Doe')
		// tb: person
		// id: person:1
		// v: { name: 'John Doe' }
		Ok(Iterable::Mergeable(doc_ctx, tb, id, v))
	}
}

fn extract_table_and_rid_key(
	record: &Value,
	into: &Option<TableName>,
) -> Result<(TableName, Option<RecordIdKey>)> {
	let Some(tb) = into else {
		let record = record.rid();
		let Value::RecordId(rid) = record else {
			bail!(Error::InsertStatementId {
				value: record.to_sql(),
			});
		};
		return Ok((rid.table, Some(rid.key)));
	};

	let rid = match record.rid() {
		// There is a floating point number for the id field
		// TODO: Is this correct? Rounding to int seems like unexpected behavior.
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
		Value::RecordId(id) => {
			// TODO: Perhaps check if the table in the RID matches the table we're inserting into.
			Some(id.key)
		}
		// There is no record id field
		Value::None => None,
		// Any other value cannot be converted to a record id key
		v => {
			bail!(Error::InsertStatementId {
				value: v.to_sql(),
			});
		}
	};

	Ok((tb.clone(), rid))
}
