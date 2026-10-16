//! The host's side of the instructions that fill a view from the tables it
//! reads.
//!
//! A view's backfill is a program: a select over the view's sources computes
//! the rows (or the groups) the view holds, and these instructions turn each
//! into the view table's record. A view's rows are metadata no statement can
//! express — an aggregated row's stats — so they are written to storage
//! directly, and then run through the view table's own order (its indexes, its
//! second-order views, events, live queries and change feed), as the
//! incremental path runs the order for a created row. A materialized row whose
//! key the backfill already wrote, because two source tables hold a record
//! with the same id, replaces that row as the incremental path does, and runs
//! the order for an updated row. The statement that defines the view changes
//! the catalog the order is read from, so the order is the one of the view
//! table's catalog version when the backfill meets the table, served from the
//! compile cache ([`super::fault::view_rows`]).

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::anyhow;
use surrealdb_kvs::key::KVKey as _;

use super::host::run_value_row;
use super::interp::State;
use super::program::{Backfill, Program, StoreInstr};
use super::scalar::Scalar;
use super::store::{read, table_name_of, write};
use super::write::WriteState;
use crate::catalog::aggregation::{self, backfill_stats};
use crate::catalog::providers::TableProvider as _;
use crate::catalog::{Metadata, Record, RecordType, encode_whole_record_with};
use crate::doc::Action;
use crate::exec::operators::write::ctx::{RecordFormatSlot, ResolvedFormat, TableExistence};
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::scope::{WritePurpose, WriteScope};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::key::schema::RecordKey;
use crate::val::{Array, RecordId, RecordIdKey, TableName, Value};

fn internal(why: impl Into<String>) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(why.into())))
}

/// The format the rows of view table `table` are written in, with the layouts a packed format
/// reads its descriptor from.
///
/// The backfill holds a slot for the table it fills
/// ([`RecordFormatSlot::resolve`](crate::exec::operators::write::ctx::RecordFormatSlot::resolve)),
/// and starts a fresh one when it moves to another table.
async fn row_format<'s>(
	state: &'s mut State,
	txn: &crate::kvs::Transaction,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	table: &TableName,
) -> FlowResult<ResolvedFormat<'s>> {
	if state.backfill_format.as_ref().is_none_or(|(held, _)| held != table) {
		state.backfill_format = None;
	}
	let (_, slot) =
		state.backfill_format.get_or_insert_with(|| (table.clone(), RecordFormatSlot::empty()));
	slot.resolve(txn, ns, db, table).await.map_err(ControlFlow::Err)
}

/// Take backfill instruction `instr`, answering the instruction the body
/// jumps to, or `None` to continue with the next.
pub(super) async fn step(
	program: &Program,
	instr: &StoreInstr,
	regs: &mut [Scalar<'_>],
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Option<usize>> {
	match instr {
		StoreInstr::EvaluateOver {
			pipeline,
			row,
			dst,
		} => {
			let image = read(regs, *row);
			let value =
				super::write::run_image_in_frame(program, *pipeline, image, state, ctx).await?;
			write(regs, *dst, value);
		}
		StoreInstr::BackfillRow {
			table,
			row,
		} => {
			let table = table_name_of(&read(regs, *table))?;
			let Value::Object(mut object) = read(regs, *row) else {
				return Err(internal("initial select for view did not return an array of objects"));
			};
			let Some(Value::RecordId(id)) = object.remove("id") else {
				return Err(internal("select results did not contain a record id"));
			};
			let (ns, db) = ids(ctx).await?;
			let record = Record::new(Value::Object(object));
			let key = RecordKey {
				ns,
				db,
				tb: Cow::Borrowed(&table),
				id: Cow::Borrowed(&id.key),
			};
			let txn = ctx.txn();
			// A row is stored under its source record's key, so source records of different tables
			// that share an id make one row. The view table was emptied before the backfill, so a
			// row already under the key is one this backfill wrote, and it is replaced as
			// maintaining the view replaces it: the source read last holds the row.
			let before = match txn.get_key_raw(&key, None).await.map_err(ControlFlow::Err)? {
				Some(bytes) => Some(
					txn.decode_record_as_written(
						ns,
						db,
						&table,
						&bytes,
						RecordId {
							table: table.clone(),
							key: id.key.clone(),
						},
						None,
					)
					.await
					.map_err(ControlFlow::Err)?,
				),
				None => None,
			};
			// The row names no key, so it is never packed and its layouts are not checked against
			// its table; a view's layout is the document layout.
			let bytes = {
				let resolved = row_format(state, &txn, ns, db, &table).await?;
				encode_whole_record_with(resolved.format(), resolved.layouts(), &record, None)
					.map_err(ControlFlow::Err)?
			};
			let key = key.encode_key().map_err(ControlFlow::Err)?;
			txn.set(key, bytes).await.map_err(ControlFlow::Err)?;
			txn.forget_record(ns, db, &table, &id.key);
			maintain(state, ctx, &table, id.key, before.as_ref(), &record).await?;
		}
		StoreInstr::BackfillGroup {
			backfill,
			row,
			dst,
			empty,
		} => {
			let (group, computed) = split(backfill, read(regs, *row))?;
			let Some(stats) = stats(backfill, &computed)? else {
				return Ok(Some(*empty as usize));
			};
			let document = aggregation::create_field_document(&group, &stats);
			write(regs, *dst, Value::Object(document));
		}
		StoreInstr::BackfillGroupRow {
			backfill,
			table,
			row,
			fields,
		} => {
			let table = table_name_of(&read(regs, *table))?;
			let (group, computed) = split(backfill, read(regs, *row))?;
			let Some(stats) = stats(backfill, &computed)? else {
				return Ok(None);
			};
			let mut data = Value::empty_object();
			for (path, value) in fields {
				data.put(path.as_ref(), read(regs, *value));
			}
			let record = Arc::new(Record {
				metadata: Some(Metadata {
					record_type: RecordType::Table,
					aggregation_stats: stats,
				}),
				data,
			});
			let key = RecordIdKey::Array(Array(group));
			let (ns, db) = ids(ctx).await?;
			ctx.txn()
				.put_record(ns, db, &table, &key, Arc::clone(&record))
				.await
				.map_err(ControlFlow::Err)?;
			maintain(state, ctx, &table, key, None, &record).await?;
		}
		_ => return Err(internal("a store instruction taken as a backfill instruction")),
	}
	Ok(None)
}

/// The namespace and database ids the session's catalog writes go to.
async fn ids(
	ctx: &ExecutionContext,
) -> FlowResult<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)> {
	let options = ctx.options().ok_or_else(|| internal("a view filled without options"))?;
	ctx.ctx().get_ns_db_ids(options).await.map_err(ControlFlow::Err)
}

/// A row of an aggregated view's backfill select as the group's keys and
/// the values its aggregates were computed to.
fn split(backfill: &Backfill, row: Value) -> FlowResult<(Vec<Value>, Vec<Value>)> {
	let Value::Object(mut object) = row else {
		return Err(internal("select without VALUE did not return an object"));
	};
	let mut group = Vec::with_capacity(backfill.groups);
	for g in 0..backfill.groups {
		let Some(key) = object.remove(&format!("g{g}")) else {
			return Err(internal("select result did not contain a field for a selection"));
		};
		group.push(key);
	}
	let Some(Value::Array(Array(computed))) = object.remove("a") else {
		return Err(internal("select result did not contain a field for a selection"));
	};
	Ok((group, computed))
}

/// The stats of the group whose aggregates were computed to `computed`, or
/// `None` for a row that stands for no group.
fn stats(
	backfill: &Backfill,
	computed: &[Value],
) -> FlowResult<Option<Vec<aggregation::AggregationStat>>> {
	backfill_stats(&backfill.aggregations, &backfill.values, computed).map_err(ControlFlow::Err)
}

/// Run the order of view table `table` over `record`, the row just stored
/// under `key`: a write of its own over a write state stacked on the
/// statement's scope, in the view table's view-maintenance context. The row is
/// created, or updated from `before` when it replaced a row the backfill wrote
/// under the same key. The order is the one of the table's catalog version,
/// taken the first time the backfill meets the table
/// ([`super::fault::view_rows`]) and held for the rest of it.
async fn maintain(
	state: &mut State,
	ctx: &ExecutionContext,
	table: &TableName,
	key: RecordIdKey,
	before: Option<&Record>,
	record: &Record,
) -> FlowResult<()> {
	let rows = match &state.backfill_order {
		Some((held, rows)) if held == table => Arc::clone(rows),
		_ => {
			let rows = super::fault::view_rows(table, ctx).await?;
			state.backfill_order = Some((table.clone(), Arc::clone(&rows)));
			rows
		}
	};
	let scope = Arc::clone(
		state
			.backfill
			.get_or_insert_with(|| WriteScope::new(ctx.clone(), TableExistence::DefinedOnDemand)),
	);
	let id = RecordId {
		table: table.clone(),
		key,
	};
	let write_ctx = scope.write_ctx_for(&id.table, WritePurpose::ViewMaintenance).await?;
	let exec = write_ctx.exec().clone();
	let action = match before {
		Some(_) => Action::Update,
		None => Action::Create,
	};
	let row = WriteRow::for_view_row(id, before, Some(record));
	let pipeline = rows
		.pipelines
		.get(super::write::action_pipeline(action))
		.ok_or_else(|| internal("a view table's order the program does not hold"))?;
	let mut nested = State::new(&rows, Vec::new(), exec.function_registry())?;
	nested.frames = rows.permissions.iter().map(|spec| spec.context(&exec)).collect();
	nested.permissions_bypassed = state.permissions_bypassed;
	nested.rowless = true;
	nested.write = Some(Box::new(WriteState::view_row(scope, row, write_ctx)));
	match Box::pin(run_value_row(&rows, pipeline, Value::None, &mut nested, &exec)).await {
		Ok(_) => Ok(()),
		Err(ControlFlow::Err(error)) => Err(ControlFlow::Err(error)),
		// Nothing a schema statement runs on its own behalf is inside a loop
		// or a function body, so a `BREAK`, `CONTINUE` or `RETURN` reaching
		// here escaped the construct that gives it meaning.
		Err(_) => Err(ControlFlow::Err(anyhow!(crate::exec::Error::InvalidControlFlow))),
	}
}
