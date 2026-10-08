//! The host's side of the instructions that fill a view from the tables it
//! reads.
//!
//! A view's backfill is a program: a select over the view's sources computes
//! the rows (or the groups) the view holds, and these instructions turn each
//! into the view table's record. A view's rows are metadata no statement can
//! express — an aggregated row's stats — so they are written to storage
//! directly, and then run through the view table's own order (its indexes, its
//! second-order views, events, live queries and change feed), which the
//! program holds as a unit lowered with it, as the incremental path runs the
//! order for a created row.

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::anyhow;

use super::host::{admitted_unit, run_value_row, unit_table};
use super::interp::State;
use super::program::{Backfill, Program, StoreInstr, UnitId};
use super::scalar::Scalar;
use super::store::{read, table_name_of, write};
use super::write::WriteState;
use crate::catalog::aggregation::{self, backfill_stats};
use crate::catalog::providers::TableProvider as _;
use crate::catalog::{Metadata, Record, RecordType};
use crate::exec::operators::write::clauses::WriteClauses;
use crate::exec::operators::write::ctx::TableExistence;
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::scope::{WritePurpose, WriteScope};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::key::schema::RecordKey;
use crate::val::{Array, RecordId, RecordIdKey, TableName, Value};

fn internal(why: impl Into<String>) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(why.into())))
}

/// The pipeline of a view table's order that takes a created row.
const CREATE: usize = 0;

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
			order,
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
			ctx.txn().put_key(&key, &record).await.map_err(ControlFlow::Err)?;
			maintain(program, state, ctx, *order, &table, id.key, &record).await?;
			yield_now!();
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
			order,
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
			maintain(program, state, ctx, *order, &table, key, &record).await?;
			yield_now!();
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

/// Run the order of view table `table`, unit `order`, over `record`, the row
/// just stored under `key`: a write of its own over a write state stacked on
/// the statement's scope, in the view table's view-maintenance context. With
/// no `order`, the table's order is lowered the first time the backfill meets
/// the table ([`super::fault::view_rows`]) and held for the rest of it.
async fn maintain(
	program: &Program,
	state: &mut State,
	ctx: &ExecutionContext,
	order: Option<UnitId>,
	table: &TableName,
	key: RecordIdKey,
	record: &Record,
) -> FlowResult<()> {
	let rows = match order {
		Some(order) => {
			let units = unit_table(program, state, ctx)?;
			Arc::clone(admitted_unit(&units, order, ctx)?)
		}
		None => match &state.backfill_order {
			Some((held, rows)) if held == table => Arc::clone(rows),
			_ => {
				let rows = super::fault::view_rows(table, ctx).await?;
				state.backfill_order = Some((table.clone(), Arc::clone(&rows)));
				rows
			}
		},
	};
	let scope = Arc::clone(state.backfill.get_or_insert_with(|| {
		WriteScope::new(
			ctx.clone(),
			Arc::new(WriteClauses::default()),
			None,
			TableExistence::DefinedOnDemand,
		)
	}));
	let id = RecordId {
		table: table.clone(),
		key,
	};
	let write_ctx = scope.write_ctx_for(&id.table, WritePurpose::ViewMaintenance).await?;
	let exec = write_ctx.exec().clone();
	let row = WriteRow::for_view_row(id, None, Some(record));
	let pipeline = rows
		.pipelines
		.get(CREATE)
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
