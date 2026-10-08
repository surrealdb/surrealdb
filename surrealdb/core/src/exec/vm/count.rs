//! The run of a count source ([`Source::Count`](super::program::Source)).
//!
//! A count is answered by the cheapest structure that holds it: the keys of
//! a record range, the deltas of a `COUNT` index, the keys of a B-tree index
//! or the cardinality of a fused bitmap, each read through
//! [`Services`](super::services::Services). What the lowering decided from
//! the catalog — the table's permission, which index answers, and whether an
//! ordinary pipeline stands beside the shortcut — is read off the source; the
//! run reads only the table's definition, as of the statement's version, for
//! its existence and its kind.
//!
//! A caller whose reads are checked meets the table's permission first: a
//! table that admits no record counts none, and one that judges each record
//! is answered by the ordinary pipeline's own row, which applies the
//! permission, the predicate and the projection as any statement does. A
//! caller whose reads are not checked takes the shortcut whatever the
//! permission.

use std::sync::Arc;

use futures::StreamExt;

use super::UnitTable;
use super::host::{execute_program, host_scalar, out_of_line};
use super::program::{CountSource, CountVia, CountedValue, HostScalar, Program, UnitId};
use super::services::{EngineServices, Services};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::iam::Action;
use crate::val::{RecordIdKey, TableName, Value};

/// What a count source answers with.
pub(super) enum Counted {
	/// The number of records counted, which the source's output shape wraps.
	Count(usize),
	/// The rows of the ordinary pipeline, which are the statement's own.
	Rows(Vec<Value>),
}

/// The answer of count source `source` read as of `version`.
pub(super) async fn count(
	program: &Program,
	source: &CountSource,
	version: Option<u64>,
	units: Option<UnitTable>,
	ctx: &ExecutionContext,
) -> FlowResult<Counted> {
	let db_ctx = ctx.database()?;
	let check_perms = crate::exec::permission::should_check_perms(db_ctx, Action::View)?;
	let services = EngineServices::new(ctx);
	match source {
		CountSource::All {
			table,
			source,
			select,
			deltas,
			ordinary,
		} => {
			let value = match source {
				CountedValue::Table(table) => Value::Table(table.clone()),
				CountedValue::Code(pipeline) => {
					let scalar = HostScalar::Code(*pipeline);
					out_of_line(|| host_scalar(program, &scalar, ctx, None)).await?
				}
			};
			let (name, rid) = match value {
				Value::Table(name) => (name, None),
				Value::RecordId(rid) => (rid.table.clone(), Some(rid)),
				_ => {
					return Err(ControlFlow::Err(anyhow::anyhow!(
						"CountScan received a non-table source"
					)));
				}
			};
			debug_assert_eq!(&name, table);
			check_table(&services, &name, version, true).await?;
			if check_perms {
				match select {
					crate::exec::vm::plan::CountSelect::Deny => return Ok(Counted::Count(0)),
					crate::exec::vm::plan::CountSelect::Conditional => {
						return ordinary_rows(program, units, *ordinary, ctx).await;
					}
					crate::exec::vm::plan::CountSelect::Allow => {}
				}
			}
			let count = match &rid {
				Some(rid) => match &rid.key {
					RecordIdKey::Range(range) => {
						services
							.count_keys(services.record_range(&rid.table, range)?, version)
							.await?
					}
					_ => usize::from(services.record_exists(rid, version).await?),
				},
				None => match deltas {
					Some(index) if version.is_none() => {
						services.count_index_deltas(&name, *index).await?
					}
					_ => services.count_keys(services.table_range(&name)?, version).await?,
				},
			};
			Ok(Counted::Count(count))
		}
		CountSource::Indexed {
			table,
			select,
			via,
			ordinary,
		} => {
			check_table(&services, table, version, false).await?;
			if check_perms {
				match select {
					crate::exec::vm::plan::CountSelect::Deny => return Ok(Counted::Count(0)),
					crate::exec::vm::plan::CountSelect::Conditional => {
						return ordinary_rows(program, units, *ordinary, ctx).await;
					}
					crate::exec::vm::plan::CountSelect::Allow => {}
				}
			}
			let count = match via {
				CountVia::Deltas {
					index,
				} => services.count_index_deltas(table, *index).await?,
				CountVia::BTreeKeys {
					index,
					access,
				} => services.count_btree_keys(index, access).await?,
				CountVia::Bitmap(plan) => services.bitmap_cardinality(table, plan).await?,
			};
			Ok(Counted::Count(count))
		}
	}
}

/// Fails unless `table` is defined as of `version`. A count of keys reads no
/// record, so a lightweight relation, which stores none, is refused when
/// `by_keys`.
async fn check_table(
	services: &EngineServices<'_>,
	table: &TableName,
	version: Option<u64>,
	by_keys: bool,
) -> FlowResult<()> {
	let Some(def) = services.read_table(table, version).await? else {
		return Err(ControlFlow::Err(anyhow::Error::new(crate::catalog::Error::TbNotFound {
			name: table.clone(),
		})));
	};
	if by_keys && crate::kvs::lightweight::lightweight_relation(&def.table_type).is_some() {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"a LIGHTWEIGHT relation cannot be scanned by this execution path; \
			 reference the table statically so the record-less scan can serve it"
		)));
	}
	Ok(())
}

/// The rows of ordinary unit `unit`, the statement as a pipeline of scan,
/// permission, predicate and aggregate answers it, run in `ctx`.
async fn ordinary_rows(
	program: &Program,
	units: Option<UnitTable>,
	unit: Option<UnitId>,
	ctx: &ExecutionContext,
) -> FlowResult<Counted> {
	let internal = |why: &str| {
		ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(why.to_owned())))
	};
	let unit = unit.ok_or_else(|| internal("a count has no ordinary pipeline to run"))?;
	let units = if program.units.is_empty() {
		units
			.or_else(|| ctx.vm_units().cloned())
			.ok_or_else(|| internal("a count runs a unit and holds no unit table"))?
	} else {
		Arc::clone(&program.units)
	};
	let unit: Arc<Program> = Arc::clone(super::host::admitted_unit(&units, unit, ctx)?);
	// A unit that holds no arena of its own finds the units its ids name in
	// the table it was found in.
	let ctx = if unit.units.is_empty() {
		ctx.with_vm_units(units)
	} else {
		ctx.clone()
	};
	let rows = crate::exec::trampoline::nest(async move {
		let mut stream = execute_program(&unit, &ctx)?;
		let mut rows = Vec::new();
		while let Some(batch) = stream.next().await {
			rows.extend(batch?.into_values());
		}
		Ok::<_, ControlFlow>(rows)
	})
	.await?;
	Ok(Counted::Rows(rows))
}
