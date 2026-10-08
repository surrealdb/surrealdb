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

/// The parts the rest of a key range is divided into once its first call
/// shows it holds more than one call's keys, where the backend counts in
/// parallel.
const COUNT_PARTS: usize = 64;

/// The most calls of a key count in flight at once, each counting part of
/// the range on the backend's own threads.
const COUNTS_IN_FLIGHT: usize = 16;

/// The number of keys in `range` as of `version`, counted by key alone, a
/// unit a key examined, with a yield point between calls.
///
/// A backend whose count is [`CountCost::Logarithmic`] answers the range in
/// one call, charged a unit. Any other counts through
/// [`count_bounded`](crate::kvs::Transaction::count_bounded), each call
/// examining no more keys than the fuel left and resumed at the key it
/// stopped on: one call counting a whole range would be a walk no quantum
/// bounds. Where the backend counts off the calling thread
/// ([`Transactor::parallel_count`]), what the first call leaves is divided
/// into parts over the band its keys occupy, and the parts are counted a
/// quantum of keys a call, several calls in flight at once, each charged as
/// it returns.
///
/// [`CountCost::Logarithmic`]: surrealdb_kvs::CountCost::Logarithmic
/// [`Transactor::parallel_count`]: surrealdb_datastore::Transactor::parallel_count
pub(super) async fn count_keys(
	ctx: &ExecutionContext,
	range: crate::key::RawRange,
	version: Option<u64>,
) -> FlowResult<usize> {
	use anyhow::Context as _;

	let txn = ctx.txn();
	if txn.count_cost() == surrealdb_kvs::CountCost::Logarithmic {
		let counted = txn.count(range, version).await.context("Failed to count records")?;
		super::fuel::tick(ctx, 1).await?;
		return Ok(counted);
	}
	let most = super::fuel::batch(crate::kvs::NORMAL_BATCH_SIZE);
	let (mut counted, mut resume) =
		txn.count_bounded(range.clone(), version, most).await.context("Failed to count records")?;
	super::fuel::tick(ctx, super::fuel::units(counted)).await?;
	if txn.parallel_count() {
		return match resume {
			Some(from) => Ok(counted + Box::pin(count_rest(ctx, &range, from, version)).await?),
			None => Ok(counted),
		};
	}
	let (_, end) = range.bounds();
	while let Some(from) = resume {
		let most = super::fuel::batch(crate::kvs::NORMAL_BATCH_SIZE);
		let (part, next) = txn
			.count_bounded(range.part(&from, end), version, most)
			.await
			.context("Failed to count records")?;
		counted += part;
		resume = next;
		super::fuel::tick(ctx, super::fuel::units(part)).await?;
	}
	Ok(counted)
}

/// The keys of `range` from `from` on, counted in parts over the band they
/// occupy ([`count_keys`]).
pub(super) async fn count_rest(
	ctx: &ExecutionContext,
	range: &crate::key::RawRange,
	from: Vec<u8>,
	version: Option<u64>,
) -> FlowResult<usize> {
	use anyhow::Context as _;
	use futures::stream::FuturesUnordered;

	use crate::kvs::Direction;

	let txn = ctx.txn();
	// The band runs from `from` to just after the range's last key, found by
	// one seek from its end.
	let (_, end) = range.bounds();
	let mut tail = txn
		.open_keys_cursor_raw(range.part(&from, end), Direction::Backward, 0, version)
		.await
		.context("Failed to count records")?;
	let mut band_end = None;
	tail.for_each(1, &mut |key| {
		band_end = Some(key.to_vec());
		Ok(std::ops::ControlFlow::Continue(()))
	})
	.await
	.context("Failed to count records")?;
	drop(tail);
	let Some(mut band_end) = band_end else {
		return Ok(0);
	};
	band_end.push(0);
	let budget = super::fuel::quantum(ctx);
	let in_flight = std::thread::available_parallelism()
		.map_or(4, std::num::NonZeroUsize::get)
		.min(COUNTS_IN_FLIGHT);
	let call = |lo: Vec<u8>, hi: Vec<u8>| {
		let txn = &txn;
		let part = range.part(&lo, &hi);
		async move { (txn.count_bounded(part, version, budget).await, hi) }
	};
	let mut parts =
		crate::exec::operators::scan::extents::split_range(&from, &band_end, COUNT_PARTS)
			.into_iter();
	let mut running = FuturesUnordered::new();
	for (lo, hi) in parts.by_ref().take(in_flight) {
		running.push(call(lo, hi));
	}
	let mut counted = 0;
	while let Some((part, hi)) = running.next().await {
		let (part, resume) = part.context("Failed to count records")?;
		crate::exec::operators::check_cancelled(ctx)?;
		counted += part;
		match resume {
			Some(lo) => running.push(call(lo, hi)),
			None => {
				if let Some((lo, hi)) = parts.next() {
					running.push(call(lo, hi));
				}
			}
		}
		super::fuel::tick(ctx, super::fuel::units(part)).await?;
	}
	Ok(counted)
}
