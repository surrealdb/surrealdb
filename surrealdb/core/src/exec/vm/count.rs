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

/// The answer of count source `source` read as of `version`. With `at_most`,
/// a count of index keys stops once it reaches that many, which is then its
/// answer.
pub(super) async fn count(
	program: &Program,
	source: &CountSource,
	version: Option<u64>,
	units: Option<UnitTable>,
	ctx: &ExecutionContext,
	at_most: Option<usize>,
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
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Number(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)
				| Value::Array(_)
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::File(_)
				| Value::Regex(_)
				| Value::Range(_)
				| Value::Closure(_) => {
					return Err(ControlFlow::Err(anyhow::anyhow!(
						"CountScan received a non-table source"
					)));
				}
			};
			debug_assert_eq!(&name, table);
			check_table(&services, &name, version, true).await?;
			if check_perms {
				match select {
					crate::exec::vm::payload::CountSelect::Deny => return Ok(Counted::Count(0)),
					crate::exec::vm::payload::CountSelect::Conditional => {
						return ordinary_rows(program, units, *ordinary, version, ctx).await;
					}
					crate::exec::vm::payload::CountSelect::Allow => {}
				}
			}
			let count = match &rid {
				Some(rid) => match &rid.key {
					RecordIdKey::Range(range) => {
						services
							.count_keys(services.record_range(&rid.table, range)?, version)
							.await?
					}
					RecordIdKey::Number(_)
					| RecordIdKey::String(_)
					| RecordIdKey::Uuid(_)
					| RecordIdKey::Array(_)
					| RecordIdKey::Object(_) => usize::from(services.record_exists(rid, version).await?),
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
					crate::exec::vm::payload::CountSelect::Deny => return Ok(Counted::Count(0)),
					crate::exec::vm::payload::CountSelect::Conditional => {
						return ordinary_rows(program, units, *ordinary, version, ctx).await;
					}
					crate::exec::vm::payload::CountSelect::Allow => {}
				}
			}
			let count = match via {
				CountVia::Deltas {
					index,
				} => services.count_index_deltas(table, *index).await?,
				CountVia::BTreeKeys(walks) => {
					let mut count = 0;
					for walk in walks.iter() {
						let left = at_most.map(|most| most.saturating_sub(count));
						if left == Some(0) {
							break;
						}
						count +=
							super::entries::count(ctx, &services, &walk.index, &walk.access, left)
								.await?;
					}
					count
				}
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

/// Runs ordinary unit `unit` in `ctx` and returns its rows. The unit is the
/// statement as a pipeline of scan, permission, predicate and aggregate.
async fn ordinary_rows(
	program: &Program,
	units: Option<UnitTable>,
	unit: Option<UnitId>,
	version: Option<u64>,
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
	let ctx = if version.is_some() && ctx.version_stamp() != version {
		ctx.with_version_stamp(version)
	} else {
		ctx
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

/// The number of parts the keys left after a first count call are split
/// into when the backend counts in parallel.
const COUNT_PARTS: usize = 64;

/// The most calls of a key count in flight at once, each counting part of
/// the range on the backend's own threads.
const COUNTS_IN_FLIGHT: usize = 16;

/// The number of keys in `range` as of `version`, counted by key alone. Each
/// call charges one unit per key it examines and yields between calls.
///
/// A backend whose count is [`CountCost::Logarithmic`] answers the range in
/// one call, charged one unit. Any other backend counts through
/// [`count_bounded`](crate::kvs::Transaction::count_bounded). Each call
/// examines at most the batch it is given and resumes at the key it stopped
/// on, so no single call walks an unbounded range. Where the backend counts
/// off the calling thread ([`Transactor::parallel_count`]), the keys left
/// after the first call are split into parts over the band they occupy. The
/// parts are counted several at a time, each charged as its call returns.
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
	let mut parts = surrealdb_kvs::split::split_range(&from, &band_end, COUNT_PARTS).into_iter();
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

#[cfg(test)]
mod range_shape {
	use surrealdb_kvs::split::split_range;

	use crate::key::KVKey;
	use crate::key::schema::RecordPrefix;

	/// A table's own record range divides into the extents a parallel count asks for.
	///
	/// The splitter divides on the bytes that distinguish the bounds, so what it can do depends on
	/// the shape of the range the scan is given. A range whose bounds differ only in a final marker
	/// byte offers one value to divide, and the count that asked for many extents would get one.
	/// The extents of a table's range hold comparable numbers of its records.
	///
	/// Dividing the range evenly divides the rows evenly only if the keys are spread evenly
	/// through it. A generated record id is base32 text, so its bytes occupy a narrow band of the
	/// range the splitter divides; extents outside that band hold nothing and the count is as slow
	/// as whichever extent holds the rows.
	#[test]
	fn extents_hold_comparable_numbers_of_real_record_keys() {
		let range = RecordPrefix {
			ns: crate::catalog::NamespaceId(1),
			db: crate::catalog::DatabaseId(1),
			tb: std::borrow::Cow::Owned(crate::val::TableName::from("person")),
		}
		.range()
		.expect("record range");
		let (lo, hi) = range.bounds();

		// Keys as the default generator makes them: 20 characters of base32 text.
		const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
		let mut seed = 0x2545_F491_4F6C_DD1Du64;
		let keys: Vec<Vec<u8>> = (0..5000)
			.map(|_| {
				let id: String = (0..20)
					.map(|_| {
						seed ^= seed << 13;
						seed ^= seed >> 7;
						seed ^= seed << 17;
						ALPHABET[(seed % ALPHABET.len() as u64) as usize] as char
					})
					.collect();
				crate::key::schema::RecordKey {
					ns: crate::catalog::NamespaceId(1),
					db: crate::catalog::DatabaseId(1),
					tb: std::borrow::Cow::Owned(crate::val::TableName::from("person")),
					id: std::borrow::Cow::Owned(crate::val::RecordIdKey::String(id.into())),
				}
				.encode_key()
				.expect("encode")
				.as_slice()
				.to_vec()
			})
			.collect();

		// The scan divides the band the keys occupy, found as `count_rest` finds it,
		// not the range's own bounds.
		let mut sorted = keys.clone();
		sorted.sort();
		let first = sorted.first().expect("keys").clone();
		let mut band_end = sorted.last().expect("keys").clone();
		band_end.push(0);
		assert!(
			first.as_slice() >= lo && band_end.as_slice() <= hi,
			"the band sits inside the range"
		);
		let extents = split_range(&first, &band_end, 12);
		let counts: Vec<usize> = extents
			.iter()
			.map(|(s, e)| {
				keys.iter()
					.filter(|k| k.as_slice() >= s.as_slice() && k.as_slice() < e.as_slice())
					.count()
			})
			.collect();
		let occupied = counts.iter().filter(|&&c| c > 0).count();
		let biggest = counts.iter().max().copied().unwrap_or(0);
		assert_eq!(counts.iter().sum::<usize>(), keys.len(), "every key lands in one extent");
		assert!(
			occupied >= extents.len() / 2,
			"only {occupied} of {} extents hold any rows: {counts:?}",
			extents.len()
		);
		assert!(
			biggest <= keys.len() / extents.len() * 3,
			"one extent holds {biggest} of {} rows; a parallel count is as slow as that extent: \
			 {counts:?}",
			keys.len()
		);
	}

	#[test]
	fn a_tables_record_range_divides_into_extents() {
		let range = RecordPrefix {
			ns: crate::catalog::NamespaceId(1),
			db: crate::catalog::DatabaseId(1),
			tb: std::borrow::Cow::Owned(crate::val::TableName::from("person")),
		}
		.range()
		.expect("record range");
		let (lo, hi) = range.bounds();
		let extents = split_range(lo, hi, 16);
		assert!(
			extents.len() > 1,
			"a table's record range must divide, or a parallel count silently runs one extent; \
			 lo={lo:?} hi={hi:?} gave {} extent(s)",
			extents.len()
		);
	}
}
