//! Sources that do not read a table the program names, and statements
//! nested in an expression. Both run over the program's own state.
//!
//! A value in `FROM` ([`Source::Values`]) is evaluated when its pipeline opens
//! and read as [`ValuesRead`](super::payload::ValuesRead) describes. A record id among the values
//! is read under the caller's permissions. A table found by a dynamic read is read as
//! the pipeline takes its rows: storage is asked for no more records than the
//! pipeline's paging admits, and no more once a limit of the pipeline is
//! spent. When the program was lowered with the units that read each table its
//! values can name ([`super::dispatch`]), the record ids and tables are read by
//! the unit of their table. A record no unit reads is read by the fetch helpers.
//! A table no unit reads was not in the catalog when the program was lowered. The run
//! created it by writing to it, so no unit was lowered for it. It is read
//! by key range, the way a scan reads any table ([`undispatched_table`]).
//! Nothing is planned or lowered while the program runs.
//!
//! A statement in `FROM` ([`Source::Nested`]) and a statement in scalar
//! position ([`BlockInstr::Nested`]) are groups of the program
//! ([`Program::groups`]). [`super::host::run_group`] runs them over the
//! running state. A statement in scalar position runs in the frame where
//! it is evaluated, with the row as its `$parent` and `$this`. It does not
//! see the row-level state of the pipeline that evaluates it: the frame,
//! the permission bypass, the write in flight, the record a computed frame
//! computes, and the zero limit of a statement in `FROM`. That state is set
//! aside before the statement runs and restored after it. The run is a frame
//! of the statement's driver, so nesting uses no thread stack.
//!
//! A statement joined to the row it is evaluated for reads through a probe
//! ([`Source::Probe`]). The rows under its join predicate are read once,
//! at the first opening, and kept in a map from the value of the join
//! field to the rows holding it ([`Probe`]). Each opening yields the kept
//! rows whose field may equal the key, in the order they were read.

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;

use super::host::{
	Ended, Out, execute_program_reading, flush, host_scalar, limits_spent, run_group, run_rows,
	run_value_row,
};
use super::interp::{Kept, State};
use super::ledger::Site;
use super::membership::Members;
use super::payload::ValuesRead;
use super::program::{BlockInstr, HostScalar, Pipeline, Program, Source};
use crate::exec::operators::fetch::fetch_raw_record;
use crate::exec::operators::scan::common::resolve_version_stamp;
use crate::exec::operators::scan::pipeline::kv_scan_stream;
use crate::exec::operators::scan::resolved::EnforcementMode;
use crate::exec::operators::scan::{execute_record_lookup, record_key_range};
use crate::exec::{ExecutionContext, FlowResult, ValueBatchStream};
use crate::expr::ControlFlow;
use crate::val::{RecordIdKey, TableName, Value};

// How many probes this thread has read the rows of, for tests that must
// know a probe read its rows once.
#[cfg(test)]
thread_local! {
	pub(super) static PROBES_BUILT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Run `pipeline` over the rows of `source`, a value or a statement in
/// `FROM`. `spent` is whether a limit of the pipeline admits no row. Answers
/// whether the pipeline stopped.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_source(
	program: &Arc<Program>,
	pipeline: &Pipeline,
	source: &Source,
	spent: bool,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<bool> {
	match source {
		Source::Values {
			value,
			read,
		} => {
			let zeroed = spent || state.zeroed;
			let reads = paged_reads(pipeline, state);
			match values(program, value, *read, zeroed, reads, ctx).await? {
				Values::Rows(rows) => {
					let rows = rows.into_iter().map(|row| (Vec::new(), row)).collect();
					run_rows(program, pipeline, rows, state, ctx, yielder, flush_at).await
				}
				Values::Stream(rows) => {
					stream_rows(program, pipeline, rows, state, ctx, yielder, flush_at).await
				}
			}
		}
		Source::Nested {
			group,
		} => {
			// The zero limit binds the statement's own reads of storage,
			// including the scans beneath it, unless it writes.
			let zeroed = (spent || state.zeroed) && program.groups[*group as usize].write.is_none();
			let outer = std::mem::replace(&mut state.zeroed, zeroed);
			let (rows, ran) = run_group(program, *group, state, ctx).await;
			state.zeroed = outer;
			ran?;
			let rows = rows.into_iter().map(|row| (Vec::new(), row)).collect();
			run_rows(program, pipeline, rows, state, ctx, yielder, flush_at).await
		}
		Source::Probe {
			build,
			probe,
			field,
			outer,
		} => {
			let kept = match &state.probes[*probe as usize] {
				Some(kept) => Arc::clone(kept),
				None => {
					let (rows, ran) = run_group(program, *build, state, ctx).await;
					ran?;
					#[cfg(test)]
					PROBES_BUILT.with(|built| built.set(built.get() + 1));
					let kept = Arc::new(Probe::new(rows, field));
					state.probes[*probe as usize] = Some(Arc::clone(&kept));
					kept
				}
			};
			// The join predicate reads the key only for a row it judges.
			if kept.rows.is_empty() {
				return Ok(false);
			}
			let parent = match ctx.binding("parent") {
				Some(crate::ctx::Binding::Value(parent)) => Some(parent.as_ref()),
				Some(crate::ctx::Binding::Forbidden(row)) => return Err(row.error()),
				None => None,
			};
			let rows = match key_at(parent, outer) {
				Some(key) => {
					kept.candidates(key).map(|at| (Vec::new(), kept.rows[at].clone())).collect()
				}
				None => kept.rows.iter().map(|row| (Vec::new(), row.clone())).collect(),
			};
			run_rows(program, pipeline, rows, state, ctx, yielder, flush_at).await
		}
		_ => Err(ControlFlow::Err(anyhow::anyhow!("a source that is not a value or a statement"))),
	}
}

/// Run `pipeline` over the rows of `rows`, a table's as they are read.
/// Answers whether the pipeline stopped: the read stops once a limit of the
/// pipeline is spent, reading no row past it.
async fn stream_rows(
	program: &Arc<Program>,
	pipeline: &Pipeline,
	mut rows: ValueBatchStream,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<bool> {
	while let Some(batch) = rows.next().await {
		// An empty batch is a reader's hand-off once its refusals spent the
		// fuel; the rows of any other spend it as they run.
		super::fuel::tick(ctx, 0).await?;
		for row in batch?.into_values() {
			if run_value_row(program, pipeline, row, state, ctx).await? == Ended::Stop
				|| limits_spent(pipeline, state)
			{
				return Ok(true);
			}
		}
		flush(state, yielder, flush_at, ctx).await?;
	}
	Ok(false)
}

/// The value at `path` of `parent`, read through objects only. Returns `None`
/// where the path meets any other value. Reading such a value, such as
/// fetching a record or mapping an array, is left to the predicate.
pub(super) fn key_at<'a>(
	parent: Option<&'a Value>,
	path: &[surrealdb_strand::Strand],
) -> Option<&'a Value> {
	let mut value = parent?;
	for field in path {
		match value {
			Value::Object(object) => value = object.get(field.as_str()).unwrap_or(&Value::None),
			_ => return None,
		}
	}
	Some(value)
}

/// The rows a probe keeps, in the order they were read. Each is indexed by
/// the value of the join field, or `NONE` when a row has none.
///
/// [`Probe::candidates`] returns a superset of the rows whose field `=`
/// ([`Value::equal`]) the key. The join predicate still judges each one.
/// A map is exact only where hashing agrees with `=`
/// ([`Value::hash_agrees_with_eq`]). A number's hash differs between its
/// representations, so numbers are indexed by representation. A key is
/// looked up in each representation it can equal exactly: an integer equals
/// a float or decimal only when both have the same integral value. A float
/// and a decimal compare approximately, so they match any row of the other
/// representation. A field value that is neither a number nor hash-agreeing
/// is a candidate for every key, and so is a regex that can match a string
/// key. A key that is neither matches every row.
pub(crate) struct Probe {
	rows: Vec<Value>,
	ints: HashMap<i64, Vec<usize>>,
	floats: HashMap<u64, Vec<usize>>,
	decimals: HashMap<rust_decimal::Decimal, Vec<usize>>,
	hashed: HashMap<Value, Vec<usize>>,
	/// Every row indexed by a float, and by a decimal.
	float_rows: Vec<usize>,
	decimal_rows: Vec<usize>,
	/// The rows whose field is a candidate for every key.
	loose: Vec<usize>,
}

impl std::fmt::Debug for Probe {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Probe").field("rows", &self.rows.len()).finish()
	}
}

/// A float's bits with both zeros one key, as `=` holds them equal.
fn float_key(f: f64) -> u64 {
	if f == 0.0 {
		0.0f64.to_bits()
	} else {
		f.to_bits()
	}
}

/// The integer a float denotes exactly, if any.
fn float_int(f: f64) -> Option<i64> {
	// The bounds are the i64 range, the upper exclusive as 2^63 is not an
	// i64.
	(f.is_finite()
		&& f.fract() == 0.0
		&& (-9_223_372_036_854_776_000.0..9_223_372_036_854_776_000.0).contains(&f))
	.then_some(f as i64)
}

impl Probe {
	fn new(rows: Vec<Value>, field: &str) -> Self {
		use crate::val::Number;
		let mut probe = Self {
			rows: Vec::new(),
			ints: HashMap::new(),
			floats: HashMap::new(),
			decimals: HashMap::new(),
			hashed: HashMap::new(),
			float_rows: Vec::new(),
			decimal_rows: Vec::new(),
			loose: Vec::new(),
		};
		for (at, row) in rows.iter().enumerate() {
			let key = match row {
				Value::Object(object) => object.get(field).unwrap_or(&Value::None),
				_ => &Value::None,
			};
			match key {
				Value::Number(Number::Int(i)) => probe.ints.entry(*i).or_default().push(at),
				Value::Number(Number::Float(f)) => {
					probe.floats.entry(float_key(*f)).or_default().push(at);
					probe.float_rows.push(at);
				}
				Value::Number(Number::Decimal(d)) => {
					probe.decimals.entry(d.normalize()).or_default().push(at);
					probe.decimal_rows.push(at);
				}
				key if key.hash_agrees_with_eq() => {
					probe.hashed.entry(key.clone()).or_default().push(at)
				}
				_ => probe.loose.push(at),
			}
		}
		probe.rows = rows;
		probe
	}

	/// The rows whose field may equal `key`, in the order they were read.
	fn candidates(&self, key: &Value) -> impl Iterator<Item = usize> {
		use crate::val::Number;
		let mut out: Vec<usize> = Vec::new();
		let mut all = false;
		let mut take = |rows: Option<&Vec<usize>>| out.extend(rows.into_iter().flatten().copied());
		match key {
			Value::Number(Number::Int(i)) => {
				take(self.ints.get(i));
				let f = *i as f64;
				if float_int(f) == Some(*i) {
					take(self.floats.get(&float_key(f)));
				}
				take(self.decimals.get(&rust_decimal::Decimal::from(*i).normalize()));
			}
			Value::Number(Number::Float(f)) => {
				take(self.floats.get(&float_key(*f)));
				if let Some(i) = float_int(*f) {
					take(self.ints.get(&i));
				}
				take(Some(&self.decimal_rows));
			}
			Value::Number(Number::Decimal(d)) => {
				take(self.decimals.get(&d.normalize()));
				if d.fract().is_zero()
					&& let Some(i) = rust_decimal::prelude::ToPrimitive::to_i64(d)
				{
					take(self.ints.get(&i));
				}
				take(Some(&self.float_rows));
			}
			key if key.hash_agrees_with_eq() => take(self.hashed.get(key)),
			_ => all = true,
		}
		if all {
			out = (0..self.rows.len()).collect();
		} else {
			out.extend(self.loose.iter().copied());
			out.sort_unstable();
			out.dedup();
		}
		out.into_iter()
	}
}

/// What a value in `FROM` yields: its rows, or, for a table it names, the
/// table's rows as they are read.
enum Values {
	Rows(Vec<Value>),
	/// The rows of a table: those its unit reads, or every record of a table
	/// no unit reads ([`undispatched_table`]).
	Stream(ValueBatchStream),
}

/// How many rows `pipeline` takes before a limit of it is spent, as `state`
/// pages it: the rows its `START` skips and its `LIMIT` admits, the fewest
/// of its limits and at least one; `None` where none bounds it.
fn paged_reads(pipeline: &Pipeline, state: &State) -> Option<u32> {
	pipeline
		.limits
		.iter()
		.filter_map(|at| {
			let paging = state.paging.get(*at as usize)?;
			paging.remaining.map(|remaining| paging.skip.saturating_add(remaining))
		})
		.min()
		.map(|reads| u32::try_from(reads.max(1)).unwrap_or(u32::MAX))
}

/// The value of a `FROM` source from the host's run of it: a `RETURN` inside it
/// is its value, and a `BREAK` or `CONTINUE` passes outwards to the loop the
/// statement is written in.
fn source_value(ran: FlowResult<Value>) -> FlowResult<Value> {
	match ran {
		Err(ControlFlow::Return(value)) => Ok(value),
		ran => ran,
	}
}

/// The rows `value` yields, read as `read` says; see
/// [`ValuesRead`](super::payload::ValuesRead). Under `zeroed`, a record id or
/// table a dynamic read finds reads no record; a table a dynamic read finds
/// is read `reads` records at a time first, where the pipeline takes no more
/// ([`paged_reads`]).
async fn values(
	program: &Program,
	value: &HostScalar,
	read: ValuesRead,
	zeroed: bool,
	reads: Option<u32>,
	ctx: &ExecutionContext,
) -> FlowResult<Values> {
	match read {
		ValuesRead::Expression {
			written,
		} => {
			// In a nested statement, `$this` is the enclosing row. The expression
			// reads it as its current value and as its document.
			let (this, forbidding) = ctx.enclosing_row();
			let ctx = forbidding.as_ref().unwrap_or(ctx);
			let value = source_value(host_scalar(program, value, ctx, this).await)?;
			let rows = match value {
				Value::Array(array) => {
					let mut rows = Vec::with_capacity(array.len());
					for value in array {
						match value {
							Value::Array(inner) if written => rows.extend(inner),
							other => rows.push(other),
						}
					}
					records_of(ctx, rows).await?
				}
				Value::None | Value::Null => Vec::new(),
				Value::RecordId(rid) => {
					match crate::exec::operators::fetch::fetch_record(ctx, &rid).await? {
						Value::None => Vec::new(),
						record => vec![record],
					}
				}
				other => vec![other],
			};
			Ok(Values::Rows(rows))
		}
		ValuesRead::Locked => {
			let value = source_value(host_scalar(program, value, ctx, None).await)?;
			// A locked read is of the record id the value is, which the units
			// serving the table do not lock.
			let Value::RecordId(rid) = value else {
				return Err(ControlFlow::Err(anyhow::Error::new(crate::exec::Error::Query {
					message: crate::dbs::FOR_UPDATE_TARGETS_ERROR.to_string(),
				})));
			};
			let db_ctx = ctx.database()?;
			let check_perms =
				crate::exec::permission::should_check_perms(db_ctx, crate::iam::Action::View)
					.map_err(|e| ControlFlow::Err(e.into()))?;
			let version = resolve_version_stamp(ctx, None).await?;
			let rows = execute_record_lookup(
				&rid,
				version,
				true,
				check_perms,
				None,
				ctx,
				None,
				zeroed.then_some(0),
				0,
				EnforcementMode::Runtime,
			)
			.await?;
			Ok(Values::Rows(rows))
		}
		ValuesRead::Dynamic => {
			let value = source_value(host_scalar(program, value, ctx, None).await)?;
			if let Some(deref) = ctx.vm_deref().cloned()
				&& reads_through(&deref, &value, ctx).await?
			{
				return dispatched(&deref, value, zeroed, reads, ctx).await;
			}
			match value {
				Value::RecordId(rid)
					if !zeroed
						&& let Some(view) =
							own_record(ctx, &rid, resolve_version_stamp(ctx, None).await?)
								.await? =>
				{
					Ok(Values::Rows(view.into_iter().collect()))
				}
				Value::RecordId(rid) => {
					let db_ctx = ctx.database()?;
					let check_perms = crate::exec::permission::should_check_perms(
						db_ctx,
						crate::iam::Action::View,
					)
					.map_err(|e| ControlFlow::Err(e.into()))?;
					let version = resolve_version_stamp(ctx, None).await?;
					// A zero limit checks the table and its permission and
					// reads no record.
					let rows = execute_record_lookup(
						&rid,
						version,
						false,
						check_perms,
						None,
						ctx,
						None,
						zeroed.then_some(0),
						0,
						EnforcementMode::Runtime,
					)
					.await?;
					Ok(Values::Rows(rows))
				}
				Value::Array(array) => Ok(Values::Rows(records_of(ctx, array.0).await?)),
				Value::Table(table) => {
					let db_ctx = ctx.database()?;
					let version = resolve_version_stamp(ctx, None).await?;
					if absent_and_hidden(ctx, &table, version).await? {
						return Ok(Values::Rows(Vec::new()));
					}
					if db_ctx.get_table_def(&table, version).await?.is_none() {
						return Err(ControlFlow::Err(anyhow::Error::new(
							crate::catalog::Error::TbNotFound {
								name: table,
							},
						)));
					}
					// A zero limit reads no record once the table is known.
					if zeroed {
						return Ok(Values::Rows(Vec::new()));
					}
					undispatched_table(ctx, &table, version, reads).await
				}
				Value::None | Value::Null => Ok(Values::Rows(Vec::new())),
				other => Ok(Values::Rows(vec![other])),
			}
		}
	}
}

/// Whether the units of `deref` read `value`: it is a record id or a table
/// a unit of the program or of the run serves in the frame `ctx` runs in
/// ([`super::Deref::serving`]), or a value that names none.
async fn reads_through(
	deref: &super::Deref,
	value: &Value,
	ctx: &ExecutionContext,
) -> FlowResult<bool> {
	let table = match value {
		Value::RecordId(rid) => &rid.table,
		Value::Table(table) => table,
		_ => return Ok(true),
	};
	Ok(deref.serving(ctx, table, ctx.root().skip_fetch_perms).await?.is_some())
}

/// The record `rid` as a statement reads it from inside one of the record's
/// own computed fields, at `version`. The view has no computed field derived,
/// because deriving them is the work this read is part of. It is the view the
/// computation holds, or else the record as stored. Returns `None` for any
/// other read, which reads the record as it reads every other.
async fn own_record(
	ctx: &ExecutionContext,
	rid: &crate::val::RecordId,
	version: Option<u64>,
) -> FlowResult<Option<Option<Value>>> {
	let Some(computing) =
		ctx.root().computing_record.as_ref().filter(|computing| computing.rid == *rid)
	else {
		return Ok(None);
	};
	Ok(Some(match &computing.stored {
		Some(stored) => Some(stored.clone()),
		None => fetch_raw_record(ctx, rid, version).await?,
	}))
}

/// The rows a dynamic read of `value` yields, read by the units of `deref`.
/// A record id or range is read by the unit of its table, as a table is.
/// An array's elements are read by [`records_of`].
/// Under `zeroed`, a record id or table reads no record once its table is
/// known to exist. A table is read by its unit with the same paging as a
/// table named in the text: the unit asks storage for no more than `reads`
/// records before it hands its rows on, and no more once the pipeline stops
/// taking rows.
async fn dispatched(
	deref: &Arc<super::Deref>,
	value: Value,
	zeroed: bool,
	reads: Option<u32>,
	ctx: &ExecutionContext,
) -> FlowResult<Values> {
	let version = resolve_version_stamp(ctx, None).await?;
	match value {
		Value::RecordId(rid) => {
			require_defined(ctx, &rid.table, version).await?;
			if zeroed {
				return Ok(Values::Rows(Vec::new()));
			}
			crate::exec::permission::validate_record_user_access(ctx.database()?)?;
			let mut rows = Vec::new();
			match &rid.key {
				RecordIdKey::Range(range) => {
					let db_ctx = ctx.database()?;
					let range = record_key_range(
						db_ctx.ns_ctx.ns.namespace_id,
						db_ctx.db.database_id,
						&rid.table,
						range,
					)?;
					let txn = ctx.txn();
					let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
					let epoch = txn.layout_epoch();
					let layouts = txn
						.table_layouts(ns, db, &rid.table, version)
						.await
						.map_err(ControlFlow::Err)?;
					let replay = crate::exec::migrate::Replay::of(&layouts, ns, db);
					let gate =
						crate::kvs::RowGate::new(layouts, ns, db, &rid.table, version, epoch)
							.map_err(ControlFlow::Err)?;
					let mut source = kv_scan_stream(
						txn,
						range,
						gate,
						version,
						None,
						crate::kvs::Direction::Forward,
						0,
						None,
						None,
						None,
						replay.clone(),
					);
					while let Some(batch) = source.next().await {
						let mut batch = batch?;
						if let Some(replay) = &replay {
							replay.apply_batch(ctx, &mut batch).await?;
						}
						for raw in batch.into_values() {
							super::fuel::tick(ctx, 1).await?;
							match deref
								.read(ctx, &rid.table, raw, ctx.root().skip_fetch_perms)
								.await?
							{
								Value::None => {}
								row => rows.push(row),
							}
						}
					}
				}
				_ if let Some(view) = own_record(ctx, &rid, version).await? => rows.extend(view),
				_ => {
					if let Some(raw) = fetch_raw_record(ctx, &rid, version).await? {
						match deref.read(ctx, &rid.table, raw, ctx.root().skip_fetch_perms).await? {
							Value::None => {}
							row => rows.push(row),
						}
					}
				}
			}
			Ok(Values::Rows(rows))
		}
		Value::Array(array) => Ok(Values::Rows(records_of(ctx, array.0).await?)),
		Value::Table(table) => {
			if absent_and_hidden(ctx, &table, version).await? {
				return Ok(Values::Rows(Vec::new()));
			}
			require_defined(ctx, &table, version).await?;
			if zeroed {
				return Ok(Values::Rows(Vec::new()));
			}
			let unchecked = ctx.root().skip_fetch_perms;
			let Some(served) = deref.serving(ctx, &table, unchecked).await? else {
				return Err(internal(format!(
					"the table {table} read in a frame no unit of it was lowered for"
				)));
			};
			// A loaded unit holds the units it calls.
			let program = Arc::clone(served.program());
			Ok(Values::Stream(execute_program_reading(&program, ctx, reads)?))
		}
		Value::None | Value::Null => Ok(Values::Rows(Vec::new())),
		other => Ok(Values::Rows(vec![other])),
	}
}

/// Whether `table` is not defined as of `version` and the caller's reads are permission-checked,
/// so that a read of it by a value answers no row, as a scan of it by name does, rather than
/// disclosing the absence.
async fn absent_and_hidden(
	ctx: &ExecutionContext,
	table: &TableName,
	version: Option<u64>,
) -> FlowResult<bool> {
	let db_ctx = ctx.database()?;
	if db_ctx.get_table_def(table, version).await?.is_some() {
		return Ok(false);
	}
	crate::exec::permission::should_check_perms(db_ctx, crate::iam::Action::View)
		.map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))
}

/// Fails with the error of a table that is not defined, as of `version`.
async fn require_defined(
	ctx: &ExecutionContext,
	table: &TableName,
	version: Option<u64>,
) -> FlowResult<()> {
	if ctx.database()?.get_table_def(table, version).await?.is_none() {
		return Err(ControlFlow::Err(anyhow::Error::new(crate::catalog::Error::TbNotFound {
			name: table.clone(),
		})));
	}
	Ok(())
}

/// `values` as rows: `NONE` and `NULL` dropped, each record id replaced by
/// the record it names, read with the caller's permissions and computed
/// fields, and dropped where no record answers.
async fn records_of(ctx: &ExecutionContext, mut values: Vec<Value>) -> FlowResult<Vec<Value>> {
	values.retain(|v| !matches!(v, Value::None | Value::Null));
	if !values.is_empty() {
		crate::exec::operators::fetch::batch_fetch_in_place(ctx, &mut values).await?;
		values.retain(|v| !matches!(v, Value::None | Value::Null));
	}
	Ok(values)
}

/// The rows of `table` when no unit of the program or of the run reads it in
/// the frame `ctx` runs in ([`super::Deref::serving`]). This covers a table
/// no unit was lowered for, and a table read in a frame other than the one
/// its unit was lowered for. Records come in key order, as the pipeline
/// takes them. Each is read the way a fetch by id reads it
/// ([`crate::exec::operators::fetch::process_fetched_record`]): the table
/// permission, the computed fields and the field permissions apply where the
/// table defines them. Otherwise the record is read as storage holds it. A
/// table that the program's own units read in another frame is an internal
/// error, and so is a lightweight relation, which keeps no records to scan.
/// Storage is asked for no more than `reads` records first, and the pipeline takes no more.
async fn undispatched_table(
	ctx: &ExecutionContext,
	table: &TableName,
	version: Option<u64>,
	reads: Option<u32>,
) -> FlowResult<Values> {
	if ctx.vm_deref().is_some_and(|deref| deref.holds(table)) {
		return Err(internal(format!(
			"the table {table} read in a frame its unit was not lowered for"
		)));
	}
	let db_ctx = ctx.database()?;
	let descriptor = crate::exec::vm::descriptor::table_at(ctx, table, version)
		.await
		.map_err(ControlFlow::Err)?;
	let definition = descriptor.defined().map_err(ControlFlow::Err)?;
	let (ns, db) = (descriptor.ns, descriptor.db);
	let fields = &descriptor.fields;
	if matches!(
		&definition.table_type,
		crate::catalog::TableType::Relation(relation) if relation.lightweight
	) {
		return Err(internal(format!("the lightweight relation {table} read with no unit")));
	}
	crate::exec::permission::validate_record_user_access(db_ctx)?;
	let checked = crate::exec::permission::should_check_perms(db_ctx, crate::iam::Action::View)?;
	// A table with migration steps has its records brought through the steps
	// they have not passed before anything reads them.
	let replay = descriptor.replay();
	// A table with no field and no migration step, and a select permission a
	// checked caller is refused or granted whole, is read as storage holds it.
	let plain = fields.is_empty()
		&& replay.is_none()
		&& match &definition.permissions.select {
			_ if !checked => true,
			crate::catalog::Permission::None => return Ok(Values::Rows(Vec::new())),
			crate::catalog::Permission::Full => true,
			crate::catalog::Permission::Specific(_) => ctx.root().skip_fetch_perms,
		};
	let range = crate::key::schema::RecordPrefix {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(table),
	}
	.range()?;
	let records = kv_scan_stream(
		ctx.txn(),
		range,
		descriptor.row_gate().map_err(ControlFlow::Err)?,
		version,
		None,
		crate::kvs::Direction::Forward,
		0,
		reads,
		None,
		None,
		replay.clone(),
	);
	if plain {
		return Ok(Values::Stream(records));
	}
	let ctx = ctx.clone();
	Ok(Values::Stream(Box::pin(records.then(move |batch| {
		let ctx = ctx.clone();
		let replay = replay.clone();
		async move {
			let mut batch = batch?;
			if let Some(replay) = &replay {
				replay.apply_batch(&ctx, &mut batch).await?;
			}
			// The records the permission refuses spend the fuel here, as
			// their rows never run.
			let read = batch.len();
			let mut rows = Vec::new();
			for mut record in batch.into_values() {
				let rid = match &record {
					Value::Object(object) => match object.get("id") {
						Some(Value::RecordId(rid)) => rid.clone(),
						_ => continue,
					},
					_ => continue,
				};
				if crate::exec::operators::fetch::process_fetched_record(&ctx, &rid, &mut record)
					.await?
				{
					rows.push(record);
				}
			}
			super::fuel::tick(&ctx, super::fuel::units(read - rows.len())).await?;
			Ok(crate::exec::ValueBatch::from(rows))
		}
	}))))
}

fn internal(why: String) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(why)))
}

/// The value of the statement [`BlockInstr::Nested`] names, evaluated for
/// `row` — `None` for a pipeline with no row — in permission frame `frame`
/// when set; see the module.
pub(super) async fn statement(
	program: &Program,
	instr: &BlockInstr,
	row: Option<&Value>,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	let BlockInstr::Nested {
		nested,
		frame,
		..
	} = instr
	else {
		return Err(ControlFlow::Err(anyhow::anyhow!("a nested step at another instruction")));
	};
	let spec = &program.nested[*nested as usize];
	if spec.once
		&& let Some(Kept::Value(value)) = &state.once[*nested as usize]
	{
		return Ok(value.clone());
	}
	let mut rows = run_statement(program, *nested, *frame, row, state, ctx).await?;
	let value = if program.groups[spec.group as usize].scalar {
		rows.pop().unwrap_or(Value::None)
	} else {
		Value::Array(crate::val::Array(rows))
	};
	if spec.once {
		state.once[*nested as usize] = Some(Kept::Value(value.clone()));
		let kept = crate::exec::memory::size::kept_bytes(&value);
		state.keep(Site::Once(*nested as usize), kept, ctx).await?;
	}
	Ok(value)
}

/// Whether `probe` is a member of the rows of the statement
/// [`BlockInstr::Member`] names, not a member when it negates, evaluated for
/// `row` as [`statement`] evaluates it. The rows are kept as a membership
/// index ([`Members`]) from the first evaluation, which answers every later
/// one.
pub(super) async fn member(
	program: &Program,
	instr: &BlockInstr,
	row: Option<&Value>,
	probe: &Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<bool> {
	let BlockInstr::Member {
		nested,
		negate,
		frame,
		..
	} = instr
	else {
		return Err(ControlFlow::Err(anyhow::anyhow!("a membership step at another instruction")));
	};
	if let Some(Kept::Members(members)) = &state.once[*nested as usize] {
		return Ok(members.contains(probe) != *negate);
	}
	let rows = run_statement(program, *nested, *frame, row, state, ctx).await?;
	let members = Members::keep(rows, ctx).await?;
	let found = members.contains(probe);
	state.once[*nested as usize] = Some(Kept::Members(Box::new(members)));
	Ok(found != *negate)
}

/// The rows of nested statement `nested`'s group, evaluated for `row` in
/// permission frame `frame` when set; see the module.
async fn run_statement(
	program: &Program,
	nested: u32,
	frame: Option<u16>,
	row: Option<&Value>,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Vec<Value>> {
	let spec = &program.nested[nested as usize];
	let running = state
		.program
		.clone()
		.ok_or_else(|| ControlFlow::Err(anyhow::anyhow!("a nested statement with no program")))?;
	// The frame a subquery runs in: the row bound as `$parent` and `$this`,
	// and as the current row when the statement inherits it; unchecked reads,
	// and the record a computed frame computes, carried in.
	let frame = {
		let eval_ctx = super::host::frame_eval(program, state, ctx, frame);
		let exec_ctx = eval_ctx.exec_ctx;
		let mut nested_ctx =
			exec_ctx.entering_statement(exec_ctx.row_access(row), spec.inherits_row);
		if eval_ctx.skip_fetch_perms {
			nested_ctx = nested_ctx.with_skip_fetch_perms(true);
		}
		if let Some(record) = &eval_ctx.computing_record {
			nested_ctx = nested_ctx.with_computing_record(Some(Arc::clone(record)));
		}
		nested_ctx
	};
	let group = match &spec.hashed {
		Some((hashed, path))
			if state.lookups[nested as usize] >= super::payload::LOOKUP_EVALUATIONS
				|| !row
					.and_then(|row| key_at(Some(row), path))
					.is_some_and(super::host::lookup_key) =>
		{
			*hashed
		}
		Some(_) => {
			state.lookups[nested as usize] += 1;
			spec.group
		}
		None => spec.group,
	};
	let aside = Aside {
		frame: state.frame.replace(frame.clone()),
		write_input: state.write_input.take(),
		computing: state.computing.take(),
		unchecked_reads: std::mem::replace(&mut state.unchecked_reads, false),
		permissions_bypassed: std::mem::replace(
			&mut state.permissions_bypassed,
			frame.root().skip_fetch_perms,
		),
		zeroed: std::mem::replace(&mut state.zeroed, false),
	};
	let running_state = std::mem::replace(state, State::vacant());
	let (running_state, rows, ran) = crate::exec::trampoline::nest(async move {
		let mut running_state = running_state;
		let (rows, ran) = run_group(&running, group, &mut running_state, &frame).await;
		(running_state, rows, ran)
	})
	.await;
	*state = running_state;
	aside.restore(state);
	ran?;
	Ok(rows)
}

/// The row-level state of the pipeline evaluating a nested statement, set
/// aside while the statement runs.
struct Aside {
	frame: Option<ExecutionContext>,
	write_input: Option<Value>,
	computing: Option<Arc<crate::exec::eval_context::ComputingRecord>>,
	unchecked_reads: bool,
	permissions_bypassed: bool,
	zeroed: bool,
}

impl Aside {
	fn restore(self, state: &mut State) {
		state.frame = self.frame;
		state.write_input = self.write_input;
		state.computing = self.computing;
		state.unchecked_reads = self.unchecked_reads;
		state.permissions_bypassed = self.permissions_bypassed;
		state.zeroed = self.zeroed;
	}
}
