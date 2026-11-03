//! Sources that do not read a table the program names, and statements
//! nested in an expression. Both run over the program's own state.
//!
//! A value in `FROM` ([`Source::Values`]) is evaluated when its pipeline opens
//! and read as [`ValuesRead`](super::payload::ValuesRead) describes. A record id among the values
//! is read under the caller's permissions. A table that is the value is read as
//! the pipeline takes its rows: storage is asked for no more records than the
//! pipeline's paging admits, and no more once a limit of the pipeline is
//! spent. When the program was lowered with the units that read each table its
//! values can name ([`super::dispatch`]), the record ids and tables are read by
//! the unit of their table. A record no unit reads is read by the fetch helpers.
//! A table no unit reads was not in the catalog when the program was lowered. The run
//! created it by writing to it, so no unit was lowered for it. It is read
//! by key range, the way a scan reads any table ([`undispatched_table`]).
//! Nothing is planned or lowered while the program runs. A value a write's
//! subjects are read from is read as storage holds the records it names, for
//! the write to judge ([`stored_records`]).
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
use crate::exec::fan_out::evaluate_each;
use crate::exec::operators::fetch::fetch_raw_record;
use crate::exec::operators::link_window::LinkPool;
use crate::exec::operators::scan::common::resolve_version_stamp;
use crate::exec::operators::scan::pipeline::{ScalarRows, kv_scan_stream, read_by_id};
use crate::exec::operators::scan::resolved::EnforcementMode;
use crate::exec::operators::scan::{execute_record_lookup, record_key_range};
use crate::exec::{AccessMode, ExecutionContext, FlowResult, ValueBatchStream};
use crate::expr::ControlFlow;
use crate::val::{RecordId, RecordIdKey, TableName, Value};

// How many probes this thread has read the rows of, for tests that must
// know a probe read its rows once.
#[cfg(test)]
thread_local! {
	pub(super) static PROBES_BUILT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many times the process has run a statement in scalar position rather
/// than answering it from a kept value, by the database of the session the run
/// serves. A test reads the count of a database only it uses, whichever thread
/// a run took and whatever other tests run beside it.
#[cfg(test)]
pub(super) static STATEMENTS_RUN: std::sync::LazyLock<std::sync::Mutex<HashMap<String, usize>>> =
	std::sync::LazyLock::new(Default::default);

/// Counts a run of a statement in scalar position against the database of the
/// session `ctx` serves ([`STATEMENTS_RUN`]).
#[cfg(test)]
fn note_statement_run(ctx: &ExecutionContext) {
	let Some(db) = ctx
		.ctx()
		.value("session")
		.and_then(|session| session.as_object())
		.and_then(|session| session.get("db"))
		.map(Value::to_raw_string)
	else {
		return;
	};
	let mut runs = STATEMENTS_RUN.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
	*runs.entry(db).or_insert(0) += 1;
}

/// Run `pipeline` over the rows of `source`, a value or a statement in
/// `FROM`. `spent` is whether a limit of the pipeline admits no row. Under a
/// write in flight, the rows of a value are the write's subjects: the records
/// it names are read as storage holds them ([`stored_records`]). Answers
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
			// A statement run beneath a write runs with the write set aside
			// ([`run_group`]), so a write state here is the one this
			// pipeline's rows are written through.
			let subjects = state.write.is_some();
			// The records an array names are read in windows where the plan gave the pipeline
			// them ([`Pipeline::deref_windows`]).
			let links = pipeline.deref_windows.then_some(&state.link_pool);
			match values(program, value, *read, zeroed, reads, subjects, links, ctx).await? {
				// The records a value names are named by the statement; those a
				// range it evaluates holds are read from the table, a read the
				// write begins here.
				Values::Rows(rows, range) => {
					let rows = rows.into_iter().map(|row| (Vec::new(), row)).collect();
					let named = match state.write.as_deref_mut() {
						Some(write) => {
							if let Some(table) = &range {
								write.read_table(table)?;
							}
							Some(write.row_named(range.is_none()))
						}
						None => None,
					};
					let ran =
						run_rows(program, pipeline, rows, state, ctx, yielder, flush_at).await;
					if let (Some(named), Some(write)) = (named, state.write.as_deref_mut()) {
						write.row_named(named);
					}
					ran
				}
				// A write reading the records of a table a value names begins
				// that read as the stream opens.
				Values::Stream(rows, table) => {
					if let Some(write) = state.write.as_deref_mut() {
						write.read_table(&table)?;
					}
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
			// The statement's rows are values it answered, so a write names
			// each record among them rather than reading it from a table.
			let named = state.write.as_deref_mut().map(|write| write.row_named(true));
			let ran = run_rows(program, pipeline, rows, state, ctx, yielder, flush_at).await;
			if let (Some(named), Some(write)) = (named, state.write.as_deref_mut()) {
				write.row_named(named);
			}
			ran
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
		Source::Scan {
			..
		}
		| Source::Record {
			..
		}
		| Source::Index {
			..
		}
		| Source::Lookup {
			..
		}
		| Source::IndexUnion {
			..
		}
		| Source::Bitmap {
			..
		}
		| Source::FullText {
			..
		}
		| Source::Knn {
			..
		}
		| Source::Count {
			..
		}
		| Source::Union(_)
		| Source::Sorted {
			..
		}
		| Source::Groups {
			..
		}
		| Source::Split {
			..
		}
		| Source::Join {
			..
		}
		| Source::Once
		| Source::Changes {
			..
		}
		| Source::WriteTargets {
			..
		}
		| Source::WriteInserts {
			..
		}
		| Source::WriteEdges {
			..
		}
		| Source::LightweightEdges {
			..
		}
		| Source::Vacant {
			..
		} => Err(ControlFlow::Err(anyhow::anyhow!("a source that is not a value or a statement"))),
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
		match value.as_object() {
			Some(object) => value = object.get(field.as_str()).unwrap_or(&Value::None),
			None => return None,
		}
	}
	Some(value)
}

/// The rows a probe keeps, in the order they were read. Each is indexed by
/// the value of the join field, or `NONE` when a row has none.
///
/// [`Probe::candidates`] answers a superset of the rows whose field `=`
/// ([`Value::equal`]) the key, which the join predicate still judges. A map
/// is decisive only where hashing agrees with `=`
/// ([`Value::hash_agrees_with_eq`]), as it does for numbers of every kind. A
/// field whose value does not agree, a regex matching a string key among
/// them, is a candidate for every key; a key that does not agree matches
/// every row.
pub(crate) struct Probe {
	rows: Vec<Value>,
	hashed: HashMap<Value, Vec<usize>>,
	/// The rows whose field is a candidate for every key.
	loose: Vec<usize>,
}

impl std::fmt::Debug for Probe {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Probe").field("rows", &self.rows.len()).finish()
	}
}

impl Probe {
	fn new(rows: Vec<Value>, field: &str) -> Self {
		let mut probe = Self {
			rows: Vec::new(),
			hashed: HashMap::new(),
			loose: Vec::new(),
		};
		for (at, row) in rows.iter().enumerate() {
			let key = match row.as_object() {
				Some(object) => object.get(field).unwrap_or(&Value::None),
				None => &Value::None,
			};
			if key.hash_agrees_with_eq() {
				probe.hashed.entry(key.clone()).or_default().push(at)
			} else {
				probe.loose.push(at)
			}
		}
		probe.rows = rows;
		probe
	}

	/// The rows whose field may equal `key`, in the order they were read.
	///
	/// [`Probe::new`] files rows in the order they were read, so each list in
	/// `hashed` and `loose` is ascending and holds a row once: a key's own
	/// list is borrowed as it is, and only its union with `loose` is sorted
	/// into a buffer.
	fn candidates<'a>(&'a self, key: &Value) -> Candidates<'a> {
		if !key.hash_agrees_with_eq() {
			return Candidates::Every(0..self.rows.len());
		}
		let own = self.hashed.get(key).map_or(&[][..], Vec::as_slice);
		if self.loose.is_empty() {
			return Candidates::Listed(own.iter().copied());
		}
		let mut out: Vec<usize> = own.iter().chain(&self.loose).copied().collect();
		out.sort_unstable();
		out.dedup();
		Candidates::Merged(out.into_iter())
	}
}

/// The rows [`Probe::candidates`] answers: every row, a key's own list
/// borrowed from the probe, or that list merged with the loose rows.
enum Candidates<'a> {
	Every(std::ops::Range<usize>),
	Listed(std::iter::Copied<std::slice::Iter<'a, usize>>),
	Merged(std::vec::IntoIter<usize>),
}

impl Iterator for Candidates<'_> {
	type Item = usize;

	#[inline]
	fn next(&mut self) -> Option<usize> {
		match self {
			Self::Every(rows) => rows.next(),
			Self::Listed(rows) => rows.next(),
			Self::Merged(rows) => rows.next(),
		}
	}

	#[inline]
	fn size_hint(&self) -> (usize, Option<usize>) {
		match self {
			Self::Every(rows) => rows.size_hint(),
			Self::Listed(rows) => rows.size_hint(),
			Self::Merged(rows) => rows.size_hint(),
		}
	}
}

/// What a value in `FROM` yields: its rows, or, for a table it names, the
/// table's rows as they are read.
enum Values {
	/// Rows read whole before any runs: the records a range of table `1`
	/// holds, read from storage, or, where `1` is `None`, the records and
	/// values the value names.
	Rows(Vec<Value>, Option<TableName>),
	/// The rows of `1`, a table: those its unit reads, or every record of a
	/// table no unit reads ([`undispatched_table`]).
	Stream(ValueBatchStream, TableName),
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

/// The rows `value` yields, read as `read` says; see
/// [`ValuesRead`](super::payload::ValuesRead). Under `zeroed`, a record id or
/// table a dynamic read finds, and a record range or table any read finds,
/// reads no record; such a table is read `reads` records at a time first,
/// where the pipeline takes no more ([`paged_reads`]). Where `subjects`, the
/// value is evaluated as `read` says and the records it names are read as
/// storage holds them ([`stored_records`]), whatever the limit. The records
/// an array names are read through the windows of `links`, where given
/// ([`records_of`]). A `FROM` source is no context, so a `RETURN` evaluating
/// it signals passes out of the statement.
#[allow(clippy::too_many_arguments)]
async fn values(
	program: &Program,
	value: &HostScalar,
	read: ValuesRead,
	zeroed: bool,
	reads: Option<u32>,
	subjects: bool,
	links: Option<&LinkPool>,
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
			let value = host_scalar(program, value, ctx, this).await?;
			if subjects {
				return stored_records(value, written, ctx).await;
			}
			let rows = match value {
				Value::Array(array) => {
					let mut rows = Vec::with_capacity(array.len());
					for value in array {
						match value {
							Value::Array(inner) if written => rows.extend(inner),
							other @ (Value::None
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
							| Value::Table(_)
							| Value::RecordId(_)
							| Value::File(_)
							| Value::Regex(_)
							| Value::Range(_)
							| Value::Closure(_)) => rows.push(other),
						}
					}
					records_of(ctx, rows, links).await?
				}
				Value::None | Value::Null => Vec::new(),
				// A record range or a table is a set of records, read as a
				// dynamic read reads it.
				Value::RecordId(rid) if rid.key.is_range() => {
					return found(Value::RecordId(rid), zeroed, reads, links, ctx).await;
				}
				table @ Value::Table(_) => return found(table, zeroed, reads, links, ctx).await,
				Value::RecordId(rid) => {
					let record = crate::exec::operators::fetch::fetch_record(ctx, &rid).await?;
					if record.is_none() {
						Vec::new()
					} else {
						vec![record]
					}
				}
				other @ (Value::Bool(_)
				| Value::Number(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)
				| Value::Set(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::File(_)
				| Value::Regex(_)
				| Value::Range(_)
				| Value::Closure(_)) => vec![other],
			};
			Ok(Values::Rows(rows, None))
		}
		ValuesRead::Locked => {
			let value = host_scalar(program, value, ctx, None).await?;
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
			Ok(Values::Rows(rows, None))
		}
		ValuesRead::Dynamic => {
			let value = host_scalar(program, value, ctx, None).await?;
			if subjects {
				return stored_records(value, false, ctx).await;
			}
			found(value, zeroed, reads, links, ctx).await
		}
	}
}

/// The records `value` names as storage holds them, read as a write's subjects,
/// which the write's gate judges as it writes each one: no permission or
/// computed field is applied to them here. A record id, and an object carrying
/// one as its `id`, is the record stored under that id, and no row where none
/// is: the object is never taken as the record's image. A record range or a
/// table is every record stored in it, in key order, read as the write takes
/// them. Its table is required to be defined, and an undefined one answers as
/// the same target written out answers it: no row to a caller whose reads are
/// permission-checked ([`absent_and_hidden`]), and the error of an undefined
/// table to any other. An array's
/// elements are the rows, an element that is an array contributing its
/// elements, one level deep, where `written`; each record id or object among
/// them is read as one is, and a range among them names no row, as a read of
/// the array reads none. `NONE` and `NULL` are no row, and any other value is
/// one row.
async fn stored_records(value: Value, written: bool, ctx: &ExecutionContext) -> FlowResult<Values> {
	let version = ctx.version_stamp();
	let subjects = match value {
		Value::RecordId(RecordId {
			table,
			key: RecordIdKey::Range(range),
		}) => {
			if absent_and_hidden(ctx, &table, version).await? {
				return Ok(Values::Rows(Vec::new(), None));
			}
			require_defined(ctx, &table, version).await?;
			let scan =
				crate::exec::operators::write::scan_subjects(ctx, &table, Some(&range)).await?;
			let rows = replayed(scan, &table, version, ctx).await?;
			return Ok(Values::Stream(rows, table));
		}
		Value::Table(table) => {
			if absent_and_hidden(ctx, &table, version).await? {
				return Ok(Values::Rows(Vec::new(), None));
			}
			require_defined(ctx, &table, version).await?;
			let scan = crate::exec::operators::write::scan_subjects(ctx, &table, None).await?;
			let rows = replayed(scan, &table, version, ctx).await?;
			return Ok(Values::Stream(rows, table));
		}
		Value::Array(array) => {
			let mut subjects = Vec::with_capacity(array.len());
			for value in array {
				match value {
					Value::Array(inner) if written => {
						subjects.extend(inner.into_iter().filter_map(stored_subject))
					}
					other @ (Value::None
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
					| Value::Table(_)
					| Value::RecordId(_)
					| Value::File(_)
					| Value::Regex(_)
					| Value::Range(_)
					| Value::Closure(_)) => subjects.extend(stored_subject(other)),
				}
			}
			subjects
		}
		other @ (Value::None
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
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => stored_subject(other).into_iter().collect(),
	};
	Ok(Values::Rows(stored_rows(ctx, subjects, version).await?, None))
}

/// `scan`, the stored records of `table` as
/// [`scan_subjects`](crate::exec::operators::write::scan_subjects) reads them
/// at `version`, each brought through the migration steps of `table` it has
/// not passed, as a scan of the table hands its records on. Its batches carry
/// each record's stamp, which the replay takes.
async fn replayed(
	scan: ValueBatchStream,
	table: &TableName,
	version: Option<u64>,
	ctx: &ExecutionContext,
) -> FlowResult<ValueBatchStream> {
	let Some(replay) = crate::exec::migrate::replay_for(ctx, table, version).await? else {
		return Ok(scan);
	};
	let ctx = ctx.clone();
	Ok(Box::pin(scan.then(move |batch| {
		let (ctx, replay) = (ctx.clone(), replay.clone());
		async move {
			let mut batch = batch?;
			replay.apply_batch(&ctx, &mut batch).await?;
			Ok(batch)
		}
	})))
}

/// `value` as one subject of [`stored_records`]: a record id, or the id an
/// object carries as its `id`, names the record stored under it, so the
/// object is never taken as the record's image; a range among an array's
/// elements, `NONE` and `NULL` are none; any other value is itself.
fn stored_subject(value: Value) -> Option<Value> {
	match value {
		Value::None | Value::Null => None,
		Value::RecordId(rid) if rid.key.is_range() => None,
		Value::Object(object) => match object.rid() {
			Some(rid) if rid.key.is_range() => None,
			Some(rid) => Some(Value::RecordId(rid)),
			None => Some(Value::Object(object)),
		},
		other @ (Value::Bool(_)
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
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => Some(other),
	}
}

/// `subjects`, each record id among them replaced by the record stored under
/// it at `version` and dropped where none is. The reads touch storage only,
/// so they are overlapped as [`evaluate_each`] admits a read-only batch.
async fn stored_rows(
	ctx: &ExecutionContext,
	mut subjects: Vec<Value>,
	version: Option<u64>,
) -> FlowResult<Vec<Value>> {
	let (at, rids): (Vec<usize>, Vec<RecordId>) = subjects
		.iter()
		.enumerate()
		.filter_map(|(at, subject)| subject.as_record().map(|rid| (at, rid.clone())))
		.unzip();
	if rids.is_empty() {
		return Ok(subjects);
	}
	let stored = evaluate_each(ctx, AccessMode::ReadOnly, &rids, |rid| async move {
		Ok(fetch_raw_record(ctx, rid, version).await?.unwrap_or(Value::None))
	})
	.await?;
	for (at, record) in at.into_iter().zip(stored) {
		// A subject of a write whose content is not an object stays its record id,
		// so the write reads that record by id, as it reads any record it names.
		subjects[at] = match record {
			record if read_by_id(&record) => subjects[at].clone(),
			record => record,
		};
	}
	subjects.retain(|subject| !matches!(subject, Value::None));
	Ok(subjects)
}

/// The rows a read of `value`, a value found when the statement runs, yields:
/// a record id or range read under the caller's table and field permissions,
/// checking only its table and permission under `zeroed`, a range read in key
/// order as the pipeline takes its rows, as a table is; a table read whole
/// under them, in key order, `reads` records at a time first where the
/// pipeline takes no more, and not at all under `zeroed` once its existence is
/// checked; an array's elements as the rows, its records read with the
/// caller's permissions and computed fields; `NONE` and `NULL` as no row; and
/// any other value as one row.
async fn found(
	value: Value,
	zeroed: bool,
	reads: Option<u32>,
	links: Option<&LinkPool>,
	ctx: &ExecutionContext,
) -> FlowResult<Values> {
	if let Some(deref) = ctx.vm_deref().cloned()
		&& reads_through(&deref, &value, ctx).await?
	{
		return dispatched(&deref, value, zeroed, reads, links, ctx).await;
	}
	match value {
		Value::RecordId(rid)
			if !zeroed
				&& let Some(view) =
					own_record(ctx, &rid, resolve_version_stamp(ctx, None).await?).await? =>
		{
			Ok(Values::Rows(view.into_iter().collect(), None))
		}
		Value::RecordId(rid) => {
			let db_ctx = ctx.database()?;
			let version = resolve_version_stamp(ctx, None).await?;
			require_defined(ctx, &rid.table, version).await?;
			// A range no unit of the program holds is read as the pipeline
			// takes its rows, as such a table is.
			if let RecordIdKey::Range(range) = &rid.key
				&& !zeroed && !ctx.vm_deref().is_some_and(|deref| deref.holds(&rid.table))
			{
				return undispatched_table(ctx, &rid.table, Some(range), version, reads).await;
			}
			let check_perms =
				crate::exec::permission::should_check_perms(db_ctx, crate::iam::Action::View)
					.map_err(|e| ControlFlow::Err(e.into()))?;
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
			let range = rid.key.is_range().then(|| rid.table.clone());
			Ok(Values::Rows(rows, range))
		}
		Value::Array(array) => Ok(Values::Rows(records_of(ctx, array.0, links).await?, None)),
		Value::Table(table) => {
			let version = resolve_version_stamp(ctx, None).await?;
			if absent_and_hidden(ctx, &table, version).await? {
				return Ok(Values::Rows(Vec::new(), None));
			}
			require_defined(ctx, &table, version).await?;
			// A zero limit reads no record once the table is known.
			if zeroed {
				return Ok(Values::Rows(Vec::new(), None));
			}
			undispatched_table(ctx, &table, None, version, reads).await
		}
		Value::None | Value::Null => Ok(Values::Rows(Vec::new(), None)),
		other @ (Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => Ok(Values::Rows(vec![other], None)),
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
		| Value::Closure(_) => return Ok(true),
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
/// taking rows. A range is read in key order the same way, each record handed
/// to its table's unit as storage returns it.
async fn dispatched(
	deref: &Arc<super::Deref>,
	value: Value,
	zeroed: bool,
	reads: Option<u32>,
	links: Option<&LinkPool>,
	ctx: &ExecutionContext,
) -> FlowResult<Values> {
	let version = resolve_version_stamp(ctx, None).await?;
	match value {
		Value::RecordId(rid) => {
			require_defined(ctx, &rid.table, version).await?;
			if zeroed {
				return Ok(Values::Rows(Vec::new(), None));
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
					// The range is read as the pipeline takes its rows, as a
					// table is: no more records asked of storage first than
					// its paging admits.
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
					let source = kv_scan_stream(
						txn,
						range,
						gate,
						version,
						None,
						crate::kvs::Direction::Forward,
						0,
						reads,
						None,
						None,
						replay.clone(),
						ScalarRows::Content,
					);
					let (deref, ctx, table) = (Arc::clone(deref), ctx.clone(), rid.table.clone());
					let unchecked = ctx.root().skip_fetch_perms;
					return Ok(Values::Stream(
						Box::pin(source.then(move |batch| {
							let (deref, ctx, table, replay) =
								(Arc::clone(&deref), ctx.clone(), table.clone(), replay.clone());
							async move {
								let mut batch = batch?;
								if let Some(replay) = &replay {
									replay.apply_batch(&ctx, &mut batch).await?;
								}
								let mut rows = Vec::new();
								for raw in batch.into_values() {
									super::fuel::tick(&ctx, 1).await?;
									let row = deref.read(&ctx, &table, raw, unchecked).await?;
									if !row.is_none() {
										rows.push(row);
									}
								}
								Ok(crate::exec::ValueBatch::from(rows))
							}
						})),
						rid.table.clone(),
					));
				}
				RecordIdKey::Number(_)
				| RecordIdKey::String(_)
				| RecordIdKey::Uuid(_)
				| RecordIdKey::Array(_)
				| RecordIdKey::Object(_)
					if let Some(view) = own_record(ctx, &rid, version).await? =>
				{
					rows.extend(view)
				}
				RecordIdKey::Number(_)
				| RecordIdKey::String(_)
				| RecordIdKey::Uuid(_)
				| RecordIdKey::Array(_)
				| RecordIdKey::Object(_) => {
					if let Some(raw) = fetch_raw_record(ctx, &rid, version).await? {
						let row =
							deref.read(ctx, &rid.table, raw, ctx.root().skip_fetch_perms).await?;
						if !row.is_none() {
							rows.push(row);
						}
					}
				}
			}
			let range = rid.key.is_range().then(|| rid.table.clone());
			Ok(Values::Rows(rows, range))
		}
		Value::Array(array) => Ok(Values::Rows(records_of(ctx, array.0, links).await?, None)),
		Value::Table(table) => {
			if absent_and_hidden(ctx, &table, version).await? {
				return Ok(Values::Rows(Vec::new(), None));
			}
			require_defined(ctx, &table, version).await?;
			if zeroed {
				return Ok(Values::Rows(Vec::new(), None));
			}
			let unchecked = ctx.root().skip_fetch_perms;
			let Some(served) = deref.serving(ctx, &table, unchecked).await? else {
				return Err(internal(format!(
					"the table {table} read in a frame no unit of it was lowered for"
				)));
			};
			// A loaded unit holds the units it calls.
			let program = Arc::clone(served.program());
			Ok(Values::Stream(execute_program_reading(&program, ctx, reads)?, table))
		}
		Value::None | Value::Null => Ok(Values::Rows(Vec::new(), None)),
		other @ (Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => Ok(Values::Rows(vec![other], None)),
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
	// An absence the execution's catalog cache holds is read again before it
	// is believed: a block that defined the table by writing to it after an
	// earlier read cached its absence leaves the cache behind the catalog.
	if ctx.database()?.table_is_undefined(table, version).await? {
		return Err(ControlFlow::Err(anyhow::Error::new(crate::catalog::Error::TbNotFound {
			name: table.clone(),
		})));
	}
	Ok(())
}

/// `values` as rows: `NONE` and `NULL` dropped, each record id replaced by
/// the record it names, read with the caller's permissions and computed
/// fields, and dropped where no record answers. Every record is read, in
/// order, before any row is handed on, so the first id whose read fails fails
/// the source before a row runs. With `links`, the records are read through
/// its windows, one multi-get for each run of ids.
async fn records_of(
	ctx: &ExecutionContext,
	mut values: Vec<Value>,
	links: Option<&LinkPool>,
) -> FlowResult<Vec<Value>> {
	values.retain(|v| !matches!(v, Value::None | Value::Null));
	if !values.is_empty() {
		crate::exec::operators::fetch::batch_fetch_in_place(
			ctx,
			&mut values,
			crate::exec::operators::fetch::FetchLinks {
				pool: links,
				ahead: None,
			},
		)
		.await?;
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
/// Where `range` is given, only the records of the table in that key range are read.
async fn undispatched_table(
	ctx: &ExecutionContext,
	table: &TableName,
	range: Option<&crate::val::RecordIdKeyRange>,
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
			crate::catalog::Permission::None => return Ok(Values::Rows(Vec::new(), None)),
			crate::catalog::Permission::Full => true,
			crate::catalog::Permission::Specific(_) => ctx.root().skip_fetch_perms,
		};
	let range = match range {
		Some(range) => record_key_range(ns, db, table, range)?,
		None => crate::key::schema::RecordPrefix {
			ns,
			db,
			tb: std::borrow::Cow::Borrowed(table),
		}
		.range()?,
	};
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
		ScalarRows::Content,
	);
	if plain {
		return Ok(Values::Stream(records, table.clone()));
	}
	let ctx = ctx.clone();
	Ok(Values::Stream(
		Box::pin(records.then(move |batch| {
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
					let rid = match record.as_object() {
						Some(object) => match object.get("id").and_then(Value::as_record) {
							Some(rid) => rid.clone(),
							None => continue,
						},
						None => continue,
					};
					if crate::exec::operators::fetch::process_fetched_record(
						&ctx,
						&rid,
						&mut record,
					)
					.await?
					{
						rows.push(record);
					}
				}
				super::fuel::tick(&ctx, super::fuel::units(read - rows.len())).await?;
				Ok(crate::exec::ValueBatch::from(rows))
			}
		})),
		table.clone(),
	))
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
	#[cfg(test)]
	note_statement_run(ctx);
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

#[cfg(test)]
mod probe_tests {
	use std::collections::BTreeMap;

	use surrealdb_strand::Strand;

	use super::Probe;
	use crate::val::{Number, Object, Regex, Value};

	fn row(k: Value) -> Value {
		Value::Object(Object::from(BTreeMap::from([(Strand::from("k"), k)])))
	}

	fn dec(s: &str) -> Value {
		Value::Number(Number::Decimal(s.parse().unwrap()))
	}

	fn candidates(probe: &Probe, key: &Value) -> Vec<usize> {
		probe.candidates(key).collect()
	}

	/// Every kind of one number finds the rows holding any kind of it, and a
	/// float and a decimal that are not exactly equal find only their own.
	#[test]
	fn a_number_of_any_kind_finds_every_row_holding_it() {
		let probe = Probe::new(
			vec![
				row(Value::Number(Number::Int(42))),
				row(Value::Number(Number::Float(42.0))),
				row(dec("42.00")),
				row(Value::Number(Number::Float(0.1))),
				row(dec("0.1")),
				row(Value::from("42")),
			],
			"k",
		);
		for key in [Value::Number(Number::Int(42)), Value::Number(Number::Float(42.0)), dec("42")] {
			assert_eq!(candidates(&probe, &key), vec![0, 1, 2], "{key:?}");
		}
		assert_eq!(candidates(&probe, &Value::Number(Number::Float(0.1))), vec![3]);
		assert_eq!(candidates(&probe, &dec("0.10")), vec![4]);
		assert_eq!(candidates(&probe, &Value::Number(Number::Int(7))), Vec::<usize>::new());
	}

	/// A row whose field hashes apart from what it equals, a regex here, is a
	/// candidate for every key, merged with the hashed rows in read order.
	#[test]
	fn a_loose_row_is_a_candidate_for_every_key() {
		let regex = Value::Regex(Regex(crate::val::regex::regex_new("4").expect("a regex")));
		let probe = Probe::new(
			vec![
				row(Value::Number(Number::Float(42.0))),
				row(regex),
				row(Value::Number(Number::Int(42))),
				row(Value::from("x")),
			],
			"k",
		);
		assert_eq!(candidates(&probe, &Value::Number(Number::Int(42))), vec![0, 1, 2]);
		assert_eq!(candidates(&probe, &Value::from("x")), vec![1, 3]);
	}

	/// A key whose hash disagrees with its equality, a regex here, is a
	/// candidate key for every row.
	#[test]
	fn a_loose_key_finds_every_row() {
		let regex = Value::Regex(Regex(crate::val::regex::regex_new("4").expect("a regex")));
		let probe = Probe::new(
			vec![
				row(Value::Number(Number::Float(42.0))),
				row(Value::from("x")),
				row(Value::Number(Number::Int(7))),
			],
			"k",
		);
		assert_eq!(candidates(&probe, &regex), vec![0, 1, 2]);
	}
}
