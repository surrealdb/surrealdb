//! The host a program runs under.
//!
//! [`VmOperator`] is where a program meets the async engine. It evaluates the
//! statement's paging clauses, then runs the program's pipelines in order:
//! one that runs in place inside the storage cursor, on each record's stored
//! bytes; any other over the rows its source holds in memory — records the scan
//! decoded, rows a sort ordered, groups an aggregate finished — evaluating the
//! expressions its body hands back. To the operators and executor around it,
//! it is an operator like any other.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use common::future::stream::Yielder;
use futures::StreamExt;
use surrealdb_strand::Strand;

use super::batch::RecordReader;
use super::groups::GroupMut;
use super::interp::{self, Arrivals, Groups, Paging, Pending, Region, State, Step};
use super::ir_host::IrResume;
use super::morsel::Rows;
use super::program::{Enforce, Instr, Pipeline, Program, STACK_REGISTERS, Source, StoreInstr};
use super::row::{RowReader, StoredRow, ValueRow};
use super::row_packed::PackedRowReader;
use super::scalar::{Held, Scalar};
use super::services::{EngineServices, Services};
use super::write;
use crate::catalog::{StoredRecord, TableLayouts};
use crate::exec::function::AccumulatorState;
use crate::exec::index::access_path::access_repeats_records;
use crate::exec::memory::runs::{BreakerKind, RunOrder, RunSpec};
use crate::exec::migrate::{Replay, stamp_of};
use crate::exec::operators::SortDirection;
use crate::exec::operators::fetch::fetch_raw_record;
use crate::exec::operators::limit::{PagingClause, paging_count};
use crate::exec::operators::scan::common::{
	DecodeFields, ReadAheadRecord, ensure_below_memory_threshold, resolve_version_stamp,
};
use crate::exec::operators::scan::pipeline::{
	ScalarRows, decode_record_value, kv_scan_stream, read_by_id,
};
use crate::exec::operators::scan::{
	FULL_TEXT_BATCH_SIZE, FusedCandidates, MergeMode, RESOLVE_BATCH_SIZE, record_key_range,
};
use crate::exec::packed_view::AdmittedRow;
use crate::exec::permission::validate_record_user_access;
use crate::exec::planner::placement::MorselInput;
use crate::exec::pre_decode_filter::{PreDecodeFilter, PreDecodeFilterOutcome};
use crate::exec::vm::degree::DegreeCounter;
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::key::schema::RecordPrefix;
use crate::kvs::{Admission, RowGate};
use crate::val::{RecordId, RecordIdKey, Value};

/// A statement run by the bytecode VM.
#[derive(Debug)]
pub(crate) struct VmOperator {
	program: Arc<Program>,
	metrics: Arc<OperatorMetrics>,
	/// The catalog ids the statement's planning held, which the program's
	/// draws take: kept until the operator and every run of it are dropped.
	held: Option<Arc<super::speculate::HeldIds>>,
	/// The program's late-bound statements, as written, which its blocks
	/// serve when they reach them.
	served: Option<super::ServedStatements>,
}

impl VmOperator {
	/// The operator's name, which no other operator takes.
	pub(super) const NAME: &'static str = "Bytecode";

	pub(super) fn new(program: Program) -> Self {
		Self {
			program: Arc::new(program),
			metrics: Arc::new(OperatorMetrics::new()),
			held: None,
			served: None,
		}
	}

	/// This operator serving `served`, its program's late-bound statements.
	pub(super) fn serving(mut self, served: Vec<Arc<crate::expr::logical::LogicalOp>>) -> Self {
		self.served = (!served.is_empty()).then(|| served.into());
		self
	}

	/// This operator keeping `held` for as long as it or a run of it lasts.
	pub(super) fn holding(mut self, held: Option<Arc<super::speculate::HeldIds>>) -> Self {
		self.held = held;
		self
	}
}

/// A run's stream keeping the catalog ids its draws take until it is dropped.
struct Holding {
	stream: ValueBatchStream,
	_held: Arc<super::speculate::HeldIds>,
}

impl futures::Stream for Holding {
	type Item = FlowResult<ValueBatch>;

	fn poll_next(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Self::Item>> {
		self.stream.as_mut().poll_next(cx)
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.stream.size_hint()
	}
}

impl ExecOperator for VmOperator {
	fn required_context(&self) -> ContextLevel {
		self.program.required_context
	}

	fn access_mode(&self) -> AccessMode {
		self.program.access_mode
	}

	/// A write's `ONLY` makes its value the one row it returned, which the
	/// write body judges itself.
	fn output_shape(&self) -> crate::exec::OutputShape {
		output_shape(&self.program)
	}

	fn execute_encoded(
		&self,
		ctx: &ExecutionContext,
		format: surrealdb_rpc::RowFormat,
	) -> FlowResult<ValueBatchStream> {
		self.run(ctx, Some(format))
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		self.run(ctx, None)
	}
}

impl VmOperator {
	/// Run the program under the statements its blocks serve, its own rows
	/// encoded in `format` when given.
	fn run(
		&self,
		ctx: &ExecutionContext,
		format: Option<surrealdb_rpc::RowFormat>,
	) -> FlowResult<ValueBatchStream> {
		let serving;
		let ctx = match &self.served {
			Some(served) => {
				serving = ctx.with_vm_served(Arc::clone(served));
				&serving
			}
			None => ctx,
		};
		let stream = execute_program_in(&self.program, ctx, None, format)?;
		let stream: ValueBatchStream = match &self.held {
			Some(held) => Box::pin(Holding {
				stream,
				_held: Arc::clone(held),
			}),
			None => stream,
		};
		Ok(monitor_stream(stream, "Bytecode", &self.metrics))
	}
}

/// How many rows `program` yields before its tail: at most one for a write
/// that is `ONLY` and for a statement whose value is its one row.
pub(super) fn cardinality_hint(program: &Program) -> crate::exec::CardinalityHint {
	match &program.write {
		Some(spec) if spec.only => crate::exec::CardinalityHint::AtMostOne,
		_ if program.scalar => crate::exec::CardinalityHint::AtMostOne,
		_ => crate::exec::CardinalityHint::Unbounded,
	}
}

/// What `program`'s value is made of: its one row, or its rows. `ONLY` in the
/// tail makes it the one row. A write's `ONLY` makes its value the one row it
/// returned, which the write body judges itself, with or without a `TIMEOUT`.
/// Any other statement with a `TIMEOUT` hands on rows.
pub(super) fn output_shape(program: &Program) -> crate::exec::OutputShape {
	match (&program.tail, &program.write) {
		(Some(tail), _) if tail.only => crate::exec::OutputShape::Scalar,
		_ if program.rows => crate::exec::OutputShape::Rows,
		(_, Some(spec)) if spec.only => crate::exec::OutputShape::Scalar,
		(Some(tail), _) if tail.timeout.is_some() => crate::exec::OutputShape::Rows,
		_ if program.scalar => crate::exec::OutputShape::Scalar,
		_ => crate::exec::OutputShape::Rows,
	}
}

/// Run `program` in `ctx`: the stream of its rows, under the tail it holds,
/// on a frame stack of its own for the statements it nests.
pub(super) fn execute_program(
	program: &Arc<Program>,
	ctx: &ExecutionContext,
) -> FlowResult<ValueBatchStream> {
	execute_program_reading(program, ctx, None)
}

/// [`execute_program`] for a reader that takes no more than `reads` rows
/// before its paging is spent, where it says: a program with no tail hands
/// on its rows that many at a time, and its first read of storage takes no
/// more records than that from its cursor first ([`State::scan_hint`]).
pub(super) fn execute_program_reading(
	program: &Arc<Program>,
	ctx: &ExecutionContext,
	reads: Option<u32>,
) -> FlowResult<ValueBatchStream> {
	execute_program_in(program, ctx, reads, None)
}

/// [`execute_program_reading`], with the statement's own rows encoded in
/// `format` when given (see [`ExecOperator::execute_encoded`]).
pub(super) fn execute_program_in(
	program: &Arc<Program>,
	ctx: &ExecutionContext,
	reads: Option<u32>,
	format: Option<surrealdb_rpc::RowFormat>,
) -> FlowResult<ValueBatchStream> {
	// The run's table is set up first: a dispatch shares the reads it links.
	let unitted = super::closure::with_units(program, ctx);
	let ctx = unitted.as_ref().unwrap_or(ctx);
	let dispatched = super::dispatch::with_dispatch(program, ctx);
	let ctx = dispatched.as_ref().unwrap_or(ctx);
	match &program.tail {
		Some(_) => Ok(execute_tailed(program, ctx, format)),
		None => execute_untailed(program, ctx, reads, format),
	}
}

/// `program` under its tail: the `VERSION` stamp its reads are made at, the
/// `TIMEOUT` that bounds it, and `ONLY` over its rows.
///
/// The `VERSION` clause is evaluated first, then the `TIMEOUT` clause in the
/// frame the stamp makes, and the statement starts only once both are read. A
/// statement still running at its deadline fails with the timeout error; one
/// that finishes in time is not failed by the clock. `ONLY` hands on the one
/// row, `NONE` for none, and fails over more than one.
fn execute_tailed(
	program: &Arc<Program>,
	ctx: &ExecutionContext,
	format: Option<surrealdb_rpc::RowFormat>,
) -> ValueBatchStream {
	let program = Arc::clone(program);
	let ctx = ctx.clone();
	let stream = common::future::stream::try_async_stream(async move |mut yielder: Yielder<_>| {
		let Some(tail) = &program.tail else {
			return Err(ControlFlow::Err(anyhow::anyhow!("a tail run for a program with none")));
		};
		let ctx = match &tail.version {
			Some(version) => {
				let value = host_scalar(&program, version, &ctx, None).await?;
				let stamp = value
					.cast_to::<crate::val::Datetime>()
					.map_err(|e| anyhow::anyhow!("{e}"))?
					.to_version_stamp(ctx.txn().version_impl().as_ref())?;
				ctx.with_version_stamp(Some(stamp))
			}
			None => ctx,
		};
		// The clause is cast, not coerced, so a duration written down as text
		// names one; `NONE` names no bound at all.
		let duration = match &tail.timeout {
			Some(timeout) => host_scalar(&program, timeout, &ctx, None)
				.await?
				.cast_to::<Option<crate::val::Duration>>()
				.map_err(anyhow::Error::new)?,
			None => None,
		};
		// A duration the clock cannot reach names no deadline: it is the
		// invalid clause it is, not a very distant moment.
		let deadline = match duration {
			Some(duration) => {
				let Some(instant) = common::time::Instant::now().checked_add(duration.0) else {
					return Err(ControlFlow::Err(anyhow::Error::new(
						crate::kvs::DatastoreError::InvalidTimeout(duration.0.as_secs()),
					)));
				};
				Some((duration, instant))
			}
			None => None,
		};
		// `ONLY` alone takes at most two rows, the second to refuse it, so
		// reading ahead on a task of its own gains nothing.
		let cardinality = if tail.only && tail.timeout.is_none() && tail.version.is_none() {
			crate::exec::CardinalityHint::AtMostOne
		} else {
			cardinality_hint(&program)
		};
		let input = crate::exec::buffer_stream(
			// `ONLY` reads its one row back, so it is never encoded.
			execute_untailed(&program, &ctx, None, format.filter(|_| !tail.only))?,
			program.access_mode,
			cardinality,
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		// A boundary: what the statement nests runs within this stream's
		// poll, under the timer, rather than on a driver above it.
		let mut input: ValueBatchStream = if tail.timeout.is_some() {
			Box::pin(crate::exec::trampoline::boundary(input))
		} else {
			input
		};
		let mut one: Vec<Value> = Vec::new();
		loop {
			let next = match &deadline {
				Some((duration, instant)) => {
					let remaining = instant.saturating_duration_since(common::time::Instant::now());
					let timed_out = || {
						ControlFlow::Err(anyhow::anyhow!(crate::err::EngineError::QueryTimedout(
							duration.0
						)))
					};
					if remaining.is_zero() {
						return Err(timed_out());
					}
					match common::time::timeout(remaining, input.next()).await {
						Ok(next) => next,
						Err(_) => {
							stopped_at_deadline(&program, &ctx);
							return Err(timed_out());
						}
					}
				}
				None => input.next().await,
			};
			let Some(batch) = next else {
				break;
			};
			let batch = batch?;
			if tail.only {
				one.extend(batch.into_values());
				if one.len() > 1 {
					return Err(ControlFlow::Err(anyhow::anyhow!(
						crate::exec::Error::SingleOnlyOutput
					)));
				}
			} else {
				yielder.emit(batch).await;
			}
		}
		if tail.only {
			yielder.emit(ValueBatch::new(vec![one.pop().unwrap_or(Value::None)])).await;
		}
		Ok(())
	});
	crate::exec::compiles::sealed_stream(Box::pin(stream))
}

/// Run `program` in `ctx`, its tail not applied: the stream of its rows, on a
/// frame stack of its own for the statements it nests.
fn execute_untailed(
	program: &Arc<Program>,
	ctx: &ExecutionContext,
	reads: Option<u32>,
	format: Option<surrealdb_rpc::RowFormat>,
) -> FlowResult<ValueBatchStream> {
	{
		// The bound plan carries the statement's table and field permissions
		// as nodes, and the VM runs only plans that have none to apply.
		// Confinement to the principal's own namespace and database is not
		// among them, so it is checked here, as every scan checks it. A write
		// checks it per record, as the head of every order.
		if let Some(spec) = &program.write {
			refuse_unwritable(spec, ctx)?;
			let stream = write::stream(Arc::clone(program), ctx.clone());
			return Ok(crate::exec::compiles::while_running(Box::pin(
				crate::exec::trampoline::Trampolined::root(stream),
			)));
		}
		// An expression statement reads nothing a scan would, and may run
		// outside any database.
		if !program.scalar {
			validate_record_user_access(ctx.database()?)?;
		}
		let program = Arc::clone(program);
		let ctx = ctx.clone();
		let stream =
			common::future::stream::try_async_stream(async move |mut stream: Yielder<_>| {
				let mut yielder = Out::Stream(&mut stream);
				// Rows leave encoded only for a program whose output is its
				// own rows alone: one that runs block statements over its
				// state gathers theirs there too.
				let format = format.filter(|_| program.groups.is_empty());
				let mut state =
					out_of_line(|| run_untailed(&program, &ctx, &mut yielder, reads, format))
						.await?;
				if let Some(rows) = state.framed_output().map_err(ControlFlow::Err)? {
					if !rows.is_empty() {
						stream.emit(ValueBatch::encoded(rows)).await;
					}
					return Ok(());
				}
				let rest = state.take_output();
				if !rest.is_empty() {
					stream.emit(ValueBatch::new(rest)).await;
				}
				Ok(())
			});
		// The statements it nests — subqueries, a block's statements, the
		// bodies they call — run on this stream's frame stack.
		let stream = crate::exec::trampoline::Trampolined::root(Box::pin(stream));
		Ok(crate::exec::compiles::while_running(Box::pin(stream)))
	}
}

/// Hands `state`'s output frame the seed shape `program` was compiled with, when its rows are
/// written shaped. Out of line, so the frame of a run that nests runs stays its size.
#[cfg(feature = "shaped")]
#[inline(never)]
fn seed_frame(program: &Program, state: &mut State) {
	if let (Some(table), Some(seed), Some(frame)) =
		(&program.table, &program.shaped_seed, &mut state.row_frame)
	{
		frame.seed(table.as_str(), std::sync::Arc::clone(seed));
	}
}

/// Run the pipelines of `program`, a statement that writes nothing, in `ctx`,
/// handing their rows to `yielder`: its paging and its `VERSION` stamp
/// evaluated first, over a state of its own, which is answered holding the
/// rows not yet flushed.
async fn run_untailed(
	program: &Arc<Program>,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	reads: Option<u32>,
	format: Option<surrealdb_rpc::RowFormat>,
) -> FlowResult<interp::PooledState> {
	let mut paging = Vec::with_capacity(program.limits.len());
	for spec in &program.limits {
		paging.push(match spec.group {
			// A block statement's paging is evaluated when the block reaches
			// it.
			Some(_) => Paging {
				skip: 0,
				remaining: None,
			},
			None => out_of_line(|| paging_of(program, spec, ctx)).await?,
		});
	}
	let mut flush_at = ctx.root().ctx.config.exec.scan_batch_size.max(1);
	if let Some(reads) = reads {
		flush_at = flush_at.min(reads as usize).max(1);
	}
	// Under `VERSION`, every scan reads the state as of the stamp the
	// enclosing scope evaluated.
	let version = out_of_line(|| resolve_version_stamp(ctx, None)).await?;
	let mut state = State::new(program, paging, ctx.function_registry())?;
	inherit_knn(program, &mut state, ctx);
	state.row_frame =
		format.map(|format| super::row_frame::RowFrame::with_format(format).expecting(flush_at));
	#[cfg(feature = "shaped")]
	seed_frame(program, &mut state);
	// A statement spends no more than its own quantum before its first yield.
	super::fuel::cap(super::fuel::quantum(ctx));
	state.build_budget = ctx.root().ctx.config.exec.gql_max_join_build_rows;
	state.scan_hint = reads;
	for (at, spec) in program.sorts.iter().enumerate() {
		if spec.pushdown || spec.shares_floor {
			state.topk_cells[at] =
				Some(Arc::new(crate::exec::topk_pushdown::TopKThresholdCell::default()));
		}
	}
	if !program.groups.is_empty() {
		state.program = Some(Arc::clone(program));
	}
	// Inside a permission predicate every row passes, as the permission
	// operator passes it.
	state.permissions_bypassed = ctx.root().skip_fetch_perms;
	state.frames = program.permissions.iter().map(|spec| spec.context(ctx)).collect();
	bound_breakers(program, &mut state, ctx);
	run_pipelines(program, None, version, &mut state, ctx, yielder, flush_at).await?;
	Ok(state)
}

/// A block statement's rows, and how it ended.
pub(super) type GroupRun<'a> =
	std::pin::Pin<Box<dyn std::future::Future<Output = (Vec<Value>, FlowResult<()>)> + Send + 'a>>;

/// Run block statement `group` of `program` over `state`, in `frame`: its
/// pipelines, with its paging, sorts, aggregates and permission frames
/// started afresh, and its write — for a write — on the one write state the
/// statement keeps, started afresh. Answers its rows: once a signal or an
/// error ends it, only those it had flushed, as a stream's reader holds them.
///
/// The block's own row-level state is kept aside while the statement runs.
pub(super) fn run_group<'a>(
	program: &'a Arc<Program>,
	group: u16,
	state: &'a mut State,
	frame: &'a ExecutionContext,
) -> GroupRun<'a> {
	crate::exec::vm::host::out_of_line_boxed(|| async move {
		let (out, mut ran, output) =
			run_group_out(program, group, state, frame, Out::Gather(Vec::new())).await;
		let mut rows = match out {
			Out::Gather(rows) => rows,
			Out::Stream(_) | Out::Sink(_) => Vec::new(),
		};
		if ran.is_ok() {
			rows.append(&mut state.output);
		}
		state.output = output;
		// `ONLY` hands on the one row, or `NONE`, and nothing when it fails.
		if program.groups.get(group as usize).is_some_and(|spec| spec.only) {
			if rows.len() > 1 {
				ran = Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::SingleOnlyOutput)));
			}
			match ran {
				Ok(()) if rows.is_empty() => rows.push(Value::None),
				Ok(()) => {}
				Err(_) => rows.clear(),
			}
		}
		(rows, ran)
	})
}

/// Run block statement `group` of `program` as [`run_group`] does, handing
/// its rows to `sink` a batch at a time rather than gathering them: for a
/// reader of the rows that bounds what it holds, such as a join's build.
/// `ONLY` is not applied.
pub(super) fn run_group_into<'a>(
	program: &'a Arc<Program>,
	group: u16,
	state: &'a mut State,
	frame: &'a ExecutionContext,
	sink: &'a mut (dyn RowSink + Send),
) -> crate::exec::BoxFut<'a, FlowResult<()>> {
	crate::exec::vm::host::out_of_line_boxed(|| async move {
		let (out, ran, output) = run_group_out(program, group, state, frame, Out::Sink(sink)).await;
		let rest = std::mem::replace(&mut state.output, output);
		ran?;
		if let Out::Sink(sink) = out {
			sink.take(rest, frame).await?;
		}
		Ok(())
	})
}

/// [`run_group`]'s run into `out`, the block's row-level state kept aside
/// and put back, but for its output: the rows the run left are in
/// `state.output`, and the output kept aside is answered for the caller to
/// put back once it has taken them.
fn run_group_out<'a, 'o>(
	program: &'a Arc<Program>,
	group: u16,
	state: &'a mut State,
	frame: &'a ExecutionContext,
	mut out: Out<'o, 'o>,
) -> crate::exec::BoxFut<'a, (Out<'o, 'o>, FlowResult<()>, Vec<Value>)>
where
	'o: 'a,
{
	crate::exec::vm::host::out_of_line_boxed(|| async move {
		if state.program.is_none() {
			state.program = Some(Arc::clone(program));
		}
		let output = state.take_output();
		let write = state.write.take();
		let rowless = std::mem::replace(&mut state.rowless, false);
		let undocumented = std::mem::replace(&mut state.undocumented, false);
		let root = state.root.take();
		let in_place = std::mem::replace(&mut state.read_in_place_permission_errors, false);
		let sorted_keys = std::mem::take(&mut state.sorted_keys);
		let hand_on = std::mem::take(&mut state.hand_on);
		let gather_account = state.gather_account.take();
		let ran = run_tailed(program, group, state, frame, &mut out).await;
		// The rows gathered leave the statement as its value. A `LET`'s value is
		// bound next, and takes over the account that charged its rows; any
		// other leaves with the value, and its account is released.
		let account = std::mem::replace(&mut state.gather_account, gather_account);
		if hand_on {
			state.handed = account;
		}
		if let Some(slot) = state.group_writes.get_mut(group as usize) {
			*slot = state.write.take();
		}
		state.write = write;
		state.rowless = rowless;
		state.undocumented = undocumented;
		state.root = root;
		state.read_in_place_permission_errors = in_place;
		state.sorted_keys = sorted_keys;
		(out, ran, output)
	})
}

/// Where a statement's rows go when they are neither streamed nor gathered:
/// a reader that takes them a batch at a time as they are flushed.
pub(super) trait RowSink {
	/// Take `rows`, the next batch in order.
	fn take<'a>(
		&'a mut self,
		rows: Vec<Value>,
		ctx: &'a ExecutionContext,
	) -> crate::exec::BoxFut<'a, FlowResult<()>>;
}

/// A state for `program`, a block statement compiled as a program of its own,
/// in `ctx`: [`run_group`] starts its paging, sorts, aggregates and
/// permission frames afresh per reach.
pub(super) fn statement_state(
	program: &Program,
	ctx: &ExecutionContext,
) -> anyhow::Result<interp::PooledState> {
	let mut state = unit_state(program, ctx)?;
	host_frames(program, &mut state, ctx);
	Ok(state)
}

/// [`statement_state`] without what only a host step reads — the permission
/// frames and the sorts' spill — for a unit's body run in place
/// ([`run_unit_inline`]); [`host_frames`] completes it for the host.
pub(super) fn unit_state(
	program: &Program,
	ctx: &ExecutionContext,
) -> anyhow::Result<interp::PooledState> {
	let paging = program
		.limits
		.iter()
		.map(|_| Paging {
			skip: 0,
			remaining: None,
		})
		.collect();
	let mut state = State::new(program, paging, ctx.function_registry())?;
	inherit_knn(program, &mut state, ctx);
	state.build_budget = ctx.root().ctx.config.exec.gql_max_join_build_rows;
	state.permissions_bypassed = ctx.root().skip_fetch_perms;
	Ok(state)
}

/// Give `state`, a new state of `program` run in `ctx`, the distances of the KNN search of the
/// statement `program` is nested in ([`ExecutionContext::inherited_knn`]) as those of its own
/// code, where its own code publishes no distances of a search of its own: its index functions
/// report against the enclosing statement's search, as a nested statement inherits that scope. A
/// statement the program runs in place publishes into a slot of its own group, and leaves the
/// program's own slot, `0`, to inherit.
fn inherit_knn(program: &Program, state: &mut State, ctx: &ExecutionContext) {
	fn publishes(source: &super::program::Source) -> bool {
		match source {
			super::program::Source::Union(members) => members.iter().any(publishes),
			super::program::Source::Knn {
				slot: Some(0),
				..
			} => true,
			super::program::Source::Scan {
				..
			}
			| super::program::Source::Record {
				..
			}
			| super::program::Source::Index {
				..
			}
			| super::program::Source::Lookup {
				..
			}
			| super::program::Source::IndexUnion {
				..
			}
			| super::program::Source::Bitmap {
				..
			}
			| super::program::Source::FullText {
				..
			}
			| super::program::Source::Knn {
				..
			}
			| super::program::Source::Count {
				..
			}
			| super::program::Source::Values {
				..
			}
			| super::program::Source::Nested {
				..
			}
			| super::program::Source::Probe {
				..
			}
			| super::program::Source::Sorted {
				..
			}
			| super::program::Source::Groups {
				..
			}
			| super::program::Source::Split {
				..
			}
			| super::program::Source::Join {
				..
			}
			| super::program::Source::Once
			| super::program::Source::Changes {
				..
			}
			| super::program::Source::WriteTargets {
				..
			}
			| super::program::Source::WriteInserts {
				..
			}
			| super::program::Source::WriteEdges {
				..
			}
			| super::program::Source::LightweightEdges {
				..
			}
			| super::program::Source::Vacant {
				..
			} => false,
		}
	}
	let Some(knn) = ctx.inherited_knn() else {
		return;
	};
	if program.sorts.iter().any(|sort| sort.knn_slot == Some(0))
		|| program.pipelines.iter().any(|pipeline| publishes(&pipeline.source))
	{
		return;
	}
	state.knn_contexts = vec![Some(Arc::clone(knn))];
}

/// Give `state`, a [`unit_state`], the permission frames and the sorts'
/// spill of `program` in `ctx`, which the host's steps read.
pub(super) fn host_frames(program: &Program, state: &mut State, ctx: &ExecutionContext) {
	state.frames = program.permissions.iter().map(|spec| spec.context(ctx)).collect();
	bound_breakers(program, state, ctx);
}

/// Run block statement `group` under its tail: in the frame its `VERSION`
/// reads at, and failed once its `TIMEOUT` has passed.
async fn run_tailed(
	program: &Arc<Program>,
	group: u16,
	state: &mut State,
	frame: &ExecutionContext,
	out: &mut Out<'_, '_>,
) -> FlowResult<()> {
	let spec = program.groups.get(group as usize).ok_or_else(|| {
		ControlFlow::Err(anyhow::anyhow!("a block statement the program does not hold"))
	})?;
	let versioned;
	let frame = match &spec.version {
		Some(version) => {
			let value = host_scalar(program, version, frame, None).await?;
			let stamp = value
				.cast_to::<crate::val::Datetime>()
				.map_err(|e| anyhow::anyhow!("{e}"))?
				.to_version_stamp(frame.txn().version_impl().as_ref())?;
			versioned = frame.clone().with_version_stamp(Some(stamp));
			&versioned
		}
		None => frame,
	};
	let Some(timeout) = &spec.timeout else {
		return start_group(program, group, state, frame, out).await;
	};
	let duration = host_scalar(program, timeout, frame, None)
		.await?
		.cast_to::<Option<crate::val::Duration>>()
		.map_err(anyhow::Error::new)?;
	let Some(duration) = duration else {
		return start_group(program, group, state, frame, out).await;
	};
	let Some(deadline) = common::time::Instant::now().checked_add(duration.0) else {
		return Err(ControlFlow::Err(anyhow::Error::new(
			crate::kvs::DatastoreError::InvalidTimeout(duration.0.as_secs()),
		)));
	};
	let timed_out =
		|| ControlFlow::Err(anyhow::anyhow!(crate::err::EngineError::QueryTimedout(duration.0)));
	let remaining = deadline.saturating_duration_since(common::time::Instant::now());
	match common::time::timeout(remaining, start_group(program, group, state, frame, out)).await {
		// A statement that finishes past its deadline has timed out too.
		Ok(ran) => {
			ran?;
			if common::time::Instant::now() >= deadline {
				return Err(timed_out());
			}
			Ok(())
		}
		Err(_) => {
			stopped_at_deadline(program, frame);
			Err(timed_out())
		}
	}
}

/// Marks the transaction of a statement that was dropped at its deadline.
///
/// A deadline stops a statement by dropping its future, which can land inside
/// a record's write: the record is in the transaction and the index entries
/// written so far with it, and a client-owned transaction survives the
/// statement's error and can still commit. A program that may write therefore
/// makes the commit of its transaction refuse.
fn stopped_at_deadline(program: &Program, frame: &ExecutionContext) {
	if program.access_mode == crate::exec::AccessMode::ReadWrite {
		frame.txn().poison_interrupted_write();
	}
}

async fn start_group(
	program: &Arc<Program>,
	group: u16,
	state: &mut State,
	frame: &ExecutionContext,
	out: &mut Out<'_, '_>,
) -> FlowResult<()> {
	let spec = program.groups.get(group as usize).ok_or_else(|| {
		ControlFlow::Err(anyhow::anyhow!("a block statement the program does not hold"))
	})?;
	// Checked per reach, as a statement's program checks it when it starts.
	match &spec.write {
		Some(write) => refuse_unwritable(write, frame)?,
		None if !spec.rowless => validate_record_user_access(frame.database()?)?,
		None => {}
	}
	for (at, limit) in program.limits.iter().enumerate() {
		if limit.group == Some(group) {
			state.paging[at] = out_of_line(|| paging_of(program, limit, frame)).await?;
		}
	}
	for at in spec.sorts.clone() {
		let at = at as usize;
		state.sorts[at].clear();
		state.sort_slabs[at].clear();
		state.sort_arrivals[at] = Default::default();
		state.shared_floors[at] = Default::default();
		if let Some(sort_spec) = program.sorts.get(at)
			&& (sort_spec.pushdown || sort_spec.shares_floor)
		{
			let cell = Arc::new(crate::exec::topk_pushdown::TopKThresholdCell::default());
			state.topk_cells[at] = Some(cell);
		}
		state.sort_runs[at] = None;
		state.sort_accounts[at].clear();
		if let Some(spill) = state.sort_spill[at].as_mut() {
			spill.floor = None;
			spill.taken_bytes = 0;
			spill.taken_rows = 0;
		}
	}
	for at in spec.aggregates.clone() {
		state.reset_aggregate(program, at as usize);
	}
	super::window_step::reset_windows(program, group, state);
	for at in spec.permissions.clone() {
		state.frames[at as usize] = program.permissions[at as usize].context(frame);
	}
	for (at, nested) in program.nested.iter().enumerate() {
		if nested.owner == Some(group) {
			state.reset_once(at);
			state.lookups[at] = 0;
		}
	}
	for (at, owner) in program.probes.iter().enumerate() {
		if *owner == Some(group) {
			state.probes[at] = None;
		}
	}
	for (at, join) in program.joins.iter().enumerate() {
		if join.owner == Some(group) {
			state.joins[at] = None;
			state.join_lookups[at] = 0;
			state.held[at] = None;
		}
	}
	for (at, owner) in program.verdicts.iter().enumerate() {
		if *owner == Some(group) {
			state.verdicts[at] = None;
		}
	}
	for (at, owner) in program.distincts.iter().enumerate() {
		if *owner == Some(group) {
			state.seen[at] = crate::exec::operators::SeenSet::new();
		}
	}
	let flush_at = frame.root().ctx.config.exec.scan_batch_size.max(1);
	let version = out_of_line(|| resolve_version_stamp(frame, None)).await?;
	match &spec.write {
		Some(write) => {
			// Whether a unique-index conflict must withhold the stored record and
			// value. Read before `in_write`, so it reflects the caller's own
			// permission status rather than the suppressed write frame, or the
			// status a frame running on a caller's behalf was seeded with. A
			// grouped write does not pass through the stream, so its conflict is
			// redacted here instead ([`write::hide_conflict_record`]).
			let hide_index_conflict = frame
				.should_check_perms(crate::iam::Action::View)
				.map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?
				|| frame.caller_view_checked();
			// What a write reaches runs in the write's frame.
			let writing;
			let frame = if frame.root().writing {
				frame
			} else {
				writing = frame.clone().in_write();
				&writing
			};
			let kept = state.group_writes.get_mut(group as usize).and_then(Option::take);
			state.write = Some(match kept {
				Some(mut kept) => {
					kept.reset(write, group, frame)?;
					kept
				}
				None => {
					let mut fresh = Box::new(write::WriteState::new(write, frame)?);
					fresh.group = Some(group);
					fresh
				}
			});
			write::run_write(program, Some(group), version, state, frame, out, flush_at)
				.await
				.map_err(|err| write::hide_conflict_record(err, hide_index_conflict))
		}
		None => run_pipelines(program, Some(group), version, state, frame, out, flush_at).await,
	}
}

/// The query vector `vector` computes: evaluated once, with no row in scope
/// and the row being scanned forbidden to it, and coerced to
/// `array<number>`. A value that does not coerce, an evaluation that fails,
/// one that reads the row being scanned, or a vector no distance ranks a
/// record against (empty, or holding a NaN or an infinity) fails the
/// statement.
pub(super) async fn evaluate_knn_query(
	program: &Program,
	vector: &super::program::DeferredVector,
	ctx: &ExecutionContext,
) -> FlowResult<Arc<[crate::val::Number]>> {
	let value = match &vector.scalar {
		super::program::HostScalar::Const(value) => Value::clone(value),
		scalar => {
			let forbidden = ctx.with_forbidden_row(Arc::clone(&vector.text));
			host_scalar(program, scalar, &forbidden, None).await?
		}
	};
	let values = value
		.coerce_to::<Vec<crate::val::Number>>()
		.map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;
	if !crate::exec::planner::util::is_rankable_query_vector(&values) {
		return Err(ControlFlow::Err(anyhow::Error::new(crate::exec::Error::InvalidKnnVector {
			vector: vector.text.to_string(),
		})));
	}
	Ok(Arc::from(values))
}

/// Evaluate the query vector of brute-force KNN sort `sort`, computed when the
/// statement runs, into its cell of [`State::knn_queries`] unless the run has
/// evaluated it already.
///
/// The host calls it when a row reaches a ranking against a vector the run
/// has not evaluated ([`Step::KnnQuery`]): a pipeline whose rows never reach
/// the ranking — a limit that admits none, a source with no row, a condition
/// beneath the ranking that rejects every row — never evaluates the vector.
/// A vector a parameter of the request or the catalog holds is evaluated as
/// the run of its group starts instead ([`run_pipelines`]), whatever rows
/// reach the ranking. The cell is shared by the batches of a region, which
/// wait on the one evaluation the first of them starts.
pub(super) async fn evaluate_knn_query_of(
	program: &Program,
	sort: u16,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	let at = sort as usize;
	let Some(query) = program.sorts.get(at).and_then(|spec| spec.knn_query.as_ref()) else {
		return Ok(());
	};
	if state.knn_queries.len() <= at {
		state.knn_queries.resize_with(at + 1, Default::default);
	}
	let cell = Arc::clone(&state.knn_queries[at]);
	read_knn_query(&cell, || evaluate_knn_query(program, &query.vector, ctx)).await?;
	Ok(())
}

/// The vector `cell` holds, evaluated by `evaluate` unless an earlier reader
/// of the cell evaluated it, or is evaluating it, already. `evaluate` runs at
/// most once for the cell: every reader, the one that ran it included, takes
/// the one outcome it left, a failure included ([`super::interp::KnnQueryOutcome`]).
pub(super) async fn read_knn_query<F, E>(
	cell: &super::interp::KnnQueryCell,
	evaluate: F,
) -> FlowResult<Arc<[crate::val::Number]>>
where
	F: FnOnce() -> E,
	E: std::future::Future<Output = FlowResult<Arc<[crate::val::Number]>>>,
{
	cell.get_or_init(|| async { super::interp::KnnQueryOutcome::of(evaluate().await) }).await.read()
}

/// The query vector `query` of a KNN search, computed when the statement runs:
/// evaluated by the first source of the run that searches with it, as it opens,
/// and read by every other.
async fn evaluate_knn_search(
	program: &Program,
	query: &Arc<super::program::KnnQuery>,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Arc<[crate::val::Number]>> {
	let cell = match state.knn_searches.iter().find(|(held, _)| Arc::ptr_eq(held, query)) {
		Some((_, cell)) => Arc::clone(cell),
		None => {
			let cell = super::interp::KnnQueryCell::default();
			state.knn_searches.push((Arc::clone(query), Arc::clone(&cell)));
			cell
		}
	};
	read_knn_query(&cell, || evaluate_knn_query(program, &query.vector, ctx)).await
}

/// Let go of the query vectors of the brute-force KNN sorts and the KNN
/// searches `group` owns, as a run of its pipelines starts: each run evaluates
/// its own, the first time a row is ranked against it
/// ([`evaluate_knn_query_of`]) or a source searches with it
/// ([`evaluate_knn_search`]), where a run of a correlated statement may compute
/// another vector than the run before. A block statement also lets go of the
/// distances its search published (its slot, `group + 1`), which the new run
/// publishes afresh; the program's own slot may hold the distances it inherits.
pub(super) fn forget_knn_queries(program: &Program, group: Option<u16>, state: &mut State) {
	if let Some(group) = group
		&& let Some(slot) = state.knn_contexts.get_mut(usize::from(group) + 1)
	{
		*slot = None;
	}
	for (at, spec) in program.sorts.iter().enumerate() {
		if spec.knn_query.as_ref().is_some_and(|query| query.group == group)
			&& let Some(held) = state.knn_queries.get_mut(at)
		{
			*held = Default::default();
		}
	}
	state.knn_searches.retain(|(query, _)| query.group != group);
}

/// Whether this run of `pipeline` runs its body on the stored row: the pipeline is planned so
/// ([`Pipeline::in_place`]), and [`measures_on_host`] is clear. A run that does not reads its rows
/// decoded, as a pipeline planned to does: in morsels of batches where the plan parallelises it,
/// and with bulk copies that may stop for a yield.
pub(super) fn runs_in_place(pipeline: &Pipeline, state: &State) -> bool {
	pipeline.in_place && !measures_on_host(pipeline, state)
}

/// Whether `pipeline` holds a vector measure this run settled for the host to answer
/// ([`MeasureSettled::Host`](super::measure::MeasureSettled)), which hands the host a step for
/// each row.
fn measures_on_host(pipeline: &Pipeline, state: &State) -> bool {
	use super::measure::MeasureSettled;
	let host = |cell: Option<&MeasureSettled>| matches!(cell, Some(MeasureSettled::Host));
	state.measures.iter().any(|cell| host(cell.as_deref()))
		&& pipeline.code.iter().any(|instr| {
			if let Instr::Measure {
				measure,
				..
			} = instr
			{
				host(state.measures.get(measure.call as usize).and_then(Option::as_deref))
			} else {
				false
			}
		})
}

/// Whether a pipeline `group` owns holds a vector measure ([`Instr::Measure`]).
fn holds_measures(program: &Program, group: Option<u16>) -> bool {
	program
		.pipelines
		.iter()
		.filter(|pipeline| pipeline.group == group)
		.any(|pipeline| pipeline.code.iter().any(|instr| matches!(instr, Instr::Measure { .. })))
}

/// Settle the vector measures of the pipelines `group` owns for this run of
/// them ([`super::measure`]): each measure's capability check, and its query
/// argument where the check allows the call. A failure of either leaves the
/// measure to the host, which raises it at the first row that reaches the
/// measure, so settling raises nothing.
pub(super) async fn settle_measures(
	program: &Program,
	group: Option<u16>,
	state: &mut State,
	ctx: &ExecutionContext,
) {
	for pipeline in program.pipelines.iter().filter(|pipeline| pipeline.group == group) {
		for instr in &pipeline.code {
			if let Instr::Measure {
				measure,
				..
			} = instr
			{
				settle_measure(program, measure, state, ctx).await;
			}
		}
	}
}

/// Settle `measure` for this run into its cell of [`State::measures`]: the check of its builtin
/// against the capabilities in the statement's frame, then its query argument, read with no row
/// as [`Instr::LoadParam`] reads it. The measure is left to the host for each row
/// ([`MeasureSettled::Host`](super::measure::MeasureSettled)) where the check refuses the call,
/// where a read fails, and where the query is a parameter the catalog defines whose
/// `PERMISSIONS` predicate is judged on every read.
async fn settle_measure(
	program: &Program,
	measure: &super::measure::MeasureSpec,
	state: &mut State,
	ctx: &ExecutionContext,
) {
	use super::measure::{MeasureQuery, MeasureSettled};
	let at = measure.call as usize;
	let eval_ctx = frame_eval(program, state, ctx, None);
	let settled = match measured_builtin(&program.calls[at].name, &eval_ctx) {
		None => MeasureSettled::Host,
		Some(function) => match measure.query {
			MeasureQuery::Const(index) => {
				MeasureSettled::allowed(function, program.consts[index as usize].clone())
			}
			MeasureQuery::Param(index) => {
				let spec = &program.params[index as usize];
				match super::function::frame_param(spec, &eval_ctx) {
					Ok(Some(value)) => MeasureSettled::allowed(function, value),
					Err(_) => MeasureSettled::Host,
					Ok(None) => {
						match super::function::stored_param_once(program, spec, &eval_ctx).await {
							Ok(Some(value)) => MeasureSettled::allowed(function, value),
							Ok(None) | Err(_) => MeasureSettled::Host,
						}
					}
				}
			}
		},
	};
	if state.measures.len() <= at {
		state.measures.resize(at + 1, None);
	}
	state.measures[at] = Some(Arc::new(settled));
}

/// The registry's builtin `name`, where the interpreter may answer a call of it over a row itself:
/// the capabilities in the frame `ctx` runs in allow the call, and the builtin is pure, needs no
/// host to run and holds nothing [`call_builtin`] refuses or meters. `None` for the host to answer.
fn measured_builtin(
	name: &str,
	ctx: &EvalContext<'_>,
) -> Option<&'static dyn crate::exec::function::ScalarFunction> {
	ctx.check_allowed_function(name).ok()?;
	let function = ctx.exec_ctx.function_registry().get(name)?;
	let plain = function.is_pure()
		&& !function.is_async()
		&& function.metered().is_none()
		&& refuse_mutating_builtin(function.name(), function.effects(), ctx.exec_ctx).is_ok();
	plain.then_some(function)
}

/// Run the pipelines of `program` that `group` owns — the program's own for
/// `None` — in order, over `state`, handing their rows to `yielder`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_pipelines(
	program: &Arc<Program>,
	group: Option<u16>,
	version: Option<u64>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<()> {
	forget_knn_queries(program, group, state);
	// Settled out of line, so the future of a run of the pipelines holds none of the evaluation.
	if holds_measures(program, group) {
		out_of_line(|| settle_measures(program, group, state, ctx)).await;
	}
	// A ranking's vector a parameter holds is evaluated before any row is read, so an unrankable
	// one refuses the statement whatever rows it reads.
	for (at, spec) in program.sorts.iter().enumerate() {
		if spec.knn_query.as_ref().is_some_and(|query| query.group == group && query.before_rows) {
			let sort = u16::try_from(at).map_err(|_| {
				ControlFlow::Err(anyhow::anyhow!("a KNN sort index past the sort pool"))
			})?;
			evaluate_knn_query_of(program, sort, state, ctx).await?;
		}
	}
	for (index, pipeline) in
		program.pipelines.iter().enumerate().filter(|(_, pipeline)| pipeline.group == group)
	{
		#[cfg(test)]
		super::row_packed::observed::note_pipeline(pipeline, state);
		// The right input of a join that holds its left row is read only once
		// the left input handed it one, as a build side is read only once a
		// probe row arrives.
		let streamed = program.joins.iter().position(|join| join.streams == Some(index));
		if let Some(join) = streamed
			&& state.held[join].is_none()
		{
			continue;
		}
		// A join whose build side writes runs it before the pipeline that
		// reads the rows it takes starts, so those rows read what it wrote.
		for (split, spec) in program.splits.iter().enumerate() {
			if spec.build_before == Some(index)
				&& let super::program::SplitKind::Step(step) = &spec.kind
			{
				out_of_line(|| super::binding::prepare_join(split as u16, step, state, ctx))
					.await?;
			}
		}
		// A pipeline one of whose limits admits nothing reads no
		// record: it opens no read of storage — a table, an index,
		// a record id or range, a union of them — and no sort or
		// aggregate, which has already drained its own input. A source that is not a read
		// of storage still yields its rows and every row is
		// judged, so the errors its layers and predicates raise
		// are raised.
		//
		// A statement in `FROM` bound to its reader's zero limit reads no
		// record of storage either ([`State::zeroed`]).
		//
		// A locked read (`FOR UPDATE`) is read whatever its limit: the lock
		// is a side effect of the read, and the limit only drops its row.
		let reads_storage = (pipeline.source.reads_storage()
			|| matches!(pipeline.source, Source::Count { .. } | Source::Probe { .. }))
			&& !matches!(
				pipeline.source,
				Source::Record {
					for_update: true,
					..
				}
			);
		let skippable = reads_storage
			|| matches!(pipeline.source, Source::Sorted { .. } | Source::Groups { .. });
		let spent = limits_spent(pipeline, state);
		if (skippable && spent) || (reads_storage && state.zeroed) {
			if let Some(join) = streamed {
				state.held[join] = None;
			}
			// The table a read names is still required defined, as opening it would.
			if let Source::Vacant {
				table,
			} = &pipeline.source
			{
				require_defined(ctx, table, version).await?;
			} else if let Some(table) = pipeline.source.table() {
				let descriptor = crate::exec::vm::descriptor::table_at(ctx, table, version)
					.await
					.map_err(ControlFlow::Err)?;
				let db_ctx = ctx.database().context("the VM requires a database context")?;
				// A scan, and a read of the records a list names, which stands for one, read an
				// absent table as empty where the absence is hidden, as when they run.
				let hidden = absence_hidden(&descriptor, db_ctx)?
					&& match &pipeline.source {
						Source::Scan {
							..
						} => true,
						Source::Record {
							record,
							..
						} => matches!(
							host_scalar(program, record, ctx, None).await?,
							Value::Array(_)
						),
						Source::Index {
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
						| Source::Values {
							..
						}
						| Source::Nested {
							..
						}
						| Source::Probe {
							..
						}
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
						} => false,
					};
				if !hidden {
					descriptor.defined().map_err(ControlFlow::Err)?;
				}
			}
			continue;
		}
		match &pipeline.source {
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
			| Source::LightweightEdges {
				..
			}
			| Source::Vacant {
				..
			}
			| Source::Union(_) => {
				out_of_line(|| {
					run_storage_source(
						program,
						pipeline,
						&pipeline.source,
						version,
						state,
						ctx,
						yielder,
						flush_at,
					)
				})
				.await?;
			}
			Source::Values {
				..
			}
			| Source::Nested {
				..
			}
			| Source::Probe {
				..
			} => {
				out_of_line(|| {
					super::nested::run_source(
						program,
						pipeline,
						&pipeline.source,
						spent,
						state,
						ctx,
						yielder,
						flush_at,
					)
				})
				.await?;
			}
			// The counted row, counted as the count fast path's
			// operators count it.
			Source::Count {
				count,
				output,
				cap,
			} => {
				let rows = match out_of_line(|| {
					super::count::count(
						program,
						count,
						version,
						state.units.clone(),
						ctx,
						cap.map(|cap| cap.reach()),
					)
				})
				.await?
				{
					super::count::Counted::Count(n) => {
						output.batch(cap.map_or(n, |cap| cap.apply(n))).into_values()
					}
					super::count::Counted::Rows(rows) => rows,
				};
				for row in rows {
					out_of_line(|| run_value_row(program, pipeline, row, state, ctx)).await?;
				}
				out_of_line(|| flush(state, yielder, flush_at, ctx)).await?;
			}
			// The change sets, read when the source opens, each a row.
			Source::Changes {
				table,
				since,
				limit,
			} => {
				let opt = ctx.options().ok_or_else(|| {
					ControlFlow::Err(anyhow::anyhow!(
						"a change feed read without the session's options"
					))
				})?;
				let rows = crate::cf::reader::changes(
					ctx.ctx(),
					opt,
					table.as_ref(),
					since.clone(),
					*limit,
				)
				.await
				.map_err(ControlFlow::Err)?;
				for row in rows {
					out_of_line(|| run_value_row(program, pipeline, row, state, ctx)).await?;
				}
				out_of_line(|| flush(state, yielder, flush_at, ctx)).await?;
			}
			Source::Sorted {
				sort,
			} => {
				let spec = &program.sorts[*sort as usize];
				if let Some(runs) = state.sort_runs[*sort as usize].take()
					&& let Some(memory) = state.memory.clone()
				{
					let mut held = std::mem::take(&mut state.sorts[*sort as usize]);
					let slab = std::mem::take(&mut state.sort_slabs[*sort as usize]);
					let keys = match spec.random {
						true => {
							super::sort::with_random_keys(&mut held);
							vec![super::sort::RANDOM_ORDER]
						}
						false => spec.keys.clone(),
					};
					let keep = spec.keep;
					let held =
						super::sort::sorted(held, &keys, keep.unwrap_or(usize::MAX), ctx).await?;
					let held = super::spill::records_of(held, &slab)?;
					// The rows the merge still holds stay charged until the drain ends, so a
					// breaker the sorted rows feed charges against what the sort holds, while
					// the sort's own account starts again empty for no spill pass to visit.
					let _draining = state.sort_accounts[*sort as usize].detach();
					let order = super::spill::sort_order(keys);
					let mut merge = runs.merge(&memory, held, order, keep, ctx).await?;
					loop {
						let most = ctx.root().ctx.config.exec.scan_batch_size.max(1);
						let chunk = merge.next_chunk(most, ctx).await?;
						if chunk.is_empty() {
							break;
						}
						let stopped = out_of_line(|| {
							run_rows(program, pipeline, chunk, state, ctx, yielder, flush_at)
						})
						.await?;
						if stopped {
							break;
						}
					}
					continue;
				}
				// Charged until the drain ends, as above.
				let _draining = state.sort_accounts[*sort as usize].detach();
				let rows = std::mem::take(&mut state.sorts[*sort as usize]);
				let slab = std::mem::take(&mut state.sort_slabs[*sort as usize]);
				let keep = spec.keep.unwrap_or(usize::MAX);
				let rows = if spec.random {
					out_of_line(|| super::sort::shuffled(rows, keep, ctx)).await?
				} else {
					out_of_line(|| super::sort::sorted(rows, &spec.keys, keep, ctx)).await?
				};
				// A statement whose rows leave encoded, and whose body after the
				// sort hands the host no step, runs a row held as stored bytes
				// on those bytes, a unit each, charged a stretch at a time.
				if pipeline.stored_rows
					&& !measures_on_host(pipeline, state)
					&& state.row_frame.is_some()
					&& spec.knn_slot.is_none()
				{
					let mut stretch = super::fuel::left().clamp(1, 256);
					let mut taken = 0;
					for (keys, row) in rows {
						state.sorted_keys = keys;
						let stop = match row.stored(&slab) {
							// Stored bytes in a sort were settled from a document or
							// `revision` row: a packed row is settled as a value, so
							// the replay needs no layouts.
							Some((key, value)) => {
								in_place_row(program, pipeline, key, value, None, state)
									.map_err(ControlFlow::Err)?
							}
							None => {
								let row = row.into_value().map_err(ControlFlow::Err)?;
								out_of_line(|| run_value_row(program, pipeline, row, state, ctx))
									.await? == Ended::Stop
							}
						};
						if stop {
							break;
						}
						taken += 1;
						if taken == stretch {
							super::fuel::tick(ctx, taken).await?;
							stretch = super::fuel::left().clamp(1, 256);
							taken = 0;
						}
						flush(state, yielder, flush_at, ctx).await?;
					}
					super::fuel::tick(ctx, taken).await?;
					continue;
				}
				// A row held as stored bytes is decoded as the sort hands it
				// on, a unit each, charged a stretch at a time. Only a brute-force
				// KNN needs every row decoded before the first is yielded; any
				// other sort decodes a row as the pipeline after it takes the row,
				// so the decoded rows are never all held at once.
				if spec.knn_slot.is_none() {
					let mut stretch = super::fuel::left().clamp(1, 256);
					let mut taken = 0;
					for (keys, row) in rows {
						let row = row.into_value_in(&slab).map_err(ControlFlow::Err)?;
						state.sorted_keys = keys;
						let stop =
							out_of_line(|| run_value_row(program, pipeline, row, state, ctx))
								.await? == Ended::Stop;
						if stop {
							break;
						}
						taken += 1;
						if taken == stretch {
							super::fuel::tick(ctx, taken).await?;
							stretch = super::fuel::left().clamp(1, 256);
							taken = 0;
						}
						flush(state, yielder, flush_at, ctx).await?;
					}
					super::fuel::tick(ctx, taken).await?;
				} else {
					let mut decoded = Vec::with_capacity(rows.len());
					let mut stretch = super::fuel::left().clamp(1, 256);
					let mut taken = 0;
					for (keys, row) in rows {
						decoded.push((keys, row.into_value_in(&slab).map_err(ControlFlow::Err)?));
						taken += 1;
						if taken == stretch {
							super::fuel::tick(ctx, taken).await?;
							stretch = super::fuel::left().clamp(1, 256);
							taken = 0;
						}
					}
					super::fuel::tick(ctx, taken).await?;
					let rows = decoded;
					// A brute-force KNN publishes the distance it ranked each row
					// by before any row is yielded, so a projection reading
					// them finds them.
					if let Some(slot) = spec.knn_slot {
						let published = state.knn_context(slot);
						published
							.insert_all(rows.iter().filter_map(|(keys, row)| {
								match (keys.first(), row) {
									(Some(Value::Number(distance)), Value::Object(object)) => {
										match object.get("id") {
											Some(Value::RecordId(rid)) => {
												Some((rid.clone(), distance.clone()))
											}
											_ => None,
										}
									}
									_ => None,
								}
							}))
							.await;
					}
					out_of_line(|| {
						run_rows(program, pipeline, rows, state, ctx, yielder, flush_at)
					})
					.await?;
				}
			}
			Source::Once => {
				// The enclosing row is read where it is held, and copied only
				// where the body changes it or hands it on.
				let row = ctx.current_row();
				state.rowless = row.is_none();
				out_of_line(|| {
					run_cow_row(
						program,
						pipeline,
						row.map_or(Cow::Owned(Value::None), Cow::Borrowed),
						state,
						ctx,
					)
				})
				.await?;
			}
			Source::WriteTargets {
				..
			}
			| Source::WriteInserts {
				..
			}
			| Source::WriteEdges {
				..
			} => {
				return Err(ControlFlow::Err(anyhow::anyhow!("a write's subjects in a read")));
			}
			// A split's rows run as its input's pipeline settles each row it
			// splits; a write stage's, which keeps its rows until that
			// pipeline has finished, run now. A join's pairings run as its
			// probe pipeline settles each probe row.
			Source::Split {
				split,
			} => {
				if let super::program::SplitKind::Step(step) = &program.splits[*split as usize].kind
					&& matches!(
						step,
						super::binding::BindingStep::Distinct
							| super::binding::BindingStep::Join(_)
					) {
					let merge = out_of_line(|| {
						super::binding::finish_deferred(program, *split, step, state, ctx)
					})
					.await?;
					if let Some(mut merge) = merge {
						let most = ctx.root().ctx.config.exec.scan_batch_size.max(1);
						'rows: loop {
							let chunk = merge.next_chunk(most, ctx).await?;
							if chunk.is_empty() {
								break;
							}
							for (_, row) in chunk {
								if out_of_line(|| run_value_row(program, pipeline, row, state, ctx))
									.await? == Ended::Stop
								{
									break 'rows;
								}
							}
							out_of_line(|| flush(state, yielder, flush_at, ctx)).await?;
						}
						out_of_line(|| flush(state, yielder, flush_at, ctx)).await?;
					}
				} else if let super::program::SplitKind::Step(step) =
					&program.splits[*split as usize].kind
					&& step.buffers()
				{
					let rows = out_of_line(|| {
						super::binding::finish_step(program, *split, step, state, ctx)
					})
					.await?;
					for row in rows {
						if out_of_line(|| run_value_row(program, pipeline, row, state, ctx)).await?
							== Ended::Stop
						{
							break;
						}
					}
					out_of_line(|| flush(state, yielder, flush_at, ctx)).await?;
				} else if let super::program::SplitKind::Window(_) =
					&program.splits[*split as usize].kind
					&& let Some(mut window) = super::window_step::take_window(*split, state)
				{
					let drained = out_of_line(|| {
						drain_window(program, pipeline, &mut window, state, ctx, yielder, flush_at)
					})
					.await;
					window.clear();
					drained?;
				}
			}
			Source::Join {
				join,
			} => {
				out_of_line(|| {
					finish_spilled_join(program, pipeline, *join, state, ctx, yielder, flush_at)
				})
				.await?;
			}
			Source::Groups {
				aggregate,
			} => {
				if let Some(runs) = state.group_runs[*aggregate as usize].take()
					&& let Some(memory) = state.memory.clone()
				{
					out_of_line(|| {
						finish_spilled_groups(
							program, pipeline, *aggregate, runs, memory, state, ctx, yielder,
							flush_at,
						)
					})
					.await?;
					continue;
				}
				state.group_accounts[*aggregate as usize].clear();
				let groups = state.groups[*aggregate as usize].take();
				let rows = out_of_line(|| finish_groups(program, *aggregate, groups, ctx)).await?;
				let rows = rows.into_iter().map(|row| (Vec::new(), row)).collect();
				out_of_line(|| run_rows(program, pipeline, rows, state, ctx, yielder, flush_at))
					.await?;
			}
		}
		if let Some(join) = streamed {
			out_of_line(|| finish_held_join(program, join as u16, state, ctx)).await?;
			out_of_line(|| flush(state, yielder, flush_at, ctx)).await?;
		}
	}
	Ok(())
}

/// Whether a limit of `pipeline` admits no row.
pub(super) fn limits_spent(pipeline: &Pipeline, state: &State) -> bool {
	pipeline.limits.iter().any(|id| state.paging[*id as usize].remaining == Some(0))
}

/// The paging `spec` names, its clauses evaluated in `ctx`.
async fn paging_of(
	program: &Program,
	spec: &super::program::LimitSpec,
	ctx: &ExecutionContext,
) -> FlowResult<Paging> {
	let remaining = match &spec.limit {
		Some(clause) => Some(paging_clause(program, clause, ctx, PagingClause::Limit).await?),
		None => None,
	};
	let skip = match &spec.start {
		Some(clause) => paging_clause(program, clause, ctx, PagingClause::Start).await?,
		None => 0,
	};
	Ok(Paging {
		skip,
		remaining,
	})
}

/// Where the rows a pipeline hands on go: the statement's stream, flushed a
/// batch at a time, or — for a statement a block runs within its program —
/// the state's output, gathered into the statement's value once it is done.
pub(super) enum Out<'o, 'y> {
	Stream(&'o mut Yielder<'y, ValueBatch>),
	/// The rows flushed so far. A statement a signal interrupts keeps these,
	/// and not the rows its state still held, as a stream's reader would.
	Gather(Vec<Value>),
	/// A reader taking each batch as it is flushed ([`RowSink`]).
	Sink(&'o mut (dyn RowSink + Send)),
}

/// `make`'s future, built in a frame of its own and boxed.
///
/// In an unoptimised build, an `.await` of a future built in place reserves
/// the future's whole size in the awaiting function's frame, once per call
/// site, and the host's generators and step loop, the planner and the
/// lowering have many. Built here, the caller holds only the box, which keeps
/// the native stack each poll of a program, and each step of planning one,
/// takes small: the lowering recurses through itself, and a frame holding a
/// copy of its own future per call site grows the stack by that much at
/// every level.
///
/// The box is polled through [`Polled::poll`], which is never inlined, so the
/// body of `F` is generated once, in that function, rather than inlined into
/// the state machine of every caller that awaits it.
#[inline(never)]
pub(crate) fn out_of_line<F: std::future::Future>(make: impl FnOnce() -> F) -> Polled<F> {
	Polled(Box::pin(make()))
}

/// `make`'s future, built in a frame of its own and boxed, for a caller that
/// coerces the box to a boxed trait object: that box is polled through its
/// vtable, so it needs no [`Polled`] wrapper.
#[inline(never)]
pub(crate) fn out_of_line_boxed<F: std::future::Future>(
	make: impl FnOnce() -> F,
) -> std::pin::Pin<Box<F>> {
	Box::pin(make())
}

/// A boxed future polled through a call that is never inlined ([`out_of_line`]).
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub(crate) struct Polled<F>(std::pin::Pin<Box<F>>);

impl<F: std::future::Future> std::future::Future for Polled<F> {
	type Output = F::Output;

	#[inline(never)]
	fn poll(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<F::Output> {
		self.0.as_mut().poll(cx)
	}
}

/// Run a pipeline over the rows of a source in storage: a table, a record id,
/// or each source of a union in written order. Answers whether the pipeline
/// stopped, taking no more rows, so that a union opens no later source.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_storage_source(
	program: &Arc<Program>,
	pipeline: &Pipeline,
	source: &Source,
	version: Option<u64>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<bool> {
	let db_ctx = ctx.database().context("the VM requires a database context")?;
	// A write reading its subjects begins its read of each table the source
	// opens before any row of it runs.
	if let Some(write) = state.write.as_deref_mut() {
		let mut tables = Vec::new();
		super::write::opened_tables(source, &mut tables);
		for table in tables {
			write.read_table(table)?;
		}
	}
	match source {
		Source::Scan {
			table,
			direction,
			range: key_range,
			..
		} => {
			let descriptor = crate::exec::vm::descriptor::table_at(ctx, table, version)
				.await
				.map_err(ControlFlow::Err)?;
			if absence_hidden(&descriptor, db_ctx)? {
				return Ok(false);
			}
			let def = descriptor.defined().map_err(ControlFlow::Err)?;
			refuse_lightweight(table, def)?;
			let range = RecordPrefix {
				ns: db_ctx.ns_ctx.ns.namespace_id,
				db: db_ctx.db.database_id,
				tb: Cow::Borrowed(table),
			}
			.range()?;
			match key_range {
				None => {
					let gate = descriptor.row_gate().map_err(ControlFlow::Err)?;
					scan_range(
						program,
						pipeline,
						range,
						gate,
						descriptor.replay(),
						*direction,
						version,
						state,
						ctx,
						yielder,
						flush_at,
						None,
					)
					.await
				}
				Some(key_range) => {
					out_of_line(|| {
						scan_key_ranges(
							program,
							pipeline,
							&descriptor,
							table,
							key_range,
							range,
							*direction,
							version,
							state,
							ctx,
							yielder,
							flush_at,
						)
					})
					.await
				}
			}
		}
		Source::Vacant {
			table,
		} => {
			require_defined(ctx, table, version).await?;
			Ok(false)
		}
		Source::Record {
			table,
			record,
			for_update,
		} => {
			let descriptor = crate::exec::vm::descriptor::table_at(ctx, table, version)
				.await
				.map_err(ControlFlow::Err)?;
			// A list stands for the scan of the table it names records of, so an absent table
			// reads as empty to a caller from whom the absence is hidden, as the scan does
			// ([`absence_hidden`]); a record read by its id raises the absence.
			let def = match descriptor.defined() {
				Ok(def) => def,
				Err(error) if absence_hidden(&descriptor, db_ctx)? => {
					let val = host_scalar(program, record, ctx, None).await?;
					return if val.is_array() {
						Ok(false)
					} else {
						Err(ControlFlow::Err(error))
					};
				}
				Err(error) => return Err(ControlFlow::Err(error)),
			};
			match host_scalar(program, record, ctx, None).await? {
				// A locked read is of one record, and of no historical version.
				Value::RecordId(rid) if *for_update && rid.key.is_range() => {
					Err(ControlFlow::Err(anyhow::Error::new(crate::exec::Error::Query {
						message: crate::dbs::FOR_UPDATE_TARGETS_ERROR.to_string(),
					})))
				}
				Value::RecordId(_) if *for_update && version.is_some() => {
					Err(ControlFlow::Err(anyhow::Error::new(crate::exec::Error::Query {
						message: crate::dbs::FOR_UPDATE_VERSION_ERROR.to_string(),
					})))
				}
				// A range reads its records in key order, as a scan.
				Value::RecordId(rid) if rid.key.is_range() => {
					refuse_lightweight(table, def)?;
					let RecordIdKey::Range(bounds) = &rid.key else {
						unreachable!("checked a range above")
					};
					let range = record_key_range(
						db_ctx.ns_ctx.ns.namespace_id,
						db_ctx.db.database_id,
						table,
						bounds,
					)?;
					// A write over the records of a range begins reading them
					// once its bounds are known, so a record evaluating them
					// created is one the read begins with.
					if let Some(write) = state.write.as_deref_mut() {
						write.read_table(table)?;
					}
					let gate = descriptor.row_gate().map_err(ControlFlow::Err)?;
					scan_range(
						program,
						pipeline,
						range,
						gate,
						descriptor.replay(),
						crate::kvs::Direction::Forward,
						version,
						state,
						ctx,
						yielder,
						flush_at,
						None,
					)
					.await
				}
				// A record that does not exist is no row.
				Value::RecordId(rid) => {
					let row = if *for_update {
						crate::exec::operators::fetch::fetch_raw_record_for_update(ctx, &rid)
							.await?
					} else {
						fetch_raw_record(ctx, &rid, version).await?
					};
					let Some(row) = row else {
						return Ok(false);
					};
					let row = write_subject_row(state.write.is_some(), &rid, row);
					let named = state.write.as_deref_mut().map(|write| write.row_named(true));
					let step = run_value_row(program, pipeline, row, state, ctx).await;
					if let (Some(named), Some(write)) = (named, state.write.as_deref_mut()) {
						write.row_named(named);
					}
					let step = step?;
					flush(state, yielder, flush_at, ctx).await?;
					Ok(step == Ended::Stop)
				}
				// The records a list names, in its order, each run as the record of a record id:
				// the planner reads `id IN <list>` this way, the list holding record ids of the
				// table in key order. A record that does not exist is no row.
				Value::Array(records) if !*for_update => {
					refuse_lightweight(table, def)?;
					for record in records.iter() {
						super::fuel::tick(ctx, 1).await?;
						let Value::RecordId(rid) = record else {
							return Err(ControlFlow::Err(anyhow::anyhow!(
								"a record source listed {record:?}, not a record id"
							)));
						};
						if rid.key.is_range() {
							return Err(ControlFlow::Err(anyhow::anyhow!(
								"a record source listed the range {rid:?}"
							)));
						}
						let Some(row) = fetch_raw_record(ctx, rid, version).await? else {
							continue;
						};
						let row = write_subject_row(state.write.is_some(), rid, row);
						let named = state.write.as_deref_mut().map(|write| write.row_named(true));
						let step = run_value_row(program, pipeline, row, state, ctx).await;
						if let (Some(named), Some(write)) = (named, state.write.as_deref_mut()) {
							write.row_named(named);
						}
						let stop = step? == Ended::Stop;
						flush(state, yielder, flush_at, ctx).await?;
						if stop {
							return Ok(true);
						}
					}
					Ok(false)
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
				| Value::File(_)
				| Value::Regex(_)
				| Value::Range(_)
				| Value::Closure(_)) => Err(ControlFlow::Err(anyhow::anyhow!(
					"a record source evaluated to {other:?}, not a record id"
				))),
			}
		}
		// An index lists record ids in its key order; each record is read and
		// run, and an id whose record is gone is no row, as the index scan
		// operator reads them.
		Source::Index {
			index,
			access,
			direction,
			decode,
		} => {
			let distinct = access_repeats_records(index, access);
			let id_only = crate::exec::index::access_path::index_scan_is_id_only(
				distinct,
				access,
				version.is_some(),
				decode.as_ref(),
			);
			let mut records = IndexRecords::open(
				ctx,
				index,
				access,
				*direction,
				version,
				distinct,
				decode.as_ref(),
			)?
			.with_id_only(id_only);
			let paged = super::morsel::body_breakers(&pipeline.code).paged;
			if !id_only && index_runs_in_place(pipeline, state) {
				return index_in_place(
					program,
					pipeline,
					&mut records,
					paged,
					state,
					ctx,
					yielder,
					flush_at,
				)
				.await;
			}
			let mut rows = Rows::of(program, pipeline, ctx);
			loop {
				records.paged = rows.wanted(paged, state);
				let row = match records.next(ctx).await {
					Ok(Some(row)) => row,
					Ok(None) => break,
					Err(e) => return rows.fail(e, state, ctx, yielder, flush_at).await,
				};
				if rows.row(program, pipeline, row, state, ctx, yielder, flush_at).await? {
					return Ok(true);
				}
			}
			rows.finish(state, ctx, yielder, flush_at).await
		}
		// The records the index lists under the key, where it lists every
		// record whose field equals the key; otherwise the table whole. The
		// index is the one the plan read from the catalog under the
		// snapshot this run reads.
		Source::Lookup {
			table,
			index,
			outer,
		} => {
			let descriptor = crate::exec::vm::descriptor::table_at(ctx, table, version)
				.await
				.map_err(ControlFlow::Err)?;
			let def = descriptor.defined().map_err(ControlFlow::Err)?;
			refuse_lightweight(table, def)?;
			let key = match outer {
				Some(outer) => super::nested::key_at(ctx.value("parent"), outer).cloned(),
				None => state.lookup_key.clone(),
			};
			let key = key.filter(|key| version.is_none() && lookup_key(key));
			let Some(key) = key else {
				let range = RecordPrefix {
					ns: db_ctx.ns_ctx.ns.namespace_id,
					db: db_ctx.db.database_id,
					tb: Cow::Borrowed(table),
				}
				.range()?;
				let gate = descriptor.row_gate().map_err(ControlFlow::Err)?;
				return scan_range(
					program,
					pipeline,
					range,
					gate,
					descriptor.replay(),
					crate::kvs::Direction::Forward,
					version,
					state,
					ctx,
					yielder,
					flush_at,
					None,
				)
				.await;
			};
			let access = crate::exec::index::access_path::BTreeAccess::literal_equality(key);
			let mut records = IndexRecords::open(
				ctx,
				index,
				&access,
				crate::kvs::Direction::Forward,
				version,
				false,
				None,
			)?;
			if index_runs_in_place(pipeline, state) {
				return index_in_place(
					program,
					pipeline,
					&mut records,
					None,
					state,
					ctx,
					yielder,
					flush_at,
				)
				.await;
			}
			let mut rows = Rows::of(program, pipeline, ctx);
			loop {
				let row = match records.next(ctx).await {
					Ok(Some(row)) => row,
					Ok(None) => break,
					Err(e) => return rows.fail(e, state, ctx, yielder, flush_at).await,
				};
				if rows.row(program, pipeline, row, state, ctx, yielder, flush_at).await? {
					return Ok(true);
				}
			}
			rows.finish(state, ctx, yielder, flush_at).await
		}
		// Each record once, as the union index operator combines its
		// branches: drained in branch order, or merged by the key each branch
		// is sorted by, the earlier branch first between equal keys. A branch
		// that reaches a record through several entries lists it once, below
		// the merge, unless the merge already admits each record once.
		Source::IndexUnion {
			branches,
			merge,
			decode,
			topk,
		} => {
			out_of_line(|| async move {
				let branch_distinct =
					matches!(merge, Some(MergeMode::ById(_) | MergeMode::ByIndexKey { .. }));
				// A merge by id follows the order its branches list records in: an
				// index spelling its entries' ids as identities lists the records of
				// one value as a table scan does, one stamped earlier by their ids'
				// values.
				let by_encoding = branches
					.iter()
					.all(|b| crate::catalog::spells_identity_entry_ids(b.index.format_version));
				// Each full-text branch's clause context, shared with the program's calls on it.
				let mut contexts: Vec<Option<super::interp::ScanMatch>> = branches
					.iter()
					.map(|b| match &b.read {
						super::payload::BranchRead::FullText {
							query,
							..
						} => {
							let definition = b.index.definition();
							state.scan_match_context(
								program,
								&definition.table_name,
								definition,
								query,
							)
						}
						super::payload::BranchRead::BTree(_) => None,
					})
					.collect();
				// Where the sort above orders by a sum of the branches' clause scores, every
				// full-text branch passes over the hits whose sum cannot enter its top k. Every
				// clause of the sum is scored in its own context, read before any branch lists
				// a hit; a clause no call scores leaves the branches rejecting nothing.
				let union_scores = match topk {
					Some(topk) => {
						let mut scored = Vec::new();
						topk.sum.clauses(&mut scored);
						let clauses = scored
							.iter()
							.map(|clause| {
								branches.iter().zip(&contexts).find_map(|(b, context)| {
									let super::payload::BranchRead::FullText {
										query,
										operator,
									} = &b.read
									else {
										return None;
									};
									let context =
										context.as_ref().filter(|context| context.scored)?;
									(operator.rf == Some(*clause)).then(|| {
										super::fulltext::UnionClause {
											clause: *clause,
											table: b.index.definition().table_name.clone(),
											index: b.index.clone(),
											query: query.clone(),
											context: std::sync::Arc::clone(&context.context),
										}
									})
								})
							})
							.collect::<Option<Vec<_>>>();
						let cell = state.topk_cells.get(topk.scan.sort as usize).cloned().flatten();
						match (clauses, cell) {
							(Some(clauses), Some(cell)) => {
								Some(std::sync::Arc::new(super::fulltext::UnionScores {
									cell,
									direction: topk.scan.direction,
									single_key: topk.scan.single_key,
									sum: topk.sum.clone(),
									clauses,
								}))
							}
							_ => None,
						}
					}
					None => None,
				};
				if let Some(union_scores) = &union_scores {
					let services = EngineServices::new(ctx);
					if let Err(e) = union_scores.prepare(ctx, &services).await {
						let mut rows = Rows::of(program, pipeline, ctx);
						return rows.fail(e, state, ctx, yielder, flush_at).await;
					}
				}
				if merge.is_none()
					&& !branches.is_empty()
					&& branches.iter().all(|b| {
						matches!(b.index.definition().index, crate::catalog::Index::Spatial)
							&& matches!(b.read, super::payload::BranchRead::BTree(_))
					}) {
					// Point-only `SPATIAL` index union branches over the same index with
					// strictly disjoint ascending integer cell ranges contain at most one entry
					// per record across all branches, so cross-branch `HashSet` deduplication is
					// unnecessary; when only `id` is needed at the current version, rows can also
					// be constructed directly from the index's `RecordId`s.
					let disjoint_point_ranges =
						crate::exec::index::access_path::disjoint_spatial_point_union_filter(
							branches.iter().map(|b| {
								let access = match &b.read {
									super::payload::BranchRead::BTree(access) => Some(&**access),
									super::payload::BranchRead::FullText {
										..
									} => None,
								};
								(b.index.definition().index_id, access)
							}),
						)
						.is_some();
					let scans = branches
						.iter()
						.map(|b| match &b.read {
							super::payload::BranchRead::BTree(access) => {
								super::entries::EntryScan::open(ctx, &b.index, access, b.direction)
							}
							super::payload::BranchRead::FullText {
								..
							} => unreachable!("checked all BTree"),
						})
						.collect::<FlowResult<Vec<_>>>()?;
					let id_only = crate::exec::index::access_path::spatial_union_is_id_only(
						disjoint_point_ranges,
						version.is_some(),
						decode.as_ref(),
					);
					let mut records = IndexRecords::chain(
						ctx,
						scans,
						version,
						!disjoint_point_ranges,
						decode.as_ref(),
						id_only,
					);
					let mut rows = Rows::of(program, pipeline, ctx);
					let paged = super::morsel::body_breakers(&pipeline.code).paged;
					loop {
						records.paged = rows.wanted(paged, state);
						let row = match records.next(ctx).await {
							Ok(Some(row)) => row,
							Ok(None) => break,
							Err(e) => return rows.fail(e, state, ctx, yielder, flush_at).await,
						};
						if rows.row(program, pipeline, row, state, ctx, yielder, flush_at).await? {
							return Ok(true);
						}
					}
					return rows.finish(state, ctx, yielder, flush_at).await;
				}
				// The first batch of a branch rejecting by the sum is as large as the sort keeps.
				let first_batch = topk
					.as_ref()
					.and_then(|topk| program.sorts.get(topk.scan.sort as usize)?.keep)
					.map_or(FULL_TEXT_BATCH_SIZE, |keep| keep.clamp(1, FULL_TEXT_BATCH_SIZE));
				let mut branches = branches
					.iter()
					.zip(contexts.iter_mut())
					.map(|(b, context)| match &b.read {
						super::payload::BranchRead::BTree(access) => {
							let distinct =
								branch_distinct && access_repeats_records(&b.index, access);
							IndexRecords::open(
								ctx,
								&b.index,
								access,
								b.direction,
								version,
								distinct,
								decode.as_ref(),
							)
						}
						super::payload::BranchRead::FullText {
							query,
							operator,
						} => Ok(IndexRecords::full_text(
							ctx,
							&b.index,
							query,
							operator,
							context.take(),
							version,
							decode.as_ref(),
							union_scores.clone(),
							first_batch,
						)),
					})
					.collect::<FlowResult<Vec<_>>>()?;
				let mut rows = Rows::of(program, pipeline, ctx);
				// A failure to read is raised after the batches before it merge.
				macro_rules! read {
					($read:expr) => {
						match $read {
							Ok(value) => value,
							Err(e) => return rows.fail(e, state, ctx, yielder, flush_at).await,
						}
					};
				}
				// A record is passed once whichever branches list it, told apart by
				// the record it addresses (`RecordAddress`), not by `RecordId`'s
				// equality, under which `k:[1]` and `k:[1dec]` are one.
				let Some(merge) = merge else {
					let mut seen = HashSet::new();
					for branch in &mut branches {
						while let Some(row) = read!(branch.next(ctx).await) {
							if let Some(rid) = record_id(&row)
								&& !seen.insert(crate::val::RecordAddress(rid))
							{
								continue;
							}
							if rows
								.row(program, pipeline, row, state, ctx, yielder, flush_at)
								.await?
							{
								return Ok(true);
							}
						}
					}
					return rows.finish(state, ctx, yielder, flush_at).await;
				};
				let descending = matches!(merge.direction(), SortDirection::Desc);
				let mut last = None;
				let mut seen = HashSet::new();
				loop {
					let mut best: Option<(usize, MergeKey)> = None;
					for (i, branch) in branches.iter_mut().enumerate() {
						// A row with no key is passed over, as the union
						// operator passes over it.
						let Some(key) = read!(branch.peek(ctx).await)
							.and_then(|row| merge_key(merge, row, by_encoding))
						else {
							continue;
						};
						let better = best.as_ref().is_none_or(|(_, prev)| {
							let ordering = key.cmp(prev);
							if descending {
								ordering.is_gt()
							} else {
								ordering.is_lt()
							}
						});
						if better {
							best = Some((i, key));
						}
					}
					let Some((i, key)) = best else {
						return rows.finish(state, ctx, yielder, flush_at).await;
					};
					let row =
						read!(branches[i].next(ctx).await).expect("the branch's head was peeked");
					// Merged by id, a record listed by several branches arrives
					// once per branch in succession.
					if let MergeKey::Rid(rid) = &key {
						if last.as_ref() == Some(&rid.address) {
							continue;
						}
						last = Some(rid.address.clone());
					}
					if matches!(merge, MergeMode::ByIndexKeyDedup { .. })
						&& let Some(rid) = record_id(&row)
						&& !seen.insert(crate::val::RecordAddress(rid))
					{
						continue;
					}
					if rows.row(program, pipeline, row, state, ctx, yielder, flush_at).await? {
						return Ok(true);
					}
				}
			})
			.await
		}
		// The doc-IDs a fused plan leaves, resolved and read in batches as the
		// bitmap resolve operator reads them; on an overflow, its fallback.
		Source::Bitmap {
			table,
			root,
			fallback,
			decode,
		} => {
			let services = EngineServices::new(ctx);
			let bitmap = match services.evaluate_bitmap(table, root, fallback.is_none()).await? {
				FusedCandidates::Bitmap(bitmap) => bitmap,
				FusedCandidates::Overflow => {
					let Some(fallback) = fallback else {
						return Err(ControlFlow::Err(anyhow::anyhow!(
							"an anchored bitmap plan overflowed its branch budget"
						)));
					};
					return out_of_line(|| {
						run_storage_source(
							program, pipeline, fallback, version, state, ctx, yielder, flush_at,
						)
					})
					.await;
				}
			};
			let mut doc_ids = bitmap.into_iter();
			let mut rows = Rows::of(program, pipeline, ctx);
			macro_rules! read {
				($read:expr) => {
					match $read {
						Ok(value) => value,
						Err(e) => return rows.fail(e, state, ctx, yielder, flush_at).await,
					}
				};
			}
			// The first read is a probe and each later one is sized from the
			// stored bytes of the read before it; the records each read finds
			// are decoded a byte budget at a time.
			let mut reader = RecordReader::stored(ctx, RESOLVE_BATCH_SIZE, None, decode.clone());
			loop {
				if reader.is_drained() {
					let chunk: Vec<u64> = doc_ids.by_ref().take(reader.read_len()).collect();
					if chunk.is_empty() {
						return rows.finish(state, ctx, yielder, flush_at).await;
					}
					reader.push(read!(services.resolve_doc_ids(table, &chunk).await));
					// The ids resolved are a unit each, whether or not their
					// records still exist; each record read is a row of its own.
					read!(super::fuel::tick(ctx, super::fuel::units(chunk.len())).await);
				}
				let records = read!(reader.next_rows(ctx).await);
				for row in records {
					if rows.row(program, pipeline, row, state, ctx, yielder, flush_at).await? {
						return Ok(true);
					}
				}
			}
		}
		// The hits of a full-text query, read a batch at a time: the table's
		// permission and fields are the program's, applied to the rows.
		Source::FullText {
			table,
			index,
			query,
			operator,
			decode,
			topk,
		} => {
			let services = EngineServices::new(ctx);
			let mut rows = Rows::of(program, pipeline, ctx);
			macro_rules! read {
				($read:expr) => {
					match $read {
						Ok(value) => value,
						Err(e) => return rows.fail(e, state, ctx, yielder, flush_at).await,
					}
				};
			}
			// The calls of the program that name this scan's clause read the
			// terms the scan reads, through the context they share.
			let shared = state.scan_match_context(program, table, index.definition(), query);
			let Some(mut listing) = read!(
				super::fulltext::candidates(
					ctx,
					&services,
					table,
					index,
					query,
					operator,
					shared.as_ref().map(|shared| &*shared.context)
				)
				.await
			) else {
				return rows.finish(state, ctx, yielder, flush_at).await;
			};
			let mut reader = RecordReader::stored(ctx, RESOLVE_BATCH_SIZE, version, decode.clone());
			// A scan passing hits over by the sort's threshold starts with batches as
			// large as the sort keeps and doubles them, so it reads the threshold
			// often while it is still loose, which is when it is tightening fastest.
			let mut limit = topk
				.as_ref()
				.and_then(|topk| program.sorts.get(topk.sort as usize)?.keep)
				.map_or(FULL_TEXT_BATCH_SIZE, |keep| keep.clamp(1, FULL_TEXT_BATCH_SIZE));
			loop {
				// The hits whose score cannot enter the sort's top k are passed
				// over before anything else is read of them.
				let threshold = topk.as_ref().and_then(|topk| {
					let cell = state.topk_cells.get(topk.sort as usize)?.as_ref()?;
					Some(super::fulltext::Threshold::Single(topk, cell.snapshot()?))
				});
				// Until the sort publishes a threshold, a batch stays as large as the
				// sort keeps, so it publishes one after as few records as it can.
				let batch = limit;
				if threshold.is_some() {
					limit = limit.saturating_mul(2).min(FULL_TEXT_BATCH_SIZE);
				}
				let Some(rids) = read!(
					super::fulltext::next_records(
						ctx,
						&mut listing,
						batch,
						shared.as_ref(),
						index.definition().index_id,
						threshold,
					)
					.await
				) else {
					return rows.finish(state, ctx, yielder, flush_at).await;
				};
				if rids.is_empty() {
					continue;
				}
				reader.push(rids);
				while !reader.is_drained() {
					let records = read!(reader.next_rows(ctx).await);
					for row in records {
						if rows.row(program, pipeline, row, state, ctx, yielder, flush_at).await? {
							return Ok(true);
						}
					}
				}
			}
		}
		// The nearest records of a vector, nearest first: the table's
		// permission and fields are the program's, applied to the rows.
		Source::Knn {
			table,
			index,
			vector,
			vector_query,
			k,
			ef,
			slot,
			gate,
			ids_only,
		} => {
			let mut rows = Rows::of(program, pipeline, ctx);
			macro_rules! read {
				($read:expr) => {
					match $read {
						Ok(value) => value,
						Err(e) => return rows.fail(e, state, ctx, yielder, flush_at).await,
					}
				};
			}
			// A vector computed when the statement runs is evaluated as the
			// source opens, before the search reads anything.
			let vector = match vector_query {
				Some(query) => read!(evaluate_knn_search(program, query, state, ctx).await),
				None => Arc::clone(vector),
			};
			let candidates = read!(
				super::knn::search(
					program,
					ctx,
					table,
					index,
					&vector,
					*k,
					*ef,
					version,
					gate.as_ref(),
				)
				.await
			);
			// The distances are published before any record is yielded, so
			// a projection reading them finds them.
			if let Some(slot) = slot {
				state
					.knn_context(*slot)
					.insert_all(
						candidates.iter().map(|(rid, distance)| {
							(rid.clone(), crate::val::Number::Float(*distance))
						}),
					)
					.await;
			}
			let rids: Vec<crate::val::RecordId> =
				candidates.into_iter().map(|(rid, _distance)| rid).collect();
			// A read at the current version, the one the index describes,
			// returns ids of records that exist in the transaction's snapshot;
			// a row nothing reads more of than its id is built from the id.
			if *ids_only && version.is_none() {
				for rid in rids {
					// A single entry is sorted by construction.
					let row = crate::val::Object(crate::VecMap::from_sorted_vec_unchecked(vec![(
						"id".into(),
						Value::RecordId(rid),
					)]));
					if rows
						.row(program, pipeline, Value::Object(row), state, ctx, yielder, flush_at)
						.await?
					{
						return Ok(true);
					}
				}
				return rows.finish(state, ctx, yielder, flush_at).await;
			}
			let mut reader = RecordReader::new(
				ctx,
				RESOLVE_BATCH_SIZE,
				version,
				crate::kvs::CachePolicy::ReadWrite,
			);
			reader.push(rids);
			while !reader.is_drained() {
				let records = read!(reader.next_rows(ctx).await);
				for row in records {
					if rows.row(program, pipeline, row, state, ctx, yielder, flush_at).await? {
						return Ok(true);
					}
				}
			}
			rows.finish(state, ctx, yielder, flush_at).await
		}
		// The edges of a lightweight relation, walked over adjacency: each
		// edge's record is synthesized from its id, as a stored record is
		// read.
		Source::LightweightEdges {
			table,
			record,
			direction,
		} => {
			let descriptor = crate::exec::vm::descriptor::table_at(ctx, table, version)
				.await
				.map_err(ControlFlow::Err)?;
			let def = descriptor.defined().map_err(ControlFlow::Err)?;
			if crate::kvs::lightweight::lightweight_relation(&def.table_type).is_none() {
				return Err(ControlFlow::Err(anyhow::anyhow!(
					"`{table}` was planned as a lightweight relation and is not one"
				)));
			}
			let wanted = match record {
				None => None,
				Some(record) => match host_scalar(program, record, ctx, None).await? {
					Value::RecordId(rid) if rid.key.is_range() => Some(rid.key),
					// An edge named by its id is read as a record is.
					Value::RecordId(rid) => {
						let Some(row) = fetch_raw_record(ctx, &rid, version).await? else {
							return Ok(false);
						};
						let named = state.write.as_deref_mut().map(|write| write.row_named(true));
						let step = run_value_row(program, pipeline, row, state, ctx).await;
						if let (Some(named), Some(write)) = (named, state.write.as_deref_mut()) {
							write.row_named(named);
						}
						let step = step?;
						flush(state, yielder, flush_at, ctx).await?;
						return Ok(step == Ended::Stop);
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
					| Value::File(_)
					| Value::Regex(_)
					| Value::Range(_)
					| Value::Closure(_)) => {
						return Err(ControlFlow::Err(anyhow::anyhow!(
							"a record source evaluated to {other:?}, not a record id"
						)));
					}
				},
			};
			// A write over the edges of a range begins reading them once its
			// bounds are known, as it reads a record range.
			if wanted.is_some()
				&& let Some(write) = state.write.as_deref_mut()
			{
				write.read_table(table)?;
			}
			let services = EngineServices::new(ctx);
			let mut edges = services.scan_lightweight_edges(table, wanted, *direction, version);
			let mut rows = Rows::of(program, pipeline, ctx);
			while let Some(batch) = edges.next().await {
				let rids = match batch {
					Ok(rids) => rids,
					Err(e) => return rows.fail(e, state, ctx, yielder, flush_at).await,
				};
				for rid in rids {
					if let Err(e) = super::fuel::tick(ctx, 1).await {
						return rows.fail(e, state, ctx, yielder, flush_at).await;
					}
					let row = match services.read_record(&rid, version).await {
						Ok(Some(row)) => row,
						Ok(None) => continue,
						Err(e) => return rows.fail(e, state, ctx, yielder, flush_at).await,
					};
					if rows.row(program, pipeline, row, state, ctx, yielder, flush_at).await? {
						return Ok(true);
					}
				}
			}
			rows.finish(state, ctx, yielder, flush_at).await
		}
		Source::Union(sources) => {
			for source in sources {
				// A source after the limit is reached is not read: the
				// limit is judged before each one is opened.
				let spent = limits_spent(pipeline, state);
				let stopped = if source.reads_storage() {
					// Under a zero limit a read of storage reads no record,
					// unless it is a locked read, and a source that is not one
					// still yields its rows.
					let locked = matches!(
						source,
						Source::Record {
							for_update: true,
							..
						}
					);
					if (spent || state.zeroed) && !locked {
						continue;
					}
					out_of_line(|| {
						run_storage_source(
							program, pipeline, source, version, state, ctx, yielder, flush_at,
						)
					})
					.await?
				} else {
					out_of_line(|| {
						super::nested::run_source(
							program, pipeline, source, spent, state, ctx, yielder, flush_at,
						)
					})
					.await?
				};
				if stopped {
					return Ok(true);
				}
			}
			Ok(false)
		}
		Source::Sorted {
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
		| Source::Values {
			..
		}
		| Source::Nested {
			..
		}
		| Source::Probe {
			..
		}
		| Source::Count {
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
		| Source::Changes {
			..
		}
		| Source::Once => unreachable!("a source that is not a storage read"),
	}
}

/// The records an index lists, in its order: each batch of record ids is
/// read as the index scan operator reads it, an id whose record is gone
/// yielding nothing.
///
/// A scan that reaches a record through several entries (see
/// [`access_repeats_records`]) lists it once: the first entry to name it, in the
/// index's order, stands for the record. The repeats are dropped from the ids
/// before any record is read, so they cost no read.
/// Whether a B-tree equality on `key` lists every record whose field `=` the
/// key ([`Source::Lookup`]): a value keyed alike by every value equal to it,
/// whatever format the index was stamped at. An array is listed by its
/// elements. An index stamped below
/// [`crate::catalog::BTREE_NORMALISED_DECIMAL_KEYS_FORMAT_VERSION`] keys a
/// computed `2.50dec` apart from an equal `2.5dec` or `2.5f`, so a float, a
/// decimal and a record id with a composite key, which may hold either, are
/// read through the table. An integer is looked up on every index; one stamped
/// below that format misses a computed decimal equal to it (`2.0dec`) for every
/// read until it is rebuilt. A string equals a regex it matches, but no entry is
/// keyed by a regex.
pub(super) fn lookup_key(key: &Value) -> bool {
	match key {
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(crate::val::Number::Int(_))
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Bytes(_)
		| Value::String(_) => true,
		Value::RecordId(rid) => {
			matches!(
				rid.key,
				RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_)
			)
		}
		Value::Number(_)
		| Value::Array(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => false,
	}
}

struct IndexRecords {
	listing: Listing,
	reader: RecordReader,
	rows: std::collections::VecDeque<Value>,
	exhausted: bool,
	/// The ids listed so far, kept only by a scan that can name a record more
	/// than once.
	seen: Option<HashSet<crate::val::RecordAddress>>,
	/// Whether rows can be synthesized directly as `{ id: rid }` from the
	/// index's `RecordId`s without fetching records from storage.
	id_only: bool,
	/// The most rows the pipeline reading these records still takes under
	/// its own paging, so that a B-tree read asks for no more entries than
	/// that; `usize::MAX` where it pages nothing.
	paged: usize,
}

/// What an index lists.
enum Listing {
	/// The entries of a B-tree index under an access.
	BTree(super::entries::EntryScan),
	/// A sequence of B-tree entry scans drained in branch order into a shared
	/// batch.
	Chain {
		scans: Vec<super::entries::EntryScan>,
		branch_idx: usize,
	},
	/// The hits of a full-text query, opened when the first is asked for.
	FullText {
		index: crate::exec::index::access_path::IndexRef,
		query: String,
		operator: crate::expr::operator::MatchesOperator,
		/// The context of the clause the listing answers, shared with the
		/// program's calls that name it
		/// ([`super::interp::State::scan_match_context`]).
		shared: Option<super::interp::ScanMatch>,
		hits: Option<Option<Box<crate::exec::operators::scan::FullTextHits>>>,
		/// What the branch of an index union rejects hits by, where the sort above
		/// orders by a sum of the union's clause scores.
		union: Option<std::sync::Arc<super::fulltext::UnionScores>>,
		/// How many hits the next batch takes: as many as the sort keeps until it
		/// publishes a threshold, then twice the last, up to the full batch size.
		limit: usize,
	},
}

impl IndexRecords {
	/// The records of B-tree index `index` under `access`, each once when
	/// `distinct`.
	fn open(
		ctx: &ExecutionContext,
		index: &crate::exec::index::access_path::IndexRef,
		access: &crate::exec::index::access_path::BTreeAccess,
		direction: crate::kvs::Direction,
		version: Option<u64>,
		distinct: bool,
		decode: Option<&DecodeFields>,
	) -> FlowResult<Self> {
		Ok(Self {
			listing: Listing::BTree(super::entries::EntryScan::open(
				ctx, index, access, direction,
			)?),
			reader: RecordReader::stored(ctx, RESOLVE_BATCH_SIZE, version, decode.cloned()),
			rows: Default::default(),
			exhausted: false,
			seen: distinct.then(HashSet::new),
			id_only: false,
			paged: usize::MAX,
		})
	}

	fn with_id_only(mut self, id_only: bool) -> Self {
		self.id_only = id_only;
		self
	}

	/// A sequence of B-tree entry scans drained in branch order into one
	/// shared [`RecordReader`], deduplicating across branches when `distinct`.
	fn chain(
		ctx: &ExecutionContext,
		scans: Vec<super::entries::EntryScan>,
		version: Option<u64>,
		distinct: bool,
		decode: Option<&DecodeFields>,
		id_only: bool,
	) -> Self {
		Self {
			listing: Listing::Chain {
				scans,
				branch_idx: 0,
			},
			reader: RecordReader::stored(ctx, RESOLVE_BATCH_SIZE, version, decode.cloned()),
			rows: Default::default(),
			exhausted: false,
			seen: distinct.then(HashSet::new),
			id_only,
			paged: usize::MAX,
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn full_text(
		ctx: &ExecutionContext,
		index: &crate::exec::index::access_path::IndexRef,
		query: &str,
		operator: &crate::expr::operator::MatchesOperator,
		shared: Option<super::interp::ScanMatch>,
		version: Option<u64>,
		decode: Option<&DecodeFields>,
		union: Option<std::sync::Arc<super::fulltext::UnionScores>>,
		first_batch: usize,
	) -> Self {
		let limit = if union.is_some() {
			first_batch
		} else {
			FULL_TEXT_BATCH_SIZE
		};
		Self {
			listing: Listing::FullText {
				index: index.clone(),
				query: query.to_owned(),
				operator: operator.clone(),
				shared,
				hits: None,
				union,
				limit,
			},
			reader: RecordReader::stored(ctx, RESOLVE_BATCH_SIZE, version, decode.cloned()),
			rows: Default::default(),
			exhausted: false,
			seen: None,
			id_only: false,
			paged: usize::MAX,
		}
	}

	/// The next batch of record ids the index lists; empty once it has none.
	async fn next_rids(&mut self, ctx: &ExecutionContext) -> FlowResult<Vec<crate::val::RecordId>> {
		let services = EngineServices::new(ctx);
		match &mut self.listing {
			Listing::BTree(entries) => loop {
				let mut batch =
					entries.next_batch(ctx, &services, self.reader.read_len(), self.paged).await?;
				if batch.is_empty() {
					return Ok(Vec::new());
				}
				if let Some(seen) = &mut self.seen {
					batch.retain(|rid| seen.insert(crate::val::RecordAddress(rid.clone())));
					if batch.is_empty() {
						continue;
					}
				}
				return Ok(batch);
			},
			Listing::Chain {
				scans,
				branch_idx,
			} => {
				let target = self.reader.read_len().min(self.paged.max(1));
				let mut batch = Vec::new();
				while *branch_idx < scans.len() && batch.len() < target {
					let want = target - batch.len();
					let next =
						scans[*branch_idx].next_batch(ctx, &services, want, self.paged).await?;
					if next.is_empty() {
						*branch_idx += 1;
						continue;
					}
					if let Some(seen) = &mut self.seen {
						batch.extend(
							next.into_iter()
								.filter(|rid| seen.insert(crate::val::RecordAddress(rid.clone()))),
						);
					} else {
						batch.extend(next);
					}
				}
				Ok(batch)
			}
			Listing::FullText {
				index,
				query,
				operator,
				shared,
				hits,
				union,
				limit,
			} => {
				if hits.is_none() {
					let table = index.definition().table_name.clone();
					*hits = Some(
						super::fulltext::candidates(
							ctx,
							&services,
							&table,
							index,
							query,
							operator,
							shared.as_ref().map(|shared| &*shared.context),
						)
						.await?
						.map(Box::new),
					);
				}
				let Some(listing) = hits.as_mut().and_then(Option::as_mut) else {
					return Ok(Vec::new());
				};
				// A batch the listing hands on empty is not the end of the hits: only the
				// end of the hits ends the listing.
				loop {
					// A branch rejecting by the sum reads the sort's threshold each batch,
					// with batches doubling from the sort's size once it has one.
					let threshold = union.as_deref().and_then(|union| {
						Some(super::fulltext::Threshold::Sum(
							union,
							union.cell.snapshot()?,
							operator.rf,
						))
					});
					let batch = *limit;
					if threshold.is_some() {
						*limit = limit.saturating_mul(2).min(FULL_TEXT_BATCH_SIZE);
					}
					let next = super::fulltext::next_records(
						ctx,
						listing,
						batch,
						shared.as_ref(),
						index.definition().index_id,
						threshold,
					)
					.await?;
					match next {
						Some(rids) if rids.is_empty() => continue,
						Some(rids) => return Ok(rids),
						None => return Ok(Vec::new()),
					}
				}
			}
		}
	}

	/// The next record, without taking it.
	async fn peek(&mut self, ctx: &ExecutionContext) -> FlowResult<Option<&Value>> {
		while self.rows.is_empty() && !self.exhausted {
			if self.id_only {
				let batch = self.next_rids(ctx).await?;
				if batch.is_empty() {
					self.exhausted = true;
					break;
				}
				self.rows.extend(batch.into_iter().map(|rid| {
					Value::Object(crate::val::Object(crate::VecMap::from_sorted_vec_unchecked(
						vec![("id".into(), Value::RecordId(rid))],
					)))
				}));
				break;
			}
			if self.reader.is_drained() {
				let batch = self.next_rids(ctx).await?;
				if batch.is_empty() {
					self.exhausted = true;
					break;
				}
				self.reader.push(batch);
			}
			let rows = self.reader.next_rows(ctx).await?;
			// The records read are a unit each, which a batch whose records
			// are all gone spends too.
			super::fuel::tick(ctx, super::fuel::units(rows.len()).max(1)).await?;
			self.rows.extend(rows);
		}
		Ok(self.rows.front())
	}

	/// The next record.
	async fn next(&mut self, ctx: &ExecutionContext) -> FlowResult<Option<Value>> {
		self.peek(ctx).await?;
		Ok(self.rows.pop_front())
	}

	/// The next batch of records the index lists, undecoded, for a run whose body reads its rows
	/// on their stored bytes ([`index_in_place`]): `None` once the index lists no more. A batch
	/// holds at least one record. Not for a listing read as ids alone, nor once [`Self::peek`] has
	/// decoded rows.
	async fn next_stored(
		&mut self,
		ctx: &ExecutionContext,
	) -> FlowResult<Option<std::collections::VecDeque<(RecordId, ReadAheadRecord)>>> {
		while !self.exhausted {
			if self.reader.is_drained() {
				let batch = self.next_rids(ctx).await?;
				if batch.is_empty() {
					self.exhausted = true;
					break;
				}
				self.reader.push(batch);
			}
			let records = self.reader.next_stored(ctx).await?;
			// The records read are a unit each, which a batch whose records
			// are all gone spends too.
			super::fuel::tick(ctx, super::fuel::units(records.len()).max(1)).await?;
			if !records.is_empty() {
				return Ok(Some(records));
			}
		}
		Ok(None)
	}
}

/// Whether the records an index lists run `pipeline`'s body on their stored bytes, one after
/// another ([`index_in_place`]): where the body runs in place ([`runs_in_place`]) and the plan runs
/// the pipeline serially, or as morsels of batches the coordinator reads for a statement whose rows
/// leave encoded.
///
/// Such morsels are handed their records decoded, and hand back the rows they emit decoded, so the
/// coordinator decodes every record and encodes every row the morsels emit, and the morsels run the
/// body alone. Read in place, the coordinator runs the body and transcodes each record emitted
/// unchanged from its stored bytes, which costs it less than the decode and the encode it makes
/// either way, as the body that runs in place holds only field reads, comparisons, arithmetic and
/// the building of rows; so the run narrows the plan's morsels to one run in place. A body holding
/// a vector measure, whose work for a row may outweigh the row's decode, keeps its morsels. A merge
/// of morsels answers the rows of the serial run, so the narrowed run answers what the morsels
/// would.
fn index_runs_in_place(pipeline: &Pipeline, state: &State) -> bool {
	#[cfg(test)]
	if INDEX_DECODES.with(std::cell::Cell::get) {
		return false;
	}
	let in_place = runs_in_place(pipeline, state)
		&& match &pipeline.parallel {
			crate::exec::planner::placement::Parallel::Serial => true,
			crate::exec::planner::placement::Parallel::Morsels(plan) => {
				state.row_frame.is_some()
					&& !pipeline.code.iter().any(|instr| matches!(instr, Instr::Measure { .. }))
					&& plan.input == MorselInput::Batches
					&& plan.combine != crate::exec::planner::placement::Combine::OrderedApply
			}
		};
	#[cfg(test)]
	if in_place {
		INDEX_IN_PLACE.with(|runs| runs.set(runs.get() + 1));
	}
	in_place
}

#[cfg(test)]
thread_local! {
	/// Whether this thread's index reads decode their records even where their body runs in place,
	/// for tests that hold the two paths to the same answer.
	pub(super) static INDEX_DECODES: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
	/// How many index reads on this thread ran their records on their stored bytes.
	pub(super) static INDEX_IN_PLACE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Run `pipeline`, whose body runs in place ([`runs_in_place`]), over the records `records` lists,
/// each on its stored bytes as a scan of the table's range runs its records
/// ([`in_place_row_admitted`]), so a record the body emits into a frame unchanged is transcoded
/// from its bytes there and never decoded. Answers whether the pipeline takes no more rows.
///
/// Each stored record was admitted against its table's layouts as it was read
/// ([`ReadAhead::read`](crate::exec::operators::scan::common::ReadAhead::read)); a packed one is
/// read through the layouts that admitted it. A record stamped below its table's newest migration
/// step is decoded, brought through the steps it has not passed and run as a value, as is a record
/// with no stored value that the read resolved (a lightweight edge); a resolved record with no
/// data is no row. The records run in the order the index lists them, and the rows they leave are
/// handed on whenever they reach `flush_at`, after each record. The body runs on this task's
/// thread and charges its work to the thread's fuel; each record spends a unit besides, and the
/// run yields between records once the fuel is spent.
///
/// `paged` is the body's own paging clause, which bounds the entries the index reads; `None` reads
/// them unbounded.
#[allow(clippy::too_many_arguments)]
async fn index_in_place(
	program: &Program,
	pipeline: &Pipeline,
	records: &mut IndexRecords,
	paged: Option<u16>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<bool> {
	use surrealdb_kvs::key::KVKey;
	let db_ctx = ctx.database().context("the VM requires a database context")?;
	let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	// The migration steps of the table the previous record belongs to, which the next record of
	// that table reuses.
	let mut steps: Option<(crate::val::TableName, Option<Replay>)> = None;
	let stopped = 'scan: loop {
		records.paged = Rows::Serial.wanted(paged, state);
		let Some(batch) = records.next_stored(ctx).await? else {
			break false;
		};
		for (rid, record) in batch {
			let stop = match record {
				ReadAheadRecord::Stored(value, layouts) => {
					let replay = match &steps {
						Some((table, replay)) if *table == rid.table => replay.clone(),
						_ => {
							let replay = crate::exec::migrate::replay_for(
								ctx,
								&rid.table,
								ctx.version_stamp(),
							)
							.await?;
							steps = Some((rid.table.clone(), replay.clone()));
							replay
						}
					};
					let key = crate::key::schema::RecordKey {
						ns,
						db,
						tb: Cow::Borrowed(&rid.table),
						id: Cow::Borrowed(&rid.key),
					}
					.encode_key()
					.map_err(ControlFlow::Err)?;
					match replay.filter(|replay| !replay.current(&value)) {
						Some(replay) => {
							let mut row = decode_record_value(&key, &value, layouts.as_deref())
								.map_err(ControlFlow::Err)?;
							replay.apply(ctx, &mut row, stamp_of(&value)).await?;
							run_resolved(program, pipeline, row, state)?
						}
						None => scan_row(
							program,
							pipeline,
							None,
							None,
							None,
							&key,
							&value,
							layouts.as_deref(),
							state,
						)
						.map_err(ControlFlow::Err)?,
					}
				}
				ReadAheadRecord::Resolved(record) => {
					let row = match Arc::try_unwrap(record) {
						Ok(record) => record.data,
						Err(record) => record.data.clone(),
					};
					if row.is_none() {
						continue;
					}
					run_resolved(program, pipeline, row, state)?
				}
				ReadAheadRecord::Pending => {
					return Err(ControlFlow::Err(anyhow::anyhow!(
						crate::err::EngineError::Internal(
							"a read-ahead record was left unresolved".into()
						)
					)));
				}
			};
			// The rows are handed on as each reaches `flush_at`, as a run of decoded rows hands
			// them on, however many records the storage read took at once.
			flush(state, yielder, flush_at, ctx).await?;
			if stop {
				break 'scan true;
			}
			// Each record run spends a unit of fuel, as a decoded row does, on the meter the body
			// charged its own work to; once the meter is spent the run reaches a yield point,
			// which checks the statement's deadline and cancellation, before the next record.
			if super::fuel::charge(1) {
				out_of_line(|| super::fuel::yield_point(ctx)).await?;
			}
		}
		// A process past its memory threshold fails here rather than reading on.
		ensure_below_memory_threshold()?;
	};
	if let Some(ahead) = &mut state.link_ahead {
		ahead.clear();
	}
	state.frontier_rows.clear();
	Ok(stopped)
}

/// Run `pipeline`'s in-place body over `row`, a record decoded rather than read on its stored
/// bytes, and settle the row it leaves pending as the value. Answers whether the pipeline takes no
/// more rows.
fn run_resolved(
	program: &Program,
	pipeline: &Pipeline,
	row: Value,
	state: &mut State,
) -> FlowResult<bool> {
	let stop = finish_in_place(run_in_place(program, pipeline, &ValueRow(&row), state))
		.map_err(ControlFlow::Err)?;
	if state.pending.is_some() {
		state.settle(program, row);
	}
	Ok(stop)
}

/// Whether the table `descriptor` names is absent and the caller's reads are
/// permission-checked, so that the absence is not disclosed to it. Whatever
/// reads or writes a table by name answers an absent table with this verdict,
/// so that no statement tells a checked caller more of the catalog than its
/// scan does: a scan of the table by name, whether it feeds a read or a write,
/// reads as empty, as a scan of a table whose permission the caller fails.
fn absence_hidden(
	descriptor: &crate::exec::vm::descriptor::TableDescriptor,
	db_ctx: &crate::exec::DatabaseContext,
) -> FlowResult<bool> {
	if descriptor.definition.is_some() {
		return Ok(false);
	}
	crate::exec::permission::should_check_perms(db_ctx, crate::iam::Action::View)
		.map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))
}

/// Require `table`, which a write names and reads no record of, to be
/// defined: raise the error of an undefined table, except for a caller whose
/// reads are permission-checked, to whom the absence is hidden
/// ([`absence_hidden`]).
pub(super) async fn require_defined(
	ctx: &ExecutionContext,
	table: &crate::val::TableName,
	version: Option<u64>,
) -> FlowResult<()> {
	let db_ctx = ctx.database().context("the VM requires a database context")?;
	let descriptor = crate::exec::vm::descriptor::table_at(ctx, table, version)
		.await
		.map_err(ControlFlow::Err)?;
	if !absence_hidden(&descriptor, db_ctx)? {
		descriptor.defined().map_err(ControlFlow::Err)?;
	}
	Ok(())
}

/// Raise the internal error of a scan that was planned as a read of a record
/// range and finds `table` a lightweight relation, which has none.
fn refuse_lightweight(
	table: &crate::val::TableName,
	def: &crate::catalog::TableDefinition,
) -> FlowResult<()> {
	if crate::kvs::lightweight::lightweight_relation(&def.table_type).is_some() {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"`{table}` is a lightweight relation and was planned as a record range"
		)));
	}
	Ok(())
}

/// What an index union merges its branches by: a record's id ([`MergeRid`]),
/// or the value at the merge path.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum MergeKey {
	Rid(MergeRid),
	Field(Value),
}

/// A record id as a merge by id orders it: equal exactly when it names the
/// same record, so `k:[1]` and `k:[1dec]` stay two, and ordered as the merge's
/// branches list records — by the key's encoding where `by_encoding`
/// ([`crate::val::RecordIdKey::cmp_by_encoding`]), else by value with the
/// encoding breaking ties ([`crate::val::RecordIdKey::cmp_by_record_address`]).
struct MergeRid {
	address: crate::val::RecordAddress,
	by_encoding: bool,
}

impl PartialEq for MergeRid {
	fn eq(&self, other: &Self) -> bool {
		self.address == other.address
	}
}

impl Eq for MergeRid {}

impl PartialOrd for MergeRid {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for MergeRid {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		let (a, b) = (&self.address.0, &other.address.0);
		a.table.cmp(&b.table).then_with(|| {
			if self.by_encoding {
				a.key.cmp_by_encoding(&b.key)
			} else {
				a.key.cmp_by_record_address(&b.key)
			}
		})
	}
}

/// The key `row` merges by, as the union index operator reads it: a row with
/// no record id has no key when merging by id.
fn merge_key(merge: &MergeMode, row: &Value, by_encoding: bool) -> Option<MergeKey> {
	match merge {
		MergeMode::ById(_) => record_id(row).map(|rid| {
			MergeKey::Rid(MergeRid {
				address: crate::val::RecordAddress(rid),
				by_encoding,
			})
		}),
		MergeMode::ByIndexKey {
			path,
			..
		}
		| MergeMode::ByIndexKeyDedup {
			path,
			..
		} => Some(MergeKey::Field(path.extract(row).into_owned())),
	}
}

/// The record id a row carries at `id`.
fn record_id(row: &Value) -> Option<crate::val::RecordId> {
	let object = row.as_object()?;
	if let Some(Value::RecordId(rid)) = object.get("id") {
		Some(rid.clone())
	} else {
		None
	}
}

/// The probe of the top-k pushdown `pipeline`'s scan carries, armed with the
/// threshold cell of the sort that feeds on it in the run in progress; `None`
/// for a scan with no pushdown or whose sort holds no cell, and for a
/// key-range scan cut by the second of a datetime component, which reads no
/// stored bytes for the key ([`CoarseGate`]).
pub(super) fn armed_probe(
	pipeline: &Pipeline,
	state: &State,
) -> Option<Arc<crate::exec::topk_pushdown::TopKThresholdProbe>> {
	let topk = pipeline.source.topk()?;
	if topk.coarse.is_some() {
		return None;
	}
	let cell = state.topk_cells.get(topk.sort as usize)?.as_ref()?;
	Some(topk.probe(cell))
}

/// The cut a key-range scan makes beneath a bounded sort ordered first by a
/// datetime component of the record id, which the scan delivers in the order
/// of its whole second ([`super::payload::CoarseCutoff`]).
///
/// The sort publishes the first key of the last row it keeps once it holds
/// as many as it keeps. A record whose component is in a second past that
/// key's, in the scan's direction, orders strictly after every kept row, and
/// so does every record the scan reads after it: the scan stops before it.
/// The threshold is read at each record until the sort first publishes one,
/// and afresh before each batch the scan reads after that; a threshold read
/// late is looser, which cuts later, never wrongly.
struct CoarseGate {
	position: u32,
	direction: crate::kvs::Direction,
	/// The rows the sort keeps: the first step of a decoded scan's reads
	/// ([`scan_decoded_cut`]).
	keep: u32,
	cell: Arc<crate::exec::topk_pushdown::TopKThresholdCell>,
	threshold: Option<Arc<Value>>,
	/// Whether the scan has passed the threshold's second: no range after
	/// the one it passed it in is read.
	passed: bool,
}

impl CoarseGate {
	/// The gate of `pipeline`'s scan reading in `direction`, in the run in
	/// progress; `None` where the scan makes no such cut or its sort holds no
	/// cell.
	fn of(
		program: &Program,
		pipeline: &Pipeline,
		state: &State,
		direction: crate::kvs::Direction,
	) -> Option<Self> {
		let topk = pipeline.source.topk()?;
		let coarse = topk.coarse?;
		let cell = state.topk_cells.get(topk.sort as usize)?.as_ref()?;
		let keep = program
			.sorts
			.get(topk.sort as usize)
			.and_then(|sort| sort.keep)
			.map_or(crate::kvs::NORMAL_BATCH_SIZE, |keep| u32::try_from(keep).unwrap_or(u32::MAX));
		Some(Self {
			position: coarse.position,
			direction,
			keep,
			cell: Arc::clone(cell),
			threshold: None,
			passed: false,
		})
	}

	/// Read the threshold the sort last published.
	fn refresh(&mut self) {
		self.threshold = self.cell.snapshot();
	}

	/// Whether the scan has passed the threshold's second at the record whose
	/// key is `key`; once it has, it stays passed.
	fn passes(&mut self, key: &RecordIdKey) -> bool {
		if self.passed {
			return true;
		}
		if self.threshold.is_none() {
			self.refresh();
		}
		let (Some(threshold), Some(element)) = (
			self.threshold.as_deref(),
			crate::exec::operators::scan::key_range::key_element(key, self.position),
		) else {
			return false;
		};
		self.passed = crate::exec::operators::scan::key_range::past_second(
			element,
			threshold,
			self.direction,
		);
		self.passed
	}

	/// [`Self::passes`] for a stored record, by its key bytes. The key is
	/// decoded only once the sort has published a threshold: before that no
	/// record passes it.
	fn passes_stored(&mut self, key: &[u8]) -> bool {
		if self.passed {
			return true;
		}
		if self.threshold.is_none() {
			self.refresh();
			if self.threshold.is_none() {
				return false;
			}
		}
		use crate::key::KVKeyDecode as _;
		match crate::key::schema::RecordKey::decode_key(key) {
			Ok(decoded) => self.passes(&decoded.id),
			Err(_) => false,
		}
	}
}

/// Run a pipeline over the records of the key ranges `key_range` builds in
/// `table`, the table's whole record range, range by range in key order along
/// `direction`. Answers whether the pipeline stopped, taking no more rows.
///
/// Each bound is evaluated once, here. A bound whose evaluation fails reads as
/// `NONE`, which widens what it bounds: the predicate above the scan judges
/// every row, so the rows are those a scan of the whole table would yield it.
/// A range after the pipeline's limit is spent, or after the scan's cut has
/// fired ([`CoarseGate`]), is not opened. Each range is read through a
/// generation check of its own, taken from `descriptor`.
#[allow(clippy::too_many_arguments)]
async fn scan_key_ranges(
	program: &Arc<Program>,
	pipeline: &Pipeline,
	descriptor: &super::descriptor::TableDescriptor,
	name: &crate::val::TableName,
	key_range: &super::program::ScanRange,
	table: crate::key::RawRange,
	direction: crate::kvs::Direction,
	version: Option<u64>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<bool> {
	let mut bounds = Vec::with_capacity(key_range.bounds.len());
	for bound in key_range.bounds.iter() {
		let value = match host_scalar(program, bound, ctx, None).await {
			Ok(value) => value,
			// A bound that cannot be evaluated bounds nothing: the scan widens
			// rather than raising, and the predicate that holds the same
			// expression raises where the statement owes it.
			Err(_) => Value::None,
		};
		bounds.push(value);
	}
	let ranges = match &key_range.spec {
		super::program::ScanSpec::Components(spec) => {
			crate::exec::operators::scan::key_range::record_key_ranges(
				&table, spec, &bounds, direction,
			)
		}
		super::program::ScanSpec::RecordKey(spec) => {
			let db_ctx = ctx.database().context("the VM requires a database context")?;
			crate::exec::operators::scan::id_range::record_id_ranges(
				db_ctx.ns_ctx.ns.namespace_id,
				db_ctx.db.database_id,
				name,
				&table,
				*spec,
				&bounds,
			)
		}
	};
	let mut coarse = CoarseGate::of(program, pipeline, state, direction);
	for (at, range) in ranges.into_iter().enumerate() {
		if at > 0 && limits_spent(pipeline, state) {
			return Ok(true);
		}
		if coarse.as_ref().is_some_and(|coarse| coarse.passed) {
			return Ok(false);
		}
		let gate = descriptor.row_gate().map_err(ControlFlow::Err)?;
		if scan_range(
			program,
			pipeline,
			range,
			gate,
			descriptor.replay(),
			direction,
			version,
			state,
			ctx,
			yielder,
			flush_at,
			coarse.as_mut(),
		)
		.await?
		{
			return Ok(true);
		}
	}
	Ok(false)
}

/// Run a pipeline over every record of `range`: inside the storage cursor on
/// the stored bytes when it runs in place, over the decoded records otherwise.
///
/// `gate` is the generation check of the table `range` reads: each record is
/// admitted by it before the top-k probe, the pre-decode filter or the body
/// reads any field of the record. `replay` is the table's migration steps,
/// when it has any: a record stamped at or after the newest step is read as
/// stored, in place and by the pre-decode filter and the top-k probe alike,
/// and any other is decoded and brought through the steps it has not passed
/// before the body or a judge of its bytes sees it. With `coarse`, the scan
/// stops before the first record past its cut.
///
/// The windows of the source rows' links and lookups ([`State::link_ahead`],
/// [`State::frontier_rows`]) are emptied however the scan ends: a spent
/// limit, a failing row and a failing source included.
#[allow(clippy::too_many_arguments)]
async fn scan_range(
	program: &Arc<Program>,
	pipeline: &Pipeline,
	range: crate::key::RawRange,
	gate: RowGate,
	replay: Option<Replay>,
	direction: crate::kvs::Direction,
	version: Option<u64>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
	coarse: Option<&mut CoarseGate>,
) -> FlowResult<bool> {
	let ended = scan_rows(
		program, pipeline, range, gate, replay, direction, version, state, ctx, yielder, flush_at,
		coarse,
	)
	.await;
	#[cfg(test)]
	if !state.frontier_rows.is_empty() {
		super::graph::frontier::counters::end_held();
	}
	if let Some(ahead) = &mut state.link_ahead {
		ahead.clear();
	}
	state.frontier_rows.clear();
	ended
}

/// The row of the record `rid` a write names. A record whose content is not an
/// object or `NONE` is passed on as its record id, which the write reads by id,
/// so that the content of the stored record is read as the object of its id.
fn write_subject_row(writing: bool, rid: &RecordId, row: Value) -> Value {
	if writing && read_by_id(&row) {
		Value::RecordId(rid.clone())
	} else {
		row
	}
}

/// [`scan_range`] without the emptying of the windows the scan filled.
#[allow(clippy::too_many_arguments)]
async fn scan_rows(
	program: &Arc<Program>,
	pipeline: &Pipeline,
	range: crate::key::RawRange,
	gate: RowGate,
	replay: Option<Replay>,
	direction: crate::kvs::Direction,
	version: Option<u64>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
	coarse: Option<&mut CoarseGate>,
) -> FlowResult<bool> {
	// The first read of storage of a run whose reader says how many rows it
	// takes reads no more records than that before it hands its rows on.
	let first = state.scan_hint.take();
	// A body holding a measure the host answers hands the host a step per row, so the run reads
	// its rows decoded.
	let in_place = runs_in_place(pipeline, state);
	if in_place {
		// A body in the storage cursor planned as morsels reads its key range in parts; a scan cut
		// by a coarse gate reads in one cursor, which the gate stops, and a scan of a table whose
		// records may be stamped below its newest migration step reads in one cursor, which
		// brings each such record through the steps it has not passed.
		if coarse.is_none()
			&& replay.is_none()
			&& super::morsel::effective_input(pipeline) == Some(MorselInput::Range)
		{
			// Morsels decode a packed record through the layouts the gate lists as the scan
			// opens.
			let mut gate = gate;
			gate.refresh(&ctx.txn()).await.map_err(ControlFlow::Err)?;
			let layouts = Some(Arc::clone(gate.layouts()));
			return super::morsel::scan_range_in_place(
				program, pipeline, range, direction, version, first, layouts, state, ctx, yielder,
				flush_at,
			)
			.await;
		}
		return scan_in_place(
			program,
			pipeline,
			range,
			gate,
			replay.as_ref(),
			direction,
			version,
			first,
			state,
			ctx,
			yielder,
			flush_at,
			coarse,
		)
		.await;
	}
	if let Some(coarse) = coarse {
		return out_of_line(|| {
			scan_decoded_cut(
				program,
				pipeline,
				range,
				gate,
				replay.as_ref(),
				direction,
				version,
				first,
				state,
				ctx,
				yielder,
				flush_at,
				coarse,
			)
		})
		.await;
	}
	let topk_probe = armed_probe(pipeline, state);
	// The body's first step refuses what the shape judges false, so a record
	// the stored bytes show it would refuse is not decoded.
	let prefilter = pipeline
		.source
		.prefilter()
		.map(|shape| shape.filter(ctx.root().ctx.config.exec.idiom_recursion_limit));
	let mut source = kv_scan_stream(
		ctx.txn(),
		range,
		gate,
		version,
		None,
		direction,
		0,
		first,
		prefilter,
		topk_probe,
		replay.clone(),
		if program.write.is_some() {
			ScalarRows::RecordIds
		} else {
			ScalarRows::Content
		},
	);
	// The rows of a program that does heavy work for each, reading only, are
	// read and decoded on a task of their own, ahead of the work.
	if program.access_mode == crate::exec::access_mode::AccessMode::ReadOnly
		&& program
			.splits
			.iter()
			.any(|split| matches!(split.kind, super::program::SplitKind::Step(_)))
	{
		source = crate::exec::buffer_stream(
			source,
			program.access_mode,
			crate::exec::CardinalityHint::Unbounded,
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
	}
	let mut rows = Rows::of_run(program, pipeline, in_place, ctx).reading(first);
	while let Some(batch) = source.next().await {
		// The source spends the records it reads and refuses before decoding
		// them, and hands on an empty batch once that spends the fuel; the
		// records it hands on are a unit each as their rows run.
		let mut batch = match super::fuel::tick(ctx, 0).await.and(batch) {
			Ok(batch) => batch,
			Err(e) => return rows.fail(e, state, ctx, yielder, flush_at).await,
		};
		if let Some(replay) = &replay
			&& let Err(e) = replay.apply_batch(ctx, &mut batch).await
		{
			return rows.fail(e, state, ctx, yielder, flush_at).await;
		}
		#[cfg(test)]
		RECORDS_SCANNED.with(|scanned| scanned.set(scanned.get() + batch.len()));
		// The links of the rows are read ahead a run of rows at a time, each run before its rows.
		let mut pending = batch.into_values().into_iter();
		while pending.len() > 0 {
			let run = match rows.ahead(program, pipeline, pending.as_slice(), state, ctx).await {
				Ok(run) => run.max(1),
				Err(e) => return rows.fail(e, state, ctx, yielder, flush_at).await,
			};
			for row in pending.by_ref().take(run) {
				if rows.row(program, pipeline, row, state, ctx, yielder, flush_at).await? {
					return Ok(true);
				}
			}
		}
	}
	rows.finish(state, ctx, yielder, flush_at).await
}

#[cfg(test)]
thread_local! {
	/// How many records this thread's scans of a table's range read from
	/// storage, for tests that must know how far a scan read.
	pub(super) static RECORDS_SCANNED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The register file of a body run inside the storage cursor that holds no
/// more registers than this.
const SMALL_REGISTERS: usize = 8;

/// Run a pipeline whose rows are decoded over the records of `range`, which
/// `coarse` cuts: in steps, each step's rows handed on — to the bounded sort
/// that publishes the threshold the cut reads — before the next step is
/// read, and the cut judged on each record's key before it is decoded.
///
/// The first step reads no more records than the sort keeps (nor than
/// `first`, where it is given), and each step after it twice as many as the
/// one before, up to a whole batch. A scan whose sort fills from its first
/// records therefore reads about as many records as it keeps, plus those of
/// the second its last kept row is in; one read in whole batches, or ahead of
/// its rows, would read records the cut can no longer stop. What it reads and
/// refuses is charged as a scan of the decoded rows charges it.
///
/// Each record is admitted by `gate` before the pre-decode filter or the
/// decode reads it. A record naming a generation the gate does not list yet
/// ends the step and is held, copied: the gate reloads the table's layouts,
/// which cannot be done inside the cursor, and the held record is admitted
/// and decoded last in the step, so records keep the order they are stored in.
#[allow(clippy::too_many_arguments)]
async fn scan_decoded_cut(
	program: &Arc<Program>,
	pipeline: &Pipeline,
	range: crate::key::RawRange,
	mut gate: RowGate,
	replay: Option<&Replay>,
	direction: crate::kvs::Direction,
	version: Option<u64>,
	first: Option<u32>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
	coarse: &mut CoarseGate,
) -> FlowResult<bool> {
	// The body's first step refuses what the shape judges false, so a record
	// the stored bytes show it would refuse is not decoded.
	let prefilter = pipeline
		.source
		.prefilter()
		.map(|shape| shape.filter(ctx.root().ctx.config.exec.idiom_recursion_limit));
	let txn = ctx.txn();
	let mut cursor = txn
		.open_vals_cursor_raw(range, direction, 0, version)
		.await
		.context("Failed to open scan cursor")?;
	let mut step = first
		.map_or(coarse.keep, |first| first.min(coarse.keep))
		.clamp(1, crate::kvs::NORMAL_BATCH_SIZE);
	loop {
		let size = super::fuel::batch(step);
		coarse.refresh();
		let mut decoded = Vec::with_capacity(size as usize);
		// The generation each decoded record is stamped with, where the table
		// has migration steps.
		let mut stamps: Vec<Option<crate::catalog::Generation>> = Vec::new();
		let mut failed = None;
		let mut held: Option<(Vec<u8>, Vec<u8>)> = None;
		let mut cut = false;
		// The step is admitted and decoded against one view of the table's
		// layouts: the visitor runs no statement that could move the
		// transaction's layout epoch between this refresh and the end of the
		// step.
		gate.refresh(&txn).await.map_err(ControlFlow::Err)?;
		let layouts: &TableLayouts = gate.layouts();
		let stats = cursor
			.for_each(size, &mut |key, value| {
				if coarse.passes_stored(key) {
					cut = true;
					return Ok(std::ops::ControlFlow::Break(()));
				}
				match gate.admit(value) {
					Admission::Admit => {}
					Admission::Reload => {
						held = Some((key.to_vec(), value.to_vec()));
						return Ok(std::ops::ControlFlow::Break(()));
					}
					Admission::Refuse(e) => {
						failed = Some(ControlFlow::Err(e));
						return Ok(std::ops::ControlFlow::Break(()));
					}
				}
				let stamp = replay.map(|_| stamp_of(value));
				// A record behind a migration step is judged as it reads after
				// the steps, not by its stored bytes.
				let judged = match (replay, stamp) {
					(Some(replay), Some(stamp)) => !replay.behind(stamp),
					_ => true,
				};
				// A packed row the filter checked is decoded from the checked row when kept.
				let admitted = match prefilter.as_deref() {
					Some(filter) if judged => {
						AdmittedRow::for_filter(filter, key, value, Some(layouts))
					}
					_ => AdmittedRow::Bytes(value),
				};
				if judged
					&& let Some(filter) = prefilter.as_deref()
					&& filter.apply_in(&admitted, Some(layouts)) == PreDecodeFilterOutcome::Reject
				{
					return Ok(std::ops::ControlFlow::Continue(()));
				}
				match admitted.decode(key, value, Some(layouts)) {
					Ok(row) => {
						decoded.push(row);
						stamps.extend(stamp);
						Ok(std::ops::ControlFlow::Continue(()))
					}
					Err(e) => {
						failed = Some(ControlFlow::Err(e));
						Ok(std::ops::ControlFlow::Break(()))
					}
				}
			})
			.await
			.context("Failed to scan record")?;
		if let Some(e) = failed {
			return Err(e);
		}
		let reloaded = held.is_some();
		if let Some((key, value)) = held {
			gate.reload(&txn).await.map_err(ControlFlow::Err)?;
			match gate.admit(&value) {
				Admission::Admit => {}
				Admission::Refuse(e) => return Err(ControlFlow::Err(e)),
				Admission::Reload => {
					return Err(ControlFlow::Err(anyhow::anyhow!(
						"a record's generation was unlisted again after the layouts reloaded"
					)));
				}
			}
			let stamp = replay.map(|_| stamp_of(&value));
			let judged = match (replay, stamp) {
				(Some(replay), Some(stamp)) => !replay.behind(stamp),
				_ => true,
			};
			let layouts = Some(&**gate.layouts());
			let admitted = match prefilter.as_deref() {
				Some(filter) if judged => AdmittedRow::for_filter(filter, &key, &value, layouts),
				_ => AdmittedRow::Bytes(&value),
			};
			let refused = judged
				&& prefilter.as_deref().is_some_and(|filter| {
					filter.apply_in(&admitted, layouts) == PreDecodeFilterOutcome::Reject
				});
			if !refused {
				decoded.push(admitted.decode(&key, &value, layouts).map_err(ControlFlow::Err)?);
				stamps.extend(stamp);
			}
		}
		#[cfg(test)]
		RECORDS_SCANNED.with(|scanned| scanned.set(scanned.get() + decoded.len()));
		ensure_below_memory_threshold()?;
		// The records read and refused before decoding spend the fuel here;
		// a record decoded is spent as its row runs.
		let refused = (stats.rows as usize).saturating_sub(decoded.len());
		super::fuel::charge(super::fuel::units(refused));
		super::fuel::tick(ctx, 0).await?;
		let mut rows = Rows::of(program, pipeline, ctx).reading(Some(size));
		for (at, mut row) in decoded.into_iter().enumerate() {
			if let Some(replay) = replay
				&& let Err(e) = replay.apply(ctx, &mut row, stamps.get(at).copied().flatten()).await
			{
				return rows.fail(e, state, ctx, yielder, flush_at).await;
			}
			if rows.row(program, pipeline, row, state, ctx, yielder, flush_at).await? {
				return Ok(true);
			}
		}
		if rows.finish(state, ctx, yielder, flush_at).await? {
			return Ok(true);
		}
		// A step that ended on a held record read fewer than `size` records
		// without reaching the end of the range.
		if cut || (!reloaded && stats.rows < u64::from(size)) {
			return Ok(false);
		}
		step = step.saturating_mul(2).min(crate::kvs::NORMAL_BATCH_SIZE);
	}
}

/// Run an in-place pipeline over every record of `range`, inside the storage
/// cursor, on each record's stored bytes: no more than `first` records before
/// the rows are handed on the first time, where it is given.
///
/// Each record is admitted by `gate` before the top-k probe or the body reads
/// a field of it. A record naming a generation the gate does not list yet ends
/// the chunk and is held, copied: the gate reloads the table's layouts, which
/// cannot be done inside the cursor, and the held record is admitted and run
/// before the scan goes on, so records run in the order they are stored.
#[allow(clippy::too_many_arguments)]
async fn scan_in_place(
	program: &Program,
	pipeline: &Pipeline,
	range: crate::key::RawRange,
	mut gate: RowGate,
	replay: Option<&Replay>,
	direction: crate::kvs::Direction,
	version: Option<u64>,
	mut first: Option<u32>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
	mut coarse: Option<&mut CoarseGate>,
) -> FlowResult<bool> {
	let topk_probe = armed_probe(pipeline, state);
	let prefilter = in_place_prefilter(pipeline, ctx);
	let txn = ctx.txn();
	let reader: Arc<dyn super::read_ahead::Reader> = Arc::clone(&txn) as _;
	let budget = pipeline.read_ahead.then(|| ctx.root().ctx.config.exec.scan_batch_bytes);
	let mut source =
		super::read_ahead::ChunkSource::open(&reader, range, direction, version, budget)
			.await
			.context("Failed to open scan cursor")?;
	// The source ends its producer on every exit path, so the loop runs in a block whose result
	// is returned once the source has finished.
	let outcome: FlowResult<bool> = async {
		loop {
			let mut failed = None;
			let mut stopped = false;
			let mut held: Option<(Vec<u8>, Vec<u8>)> = None;
			// The record the chunk stopped on because it is stamped below the
			// table's newest migration step: its stored bytes are not what a read
			// of it sees, so it is decoded, brought through the steps and run as a
			// value once the chunk ends.
			let mut stale: Option<(Vec<u8>, Vec<u8>)> = None;
			let chunk =
				super::fuel::batch(first.take().map_or(crate::kvs::NORMAL_BATCH_SIZE, |first| {
					first.clamp(1, crate::kvs::NORMAL_BATCH_SIZE)
				}));
			// The chunk is admitted and run against one view of the table's layouts:
			// the visitor runs no statement that could move the transaction's layout
			// epoch between this refresh and the end of the chunk.
			gate.refresh(&txn).await.map_err(ControlFlow::Err)?;
			let layouts: &TableLayouts = gate.layouts();
			// The body cannot yield inside the cursor, so the chunk counts the
			// rows it reads against the fuel left and ends once that is spent: a
			// unit per record read, run or refused. What the bodies charge besides
			// is drawn on the meter as they run and settled at the chunk's end. The body holds no
			// backward jump (`lower`'s in-place rule), so the interpreter answers no
			// `Step::Yield` here.
			let quantum = super::fuel::quantum(ctx);
			let budget = super::fuel::left();
			let mut unspent = budget;
			let mut body_spent = 0;
			if let Some(coarse) = coarse.as_deref_mut() {
				coarse.refresh();
			}
			let mut cut = false;
			// The threshold is read once a chunk, after the rows before it reached the sort: a
			// stale one only rejects less.
			let topk_threshold = topk_probe.as_ref().and_then(|probe| probe.snapshot());
			let stats = source
				.for_each(chunk, &mut |key, value| {
					if coarse.as_deref_mut().is_some_and(|coarse| coarse.passes_stored(key)) {
						cut = true;
						return Ok(std::ops::ControlFlow::Break(()));
					}
					unspent = unspent.saturating_sub(1);
					let next = if unspent == 0 {
						std::ops::ControlFlow::Break(())
					} else {
						std::ops::ControlFlow::Continue(())
					};
					match gate.admit(value) {
						Admission::Admit => {}
						Admission::Reload => {
							held = Some((key.to_vec(), value.to_vec()));
							return Ok(std::ops::ControlFlow::Break(()));
						}
						Admission::Refuse(e) => {
							failed = Some(e);
							return Ok(std::ops::ControlFlow::Break(()));
						}
					}
					if replay.is_some_and(|replay| !replay.current(value)) {
						stale = Some((key.to_vec(), value.to_vec()));
						return Ok(std::ops::ControlFlow::Break(()));
					}
					match super::fuel::measure(&mut body_spent, quantum, || {
						scan_row(
							program,
							pipeline,
							prefilter.as_deref(),
							topk_probe.as_ref(),
							topk_threshold.as_deref(),
							key,
							value,
							Some(layouts),
							state,
						)
					}) {
						Ok(false) => Ok(next),
						Ok(true) => {
							stopped = true;
							Ok(std::ops::ControlFlow::Break(()))
						}
						Err(e) => {
							failed = Some(e);
							Ok(std::ops::ControlFlow::Break(()))
						}
					}
				})
				.await
				.context("Failed to scan record")?;
			super::fuel::charge((budget - unspent).saturating_add(body_spent));
			if let Some(e) = failed {
				return Err(ControlFlow::Err(e));
			}
			if let Some((key, value)) = held {
				gate.reload(&txn).await.map_err(ControlFlow::Err)?;
				match gate.admit(&value) {
					Admission::Admit => {}
					Admission::Refuse(e) => return Err(ControlFlow::Err(e)),
					Admission::Reload => {
						return Err(ControlFlow::Err(anyhow::anyhow!(
							"a record's generation was unlisted again after the layouts reloaded"
						)));
					}
				}
				stopped = if replay.is_some_and(|replay| !replay.current(&value)) {
					stale = Some((key, value));
					false
				} else {
					scan_row(
						program,
						pipeline,
						prefilter.as_deref(),
						topk_probe.as_ref(),
						topk_threshold.as_deref(),
						&key,
						&value,
						Some(&**gate.layouts()),
						state,
					)
					.map_err(ControlFlow::Err)?
				};
			}
			if let (Some((key, value)), Some(replay)) = (stale.take(), replay) {
				let mut record = decode_record_value(&key, &value, Some(&**gate.layouts()))
					.map_err(ControlFlow::Err)?;
				replay.apply(ctx, &mut record, stamp_of(&value)).await?;
				stopped =
					finish_in_place(run_in_place(program, pipeline, &ValueRow(&record), state))
						.map_err(ControlFlow::Err)?;
				if state.pending.is_some() {
					state.settle(program, record);
				}
			}
			#[cfg(test)]
			RECORDS_SCANNED.with(|scanned| scanned.set(scanned.get() + stats.rows as usize));
			// The rows the chunk emitted are held until `flush` hands them on, so a
			// process past its memory threshold fails here rather than scanning on.
			ensure_below_memory_threshold()?;
			flush(state, yielder, flush_at, ctx).await?;
			if stopped || cut || stats.rows == 0 {
				return Ok(stopped);
			}
			if super::fuel::spent() {
				out_of_line(|| super::fuel::yield_point(ctx)).await?;
			}
		}
	}
	.await;
	source.finish().await;
	outcome
}

/// Run an admitted record of an in-place scan: refused unread when the scan's
/// pre-decode filter judges it false from its stored bytes ([`in_place_rejects`])
/// or the top-k probe shows its ORDER BY key cannot beat the sort's worst kept
/// row, otherwise run through the body ([`in_place_row_admitted`]). Answers
/// whether the pipeline takes no more rows.
///
/// The caller has admitted the record's generation ([`RowGate::admit`]), so
/// neither the probe nor the body reads a field of a record the table's
/// layouts do not cover, and passes the layouts that admitted it, which a
/// packed record is checked and decoded with. A packed record is checked once
/// ([`admit_row`]), for the probe and the body together.
#[inline]
#[allow(clippy::too_many_arguments)]
fn scan_row(
	program: &Program,
	pipeline: &Pipeline,
	prefilter: Option<&PreDecodeFilter>,
	probe: Option<&Arc<crate::exec::topk_pushdown::TopKThresholdProbe>>,
	threshold: Option<&Value>,
	key: &[u8],
	value: &[u8],
	layouts: Option<&TableLayouts>,
	state: &mut State,
) -> anyhow::Result<bool> {
	let admitted = admit_row(pipeline, probe.zip(threshold), key, value, layouts);
	if let Some(filter) = prefilter
		&& in_place_rejects(filter, &admitted, layouts)
	{
		return Ok(false);
	}
	if let (Some(probe), Some(threshold)) = (probe, threshold)
		&& probe.rejects(threshold, &admitted)
	{
		return Ok(false);
	}
	in_place_row_admitted(program, pipeline, key, value, &admitted, layouts, state)
}

/// The pre-decode filter an in-place scan runs ahead of its body
/// ([`in_place_rejects`]), compiled once for the scan from the shape its
/// source carries: `None` when the scan carries none.
///
/// The shape is the WHERE the body refuses rows by first, so a record the
/// filter judges false is one the body would refuse: the scan passes it over
/// without running the body.
pub(super) fn in_place_prefilter(
	pipeline: &Pipeline,
	ctx: &ExecutionContext,
) -> Option<Arc<PreDecodeFilter>> {
	pipeline
		.source
		.prefilter()
		.map(|shape| shape.filter(ctx.root().ctx.config.exec.idiom_recursion_limit))
}

/// Whether `filter` refuses the admitted record of an in-place scan from its
/// stored bytes, ahead of the body.
///
/// The filter judges a record only where it reads it without a walk of its own:
/// a packed row the admission checked ([`AdmittedRow::Packed`]), and a document
/// of a generation `layouts` vouch for ([`TableLayouts::vouches_for`]). Either
/// is a row the whole decode accepts, so the filter refuses one only where the
/// predicate is false of the record the decode holds, as ADR 0010 requires.
/// Any other record is left to the body, which reads it as it would: a
/// document of an unverified generation is walked once, by the body's
/// admission, rather than once for the filter and again for the body.
#[inline]
pub(super) fn in_place_rejects(
	filter: &PreDecodeFilter,
	admitted: &AdmittedRow<'_>,
	layouts: Option<&TableLayouts>,
) -> bool {
	let judged = match admitted {
		AdmittedRow::Packed {
			..
		} => true,
		AdmittedRow::Bytes(value) => layouts.is_some_and(|layouts| {
			matches!(
				StoredRecord::parse(value),
				Ok(Some(record)) if !record.is_packed() && layouts.vouches_for(record.stamp())
			)
		}),
		AdmittedRow::Refused => false,
	};
	let rejects = judged && filter.apply_in(admitted, layouts) == PreDecodeFilterOutcome::Reject;
	// A packed row the filter refuses has been read in place, by the filter rather than the body.
	#[cfg(test)]
	if rejects && matches!(admitted, AdmittedRow::Packed { .. }) {
		super::row_packed::observed::note_in_place();
	}
	rejects
}

/// The record `value` of `key`, checked when a packed row is read where its columns lie: by the
/// probe `probing` armed, where it may reject a packed row, or by a body that reads fields of the
/// row ([`Pipeline::reads_row`]). Any other record is its bytes, which a packed row takes as one
/// to decode whole.
///
/// The check is made only where something reads its result, so a scan that neither probes nor
/// reads columns pays none for its packed rows.
pub(super) fn admit_row<
	'a,
	P: std::borrow::Borrow<crate::exec::topk_pushdown::TopKThresholdProbe>,
>(
	pipeline: &Pipeline,
	probing: Option<(&P, &Value)>,
	key: &'a [u8],
	value: &'a [u8],
	layouts: Option<&'a TableLayouts>,
) -> AdmittedRow<'a> {
	if pipeline.reads_row || probing.is_some_and(|(probe, _)| probe.borrow().reads_packed_rows()) {
		AdmittedRow::of(key, value, layouts)
	} else {
		AdmittedRow::Bytes(value)
	}
}

/// Run `pipeline`'s body over the stored record `key` names, whose encoded
/// value is `value`, in place: the body hands the host no step (its pipeline
/// is [`Pipeline::in_place`] or [`Pipeline::stored_rows`]), and the row it
/// leaves pending is settled from the stored bytes. Answers whether the
/// pipeline takes no more rows.
///
/// A record in a packed layout is read through its generation's descriptor in
/// `layouts` (the layouts that admitted it, refused as `NoLayouts` when
/// `None`).
///
/// [`in_place_row_admitted`] over a record not yet checked.
pub(super) fn in_place_row(
	program: &Program,
	pipeline: &Pipeline,
	key: &[u8],
	value: &[u8],
	layouts: Option<&TableLayouts>,
	state: &mut State,
) -> anyhow::Result<bool> {
	in_place_row_admitted(program, pipeline, key, value, &AdmittedRow::Bytes(value), layouts, state)
}

/// Run `pipeline`'s body over the stored record `key` names, whose encoded
/// value is `value` and which `admitted` shows checked where it is a packed
/// row ([`admit_row`]), in place. Answers whether the pipeline takes no more
/// rows.
///
/// A body that reads fields of a packed row ([`Pipeline::reads_row`]) reads
/// them where they lie, from the checked row: a row the check refused is
/// refused before the body runs, with the error its whole decode raises, and
/// one not yet checked is checked here ([`PackedRowReader`]). Any other body
/// runs on the record decoded whole when it first asks for it. The row the
/// body leaves pending is settled as the value the whole decode answers,
/// except a row emitted untouched into a frame, which is transcoded from its
/// bytes there ([`State::settle_packed`]); so a packed record never becomes a
/// sort's stored bytes or a spill's, and neither needs layouts when it is read
/// again.
#[allow(clippy::too_many_arguments)]
pub(super) fn in_place_row_admitted(
	program: &Program,
	pipeline: &Pipeline,
	key: &[u8],
	value: &[u8],
	admitted: &AdmittedRow<'_>,
	layouts: Option<&TableLayouts>,
	state: &mut State,
) -> anyhow::Result<bool> {
	if StoredRecord::is_packed_bytes(value) {
		if !pipeline.reads_row {
			let row = super::row_packed::LazyPackedRow::new(key, value, layouts, admitted);
			let stop = finish_in_place(run_in_place(program, pipeline, &row, state))?;
			if state.pending.is_some() {
				state.decoded += 1;
				match row.into_parts() {
					(Some(row), _) => state.settle(program, row),
					(None, admitted) => {
						state.settle_packed(program, key, value, layouts, &admitted)?
					}
				}
			}
			return Ok(stop);
		}
		let row = match admitted {
			AdmittedRow::Packed {
				row,
				key,
			} => PackedRowReader::of(key, *row),
			AdmittedRow::Refused => return Err(super::row_packed::refusal(key, value, layouts)),
			AdmittedRow::Bytes(_) => PackedRowReader::new(key, value, layouts)?,
		};
		let stop = finish_in_place(run_in_place(program, pipeline, &row, state))?;
		if state.pending.is_some() {
			state.decoded += 1;
			state.settle(program, row.into_value()?);
		}
		return Ok(stop);
	}
	let row = StoredRow::admitted(key, value, layouts);
	let stop = finish_in_place(run_in_place(program, pipeline, &row, state))?;
	if state.pending.is_some() {
		state.decoded += 1;
		state.settle_stored(program, key, value)?;
	}
	Ok(stop)
}

/// Run `pipeline`'s in-place body over `row`, with a register file sized to the
/// body.
fn run_in_place<'r>(
	program: &'r Program,
	pipeline: &'r Pipeline,
	row: &'r dyn RowReader,
	state: &mut State,
) -> anyhow::Result<Step> {
	// A body that holds few registers gets a register file of its
	// size: the file is made and dropped for every record.
	if pipeline.registers <= SMALL_REGISTERS {
		let mut regs: [Scalar<'_>; SMALL_REGISTERS] = std::array::from_fn(|_| Scalar::None);
		interp::run_row(program, pipeline, row, &mut regs[..pipeline.registers], state, 0)
	} else {
		let mut stack;
		let mut spilled;
		let regs: &mut [Scalar<'_>] = if pipeline.registers <= STACK_REGISTERS {
			stack = std::array::from_fn::<Scalar<'_>, STACK_REGISTERS, _>(|_| Scalar::None);
			&mut stack[..pipeline.registers]
		} else {
			spilled = (0..pipeline.registers).map(|_| Scalar::None).collect::<Vec<_>>();
			&mut spilled[..]
		};
		interp::run_row(program, pipeline, row, regs, state, 0)
	}
}

/// What a step an in-place body ended on means for the scan: whether the
/// pipeline takes no more rows. A step that hands the host work is an error,
/// because an in-place body hands it none.
fn finish_in_place(step: anyhow::Result<Step>) -> anyhow::Result<bool> {
	match step {
		Ok(Step::Next) => Ok(false),
		Ok(Step::Stop) => Ok(true),
		Ok(Step::WriteProjection {
			..
		})
		| Ok(Step::LandRow {
			..
		})
		| Ok(Step::BindRow {
			..
		})
		| Ok(Step::Write {
			..
		})
		| Ok(Step::Store {
			..
		})
		| Ok(Step::Block {
			..
		})
		| Ok(Step::Param {
			..
		})
		| Ok(Step::CallAllowed {
			..
		})
		| Ok(Step::FieldTypes {
			..
		})
		| Ok(Step::Access {
			..
		})
		| Ok(Step::Each {
			..
		})
		| Ok(Step::Fetch {
			..
		})
		| Ok(Step::FetchRow {
			..
		})
		| Ok(Step::Call {
			..
		})
		| Ok(Step::IndexFn {
			..
		})
		| Ok(Step::Matches {
			..
		})
		| Ok(Step::Method {
			..
		})
		| Ok(Step::Subquery {
			..
		})
		| Ok(Step::Lookup {
			..
		})
		| Ok(Step::Destructure {
			..
		})
		| Ok(Step::JudgeCopy {
			..
		})
		| Ok(Step::Omit {
			..
		})
		| Ok(Step::Signal {
			..
		})
		| Ok(Step::Recurse {
			..
		})
		| Ok(Step::Closure {
			..
		})
		| Ok(Step::NeedUnit {
			..
		})
		| Ok(Step::Function {
			..
		})
		| Ok(Step::External {
			..
		})
		| Ok(Step::Ir {
			..
		})
		| Ok(Step::Degree {
			..
		})
		| Ok(Step::KnnQuery {
			..
		})
		| Ok(Step::Measure {
			..
		}) => Err(anyhow::anyhow!("an in-place pipeline evaluated an expression")),
		Ok(Step::Yield {
			..
		}) => Err(anyhow::anyhow!("an in-place pipeline yielded inside the storage cursor")),
		Err(e) => Err(e),
	}
}

/// Run a pipeline over rows held in memory. Each row spends a unit of fuel
/// as its body is entered ([`run_body`]), which yields once it is spent.
pub(super) async fn run_rows(
	program: &Program,
	pipeline: &Pipeline,
	rows: Vec<(Vec<Value>, Value)>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<bool> {
	for (keys, row) in rows {
		// A sorted row's keys, which a projection sharing them reads.
		state.sorted_keys = keys;
		if run_value_row(program, pipeline, row, state, ctx).await? == Ended::Stop {
			return Ok(true);
		}
		flush(state, yielder, flush_at, ctx).await?;
	}
	Ok(false)
}

/// Run host pipeline `pipeline`, an index into `program.host_code`, over `image`
/// in place and synchronously, when its body answers without handing the host a
/// step. `image` is read as a document row.
/// `None` when the body hands the host a step or fails, having kept nothing it
/// did; the caller then runs the pipeline whole.
///
/// A run over a state already inside an [`Instr::TryNone`] region leaves those
/// regions as it found them: it catches only the errors of the regions it opens
/// itself.
pub(super) fn run_image_inline(
	program: &Program,
	pipeline: u16,
	image: &Value,
	state: &mut State,
) -> Option<Value> {
	const INLINE_REGISTERS: usize = 8;
	let pipeline = &program.host_code[pipeline as usize];
	let reader = super::row::ValueRow(image);
	let mut stack;
	let mut spilled;
	let regs: &mut [Scalar<'_>] = if pipeline.registers <= INLINE_REGISTERS {
		stack = [const { Scalar::None }; INLINE_REGISTERS];
		&mut stack[..pipeline.registers]
	} else {
		spilled = (0..pipeline.registers).map(|_| Scalar::None).collect::<Vec<_>>();
		&mut spilled[..]
	};
	// The run is a unit of work; once the fuel is spent the caller runs the
	// pipeline whole, which yields before it starts.
	if super::fuel::charge(1) {
		return None;
	}
	let (output, tries) = (state.output.len(), state.tries.len());
	let rowless = std::mem::replace(&mut state.rowless, false);
	let undocumented = std::mem::replace(&mut state.undocumented, false);
	let root = state.root.take();
	let step = interp::run_row(program, pipeline, &reader, regs, state, 0);
	state.rowless = rowless;
	state.undocumented = undocumented;
	state.root = root;
	match step {
		Ok(Step::Next | Step::Stop) if state.output.len() == output + 1 => state.output.pop(),
		// A yield, like any host step, is left to the caller's run of the
		// whole pipeline, which takes it.
		_ => {
			state.output.truncate(output);
			state.tries.truncate(tries);
			None
		}
	}
}

/// Where a unit's body run in place ([`run_unit_inline`]) left its row when
/// it could not finish it there.
pub(super) struct Unfinished<'r> {
	/// The row, with the layers the body landed on it before it stopped.
	pub(super) row: Cow<'r, Value>,
	/// Where the host continues the body over `row`: `Some` with the
	/// instruction that begins the stretch the body stopped in, on the same
	/// state, which holds the body's work up to that stretch and nothing of
	/// the stretch itself; `None` where the body is run whole on a fresh
	/// state, `row` being as it was handed in.
	pub(super) resume: Option<usize>,
	/// Whether the body stopped because the fuel was spent rather than for a
	/// step only the host takes: the body is no less fit to run in place the
	/// next time.
	pub(super) yielded: bool,
}

/// Run `pipeline`'s body over `row`, a unit's one row on a state of the
/// unit's own ([`unit_state`]), in place and synchronously, when the body
/// answers without handing the host a step other than landing a layer, which
/// it lands here: how the row ended, with what the body left pending settled,
/// `row` copied only where the body changes it or hands it on.
///
/// The row is handed back ([`Unfinished`]) when the body hands the host
/// another step, fails, or splits the row. A body whose program pages, sorts,
/// aggregates or runs block statements holds work a stretch cannot be run
/// twice over, so it stops at its first layer and is run whole on a fresh
/// state. Any other is continued from the start of the stretch it stopped in
/// — the start of the body, or the instruction after the last layer landed —
/// with the stretch's open `TRY` regions, output and pending row given back;
/// what else a stretch sets (the record being computed, a landing) it sets
/// afresh when it runs again. The body binds no block slot itself, which
/// only a host step does, so the state's slots are still as the caller set
/// them.
pub(super) fn run_unit_inline<'r>(
	program: &Program,
	pipeline: &Pipeline,
	mut row: Cow<'r, Value>,
	state: &mut State,
) -> Result<Step, Unfinished<'r>> {
	/// The registers of a body small enough to run in place, in two sizes,
	/// so that the most common bodies, the smallest, set up the fewest.
	const FEW: usize = 4;
	const MANY: usize = 16;
	let resumable = program.limits.is_empty()
		&& program.sorts.is_empty()
		&& program.aggregates.is_empty()
		&& program.groups.is_empty();
	// A row the pipeline enforces the table's permissions, computed fields
	// and field permissions on is enforced by the host before the body runs
	// ([`run_cow_row_from`]), so the body is run whole there.
	if pipeline.enforce.is_some() {
		return Err(Unfinished {
			row,
			resume: None,
			yielded: false,
		});
	}
	// The run is a unit of work. Once the fuel is spent the body is handed
	// to the caller's host run, which yields before it enters the body and
	// never hands it back here, so a spent meter cannot bounce a row between
	// the two.
	if super::fuel::charge(1) {
		return Err(Unfinished {
			row,
			resume: resumable.then_some(0),
			yielded: true,
		});
	}
	let (output, tries) = (state.output.len(), state.tries.len());
	let mut start = 0;
	loop {
		let step = {
			let reader = ValueRow(&row);
			let registers = pipeline.registers;
			if registers <= FEW {
				let mut regs = [const { Scalar::None }; FEW];
				interp::run_row(program, pipeline, &reader, &mut regs[..registers], state, start)
			} else if registers <= MANY {
				let mut regs = [const { Scalar::None }; MANY];
				interp::run_row(program, pipeline, &reader, &mut regs[..registers], state, start)
			} else {
				return Err(Unfinished {
					row,
					resume: resumable.then_some(0),
					yielded: false,
				});
			}
		};
		match step {
			// A layer landed with a `TRY` region open across it is left to the
			// host, which catches into that region after the landing.
			Ok(Step::LandRow {
				resume,
			}) if resumable && state.tries.len() == tries => {
				land(state, &mut row);
				start = resume;
			}
			Ok(step @ (Step::Next | Step::Stop))
				if !matches!(
					state.pending,
					Some(Pending::Split { .. } | Pending::Join { .. } | Pending::JoinPair { .. })
				) =>
			{
				if state.pending.is_some() {
					state.settle(program, row.into_owned());
				}
				return Ok(step);
			}
			// A yield, like any host step, hands the row to the host, which
			// continues it from the start of the stretch.
			step => {
				if resumable {
					state.output.truncate(output);
					state.tries.truncate(tries);
					state.pending = None;
					state.landing = None;
				}
				return Err(Unfinished {
					row,
					resume: resumable.then_some(start),
					yielded: matches!(step, Ok(Step::Yield { .. })),
				});
			}
		}
	}
}

/// Land the columns [`State::landing`] holds on `row`: its cuts removed, then
/// its sets written.
fn land(state: &mut State, row: &mut Cow<'_, Value>) {
	if let (Some((cuts, sets)), Value::Object(obj)) = (state.landing.take(), row.to_mut()) {
		for name in cuts.keys() {
			obj.remove(name.as_str());
		}
		for (name, value) in sets {
			obj.insert(name, value);
		}
	}
}

pub(super) async fn run_value_row(
	program: &Program,
	pipeline: &Pipeline,
	row: Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Ended> {
	run_cow_row(program, pipeline, Cow::Owned(row), state, ctx).await
}

/// [`run_value_row`] over `row`, which is copied only where the body changes
/// it or hands it on: a host pipeline evaluated over a row it only reads runs
/// over the row where it is.
pub(super) async fn run_cow_row(
	program: &Program,
	pipeline: &Pipeline,
	row: Cow<'_, Value>,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Ended> {
	run_cow_row_from(program, pipeline, row, state, ctx, 0).await
}

/// [`run_cow_row`] from instruction `start` of the body, with fresh
/// registers: where the body begins, or where a run in place left it
/// ([`Unfinished::resume`]).
pub(super) async fn run_cow_row_from(
	program: &Program,
	pipeline: &Pipeline,
	row: Cow<'_, Value>,
	state: &mut State,
	ctx: &ExecutionContext,
	start: usize,
) -> FlowResult<Ended> {
	let mut row = row;
	// A source whose rows no node of the plan enforces the table's
	// permission, computed fields and field permissions on is put through
	// them first, as the table's scan does: a row the permission refuses is
	// no row.
	if start == 0
		&& let Some(enforce) = &pipeline.enforce
	{
		let mut enforced = row.into_owned();
		if !enforce_row(enforce, &mut enforced, state, ctx).await? {
			// A row the permission refuses never enters the body, which spends
			// the unit of a row that does.
			super::fuel::tick(ctx, 1).await?;
			return Ok(Ended::Next);
		}
		row = Cow::Owned(enforced);
	}
	let base = state.tries.len();
	let stepped = run_body(program, pipeline, row, state, ctx, base, start).await;
	state.tries.truncate(base);
	stepped
}

/// Put `row` through the enforcement `enforce` names, in place, and answer
/// whether the row is shown: only [`Enforce::Rows`] can refuse one. A row that
/// is no record (an object with a record id) is left as it is.
async fn enforce_row(
	enforce: &Enforce,
	row: &mut Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<bool> {
	use crate::exec::operators::scan::pipeline::{
		build_field_state, filter_fields_by_permission, materialise_fields_with_permissions,
	};
	let record = record_id(row);
	let frame = frame_ctx(state, ctx, None);
	match enforce {
		Enforce::Rows {
			needed,
			..
		} => {
			let Some(rid) = record else {
				return Ok(true);
			};
			// The table's read unit, which derives every field, enforces the
			// row where one serves the frame and the pipeline reads the row
			// whole, as a read of the record by id is enforced.
			if needed.is_none()
				&& let Some(deref) = frame.vm_deref()
				&& let Some(read) =
					out_of_line(|| deref.read_record(frame, &rid.table, row, true)).await
			{
				return read;
			}
			out_of_line(|| {
				crate::exec::operators::fetch::process_fetched_record_for(
					frame,
					&rid,
					row,
					needed.as_deref(),
				)
			})
			.await
		}
		Enforce::Image(table) => {
			if record.is_none() {
				return Ok(true);
			}
			let db_ctx = frame.database().context("an image enforced with no database context")?;
			let checked =
				crate::exec::permission::should_check_perms(db_ctx, crate::iam::Action::View)
					.map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;
			let fields = build_field_state(frame, table, checked, None).await?;
			materialise_fields_with_permissions(
				frame,
				&fields,
				row,
				frame.root().skip_fetch_perms,
				checked,
			)
			.await?;
			Ok(true)
		}
		Enforce::Guards(table) => {
			if record.is_none() {
				return Ok(true);
			}
			let fields = build_field_state(frame, table, true, None).await?;
			filter_fields_by_permission(frame, &fields, row).await?;
			Ok(true)
		}
	}
}

/// How a row's run ended, once its driver has taken every step the body
/// handed it, a yield among them: the pipeline moves to its next row, or
/// takes no more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Ended {
	Next,
	Stop,
}

/// Where a stretch of a body, run between the steps that rewrite the row,
/// left it.
enum Stretch {
	/// The row is done.
	Ended(Ended),
	/// Land a layer, then continue at the instruction.
	Land(usize),
	/// Bind the row under the field, then continue at the instruction.
	Bind(u32, usize),
	/// Take the write instruction, which names where the row continues.
	Write(usize),
}

async fn run_body(
	program: &Program,
	pipeline: &Pipeline,
	mut row: Cow<'_, Value>,
	state: &mut State,
	ctx: &ExecutionContext,
	base: usize,
	mut start: usize,
) -> FlowResult<Ended> {
	// A pipeline that runs over no row, where a per-row evaluation would have
	// run over a KNN query vector's row being scanned, reads that row as
	// forbidden.
	let forbidden = if state.rowless {
		frame_ctx(state, ctx, None).root().forbidden_row.clone().filter(|f| f.covers_row())
	} else {
		None
	};
	let step = loop {
		let step = {
			let value_row;
			let forbidden_row;
			let reader: &(dyn RowReader + Sync) = match &forbidden {
				Some(forbidden) => {
					forbidden_row = super::row::ForbiddenRowReader(forbidden);
					&forbidden_row
				}
				None => {
					value_row = ValueRow(&row);
					&value_row
				}
			};
			let mut small;
			let mut stack;
			let mut spilled;
			let regs: &mut [Scalar<'_>] = if pipeline.registers <= SMALL_REGISTERS {
				small = std::array::from_fn::<Scalar<'_>, SMALL_REGISTERS, _>(|_| Scalar::None);
				&mut small[..pipeline.registers]
			} else if pipeline.registers <= STACK_REGISTERS {
				stack = std::array::from_fn::<Scalar<'_>, STACK_REGISTERS, _>(|_| Scalar::None);
				&mut stack[..pipeline.registers]
			} else {
				spilled = (0..pipeline.registers).map(|_| Scalar::None).collect::<Vec<_>>();
				&mut spilled[..]
			};
			let mut pc = start;
			// An error about the shape of the data inside an `Instr::TryNone`
			// region is caught by the innermost, and the body resumes where it
			// names, unless the region ran a nested statement
			// ([`runs_nested_statement`]).
			loop {
				let attempt: FlowResult<Stretch> = async {
					// The instruction whose closures were linked last, and not
					// yet taken since: an invocation it makes that finds no unit
					// standing does not link again.
					let mut relinked: Option<usize> = None;
					loop {
						// Each entry into the body — the row's first, and each
						// resume after a host step — spends a unit of fuel.
						if super::fuel::charge(1) {
							out_of_line(|| super::fuel::yield_point(ctx)).await?;
						}
						// A region the body opens records the count as it stands now.
						state.nested_mark = ctx.nested_runs();
						let step = interp::run_row(program, pipeline, reader, regs, state, pc)
							.map_err(ControlFlow::Err)?;
						if runs_nested_statement(program, pipeline, &step) {
							ctx.note_nested_run();
						}
						match step {
							Step::Yield {
								resume,
							} => {
								out_of_line(|| super::fuel::yield_point(ctx)).await?;
								pc = resume;
							}
							Step::KnnQuery {
								at,
							} => {
								let Instr::KnnRank {
									rank,
									..
								} = &pipeline.code[at]
								else {
									unreachable!("a KNN query step at a KNN ranking instruction")
								};
								let super::program::KnnVector::Query(sort) = rank.vector else {
									unreachable!("a KNN query step for a literal query vector")
								};
								out_of_line(|| evaluate_knn_query_of(program, sort, state, ctx))
									.await?;
								pc = at;
							}
							Step::Measure {
								at,
							} => {
								let Instr::Measure {
									measure,
									field,
									dst,
								} = &pipeline.code[at]
								else {
									unreachable!("a measure step at a measure instruction")
								};
								let settled = state
									.measures
									.get(measure.call as usize)
									.is_some_and(Option::is_some);
								if !settled {
									out_of_line(|| settle_measure(program, measure, state, ctx))
										.await;
									pc = at;
								} else {
									// The host answers the row as the call's own instructions
									// do: the capability check, the query read with the row in
									// scope, then the registry's builtin over the query and the
									// row's field.
									let spec = &program.calls[measure.call as usize];
									frame_eval(program, state, ctx, None)
										.check_allowed_function(&spec.name)
										.map_err(ControlFlow::Err)?;
									let eval_ctx = frame_eval(program, state, ctx, None);
									let eval_ctx = row_eval(state, eval_ctx, &row);
									let query = match measure.query {
										super::measure::MeasureQuery::Const(index) => {
											program.consts[index as usize].clone()
										}
										super::measure::MeasureQuery::Param(param) => {
											let param = &program.params[param as usize];
											match super::function::frame_param(param, &eval_ctx)? {
												Some(value) => value,
												None => {
													out_of_line(|| {
														super::function::stored_param(
															program, param, &eval_ctx,
														)
													})
													.await?
												}
											}
										}
									};
									let value = interp::read_operand(
										regs,
										reader,
										program,
										super::program::Operand::Field(*field),
									)
									.map_err(ControlFlow::Err)?;
									let args = measure.args(value, query);
									let measured =
										out_of_line(|| call_builtin(spec, args, eval_ctx)).await?;
									regs[*dst as usize] = Scalar::Value(Held::Owned(measured));
									pc = at + 1;
								}
							}
							Step::Param {
								param,
								dst,
								frame,
								resume,
							} => {
								let eval_ctx = frame_eval(program, state, ctx, frame);
								let eval_ctx = row_eval(state, eval_ctx, &row);
								let spec = &program.params[param as usize];
								// A bound value is held, not copied: the row is
								// borrowed, as the registers already borrow it,
								// `NONE` is borrowed from the constant, and a
								// parameter of the context's chain shares the
								// allocation the context holds it in, so a path
								// into any of them reads it in place. Any other
								// binding the frame holds is copied.
								use super::function::FrameBinding;
								let held = match super::function::frame_binding(spec, &eval_ctx)? {
									Some(FrameBinding::Borrowed(value))
										if std::ptr::eq(value, &*row) =>
									{
										Held::Borrowed(&row)
									}
									Some(FrameBinding::Borrowed(Value::None)) => {
										Held::Borrowed(&Value::NONE)
									}
									Some(FrameBinding::Borrowed(value)) => {
										Held::Owned(value.clone())
									}
									Some(FrameBinding::Shared(value)) => {
										Held::Shared(Arc::clone(value))
									}
									None => Held::Owned(
										out_of_line(|| {
											super::function::stored_param(program, spec, &eval_ctx)
										})
										.await?,
									),
								};
								regs[dst as usize] = Scalar::Value(held);
								pc = resume;
							}
							Step::Access {
								at,
							} => {
								let Instr::Access {
									src,
									field,
									dst,
									frame,
								} = &pipeline.code[at]
								else {
									unreachable!("an access step at an access instruction")
								};
								// The base is read in place, never copied to read it.
								let base = interp::operand_value(regs, reader, program, *src)
									.map_err(ControlFlow::Err)?;
								let eval_ctx = frame_eval(program, state, ctx, *frame);
								let mut eval_ctx = row_eval(state, eval_ctx, &row);
								if pipeline.deref_windows {
									eval_ctx.link_windows = Some(&state.link_pool);
								}
								// Every access of a pipeline whose plan reads dereferences ahead
								// is offered the window, not only those of the keyed fields. The
								// window is keyed by record id and holds bytes that no principal's
								// permissions have touched, so an access of another field, or of a
								// record the window does not hold, reads for itself or takes bytes
								// it would have read, and judges them in its own frame.
								eval_ctx.link_ahead = state.link_ahead.as_ref().filter(|_| {
									pipeline
										.rows_ahead
										.as_ref()
										.is_some_and(|spec| spec.fetch.is_none())
								});
								let value = out_of_line(|| {
									crate::exec::vm::access::field::evaluate_field(
										&base,
										program.fields[*field as usize].as_str(),
										eval_ctx,
									)
								})
								.await?;
								regs[*dst as usize] = Scalar::Value(Held::Owned(value));
								pc = at + 1;
							}
							Step::Each {
								at,
							} => {
								let Instr::Each {
									src,
									dst,
									frame,
								} = &pipeline.code[at]
								else {
									unreachable!("an each step at an each instruction")
								};
								// The base is read in place, never copied to read it.
								let base = interp::operand_value(regs, reader, program, *src)
									.map_err(ControlFlow::Err)?;
								let eval_ctx = frame_eval(program, state, ctx, *frame);
								let mut eval_ctx = row_eval(state, eval_ctx, &row);
								if pipeline.deref_windows {
									eval_ctx.link_windows = Some(&state.link_pool);
								}
								let value = out_of_line(|| {
									crate::exec::vm::access::array_ops::evaluate_all(
										&base, eval_ctx,
									)
								})
								.await?;
								regs[*dst as usize] = Scalar::Value(Held::Owned(value));
								pc = at + 1;
							}
							Step::FetchRow {
								at,
							} => {
								let Instr::FetchRow {
									dst,
									none,
								} = &pipeline.code[at]
								else {
									unreachable!("a row fetch step at a row fetch instruction")
								};
								let Value::RecordId(rid) = &*row else {
									unreachable!("a row fetch step over a row that is a record id")
								};
								let frame = frame_ctx(state, ctx, None);
								let fetched = out_of_line(|| {
									crate::exec::operators::fetch::fetch_record(frame, rid)
								})
								.await?;
								if matches!(fetched, Value::None) {
									pc = *none as usize;
								} else {
									regs[*dst as usize] = Scalar::Value(Held::Owned(fetched));
									pc = at + 1;
								}
							}
							Step::Fetch {
								at,
							} => {
								let Instr::Fetch {
									fetch,
									src,
									dst,
								} = &pipeline.code[at]
								else {
									unreachable!("a fetch step at a fetch instruction")
								};
								let value = if *src == super::program::ROW {
									Value::clone(&row)
								} else {
									std::mem::replace(&mut regs[*src as usize], Scalar::None)
										.into_value()
								};
								let index = ProgramIndex {
									program,
								};
								let plan = &program.fetches[*fetch as usize];
								let links = crate::exec::operators::fetch::FetchLinks {
									pool: plan.windows.then_some(&state.link_pool),
									// Only the `FETCH` the plan read ahead for takes the links of
									// the batch.
									ahead: state.link_ahead.as_ref().filter(|_| {
										pipeline
											.rows_ahead
											.as_ref()
											.is_some_and(|spec| spec.fetch == Some(*fetch))
									}),
								};
								let fetched = out_of_line(|| {
									crate::exec::operators::fetch::fetch_fields_with(
										ctx,
										value,
										&plan.paths,
										&index,
										links,
									)
								})
								.await?;
								regs[*dst as usize] = Scalar::Value(Held::Owned(fetched));
								pc = at + 1;
							}
							Step::CallAllowed {
								call,
								frame,
								resume,
							} => {
								frame_eval(program, state, ctx, frame)
									.check_allowed_function(&program.calls[call as usize].name)
									.map_err(ControlFlow::Err)?;
								pc = resume;
							}
							Step::FieldTypes {
								at,
							} => {
								let Instr::CoerceField {
									reg,
									kind,
									field,
								} = &pipeline.code[at]
								else {
									unreachable!("a field-types step at a field coercion")
								};
								let value =
									std::mem::replace(&mut regs[*reg as usize], Scalar::None)
										.into_value();
								let judged = super::schema_host::check_types_held(
									ctx,
									&program.kinds[*kind as usize],
									value,
								)
								.await;
								match judged {
									Ok(value) => {
										regs[*reg as usize] = Scalar::Value(Held::Owned(value));
									}
									Err(error) => {
										return Err(ControlFlow::Err(
											interp::field_coerce(program, reader, *field, error)
												.map_err(ControlFlow::Err)?,
										));
									}
								}
								pc = at + 1;
							}
							Step::Call {
								at,
							} => {
								let Instr::Call {
									call,
									args,
									dst,
									frame,
									elem,
								} = &pipeline.code[at]
								else {
									unreachable!("a call step at a call instruction")
								};
								let args = args
									.iter()
									.map(|arg| interp::read_operand(regs, reader, program, *arg))
									.collect::<anyhow::Result<Vec<_>>>()
									.map_err(ControlFlow::Err)?;
								let element = elem.map(|elem| regs[elem as usize].to_value());
								let eval_ctx = frame_eval(program, state, ctx, *frame);
								let eval_ctx = row_eval(state, eval_ctx, &row);
								let eval_ctx = match &element {
									Some(element) => eval_ctx.with_element(element),
									None => eval_ctx,
								};
								let spec = &program.calls[*call as usize];
								let value =
									out_of_line(|| call_builtin(spec, args, eval_ctx)).await?;
								regs[*dst as usize] = Scalar::Value(Held::Owned(value));
								pc = at + 1;
							}
							Step::IndexFn {
								at,
							} => {
								let Instr::CallIndexFn {
									call,
									args,
									dst,
									frame,
								} = &pipeline.code[at]
								else {
									unreachable!(
										"an index-function step at an index-function instruction"
									)
								};
								if let Some(value) = knn_read(
									program,
									state,
									ctx,
									*frame,
									&row,
									&program.calls[*call as usize],
								)
								.await
								{
									regs[*dst as usize] = Scalar::Value(Held::Owned(value));
									pc = at + 1;
								} else {
									let mut args = args
										.iter()
										.map(|arg| {
											interp::read_operand(regs, reader, program, *arg)
										})
										.collect::<anyhow::Result<Vec<_>>>()
										.map_err(ControlFlow::Err)?;
									let spec = &program.calls[*call as usize];
									let queries = interp::runtime_queries(spec, &mut args)
										.map_err(ControlFlow::Err)?;
									let run = state
										.index_run_for(
											*call,
											spec,
											ctx.function_registry(),
											queries.as_deref(),
										)
										.map_err(ControlFlow::Err)?;
									let eval_ctx = frame_eval(program, state, ctx, *frame);
									let eval_ctx = row_eval(state, eval_ctx, &row);
									let value = out_of_line(|| async {
										run.function
											.invoke_async(&eval_ctx, &run.context, args)
											.await
											.map_err(ControlFlow::Err)
									})
									.await?;
									regs[*dst as usize] = Scalar::Value(Held::Owned(value));
									pc = at + 1;
								}
							}
							Step::Matches {
								at,
							} => {
								let Instr::Matches {
									matches,
									query,
									left,
									dst,
									frame,
								} = &pipeline.code[at]
								else {
									unreachable!("a matches step at a matches instruction")
								};
								let query = match query {
									Some(query) => Some(
										interp::read_operand(regs, reader, program, *query)
											.map_err(ControlFlow::Err)?,
									),
									None => None,
								};
								let left = match left {
									Some(left) => Some(
										interp::read_operand(regs, reader, program, *left)
											.map_err(ControlFlow::Err)?,
									),
									None => None,
								};
								let spec = &program.matches[*matches as usize];
								let eval = state.matches_eval(*matches, spec);
								let eval_ctx = frame_eval(program, state, ctx, *frame);
								let eval_ctx = row_eval(state, eval_ctx, &row);
								let value = out_of_line(|| {
									eval.evaluate(
										&eval_ctx,
										|| async {
											match (spec.left, left) {
												(Some(code), _) => {
													host_scalar(
														program,
														&super::program::HostScalar::Code(code),
														ctx,
														Some(&*row),
													)
													.await
												}
												(None, Some(value)) => Ok(value),
												(None, None) => {
													Err(ControlFlow::Err(anyhow::anyhow!(
														"a MATCHES read an operand it was lowered without"
													)))
												}
											}
										},
										|| async {
											query.ok_or_else(|| {
												ControlFlow::Err(anyhow::anyhow!(
													"a MATCHES read a query it was lowered without"
												))
											})
										},
									)
								})
								.await?;
								regs[*dst as usize] = Scalar::Value(Held::Owned(value));
								pc = at + 1;
							}
							Step::Method {
								at,
							} => {
								let (method_index, recv, args, dst, frame, closure) =
									if let Instr::Method {
										method,
										recv,
										args,
										dst,
										frame,
									} = &pipeline.code[at]
									{
										(method, recv, args, dst, frame, false)
									} else if let Instr::MethodClosure {
										method,
										recv,
										args,
										dst,
										frame,
									} = &pipeline.code[at]
									{
										(method, recv, args, dst, frame, true)
									} else {
										unreachable!("a method step at a method instruction")
									};
								let recv = interp::read_operand(regs, reader, program, *recv)
									.map_err(ControlFlow::Err)?;
								let args = args
									.iter()
									.map(|arg| interp::read_operand(regs, reader, program, *arg))
									.collect::<anyhow::Result<Vec<_>>>()
									.map_err(ControlFlow::Err)?;
								let method = &program.methods[*method_index as usize];
								let descriptor = state.method_descriptor(
									*method_index,
									method,
									ctx.function_registry(),
								);
								let eval_ctx = frame_eval(program, state, ctx, *frame);
								let eval_ctx = row_eval(state, eval_ctx, &row);
								let value = out_of_line(|| {
									call_method(method, descriptor, recv, args, closure, eval_ctx)
								})
								.await?;
								regs[*dst as usize] = Scalar::Value(Held::Owned(value));
								pc = at + 1;
							}
							Step::Function {
								at,
							} => {
								use super::program::CALL_FRAME;
								match &pipeline.code[at] {
									Instr::FunctionEnter {
										function,
										frame,
										outer,
										elem,
									} => {
										if state.functions.len() <= *function as usize {
											state.functions.resize_with(
												*function as usize + 1,
												Default::default,
											);
										}
										let mut known = std::mem::take(
											&mut state.functions[*function as usize],
										);
										let element =
											elem.map(|elem| regs[elem as usize].to_value());
										let eval_ctx = frame_eval(program, state, ctx, *outer);
										let eval_ctx = row_eval(state, eval_ctx, &row);
										let eval_ctx = match &element {
											Some(element) => eval_ctx.with_value(element),
											None => eval_ctx,
										};
										let spec = &program.functions[*function as usize];
										// Only a guard lowered as a unit reads the table.
										let units = match spec.guard {
											Some((_, super::program::GuardCode::Unit(_), _)) => {
												Some(unit_table(program, state, eval_ctx.exec_ctx)?)
											}
											Some((_, super::program::GuardCode::Host(_), _))
											| None => None,
										};
										let entered = out_of_line(|| {
											super::function::enter(
												program, spec, &mut known, &eval_ctx, units,
											)
										})
										.await;
										state.functions[*function as usize] = known;
										let (entered, principal) = entered?;
										let slot = (*frame - CALL_FRAME) as usize;
										if state.calls.len() <= slot {
											state.calls.resize_with(slot + 1, || None);
											state.call_principals.resize(slot + 1, 0);
										}
										state.calls[slot] = Some(entered);
										state.call_principals[slot] = principal;
									}
									Instr::FunctionBind {
										function,
										frame,
										args,
										params,
									} => {
										let args = args
											.iter()
											.map(|arg| {
												interp::read_operand(regs, reader, program, *arg)
											})
											.collect::<anyhow::Result<Vec<_>>>()
											.map_err(ControlFlow::Err)?;
										let entered = state.calls[(*frame - CALL_FRAME) as usize]
											.as_ref()
											.expect("a function bound after it is entered");
										let spec = &program.functions[*function as usize];
										let bound =
											super::function::bind_args(spec, entered, args)?;
										// The re-entry a call of the body would take.
										entered.limited.check_dive().map_err(ControlFlow::Err)?;
										for (i, value) in bound.into_iter().enumerate() {
											regs[*params as usize + i] =
												Scalar::Value(Held::Owned(value));
										}
									}
									Instr::FunctionReturn {
										function,
										frame,
										src,
										dst,
									} => {
										let value =
											interp::read_operand(regs, reader, program, *src)
												.map_err(ControlFlow::Err)?;
										let entered = state.calls[(*frame - CALL_FRAME) as usize]
											.take()
											.expect("a function returned from after it is entered");
										let spec = &program.functions[*function as usize];
										regs[*dst as usize] = Scalar::Value(Held::Owned(
											super::function::returned(spec, &entered, value)?,
										));
									}
									Instr::FunctionCall {
										function,
										frame,
										args,
										dst,
										outer,
										elem,
									} => {
										let args = args
											.iter()
											.map(|arg| {
												interp::read_operand(regs, reader, program, *arg)
											})
											.collect::<anyhow::Result<Vec<_>>>()
											.map_err(ControlFlow::Err)?;
										let element =
											elem.map(|elem| regs[elem as usize].to_value());
										let entered = state.calls[(*frame - CALL_FRAME) as usize]
											.take()
											.expect("a function called after it is entered");
										let spec = &program.functions[*function as usize];
										// A redefined function runs the call unit of the definition
										// the transaction reads now, from the caller's evaluation.
										let value = if entered.redefined {
											let mut linked = state
												.functions
												.get_mut(*function as usize)
												.and_then(|known| known.linked.take());
											let eval_ctx = frame_eval(program, state, ctx, *outer);
											let eval_ctx = row_eval(state, eval_ctx, &row);
											let eval_ctx = match &element {
												Some(element) => eval_ctx.with_value(element),
												None => eval_ctx,
											};
											let value = out_of_line(|| {
												super::call_unit::call(
													&spec.name,
													spec.plan_depth,
													args,
													&eval_ctx,
													&mut linked,
												)
											})
											.await;
											if let Some(known) =
												state.functions.get_mut(*function as usize)
											{
												known.linked = linked;
											}
											value?
										} else {
											let eval_ctx = frame_eval(program, state, ctx, *outer);
											let eval_ctx = row_eval(state, eval_ctx, &row);
											let eval_ctx = match &element {
												Some(element) => eval_ctx.with_value(element),
												None => eval_ctx,
											};
											let units =
												unit_table(program, state, eval_ctx.exec_ctx)?;
											let principal = state.call_principals
												[(*frame - CALL_FRAME) as usize];
											out_of_line(|| {
												super::function::call(
													units, spec, &entered, principal, &eval_ctx,
													args,
												)
											})
											.await?
										};
										regs[*dst as usize] = Scalar::Value(Held::Owned(value));
									}
									Instr::Dive {
										frame,
									} => {
										frame_ctx(state, ctx, *frame)
											.dive()
											.map_err(ControlFlow::Err)?;
									}
									Instr::CoerceField {
										..
									}
									| Instr::BeginComputing {
										..
									}
									| Instr::LandRow {
										..
									}
									| Instr::BindRow {
										..
									}
									| Instr::JumpIfPermissionsBypassed {
										..
									}
									| Instr::CompareFieldInt {
										..
									}
									| Instr::JumpIfFieldInt {
										..
									}
									| Instr::JumpIfCompare {
										..
									}
									| Instr::Compare {
										..
									}
									| Instr::Arith {
										..
									}
									| Instr::Unary {
										..
									}
									| Instr::NewArray {
										..
									}
									| Instr::ArrayPush {
										..
									}
									| Instr::LoadParam {
										..
									}
									| Instr::LoadSlot {
										..
									}
									| Instr::SortKey {
										..
									}
									| Instr::Fetch {
										..
									}
									| Instr::Access {
										..
									}
									| Instr::Each {
										..
									}
									| Instr::Throw {
										..
									}
									| Instr::FilterBegin {
										..
									}
									| Instr::MapBegin {
										..
									}
									| Instr::CollectPush {
										..
									}
									| Instr::Nav {
										..
									}
									| Instr::CallAllowed {
										..
									}
									| Instr::Call {
										..
									}
									| Instr::ArrayArg {
										..
									}
									| Instr::ElemNext {
										..
									}
									| Instr::ClosureBind {
										..
									}
									| Instr::ClosureReturn {
										..
									}
									| Instr::Subquery {
										..
									}
									| Instr::MakeClosure {
										..
									}
									| Instr::ClosureField {
										..
									}
									| Instr::RequireClosure {
										..
									}
									| Instr::Invoke {
										..
									}
									| Instr::Lookup {
										..
									}
									| Instr::Signal {
										..
									}
									| Instr::Extract {
										..
									}
									| Instr::KnnRank {
										..
									}
									| Instr::Measure {
										..
									}
									| Instr::Mock {
										..
									}
									| Instr::Recurse {
										..
									}
									| Instr::Destructure {
										..
									}
									| Instr::RecordId {
										..
									}
									| Instr::MethodResolve {
										..
									}
									| Instr::Method {
										..
									}
									| Instr::MethodClosure {
										..
									}
									| Instr::JumpIfNullish {
										..
									}
									| Instr::JumpIfNone {
										..
									}
									| Instr::JumpIf {
										..
									}
									| Instr::Skip {
										..
									}
									| Instr::Take {
										..
									}
									| Instr::JumpIfLimitReached {
										..
									}
									| Instr::NewObject {
										..
									}
									| Instr::SeedObject {
										..
									}
									| Instr::BuildObject {
										..
									}
									| Instr::LoadPath {
										..
									}
									| Instr::LoadRow {
										..
									}
									| Instr::FetchRow {
										..
									}
									| Instr::RemoveField {
										..
									}
									| Instr::RemovePath {
										..
									}
									| Instr::Omit {
										..
									}
									| Instr::SetField {
										..
									}
									| Instr::WriteProjection {
										..
									}
									| Instr::Load {
										..
									}
									| Instr::Emit {
										..
									}
									| Instr::EmitEnd {
										..
									}
									| Instr::BuildObjectEmit {
										..
									}
									| Instr::FilterFieldInt {
										..
									}
									| Instr::FilterCompare {
										..
									}
									| Instr::Filter {
										..
									}
									| Instr::SplitPush {
										..
									}
									| Instr::SortPush {
										..
									}
									| Instr::Distinct {
										..
									}
									| Instr::JoinProbe {
										..
									}
									| Instr::VerdictJump {
										..
									}
									| Instr::VerdictSet {
										..
									}
									| Instr::JoinPair {
										..
									}
									| Instr::GroupKey {
										..
									}
									| Instr::GroupSelect {
										..
									}
									| Instr::AccUpdate {
										..
									}
									| Instr::Jump {
										..
									}
									| Instr::Next {
										..
									}
									| Instr::Write(_)
									| Instr::Store(_)
									| Instr::Block(_)
									| Instr::EndRow
									| Instr::StopPipeline
									| Instr::TryNone {
										..
									}
									| Instr::TryEnd
									| Instr::MethodRegion {
										..
									}
									| Instr::PermissionRegion {
										..
									}
									| Instr::GroupFirstNeeded {
										..
									}
									| Instr::GroupFirstSet {
										..
									}
									| Instr::CallIndexFn {
										..
									}
									| Instr::Matches {
										..
									}
									| Instr::ExternalEnter {
										..
									}
									| Instr::ExternalCall {
										..
									}
									| Instr::NewSet {
										..
									}
									| Instr::Degree {
										..
									}
									| Instr::RepeatRecurse {
										..
									}
									| Instr::JumpIfNotObject {
										..
									}
									| Instr::JumpIfOpaque {
										..
									}
									| Instr::InvalidControlFlow
									| Instr::JumpIfNotRecordId {
										..
									}
									| Instr::DocumentRoot {
										..
									}
									| Instr::JudgeCopy {
										..
									}
									| Instr::Ir(_) => unreachable!("a function step at a function instruction"),
								}
								pc = at + 1;
							}
							Step::Subquery {
								at,
							} => {
								let Instr::Subquery {
									subquery,
									dst,
									frame,
									caught,
									doc,
								} = &pipeline.code[at]
								else {
									unreachable!("a subquery step at a subquery instruction")
								};
								let over = doc
									.map(|doc| {
										interp::read_operand(
											regs,
											reader,
											program,
											super::program::Operand::Reg(doc),
										)
									})
									.transpose()
									.map_err(ControlFlow::Err)?;
								let eval_ctx = frame_eval(program, state, ctx, *frame);
								let eval_ctx = match &over {
									Some(over) => eval_ctx.with_value_and_doc(over),
									None => row_eval(state, eval_ctx, &row),
								};
								let spec = &program.subqueries[*subquery as usize];
								let inputs = unit_inputs(regs, reader, program, &spec.inputs)
									.map_err(ControlFlow::Err)?;
								let units = unit_table(program, state, ctx)?;
								// The distances the search of the statement holding the
								// subquery publishes, in the slot of its group.
								let knn = state
									.knn_contexts
									.get(pipeline.group.map_or(0, |group| usize::from(group) + 1))
									.cloned()
									.flatten();
								let evaluated = out_of_line(|| {
									run_subprogram(&units, spec, eval_ctx, inputs, knn)
								})
								.await;
								match (evaluated, caught) {
									(Ok(value), _) => {
										regs[*dst as usize] = Scalar::Value(Held::Owned(value));
										pc = at + 1;
									}
									(
										Err(ControlFlow::Return(value)),
										Some(super::program::Catch {
											on_return: Some(catch),
											..
										}),
									) => {
										regs[catch.value as usize] =
											Scalar::Value(Held::Owned(value));
										pc = catch.label as usize;
									}
									(Err(ControlFlow::Err(e)), _) => {
										return Err(ControlFlow::Err(e));
									}
									(
										Err(ControlFlow::Break | ControlFlow::Continue),
										Some(super::program::Catch {
											loops_fail: true,
											..
										}),
									) => {
										return Err(ControlFlow::Err(anyhow::Error::new(
											crate::exec::Error::InvalidControlFlow,
										)));
									}
									(
										Err(ControlFlow::Break),
										Some(super::program::Catch {
											on_break: Some(target),
											..
										}),
									) => {
										pc = *target as usize;
									}
									(
										Err(ControlFlow::Continue),
										Some(super::program::Catch {
											on_continue: Some(target),
											..
										}),
									) => {
										pc = *target as usize;
									}
									(Err(signal), _) => {
										return Err(escaped_signal(program, *frame, signal));
									}
								}
							}
							Step::Closure {
								at,
							} => {
								let (value, dst) = if let Instr::MakeClosure {
									closure,
									slots,
									dst,
								} = &pipeline.code[at]
								{
									let index = *closure as usize;
									let spec = &program.closure_values[index];
									let mut captures = crate::dbs::Variables::new();
									for (name, slot) in spec.slots.iter().zip(slots) {
										let value =
											interp::read_operand(regs, reader, program, *slot)
												.map_err(ControlFlow::Err)?;
										captures.insert(name.as_str(), value);
									}
									let eval_ctx = frame_eval(program, state, ctx, spec.frame);
									// A parameter the body reads that is bound to a KNN
									// query vector's row being scanned has no value to
									// capture: the closure captures the refusal, which an
									// invocation raises.
									let mut forbidden = None;
									for name in &spec.framed {
										let bound = match eval_ctx.write_binding(name) {
											Some(value) => Some(value),
											None => match eval_ctx.exec_ctx.lexical_binding(name) {
												Some(crate::ctx::Binding::Value(value)) => {
													Some(value.as_ref())
												}
												Some(crate::ctx::Binding::Forbidden(row)) => {
													forbidden = Some(Arc::clone(row));
													None
												}
												None => None,
											},
										};
										if let Some(value) = bound {
											captures.insert(name.as_str(), value.clone());
										}
									}
									let compiled =
										crate::exec::vm::calls::CompiledClosure::planned(
											&spec.closure,
											spec.digest,
											captures,
										)
										.reading_row(spec.reads_row);
									let compiled = match forbidden {
										Some(row) => compiled.capturing_forbidden_row(row),
										None => compiled,
									};
									(
										Value::Closure(Box::new(crate::val::Closure::Builtin(
											Arc::new(compiled),
										))),
										*dst,
									)
								} else if let Instr::Invoke {
									target,
									args,
									dst,
									frame,
									elem,
								} = &pipeline.code[at]
								{
									let target =
										interp::read_operand(regs, reader, program, *target)
											.map_err(ControlFlow::Err)?;
									let Value::Closure(closure) = &target else {
										unreachable!("an invocation's target was checked a closure")
									};
									let mut values = Vec::with_capacity(args.len());
									for arg in args {
										values.push(
											interp::read_operand(regs, reader, program, *arg)
												.map_err(ControlFlow::Err)?,
										);
									}
									let element = elem.map(|elem| regs[elem as usize].to_value());
									let eval_ctx = frame_eval(program, state, ctx, *frame);
									let eval_ctx = row_eval(state, eval_ctx, &row);
									let eval_ctx = match &element {
										Some(element) => eval_ctx.with_element(element),
										None => eval_ctx,
									};
									// A unit the instruction linked runs; one the run's
									// table no longer holds standing is linked again.
									let relinking = relinked == Some(at);
									let value = match super::closure::compiled_of(&target) {
										Some(compiled) => {
											let ran = out_of_line(|| {
												compiled.invoke_linked(&eval_ctx, values, relinking)
											})
											.await;
											let Some(ran) = ran else {
												let digest = compiled.digest();
												state.forget_linked(*frame, digest);
												pc = at;
												continue;
											};
											ran?
										}
										None => {
											out_of_line(|| {
												crate::exec::vm::calls::helpers::invoke_uncompiled_closure(
														closure, values, &eval_ctx,
													)
											})
											.await?
										}
									};
									(value, *dst)
								} else {
									unreachable!("a closure step at a closure instruction")
								};
								regs[dst as usize] = Scalar::Value(Held::Owned(value));
								relinked = None;
								pc = at + 1;
							}
							Step::NeedUnit {
								at,
							} => {
								let (frame, closures) =
									closures_handed(program, &pipeline.code[at], regs, reader)
										.map_err(ControlFlow::Err)?;
								let mut linked = Vec::with_capacity(closures.len());
								{
									let eval_ctx = frame_eval(program, state, ctx, frame);
									let eval_ctx = row_eval(state, eval_ctx, &row);
									for closure in &closures {
										let Some(compiled) = super::closure::compiled_of(closure)
										else {
											continue;
										};
										if !state.has_linked(frame, compiled.digest()) {
											out_of_line(|| {
												super::closure::link(compiled, &eval_ctx)
											})
											.await?;
											linked.push(compiled.digest());
										}
									}
								}
								for digest in linked {
									state.note_linked(frame, digest);
								}
								relinked = Some(at);
								pc = at;
							}
							Step::Lookup {
								at,
							} => {
								let Instr::Lookup {
									lookup,
									src,
									dst,
									frame,
								} = &pipeline.code[at]
								else {
									unreachable!("a lookup step at a lookup instruction")
								};
								let origin = interp::read_operand(regs, reader, program, *src)
									.map_err(ControlFlow::Err)?;
								let spec = &program.lookups[*lookup as usize];
								let doc = if state.rowless {
									None
								} else {
									Some(&*row)
								};
								// What a write's gate, or a computed field derived for a
								// permission, reads is not permission-checked.
								let gate = frame.is_some_and(|frame| {
									super::program::is_permission_frame(Some(frame))
										&& matches!(
											program.permissions[frame as usize].mode,
											super::program::FrameMode::Gate(_)
												| super::program::FrameMode::Derived
										)
								});
								let frame = frame_ctx(state, ctx, *frame).clone();
								let frame = if gate {
									frame.with_skip_fetch_perms(true)
								} else {
									frame
								};
								let value = out_of_line(|| {
									super::graph::lookup(program, spec, origin, doc, state, &frame)
								})
								.await?;
								regs[*dst as usize] = Scalar::Value(Held::Owned(value));
								pc = at + 1;
							}
							Step::Recurse {
								at,
							} => {
								let (src, spec, target, dst) = if let Instr::Recurse {
									src,
									spec,
									target,
									dst,
								} = &pipeline.code[at]
								{
									(src, spec, target, dst)
								} else if let Instr::RepeatRecurse {
									src,
									dst,
								} = &pipeline.code[at]
								{
									let value = interp::read_operand(regs, reader, program, *src)
										.map_err(ControlFlow::Err)?;
									let eval_ctx = frame_eval(program, state, ctx, None);
									let eval_ctx = match &state.recursion {
										Some(recursion) => {
											eval_ctx.with_recursion_ctx(recursion.clone())
										}
										None => eval_ctx,
									};
									let value = out_of_line(|| {
										crate::exec::operators::recursion::evaluate_repeat_recurse(
											&value, eval_ctx,
										)
									})
									.await?;
									regs[*dst as usize] = Scalar::Value(Held::Owned(value));
									pc = at + 1;
									continue;
								} else {
									unreachable!("a recursion step at a recursion instruction")
								};
								let value = interp::read_operand(regs, reader, program, *src)
									.map_err(ControlFlow::Err)?;
								let target = target
									.map(|target| {
										interp::read_operand(regs, reader, program, target)
									})
									.transpose()
									.map_err(ControlFlow::Err)?;
								let skip = frame_eval(program, state, ctx, None).skip_fetch_perms;
								let frame = frame_ctx(state, ctx, None).clone();
								let frame = if skip {
									frame.with_skip_fetch_perms(true)
								} else {
									frame
								};
								// Every depth reads the inputs, evaluated once, from the frame it
								// runs in.
								let frame = match spec.inputs.is_empty() {
									true => frame,
									false => frame.with_params(
										unit_inputs(regs, reader, program, &spec.inputs)
											.map_err(ControlFlow::Err)?,
									),
								};
								let units = match spec.body {
									Some(_) => Some(unit_table(program, state, ctx)?),
									None => None,
								};
								let value = out_of_line(|| {
									recurse(spec, units, &value, target.as_ref(), &frame)
								})
								.await?;
								regs[*dst as usize] = Scalar::Value(Held::Owned(value));
								pc = at + 1;
							}
							Step::Signal {
								at,
							} => {
								let Instr::Signal {
									brk,
									frame,
								} = &pipeline.code[at]
								else {
									unreachable!("a signal step at a signal instruction")
								};
								let signal = if *brk {
									ControlFlow::Break
								} else {
									ControlFlow::Continue
								};
								return Err(escaped_signal(program, *frame, signal));
							}
							Step::Omit {
								at,
							} => {
								let Instr::Omit {
									object,
									walk,
								} = &pipeline.code[at]
								else {
									unreachable!("an omit step at an omit instruction")
								};
								let mut value =
									std::mem::replace(&mut regs[*object as usize], Scalar::None)
										.into_value();
								let frame = frame_ctx(state, ctx, None).clone();
								out_of_line(|| {
									super::omit::apply(program, walk, &mut value, state, &frame)
								})
								.await?;
								regs[*object as usize] = Scalar::Value(Held::Owned(value));
								pc = at + 1;
							}
							Step::Destructure {
								at,
							} => {
								let Instr::Destructure {
									src,
									fields,
									dst,
									frame,
								} = &pipeline.code[at]
								else {
									unreachable!("a destructure step at a destructure instruction")
								};
								let value = interp::read_operand(regs, reader, program, *src)
									.map_err(ControlFlow::Err)?;
								let skip = frame_eval(program, state, ctx, *frame).skip_fetch_perms;
								let frame = frame_ctx(state, ctx, *frame).clone();
								let value = out_of_line(|| {
									destructure(program, fields, value, skip, state, &frame)
								})
								.await?;
								regs[*dst as usize] = Scalar::Value(Held::Owned(value));
								pc = at + 1;
							}
							Step::JudgeCopy {
								at,
							} => {
								let Instr::JudgeCopy {
									pipeline: judge,
									src,
									dst,
								} = &pipeline.code[at]
								else {
									unreachable!("a judge step at a judge instruction")
								};
								let value = interp::read_operand(
									regs,
									reader,
									program,
									super::program::Operand::Reg(*src),
								)
								.map_err(ControlFlow::Err)?;
								let frame = state.frame.clone();
								let verdict = out_of_line(|| {
									super::write::run_image(
										program, *judge, value, frame, state, ctx,
									)
								})
								.await?;
								regs[*dst as usize] = Scalar::Value(Held::Owned(verdict));
								pc = at + 1;
							}
							Step::Block {
								at,
							} => {
								let Instr::Block(instr) = &pipeline.code[at] else {
									unreachable!("a block step at a block instruction")
								};
								// Entering a block evaluated in place did its work when the
								// step was counted.
								if let super::program::BlockInstr::Enter = instr {
									pc = at + 1;
									continue;
								}
								if let super::program::BlockInstr::Nested {
									dst,
									caught,
									frame,
									..
								}
								| super::program::BlockInstr::Member {
									dst,
									caught,
									frame,
									..
								} = instr
								{
									let doc = (!state.rowless).then_some(&*row);
									let evaluated = if let super::program::BlockInstr::Member {
										probe,
										..
									} = instr
									{
										let probe = regs[*probe as usize].to_value();
										out_of_line(|| {
											super::nested::member(
												program, instr, doc, &probe, state, ctx,
											)
										})
										.await
										.map(Scalar::Bool)
									} else {
										out_of_line(|| {
											super::nested::statement(
												program, instr, doc, state, ctx,
											)
										})
										.await
										.map(|value| Scalar::Value(Held::Owned(value)))
									};
									match (evaluated, caught) {
										(Ok(value), _) => {
											regs[*dst as usize] = value;
											pc = at + 1;
										}
										(
											Err(ControlFlow::Return(value)),
											Some(super::program::Catch {
												on_return: Some(catch),
												..
											}),
										) => {
											regs[catch.value as usize] =
												Scalar::Value(Held::Owned(value));
											pc = catch.label as usize;
										}
										(
											Err(ControlFlow::Break | ControlFlow::Continue),
											Some(super::program::Catch {
												loops_fail: true,
												..
											}),
										) => {
											return Err(ControlFlow::Err(anyhow::Error::new(
												crate::exec::Error::InvalidControlFlow,
											)));
										}
										(
											Err(ControlFlow::Break),
											Some(super::program::Catch {
												on_break: Some(target),
												..
											}),
										) => {
											pc = *target as usize;
										}
										(
											Err(ControlFlow::Continue),
											Some(super::program::Catch {
												on_continue: Some(target),
												..
											}),
										) => {
											pc = *target as usize;
										}
										(Err(signal), _) => {
											return Err(escaped_signal(program, *frame, signal));
										}
									}
									continue;
								}
								// Boxed: a block step runs a statement's group, whose
								// pipelines run beneath it.
								pc = match out_of_line(|| {
									super::block::step(program, instr, regs, state, ctx)
								})
								.await?
								{
									Some(target) => target,
									None => at + 1,
								};
							}
							Step::Store {
								at,
							} => {
								let Instr::Store(instr) = &pipeline.code[at] else {
									unreachable!("a store step at a store instruction")
								};
								// Boxed: a store step's future is large, and this one
								// is part of every row's.
								let target = if instr.is_dml() {
									out_of_line(|| {
										super::store::step_dml(
											program,
											instr,
											regs,
											&mut state.reserved_taken,
											ctx,
										)
									})
									.await?
								} else {
									// The runtime of every other store instruction is the
									// surface's: an entry that serves data statements alone
									// holds none.
									let Some(schema) = ctx.ctx().surface().schema_steps else {
										return Err(ControlFlow::Err(anyhow::Error::new(
											crate::exec::Error::PlannerUnsupported {
												reason: super::ir_lower::NOT_SERVED.into(),
											},
										)));
									};
									if matches!(
										instr,
										StoreInstr::EvaluateOver { .. }
											| StoreInstr::BackfillRow { .. }
											| StoreInstr::BackfillGroup { .. }
											| StoreInstr::BackfillGroupRow { .. }
											| StoreInstr::RetireViewRows { .. }
									) {
										// The surface's steps answer a boxed future already.
										schema.backfill(program, instr, regs, state, ctx).await?
									} else {
										// A blocking build lends the statement's registration
										// with the node's place pool to the build it waits on.
										let lent = if let StoreInstr::BuildIndex {
											blocking: true,
											..
										} = instr
										{
											Some(super::morsel::places::of(state).clone())
										} else {
											None
										};
										schema
											.step(
												program,
												instr,
												regs,
												&mut state.reserved_taken,
												ctx,
												lent,
											)
											.await?
									}
								};
								pc = match target {
									Some(target) => target,
									None => at + 1,
								};
							}
							Step::WriteProjection {
								at,
							} => {
								let Instr::WriteProjection {
									call,
									args,
									object,
									path,
									aliased,
								} = &pipeline.code[at]
								else {
									unreachable!("a projection step at a projection instruction")
								};
								let args = args
									.iter()
									.map(|arg| interp::read_operand(regs, reader, program, *arg))
									.collect::<anyhow::Result<Vec<_>>>()
									.map_err(ControlFlow::Err)?;
								let eval_ctx = frame_eval(program, state, ctx, None);
								let eval_ctx = row_eval(state, eval_ctx, &row);
								let bindings = out_of_line(|| {
									project_call(&program.calls[*call as usize], args, eval_ctx)
								})
								.await?;
								let mut writes = Vec::new();
								crate::exec::operators::projection_writes(
									bindings,
									*aliased,
									&program.paths[*path as usize],
									&mut writes,
								);
								if let Scalar::Value(held) = &mut regs[*object as usize] {
									let object = held.to_mut();
									for (path, value) in writes {
										object.set_at_field_path_owned(&path, value);
									}
								}
								pc = at + 1;
							}
							Step::External {
								at,
							} => {
								if let Instr::ExternalEnter {
									external,
									frame,
								} = &pipeline.code[at]
								{
									let spec = &program.externals[*external as usize];
									let eval_ctx = frame_eval(program, state, ctx, *frame);
									let eval_ctx = row_eval(state, eval_ctx, &row);
									let entered = out_of_line(|| {
										super::external::enter(program, spec, &eval_ctx)
									})
									.await?;
									let at = *external as usize;
									if state.externals.len() <= at {
										state.externals.resize_with(at + 1, || None);
									}
									state.externals[at] = Some(entered);
								} else if let Instr::ExternalCall {
									external,
									args,
									this,
									dst,
									frame,
								} = &pipeline.code[at]
								{
									let spec = &program.externals[*external as usize];
									let args = args
										.iter()
										.map(|arg| {
											interp::read_operand(regs, reader, program, *arg)
										})
										.collect::<anyhow::Result<Vec<_>>>()
										.map_err(ControlFlow::Err)?;
									let this = this
										.map(|this| {
											interp::read_operand(regs, reader, program, this)
										})
										.transpose()
										.map_err(ControlFlow::Err)?;
									let entered = state
										.externals
										.get_mut(*external as usize)
										.and_then(Option::take)
										.ok_or_else(|| {
											ControlFlow::Err(anyhow::Error::new(
												crate::err::EngineError::Internal(
													"an external call made before it was entered"
														.to_owned(),
												),
											))
										})?;
									let site = *external as usize;
									let mut held =
										state.call_units.get_mut(site).and_then(Option::take);
									let value = {
										let eval_ctx = frame_eval(program, state, ctx, *frame);
										let eval_ctx = row_eval(state, eval_ctx, &row);
										let eval_ctx = match &this {
											Some(this) => eval_ctx.with_value(this),
											None => eval_ctx,
										};
										out_of_line(|| {
											super::external::call(
												spec, entered, args, &eval_ctx, &mut held,
											)
										})
										.await
									};
									if held.is_some() {
										if state.call_units.len() <= site {
											state.call_units.resize_with(site + 1, || None);
										}
										state.call_units[site] = held;
									}
									let value = value?;
									regs[*dst as usize] = Scalar::Value(Held::Owned(value));
								} else {
									unreachable!("an external step at an external instruction")
								}
								pc = at + 1;
							}
							Step::Degree {
								at,
							} => {
								let Instr::Degree {
									degree,
									src,
									dst,
									done,
								} = &pipeline.code[at]
								else {
									unreachable!("a degree step at a degree instruction")
								};
								let current = interp::read_operand(regs, reader, program, *src)
									.map_err(ControlFlow::Err)?;
								// The adjacency's storage answers for a run whose reads go
								// unchecked, may read the database and read the current state;
								// any other run takes the call, which reads what the run may see
								// as it stood or refuses the read as the lookup does.
								let counts = ctx.version_stamp().is_none()
									&& !ctx
										.should_check_perms(crate::iam::Action::View)
										.map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?
									&& ctx.database().is_ok_and(|db_ctx| {
										crate::exec::permission::validate_record_user_access(db_ctx)
											.is_ok()
									});
								let source =
									DegreeCounter::source(Some(&current)).filter(|_| counts);
								let counted = match source {
									Some(rid) => {
										let counter = state
											.degree_counter(
												*degree,
												&program.degrees[*degree as usize],
											)
											.map_err(ControlFlow::Err)?;
										let eval_ctx = frame_eval(program, state, ctx, None);
										out_of_line(|| async {
											counter
												.degree(&eval_ctx, &rid)
												.await
												.map_err(ControlFlow::Err)
										})
										.await?
									}
									None => None,
								};
								pc = match counted {
									Some(count) => {
										regs[*dst as usize] =
											Scalar::Value(Held::Owned(Value::Number(count.into())));
										*done as usize
									}
									None => at + 1,
								};
							}
							Step::Ir {
								at,
							} => {
								let Instr::Ir(instr) = &pipeline.code[at] else {
									unreachable!("an IR step at an IR instruction")
								};
								let services = EngineServices::new(ctx);
								let Some(host) = services.ir_host() else {
									return Err(ControlFlow::Err(anyhow::anyhow!(
										"an IR instruction ran on a host with no IR service"
									)));
								};
								match out_of_line(|| {
									super::ir_host::run(
										host, instr, at, program, reader, regs, state,
									)
								})
								.await?
								{
									IrResume::At(resume) => pc = resume,
									IrResume::EndRow => break Ok(Stretch::Ended(Ended::Next)),
									IrResume::StopPipeline => {
										break Ok(Stretch::Ended(Ended::Stop));
									}
								}
							}
							Step::Next => break Ok(Stretch::Ended(Ended::Next)),
							Step::Stop => break Ok(Stretch::Ended(Ended::Stop)),
							Step::LandRow {
								resume,
							} => break Ok(Stretch::Land(resume)),
							Step::BindRow {
								name,
								resume,
							} => break Ok(Stretch::Bind(name, resume)),
							Step::Write {
								at,
							} => break Ok(Stretch::Write(at)),
						}
					}
				}
				.await;
				match attempt {
					Ok(step) => break step,
					Err(mut error) => loop {
						match state.tries.last().copied() {
							// An error leaving a method's body is the method's,
							// in the frame that names it so.
							Some(Region::Method {
								method,
							}) if state.tries.len() > base => {
								state.tries.pop();
								if !ctx.root().writing
									&& !program.methods[method as usize].writing
									&& let ControlFlow::Err(e) = &error
									&& let Some(crate::expr::Error::InvalidFunctionArguments {
										message,
										..
									}) = e.downcast_ref::<crate::expr::Error>()
								{
									error = ControlFlow::Err(method_arguments_error(
										&program.methods[method as usize].name,
										message,
									));
								}
							}
							Some(Region::Permission {
								check,
							}) if state.tries.len() > base => {
								state.tries.pop();
								if let ControlFlow::Err(e) = &mut error {
									let raised = std::mem::replace(e, anyhow::anyhow!(""));
									*e = if state.read_in_place_permission_errors
										&& check == super::program::PermissionCheck::Table
									{
										raised.context("Permission check failed")
									} else {
										anyhow::anyhow!("{}: {raised}", check.prefix())
									};
								}
							}
							Some(Region::Try {
								catch,
								dst,
								nested,
							}) if state.tries.len() > base
								&& error.is_ignorable() && ctx.nested_runs() == nested =>
							{
								state.tries.pop();
								if let Some(dst) = dst {
									regs[dst as usize] = Scalar::None;
								}
								pc = catch;
								break;
							}
							_ => return Err(error),
						}
					},
				}
			}
		};
		// A layer rewrites the row, so nothing read from it survives.
		match step {
			Stretch::Land(resume) => {
				land(state, &mut row);
				start = resume;
			}
			Stretch::Bind(name, resume) => {
				let mut bound = crate::val::Object::default();
				bound.insert(program.fields[name as usize].as_str(), row.into_owned());
				row = Cow::Owned(Value::Object(bound));
				start = resume;
			}
			Stretch::Write(at) => {
				// Boxed: a write step's future is large, and this one is part
				// of every row's. The record's run of write instructions is
				// taken in the one step.
				start = out_of_line(|| write::steps(program, pipeline, at, state, row.to_mut()))
					.await?;
			}
			Stretch::Ended(ended) => break ended,
		}
	};
	if let Some(Pending::Split {
		split,
		value,
	}) = state.pending.take_if(|pending| matches!(pending, Pending::Split { .. }))
	{
		return run_split(program, split, value.unwrap_or_else(|| row.into_owned()), state, ctx)
			.await;
	}
	if let Some(Pending::Join {
		join,
		keys,
	}) = state.pending.take_if(|pending| matches!(pending, Pending::Join { .. }))
	{
		return run_join(program, join, row.into_owned(), keys, state, ctx).await;
	}
	if let Some(Pending::JoinPair {
		join,
	}) = state.pending.take_if(|pending| matches!(pending, Pending::JoinPair { .. }))
	{
		return run_join_pair(program, join, row.into_owned(), state, ctx).await;
	}
	if state.pending.is_some() {
		state.settle(program, row.into_owned());
	}
	Ok(step)
}

/// Run the rows split `split` turns `row` into through the split's pipeline,
/// in order: `Stop` once that pipeline takes no more rows, so the pipeline
/// that handed over `row` takes no more of its own.
async fn run_split(
	program: &Program,
	split: u16,
	row: Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Ended> {
	let spec = &program.splits[split as usize];
	let mut below = Below {
		program,
		pipeline: &program.pipelines[spec.pipeline],
		ctx,
	};
	match &spec.kind {
		super::program::SplitKind::Targets(targets) => {
			if targets.len() <= 1 {
				let mut rows = Vec::new();
				crate::exec::operators::split_row(row, targets, &mut rows);
				return below.run(rows, state).await;
			}
			let mut stack = vec![(0usize, row)];
			let mut rows = Vec::new();
			while let Some((idx, current)) = stack.pop() {
				if idx + 1 >= targets.len() {
					crate::exec::operators::split_row(current, &targets[idx..], &mut rows);
					if matches!(below.run(std::mem::take(&mut rows), state).await?, Ended::Stop) {
						return Ok(Ended::Stop);
					}
				} else {
					crate::exec::operators::split_row(current, &targets[idx..idx + 1], &mut rows);
					super::fuel::tick(ctx, rows.len().clamp(1, u32::MAX as usize) as u32).await?;
					for next in rows.drain(..).rev() {
						stack.push((idx + 1, next));
					}
				}
			}
			Ok(Ended::Next)
		}
		super::program::SplitKind::Step(step) => {
			let row = match super::binding::keep_row(step, split, row, state) {
				Ok(()) => return Ok(Ended::Next),
				Err(row) => row,
			};
			let stepped = out_of_line(|| {
				super::binding::run_step(program, split, step, row, state, ctx, &mut below)
			})
			.await?;
			match stepped {
				super::binding::Stepped::Rows(rows) => below.run(rows, state).await,
				super::binding::Stepped::Delivered(step) => Ok(step),
			}
		}
		super::program::SplitKind::Window(step) => {
			out_of_line(|| {
				super::window_step::run_window(program, split, step, row, state, ctx, &mut below)
			})
			.await
		}
	}
}

/// The pipeline below a split, as the place the rows the split yields go.
struct Below<'a> {
	program: &'a Program,
	pipeline: &'a Pipeline,
	ctx: &'a ExecutionContext,
}

impl Below<'_> {
	/// `rows`, in order, through the pipeline: `Stop` once it takes no more.
	///
	/// The rows of a call share one future, built out of line, whose size is
	/// moved once, and not once for each row.
	async fn run(&self, rows: Vec<Value>, state: &mut State) -> FlowResult<Ended> {
		let mut rows = rows.into_iter();
		let Some(split) = forwarded_split(self.pipeline) else {
			return out_of_line(|| self.each(rows, state)).await;
		};
		// A split that only keeps what it is given keeps it here, with no
		// future built for the row.
		if let super::program::SplitKind::Step(step) = &self.program.splits[split as usize].kind {
			while let Some(row) = rows.next() {
				super::fuel::tick(self.ctx, 1).await?;
				match super::binding::keep_row(step, split, row, state) {
					Ok(()) => {}
					Err(row) => {
						return out_of_line(|| self.forward(split, row, rows, state)).await;
					}
				}
			}
			return Ok(Ended::Next);
		}
		match rows.next() {
			Some(row) => out_of_line(|| self.forward(split, row, rows, state)).await,
			None => Ok(Ended::Next),
		}
	}

	/// `rows` through the pipeline, one row at a time.
	async fn each(&self, rows: std::vec::IntoIter<Value>, state: &mut State) -> FlowResult<Ended> {
		for row in rows {
			if run_value_row(self.program, self.pipeline, row, state, self.ctx).await?
				== Ended::Stop
			{
				return Ok(Ended::Stop);
			}
		}
		Ok(Ended::Next)
	}

	/// `first` and `rest` to split `split`, which the pipeline forwards its
	/// rows to.
	async fn forward(
		&self,
		split: u16,
		first: Value,
		rest: std::vec::IntoIter<Value>,
		state: &mut State,
	) -> FlowResult<Ended> {
		for row in std::iter::once(first).chain(rest) {
			super::fuel::tick(self.ctx, 1).await?;
			if run_split(self.program, split, row, state, self.ctx).await? == Ended::Stop {
				return Ok(Ended::Stop);
			}
		}
		Ok(Ended::Next)
	}
}

/// The split a pipeline hands every row it is given to, as it is given it,
/// where the pipeline does nothing else: its row is the split's.
fn forwarded_split(pipeline: &Pipeline) -> Option<u16> {
	if pipeline.enforce.is_some() || !pipeline.limits.is_empty() {
		return None;
	}
	match pipeline.code.as_slice() {
		[
			Instr::SplitPush {
				split,
				src: super::program::ROW,
			},
			Instr::EndRow,
			Instr::StopPipeline,
		] => Some(*split),
		_ => None,
	}
}

impl super::binding::StepSink for Below<'_> {
	fn rows<'a>(
		&'a mut self,
		rows: Vec<Value>,
		state: &'a mut State,
	) -> futures::future::BoxFuture<'a, FlowResult<Ended>> {
		crate::exec::vm::host::out_of_line_boxed(|| self.run(rows, state))
	}
}

/// Pair the probe row `probe`, keyed by `keys`, with the rows join `join`
/// holds for those keys, and run each pairing through the join's pipeline in
/// order: `Stop` once that pipeline takes no more rows, so the pipeline that
/// handed over `probe` takes no more of its own.
///
/// The build side is read when the first probe row arrives, and held until the
/// join's owner starts again. A pairing is the probe row with the build row
/// set under the join's alias; a probe row that is no object is paired as it
/// is. A build row pairs only if the residual holds of the row paired, and a
/// [`JoinMatch::Left`](super::payload::JoinMatch::Left) join emits a probe row no
/// build row pairs with once, with `NULL` under the alias. A
/// [`JoinMatch::Semi`](super::payload::JoinMatch::Semi) join emits a probe row
/// some build row matches once, as it is, and judges no residual.
async fn run_join(
	program: &Program,
	join: u16,
	probe: Value,
	keys: Vec<Value>,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Ended> {
	let spec = &program.joins[join as usize];
	if spec.streams.is_some() {
		return hold_join_row(join, probe, keys, state);
	}
	// The right input read through the index for this row's key alone,
	// judged below as the build table is. A key the index may not list every
	// equal record under reads the build table, which a lookup would read
	// whole for this row alone.
	let looked = match spec.lookup {
		Some((group, key))
			if state.joins[join as usize].is_none()
				&& state.join_lookups[join as usize] < super::payload::LOOKUP_EVALUATIONS
				&& keys.get(key).is_some_and(lookup_key) =>
		{
			state.join_lookups[join as usize] += 1;
			let held = state.lookup_key.replace(keys.get(key).cloned().unwrap_or(Value::None));
			let built = out_of_line(|| build_table(program, spec, group, state, ctx)).await;
			state.lookup_key = held;
			Some(Arc::new(built?))
		}
		_ => None,
	};
	if looked.is_none() && state.joins[join as usize].is_none() {
		let built = out_of_line(|| build_join(program, spec, state, ctx)).await?;
		state.joins[join as usize] = Some(built);
	}
	let table = match (looked, &mut state.joins[join as usize]) {
		(Some(table), _) => table,
		(None, Some(super::join_spill::JoinBuilt::Memory(table))) => Arc::clone(table),
		(None, Some(super::join_spill::JoinBuilt::Spilled(spilled))) => {
			// A join whose build side spilled defers its probe rows to its
			// own pipeline's turn ([`finish_spilled_join`]).
			let mut pairer = PlanPairer {
				program,
				spec,
				ctx,
				eval: None,
			};
			spilled.defer(Some(keys), probe, &mut pairer).await?;
			return Ok(Ended::Next);
		}
		(None, None) => return Ok(Ended::Next),
	};
	pair_built(program, spec, &table, probe, keys, state, ctx).await
}

/// Pair the probe row `probe`, keyed by `keys`, with the rows `table` holds
/// for those keys, as [`run_join`] pairs them.
async fn pair_built(
	program: &Program,
	spec: &super::program::JoinSpec,
	table: &crate::exec::operators::BuildTable,
	probe: Value,
	keys: Vec<Value>,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Ended> {
	use super::payload::JoinMatch;
	let pipeline = &program.pipelines[spec.pipeline];
	if spec.kind == JoinMatch::Semi {
		if table.matches(&keys).next().is_none() {
			return Ok(Ended::Next);
		}
		return out_of_line(|| run_value_row(program, pipeline, probe, state, ctx)).await;
	}
	let mut matched = false;
	let mut eval = None;
	// The probe row is paired in place with the last build row it meets,
	// which is the only one when its key is unique.
	let mut probe = Some(probe);
	let mut builds = table.matches(&keys).peekable();
	while let Some(build) = builds.next() {
		// A pairing is a unit, whether or not the residual admits it.
		super::fuel::tick(ctx, 1).await?;
		let held = probe.as_ref().expect("the probe row is held until it is paired");
		let paired = match &spec.residual {
			Some(residual) => {
				let row = if let Value::Object(_) = held {
					crate::exec::operators::with_alias(held, &spec.alias, build.clone())
				} else {
					let mut row = crate::val::Object::default();
					row.insert(spec.alias.clone(), build.clone());
					Value::Object(row)
				};
				let eval = match &mut eval {
					Some(eval) => eval,
					slot => slot.insert(statement_state(program, ctx).map_err(ControlFlow::Err)?),
				};
				let answer =
					out_of_line(|| host_scalar_in(program, residual, ctx, &row, eval)).await?;
				if !answer.is_truthy() {
					continue;
				}
				if let Value::Object(_) = held {
					// The row the residual judged is the pairing.
					row
				} else {
					held.clone()
				}
			}
			None if builds.peek().is_none() => {
				let held = probe.take().expect("the probe row is held until it is paired");
				crate::exec::operators::with_alias_owned(held, &spec.alias, build.clone())
			}
			None => crate::exec::operators::with_alias(held, &spec.alias, build.clone()),
		};
		matched = true;
		if out_of_line(|| run_value_row(program, pipeline, paired, state, ctx)).await?
			== Ended::Stop
		{
			return Ok(Ended::Stop);
		}
	}
	if !matched
		&& spec.kind == JoinMatch::Left
		&& let Some(probe) = probe
	{
		let paired = crate::exec::operators::with_alias_owned(probe, &spec.alias, Value::Null);
		if out_of_line(|| run_value_row(program, pipeline, paired, state, ctx)).await?
			== Ended::Stop
		{
			return Ok(Ended::Stop);
		}
	}
	Ok(Ended::Next)
}

/// The build side of join `spec`: its build side's rows, each keyed by the
/// build keys evaluated over the row set under the alias. Where the query
/// can spill, the rows are read a batch at a time and spilled once the lease
/// no longer covers them ([`super::join_spill`]); otherwise they are
/// gathered into a table.
async fn build_join(
	program: &Program,
	spec: &super::program::JoinSpec,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<super::join_spill::JoinBuilt> {
	let held = state.program.clone().ok_or_else(|| {
		ControlFlow::Err(anyhow::anyhow!("a join in a run that does not hold its program"))
	})?;
	let spills = ctx.ctx().memory().is_some_and(|memory| memory.can_spill())
		&& !held.groups.get(spec.build as usize).is_some_and(|group| group.only);
	if !spills {
		let table = build_table(program, spec, spec.build, state, ctx).await?;
		return Ok(super::join_spill::JoinBuilt::Memory(Arc::new(table)));
	}
	let mut sink = PlanBuildSink {
		inner: super::join_spill::BuildSink::with_width(
			spec.build_keys.len(),
			usize::MAX,
			spec.build,
			ctx,
		),
		program,
		spec,
		eval: None,
	};
	run_group_into(&held, spec.build, state, ctx, &mut sink).await?;
	Ok(match sink.inner.finish(ctx).await? {
		super::join_spill::Built::Memory(table) => {
			super::join_spill::JoinBuilt::Memory(Arc::new(table))
		}
		super::join_spill::Built::Spilled(spilled) => {
			super::join_spill::JoinBuilt::Spilled(spilled)
		}
	})
}

/// The in-memory build table of join `spec` over the rows group `group`
/// reads — its build side, or its right input read through an index for one
/// left row's key — each keyed by the build keys evaluated over the row set
/// under the alias.
async fn build_table(
	program: &Program,
	spec: &super::program::JoinSpec,
	group: u16,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<crate::exec::operators::BuildTable> {
	let held = state.program.clone().ok_or_else(|| {
		ControlFlow::Err(anyhow::anyhow!("a join in a run that does not hold its program"))
	})?;
	let mut eval = None;
	let (rows, ran) = run_group(&held, group, state, ctx).await;
	ran?;
	let mut table = crate::exec::operators::BuildTable::new();
	for row in rows {
		// A row keyed and held is a unit of the build's work.
		super::fuel::tick(ctx, 1).await?;
		let (key, row) = build_key(program, spec, row, &mut eval, ctx).await?;
		table.insert(key, row, usize::MAX)?;
	}
	Ok(table)
}

/// Hold `probe`, keyed by `keys`, the one row the left input of join `join`
/// hands it, for the right input to stream past ([`run_join_pair`]). A left
/// key that holds a regex keeps the right rows keyed instead, to pair them as
/// the build table does once the right input ends.
fn hold_join_row(
	join: u16,
	probe: Value,
	keys: Vec<Value>,
	state: &mut State,
) -> FlowResult<Ended> {
	let slot = &mut state.held[join as usize];
	if slot.is_some() {
		return Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
			"a join that holds its left input's one row was handed another".to_owned(),
		))));
	}
	let table =
		crate::exec::operators::key_has_regex(&keys).then(crate::exec::operators::BuildTable::new);
	*slot = Some(Box::new(interp::HeldJoin {
		probe,
		keys,
		matched: false,
		stopped: false,
		regex: Vec::new(),
		table,
		eval: None,
	}));
	Ok(Ended::Next)
}

/// Pair `row`, a row of the right input of join `join`, with the left row
/// the join holds, where the row's key equals the held key as the build
/// table's ordering equates them, and run the pairing through the join's
/// pipeline: `Stop` once that pipeline takes no more rows, so the right
/// input's pipeline takes no more of its own. A row whose key holds a regex
/// that matches the held key is kept, to pair once the right input ends
/// ([`finish_held_join`]).
async fn run_join_pair(
	program: &Program,
	join: u16,
	row: Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Ended> {
	let spec = &program.joins[join as usize];
	let Some(held) = state.held[join as usize].as_mut() else {
		return Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
			"a right row reached a join that holds no left row".to_owned(),
		))));
	};
	let mut eval = held.eval.take();
	let keyed = out_of_line(|| build_key(program, spec, row, &mut eval, ctx)).await;
	let held =
		state.held[join as usize].as_mut().expect("the held row is kept while its key is read");
	held.eval = eval;
	let (key, row) = keyed?;
	if let Some(table) = &mut held.table {
		table.insert(key, row, usize::MAX)?;
		return Ok(Ended::Next);
	}
	if crate::exec::operators::key_has_regex(&key) {
		if crate::exec::operators::keys_equal(&key, &held.keys) {
			held.regex.push(row);
		}
		return Ok(Ended::Next);
	}
	if key.cmp(&held.keys) != std::cmp::Ordering::Equal {
		return Ok(Ended::Next);
	}
	let probe = held.probe.clone();
	pair_held(program, join, &probe, row, state, ctx).await
}

/// Pair the held left row `probe` of join `join` with right row `build`,
/// where the residual holds of the pairing, and run it through the join's
/// pipeline.
async fn pair_held(
	program: &Program,
	join: u16,
	probe: &Value,
	build: Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Ended> {
	let spec = &program.joins[join as usize];
	let held = state.held[join as usize].as_mut().expect("a join pairs only the row it holds");
	let paired = match &spec.residual {
		Some(residual) => {
			let row = if let Value::Object(_) = probe {
				crate::exec::operators::with_alias(probe, &spec.alias, build)
			} else {
				let mut row = crate::val::Object::default();
				row.insert(spec.alias.clone(), build);
				Value::Object(row)
			};
			let mut eval = match held.eval.take() {
				Some(eval) => eval,
				None => statement_state(program, ctx).map_err(ControlFlow::Err)?,
			};
			let answer =
				out_of_line(|| host_scalar_in(program, residual, ctx, &row, &mut eval)).await;
			let held =
				state.held[join as usize].as_mut().expect("a join pairs only the row it holds");
			held.eval = Some(eval);
			if !answer?.is_truthy() {
				return Ok(Ended::Next);
			}
			if let Value::Object(_) = probe {
				row
			} else {
				probe.clone()
			}
		}
		None => crate::exec::operators::with_alias(probe, &spec.alias, build),
	};
	if let Some(held) = state.held[join as usize].as_mut() {
		held.matched = true;
	}
	let pipeline = &program.pipelines[spec.pipeline];
	let step = out_of_line(|| run_value_row(program, pipeline, paired, state, ctx)).await?;
	if step == Ended::Stop
		&& let Some(held) = state.held[join as usize].as_mut()
	{
		held.stopped = true;
	}
	Ok(step)
}

/// End join `join`'s held row once its right input has streamed past it:
/// pair the right rows a regex key matched, then — for a
/// [`JoinMatch::Left`](super::payload::JoinMatch::Left) join whose row nothing
/// paired with — emit the row once with `NULL` under the alias. A held row
/// whose key holds a regex is paired with the right rows kept, as
/// [`run_join`] pairs a probe row with its build table.
async fn finish_held_join(
	program: &Program,
	join: u16,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	let spec = &program.joins[join as usize];
	let Some(mut held) = state.held[join as usize].take() else {
		return Ok(());
	};
	if held.stopped {
		return Ok(());
	}
	if let Some(table) = held.table.take() {
		pair_built(program, spec, &table, held.probe, held.keys, state, ctx).await?;
		return Ok(());
	}
	let regex = std::mem::take(&mut held.regex);
	let probe = held.probe.clone();
	state.held[join as usize] = Some(held);
	for build in regex {
		if pair_held(program, join, &probe, build, state, ctx).await? == Ended::Stop {
			break;
		}
	}
	let Some(held) = state.held[join as usize].take() else {
		return Ok(());
	};
	if !held.matched && !held.stopped && spec.kind == super::payload::JoinMatch::Left {
		let paired = crate::exec::operators::with_alias_owned(held.probe, &spec.alias, Value::Null);
		let pipeline = &program.pipelines[spec.pipeline];
		out_of_line(|| run_value_row(program, pipeline, paired, state, ctx)).await?;
	}
	Ok(())
}

/// The key of build row `row` of join `spec` — its build keys evaluated over
/// the row set under the alias, on `eval`, made when first needed — and the
/// row.
async fn build_key(
	program: &Program,
	spec: &super::program::JoinSpec,
	row: Value,
	eval: &mut Option<interp::PooledState>,
	ctx: &ExecutionContext,
) -> FlowResult<(Vec<Value>, Value)> {
	if spec.build_keys.is_empty() {
		return Ok((Vec::new(), row));
	}
	let eval = match eval {
		Some(eval) => eval,
		slot => slot.insert(statement_state(program, ctx).map_err(ControlFlow::Err)?),
	};
	let mut seen = crate::val::Object::default();
	seen.insert(spec.alias.clone(), row);
	let seen = Value::Object(seen);
	let mut key = Vec::with_capacity(spec.build_keys.len());
	for scalar in &spec.build_keys {
		key.push(out_of_line(|| host_scalar_in(program, scalar, ctx, &seen, eval)).await?);
	}
	let row = if let Value::Object(mut seen) = seen {
		seen.remove(spec.alias.as_str()).unwrap_or(Value::None)
	} else {
		seen
	};
	Ok((key, row))
}

/// The build side of a join, keyed as it is read a batch at a time.
struct PlanBuildSink<'a> {
	inner: super::join_spill::BuildSink,
	program: &'a Program,
	spec: &'a super::program::JoinSpec,
	eval: Option<interp::PooledState>,
}

impl RowSink for PlanBuildSink<'_> {
	fn take<'a>(
		&'a mut self,
		rows: Vec<Value>,
		ctx: &'a ExecutionContext,
	) -> crate::exec::BoxFut<'a, FlowResult<()>> {
		Box::pin(async move {
			for row in rows {
				super::fuel::tick(ctx, 1).await?;
				let (key, row) =
					build_key(self.program, self.spec, row, &mut self.eval, ctx).await?;
				self.inner.push(key, row, ctx).await?;
			}
			Ok(())
		})
	}
}

/// What a join's probe row yields given its matches, as [`run_join`] pairs
/// it: each match the residual admits, under the alias, and under a left
/// join a probe row no match admits with `NULL` under it.
struct PlanPairer<'a> {
	program: &'a Program,
	spec: &'a super::program::JoinSpec,
	ctx: &'a ExecutionContext,
	eval: Option<interp::PooledState>,
}

impl super::join_spill::Pairer for PlanPairer<'_> {
	fn ctx(&self) -> &ExecutionContext {
		self.ctx
	}

	fn pair<'a>(
		&'a mut self,
		matches: &'a [&'a Value],
		probe: Value,
	) -> crate::exec::BoxFut<'a, FlowResult<Vec<Value>>> {
		Box::pin(async move {
			let (program, spec, ctx) = (self.program, self.spec, self.ctx);
			let mut out = Vec::new();
			for build in matches {
				super::fuel::tick(ctx, 1).await?;
				let paired = match &spec.residual {
					Some(residual) => {
						let row = if let Value::Object(_) = &probe {
							crate::exec::operators::with_alias(
								&probe,
								&spec.alias,
								(*build).clone(),
							)
						} else {
							let mut row = crate::val::Object::default();
							row.insert(spec.alias.clone(), (*build).clone());
							Value::Object(row)
						};
						let eval = match &mut self.eval {
							Some(eval) => eval,
							slot => slot
								.insert(statement_state(program, ctx).map_err(ControlFlow::Err)?),
						};
						let answer =
							out_of_line(|| host_scalar_in(program, residual, ctx, &row, eval))
								.await?;
						if !answer.is_truthy() {
							continue;
						}
						if let Value::Object(_) = &probe {
							row
						} else {
							probe.clone()
						}
					}
					None => {
						crate::exec::operators::with_alias(&probe, &spec.alias, (*build).clone())
					}
				};
				out.push(paired);
			}
			if out.is_empty() && spec.kind == super::payload::JoinMatch::Left {
				out.push(crate::exec::operators::with_alias_owned(probe, &spec.alias, Value::Null));
			}
			Ok(out)
		})
	}
}

/// Hand on, through join `join`'s pipeline, the pairings of the probe rows
/// it deferred once its build side spilled, in the order the in-memory join
/// pairs them. Nothing where the join kept its build side in memory.
#[allow(clippy::too_many_arguments)]
/// Runs the rows window `window` still holds through `pipeline`, the pipeline that reads it, once
/// the pipeline feeding the window has finished: the run answers its last partition, and its rows
/// are handed on a chunk at a time, flushed after each, until the run is drained or the pipeline
/// takes no more. The caller clears the run however this ends.
async fn drain_window(
	program: &Program,
	pipeline: &Pipeline,
	window: &mut super::window_step::WindowState,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<()> {
	if !window.finish(ctx).await? {
		return Ok(());
	}
	loop {
		let chunk = window.next_chunk(ctx).await?;
		if chunk.is_empty() {
			break;
		}
		for row in chunk {
			if out_of_line(|| run_value_row(program, pipeline, row, state, ctx)).await?
				== Ended::Stop
			{
				return out_of_line(|| flush(state, yielder, flush_at, ctx)).await;
			}
		}
		out_of_line(|| flush(state, yielder, flush_at, ctx)).await?;
	}
	Ok(())
}

async fn finish_spilled_join(
	program: &Program,
	pipeline: &Pipeline,
	join: u16,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<()> {
	let Some(super::join_spill::JoinBuilt::Spilled(spilled)) = state.joins[join as usize]
		.take_if(|built| matches!(built, super::join_spill::JoinBuilt::Spilled(_)))
	else {
		return Ok(());
	};
	let mut pairer = PlanPairer {
		program,
		spec: &program.joins[join as usize],
		ctx,
		eval: None,
	};
	let mut merge = spilled.finish(&mut pairer).await?;
	let most = ctx.root().ctx.config.exec.scan_batch_size.max(1);
	loop {
		let chunk = merge.next_chunk(most, ctx).await?;
		if chunk.is_empty() {
			break;
		}
		for (_, row) in chunk {
			if out_of_line(|| run_value_row(program, pipeline, row, state, ctx)).await?
				== Ended::Stop
			{
				return flush(state, yielder, flush_at, ctx).await;
			}
		}
		out_of_line(|| flush(state, yielder, flush_at, ctx)).await?;
	}
	Ok(())
}

/// Whether host step `step` of `pipeline` starts a statement nested in the
/// body: a subquery, a block statement, the statement an `EXPLAIN ANALYZE`
/// runs to measure ([`super::program::BlockInstr::RunAnalyzed`]), a block
/// evaluated in place as it is entered ([`super::program::BlockInstr::Enter`]),
/// a nested statement of an expression,
/// a late-bound statement, a `fn::` call (by its definition, or late-bound),
/// an embedded script or a module export, which run statements of their own,
/// or a builtin that runs a statement given as text or a stored endpoint
/// (`expr::method::is_writer_builtin`). A model call runs none.
///
/// The host counts each one on its context before it takes the step
/// ([`ExecutionContext::note_nested_run`]), whether it then succeeds or fails.
/// An `Instr::TryNone` region and an aggregate's finish read an error about
/// the shape of the data as `NONE` only where the count did not move while the
/// value was evaluated, so a value whose evaluation ran a nested statement
/// fails the statement around it, as it does ungrouped, and what that
/// statement wrote is undone with it.
pub(super) fn runs_nested_statement(program: &Program, pipeline: &Pipeline, step: &Step) -> bool {
	use super::program::BlockInstr;
	match step {
		Step::Subquery {
			..
		} => true,
		// A `Dive` charges the re-entry of a closure a kernel invokes, which is no
		// `fn::` call.
		Step::Function {
			at,
		} => !matches!(pipeline.code.get(*at), Some(Instr::Dive { .. })),
		Step::Block {
			at,
		} => matches!(
			pipeline.code.get(*at),
			Some(Instr::Block(
				BlockInstr::RunEmbedded { .. }
					| BlockInstr::RunServed { .. }
					| BlockInstr::RunAnalyzed { .. }
					| BlockInstr::Nested { .. }
					| BlockInstr::Member { .. }
					| BlockInstr::Enter
			))
		),
		Step::External {
			at,
		} => match pipeline.code.get(*at) {
			Some(
				Instr::ExternalEnter {
					external,
					..
				}
				| Instr::ExternalCall {
					external,
					..
				},
			) => program.externals.get(*external as usize).is_some_and(|spec| {
				matches!(
					spec.callee,
					super::program::ExternalCallee::Script { .. }
						| super::program::ExternalCallee::Module { .. }
						| super::program::ExternalCallee::Function { .. }
				)
			}),
			_ => false,
		},
		Step::Call {
			at,
		} => match pipeline.code.get(*at) {
			Some(Instr::Call {
				call,
				..
			}) => program
				.calls
				.get(*call as usize)
				.is_some_and(|spec| crate::expr::method::is_writer_builtin(&spec.name)),
			_ => false,
		},
		Step::Next
		| Step::Stop
		| Step::FetchRow {
			..
		}
		| Step::Fetch {
			..
		}
		| Step::LandRow {
			..
		}
		| Step::BindRow {
			..
		}
		| Step::WriteProjection {
			..
		}
		| Step::Omit {
			..
		}
		| Step::Access {
			..
		}
		| Step::Ir {
			..
		}
		| Step::Each {
			..
		}
		| Step::CallAllowed {
			..
		}
		| Step::FieldTypes {
			..
		}
		| Step::Method {
			..
		}
		| Step::Lookup {
			..
		}
		| Step::Signal {
			..
		}
		| Step::Recurse {
			..
		}
		| Step::Destructure {
			..
		}
		| Step::JudgeCopy {
			..
		}
		| Step::Closure {
			..
		}
		| Step::NeedUnit {
			..
		}
		| Step::Param {
			..
		}
		| Step::Store {
			..
		}
		| Step::Write {
			..
		}
		| Step::IndexFn {
			..
		}
		| Step::Matches {
			..
		}
		| Step::Degree {
			..
		}
		| Step::KnnQuery {
			..
		}
		| Step::Measure {
			..
		}
		| Step::Yield {
			..
		} => false,
	}
}

/// The rows aggregate `aggregate`'s groups finish into, as the aggregate
/// operator finishes them: `GROUP ALL`'s one group whether or not a row
/// reached it, `GROUP BY`'s groups in group-key order.
pub(super) async fn finish_groups(
	program: &Program,
	aggregate: u16,
	groups: Option<Groups>,
	ctx: &ExecutionContext,
) -> FlowResult<Vec<Value>> {
	let sink = &program.aggregates[aggregate as usize];
	let mut finish = Finish {
		program,
		sink,
		ctx,
		state: None,
		ran_nested: false,
	};
	let Some(groups) = groups else {
		return Ok(Vec::new());
	};
	let all = sink.keys == 0;
	let mut finished = groups.finish();
	let mut rows = Vec::with_capacity(finished.len());
	for at in 0..finished.len() {
		let (key, mut group) = finished.group(at);
		if all {
			finish.constant_columns(&mut group).await?;
		} else {
			// A group finished is a unit, whatever its columns evaluate.
			super::fuel::tick(ctx, 1).await?;
		}
		finish.bind_extra_args(&mut group, key).await?;
		rows.push(finish.result(key, group).await?);
	}
	Ok(rows)
}

/// The host side of an aggregate's finish: the pipelines that evaluate what
/// its groups answer once, over a state made when the first is needed and
/// kept for the rest.
struct Finish<'a> {
	program: &'a Program,
	sink: &'a super::program::AggregateSink,
	ctx: &'a ExecutionContext,
	state: Option<interp::PooledState>,
	/// Whether the last [`Self::run`] started a nested statement
	/// ([`ExecutionContext::nested_runs`]).
	ran_nested: bool,
}

impl Finish<'_> {
	/// The value pipeline `pipeline` of [`Program::host_code`] answers over
	/// `row`, or over no row.
	async fn run(&mut self, pipeline: u16, row: Option<Value>) -> FlowResult<Value> {
		let state = match &mut self.state {
			Some(state) => state,
			slot => slot.insert(statement_state(self.program, self.ctx).map_err(ControlFlow::Err)?),
		};
		state.rowless = row.is_none();
		let code = &self.program.host_code[pipeline as usize];
		let (program, ctx) = (self.program, self.ctx);
		let nested = ctx.nested_runs();
		let ran =
			out_of_line(|| run_value_row(program, code, row.unwrap_or(Value::None), state, ctx))
				.await;
		self.ran_nested = ctx.nested_runs() != nested;
		ran?;
		state.take_output().pop().ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!("a host pipeline that answered nothing"))
		})
	}

	/// Whether `error`, which the last [`Self::run`] raised, is one an
	/// aggregate reads as `NONE`: an error about the shape of the data, from a
	/// run that started no nested statement ([`runs_nested_statement`]).
	fn reads_as_none(&self, error: &ControlFlow) -> bool {
		error.is_ignorable() && !self.ran_nested
	}

	/// The document of a group's keys, each under its placeholder name.
	fn key_document(key: &[Value]) -> crate::val::Object {
		let mut document = crate::val::Object::default();
		for (idx, value) in key.iter().enumerate() {
			document.insert(crate::exec::operators::group_key_field_key(idx), value.clone());
		}
		document
	}

	/// Evaluate, against no row, each column of a `GROUP ALL` the row cannot
	/// influence and no row has evaluated, once for the group. An error about
	/// the shape of the data leaves the column without a value.
	async fn constant_columns(&mut self, group: &mut GroupMut<'_>) -> FlowResult<()> {
		for (field, spec) in self.sink.fields.iter().enumerate() {
			let Some(pipeline) = spec.finish else {
				continue;
			};
			if spec.is_group_key || spec.aggregate.is_some() || group.first(field).is_some() {
				continue;
			}
			match self.run(pipeline, None).await {
				Ok(value) => *group.first(field) = Some(value),
				Err(error) if self.reads_as_none(&error) => {
					tracing::debug!(error = %error, "Fallback expression evaluation failed (ignorable)");
				}
				Err(error) => return Err(error),
			}
		}
		Ok(())
	}

	/// Bind each aggregate's arguments after the first, which belong to the
	/// group rather than to a row: evaluated once, over a document of the
	/// group's keys, an argument reading anything else off it answering
	/// `NONE`.
	async fn bind_extra_args(&mut self, group: &mut GroupMut<'_>, key: &[Value]) -> FlowResult<()> {
		let mut document = None;
		for (field, spec) in self.sink.fields.iter().enumerate() {
			let Some(aggregate) = &spec.aggregate else {
				continue;
			};
			for (call_at, call) in aggregate.calls.iter().enumerate() {
				if call.extra_args.is_empty() {
					continue;
				}
				let mut args = Vec::with_capacity(call.extra_args.len());
				let document =
					document.get_or_insert_with(|| Value::Object(Self::key_document(key)));
				for &pipeline in &call.extra_args {
					args.push(match self.run(pipeline, Some(document.clone())).await {
						Ok(value) => value,
						Err(error) if self.reads_as_none(&error) => Value::None,
						Err(error) => return Err(error),
					});
				}
				if let Some(accumulator) = group.accumulators(field).get_mut(call_at) {
					accumulator.set_extra_args(&args);
				}
			}
		}
		Ok(())
	}

	/// The answer of one finished group: an object of each field's answer
	/// placed by its output path, or the one answer itself for `SELECT
	/// VALUE`.
	async fn result(&mut self, key: &[Value], mut group: GroupMut<'_>) -> FlowResult<Value> {
		let fields = &self.sink.fields[..];
		if fields.len() == 1
			&& fields[0].output_path.len() == 1
			&& fields[0].output_path[0].is_empty()
		{
			let first = group.first(0).take().unwrap_or(Value::None);
			return self.field(&fields[0], key, group.accumulators(0), first).await;
		}
		let mut result = crate::val::Object::default();
		for (field, spec) in fields.iter().enumerate() {
			let first = group.first(field).take().unwrap_or(Value::None);
			let value = self.field(spec, key, group.accumulators(field), first).await?;
			crate::exec::operators::set_nested_value(&mut result, &spec.output_path, value);
		}
		Ok(Value::Object(result))
	}

	/// The answer of one field of a finished group: its key, the answer of its
	/// aggregates, or the first value its group took.
	async fn field(
		&mut self,
		spec: &super::program::AggSpec,
		key: &[Value],
		accumulators: &[AccumulatorState],
		first: Value,
	) -> FlowResult<Value> {
		if let Some(index) = spec.group_key_index {
			return Ok(key.get(index).cloned().unwrap_or(Value::None));
		}
		let Some(aggregate) = &spec.aggregate else {
			return Ok(first);
		};
		// A field with no call answers by its expression over the group's keys alone, or `NULL`
		// where it has none.
		if aggregate.calls.is_empty() && aggregate.post.is_none() {
			return Ok(Value::Null);
		}
		// A failure to finalize is raised: an accumulator fails when handed a
		// value of a kind it cannot fold, which is the argument-type error the
		// scalar form of the function raises.
		let Some(post) = aggregate.post else {
			// With no expression over them, the field answers its first
			// aggregate's value.
			let mut answer = Value::Null;
			for (idx, accumulator) in accumulators.iter().enumerate() {
				let value = accumulator.finalize().map_err(ControlFlow::Err)?;
				if idx == 0 {
					answer = value;
				}
			}
			return Ok(answer);
		};
		let mut document = crate::val::Object::default();
		for (idx, accumulator) in accumulators.iter().enumerate() {
			let value = accumulator.finalize().map_err(ControlFlow::Err)?;
			document.insert(crate::exec::operators::aggregate_field_key(idx), value);
		}
		// The group's keys sit beside its answers, so a key the expression
		// names finds its placeholder.
		for (idx, value) in key.iter().enumerate() {
			document.insert(crate::exec::operators::group_key_field_key(idx), value.clone());
		}
		match self.run(post, Some(Value::Object(document))).await {
			Ok(value) => Ok(value),
			Err(error) if self.reads_as_none(&error) => {
				tracing::debug!(error = %error, "Post-expression evaluation failed (ignorable), using Null");
				Ok(Value::Null)
			}
			Err(error) => Err(error),
		}
	}
}

pub(super) async fn flush(
	state: &mut State,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	spill_breakers(state, ctx).await?;
	if let Some(write) = &state.write {
		write.judge_only()?;
		held_writes(ctx).await?;
	}
	if state.output_len() >= flush_at {
		match yielder {
			Out::Stream(yielder) => match state.framed_output().map_err(ControlFlow::Err)? {
				Some(rows) => yielder.emit(ValueBatch::encoded(rows)).await,
				None => yielder.emit(ValueBatch::new(state.take_output())).await,
			},
			Out::Gather(rows) => {
				gathered(state, ctx).await?;
				rows.append(&mut state.output)
			}
			Out::Sink(sink) => sink.take(state.take_output(), ctx).await?,
		}
	}
	Ok(())
}

/// Charge what the statement's transaction holds for its writes to the
/// query's lease: its creation log ([`crate::kvs::creations`]), which holds
/// the records created in the tables its writes are reading, on every
/// backend; and what it has written, unless the backend bounds its own write
/// buffer ([`Transaction::bounds_write_buffer`](crate::kvs::Transaction)).
/// Neither can be spilled, so the bytes are pinned: the governor sees the
/// memory, the statement waits when other queries can give memory back, and
/// the bytes are otherwise granted past the node's limit. The statement fails
/// only where its pinned bytes would pass the query's ceiling. A creation log
/// that holds what the query holds for it costs one load.
async fn held_writes(ctx: &ExecutionContext) -> FlowResult<()> {
	let (Some(memory), Some(txn)) = (ctx.ctx().memory(), ctx.try_txn()) else {
		return Ok(());
	};
	let address = Arc::as_ptr(&txn) as usize;
	memory.hold_creations(address, txn.creation_log_bytes(), ctx).await?;
	if txn.bounds_write_buffer() {
		return Ok(());
	}
	memory.hold_writes(address, txn.bytes_written(), ctx).await
}

/// Charge the rows a gather is about to take to the query's lease. A
/// gathered result becomes one value and cannot be spilled, so the charge
/// goes through [`Reservation::insist`](crate::exec::memory::Reservation):
/// the statement waits, for at most the configured wait, for other queries
/// to give memory back, and is then granted the bytes past the node's limit.
/// It fails only where the query's pinned bytes would pass its ceiling. The
/// account is released when the statement hands its value on, except that a
/// `LET`'s binding takes it over ([`State::handed`]); a value kept elsewhere
/// is charged where it is kept ([`super::ledger`]).
async fn gathered(state: &mut State, ctx: &ExecutionContext) -> FlowResult<()> {
	let Some(memory) = ctx.ctx().memory() else {
		return Ok(());
	};
	let bytes = crate::exec::memory::size::values_bytes(&state.output);
	let account = state.gather_account.get_or_insert_with(|| memory.pinned_reservation());
	if account.charge(bytes) == crate::exec::memory::Verdict::Spill {
		account.insist(ctx).await?;
	}
	Ok(())
}

/// Bound the breakers of `program` by the query's memory: every sort but a
/// brute-force KNN's charges what it holds to the query's lease, which opens
/// it with its spill headroom, and spills once the lease no longer covers it.
/// A sort its plan labels [`Spill::Bounded`](super::payload::Spill) keeps a
/// bounded number of rows: it takes nothing from its lease until it holds
/// more than a chunk, and takes no account at all when the query has no
/// temporary store: it prunes in memory and never fails for memory. A sort
/// labelled [`Spill::Spillable`](super::payload::Spill) under
/// `TEMPFILES` also spills each time it holds the external sorting buffer
/// limit's rows, when the context
/// names a temporary directory. An aggregate so labelled charges its groups.
fn bound_breakers(program: &Program, state: &mut State, ctx: &ExecutionContext) {
	let Some(memory) = ctx.ctx().memory() else {
		return;
	};
	bind_breakers(program, state, &Arc::clone(memory), tempfiles_limit(ctx));
}

/// Bound a morsel's breakers by its query's lease, where the query can
/// spill: as the statement's are, but a sort never spills by its row count.
pub(super) fn bound_batch(program: &Program, state: &mut State, ctx: &ExecutionContext) {
	if let Some(memory) = ctx.ctx().memory().filter(|memory| memory.can_spill()) {
		bind_breakers(program, state, &Arc::clone(memory), None);
	}
}

/// Reads the links a run of the leading rows of `rows` hold, before the rows run, for the `FETCH`
/// of `pipeline`'s body, where the physical plan states them ([`Pipeline::rows_ahead`]): `rows`
/// are rows of one batch that the body runs the `FETCH` over, in order. Answers how many leading
/// rows the fill covers
/// ([`RowsWindow::fill`](crate::exec::operators::link_window::RowsWindow::fill)). The records are
/// held in `state` until the next fill replaces them. A pipeline the plan states nothing for reads
/// no link ahead, and its rows are covered.
pub(super) async fn fill_rows_ahead(
	pipeline: &Pipeline,
	rows: &[Value],
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<usize> {
	let Some(spec) = &pipeline.rows_ahead else {
		return Ok(rows.len());
	};
	let ahead = state.link_ahead.get_or_insert_with(Default::default);
	ahead.fill(ctx, rows, &spec.fields, spec.fetch.is_some()).await
}

/// Reads what the first hop of the lookups of `pipeline`'s projection holds for the rows of one
/// batch, before the rows run, where the physical plan states it
/// ([`Pipeline::frontier_rows`]): `rows` are the rows of the batch that reach the projection, in
/// order. The window is held in `state` until the next batch replaces it. A pipeline the plan
/// states nothing for reads nothing ahead.
pub(super) async fn fill_frontier_rows(
	program: &Program,
	pipeline: &Pipeline,
	rows: &[Value],
	state: &mut State,
	ctx: &ExecutionContext,
) {
	let Some(spec) = &pipeline.frontier_rows else {
		return;
	};
	super::graph::fill_rows(program, &spec.chains, rows, state, ctx).await;
}

/// [`bound_breakers`] over `memory`, a sort that keeps every row spilling
/// each time it holds `tempfiles` rows where there is a limit.
fn bind_breakers(
	program: &Program,
	state: &mut State,
	memory: &Arc<crate::exec::memory::QueryMemory>,
	tempfiles: Option<usize>,
) {
	state.memory = Some(Arc::clone(memory));
	let rows = tempfiles;
	for (at, spec) in program.sorts.iter().enumerate() {
		// A brute-force KNN's sort publishes the distance of each row it
		// keeps, which the spilled merge does not: it never spills.
		if spec.knn_slot.is_some() {
			continue;
		}
		// A keep sort holds a bounded buffer, so it has no need to be
		// accounted where it could not spill.
		if spec.held == super::payload::Spill::Bounded && !memory.can_spill() {
			continue;
		}
		state.sort_accounts[at] = memory.reservation();
		state.sort_spill[at] = Some(super::sort::SortSpill {
			rows: rows.filter(|_| spec.spill && !spec.random && spec.keep.is_none()),
			keys: match spec.random {
				true => vec![super::sort::RANDOM_ORDER],
				false => spec.keys.clone(),
			},
			random: spec.random,
			keep: spec.keep,
			taken_bytes: 0,
			taken_rows: 0,
			floor: None,
		});
	}
	for (at, sink) in program.aggregates.iter().enumerate() {
		if sink.held == super::payload::Spill::Spillable {
			let savable = state
				.aggregates
				.functions
				.get(at)
				.is_none_or(|calls| super::groups::savable(calls));
			state.group_accounts[at] = match savable {
				true => memory.reservation(),
				false => memory.pinned_reservation(),
			};
		}
	}
}

/// The rows a `TEMPFILES` sort holds before it spills, when the context
/// names a temporary directory.
#[cfg(storage)]
fn tempfiles_limit(ctx: &ExecutionContext) -> Option<usize> {
	ctx.ctx()
		.temporary_directory()
		.map(|_| ctx.root().ctx.config.exec.external_sorting_buffer_limit.max(1))
}

#[cfg(not(storage))]
fn tempfiles_limit(_ctx: &ExecutionContext) -> Option<usize> {
	None
}

/// Write out as a run each sort its lease no longer covers, or that holds
/// its `TEMPFILES` limit. A breaker over its lease with no store to spill to
/// holds what it holds through [`Reservation::insist`](crate::exec::memory::Reservation):
/// the statement waits for other queries to give memory back, and is then
/// granted the bytes past the node's limit. It fails only where the query's
/// pinned bytes would pass its ceiling.
pub(super) async fn spill_breakers(state: &mut State, ctx: &ExecutionContext) -> FlowResult<()> {
	let Some(memory) = state.memory.clone() else {
		return Ok(());
	};
	state.spill_due = false;
	for at in 0..state.sorts.len() {
		let Some(spill) = &state.sort_spill[at] else {
			continue;
		};
		let by_rows = spill.rows.is_some_and(|limit| state.sorts[at].len() >= limit);
		let account = &mut state.sort_accounts[at];
		if !by_rows && !account.wants_spill() {
			continue;
		}
		if state.sorts[at].is_empty() {
			account.clear();
			continue;
		}
		if !memory.can_spill() {
			account.insist(ctx).await?;
			continue;
		}
		spill_sort(state, at, &memory, ctx).await?;
	}
	for at in 0..state.groups.len() {
		let account = &mut state.group_accounts[at];
		let Some(groups) = state.groups[at].as_mut() else {
			continue;
		};
		if account.memory().is_none() || !groups.spillable() {
			continue;
		}
		account.hold(groups.bytes());
		if !account.wants_spill() {
			continue;
		}
		if !memory.can_spill() {
			account.insist(ctx).await?;
			continue;
		}
		// Groups whose states cannot be saved stay in memory, held as bytes
		// the query cannot spill.
		if !spill_groups(state, at, &memory, ctx).await? {
			state.group_accounts[at].insist(ctx).await?;
		}
	}
	Ok(())
}

/// Write sort `at`'s buffer, which is not empty, as its next run, cut to its
/// `keep`, and reset what it keeps in memory: its account, its arrivals and,
/// for a keep sort, its floor.
pub(super) async fn spill_sort(
	state: &mut State,
	at: usize,
	memory: &Arc<crate::exec::memory::QueryMemory>,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	let Some(spill) = &state.sort_spill[at] else {
		return Err(ControlFlow::Err(anyhow::anyhow!("Internal error: a sort that cannot spill")));
	};
	let (keys, random, limit) = (spill.keys.clone(), spill.random, spill.keep);
	let mut rows = std::mem::take(&mut state.sorts[at]);
	let slab = std::mem::take(&mut state.sort_slabs[at]);
	if random {
		super::sort::with_random_keys(&mut rows);
	}
	let spec = RunSpec::new(
		BreakerKind::Sort,
		at as u16,
		keys.len(),
		RunOrder::Keys(super::spill::key_order_bytes(&keys)),
	);
	let runs = state.sort_runs[at].get_or_insert_with(Default::default);
	let last = runs.spill_sorted(memory, spec, rows, &slab, &keys, limit, ctx).await?;
	state.sort_accounts[at].clear();
	if limit.is_some() && !random {
		// The buffer is empty: the sort keeps nothing in memory, and the
		// next row starts a fresh prune or merge. Arrivals keep counting,
		// so stamps stay increasing across runs. Sorting the buffer
		// stably at any point between rows gives the order a full sort of
		// the rows that reached it would give: a merging sort stamps
		// every row with its arrival, so its order is total and holds
		// mid-merge, where rows only swap within the buffer; a pruning
		// sort holds its kept rows sorted, stably, ahead of later
		// arrivals, so among equal keys buffer position is arrival order.
		let arrivals = &mut state.sort_arrivals[at];
		*arrivals = Arrivals {
			run: arrivals.run,
			arrived: arrivals.arrived,
			..Default::default()
		};
		if let Some(last) = last {
			let spill = state.sort_spill[at].as_mut().expect("checked above");
			let lower = match &spill.floor {
				Some(floor) => super::interp::compare_keys(&last, floor, &keys) == Ordering::Less,
				None => true,
			};
			if lower {
				spill.floor = Some(last);
				#[cfg(test)]
				super::spill::FLOORS_SET.with(|floors| floors.set(floors.get() + 1));
			}
		}
	}
	Ok(())
}

/// Write aggregate `at`'s open groups as its next run; `false`, writing
/// nothing, where their states cannot be saved.
pub(super) async fn spill_groups(
	state: &mut State,
	at: usize,
	memory: &Arc<crate::exec::memory::QueryMemory>,
	ctx: &ExecutionContext,
) -> FlowResult<bool> {
	let Some(groups) = state.groups[at].as_mut() else {
		return Ok(false);
	};
	let Some(records) = groups.save_all() else {
		return Ok(false);
	};
	let spec = RunSpec::new(BreakerKind::Groups, at as u16, groups.keys(), RunOrder::Plain);
	let run = super::spill::write_run(memory, spec, records, ctx).await?;
	state.group_runs[at].get_or_insert_with(Default::default).push(run)?;
	state.group_accounts[at].clear();
	Ok(true)
}

/// Write what the statement's sort `at` holds as its next run, so that a
/// batch's runs appended after it follow every row it holds. Nothing when it
/// holds nothing.
pub(super) async fn seal_sort(
	state: &mut State,
	at: usize,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	if state.sorts[at].is_empty() {
		return Ok(());
	}
	let memory = sealing_memory(state)?;
	spill_sort(state, at, &memory, ctx).await
}

/// [`seal_sort`] for aggregate `at`'s open groups.
pub(super) async fn seal_groups(
	state: &mut State,
	at: usize,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	if state.groups[at].as_ref().is_none_or(Groups::is_empty) {
		return Ok(());
	}
	let memory = sealing_memory(state)?;
	if !spill_groups(state, at, &memory, ctx).await? {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"Internal error: a morsel spilled groups its statement cannot save"
		)));
	}
	Ok(())
}

/// The memory a statement seals a breaker through: a batch spilled where its
/// statement can, so the statement has a store.
fn sealing_memory(state: &State) -> FlowResult<Arc<crate::exec::memory::QueryMemory>> {
	state.memory.clone().filter(|memory| memory.can_spill()).ok_or_else(|| {
		ControlFlow::Err(anyhow::anyhow!(
			"Internal error: a morsel spilled where its statement cannot"
		))
	})
}

/// The rows aggregate `aggregate` answers once some of its groups have been
/// spilled: the runs and the groups still held, merged in key order, the
/// saved states of each key combined in the order they were spilled and
/// finished a batch of keys at a time.
#[allow(clippy::too_many_arguments)]
async fn finish_spilled_groups(
	program: &Program,
	pipeline: &Pipeline,
	aggregate: u16,
	runs: super::spill::Runs,
	memory: Arc<crate::exec::memory::QueryMemory>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<()> {
	let at = aggregate as usize;
	let held = match state.groups[at].take() {
		Some(mut groups) => groups.save_all().ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!("an aggregate spilled groups it cannot save"))
		})?,
		None => Vec::new(),
	};
	state.group_accounts[at].clear();
	let order: super::spill::RecordOrder = Arc::new(|(a, _), (b, _)| a.cmp(b));
	let merge = runs.merge(&memory, held, order, None, ctx).await?;
	let functions = state.aggregates.functions[at].clone();
	let keys = program.aggregates[at].keys;
	let batch = ctx.root().ctx.config.exec.scan_batch_size.max(1);
	// A batch of keys is finished once it holds half of what the query may
	// hold, so the groups finished at once stay within its lease.
	let room = memory.working() / 2;
	let mut fold = super::groups::SavedFold::new(merge, keys, functions, batch, room);
	while let Some(chunk) = fold.next_chunk(ctx).await? {
		let rows = out_of_line(|| finish_groups(program, aggregate, Some(chunk), ctx)).await?;
		let rows = rows.into_iter().map(|row| (Vec::new(), row)).collect();
		let stopped =
			out_of_line(|| run_rows(program, pipeline, rows, state, ctx, yielder, flush_at))
				.await?;
		if stopped {
			break;
		}
	}
	Ok(())
}

/// A paging clause's count, evaluated once for the statement.
async fn paging_clause(
	program: &Program,
	scalar: &super::program::HostScalar,
	ctx: &ExecutionContext,
	clause: PagingClause,
) -> FlowResult<usize> {
	let value = host_scalar(program, scalar, ctx, None).await?;
	Ok(paging_count(&value, clause).map_err(ControlFlow::Err)? as usize)
}

/// The value of host scalar `scalar`, evaluated over `row` — the value the
/// statement binds as `$this`, for a write's subjects — or no row.
pub(super) async fn host_scalar(
	program: &Program,
	scalar: &super::program::HostScalar,
	ctx: &ExecutionContext,
	row: Option<&Value>,
) -> FlowResult<Value> {
	use super::program::HostScalar;
	match scalar {
		HostScalar::Const(value) => Ok(Value::clone(value)),
		HostScalar::Code(pipeline) => {
			// The pipeline's host steps read in the program's frames, a
			// lookup's records shown through their tables' views among them.
			let mut state = statement_state(program, ctx)?;
			state.rowless = row.is_none();
			let pipeline = &program.host_code[*pipeline as usize];
			let row = row.map_or(Cow::Owned(Value::None), Cow::Borrowed);
			out_of_line(|| run_cow_row(program, pipeline, row, &mut state, ctx)).await?;
			state.output.pop().ok_or_else(|| {
				ControlFlow::Err(anyhow::anyhow!("a host pipeline that answered nothing"))
			})
		}
	}
}

/// The value host pipeline `pipeline` of [`Program::host_code`] answers over
/// `row` as its document, evaluated in the state `slot` holds, made when the
/// first row is evaluated and kept for the rest.
pub(super) async fn eval_on_row(
	program: &Program,
	pipeline: u16,
	row: Cow<'_, Value>,
	slot: &mut Option<interp::PooledState>,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	let state = match slot {
		Some(state) => state,
		slot => slot.insert(statement_state(program, ctx).map_err(ControlFlow::Err)?),
	};
	state.rowless = false;
	let code = &program.host_code[pipeline as usize];
	out_of_line(|| run_cow_row(program, code, row, state, ctx)).await?;
	state
		.take_output()
		.pop()
		.ok_or_else(|| ControlFlow::Err(anyhow::anyhow!("a host pipeline that answered nothing")))
}

/// [`host_scalar`] over `row`, evaluated on `state`, which the caller made
/// for `program` ([`statement_state`]) and keeps across evaluations.
pub(super) async fn host_scalar_in(
	program: &Program,
	scalar: &super::program::HostScalar,
	ctx: &ExecutionContext,
	row: &Value,
	state: &mut State,
) -> FlowResult<Value> {
	use super::program::HostScalar;
	match scalar {
		HostScalar::Const(value) => Ok(Value::clone(value)),
		HostScalar::Code(pipeline) => {
			state.rowless = false;
			let pipeline = &program.host_code[*pipeline as usize];
			out_of_line(|| run_cow_row(program, pipeline, Cow::Borrowed(row), state, ctx)).await?;
			state.output.pop().ok_or_else(|| {
				ControlFlow::Err(anyhow::anyhow!("a host pipeline that answered nothing"))
			})
		}
	}
}

/// What the host answers for `spec`, a call bound to a statement's KNN distances, over `row` in
/// `frame`, or `None` where the function answers it. A `KNN` operator judged per row
/// ([`IndexBinding::KnnHolds`](super::program::IndexBinding::KnnHolds)) is whether the record has
/// a distance. `vector::distance::knn()` is answered here only over a row that names no record
/// while a write has one in flight: that record's distance, or `NONE` where the search found none.
///
/// The record is the one the row names: the row itself, or its `id`. A row of a write that names
/// no record — the post-image of a record a `DELETE` removes, which its `RETURN` clause projects —
/// is read as the record in flight, so the clause reports the search's result for a record the
/// write deletes as for any other.
async fn knn_read(
	program: &Program,
	state: &mut State,
	ctx: &ExecutionContext,
	frame: Option<u16>,
	row: &Value,
	spec: &super::program::CallSpec,
) -> Option<Value> {
	let (slot, holds) = match &spec.index {
		Some(super::program::IndexBinding::KnnHolds {
			slot,
		}) => (*slot, true),
		Some(super::program::IndexBinding::Knn {
			slot,
		}) => (*slot, false),
		_ => return None,
	};
	let distances = state.knn_context(slot);
	let eval_ctx = frame_eval(program, state, ctx, frame);
	let eval_ctx = row_eval(state, eval_ctx, row);
	let named = match eval_ctx.value_in_scope().within_context() {
		Some(Value::RecordId(rid)) => Some(rid),
		Some(Value::Object(object)) => match object.get("id") {
			Some(Value::RecordId(rid)) => Some(rid),
			_ => None,
		},
		_ => None,
	};
	let record = match (named, holds) {
		(Some(rid), true) => rid,
		(Some(_), false) => return None,
		(None, _) => {
			match state.write.as_deref().and_then(super::write::WriteState::record_in_flight) {
				Some(rid) => rid,
				None if holds => return Some(Value::Bool(false)),
				None => return None,
			}
		}
	};
	let distance = distances.get(record).await;
	Some(match holds {
		true => Value::Bool(distance.is_some()),
		false => distance.map_or(Value::None, Value::Number),
	})
}

/// `eval_ctx` over `row`, the row the pipeline running over `state` runs
/// over: its current value and document, its current value alone where the
/// row is no document ([`State::undocumented`]), and neither where the
/// pipeline has no row.
fn row_eval<'c>(state: &'c State, eval_ctx: EvalContext<'c>, row: &'c Value) -> EvalContext<'c> {
	if state.rowless {
		eval_ctx
	} else if let Some(root) = &state.root {
		eval_ctx.with_value_and_doc(root).with_element(row)
	} else if state.undocumented {
		eval_ctx.with_value(row)
	} else {
		eval_ctx.with_value_and_doc(row)
	}
}

/// The evaluation context of a host step in `frame`, as [`frame_ctx`] states:
/// in a computed or index-entry frame computing the record
/// [`Instr::BeginComputing`](super::program::Instr::BeginComputing) last set,
/// in a gate frame, or while a write permission is judged
/// ([`State::unchecked_reads`]), reading without permission checks; and in
/// any frame with `$input` bound to [`State::write_input`] while that is set.
pub(super) fn frame_eval<'c>(
	program: &Program,
	state: &'c State,
	ctx: &'c ExecutionContext,
	frame: Option<u16>,
) -> EvalContext<'c> {
	let mut eval_ctx = EvalContext::from_exec_ctx(frame_ctx(state, ctx, frame));
	if state.unchecked_reads {
		eval_ctx.skip_fetch_perms = true;
	}
	if let Some(input) = &state.write_input {
		eval_ctx.write_input = Some(input);
	}
	if let Some(parent) = &state.parent {
		eval_ctx.local_params = Some(parent);
	}
	if let Some(frame) = frame
		&& super::program::is_permission_frame(Some(frame))
	{
		match program.permissions[frame as usize].mode {
			super::program::FrameMode::Computed | super::program::FrameMode::IndexEntry => {
				eval_ctx.computing_record.clone_from(&state.computing);
			}
			super::program::FrameMode::Gate(_) => eval_ctx.skip_fetch_perms = true,
			super::program::FrameMode::Derived => {
				eval_ctx.computing_record.clone_from(&state.computing);
				eval_ctx.skip_fetch_perms = true;
			}
			super::program::FrameMode::Guard(_) => {}
		}
	}
	eval_ctx
}

/// The context a host step reads in: permission or call frame `frame` when set,
/// otherwise the block's current frame or the statement's.
fn frame_ctx<'c>(
	state: &'c State,
	ctx: &'c ExecutionContext,
	frame: Option<u16>,
) -> &'c ExecutionContext {
	use super::program::CALL_FRAME;
	match frame {
		Some(frame) if frame >= CALL_FRAME => {
			match state.calls.get((frame - CALL_FRAME) as usize) {
				Some(Some(entered)) => &entered.limited,
				_ => unreachable!("a call frame read before the function is entered"),
			}
		}
		Some(frame) => &state.frames[frame as usize],
		None => state.frame.as_ref().unwrap_or(ctx),
	}
}

/// Apply builtin `call` to `args` once its arguments are evaluated: the experimental gate, then
/// the function, with the call's plan depth for `eval::*`.
async fn call_builtin(
	call: &super::program::CallSpec,
	args: Vec<Value>,
	ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let registry = ctx.exec_ctx.function_registry();
	// A projection function called for its value answers its one binding's
	// value, or every binding's in an array, as `ProjectionFunctionExec`
	// answers outside a projection.
	if let Some(projection) = registry.get_projection(&call.name) {
		let mut bindings = projection.invoke_async(&ctx, args).await.map_err(ControlFlow::Err)?;
		return Ok(if bindings.len() == 1 {
			bindings.pop().expect("one binding").1
		} else {
			Value::Array(bindings.into_iter().map(|(_, value)| value).collect::<Vec<_>>().into())
		});
	}
	let func = registry.get(&call.name).ok_or_else(|| {
		ControlFlow::Err(anyhow::anyhow!(
			"Unknown function '{}' - not found in function registry",
			call.name
		))
	})?;
	refuse_mutating_builtin(func.name(), func.effects(), ctx.exec_ctx)?;
	let mut ctx = ctx;
	ctx.plan_depth = ctx.plan_depth.max(call.plan_depth);
	invoke_builtin(func, args, &ctx).await?.map_err(ControlFlow::Err)
}

/// The effects of a builtin that reach past the transaction: a call making one runs only once the
/// creates held before it are checked.
const HELD_CREATES_FLUSHED_BY: crate::expr::logical::EffectSet =
	crate::expr::logical::EffectSet::WRITES_EXTERNAL
		.union(crate::expr::logical::EffectSet::READS_EXTERNAL)
		.union(crate::expr::logical::EffectSet::ADVANCES_STATE)
		.union(crate::expr::logical::EffectSet::BLOCKING)
		.union(crate::expr::logical::EffectSet::OPAQUE_BODY)
		.union(crate::expr::logical::EffectSet::DEFERS);

/// Invoke builtin `func` on `args`, its work drawing on the thread's fuel.
///
/// The arguments, read and converted whole before the function runs, are
/// charged first, by their size ([`super::fuel::weight`]); a call whose
/// arguments outweigh the fuel left reaches a yield point before it starts.
/// A function that walks its input ([`ScalarFunction::metered`]) then charges
/// its walk a stretch at a time and yields mid-walk once the fuel is spent;
/// one that does not is charged its result's size once it returns.
///
/// [`ScalarFunction::metered`]: crate::exec::function::ScalarFunction::metered
async fn invoke_builtin(
	func: &dyn crate::exec::function::ScalarFunction,
	args: Vec<Value>,
	ctx: &EvalContext<'_>,
) -> FlowResult<anyhow::Result<Value>> {
	let exec = ctx.exec_ctx;
	let cost = args.iter().map(super::fuel::weight).fold(0, u32::saturating_add);
	if cost > 0 {
		if cost >= super::fuel::left() {
			out_of_line(|| super::fuel::yield_point(exec)).await?;
		}
		super::fuel::charge(cost);
	}
	if !func.is_pure() || func.is_async() {
		// A call that reaches past the transaction runs only once every create held before it
		// is checked, so none runs after a create that fails.
		let effects = func.effects();
		if effects.intersects(HELD_CREATES_FLUSHED_BY) {
			exec.txn().flush_held().await.map_err(|e| ControlFlow::Err(e.into()))?;
		}
		// A call that may change something outside the transaction keeps the statement from
		// being run again after a lost conflict.
		if effects.contains(crate::expr::logical::EffectSet::WRITES_EXTERNAL) {
			exec.txn().note_external_effect();
		}
		return super::fuel::metered(exec, func.invoke_async(ctx, args)).await;
	}
	if let Some(walk) = func.metered() {
		return super::fuel::metered(exec, walk(args)).await;
	}
	let result = func.invoke(args);
	if let Ok(value) = &result {
		super::fuel::charge(super::fuel::weight(value));
	}
	Ok(result)
}

/// Refuse a call of the builtin `name`, which declares `effects`, that the
/// frame `ctx` runs in does not admit: a schema clause reaching a builtin
/// that advances state outside the transaction or blocks, or a Surrealism
/// function declared read-only reaching a mutating builtin, by a call or by a
/// method on a value alike, each refused with the frame's error.
fn refuse_mutating_builtin(
	name: &str,
	effects: crate::expr::logical::EffectSet,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	match ctx.root().no_write {
		Some(frame) if frame.refuses_builtin_call(name, effects) => {
			let construct = crate::exec::clause_effects::builtin_text(name, effects);
			Err(ControlFlow::Err(anyhow::anyhow!(frame.side_effect_error(&construct))))
		}
		_ => Ok(()),
	}
}

/// Whether `frame` evaluates a column of a projection layer
/// ([`PermissionSpec::column`](super::program::PermissionSpec::column)).
fn column_frame(program: &Program, frame: Option<u16>) -> bool {
	frame.is_some_and(|frame| {
		super::program::is_permission_frame(Some(frame))
			&& program.permissions.get(frame as usize).is_some_and(|spec| spec.column)
	})
}

/// What `signal`, raised by a statement evaluated in `frame` and taken by no
/// catch, raises there. A permission frame and a projection layer's column are
/// barriers to a loop: the loop a `BREAK` or `CONTINUE` names is not written
/// in them, so each raises [`crate::exec::Error::InvalidControlFlow`], the
/// error every barrier answers with. A `RETURN` is the value of the block that
/// holds it, which the statement evaluated in the frame takes
/// (`Lowering::own_catch`); one that reaches here was taken by
/// none, and has nowhere to go: a predicate's value never becomes the value of
/// the statement it guards, so it is an error. Outside those frames, and for
/// an error, the signal passes through unchanged.
fn escaped_signal(program: &Program, frame: Option<u16>, signal: ControlFlow) -> ControlFlow {
	match signal {
		ControlFlow::Err(_) => signal,
		signal @ (ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_))
			if column_frame(program, frame) =>
		{
			column_signal(signal)
		}
		ControlFlow::Break | ControlFlow::Continue
			if super::program::is_permission_frame(frame) =>
		{
			ControlFlow::Err(anyhow::Error::new(crate::exec::Error::InvalidControlFlow))
		}
		ControlFlow::Return(_) if super::program::is_permission_frame(frame) => {
			ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
				"unexpected control flow in a permission predicate: return".to_owned(),
			)))
		}
		signal @ (ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_)) => signal,
	}
}

/// The error `signal` raises out of a projection layer's column: a `BREAK` or
/// `CONTINUE` has no loop to target there, and a `RETURN` has nowhere to go.
fn column_signal(signal: ControlFlow) -> ControlFlow {
	match signal {
		ControlFlow::Break | ControlFlow::Continue => {
			ControlFlow::Err(anyhow::Error::new(crate::exec::Error::InvalidControlFlow))
		}
		signal @ (ControlFlow::Return(_) | ControlFlow::Err(_)) => {
			ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(format!(
				"unexpected control flow in a layer column: {signal}"
			))))
		}
	}
}

/// The bindings projection function `call` names answers for `args`, as a
/// projection function at a projection's root is invoked: by name through the
/// function registry, with no capability check of its own.
async fn project_call(
	call: &super::program::CallSpec,
	args: Vec<Value>,
	ctx: EvalContext<'_>,
) -> FlowResult<Vec<(crate::expr::idiom::Idiom, Value)>> {
	let projection =
		ctx.exec_ctx.function_registry().get_projection(&call.name).ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!(
				"Unknown projection function '{}' - not found in function registry",
				call.name
			))
		})?;
	projection.invoke_async(&ctx, args).await.map_err(ControlFlow::Err)
}

/// The unit table the ids of `program` index: its own when it is the root
/// that holds the units, and otherwise the one `state` carries for a unit
/// the host runs over a state of its own, or the bundle executing around
/// `ctx`.
pub(super) fn unit_table(
	program: &Program,
	state: &State,
	ctx: &ExecutionContext,
) -> FlowResult<Arc<[Arc<Program>]>> {
	if !program.units.is_empty() {
		return Ok(Arc::clone(&program.units));
	}
	state.units.clone().or_else(|| ctx.vm_units().cloned()).ok_or_else(|| {
		ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
			"a program calls a unit and holds no unit table".to_owned(),
		)))
	})
}

/// The digest of the class of the principal `ctx` runs under in the
/// database `ns` and `db` name
/// ([`UnitFrame::digest`](super::program::UnitFrame::digest)).
pub(super) fn class_digest_in(ctx: &ExecutionContext, ns: Option<&str>, db: Option<&str>) -> u64 {
	super::program::UnitFrame::digest(ctx.ctx().auth_enabled(), Some(&ctx.root().auth), ns, db)
}

/// The digest of the class of the principal `ctx` runs under in the
/// database `frame` names, the database its class was taken in when the unit
/// was lowered.
pub(super) fn class_digest(ctx: &ExecutionContext, frame: &super::program::UnitFrame) -> u64 {
	class_digest_in(ctx, frame.ns.as_deref(), frame.db.as_deref())
}

/// The digest of the class of the principal `ctx` runs under in the database
/// it runs in.
pub(super) fn class_digest_here(ctx: &ExecutionContext) -> u64 {
	let database = ctx.database().ok();
	class_digest_in(
		ctx,
		database.as_ref().map(|db| db.ns_name()),
		database.as_ref().map(|db| db.db_name()),
	)
}

/// Whether the frame `ctx` runs in is one `frame`, the frame a unit was
/// lowered for, admits: the principal, namespace and database it names, and
/// a permission contract no weaker than its own: a unit lowered with
/// permissions suppressed runs only where they are suppressed or where reads
/// skip them, as an event's frame does.
pub(super) fn frame_admits(frame: &super::program::UnitFrame, ctx: &ExecutionContext) -> bool {
	frame_admits_as(frame, ctx, ctx.root().skip_fetch_perms, |frame| class_digest(ctx, frame))
}

/// [`frame_admits`], for a run that skips permission checks when `skips`,
/// with the digest of the class of the principal `ctx` runs under answered by
/// `principal` for the frame, which is asked only of a frame that names one.
pub(super) fn frame_admits_as(
	frame: &super::program::UnitFrame,
	ctx: &ExecutionContext,
	skips: bool,
	principal: impl FnOnce(&super::program::UnitFrame) -> u64,
) -> bool {
	let database = ctx.database().ok();
	let names = |held: &Option<String>, running: Option<&str>| match held {
		Some(held) => running == Some(held.as_str()),
		None => true,
	};
	let root = ctx.root();
	(frame.class_digest == 0 || frame.class_digest == principal(frame))
		&& names(&frame.ns, database.as_ref().map(|db| db.ns_name()))
		&& names(&frame.db, database.as_ref().map(|db| db.db_name()))
		&& (!frame.perms_suppressed || root.perms_suppressed || skips)
		&& (frame.checked || skips)
}

/// Unit `unit` of `units`, after checking that the frame `ctx` runs in is the
/// one it was lowered for: a unit never runs under a principal or a
/// permission contract other than its own, and fails closed.
pub(super) fn admitted_unit<'u>(
	units: &'u [Arc<Program>],
	unit: super::program::UnitId,
	ctx: &ExecutionContext,
) -> FlowResult<&'u Arc<Program>> {
	admitted_unit_as(units, unit, ctx, ctx.root().skip_fetch_perms, |frame| {
		class_digest(ctx, frame)
	})
}

/// [`admitted_unit`], for a run that skips permission checks when `skips`,
/// with the digest of the class of the principal `ctx` runs under answered by
/// `principal` for the unit's frame, which is asked only of a unit that names
/// one.
pub(super) fn admitted_unit_as<'u>(
	units: &'u [Arc<Program>],
	unit: super::program::UnitId,
	ctx: &ExecutionContext,
	skips: bool,
	principal: impl FnOnce(&super::program::UnitFrame) -> u64,
) -> FlowResult<&'u Arc<Program>> {
	let internal =
		|why: String| ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(why)));
	let program = units
		.get(unit.0 as usize)
		.ok_or_else(|| internal(format!("a unit the program does not hold: {}", unit.0)))?;
	let head = program
		.unit
		.as_ref()
		.ok_or_else(|| internal(format!("unit {} holds no frame it was lowered for", unit.0)))?;
	if !frame_admits_as(&head.frame, ctx, skips, principal) {
		return Err(internal(format!(
			"unit {} was lowered for a frame other than the one it runs in",
			unit.0
		)));
	}
	Ok(program)
}

/// The name and value of each of `inputs`, read where the instruction that
/// runs their unit sits.
fn unit_inputs(
	regs: &[Scalar<'_>],
	row: &dyn RowReader,
	program: &Program,
	inputs: &[super::program::UnitInput],
) -> anyhow::Result<Vec<(Strand, Value)>> {
	inputs
		.iter()
		.map(|input| {
			Ok((input.name.clone(), interp::read_operand(regs, row, program, input.value)?))
		})
		.collect()
}

/// The value of subquery `spec` evaluated in `eval_ctx`: its unit, a program
/// of its own, run over the context the enclosing row derives
/// ([`crate::exec::vm::subquery::subquery_context`]), and its rows gathered — its one value, `NONE`
/// for none, when the unit's value is one row, and an array of its rows
/// otherwise.
///
/// The unit runs on the frame stack of the statement's driver, when there is
/// one, so what it nests does not hold this evaluation's frames on the native
/// stack beneath its own. It runs only in a frame that matches the one it was
/// lowered for. `inputs` are the values of the spec's
/// [`UnitInput`](super::program::UnitInput)s, bound under their names in that
/// context, and `knn` the distances the search of the statement holding the
/// subquery publishes, which the unit inherits ([`inherit_knn`]).
async fn run_subprogram(
	units: &super::UnitTable,
	spec: &super::program::SubquerySpec,
	eval_ctx: EvalContext<'_>,
	inputs: Vec<(Strand, Value)>,
	knn: Option<Arc<crate::exec::function::KnnContext>>,
) -> FlowResult<Value> {
	let ctx = crate::exec::vm::subquery::subquery_context(&eval_ctx, spec.inherits_row);
	let ctx = match knn {
		Some(knn) => ctx.with_inherited_knn(knn),
		None => ctx,
	};
	let ctx = match inputs.is_empty() {
		true => ctx,
		false => ctx.with_params(inputs),
	};
	let unit = Arc::clone(admitted_unit(units, spec.unit, &ctx)?);
	// A unit that holds no arena of its own finds the units its ids name in
	// the table it was found in.
	let ctx =
		if unit.units.is_empty() && !ctx.vm_units().is_some_and(|held| Arc::ptr_eq(held, units)) {
			ctx.with_vm_units(Arc::clone(units))
		} else {
			ctx
		};
	crate::exec::trampoline::nest(gather_program(unit, ctx)).await
}

/// The rows of `program` run in `ctx`, or its one value when its value is one
/// row.
///
/// A program that writes nothing and has no tail runs in place, on the frame
/// stack of its caller, with its rows gathered as a block statement's are;
/// any other runs as the stream [`execute_program`] answers. The future is a
/// trait object, since the program may call, through a function body or a
/// subquery, a program gathered here again.
pub(super) fn gather_program(
	program: Arc<Program>,
	ctx: ExecutionContext,
) -> crate::exec::BoxFut<'static, FlowResult<Value>> {
	crate::exec::vm::host::out_of_line_boxed(|| async move {
		let mut values = if program.write.is_none() && program.tail.is_none() {
			let unitted = super::closure::with_units(&program, &ctx);
			let ctx = unitted.as_ref().unwrap_or(&ctx);
			let dispatched = super::dispatch::with_dispatch(&program, ctx);
			let ctx = dispatched.as_ref().unwrap_or(ctx);
			// As `execute_untailed` confines a statement that reads.
			if !program.scalar {
				validate_record_user_access(ctx.database()?)?;
			}
			let mut out = Out::Gather(Vec::new());
			let mut state = run_untailed(&program, ctx, &mut out, None, None).await?;
			let mut values = match out {
				Out::Gather(values) => values,
				Out::Stream(_) | Out::Sink(_) => Vec::new(),
			};
			values.append(&mut state.output);
			values
		} else {
			let mut stream = execute_program(&program, &ctx)?;
			let mut values = Vec::new();
			while let Some(batch) = stream.next().await {
				values.extend(batch?.into_values());
			}
			values
		};
		if output_shape(&program).is_scalar() {
			Ok(values.pop().unwrap_or(Value::None))
		} else {
			Ok(Value::Array(crate::val::Array(values)))
		}
	})
}

/// Run `program`, a unit of `units` that [`admitted_unit`] answered for
/// `ctx`, over `row` — the value its reads of the current value take, none for
/// a unit that runs rowless — with `args` bound to its parameters in order,
/// in the frame `ctx`, and answer its value. The unit runs over a state of its
/// own, its reads checked as `ctx`'s are.
fn run_unit<'a>(
	program: &'a Program,
	units: &'a Arc<[Arc<Program>]>,
	ctx: &'a ExecutionContext,
	row: Option<&'a Value>,
	args: Vec<Value>,
) -> impl std::future::Future<Output = FlowResult<Value>> + Send + 'a {
	run_unit_in(program, units, ctx, row, args, None)
}

/// [`run_unit`], in phase `recursion` of the `.@` recursion whose body the
/// unit is, which its `.@` markers answer in ([`Instr::RepeatRecurse`]).
async fn run_unit_in(
	program: &Program,
	units: &Arc<[Arc<Program>]>,
	ctx: &ExecutionContext,
	row: Option<&Value>,
	args: Vec<Value>,
	recursion: Option<crate::exec::eval_context::RecursionCtx>,
) -> FlowResult<Value> {
	let head = program.unit.as_ref().ok_or_else(|| {
		ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
			"a unit holds no frame it was lowered for".to_owned(),
		)))
	})?;
	if args.len() != head.sig.params.len() {
		return Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
			format!(
				"a unit takes {} arguments and was called with {}",
				head.sig.params.len(),
				args.len()
			),
		))));
	}
	let mut state = statement_state(program, ctx)?;
	state.units = Some(Arc::clone(units));
	state.rowless = !head.sig.takes_row || row.is_none();
	state.slots = args.into_iter().map(Held::Owned).collect();
	state.keep_slots(ctx).await?;
	state.recursion = recursion;
	let row = row.cloned().unwrap_or(Value::None);
	let pipeline = program.own_pipeline().ok_or_else(|| {
		ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
			"a unit holds no pipeline of its own".to_owned(),
		)))
	})?;
	run_value_row(program, pipeline, row, &mut state, ctx).await?;
	Ok(state.output.pop().unwrap_or(Value::None))
}

/// The units of a root program, shared by the runs that execute them.
type UnitTable = Arc<[Arc<Program>]>;

/// A recursion's body at each depth: the unit of the root it names, run over
/// the value that depth steps from, or the value itself for a recursion with
/// no body.
struct RecurseBody<'a> {
	unit: Option<(&'a Arc<Program>, &'a UnitTable)>,
	ctx: &'a ExecutionContext,
}

impl crate::exec::operators::recursion::DepthBody for RecurseBody<'_> {
	fn eval<'a>(&'a self, value: &'a Value) -> crate::exec::BoxFut<'a, FlowResult<Value>> {
		crate::exec::vm::host::out_of_line_boxed(|| async move {
			// A depth is a unit, whatever its body runs.
			super::fuel::tick(self.ctx, 1).await?;
			match self.unit {
				Some((program, units)) => {
					run_unit(program, units, self.ctx, Some(value), Vec::new()).await
				}
				None => Ok(value.clone()),
			}
		})
	}
}

/// Recursion `spec` from `value`: its body, a unit of the root, evaluated with each depth's value
/// as the current value. `units` is the root's unit table when `spec` has a
/// body.
async fn recurse(
	spec: &super::program::RecurseSpec,
	units: Option<Arc<[Arc<Program>]>>,
	value: &Value,
	target: Option<&Value>,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	use super::program::RecurseCollect;
	use crate::exec::operators::recursion;
	let bounds = recursion::RecursionBounds {
		min: spec.min,
		max: spec.max,
		system_limit: ctx.ctx().config.exec.idiom_recursion_limit,
	};
	let unit = match (spec.body, units.as_ref()) {
		(Some(id), Some(units)) => Some((admitted_unit(units, id, ctx)?, units)),
		(Some(_), None) => {
			return Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
				"a recursion body is called and no unit table is held".to_owned(),
			))));
		}
		(None, _) => None,
	};
	if spec.repeat {
		let Some((program, units)) = unit else {
			return Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
				"a `.@` recursion with no body".to_owned(),
			))));
		};
		let body = RepeatUnit {
			program,
			units,
			ctx,
		};
		return crate::exec::vm::host::out_of_line(|| {
			recursion::evaluate_recurse_iterative_with(value, bounds, &body)
		})
		.await;
	}
	let body = RecurseBody {
		unit,
		ctx,
	};
	match &spec.collect {
		RecurseCollect::Last => {
			recursion::evaluate_recurse_default_with(value, &body, bounds).await
		}
		RecurseCollect::All {
			inclusive,
		} => recursion::evaluate_recurse_collect_with(value, &body, bounds, *inclusive).await,
		RecurseCollect::Paths {
			inclusive,
		} => recursion::evaluate_recurse_path_with(value, &body, bounds, *inclusive).await,
		RecurseCollect::Shortest {
			inclusive,
		} => {
			let target = target.ok_or_else(|| {
				ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
					"a shortest path with no target".to_owned(),
				)))
			})?;
			recursion::evaluate_recurse_shortest_with(value, target, &body, bounds, *inclusive)
				.await
		}
	}
}

/// A `.@` recursion's body at each depth: the unit of the root it names, run
/// over the value that depth steps from in the phase the recursion is in.
struct RepeatUnit<'a> {
	program: &'a Arc<Program>,
	units: &'a UnitTable,
	ctx: &'a ExecutionContext,
}

impl crate::exec::operators::recursion::RepeatBody for RepeatUnit<'_> {
	fn eval<'a>(
		&'a self,
		value: &'a Value,
		rec: crate::exec::eval_context::RecursionCtx,
	) -> crate::exec::BoxFut<'a, FlowResult<Value>> {
		crate::exec::vm::host::out_of_line_boxed(|| async move {
			super::fuel::tick(self.ctx, 1).await?;
			run_unit_in(self.program, self.units, self.ctx, Some(value), Vec::new(), Some(rec))
				.await
		})
	}
}

/// The computed indexes of a program's `FETCH` steps: each the value of its
/// host pipeline.
pub(super) struct ProgramIndex<'a> {
	pub(super) program: &'a Program,
}

impl crate::exec::operators::fetch::FetchWalk for super::program::FetchStepSpec {
	fn step(&self) -> crate::exec::operators::fetch::WalkStep<'_> {
		use crate::exec::operators::fetch::WalkStep;
		match self {
			super::program::FetchStepSpec::Field(name) => WalkStep::Field(name),
			super::program::FetchStepSpec::All => WalkStep::All,
			super::program::FetchStepSpec::First => WalkStep::First,
			super::program::FetchStepSpec::Last => WalkStep::Last,
			super::program::FetchStepSpec::Index {
				..
			} => WalkStep::Index,
			super::program::FetchStepSpec::Unsupported => WalkStep::Unsupported,
		}
	}
}

impl crate::exec::operators::fetch::FetchIndex<super::program::FetchStepSpec> for ProgramIndex<'_> {
	fn index<'a>(
		&'a self,
		ctx: &'a ExecutionContext,
		step: &'a super::program::FetchStepSpec,
	) -> crate::exec::BoxFut<'a, FlowResult<Value>> {
		crate::exec::vm::host::out_of_line_boxed(|| async move {
			if let super::program::FetchStepSpec::Index {
				pipeline,
			} = step
			{
				let scalar = super::program::HostScalar::Code(*pipeline);
				host_scalar(self.program, &scalar, ctx, None).await
			} else {
				Ok(Value::None)
			}
		})
	}
}

/// `value` destructured into `fields`: an object's fields gathered, a record id's
/// record fetched — without its table's permissions when `skip` — and
/// destructured, an array's elements each destructured, and `NONE` for
/// anything else. An aliased field is its host pipeline's value over the
/// value destructured.
async fn destructure(
	program: &Program,
	fields: &[super::program::DestructureStep],
	value: Value,
	skip: bool,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	use super::program::DestructureStep;
	// A fetch spends a unit of the computation-depth budget, as a field read
	// through a link does.
	let fetch = async |rid: &crate::val::RecordId| {
		let ctx = ctx.dive()?;
		if skip {
			crate::exec::operators::fetch::fetch_record_no_perms(&ctx, rid).await
		} else {
			crate::exec::operators::fetch::fetch_record(&ctx, rid).await
		}
	};
	match value {
		Value::Object(ref obj) => {
			let mut result = std::collections::BTreeMap::new();
			for field in fields {
				match field {
					DestructureStep::All(name) => {
						let resolved = match obj.get(name.as_str()) {
							Some(Value::RecordId(rid)) => fetch(rid).await?,
							Some(other) => other.clone(),
							None => Value::None,
						};
						if let Value::Object(nested) = resolved {
							result.insert(name.clone(), Value::Object(nested));
						}
					}
					DestructureStep::Field(name) => {
						result.insert(
							name.clone(),
							obj.get(name.as_str()).cloned().unwrap_or(Value::None),
						);
					}
					DestructureStep::Aliased(name, pipeline) => {
						let answered =
							write::run_image(program, *pipeline, value.clone(), None, state, ctx)
								.await?;
						result.insert(name.clone(), answered);
					}
					DestructureStep::Nested(name, parts) => {
						let nested = obj.get(name.as_str()).cloned().unwrap_or(Value::None);
						let answered = crate::exec::vm::host::out_of_line(|| {
							destructure(program, parts, nested, skip, state, ctx)
						})
						.await?;
						result.insert(name.clone(), answered);
					}
					DestructureStep::Rest => {
						for (key, v) in obj.iter() {
							if !result.contains_key(key.as_str()) {
								result.insert(key.clone(), v.clone());
							}
						}
					}
				}
			}
			Ok(Value::Object(crate::val::Object::from(result)))
		}
		Value::RecordId(ref rid) => {
			let mut fetched = fetch(rid).await?;
			// The record the session is signed in as reads its own `id`
			// whether or not the record, or its `id` field, can be read
			// (`$auth.{ id, email }` is `{ id: $auth, email: NONE }`), as a
			// path does.
			if crate::exec::operators::fetch::is_session_record(ctx, rid) {
				let id = Value::RecordId(rid.clone());
				if let Value::Object(obj) = &mut fetched {
					obj.insert("id".to_owned(), id);
				} else {
					fetched = Value::Object(crate::val::Object::from(
						std::collections::BTreeMap::from([("id".to_owned(), id)]),
					));
				}
			} else if fetched.is_none() {
				return Ok(Value::None);
			}
			crate::exec::vm::host::out_of_line(|| {
				destructure(program, fields, fetched, skip, state, ctx)
			})
			.await
		}
		Value::Array(items) => {
			// A collection spends a unit of the computation-depth budget on
			// top of each element's fetch: a destructure through a cycle of
			// links nests more frames per level than a field read does, and
			// the extra unit ends the cycle within the worker's stack.
			let ctx = &ctx.dive()?;
			let mut results = Vec::with_capacity(items.len());
			for item in items {
				results.push(
					crate::exec::vm::host::out_of_line(|| {
						destructure(program, fields, item, skip, state, ctx)
					})
					.await?,
				);
			}
			Ok(Value::Array(results.into()))
		}
		// A set is destructured element by element as an array is, its
		// results collected back into a set.
		Value::Set(items) => {
			// A collection spends a unit of the computation-depth budget on
			// top of each element's fetch: a destructure through a cycle of
			// links nests more frames per level than a field read does, and
			// the extra unit ends the cycle within the worker's stack.
			let ctx = &ctx.dive()?;
			let mut results = Vec::with_capacity(items.len());
			for item in items {
				results.push(
					crate::exec::vm::host::out_of_line(|| {
						destructure(program, fields, item, skip, state, ctx)
					})
					.await?,
				);
			}
			Ok(Value::Set(results.into_iter().collect()))
		}
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
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => Ok(Value::None),
	}
}

/// The frame instruction `instr` invokes closures in, and the closure values
/// it hands a callee: the target of an `Invoke`, the arguments of a call or a
/// method at the positions its builtin invokes a closure from, and the field
/// of a method-closure's receiver it calls.
fn closures_handed(
	program: &Program,
	instr: &Instr,
	regs: &[Scalar<'_>],
	row: &dyn RowReader,
) -> anyhow::Result<(Option<u16>, Vec<Value>)> {
	let read = |operand: super::program::Operand| interp::read_operand(regs, row, program, operand);
	let at = |args: &[super::program::Operand], positions: &[u16]| -> anyhow::Result<Vec<Value>> {
		positions
			.iter()
			.filter_map(|&position| args.get(position as usize))
			.map(|&arg| read(arg))
			.collect()
	};
	if let Instr::Invoke {
		target,
		frame,
		..
	} = instr
	{
		Ok((*frame, vec![read(*target)?]))
	} else if let Instr::Call {
		call,
		args,
		frame,
		..
	} = instr
	{
		Ok((*frame, at(args, &program.calls[*call as usize].closure_args)?))
	} else if let Instr::Method {
		method,
		args,
		frame,
		..
	} = instr
	{
		Ok((*frame, at(args, &program.methods[*method as usize].closure_args)?))
	} else if let Instr::MethodClosure {
		method,
		recv,
		frame,
		..
	} = instr
	{
		let name = program.methods[*method as usize].name.as_str();
		let field = if let Value::Object(mut object) = read(*recv)? {
			object.remove(name)
		} else {
			None
		};
		Ok((*frame, field.into_iter().collect()))
	} else {
		Ok((None, Vec::new()))
	}
}

/// Apply method `method` to the receiver `recv` and `args`, once the
/// receiver has been resolved and
/// the arguments evaluated: the closure the receiver holds under the method's
/// name when `closure`, and otherwise the builtin that implements the method
/// for the receiver's type, behind the capability that refuses a direct call
/// of that builtin and the experimental gate, with an argument error named
/// for the method.
async fn call_method(
	method: &super::program::MethodSpec,
	descriptor: Option<Arc<crate::exec::function::MethodDescriptor>>,
	recv: Value,
	args: Vec<Value>,
	closure: bool,
	mut ctx: EvalContext<'_>,
) -> FlowResult<Value> {
	let name = method.name.as_str();
	let unknown = |recv: &Value| {
		ControlFlow::Err(anyhow::Error::new(crate::exec::Error::InvalidFunction {
			name: name.to_string(),
			message: format!("no such method found for the {} type", recv.kind_of()),
		}))
	};
	ctx.plan_depth = ctx.plan_depth.max(method.plan_depth);
	if closure {
		let Value::Object(object) = &recv else {
			return Err(unknown(&recv));
		};
		let Some(value @ Value::Closure(body)) = object.get(name) else {
			return Err(unknown(&recv));
		};
		return match super::closure::compiled_of(value) {
			Some(compiled) => compiled.invoke(&ctx, args).await,
			None => {
				crate::exec::vm::calls::helpers::invoke_uncompiled_closure(body, args, &ctx).await
			}
		};
	}
	let Some(descriptor) = descriptor else {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"Unknown function '{name}' - not found in function registry"
		)));
	};
	let Some(func) = descriptor.resolve(&recv) else {
		return Err(unknown(&recv));
	};
	let mut func_args = Vec::with_capacity(1 + args.len());
	func_args.push(recv);
	func_args.extend(args);
	ctx.check_allowed_function(func.name()).map_err(ControlFlow::Err)?;
	refuse_mutating_builtin(func.name(), func.effects(), ctx.exec_ctx)?;
	let result = invoke_builtin(func, func_args, &ctx).await?;
	let writing = method.writing || ctx.exec_ctx.root().writing;
	let resolved = func.name();
	result.map_err(|e| match e.downcast_ref::<crate::expr::Error>() {
		// Code a write reaches names the method's own argument error after the
		// method, as a function, and leaves an error raised beneath it as it was
		// raised ([`crate::exec::context::RootContext::writing`]).
		Some(crate::expr::Error::InvalidFunctionArguments {
			name: raised,
			message,
		}) if writing => {
			if raised == resolved {
				ControlFlow::Err(anyhow::Error::new(crate::expr::Error::InvalidFunctionArguments {
					name: name.to_string(),
					message: message.clone(),
				}))
			} else {
				ControlFlow::Err(e)
			}
		}
		Some(crate::expr::Error::InvalidFunctionArguments {
			message,
			..
		}) => ControlFlow::Err(method_arguments_error(name, message)),
		_ => ControlFlow::Err(e),
	})
}

/// The error a method named `name` raises for an argument error `message`
/// raised beneath it.
pub(super) fn method_arguments_error(name: &str, message: &str) -> anyhow::Error {
	anyhow::Error::new(crate::expr::Error::InvalidMethodArguments {
		name: name.to_string(),
		message: message.to_string(),
	})
}

/// Refuse a write the frame or the transaction cannot hold, before any of the
/// statement's work begins, as the write operator refuses one.
fn refuse_unwritable(spec: &super::program::WriteSpec, ctx: &ExecutionContext) -> FlowResult<()> {
	if let Some(frame) = ctx.root().no_write {
		let construct = crate::exec::clause_effects::write_text(spec.form.kind());
		return Err(ControlFlow::Err(anyhow::anyhow!(frame.side_effect_error(&construct))));
	}
	let Some(txn) = ctx.try_txn() else {
		return Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(
			format!("Cannot run {} without a transaction", spec.form.kind())
		))));
	};
	if !txn.writeable() {
		return Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(
			format!("Cannot run {} on a read only transaction", spec.form.kind())
		))));
	}
	Ok(())
}

#[cfg(test)]
mod knn_query_cell {
	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};

	use super::read_knn_query;
	use crate::exec::vm::interp::KnnQueryCell;
	use crate::expr::ControlFlow;

	/// The error the vector's one evaluation raises.
	fn refusal() -> anyhow::Error {
		anyhow::Error::new(crate::exec::Error::InvalidKnnVector {
			vector: "$v".to_owned(),
		})
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_failed_evaluation_runs_once_and_every_reader_raises_it() {
		let cell = KnnQueryCell::default();
		let calls = Arc::new(AtomicUsize::new(0));
		let readers: Vec<_> = (0..8)
			.map(|_| {
				let cell = Arc::clone(&cell);
				let calls = Arc::clone(&calls);
				tokio::spawn(async move {
					read_knn_query(&cell, || async {
						calls.fetch_add(1, Ordering::SeqCst);
						// Let the other readers reach the cell while this one
						// evaluates.
						tokio::time::sleep(std::time::Duration::from_millis(20)).await;
						Err(ControlFlow::Err(refusal()))
					})
					.await
				})
			})
			.collect();
		let expected = crate::err::anyhow_to_types_error(refusal());
		for reader in readers {
			let Err(ControlFlow::Err(error)) = reader.await.expect("a reader") else {
				panic!("a reader that does not raise the evaluation's failure");
			};
			// Every reader raises the failure as the caller would have seen it.
			assert_eq!(crate::err::anyhow_to_types_error(error), expected);
		}
		// A reader after the failure raises it too, evaluating nothing.
		let late = read_knn_query(&cell, || async {
			calls.fetch_add(1, Ordering::SeqCst);
			Ok(Arc::from(vec![crate::val::Number::Int(1)]))
		})
		.await;
		assert!(matches!(late, Err(ControlFlow::Err(_))));
		assert_eq!(calls.load(Ordering::SeqCst), 1);
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_vector_is_evaluated_once_for_every_reader() {
		let cell = KnnQueryCell::default();
		let calls = Arc::new(AtomicUsize::new(0));
		let readers: Vec<_> = (0..8)
			.map(|_| {
				let cell = Arc::clone(&cell);
				let calls = Arc::clone(&calls);
				tokio::spawn(async move {
					read_knn_query(&cell, || async {
						calls.fetch_add(1, Ordering::SeqCst);
						tokio::time::sleep(std::time::Duration::from_millis(20)).await;
						Ok(Arc::from(vec![crate::val::Number::Int(5)]))
					})
					.await
				})
			})
			.collect();
		for reader in readers {
			let vector = reader.await.expect("a reader").expect("the vector");
			assert_eq!(&*vector, &[crate::val::Number::Int(5)]);
		}
		assert_eq!(calls.load(Ordering::SeqCst), 1);
	}
}
