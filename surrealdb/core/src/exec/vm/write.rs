//! The host's side of a write body: the record in flight, and the services
//! its instructions ask for.
//!
//! A write body names every step it takes, and the host takes each one with
//! the write path's own parts — the subjects a target names, the statement
//! gate, each table's write context, the stages and the create-failure
//! classification — so the VM and the write operator run a record the same
//! way. One record is in flight at a time, on the task that owns the
//! transaction: savepoints are a stack with no handle, and a later record's
//! stages read an earlier record's writes.

use std::sync::Arc;

use common::future::stream::Yielder;
use surrealdb_types::ToSql as _;

use super::host::{Out, flush, out_of_line, run_storage_source, run_value_row};
use super::interp::{PooledState, State};
use super::program::{
	InsertRowsSpec, Instr, Label, Pipeline, Program, Source, UnitId, WriteInstr, WriteOutput,
	WriteSpec,
};
use crate::catalog::providers::TableProvider;
use crate::exec::operators::check_cancelled;
use crate::exec::operators::scan::common::resolve_version_stamp;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::plan::{CreateFailure, WriteForm, classify_create_failure};
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::scope::{WritePurpose, WriteScope};
use crate::exec::operators::write::stage::{RecordImage, StageOutcome};
use crate::exec::operators::write::{
	RelateSide, Rows, Subject, TargetSubjects, WriteGate, counts_as_affected, insert_row,
	read_subject, refused_by, relate_endpoints, relate_row, scan_subjects, target_subjects,
};
use crate::exec::{Error as ExecError, ExecutionContext, FlowResult, ValueBatch, ValueBatchStream};
use crate::expr::{ControlFlow, Operation};
use crate::val::{RecordId, Value};

#[path = "write_cascade.rs"]
mod cascade;
#[path = "write_data.rs"]
mod data;
#[path = "write_gate.rs"]
mod gate;
#[path = "write_observe.rs"]
mod observe;
#[path = "write_schema.rs"]
mod schema;
#[path = "write_storage.rs"]
mod storage;
#[path = "write_view.rs"]
mod view;

#[cfg(all(test, feature = "kv-mem"))]
pub(super) use cascade::CASCADE_UNITS_RUN;
pub(super) use cascade::{ReferenceInstr, RepairStrategy, TableRepairs};
pub(super) use data::DataInstr;
pub(super) use gate::GateInstr;
pub(crate) use observe::notify_at;
pub(super) use observe::{
	Admission, ClauseDepth, EventClauses, LoweredEvents, LoweredSubscription, ObserveInstr,
	SubscriptionFieldsLowered,
};
#[cfg(all(test, feature = "kv-mem"))]
pub(super) use observe::{LIVES_DECLINED, LIVES_LOWERED, LIVES_RUN};
pub(super) use schema::{FieldPermission, FieldPlan, FieldWrite, SchemaInstr, SchemaPlan};
#[cfg(all(test, feature = "kv-mem"))]
pub(super) use storage::INDEX_STEPS;
pub(super) use storage::{IndexPlan, LoweredIndexes, NativeIndexKind, StorageFact, StorageInstr};
pub(super) use view::{LoweredView, LoweredViews, Recalc, ViewClause, ViewRows};

/// A write statement's state across its records.
pub(super) struct WriteState {
	scope: Arc<WriteScope>,
	gate: WriteGate,
	/// The record in flight.
	row: Option<WriteRow>,
	/// The write context of the record's table, once [`WriteInstr::Begin`]
	/// resolved it.
	ctx: Option<Arc<WriteCtx>>,
	/// The handlers of the open [`WriteInstr::TryBegin`] regions, innermost
	/// last.
	handlers: Vec<Label>,
	/// The failure a handler resumed with.
	failure: Option<ControlFlow>,
	/// The record a classified create failure is retried as an update of.
	retry: Option<RecordId>,
	/// The failure the classification carried into the retry.
	carried: Option<anyhow::Error>,
	/// Whether the carrying region is open, so a failure is replaced by the
	/// carried one.
	carrying: bool,
	/// Whether the statement asked for a single record with `ONLY`.
	only: bool,
	/// Whether `ONLY` is judged as each record is written rather than after
	/// each batch of the records a read yields.
	per_record: bool,
	/// Rows shown to the caller, which `ONLY` is judged against.
	pub(super) returned: usize,
	/// Records the statement's condition accepted, which the guaranteed
	/// record is judged against.
	matched: usize,
	/// The record's images as [`WriteInstr::ShowImage`] showed them to the
	/// caller: the pre-image, then the post-image.
	shown: [Option<Value>; 2],
	/// The write contexts a block statement's earlier reaches resolved, kept
	/// across its reaches: the state is its program's for one run, in which
	/// the catalog they were resolved against does not change.
	kept: Option<Arc<crate::exec::operators::write::scope::KeptContexts>>,
	/// The stage and cascade units already found to be lowered for the frame
	/// the statement runs in, which holds for every record of it, each by the
	/// arena it is a unit of ([`arena_of`]) and its id there.
	admitted: Vec<(usize, super::program::UnitId)>,
	/// The bundles of the stages the run loaded by table name for a write
	/// that may reach any table, each by its table and the catalog version it
	/// was loaded for, so a table's bundle is loaded once per run of the
	/// statement while its catalog stands ([`run_stages`]).
	bundles: Vec<(surrealdb_strand::TableName, Option<uuid::Uuid>, Arc<Program>)>,
	/// Whether the stages running are a stage unit's, whose failures the
	/// [`WriteInstr::DispatchTable`] that ran them hands to the body's
	/// handlers.
	nested: bool,
	/// The field schema's state for the record, while the schema stage's
	/// instructions apply it.
	schema: Option<Box<schema::SchemaRecord>>,
	/// The reference keys naming the record in flight's delete, while its
	/// cascades repair what they name.
	references: Option<Box<cascade::References>>,
	/// The states the statements of this write's cascades run over, kept
	/// between their runs, by statement.
	cascades: Vec<cascade::CascadePool>,
	/// The views a record's context last held, the views lowered for them,
	/// and whether the two are the same definitions: a context holds its
	/// views for the statement, so they are compared once rather than per
	/// record.
	views_held: Option<(Arc<[crate::catalog::TableDefinition]>, Arc<LoweredViews>, bool)>,
	/// The events a record's context last held, the events lowered for them,
	/// and whether the two are the same definitions, compared once for the
	/// same reason.
	events_held: Option<(Arc<[crate::catalog::EventDefinition]>, usize, bool)>,
	/// The block statement compiled into the program whose write this is, or
	/// `None` for the program's own.
	pub(super) group: Option<u16>,
}

impl WriteState {
	pub(super) fn new(spec: &WriteSpec, ctx: &ExecutionContext) -> FlowResult<Self> {
		Ok(Self {
			scope: scope_for(spec, ctx, None),
			gate: WriteGate::resolve(spec.form, ctx)?,
			row: None,
			ctx: None,
			handlers: Vec::new(),
			failure: None,
			retry: None,
			carried: None,
			carrying: false,
			only: spec.only,
			per_record: false,
			returned: 0,
			matched: 0,
			shown: [None, None],
			kept: None,
			admitted: Vec::new(),
			bundles: Vec::new(),
			nested: false,
			schema: None,
			references: None,
			views_held: None,
			events_held: None,
			cascades: Vec::new(),
			group: None,
		})
	}

	/// The write of a row a view's maintenance computed on its view table,
	/// in flight from the start in `ctx`, the view table's view-maintenance
	/// context: taken through the view table's order by a program of its own,
	/// over `scope`, the writing statement's. Nothing gates it, and a failure
	/// leaves its program for the maintenance that ran it.
	pub(super) fn view_row(scope: Arc<WriteScope>, row: WriteRow, ctx: Arc<WriteCtx>) -> Self {
		Self {
			scope,
			gate: WriteGate::unchecked(WriteForm::View),
			row: Some(row),
			ctx: Some(ctx),
			handlers: Vec::new(),
			failure: None,
			retry: None,
			carried: None,
			carrying: false,
			only: false,
			per_record: false,
			returned: 0,
			matched: 0,
			shown: [None, None],
			kept: None,
			admitted: Vec::new(),
			bundles: Vec::new(),
			nested: true,
			schema: None,
			references: None,
			views_held: None,
			events_held: None,
			cascades: Vec::new(),
			group: None,
		}
	}

	/// Whether `views`, the views a record's context holds, are the ones
	/// `lowered` was lowered from.
	fn views_hold(
		&mut self,
		views: &Arc<[crate::catalog::TableDefinition]>,
		lowered: &Arc<LoweredViews>,
	) -> bool {
		if let Some((held, of, hold)) = &self.views_held
			&& Arc::ptr_eq(held, views)
			&& Arc::ptr_eq(of, lowered)
		{
			return *hold;
		}
		let hold = Arc::ptr_eq(views, &lowered.definitions) || **views == *lowered.definitions;
		self.views_held = Some((Arc::clone(views), Arc::clone(lowered), hold));
		hold
	}

	/// Whether `events`, the events a record's context holds, are the ones
	/// `lowered` was lowered from.
	fn events_hold(
		&mut self,
		events: &Arc<[crate::catalog::EventDefinition]>,
		lowered: &LoweredEvents,
	) -> bool {
		let of = std::ptr::from_ref(lowered) as usize;
		if let Some((held, held_of, hold)) = &self.events_held
			&& Arc::ptr_eq(held, events)
			&& *held_of == of
		{
			return *hold;
		}
		let hold = Arc::ptr_eq(events, &lowered.events) || **events == *lowered.events;
		self.events_held = Some((Arc::clone(events), of, hold));
		hold
	}

	/// The write contexts a block statement's reaches share, when this is one.
	pub(super) fn kept_contexts(
		&self,
	) -> Option<Arc<crate::exec::operators::write::scope::KeptContexts>> {
		self.kept.clone()
	}

	/// Keep what this state compiles in `kept` from its first run on, shared
	/// with every other state given the same.
	pub(super) fn keeping_in(
		&mut self,
		kept: Arc<crate::exec::operators::write::scope::KeptContexts>,
	) {
		self.kept = Some(kept);
	}

	/// Start the write of block statement `group` afresh in `ctx`, as
	/// [`Self::new`] starts one, keeping what this state has allocated and
	/// the stage programs it has lowered, the catalog its write contexts read
	/// and what its gate has answered, all of which hold for the run.
	pub(super) fn reset(
		&mut self,
		spec: &WriteSpec,
		group: u16,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		self.scope = scope_for(spec, ctx, Some(&mut self.kept));
		self.gate.again(spec.form, ctx)?;
		self.row = None;
		self.ctx = None;
		self.handlers.clear();
		self.failure = None;
		self.retry = None;
		self.carried = None;
		self.carrying = false;
		self.only = spec.only;
		self.per_record = false;
		self.returned = 0;
		self.matched = 0;
		self.shown = [None, None];
		self.nested = false;
		self.schema = None;
		self.references = None;
		self.group = Some(group);
		Ok(())
	}

	fn row(&mut self) -> Result<&mut WriteRow, ControlFlow> {
		self.row.as_mut().ok_or_else(|| internal("a write step with no record in flight"))
	}

	fn in_flight(&mut self) -> Result<(&mut WriteRow, Arc<WriteCtx>), ControlFlow> {
		let ctx = self
			.ctx
			.clone()
			.ok_or_else(|| internal("a write step before the record's context was resolved"))?;
		Ok((self.row()?, ctx))
	}

	/// Forget the record in flight.
	fn clear(&mut self) -> Option<WriteRow> {
		self.ctx = None;
		self.handlers.clear();
		self.failure = None;
		self.retry = None;
		self.carried = None;
		self.carrying = false;
		self.shown = [None, None];
		self.schema = None;
		self.references = None;
		self.row.take()
	}

	/// Take write instruction `instr` at `at` for the record in flight, and
	/// answer where the body continues.
	async fn service(
		&mut self,
		spec: &WriteSpec,
		instr: &WriteInstr,
		at: usize,
		row: &mut Value,
		output: &mut Vec<Value>,
	) -> Result<usize, ControlFlow> {
		let next = at + 1;
		Ok(match instr {
			WriteInstr::Record => {
				let record = match std::mem::take(row) {
					// A record id names the record, as a target does.
					Value::RecordId(id) if !id.key.is_range() => {
						read_subject(self.scope.exec(), id).await?
					}
					value => {
						WriteRow::for_record(value).map_err(|err| refused_by(err, spec.form))?
					}
				};
				self.row = Some(record);
				next
			}
			WriteInstr::Gate {
				skip,
			} => {
				let table = self.row()?.table().cloned();
				let exec = self.scope.exec().clone();
				if self.gate.admits(table.as_ref(), &exec).await? {
					next
				} else {
					*skip as usize
				}
			}
			WriteInstr::Begin => {
				// Polled per record: a statement whose stages are slow pulls no
				// fresh subject for as long as one record takes.
				let exec = self.scope.exec().clone();
				check_cancelled(&exec)?;
				exec.ctx().expect_not_timedout().await.map_err(ControlFlow::Err)?;
				let table = self.row()?.table().cloned().ok_or_else(|| {
					ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(
						"A write subject names neither a record nor a table".to_string()
					)))
				})?;
				let ctx = self.scope.write_ctx_for(&table, WritePurpose::Statement).await?;
				// The record-type marker does not travel with a stored value,
				// so a record picks it up here, before any stage that
				// dispatches on it runs.
				self.row()?.load_stored_metadata(&ctx).await?;
				self.ctx = Some(ctx);
				next
			}
			WriteInstr::LoadNamedSubject => {
				let (row, ctx) = self.in_flight()?;
				row.load_named_subject(&ctx).await?;
				next
			}
			WriteInstr::JumpIfNew {
				target,
			} => {
				if self.row()?.is_new() {
					*target as usize
				} else {
					next
				}
			}
			WriteInstr::JumpIfNotIterationInitial {
				target,
			} => {
				if self.row()?.is_iteration_initial() {
					next
				} else {
					*target as usize
				}
			}
			WriteInstr::SavepointPush => {
				let (_, ctx) = self.in_flight()?;
				ctx.txn().new_save_point().await.map_err(ControlFlow::Err)?;
				next
			}
			WriteInstr::SavepointRelease => {
				let (_, ctx) = self.in_flight()?;
				ctx.txn().release_last_save_point().await.map_err(ControlFlow::Err)?;
				next
			}
			WriteInstr::SavepointRollback => {
				let (_, ctx) = self.in_flight()?;
				ctx.txn().rollback_to_save_point().await.map_err(ControlFlow::Err)?;
				next
			}
			WriteInstr::TryBegin {
				handler,
			} => {
				self.handlers.push(*handler);
				next
			}
			WriteInstr::TryEnd => {
				self.handlers.pop();
				next
			}
			WriteInstr::ClearMutated => {
				self.row()?.mutated = false;
				next
			}
			WriteInstr::RethrowSignal => match self.failure.take() {
				Some(ControlFlow::Err(err)) => {
					self.failure = Some(ControlFlow::Err(err));
					next
				}
				Some(signal) => return Err(signal),
				None => return Err(internal("a handler resumed with no failure")),
			},
			WriteInstr::ClassifyCreateFailure {
				policy,
				skip,
			} => {
				let Some(ControlFlow::Err(err)) = self.failure.take() else {
					return Err(internal("a create failure to classify that is not an error"));
				};
				match classify_create_failure(err, self.row()?, *policy) {
					CreateFailure::Skip => *skip as usize,
					CreateFailure::Raise(err) => return Err(ControlFlow::Err(err)),
					CreateFailure::RetryAsUpdate {
						record,
						carried,
					} => {
						self.retry = Some(record);
						self.carried = carried;
						next
					}
				}
			}
			WriteInstr::JumpIfDone {
				target,
			} => {
				let (_, ctx) = self.in_flight()?;
				if ctx.exec().ctx().is_done(None).await.map_err(ControlFlow::Err)? {
					*target as usize
				} else {
					next
				}
			}
			WriteInstr::ReloadForUpdate => {
				let target = self
					.retry
					.take()
					.ok_or_else(|| internal("a retry with no record to update"))?;
				let (row, ctx) = self.in_flight()?;
				let stored = ctx
					.txn()
					.get_record(
						ctx.ns.namespace_id,
						ctx.db.database_id,
						&target.table,
						&target.key,
						ctx.exec().version_stamp(),
					)
					.await
					.map_err(ControlFlow::Err)?;
				row.reset_for_update_retry(target, &stored);
				next
			}
			WriteInstr::CarryBegin => {
				self.carrying = true;
				next
			}
			WriteInstr::CarryEnd => {
				self.carrying = false;
				self.carried = None;
				next
			}
			WriteInstr::JumpIfImport {
				target,
			} => {
				let (_, ctx) = self.in_flight()?;
				if ctx.exec().options().is_some_and(|options| options.import) {
					*target as usize
				} else {
					next
				}
			}
			WriteInstr::JumpIfNoPostImage {
				target,
			} => {
				if self.row()?.current.is_none() {
					*target as usize
				} else {
					next
				}
			}
			WriteInstr::ClearCurrentReduced => {
				self.row()?.current_reduced = None;
				next
			}
			WriteInstr::ShowImage {
				..
			}
			| WriteInstr::SetOutput {
				..
			}
			| WriteInstr::DispatchTable {
				..
			}
			| WriteInstr::Data(_)
			| WriteInstr::Schema(_)
			| WriteInstr::Permissions(_)
			| WriteInstr::Observe(_)
			| WriteInstr::Storage(_) => return Err(internal("a step serviced without its program")),
			WriteInstr::RaiseCarried => match self.carried.take() {
				Some(err) => return Err(ControlFlow::Err(err)),
				None => next,
			},
			WriteInstr::End {
				skipped,
			} => {
				let mut row = self.clear().ok_or_else(|| internal("a record ended twice"))?;
				let outcome = Ok(if *skipped {
					StageOutcome::SkipRow
				} else {
					StageOutcome::Continue
				});
				let exec = self.scope.exec();
				if counts_as_affected(row.mutated, &outcome)
					&& let Some(counters) = exec.ctx().statement_counters()
				{
					counters.record_affected();
				}
				if row.matched {
					self.matched += 1;
				}
				if !*skipped && let Some(value) = row.output.take() {
					self.returned += 1;
					if self.per_record {
						self.judge_only()?;
					}
					output.push(value);
				}
				next
			}
		})
	}

	/// Refuse a statement under `ONLY` that has returned more than one row.
	pub(super) fn judge_only(&self) -> FlowResult<()> {
		if self.only && self.returned > 1 {
			return Err(ControlFlow::Err(anyhow::anyhow!(ExecError::SingleOnlyOutput)));
		}
		Ok(())
	}

	/// Where a failure raised by the record in flight goes: out of a stage
	/// program to the [`WriteInstr::DispatchTable`] that ran it, and otherwise as
	/// [`Self::unwind`] sends it.
	fn fail(&mut self, failure: ControlFlow) -> FlowResult<usize> {
		if self.nested {
			return Err(failure);
		}
		self.unwind(failure)
	}

	/// Where a failure raised by the record in flight goes: to the handler of
	/// the innermost open region, or out of the statement — replaced by the
	/// failure the carrying region holds, and after counting the record if its
	/// writes stand.
	fn unwind(&mut self, failure: ControlFlow) -> FlowResult<usize> {
		if let Some(handler) = self.handlers.pop() {
			self.failure = Some(failure);
			return Ok(handler as usize);
		}
		let failure = match (self.carrying, self.carried.take()) {
			(true, Some(carried)) => ControlFlow::Err(carried),
			_ => failure,
		};
		let outcome: Result<StageOutcome, ControlFlow> = Err(failure);
		if let Some(row) = self.clear()
			&& counts_as_affected(row.mutated, &outcome)
			&& let Some(counters) = self.scope.exec().ctx().statement_counters()
		{
			counters.record_affected();
		}
		outcome.map(|_| unreachable!("an unwound record has no outcome"))
	}
}

/// The scope a run of the write `spec` describes takes in `ctx`. A
/// late-bound write's tables' catalog may change while the program runs, so
/// its scope checks each context against the catalog before taking it again
/// and keeps none from one run to the next; any other write's run takes the
/// contexts earlier runs left in `kept`, where it is run again and again.
fn scope_for(
	spec: &WriteSpec,
	ctx: &ExecutionContext,
	kept: Option<&mut Option<Arc<crate::exec::operators::write::scope::KeptContexts>>>,
) -> Arc<WriteScope> {
	let late = spec.tables.as_ref().is_some_and(|tables| tables.revalidates);
	let (output, existence) = (spec.output.clone(), spec.form.table_existence());
	match kept {
		_ if late => WriteScope::revalidating(ctx.clone(), Arc::default(), output, existence),
		Some(kept) => WriteScope::kept(
			ctx.clone(),
			Arc::default(),
			output,
			existence,
			Arc::clone(kept.get_or_insert_with(Default::default)),
		),
		None => WriteScope::new(ctx.clone(), Arc::default(), output, existence),
	}
}

fn internal(what: &str) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(what.to_owned())))
}

/// Take the write instruction at `at` and each write instruction the body
/// continues at after it, following the jumps between them, and answer the
/// first instruction the interpreter takes.
///
/// A record's write body is a run of write instructions with jumps between
/// them. Taken here in one host step, the run is set up once rather than once
/// per instruction; what an instruction answers is as [`step`] answers it.
pub(super) async fn steps(
	program: &Program,
	pipeline: &Pipeline,
	mut at: usize,
	state: &mut State,
	row: &mut Value,
) -> FlowResult<usize> {
	loop {
		match pipeline.code.get(at) {
			Some(Instr::Write(_)) => at = step(program, pipeline, at, state, row).await?,
			Some(Instr::Jump {
				target,
			}) => at = *target as usize,
			_ => return Ok(at),
		}
	}
}

/// Take the write instruction at `at` for the record in flight, and answer
/// the instruction the body continues at.
pub(super) async fn step(
	program: &Program,
	pipeline: &Pipeline,
	at: usize,
	state: &mut State,
	row: &mut Value,
) -> FlowResult<usize> {
	let Instr::Write(instr) = &pipeline.code[at] else {
		return Err(internal("a write step at an instruction that is not one"));
	};
	let spec = spec_of(program, state)?;
	let routed = match instr {
		WriteInstr::Data(instr) => Some(data::step(program, instr, at + 1, state).await),
		WriteInstr::Schema(instr) => Some(schema::step(program, instr, at + 1, state).await),
		WriteInstr::Permissions(instr) => Some(gate::step(program, instr, at + 1, state).await),
		WriteInstr::Observe(instr) => Some(observe::step(program, instr, at + 1, state).await),
		WriteInstr::Storage(instr) => Some(storage::step(program, instr, at + 1, state).await),
		_ => None,
	};
	if let Some(routed) = routed {
		let write = state
			.write
			.as_deref_mut()
			.ok_or_else(|| internal("a write body with no write state"))?;
		return match routed {
			Ok(next) => Ok(next),
			Err(failure) => write.fail(failure),
		};
	}
	if let WriteInstr::DispatchTable {
		site,
		skip,
	} = instr
	{
		let ran = out_of_line(|| run_stages(program, *site, state)).await;
		let write = state
			.write
			.as_deref_mut()
			.ok_or_else(|| internal("a write body with no write state"))?;
		return match ran {
			Ok(true) => Ok(at + 1),
			Ok(false) => Ok(*skip as usize),
			Err(failure) => write.fail(failure),
		};
	}
	if matches!(instr, WriteInstr::ShowImage { .. } | WriteInstr::SetOutput { .. }) {
		let shown = out_of_line(|| show(program, instr, state)).await;
		let write = state
			.write
			.as_deref_mut()
			.ok_or_else(|| internal("a write body with no write state"))?;
		return match shown {
			Ok(()) => Ok(at + 1),
			Err(failure) => write.fail(failure),
		};
	}
	if matches!(instr, WriteInstr::Begin) && !program.implicit_tables.is_empty() {
		let defined = out_of_line(|| define_shared_table(program, spec, state)).await;
		if let Err(failure) = defined {
			let write = state
				.write
				.as_deref_mut()
				.ok_or_else(|| internal("a write body with no write state"))?;
			return write.fail(failure);
		}
	}
	let State {
		write,
		output,
		..
	} = state;
	let write = write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	match write.service(spec, instr, at, row, output).await {
		Ok(next) => Ok(next),
		Err(failure) => write.fail(failure),
	}
}

/// Define the table of the record in flight, about to be resolved by
/// [`WriteInstr::Begin`], with the reservations a schema statement of the
/// program's block shares with the write ([`super::program::ImplicitTable`]),
/// where resolving it defines the table (`get_or_add_tb`): the statement's
/// form defines its table on demand, it reads at no version, the database is
/// not `STRICT`, and the table has no definition. The definition is the one
/// `get_or_add_tb` makes, with the reserved id, stamps and catalog version;
/// otherwise the resolution does what it does without them.
async fn define_shared_table(
	program: &Program,
	spec: &WriteSpec,
	state: &mut State,
) -> FlowResult<()> {
	use crate::catalog::{FromStored as _, StoredTableDefinition, TableDefinition, TableId};
	use crate::exec::operators::write::ctx::TableExistence;
	let State {
		write,
		reserved_taken,
		..
	} = state;
	let write = write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let Some(table) = write.row.as_ref().and_then(|row| row.table()).cloned() else {
		return Ok(());
	};
	let untaken = |at: u32| reserved_taken.get(at as usize).is_some_and(|taken| !*taken);
	let Some(shared) = program.implicit_tables.iter().find(|shared| {
		shared.table == table && [shared.id, shared.stamp, shared.version].into_iter().all(untaken)
	}) else {
		return Ok(());
	};
	let exec = write.scope.exec();
	if !matches!(spec.form.table_existence(), TableExistence::DefinedOnDemand)
		|| exec.version_stamp().is_some()
	{
		return Ok(());
	}
	let db_ctx = exec.database().map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;
	let (ns, db) = (db_ctx.ns_ctx.ns.name.as_str(), db_ctx.db.name.as_str());
	let txn = exec.txn();
	let Some((_, database)) = crate::exec::vm::descriptor::database(exec.ctx(), ns, db, None)
		.await
		.map_err(ControlFlow::Err)?
	else {
		return Ok(());
	};
	let (ns_id, db_id) = (database.namespace_id, database.database_id);
	if database.strict
		|| crate::exec::vm::descriptor::table_at(exec, &table, None)
			.await
			.map_err(ControlFlow::Err)?
			.definition
			.is_some()
	{
		return Ok(());
	}
	let reserved = |at: u32| program.reservations[at as usize];
	let id = TableId(reserved(shared.id) as i64 as u32);
	let mut stored = StoredTableDefinition::new(ns_id, db_id, id, table.clone());
	let stamp = uuid::Uuid::from_u128(reserved(shared.stamp));
	stored.cache_fields_ts = stamp;
	stored.cache_events_ts = stamp;
	stored.cache_tables_ts = stamp;
	stored.cache_indexes_ts = stamp;
	stored.cache_lives_ts = stamp;
	let definition = TableDefinition::from_stored(&stored).map_err(ControlFlow::Err)?;
	txn.put_tb(ns, db, &definition).await.map_err(ControlFlow::Err)?;
	let version = uuid::Uuid::from_u128(reserved(shared.version));
	txn.stamp_catalog_version(ns_id, db_id, &table, Some(version))
		.await
		.map_err(ControlFlow::Err)?;
	for at in [shared.id, shared.stamp, shared.version] {
		reserved_taken[at as usize] = true;
	}
	// The id was held by the schema statement's speculation; the definition
	// confirms it, as the statement's own draw of it does.
	if let Ok(sequences) = exec.ctx().try_get_sequences() {
		sequences.confirm_catalog_id(
			&crate::kvs::sequences::CatalogSequence::Tables(ns_id, db_id),
			id.0,
		);
	}
	Ok(())
}

/// Apply the stages of run `site` to the record in flight, as the unit lowered
/// for its table applies them, and answer whether they kept it.
///
/// The unit is the one the statement's [`super::program::StageTables`] holds
/// for the table the record is written to, or, for a write that may reach any
/// table, the bundle of the table's stages the run loads by the table's name
/// ([`super::fault::serve_stage_bundle`]); a table the statement cannot reach
/// is an error. It runs over the statement's write
/// state, which it hands back whatever it answers.
async fn run_stages(program: &Program, site: u16, state: &mut State) -> FlowResult<bool> {
	let spec = spec_of(program, state)?;
	let tables =
		spec.tables.as_ref().ok_or_else(|| internal("a run of stages with no table dispatch"))?;
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (_, ctx) = write.in_flight()?;
	let exec = ctx.exec().clone();
	let outside = || internal(&format!("a table outside the dispatch set: {}", ctx.table.name));
	let (stages, units) = match &tables.by_name {
		Some(named) => {
			let table = ctx.table.name.clone();
			// A bundle is taken again for the catalog version the record's
			// write context read, so a table whose catalog changed while the
			// run went on runs the stages of the definitions it holds now.
			let version = ctx.resolved_at.0;
			let held = write
				.bundles
				.iter()
				.find(|(name, at, _)| *name == table && *at == version)
				.map(|(_, _, b)| Arc::clone(b));
			let bundle = match held {
				Some(bundle) => bundle,
				None => {
					let bundle = super::fault::serve_stage_bundle(named, &table, &exec).await?;
					let write = state
						.write
						.as_deref_mut()
						.ok_or_else(|| internal("a write body with no write state"))?;
					write.bundles.retain(|(name, _, _)| *name != table);
					write.bundles.push((table, version, Arc::clone(&bundle)));
					bundle
				}
			};
			// The bundle's entry is the program of the stages, and its arena
			// holds what they run.
			let entry = bundle
				.unit
				.as_ref()
				.and_then(|head| head.entry)
				.ok_or_else(|| internal("a table's stages that head no bundle"))?;
			admit(Arc::clone(&bundle.units), entry, state, &exec)?
		}
		None => {
			let unit = tables.unit(&ctx.table.name).ok_or_else(outside)?;
			write_unit(program, unit, state, &exec)?
		}
	};
	let pipeline = stages
		.pipelines
		.get(site as usize)
		.ok_or_else(|| internal("a run of stages the stage program does not hold"))?;
	let (mut nested, group) = enter_unit(&stages, units, state, &exec)?;
	let ran = run_value_row(&stages, pipeline, Value::None, &mut nested, &exec).await;
	leave_unit(&mut nested, group, state);
	ran?;
	match nested.take_output().pop() {
		Some(Value::Bool(kept)) => Ok(kept),
		_ => Err(internal("a run of stages that answered nothing")),
	}
}

/// Unit `unit` of the table `program`'s ids index, and that table: a unit a
/// record's write runs, its frame checked against the one the write runs in
/// once per statement.
pub(super) fn write_unit(
	program: &Program,
	unit: UnitId,
	state: &mut State,
	exec: &ExecutionContext,
) -> FlowResult<(Arc<Program>, super::UnitTable)> {
	let units = super::host::unit_table(program, state, exec)?;
	admit(units, unit, state, exec)
}

/// Unit `unit` of `units`, and `units`: a unit a record's write runs, its
/// frame checked against the one the write runs in once per statement.
pub(super) fn admit(
	units: super::UnitTable,
	unit: UnitId,
	state: &mut State,
	exec: &ExecutionContext,
) -> FlowResult<(Arc<Program>, super::UnitTable)> {
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let key = (arena_of(&units), unit);
	let held = if write.admitted.contains(&key) {
		Arc::clone(
			units.get(unit.0 as usize).ok_or_else(|| internal("a unit the table does not hold"))?,
		)
	} else {
		let held = Arc::clone(super::host::admitted_unit(&units, unit, exec)?);
		write.admitted.push(key);
		held
	};
	Ok((held, units))
}

/// The identity of the arena `units` is, which a unit id is read in: its
/// address, which the programs holding it keep from naming another arena
/// for as long as a write state that names it is kept with them.
pub(super) fn arena_of(units: &super::UnitTable) -> usize {
	Arc::as_ptr(units).cast::<()>() as usize
}

/// A state of `unit`'s own, over the unit table `units`, that holds the write
/// of `state` while the unit runs, and the block statement the write was
/// running for; [`leave_unit`] gives both back.
pub(super) fn enter_unit(
	unit: &Program,
	units: super::UnitTable,
	state: &mut State,
	exec: &ExecutionContext,
) -> FlowResult<(PooledState, Option<u16>)> {
	let mut nested = State::new(unit, Vec::new(), exec.function_registry())?;
	nested.frames = unit.permissions.iter().map(|spec| spec.context(exec)).collect();
	nested.permissions_bypassed = state.permissions_bypassed;
	nested.rowless = true;
	// The ids the unit names are the statement's.
	nested.units = Some(units);
	// The unit's write is its own, whichever statement's this is.
	let mut held = state.write.take();
	let mut group = None;
	if let Some(write) = held.as_deref_mut() {
		write.nested = true;
		group = write.group.take();
	}
	nested.write = held;
	Ok((nested, group))
}

/// Give the write [`enter_unit`] took back to `state`.
pub(super) fn leave_unit(nested: &mut State, group: Option<u16>, state: &mut State) {
	let mut held = nested.write.take();
	if let Some(write) = held.as_deref_mut() {
		write.nested = false;
		write.group = group;
	}
	state.write = held;
}

/// The slot of `image` among [`WriteState::shown`].
fn slot(image: RecordImage) -> usize {
	match image {
		RecordImage::Initial => 0,
		RecordImage::Current => 1,
	}
}

/// Take [`WriteInstr::ShowImage`] or [`WriteInstr::SetOutput`] for the record
/// in flight, running the host pipelines they name over its images.
async fn show(program: &Program, instr: &WriteInstr, state: &mut State) -> FlowResult<()> {
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (row, ctx) = write.in_flight()?;
	let exec = ctx.exec().clone();
	match instr {
		WriteInstr::ShowImage {
			image,
			view,
			checked,
		} => {
			let source = match image {
				RecordImage::Current => &row.current,
				RecordImage::Initial => &row.initial,
			};
			// No record to show, or an unchecked caller's post-image that
			// already carries its `COMPUTED` values: deriving them again would
			// answer a non-deterministic body differently from the one the
			// record's observers were given.
			let as_is = !matches!(source, Value::Object(_))
				|| row.id.is_none()
				|| (!*checked && *image == RecordImage::Current && row.fields_computed);
			let source = source.clone();
			let shown = match view {
				Some(view) if !as_is => {
					run_image(program, *view, source, None, state, &exec).await?
				}
				_ => source,
			};
			let write = state
				.write
				.as_deref_mut()
				.ok_or_else(|| internal("a write body with no write state"))?;
			if *checked && !as_is {
				let row = write.row()?;
				match image {
					RecordImage::Current => row.current_reduced = Some(shown.clone()),
					RecordImage::Initial => row.initial_reduced = Some(shown.clone()),
				}
			}
			write.shown[slot(*image)] = Some(shown);
		}
		WriteInstr::SetOutput {
			output,
		} => {
			let shown = |write: &WriteState, image: RecordImage| {
				write.shown[slot(image)]
					.clone()
					.ok_or_else(|| internal("an image output before the image was shown"))
			};
			let value = match output {
				WriteOutput::None => None,
				WriteOutput::Null => Some(Value::Null),
				WriteOutput::Shown(image) => Some(shown(write, *image)?),
				WriteOutput::Diff => {
					let before = shown(write, RecordImage::Initial)?;
					let after = shown(write, RecordImage::Current)?;
					Some(Operation::operations_to_value(before.diff(&after)))
				}
				WriteOutput::Projection(pipeline) => {
					let before = shown(write, RecordImage::Initial)?;
					let after = shown(write, RecordImage::Current)?;
					let frame = exec.with_params([("before", before), ("after", after.clone())]);
					Some(run_image(program, *pipeline, after, Some(frame), state, &exec).await?)
				}
			};
			let write = state
				.write
				.as_deref_mut()
				.ok_or_else(|| internal("a write body with no write state"))?;
			write.row()?.output = value;
		}
		_ => return Err(internal("an image step at an instruction that is not one")),
	}
	Ok(())
}

/// Run host pipeline [`Program::host_code`]`[pipeline]` over `image` as its
/// row, in `frame` when set, and answer what it emits. The statement's output
/// and the record's frame are set aside while it runs.
pub(super) async fn run_image(
	program: &Program,
	pipeline: u16,
	image: Value,
	frame: Option<ExecutionContext>,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	let outer = std::mem::replace(&mut state.frame, frame);
	let ran = run_image_in_frame(program, pipeline, image, state, ctx).await;
	state.frame = outer;
	ran
}

/// [`run_image`] in the frame `state` holds already.
pub(super) async fn run_image_in_frame(
	program: &Program,
	pipeline: u16,
	image: Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	run_host_image(program, pipeline, image, false, state, ctx).await
}

/// Host pipeline `pipeline`'s answer over `image`, in no frame of its own and,
/// when `rowless`, over no document at all rather than over `image`.
pub(super) async fn run_place(
	program: &Program,
	pipeline: u16,
	image: Value,
	rowless: bool,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	let outer = state.frame.take();
	let ran = run_host_image(program, pipeline, image, rowless, state, ctx).await;
	state.frame = outer;
	ran
}

async fn run_host_image(
	program: &Program,
	pipeline: u16,
	image: Value,
	rowless: bool,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	let pipeline = &program.host_code[pipeline as usize];
	let output = std::mem::take(&mut state.output);
	let outer = std::mem::replace(&mut state.rowless, rowless);
	let ran = run_value_row(program, pipeline, image, state, ctx).await;
	state.rowless = outer;
	let answered = std::mem::replace(&mut state.output, output).pop();
	ran?;
	answered.ok_or_else(|| internal("an image pipeline that answered nothing"))
}

/// Run a write program: its one pipeline over the statement's subjects, then
/// `NONE` for a statement under `ONLY` that returned no row, whatever the
/// reason.
///
/// A stream of its own rather than the read host's, so a statement nested in
/// a record's write carries only what writing needs on the stack.
pub(super) fn stream(program: Arc<Program>, ctx: ExecutionContext) -> ValueBatchStream {
	Box::pin(common::future::stream::try_async_stream(async move |mut stream: Yielder<_>| {
		let mut yielder = Out::Stream(&mut stream);
		let spec = program.write.as_ref().ok_or_else(|| internal("a write stream for a read"))?;
		// What a write reaches runs in the write's frame.
		let ctx = ctx.in_write();
		let flush_at = ctx.root().ctx.config.exec.scan_batch_size.max(1);
		let version = resolve_version_stamp(&ctx, None).await?;
		// A write pages nothing of its own; the paging it holds is its nested
		// statements', evaluated when each is reached.
		let paging = program
			.limits
			.iter()
			.map(|_| super::interp::Paging {
				skip: 0,
				remaining: None,
			})
			.collect();
		let mut state = State::new(&program, paging, ctx.function_registry())?;
		if !program.groups.is_empty() {
			state.program = Some(Arc::clone(&program));
		}
		state.permissions_bypassed = ctx.root().skip_fetch_perms;
		state.frames = program.permissions.iter().map(|spec| spec.context(&ctx)).collect();
		state.write = Some(Box::new(WriteState::new(spec, &ctx)?));
		run_write(&program, None, version, &mut state, &ctx, &mut yielder, flush_at).await?;
		let rest = state.take_output();
		if !rest.is_empty() {
			stream.emit(ValueBatch::new(rest)).await;
		}
		Ok(())
	}))
}

/// Run the write pipelines of `program` that `group` owns — the program's own
/// for `None` — over `state`, whose write state is that write's, handing
/// their rows to `yielder`; then judge `ONLY`, leaving `NONE` in the output
/// of a statement under `ONLY` that returned no row.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_write(
	program: &Arc<Program>,
	group: Option<u16>,
	version: Option<u64>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<()> {
	super::host::evaluate_knn_queries(program, group, state, ctx).await?;
	for pipeline in program.pipelines.iter().filter(|pipeline| pipeline.group == group) {
		match &pipeline.source {
			Source::WriteTargets {
				targets,
				deferable,
				guarantees,
			} => {
				super::host::out_of_line(|| {
					run_targets(
						program,
						pipeline,
						targets,
						(*deferable, *guarantees),
						state,
						ctx,
						yielder,
						flush_at,
					)
				})
				.await?;
			}
			Source::WriteInserts {
				..
			} => {
				super::host::out_of_line(|| {
					run_inserts(program, pipeline, state, ctx, yielder, flush_at)
				})
				.await?;
			}
			Source::WriteEdges {
				from,
				through,
				to,
			} => {
				super::host::out_of_line(|| {
					run_edges(program, pipeline, [from, through, to], state, ctx, yielder, flush_at)
				})
				.await?;
			}
			// A write's subjects are read whatever its paging.
			Source::Values {
				..
			}
			| Source::Nested {
				..
			}
			| Source::Probe {
				..
			} => {
				super::host::out_of_line(|| {
					super::nested::run_source(
						program,
						pipeline,
						&pipeline.source,
						false,
						state,
						ctx,
						yielder,
						flush_at,
					)
				})
				.await?;
			}
			Source::Sorted {
				..
			}
			| Source::Groups {
				..
			}
			| Source::Count {
				..
			} => return Err(internal("a write over rows a read holds")),
			source => {
				super::host::out_of_line(|| {
					run_storage_source(
						program, pipeline, source, version, state, ctx, yielder, flush_at,
					)
				})
				.await?;
			}
		}
	}
	let write = write_state(state)?;
	write.judge_only()?;
	let none = write.only && write.returned == 0;
	if none {
		state.output.push(Value::None);
	}
	Ok(())
}

/// Run a write body over the subjects each target names: a record to create,
/// a record to read, or the records of a table or range, in written order.
/// An `UPSERT` over a table in which it matched nothing then writes the one
/// record it guarantees. Where `guarantees`, a table's records are read by
/// another pipeline of the write, run before this one, and only that record
/// is written here.
#[allow(clippy::too_many_arguments)]
async fn run_targets(
	program: &Program,
	pipeline: &Pipeline,
	targets: &[super::program::Target],
	(deferable, guarantees): (bool, bool),
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<()> {
	let form = spec_of(program, state)?.form;
	write_state(state)?.per_record = true;
	let mut guaranteed = None;
	for target in targets {
		check_cancelled(ctx)?;
		let mut subjects = match target {
			super::program::Target::Scalar(scalar) => {
				let value =
					super::host::host_scalar(program, scalar, ctx, ctx.value("this")).await?;
				let mut named = Vec::new();
				target_subjects(value, form, deferable, &mut named)
					.map_err(|err| refused_by(err, form))?;
				TargetSubjects::Named(named.into_iter())
			}
			super::program::Target::Mock(mock) => TargetSubjects::Mock(mock.clone().into_iter()),
		};
		while let Some(subject) =
			subjects.next(form, deferable).map_err(|err| refused_by(err, form))?
		{
			check_cancelled(ctx)?;
			let mut rows = match subject {
				Subject::Row(row) => Rows::One(Some(row)),
				Subject::Read(id) => Rows::One(Some(Box::new(read_subject(ctx, id).await?))),
				Subject::Scan(table, range) => {
					// The guaranteed record's table is the last bare table the
					// caller may create a record on: one the gate denies that
					// to writes no record, and evaluates no data clause for it.
					if matches!(form, WriteForm::Upsert)
						&& range.is_none() && write_state(state)?
						.gate
						.admits_creating(&table, ctx)
						.await?
					{
						guaranteed = Some(table.clone());
					}
					if guarantees {
						continue;
					}
					Rows::Scan(scan_subjects(ctx, &table, range.as_deref())?)
				}
			};
			while let Some(row) = rows.next().await? {
				check_cancelled(ctx)?;
				write_state(state)?.row = Some(row);
				run_value_row(program, pipeline, Value::None, state, ctx).await?;
				flush(state, yielder, flush_at).await?;
			}
		}
	}
	// What the statement matched is counted, not what it returned, and an
	// import writes no record its export never held. The table is one the
	// gate admitted, and its answer is memoised.
	if write_state(state)?.matched == 0
		&& !ctx.options().is_some_and(|options| options.import)
		&& let Some(table) = guaranteed
	{
		write_state(state)?.row = Some(WriteRow::for_target(Value::Table(table))?);
		run_value_row(program, pipeline, Value::None, state, ctx).await?;
		flush(state, yielder, flush_at).await?;
	}
	Ok(())
}

/// Run a write body over an `INSERT`'s rows. Every row is evaluated before
/// the first is written, so none observes a record the statement wrote.
async fn run_inserts(
	program: &Program,
	pipeline: &Pipeline,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<()> {
	let Source::WriteInserts {
		into,
		rows,
		relation,
	} = &pipeline.source
	else {
		return Err(internal("an INSERT run over a source that is no INSERT's"));
	};
	let relation = *relation;
	write_state(state)?.per_record = true;
	let into = match into {
		Some(into) => Some(insert_into(program, into, state, ctx).await?),
		None => None,
	};
	for value in insert_values(program, rows, state, ctx).await? {
		check_cancelled(ctx)?;
		write_state(state)?.row = Some(insert_row(value, &into, relation)?);
		run_value_row(program, pipeline, Value::None, state, ctx).await?;
		flush(state, yielder, flush_at).await?;
	}
	Ok(())
}

/// The value of `scalar` over the statement's `$this`, evaluated on `state`.
async fn insert_scalar(
	program: &Program,
	scalar: &super::program::HostScalar,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	let this = ctx.value("this");
	match scalar {
		super::program::HostScalar::Code(pipeline) => {
			let image = this.cloned().unwrap_or(Value::None);
			run_place(program, *pipeline, image, this.is_none(), state, ctx).await
		}
		scalar => super::host::host_scalar(program, scalar, ctx, this).await,
	}
}

/// The table an `INSERT`'s `INTO` clause names: a table name or a string
/// naming one. Anything else refuses the statement, naming the expression
/// rather than what it produced.
async fn insert_into(
	program: &Program,
	into: &super::program::InsertInto,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<crate::val::TableName> {
	match insert_scalar(program, &into.scalar, state, ctx).await? {
		Value::Table(into) => Ok(into),
		Value::String(into) => Ok(crate::val::TableName::new(into)),
		_ => Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InsertStatement {
			value: into.sql.clone(),
		}))),
	}
}

/// The values one `INSERT` writes, evaluated: every row is produced before
/// any is written, so a row expression never observes a record this
/// statement has already written.
async fn insert_values(
	program: &Program,
	rows: &InsertRowsSpec,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Vec<Value>> {
	match rows {
		InsertRowsSpec::Tuples(tuples) => {
			let mut out = Vec::with_capacity(tuples.len());
			for tuple in tuples {
				check_cancelled(ctx)?;
				// A fresh object per row, written through the tuple's places in
				// the order the statement wrote them.
				let mut row = Value::empty_object();
				for field in tuple {
					let value = insert_scalar(program, &field.value, state, ctx).await?;
					let mut places = data::PlaceRun {
						program,
						state,
						exec: ctx,
					};
					crate::exec::operators::write::apply::set_with(
						&mut row,
						&field.place,
						value,
						&mut places,
					)
					.await?;
				}
				out.push(row);
			}
			Ok(out)
		}
		InsertRowsSpec::Value(scalar) => match insert_scalar(program, scalar, state, ctx).await? {
			Value::Array(array) => Ok(array.into_iter().collect()),
			object @ Value::Object(_) => Ok(vec![object]),
			other => Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InsertStatement {
				value: other.to_sql(),
			}))),
		},
	}
}

/// Run a write body over a `RELATE`'s edges: the cross product of the records
/// `from` and `to` name, each evaluated once, through `through`, evaluated per
/// edge so a `through` naming a fresh id per edge gets one.
async fn run_edges(
	program: &Program,
	pipeline: &Pipeline,
	[from, through, to]: [&super::program::HostScalar; 3],
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<()> {
	write_state(state)?.per_record = true;
	let this = ctx.value("this");
	let endpoint = |scalar| super::host::host_scalar(program, scalar, ctx, this);
	let froms = relate_endpoints(endpoint(from).await?, RelateSide::From)?;
	let tos = relate_endpoints(endpoint(to).await?, RelateSide::To)?;
	for f in &froms {
		for t in &tos {
			check_cancelled(ctx)?;
			let at = endpoint(through).await?;
			write_state(state)?.row = Some(relate_row(at, f.clone(), t.clone())?);
			run_value_row(program, pipeline, Value::None, state, ctx).await?;
			flush(state, yielder, flush_at).await?;
		}
	}
	Ok(())
}

/// The write `state` is running: the program's own, or that of the block
/// statement compiled into the program its write state names.
fn spec_of<'p>(program: &'p Program, state: &State) -> FlowResult<&'p WriteSpec> {
	let group = state.write.as_deref().and_then(|write| write.group);
	match group {
		None => program.write.as_ref(),
		Some(group) => program.groups.get(group as usize).and_then(|spec| spec.write.as_ref()),
	}
	.ok_or_else(|| internal("a write body in a read"))
}

fn write_state(state: &mut State) -> FlowResult<&mut WriteState> {
	state.write.as_deref_mut().ok_or_else(|| internal("a write source with no write state"))
}
