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

use anyhow::Context as _;
use common::future::stream::Yielder;
use surrealdb_types::ToSql as _;

use super::host::{Out, flush, out_of_line, run_storage_source, run_value_row};
use super::interp::{PooledState, State};
use super::ir_exec::{PendingCandidate, PrefixStop};
use super::morsel::Rows as MorselRows;
use super::program::{
	InsertRowsSpec, Instr, Label, Pipeline, Program, Source, UnitId, WriteInstr, WriteOutput,
	WriteSpec,
};
use crate::catalog::providers::TableProvider;
use crate::exec::operators::check_cancelled;
use crate::exec::operators::scan::common::resolve_version_stamp;
use crate::exec::operators::write::ctx::{TableExistence, WriteCtx};
use crate::exec::operators::write::plan::{
	CreateFailure, WriteForm, classify_create_failure, duplicate_record,
};
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

pub(super) use cascade::{MaterializeStep, ReferenceInstr, RepairStrategy, TableRepairs};
pub(super) use data::{DataInstr, KeyPart};
pub(super) use gate::GateInstr;
pub(super) use observe::{
	Admission, ClauseDepth, EventClauses, LoweredEvents, LoweredSubscription, ObserveInstr,
	SubscriptionFieldsLowered,
};
pub(crate) use observe::{notify_at, notify_unit};
pub(super) use schema::{FieldPermission, FieldPlan, FieldWrite, SchemaInstr, SchemaPlan};
#[cfg(not(target_family = "wasm"))]
pub(super) use storage::encode_ahead;
pub(super) use storage::{
	IndexPlan, LoweredIndexes, NativeIndexKind, StorageFact, StorageInstr, note_created,
};
pub(super) use view::{
	LoweredView, LoweredViews, Recalc, ViewClause, ViewRows, action_pipeline, view_row_exec,
	view_row_key,
};

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
	/// The conflict the retry raises if its record is gone after the rollback.
	retry_cause: Option<anyhow::Error>,
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
	/// that may reach any table, each with the definitions it was loaded for,
	/// so a table's bundle is loaded once per run of the statement while the
	/// definitions its write contexts read stand ([`WriteState::bundle_for`]).
	bundles: Vec<HeldBundle>,
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
	/// A watch on each table the statement has read subjects from, opened as
	/// its first read of the table began and kept until the statement ends
	/// ([`Self::read_table`]).
	reading: Vec<crate::kvs::creations::CreationWatch>,
	/// Whether the subjects being written are read from a table, rather than
	/// named, so that a record the statement created is passed over
	/// ([`Self::created_while_read`]).
	scanning: bool,
	/// Whether the row being run is a record a source fetched by the id the
	/// statement names, which is never passed over
	/// ([`Self::row_named`]).
	named: bool,
	/// The view tables whose rows are in flight through their tables' orders
	/// beneath this write, outermost first, each read by the next: empty for a
	/// statement's write, and ending with its own table for the write of a
	/// view row. A view of the record's table found among them reads itself,
	/// and maintaining it fails the write ([`view::refuse_reentry`]); each of
	/// them spends of the computation-depth budget ([`view::view_row_exec`]).
	/// Shared, so the maintenance of each record's views holds it without
	/// copying it.
	maintaining: Arc<[surrealdb_strand::TableName]>,
	/// Whether the body runs as the compute half of a write that fans out: it
	/// ends at the first marker it reaches, with the record in flight left for
	/// the candidate it hands the coordinator.
	prefix: bool,
	/// Where the compute half of the current record stopped, set by the marker
	/// that ended it and taken once by [`Self::take_prefix_stop`].
	prefix_stop: Option<PrefixStop>,
	/// The candidate the coordinator holds for the record its next run of the
	/// body resumes, taken by the host arm of the body's `WriteResume`.
	pending: Option<PendingCandidate>,
	/// The record-type marker the statement's task read for the record the compute half runs
	/// next, taken once by the body's first instruction ([`Self::hold_stored_metadata`]).
	stored_metadata: Option<Option<crate::catalog::Metadata>>,
	/// The savepoints the body has opened and not yet closed.
	savepoints: OpenSavepoints,
}

/// The savepoints a write body has opened ([`WriteInstr::SavepointPush`]) and not yet closed, and
/// the transaction they were opened on.
///
/// A savepoint is counted once it is open, until its release or rollback returns. Dropped with
/// one counted — the statement's future dropped while an attempt was open, or while its release
/// or rollback was in progress — it refuses the transaction's commit
/// ([`crate::kvs::Transaction::poison_abandoned_save_point`]): the savepoint stays on the stack
/// for some other scope to close in place of its own, and the attempt may have stored its record
/// before its index entries or observers ran. A transaction its client owns survives the dropped
/// statement and could otherwise still commit.
///
/// A release or rollback that returns, with an error or without, is no longer counted: the
/// transaction has then decided for itself whether it may commit.
#[derive(Default)]
struct OpenSavepoints {
	/// The transaction of the savepoints counted, once one has been opened.
	txn: Option<Arc<crate::kvs::Transaction>>,
	/// How many are open.
	open: u32,
}

impl OpenSavepoints {
	/// Opens a savepoint on `txn`. One that fails to open is not counted: the transaction awaits
	/// only before the storage savepoint opens, and a failure of the storage refuses its commit.
	async fn push(&mut self, txn: Arc<crate::kvs::Transaction>) -> Result<(), ControlFlow> {
		txn.new_save_point().await.map_err(ControlFlow::Err)?;
		self.open += 1;
		self.txn = Some(txn);
		Ok(())
	}

	/// Closes the last savepoint on `txn`: rolled back when `rollback`, released otherwise.
	async fn close(
		&mut self,
		txn: &crate::kvs::Transaction,
		rollback: bool,
	) -> Result<(), ControlFlow> {
		let closed = if rollback {
			txn.rollback_to_save_point().await
		} else {
			txn.release_last_save_point().await
		};
		self.open = self.open.saturating_sub(1);
		closed.map_err(ControlFlow::Err)
	}
}

impl Drop for OpenSavepoints {
	fn drop(&mut self) {
		if self.open > 0
			&& let Some(txn) = &self.txn
		{
			txn.poison_abandoned_save_point();
		}
	}
}

/// What a write's state holds once a record has ended, for a test that compares the state of a
/// write whose records resumed at a cut with that of a write that ran each record whole.
///
/// The compute half of a record changes the state's fields only through the record in flight, its
/// write context and what a stage records for the record (the shown images, the field schema's
/// state, the reference keys, the open handlers and the failure, retry and carry a handler holds),
/// and each of them is held by the candidate or cleared with the record: the counters a record
/// moves (`matched`, `returned`) are moved by the instruction that ends it.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WriteAudit {
	pub(crate) only: bool,
	pub(crate) per_record: bool,
	pub(crate) returned: usize,
	pub(crate) matched: usize,
	pub(crate) shown: [bool; 2],
	pub(crate) schema: bool,
	pub(crate) references: bool,
	pub(crate) handlers: usize,
	pub(crate) failed: bool,
	pub(crate) retry: bool,
	pub(crate) carrying: bool,
	pub(crate) nested: bool,
	pub(crate) group: Option<u16>,
	pub(crate) in_flight: bool,
}

/// The bundle of a table's stages a run holds ([`WriteState::bundle_for`]),
/// with the catalog version and the definitions of the write context it was
/// loaded for.
struct HeldBundle {
	table: surrealdb_strand::TableName,
	version: Option<uuid::Uuid>,
	fields: Arc<[crate::catalog::FieldDefinition]>,
	indexes: Arc<[crate::catalog::IndexDefinition]>,
	events: Arc<[crate::catalog::EventDefinition]>,
	views: Arc<[crate::catalog::TableDefinition]>,
	bundle: Arc<Program>,
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
			retry_cause: None,
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
			reading: Vec::new(),
			scanning: false,
			named: false,
			maintaining: Arc::default(),
			prefix: false,
			prefix_stop: None,
			pending: None,
			stored_metadata: None,
			savepoints: OpenSavepoints::default(),
		})
	}

	/// A state for the compute half of the records of a write that fans out: it shares the
	/// write's scope, so the contexts the statement resolved are the worker's too, and ends each
	/// record at the first marker of its body ([`Self::take_prefix_stop`]).
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(super) fn for_prefix(&self, spec: &WriteSpec, ctx: &ExecutionContext) -> FlowResult<Self> {
		let mut state = Self::new(spec, ctx)?;
		state.scope = Arc::clone(&self.scope);
		state.group = self.group;
		state.prefix = true;
		Ok(state)
	}

	/// Whether the body runs as the compute half of a write that fans out.
	pub(super) fn in_prefix_mode(&self) -> bool {
		self.prefix
	}

	/// Records where the compute half of the current record stopped.
	pub(super) fn stop_prefix(&mut self, stop: PrefixStop) {
		self.prefix_stop = Some(stop);
	}

	/// Where the compute half of the current record stopped, once a marker ended it.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(super) fn take_prefix_stop(&mut self) -> Option<PrefixStop> {
		self.prefix_stop.take()
	}

	/// The record in flight and its write context, left by a compute half that stopped; the
	/// state holds neither afterwards.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(super) fn take_in_flight(&mut self) -> Option<(WriteRow, Option<Arc<WriteCtx>>)> {
		let ctx = self.ctx.take();
		self.clear().map(|row| (row, ctx))
	}

	/// Holds `candidate` for the next run of the body, which resumes the record it carries.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(super) fn hold_pending(&mut self, candidate: PendingCandidate) {
		self.pending = Some(candidate);
	}

	/// Puts the record that `target` names in flight, as a write over targets begins each of its
	/// records.
	pub(super) fn begin_target(&mut self, target: Value) -> Result<(), ControlFlow> {
		self.row = Some(WriteRow::for_target(target)?);
		Ok(())
	}

	/// Holds the record-type marker stored with the record the compute half runs next, which the
	/// statement's task read ahead: the half reads no stored data.
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub(super) fn hold_stored_metadata(&mut self, stored: Option<crate::catalog::Metadata>) {
		self.stored_metadata = Some(stored);
	}

	/// What the state holds, for a test.
	#[cfg(test)]
	fn audit(&self) -> WriteAudit {
		WriteAudit {
			only: self.only,
			per_record: self.per_record,
			returned: self.returned,
			matched: self.matched,
			shown: [self.shown[0].is_some(), self.shown[1].is_some()],
			schema: self.schema.is_some(),
			references: self.references.is_some(),
			handlers: self.handlers.len(),
			failed: self.failure.is_some(),
			retry: self.retry.is_some(),
			carrying: self.carrying,
			nested: self.nested,
			group: self.group,
			in_flight: self.row.is_some() || self.ctx.is_some(),
		}
	}

	/// Whether a candidate waits for the next run of the body.
	pub(super) fn has_pending(&self) -> bool {
		self.pending.is_some()
	}

	/// The candidate that waits for the next run of the body.
	pub(super) fn take_pending(&mut self) -> Option<PendingCandidate> {
		self.pending.take()
	}

	/// Installs the record a compute half left in flight, as the stage that follows its cut finds
	/// it.
	pub(super) fn resume_candidate(&mut self, row: WriteRow, ctx: Option<Arc<WriteCtx>>) {
		self.row = Some(row);
		self.ctx = ctx;
	}

	/// The write of a row a view's maintenance computed on its view table,
	/// in flight from the start in `ctx`, the view table's view-maintenance
	/// context: taken through the view table's order by a program of its own,
	/// over `scope`, the writing statement's. Nothing gates it, and a failure
	/// leaves its program for the maintenance that ran it. `maintaining` is the
	/// view tables whose rows are in flight beneath it, ending with the row's
	/// own.
	pub(super) fn view_row(
		scope: Arc<WriteScope>,
		row: WriteRow,
		ctx: Arc<WriteCtx>,
		maintaining: Arc<[surrealdb_strand::TableName]>,
	) -> Self {
		Self {
			scope,
			gate: WriteGate::unchecked(WriteForm::View),
			row: Some(row),
			ctx: Some(ctx),
			handlers: Vec::new(),
			failure: None,
			retry: None,
			carried: None,
			retry_cause: None,
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
			reading: Vec::new(),
			scanning: false,
			named: false,
			maintaining,
			prefix: false,
			prefix_stop: None,
			pending: None,
			stored_metadata: None,
			savepoints: OpenSavepoints::default(),
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
		self.retry_cause = None;
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
		self.prefix = false;
		self.prefix_stop = None;
		self.pending = None;
		self.stored_metadata = None;
		Ok(())
	}

	fn row(&mut self) -> Result<&mut WriteRow, ControlFlow> {
		self.row.as_mut().ok_or_else(|| internal("a write step with no record in flight"))
	}

	/// The id of the record in flight, once it has one: the record the write's stages and its
	/// `RETURN` clause are run for, whatever image they are shown.
	pub(super) fn record_in_flight(&self) -> Option<&RecordId> {
		self.row.as_ref()?.id.as_ref()
	}

	/// The bundle of the stages `named`, a write that may reach any table,
	/// applies to the records of `ctx`'s table, for the definitions `ctx`
	/// read: held from an earlier record of the run whose context read the
	/// same catalog version, or the same fields, indexes, events and views,
	/// which are every definition a bundle's stages are lowered from, or served
	/// ([`super::fault::serve_stage_bundle`]) and held. A table whose context
	/// was resolved again for definitions that changed while the run went on
	/// takes the bundle of the definitions it holds now.
	async fn bundle_for(
		&mut self,
		named: &Arc<super::units::StageSetUnit>,
		ctx: &WriteCtx,
	) -> FlowResult<Arc<Program>> {
		let table = &ctx.table.name;
		let version = ctx.resolved_at;
		if let Some(held) = self.bundles.iter().find(|held| {
			held.table == *table
				&& (held.version == version
					|| (Arc::ptr_eq(&held.fields, &ctx.fields)
						&& Arc::ptr_eq(&held.indexes, &ctx.indexes)
						&& Arc::ptr_eq(&held.events, &ctx.events)
						&& Arc::ptr_eq(&held.views, &ctx.views)))
		}) {
			return Ok(Arc::clone(&held.bundle));
		}
		let bundle = super::fault::serve_stage_bundle(named, table, ctx.exec()).await?;
		self.bundles.retain(|held| held.table != *table);
		self.bundles.push(HeldBundle {
			table: table.clone(),
			version,
			fields: Arc::clone(&ctx.fields),
			indexes: Arc::clone(&ctx.indexes),
			events: Arc::clone(&ctx.events),
			views: Arc::clone(&ctx.views),
			bundle: Arc::clone(&bundle),
		});
		Ok(bundle)
	}

	/// Whether the failure a handler resumed with is a `BREAK`, `CONTINUE` or `RETURN`, which
	/// leaves the record with what it wrote, rather than an error.
	fn holds_signal(&self) -> bool {
		self.failure.as_ref().is_some_and(|failure| !matches!(failure, ControlFlow::Err(_)))
	}

	fn in_flight(&mut self) -> Result<(&mut WriteRow, Arc<WriteCtx>), ControlFlow> {
		let ctx = self
			.ctx
			.clone()
			.ok_or_else(|| internal("a write step before the record's context was resolved"))?;
		Ok((self.row()?, ctx))
	}

	/// Whether the subjects that follow are read from a table (`true`) or
	/// named by the statement (`false`).
	fn reads_subjects(&mut self, scanning: bool) {
		self.scanning = scanning;
	}

	/// End the statement's reads, closing their watches, and release what
	/// the query holds for the notes they kept ([`super::host::flush`]
	/// charges them).
	fn end_reads(&mut self) {
		self.scanning = false;
		if self.reading.is_empty() {
			return;
		}
		self.reading.clear();
		let exec = self.scope.exec();
		if let (Some(memory), Some(txn)) = (exec.ctx().memory(), exec.try_txn()) {
			memory.shed_creations(Arc::as_ptr(&txn) as usize, txn.creation_log_bytes());
		}
	}

	/// Open a creation watch on `table` for the rest of the statement if none
	/// is open yet, so a record the statement creates in `table` from now on
	/// is passed over when a later target or pipeline of the statement scans
	/// `table` ([`Self::created_while_read`]).
	pub(super) fn watch_table(&mut self, table: &crate::val::TableName) -> FlowResult<()> {
		if self.reading.iter().any(|watch| watch.table() == table) {
			return Ok(());
		}
		let exec = self.scope.exec();
		let db = exec.database().context("a write requires a database context")?;
		let watch = exec.txn().watch_creations(db.ns_ctx.ns.namespace_id, db.db.database_id, table);
		self.reading.push(watch);
		Ok(())
	}

	/// Begin a read of the records of `table`, a batch at a time through the
	/// statement's own transaction while the statement writes between
	/// batches: a later batch would see the records the statement, or one
	/// nested in it, has created since. The statement's first read of the
	/// table opens a watch on it, kept for every later read of the table until
	/// the statement ends, so a record created in the table from then on is
	/// passed over ([`Self::created_while_read`]) and one created before is
	/// read. Called as each read begins, after the value naming its range is
	/// evaluated; a read while the subjects are named watches nothing.
	pub(super) fn read_table(&mut self, table: &crate::val::TableName) -> FlowResult<()> {
		if !self.scanning {
			return Ok(());
		}
		self.watch_table(table)
	}

	/// Whether the row a source is about to run, until it has run, is a
	/// record fetched by the id the statement names rather than one read from
	/// a table: such a record reaches the gate as named, whatever the
	/// statement created.
	/// Answers whether it was so before.
	pub(super) fn row_named(&mut self, named: bool) -> bool {
		std::mem::replace(&mut self.named, named)
	}

	/// Whether the record in flight, read from a table, is one the statement
	/// created once it had begun reading that table, which the statement
	/// passes over as though the read had not reached it.
	fn created_while_read(&self) -> FlowResult<bool> {
		if self.named {
			return Ok(false);
		}
		self.read_after_created(self.row.as_ref().and_then(|row| row.id.as_ref()))
	}

	/// Whether the record `id`, read from a table rather than named, is one
	/// the statement created once it had begun reading that table: the test
	/// of [`Self::created_while_read`] for a record other than the one in
	/// flight. The compute half of a write that fans out holds no watch, so
	/// the coordinator asks this of each row it hands a worker and of each
	/// candidate it applies. Every row a write set takes is read from a
	/// table: a record fetched by the id the statement names runs whole
	/// through the body on the statement's task, never through a write set.
	pub(super) fn read_after_created(&self, id: Option<&RecordId>) -> FlowResult<bool> {
		if !self.scanning {
			return Ok(false);
		}
		let Some(id) = id else {
			return Ok(false);
		};
		let Some(watch) = self.reading.iter().find(|watch| *watch.table() == id.table) else {
			return Ok(false);
		};
		// Nothing created since the record's table was watched is one load,
		// the whole cost to a read of a table the statement creates nothing
		// in: the record's key is encoded only to ask about a creation.
		if !watch.any_created() {
			return Ok(false);
		}
		let db = self.scope.exec().database().context("a write requires a database context")?;
		let key = storage::record_key(db.ns_ctx.ns.namespace_id, db.db.database_id, id)?;
		Ok(watch.created(&key))
	}

	/// Forget the record in flight.
	fn clear(&mut self) -> Option<WriteRow> {
		self.ctx = None;
		self.handlers.clear();
		self.failure = None;
		self.retry = None;
		self.carried = None;
		self.retry_cause = None;
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
				let record = match std::mem::take(row).try_into_record() {
					// A record id names the record, as a target does.
					Ok(id) if !id.key.is_range() => read_subject(self.scope.exec(), id).await?,
					Ok(id) => WriteRow::for_record(Value::RecordId(id))
						.map_err(|err| refused_by(err, spec.form))?,
					Err(subject) => {
						WriteRow::for_record(subject).map_err(|err| refused_by(err, spec.form))?
					}
				};
				self.row = Some(record);
				next
			}
			WriteInstr::Gate {
				skip,
			} => {
				if self.created_while_read()? {
					return Ok(*skip as usize);
				}
				let table = self.row()?.table().cloned();
				if self.gate.admits(table.as_ref(), self.scope.exec()).await? {
					next
				} else {
					*skip as usize
				}
			}
			WriteInstr::Begin => {
				// Polled per record: a statement whose stages are slow pulls no
				// fresh subject for as long as one record takes.
				let exec = self.scope.exec();
				check_cancelled(exec)?;
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
				if self.prefix {
					let stored = self.stored_metadata.take().ok_or_else(|| {
						internal("the compute half of a record began with no stored marker")
					})?;
					self.row()?.adopt_metadata(stored);
				} else {
					self.row()?.load_stored_metadata(&ctx).await?;
				}
				// The bundle of a write that may reach any table is taken with
				// the context, before anything of the record runs, so it is the
				// bundle of the definitions the context read.
				if let Some(named) = spec.tables.as_ref().and_then(|tables| tables.by_name.as_ref())
				{
					self.bundle_for(named, &ctx).await?;
				}
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
				// A body is cut only where it opens no savepoint, so the state that opens one is
				// the state that closes it.
				debug_assert!(!self.prefix, "a savepoint opened by the compute half of a write");
				let (_, ctx) = self.in_flight()?;
				self.savepoints.push(ctx.txn()).await?;
				next
			}
			WriteInstr::SavepointRelease => {
				let (_, ctx) = self.in_flight()?;
				self.savepoints.close(&ctx.txn(), false).await?;
				next
			}
			WriteInstr::SavepointRollback => {
				let (_, ctx) = self.in_flight()?;
				// A `BREAK`, `CONTINUE` or `RETURN` the attempt signalled leaves it with what it
				// wrote, as it leaves a write that runs with no savepoint; an error undoes it.
				let rollback = !self.holds_signal();
				self.savepoints.close(&ctx.txn(), rollback).await?;
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
				if !self.holds_signal() {
					self.row()?.mutated = false;
				}
				next
			}
			WriteInstr::SavepointEndSkipped => {
				let reached_store = self.row()?.reached_store;
				let (_, ctx) = self.in_flight()?;
				self.savepoints.close(&ctx.txn(), !reached_store).await?;
				next
			}
			WriteInstr::ReachedStore => {
				self.row()?.reached_store = true;
				next
			}
			WriteInstr::Rethrow => match self.failure.take() {
				Some(failure) => return Err(failure),
				None => return Err(internal("a handler resumed with no failure")),
			},
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
				let duplicate = duplicate_record(&err);
				match classify_create_failure(err, self.row()?, *policy) {
					CreateFailure::Skip => {
						if let Some(record) = duplicate {
							let (row, ctx) = self.in_flight()?;
							row.replay_id_draws(&ctx, &record).await?;
						}
						*skip as usize
					}
					CreateFailure::Raise(err) => return Err(ControlFlow::Err(err)),
					CreateFailure::RetryAsUpdate {
						record,
						carried,
						cause,
					} => {
						self.retry = Some(record);
						self.carried = carried;
						self.retry_cause = cause;
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
				let (stored, stamp) = crate::exec::migrate::stamped_record(
					&ctx.txn(),
					ctx.ns.namespace_id,
					ctx.db.database_id,
					&target.table,
					&target.key,
					ctx.exec().version_stamp(),
				)
				.await
				.map_err(ControlFlow::Err)?;
				// A conflict whose record the rollback took with it — one the attempt's
				// own evaluation wrote, as an event does — leaves nothing to update.
				// Updating nothing would drop the statement without a word.
				let gone = stored.data.is_nullish();
				if !gone {
					row.replay_id_draws(&ctx, &target).await?;
				}
				row.reset_for_update_retry(target, &stored, stamp);
				row.replay_stored(ctx.exec()).await?;
				// Nothing to update raises the conflict, or the failure the attempt
				// carried, without opening the update's savepoint.
				let cause = self.retry_cause.take();
				if gone && let Some(cause) = cause.or_else(|| self.carried.take()) {
					return Err(ControlFlow::Err(cause));
				}
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
			// An update that stored the record satisfied it, whatever withheld
			// its output afterwards: the failure the attempt carried is dropped.
			WriteInstr::RaiseCarried => match self.carried.take() {
				Some(err) if !self.row()?.reached_store => return Err(ControlFlow::Err(err)),
				_ => next,
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
				#[cfg(test)]
				super::note_write_ended(self.scope.exec(), self.audit());
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
		let failure = match failure {
			ControlFlow::Err(err) => match crate::kvs::storage_error(&err) {
				Some(crate::kvs::Error::DeferredKeyExists {
					key,
				}) => ControlFlow::Err(crate::dbs::executor::held_create_exists(key)),
				_ => ControlFlow::Err(err),
			},
			other @ (ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_)) => other,
		};
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
/// late-bound write's tables' catalog may change between its runs, so each run
/// resolves its contexts afresh and keeps none for the next; any other write's
/// run takes the contexts earlier runs left in `kept`, where it is run again
/// and again.
fn scope_for(
	spec: &WriteSpec,
	ctx: &ExecutionContext,
	kept: Option<&mut Option<Arc<crate::exec::operators::write::scope::KeptContexts>>>,
) -> Arc<WriteScope> {
	let late = spec.tables.as_ref().is_some_and(|tables| tables.revalidates);
	let existence = spec.form.table_existence();
	match kept {
		_ if late => WriteScope::new(ctx.clone(), existence),
		Some(kept) => WriteScope::kept(
			ctx.clone(),
			existence,
			Arc::clone(kept.get_or_insert_with(Default::default)),
		),
		None => WriteScope::new(ctx.clone(), existence),
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
			Some(Instr::Write(_)) => {
				at = step(program, pipeline, at, state, row).await?;
				// A write instruction is a unit of work. Once the fuel is spent
				// the body continues at the next, which its driver enters after
				// its yield point: every write instruction resumes the body with
				// fresh registers, so stopping between two holds nothing.
				if super::fuel::charge(1) {
					return Ok(at);
				}
			}
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
		WriteInstr::Record
		| WriteInstr::Gate {
			..
		}
		| WriteInstr::Begin
		| WriteInstr::LoadNamedSubject
		| WriteInstr::JumpIfNew {
			..
		}
		| WriteInstr::JumpIfNotIterationInitial {
			..
		}
		| WriteInstr::SavepointPush
		| WriteInstr::SavepointRelease
		| WriteInstr::SavepointEndSkipped
		| WriteInstr::ReachedStore
		| WriteInstr::SavepointRollback
		| WriteInstr::TryBegin {
			..
		}
		| WriteInstr::TryEnd
		| WriteInstr::ClearMutated
		| WriteInstr::RethrowSignal
		| WriteInstr::Rethrow
		| WriteInstr::ClassifyCreateFailure {
			..
		}
		| WriteInstr::JumpIfDone {
			..
		}
		| WriteInstr::ReloadForUpdate
		| WriteInstr::CarryBegin
		| WriteInstr::CarryEnd
		| WriteInstr::RaiseCarried
		| WriteInstr::End {
			..
		}
		| WriteInstr::JumpIfImport {
			..
		}
		| WriteInstr::JumpIfNoPostImage {
			..
		}
		| WriteInstr::ClearCurrentReduced
		| WriteInstr::ShowImage {
			..
		}
		| WriteInstr::SetOutput {
			..
		}
		| WriteInstr::DispatchTable {
			..
		} => None,
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
		// Boxed only where a pipeline runs: the settled forms, which every
		// record of a `RETURN NONE` or plain `RETURN AFTER` write takes, build
		// no future.
		let settled = match state.write.as_deref_mut() {
			Some(write) => show_settled(instr, write),
			None => Err(internal("a write body with no write state")),
		};
		let shown = match settled {
			Ok(true) => Ok(()),
			Ok(false) => out_of_line(|| show(program, instr, state)).await,
			Err(failure) => Err(failure),
		};
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
	crate::exec::operators::write::ctx::require_authority_to_define_table(exec)
		.map_err(ControlFlow::Err)?;
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
/// for the table the record is written to: the bundle the compile linked for
/// a write that names its one table, or, for a write that may reach any
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
	let exec = ctx.exec();
	let outside = || internal(&format!("a table outside the dispatch set: {}", ctx.table.name));
	let (stages, units) = match (&tables.linked, &tables.by_name) {
		(Some(linked), _) => {
			if linked.table != ctx.table.name {
				return Err(outside());
			}
			(Arc::clone(&linked.bundle), Arc::clone(&linked.bundle.units))
		}
		(None, Some(named)) => {
			let bundle = write.bundle_for(named, &ctx).await?;
			// The bundle is the program of the stages and the root of the arena
			// that holds what they run.
			let units = Arc::clone(&bundle.units);
			(bundle, units)
		}
		(None, None) => {
			let unit = tables.unit(&ctx.table.name).ok_or_else(outside)?;
			write_unit(program, unit, state, exec)?
		}
	};
	let pipeline = stages
		.pipelines
		.get(site as usize)
		.ok_or_else(|| internal("a run of stages the stage program does not hold"))?;
	let (mut nested, entered) = enter_unit(&stages, units, state, exec)?;
	// The caller already boxes this future (`DispatchTable`), so the row runs inline in it.
	let ran = run_value_row(&stages, pipeline, Value::None, &mut nested, exec).await;
	leave_unit(&mut nested, entered, state);
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

/// What [`enter_unit`] took of a write that [`leave_unit`] gives back: the
/// block statement it was running for, and whether it was already running
/// inside a unit, which a unit entered from a unit leaves it doing.
#[derive(Clone, Copy, Debug)]
pub(super) struct Entered {
	group: Option<u16>,
	nested: bool,
}

/// A state of `unit`'s own, over the unit table `units`, that holds the write
/// of `state` while the unit runs, and what of the write [`leave_unit`] gives
/// back.
pub(super) fn enter_unit(
	unit: &Program,
	units: super::UnitTable,
	state: &mut State,
	exec: &ExecutionContext,
) -> FlowResult<(PooledState, Entered)> {
	let mut nested = State::new(unit, Vec::new(), exec.function_registry())?;
	nested.frames = unit.permissions.iter().map(|spec| spec.context(exec)).collect();
	nested.permissions_bypassed = state.permissions_bypassed;
	nested.rowless = true;
	// The ids the unit names are the statement's.
	nested.units = Some(units);
	// The unit's write is its own, whichever statement's this is.
	let mut held = state.write.take();
	let mut entered = Entered {
		group: None,
		nested: false,
	};
	if let Some(write) = held.as_deref_mut() {
		entered.nested = std::mem::replace(&mut write.nested, true);
		entered.group = write.group.take();
	}
	nested.write = held;
	Ok((nested, entered))
}

/// Give the write [`enter_unit`] took back to `state`, as it was entered.
pub(super) fn leave_unit(nested: &mut State, entered: Entered, state: &mut State) {
	let mut held = nested.write.take();
	if let Some(write) = held.as_deref_mut() {
		write.nested = entered.nested;
		write.group = entered.group;
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
/// in flight where no host pipeline runs, and answer whether it was taken.
///
/// An instruction that runs a pipeline — a shown image's view over an image
/// that is not shown as it is, or a projection — is left untouched and
/// answers `false`, for [`show`] to take. Either way the instruction has the
/// same effect: this is the part of [`show`] that awaits nothing, taken
/// without building [`show`]'s future.
fn show_settled(instr: &WriteInstr, write: &mut WriteState) -> FlowResult<bool> {
	if write.ctx.is_none() {
		return Err(internal("a write step before the record's context was resolved"));
	}
	match instr {
		WriteInstr::ShowImage {
			image,
			view,
			checked,
		} => {
			let row = write.row()?;
			let as_is = shown_as_is(row, *image, *checked);
			if view.is_some() && !as_is {
				return Ok(false);
			}
			// No pipeline runs: the output stage is the last of every order,
			// so nothing reads the image once it is shown, and the output
			// takes it.
			let shown = std::mem::replace(
				match image {
					RecordImage::Current => &mut row.current,
					RecordImage::Initial => &mut row.initial,
				},
				Value::None,
			);
			if *checked && !as_is {
				match image {
					RecordImage::Current => row.current_reduced = Some(shown.clone()),
					RecordImage::Initial => row.initial_reduced = Some(shown.clone()),
				}
			}
			write.shown[slot(*image)] = Some(shown);
			Ok(true)
		}
		WriteInstr::SetOutput {
			output,
		} => {
			// The output stage shows an image and sets the output from it, and
			// nothing reads a shown image after that: the output takes it.
			let value = match output {
				WriteOutput::None => None,
				WriteOutput::Null => Some(Value::Null),
				WriteOutput::Shown(image) => Some(take_shown(write, *image)?),
				WriteOutput::Diff => {
					let before = take_shown(write, RecordImage::Initial)?;
					let after = take_shown(write, RecordImage::Current)?;
					Some(Operation::operations_to_value(before.diff(&after)))
				}
				WriteOutput::Projection(_) => return Ok(false),
			};
			write.row()?.output = value;
			Ok(true)
		}
		WriteInstr::Record
		| WriteInstr::Gate {
			..
		}
		| WriteInstr::Begin
		| WriteInstr::LoadNamedSubject
		| WriteInstr::JumpIfNew {
			..
		}
		| WriteInstr::JumpIfNotIterationInitial {
			..
		}
		| WriteInstr::SavepointPush
		| WriteInstr::SavepointRelease
		| WriteInstr::SavepointEndSkipped
		| WriteInstr::ReachedStore
		| WriteInstr::SavepointRollback
		| WriteInstr::TryBegin {
			..
		}
		| WriteInstr::TryEnd
		| WriteInstr::ClearMutated
		| WriteInstr::RethrowSignal
		| WriteInstr::Rethrow
		| WriteInstr::ClassifyCreateFailure {
			..
		}
		| WriteInstr::JumpIfDone {
			..
		}
		| WriteInstr::ReloadForUpdate
		| WriteInstr::CarryBegin
		| WriteInstr::CarryEnd
		| WriteInstr::RaiseCarried
		| WriteInstr::End {
			..
		}
		| WriteInstr::JumpIfImport {
			..
		}
		| WriteInstr::JumpIfNoPostImage {
			..
		}
		| WriteInstr::ClearCurrentReduced
		| WriteInstr::DispatchTable {
			..
		}
		| WriteInstr::Data(_)
		| WriteInstr::Schema(_)
		| WriteInstr::Permissions(_)
		| WriteInstr::Observe(_)
		| WriteInstr::Storage(_) => Err(internal("an image step at an instruction that is not one")),
	}
}

/// Whether `image` of `row` is shown as it stands, with no view run over it:
/// there is no record to show, or it is an unchecked caller's post-image that
/// already carries its `COMPUTED` values — deriving them again would answer a
/// non-deterministic body differently from the one the record's observers
/// were given.
fn shown_as_is(row: &WriteRow, image: RecordImage, checked: bool) -> bool {
	let source = match image {
		RecordImage::Current => &row.current,
		RecordImage::Initial => &row.initial,
	};
	!matches!(source, Value::Object(_))
		|| row.id.is_none()
		|| (!checked && image == RecordImage::Current && row.fields_computed)
}

/// Take the image in `image`'s shown slot, which the output stage shows before
/// it sets the output.
fn take_shown(write: &mut WriteState, image: RecordImage) -> FlowResult<Value> {
	write.shown[slot(image)]
		.take()
		.ok_or_else(|| internal("an image output before the image was shown"))
}

/// Take [`WriteInstr::ShowImage`] or [`WriteInstr::SetOutput`] for the record
/// in flight, running the host pipelines they name over its images. An
/// instruction that runs none is taken as [`show_settled`] takes it.
async fn show(program: &Program, instr: &WriteInstr, state: &mut State) -> FlowResult<()> {
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	if show_settled(instr, write)? {
		return Ok(());
	}
	let (row, ctx) = write.in_flight()?;
	let exec = ctx.exec();
	match instr {
		WriteInstr::ShowImage {
			image,
			view: Some(view),
			checked,
		} => {
			// The view may raise, and the record's failure handlers then see
			// the row as it stood, so it runs over a copy.
			let source = match image {
				RecordImage::Current => row.current.clone(),
				RecordImage::Initial => row.initial.clone(),
			};
			let shown = run_image(program, *view, source, None, state, exec).await?;
			let write = state
				.write
				.as_deref_mut()
				.ok_or_else(|| internal("a write body with no write state"))?;
			if *checked {
				let row = write.row()?;
				match image {
					RecordImage::Current => row.current_reduced = Some(shown.clone()),
					RecordImage::Initial => row.initial_reduced = Some(shown.clone()),
				}
			}
			write.shown[slot(*image)] = Some(shown);
		}
		WriteInstr::SetOutput {
			output: WriteOutput::Projection(pipeline),
		} => {
			let before = take_shown(write, RecordImage::Initial)?;
			let after = take_shown(write, RecordImage::Current)?;
			let frame = exec.with_params([("before", before), ("after", after.clone())]);
			let value = run_image(program, *pipeline, after, Some(frame), state, exec).await?;
			let write = state
				.write
				.as_deref_mut()
				.ok_or_else(|| internal("a write body with no write state"))?;
			write.row()?.output = Some(value);
		}
		WriteInstr::ShowImage {
			..
		}
		| WriteInstr::SetOutput {
			..
		}
		| WriteInstr::Record
		| WriteInstr::Gate {
			..
		}
		| WriteInstr::Begin
		| WriteInstr::LoadNamedSubject
		| WriteInstr::JumpIfNew {
			..
		}
		| WriteInstr::JumpIfNotIterationInitial {
			..
		}
		| WriteInstr::SavepointPush
		| WriteInstr::SavepointRelease
		| WriteInstr::SavepointEndSkipped
		| WriteInstr::ReachedStore
		| WriteInstr::SavepointRollback
		| WriteInstr::TryBegin {
			..
		}
		| WriteInstr::TryEnd
		| WriteInstr::ClearMutated
		| WriteInstr::RethrowSignal
		| WriteInstr::Rethrow
		| WriteInstr::ClassifyCreateFailure {
			..
		}
		| WriteInstr::JumpIfDone {
			..
		}
		| WriteInstr::ReloadForUpdate
		| WriteInstr::CarryBegin
		| WriteInstr::CarryEnd
		| WriteInstr::RaiseCarried
		| WriteInstr::End {
			..
		}
		| WriteInstr::JumpIfImport {
			..
		}
		| WriteInstr::JumpIfNoPostImage {
			..
		}
		| WriteInstr::ClearCurrentReduced
		| WriteInstr::DispatchTable {
			..
		}
		| WriteInstr::Data(_)
		| WriteInstr::Schema(_)
		| WriteInstr::Permissions(_)
		| WriteInstr::Observe(_)
		| WriteInstr::Storage(_) => {
			return Err(internal("an image step that runs no pipeline left untaken"));
		}
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
/// Replace a unique-index conflict error with one that names neither the stored
/// record nor its indexed value, when the caller's reads are permission checked
/// (`hide`). Naming either would disclose a row or field value the caller cannot
/// read; the conflict itself is still reported (GHSA-cff7-q66j-4hc8). Applied
/// where the write's error leaves the operator, after any UPSERT/INSERT retry
/// that needs the record has run: at the stream exit for a streaming write, and
/// at the grouped-write exit for a write a block, `IF` or `FOR` runs, which does
/// not pass through the stream.
pub(super) fn hide_conflict_record(flow: ControlFlow, hide: bool) -> ControlFlow {
	if hide
		&& let ControlFlow::Err(err) = &flow
		&& let Some(index) = index_conflict_name(err)
	{
		ControlFlow::Err(anyhow::Error::new(crate::idx::Error::IndexExistsHidden {
			index,
		}))
	} else {
		flow
	}
}

/// The index name a unique-index conflict `err` reports, in whichever of the two
/// shapes the write path hands one up: a bare `idx::Error::IndexExists`, or one
/// wrapped in `crate::err::Error::Idx` before `anyhow`. `anyhow`'s downcast does
/// not look through the wrapping enum, so both are checked; any other error is
/// `None`.
fn index_conflict_name(err: &anyhow::Error) -> Option<String> {
	if let Some(crate::idx::Error::IndexExists {
		index,
		..
	}) = err.downcast_ref::<crate::idx::Error>()
	{
		return Some(index.clone());
	}
	if let Some(crate::err::Error::Idx(crate::idx::Error::IndexExists {
		index,
		..
	})) = err.downcast_ref::<crate::err::Error>()
	{
		return Some(index.clone());
	}
	None
}

pub(super) fn stream(program: Arc<Program>, ctx: ExecutionContext) -> ValueBatchStream {
	Box::pin(common::future::stream::try_async_stream(async move |mut stream: Yielder<_>| {
		let mut yielder = Out::Stream(&mut stream);
		let spec = program.write.as_ref().ok_or_else(|| internal("a write stream for a read"))?;
		// Whether a unique-index conflict must withhold the stored record and
		// value. Read before `in_write`, so it reflects the caller's own
		// permission status rather than the suppressed write frame, or the
		// status a frame running on a caller's behalf was seeded with.
		let hide_index_conflict =
			ctx.should_check_perms(crate::iam::Action::View)? || ctx.caller_view_checked();
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
		// A statement spends no more than its own quantum before its first
		// yield.
		super::fuel::cap(super::fuel::quantum(&ctx));
		if !program.groups.is_empty() {
			state.program = Some(Arc::clone(&program));
		}
		state.permissions_bypassed = ctx.root().skip_fetch_perms;
		state.frames = program.permissions.iter().map(|spec| spec.context(&ctx)).collect();
		state.write = Some(Box::new(WriteState::new(spec, &ctx)?));
		run_write(&program, None, version, &mut state, &ctx, &mut yielder, flush_at)
			.await
			.map_err(|err| hide_conflict_record(err, hide_index_conflict))?;
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
	super::host::forget_knn_queries(program, group, state);
	write_state(state)?.end_reads();
	let ran = run_write_pipelines(program, group, version, state, ctx, yielder, flush_at).await;
	// The statement's reads end however its pipelines did: finished, failed,
	// or left by a control-flow signal such as a `BREAK`. A block keeps the write state of a
	// statement it ran for the statement's next run, so a watch left open
	// would go on noting the creations of every later statement of the
	// transaction in its table.
	if let Some(write) = state.write.as_deref_mut() {
		write.end_reads();
	}
	let ran = match ran {
		Err(ControlFlow::Err(err)) => match crate::kvs::storage_error(&err) {
			Some(crate::kvs::Error::DeferredKeyExists {
				key,
			}) => Err(ControlFlow::Err(crate::dbs::executor::held_create_exists(key))),
			_ => Err(ControlFlow::Err(err)),
		},
		other => other,
	};
	ran?;
	let write = write_state(state)?;
	write.judge_only()?;
	let none = write.only && write.returned == 0;
	if none {
		state.output.push(Value::None);
	}
	Ok(())
}

/// Run the write pipelines of `program` that `group` owns over the
/// statement's subjects, for [`run_write`].
#[allow(clippy::too_many_arguments)]
async fn run_write_pipelines(
	program: &Arc<Program>,
	group: Option<u16>,
	version: Option<u64>,
	state: &mut State,
	ctx: &ExecutionContext,
	yielder: &mut Out<'_, '_>,
	flush_at: usize,
) -> FlowResult<()> {
	for pipeline in program.pipelines.iter().filter(|pipeline| pipeline.group == group) {
		// The host begins each read of a table as the source opens it
		// ([`WriteState::read_table`]); the targets say per subject.
		write_state(state)?.reads_subjects(reads_stored_records(&pipeline.source));
		#[cfg(test)]
		super::row_packed::observed::note_pipeline(pipeline, state);
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
			source @ (Source::Scan {
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
			| Source::Union(_)
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
			| Source::LightweightEdges {
				..
			}
			| Source::Vacant {
				..
			}) => {
				super::host::out_of_line(|| {
					run_storage_source(
						program, pipeline, source, version, state, ctx, yielder, flush_at,
					)
				})
				.await?;
			}
		}
	}
	Ok(())
}

/// Whether a write over the rows of `source` reads its subjects from the
/// stored records of a table, rather than from values.
fn reads_stored_records(source: &Source) -> bool {
	match source {
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
		| Source::Union(_) => true,
		Source::Count {
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
		| Source::Vacant {
			..
		} => false,
	}
}

/// The tables whose stored records `source` reads as it opens, before it
/// yields a row ([`WriteState::read_table`]). A union's sources are read as
/// each opens, and a source whose range is a value it evaluates — a record
/// range, or a lightweight relation's edges in one — begins its read once the
/// value is known, so neither names a table here.
pub(super) fn opened_tables<'a>(source: &'a Source, out: &mut Vec<&'a crate::val::TableName>) {
	match source {
		Source::Scan {
			table,
			..
		}
		| Source::Lookup {
			table,
			..
		}
		| Source::Bitmap {
			table,
			..
		}
		| Source::FullText {
			table,
			..
		}
		| Source::Knn {
			table,
			..
		}
		| Source::LightweightEdges {
			table,
			record: None,
			..
		} => out.push(table),
		Source::Index {
			index,
			..
		} => out.push(&index.definition().table_name),
		Source::IndexUnion {
			branches,
			..
		} => out.extend(branches.iter().map(|branch| &branch.index.definition().table_name)),
		Source::Union(_)
		| Source::Record {
			..
		}
		| Source::LightweightEdges {
			record: Some(_),
			..
		}
		| Source::Count {
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
		| Source::Vacant {
			..
		} => {}
	}
}

/// Run a write body over the subjects each target names: a record to create,
/// a record to read, or the records of a table or range, in written order.
/// An `UPSERT` over a table in which it matched nothing then writes the one
/// record it guarantees. Where `guarantees`, a table's records are read by
/// another pipeline of the write, run before this one, and only that record
/// is written here.
#[allow(clippy::too_many_arguments)]
async fn run_targets(
	program: &Arc<Program>,
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
		let mut subjects = match target {
			super::program::Target::Scalar(scalar) => {
				let (this, forbidding) = ctx.enclosing_row();
				let ctx = forbidding.as_ref().unwrap_or(ctx);
				let value = super::host::host_scalar(program, scalar, ctx, this).await?;
				let mut named = Vec::new();
				target_subjects(value, form, deferable, &mut named)
					.map_err(|err| refused_by(err, form))?;
				TargetSubjects::Named(named.into_iter())
			}
			super::program::Target::Mock(mock) => {
				// The ids of a range that the plan fans out are the rows of a write set: the
				// compute half of each runs on a worker, and this task applies them in the
				// order the ids are generated in.
				let mut rows = MorselRows::of(program, pipeline, ctx);
				if !matches!(rows, MorselRows::Serial) {
					if !deferable || !matches!(form, WriteForm::Create) {
						return Err(internal(
							"a write over the ids of a mock that is no create fans out",
						));
					}
					for id in mock.clone() {
						let row = Value::RecordId(id);
						rows.row(program, pipeline, row, state, ctx, yielder, flush_at).await?;
					}
					rows.finish(state, ctx, yielder, flush_at).await?;
					continue;
				}
				TargetSubjects::Mock(mock.clone().into_iter())
			}
		};
		while let Some(subject) =
			subjects.next(form, deferable).map_err(|err| refused_by(err, form))?
		{
			// A subject is a unit, whether or not a record answers it; each
			// row it yields is one more as it runs.
			super::fuel::tick(ctx, 1).await?;
			let mut rows = match subject {
				Subject::Row(row) => {
					let write = write_state(state)?;
					write.reads_subjects(false);
					if !matches!(form, WriteForm::Create)
						&& let Some(table) = row.table()
					{
						write.watch_table(table)?;
					}
					Rows::One(Some(row))
				}
				Subject::Read(id) => {
					let write = write_state(state)?;
					write.reads_subjects(false);
					if !matches!(form, WriteForm::Create) {
						write.watch_table(&id.table)?;
					}
					Rows::One(Some(Box::new(read_subject(ctx, id).await?)))
				}
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
					// A scan of the table's key range checks no catalog, so a
					// write that requires the table defined checks it here,
					// before it reads.
					if matches!(form.table_existence(), TableExistence::Required) {
						super::host::require_defined(ctx, &table, ctx.version_stamp()).await?;
					}
					let write = write_state(state)?;
					write.reads_subjects(true);
					write.read_table(&table)?;
					Rows::Scan(scan_subjects(ctx, &table, range.as_deref()).await?)
				}
			};
			while let Some(row) = rows.next(ctx).await? {
				write_state(state)?.row = Some(row);
				run_value_row(program, pipeline, Value::None, state, ctx).await?;
				flush(state, yielder, flush_at, ctx).await?;
			}
		}
	}
	// The guaranteed record is no subject of a read.
	write_state(state)?.reads_subjects(false);
	// What the statement matched is counted, not what it returned, and an
	// import writes no record its export never held. The table is one the
	// gate admitted, and its answer is memoised.
	if write_state(state)?.matched == 0
		&& !ctx.options().is_some_and(|options| options.import)
		&& let Some(table) = guaranteed
	{
		write_state(state)?.row = Some(WriteRow::for_target(Value::Table(table))?);
		run_value_row(program, pipeline, Value::None, state, ctx).await?;
		flush(state, yielder, flush_at, ctx).await?;
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
		write_state(state)?.row = Some(insert_row(value, &into, relation)?);
		run_value_row(program, pipeline, Value::None, state, ctx).await?;
		flush(state, yielder, flush_at, ctx).await?;
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
	let (this, forbidding) = ctx.enclosing_row();
	let ctx = forbidding.as_ref().unwrap_or(ctx);
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
	let val = insert_scalar(program, &into.scalar, state, ctx).await?;
	match val {
		Value::Table(into) => Ok(into),
		Value::String(into) => Ok(crate::val::TableName::new(into)),
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
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
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => {
			Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InsertStatement {
				value: into.sql.clone(),
			})))
		}
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
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_)) => {
				Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InsertStatement {
					value: other.to_sql(),
				})))
			}
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
	let (this, forbidding) = ctx.enclosing_row();
	let ctx = forbidding.as_ref().unwrap_or(ctx);
	let endpoint = |scalar| super::host::host_scalar(program, scalar, ctx, this);
	let froms = relate_endpoints(endpoint(from).await?, RelateSide::From)?;
	let tos = relate_endpoints(endpoint(to).await?, RelateSide::To)?;
	for f in &froms {
		for t in &tos {
			let at = endpoint(through).await?;
			write_state(state)?.row = Some(relate_row(at, f.clone(), t.clone())?);
			run_value_row(program, pipeline, Value::None, state, ctx).await?;
			flush(state, yielder, flush_at, ctx).await?;
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

/// A write state for the compute half of the records of the write `state` runs, over the scope
/// `state`'s own write state holds ([`WriteState::for_prefix`]).
#[cfg_attr(target_family = "wasm", allow(dead_code))]
pub(super) fn prefix_state(
	program: &Program,
	state: &State,
	ctx: &ExecutionContext,
) -> FlowResult<Box<WriteState>> {
	let spec = spec_of(program, state)?;
	let write = state
		.write
		.as_deref()
		.ok_or_else(|| internal("the compute half of a write with no write state"))?;
	Ok(Box::new(write.for_prefix(spec, ctx)?))
}

fn write_state(state: &mut State) -> FlowResult<&mut WriteState> {
	state.write.as_deref_mut().ok_or_else(|| internal("a write source with no write state"))
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use surrealdb_types::ToSql as _;

	use crate::dbs::Session;
	use crate::exec::operators::test_util::TestDb;

	const SETUP: &str = "
		DEFINE TABLE user SCHEMALESS; CREATE user:alice;
		DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM type::record('user', $id))
			DURATION FOR TOKEN 1h, FOR SESSION 1h;
		DEFINE TABLE plain SCHEMALESS PERMISSIONS FULL;
		CREATE plain:1 SET n = 1, tag = 'a';
		CREATE plain:2 SET n = 1, tag = 'a';
		CREATE plain:3 SET n = 1, tag = 'a';
	";

	/// What `sql` answers as `session`, one string per statement.
	async fn answers(db: &TestDb, session: &Session, sql: &str) -> Vec<String> {
		let responses = db.datastore().execute(sql, session, None).await.expect("it executes");
		responses
			.into_iter()
			.map(|response| match response.result {
				Ok(value) => value.to_sql(),
				Err(error) => format!("error: {error}"),
			})
			.collect()
	}

	/// An image shown through no pipeline is taken out of the row, and is the image the record
	/// held: the pre-image and the post-image, for a caller whose reads are checked and one whose
	/// reads are not.
	#[tokio::test]
	async fn an_image_shown_without_a_pipeline_is_the_image_as_it_stood() {
		let record = Session::for_record(
			"test",
			"test",
			"user",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "alice")),
		);
		for session in [TestDb::owner(), record] {
			let db = TestDb::new(SETUP).await;
			let sql = "
				UPDATE plain:1 SET n = 2 RETURN BEFORE;
				UPDATE plain:2 SET n = 2 RETURN AFTER;
				UPDATE plain:3 SET n = 2 RETURN DIFF;
				UPDATE plain:1 SET n = 3;
				UPDATE plain:1 SET n = 4 RETURN id, $before.n, $after.n;
				DELETE plain:2 RETURN BEFORE;
				CREATE plain:4 SET n = 9 RETURN AFTER;
			";
			let held = answers(&db, &session, sql).await;
			assert_eq!(
				held,
				[
					"[{ id: plain:1, n: 1, tag: 'a' }]",
					"[{ id: plain:2, n: 2, tag: 'a' }]",
					"[[{ op: 'replace', path: '/n', value: 2 }]]",
					"[{ id: plain:1, n: 3, tag: 'a' }]",
					"[{ \"$after.n\": 4, \"$before.n\": 3, id: plain:1 }]",
					"[{ id: plain:2, n: 2, tag: 'a' }]",
					"[{ id: plain:4, n: 9 }]",
				],
				"{session:?}"
			);
		}
	}

	/// Whether `program`, or a unit it holds, derives a write's snapshot by
	/// reading the components of the record id from its key
	/// ([`super::DataInstr::SnapshotKeyParts`]).
	fn snapshots_key_parts(program: &super::Program) -> bool {
		use super::super::program::{Instr, WriteInstr};
		use super::DataInstr;
		program
			.pipelines
			.iter()
			.chain(&program.host_code)
			.flat_map(|pipeline| pipeline.code.iter())
			.any(|instr| {
				matches!(instr, Instr::Write(WriteInstr::Data(DataInstr::SnapshotKeyParts { .. })))
			}) || program.units.iter().any(|unit| snapshots_key_parts(unit))
	}

	/// The owner's condition or data clause over a table whose only computed
	/// fields are the components of its record id reads them from the key, and
	/// over a table that also computes another field, or computes nothing, it
	/// derives the snapshot as before.
	#[tokio::test]
	async fn a_snapshot_of_components_alone_reads_them_from_the_key() {
		use super::super::tests_morsel::{bind, planner};
		let schema = "
			DEFINE TABLE reading PERMISSIONS FULL;
			DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
			DEFINE TABLE counter PERMISSIONS FULL;
			DEFINE FIELD id ON counter TYPE [site: string, n: int];
			DEFINE TABLE mixed PERMISSIONS FULL;
			DEFINE FIELD id ON mixed TYPE [city: string, time: datetime];
			DEFINE FIELD loud ON mixed COMPUTED string::uppercase(city);
			DEFINE TABLE plain PERMISSIONS FULL;
		";
		let db = TestDb::new_with_auth(schema).await;
		for statement in [
			"DELETE {} WHERE city = 'London'",
			"UPDATE {} SET a = 1 WHERE city = 'London'",
			"UPDATE {} SET copy = city",
			"UPSERT {} SET a = 1 WHERE city = 'London'",
		] {
			for (table, reads_key) in
				[("reading", true), ("counter", true), ("mixed", false), ("plain", false)]
			{
				let sql = statement.replace("{}", table);
				let ctx =
					db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
				let bound = bind(&ctx, &sql).await;
				let mut program = super::super::compile::compile(&planner(&ctx), &bound)
					.await
					.expect("compiles")
					.program;
				super::super::compile::seed_statement(&planner(&ctx), &mut program)
					.await
					.expect("seeds");
				ctx.txn().cancel().await.expect("cancel");
				assert_eq!(snapshots_key_parts(&program), reads_key, "`{sql}`");
			}
		}
	}

	/// A multi-target write that creates a record in an earlier target and scans the same table in
	/// a later target passes over the record it created, while still visiting records that already
	/// stood before the statement began.
	#[tokio::test]
	async fn a_later_table_target_passes_over_a_record_an_earlier_target_created() {
		let db = TestDb::new(SETUP).await;
		let sql = "
			UPSERT plain:new, plain SET n = (n ?? 0) + 1 WHERE true;
			SELECT id, n FROM plain ORDER BY id;
		";
		let held = answers(&db, &TestDb::owner(), sql).await;
		assert_eq!(
			held,
			[
				"[{ id: plain:new, n: 1 }, { id: plain:1, n: 2, tag: 'a' }, { id: plain:2, n: 2, tag: 'a' }, { id: plain:3, n: 2, tag: 'a' }]",
				"[{ id: plain:1, n: 2 }, { id: plain:2, n: 2 }, { id: plain:3, n: 2 }, { id: plain:new, n: 1 }]",
			]
		);
	}

	/// A held create that fails against a record already in storage does not leave the failed
	/// image cached in the transaction for a subsequent read in the same transaction.
	#[tokio::test]
	async fn a_failed_held_create_does_not_pollute_the_transaction_record_cache() {
		let db = TestDb::new(SETUP).await;
		let sql = "
			INSERT IGNORE INTO plain [{ id: plain:1, n: 99, tag: 'stale' }, { id: plain:4, n: 4, tag: 'b' }];
			SELECT id, n, tag FROM plain:1;
			SELECT id, n, tag FROM plain:4;
		";
		let held = answers(&db, &TestDb::owner(), sql).await;
		assert_eq!(
			held,
			[
				"[{ id: plain:4, n: 4, tag: 'b' }]",
				"[{ id: plain:1, n: 1, tag: 'a' }]",
				"[{ id: plain:4, n: 4, tag: 'b' }]",
			]
		);
	}

	/// `Datastore::execute_edge` settles held creates before committing each statement, persisting
	/// absent creates and reporting a duplicate record as `RecordExists`.
	#[tokio::test]
	async fn edge_execution_settles_held_creates_and_reports_duplicates() {
		let db = TestDb::new(SETUP).await;
		let sql = "
			CREATE plain:4 SET n = 4, tag = 'b';
			CREATE plain:1 SET n = 99, tag = 'dup';
			SELECT id, n FROM plain ORDER BY id;
		";
		let responses = db
			.datastore()
			.execute_edge(sql, &TestDb::owner(), None)
			.await
			.expect("edge query is admitted");
		let outcomes: Vec<String> = responses
			.into_iter()
			.map(|r| match r.result {
				Ok(v) => v.to_sql(),
				Err(e) => format!("error: {e}"),
			})
			.collect();
		assert_eq!(
			outcomes,
			[
				"[{ id: plain:4, n: 4, tag: 'b' }]",
				"error: Database record `plain:1` already exists",
				"[{ id: plain:1, n: 1 }, { id: plain:2, n: 1 }, { id: plain:3, n: 1 }, { id: plain:4, n: 4 }]",
			]
		);
	}

	#[tokio::test]
	async fn return_before_diff_and_projection_enforce_select_permission_on_initial_image() {
		let setup = "
			DEFINE TABLE user SCHEMALESS;
			CREATE user:alice;
			CREATE user:bob;
			DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM type::record('user', $id))
				DURATION FOR TOKEN 1h, FOR SESSION 1h;
			DEFINE TABLE doc SCHEMALESS
				PERMISSIONS
					FOR select WHERE owner = $auth.id
					FOR create, update FULL;
			CREATE doc:1 SET owner = user:bob, secret = 'bobs-data';
			CREATE doc:2 SET owner = user:bob, secret = 'bobs-diff';
			CREATE doc:3 SET owner = user:bob, secret = 'bobs-proj';
		";
		let db = TestDb::new(setup).await;
		let alice = Session::for_record(
			"test",
			"test",
			"user",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "alice")),
		);
		let sql = "
			UPDATE doc:1 SET owner = user:alice, secret = 'overwritten' RETURN BEFORE;
			UPDATE doc:2 SET owner = user:alice, secret = 'overwritten' RETURN DIFF;
			UPDATE doc:3 SET owner = user:alice, secret = 'overwritten' RETURN $before.secret;
			UPSERT doc:4 SET owner = user:alice, secret = 'created' RETURN BEFORE;
			SELECT id, secret FROM doc ORDER BY id;
		";
		let held = answers(&db, &alice, sql).await;
		assert_eq!(
			held,
			[
				"[]",
				"[]",
				"[]",
				"[NONE]",
				"[{ id: doc:1, secret: 'overwritten' }, { id: doc:2, secret: 'overwritten' }, { id: doc:3, secret: 'overwritten' }, { id: doc:4, secret: 'created' }]",
			]
		);
	}

	#[tokio::test]
	async fn vanished_and_skipped_nested_places_enforce_readonly_and_update_permissions() {
		let setup = "
			DEFINE TABLE user SCHEMALESS;
			CREATE user:alice;
			DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM type::record('user', $id))
				DURATION FOR TOKEN 1h, FOR SESSION 1h;
			DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD a ON doc TYPE option<object> PERMISSIONS FULL;
			DEFINE FIELD a.ro ON doc TYPE option<int> READONLY;
			DEFINE FIELD a.locked ON doc TYPE option<string>
				PERMISSIONS FOR create FULL, FOR select FULL, FOR update NONE;
			DEFINE FIELD arr ON doc TYPE option<array<object>> PERMISSIONS FULL;
			DEFINE FIELD arr.*.locked ON doc TYPE option<string>
				PERMISSIONS FOR create FULL, FOR select FULL, FOR update NONE;
			CREATE doc:1 SET a = { ro: 42, locked: 'keep' }, arr = [{ locked: 'elem0' }];
		";
		let db = TestDb::new(setup).await;
		let alice = Session::for_record(
			"test",
			"test",
			"user",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "alice")),
		);
		let sql = "
			UPDATE doc:1 SET a = NONE;
			UPDATE doc:1 CONTENT { a: { ro: 42 }, arr: [] };
		";
		let held = answers(&db, &alice, sql).await;
		assert!(
			held[0].contains("Found changed value for field `a.ro`"),
			"expected readonly error on parent deletion, got: {}",
			held[0]
		);
		assert_eq!(
			held[1],
			"[{ a: { locked: 'keep', ro: 42 }, arr: [{ locked: 'elem0' }], id: doc:1 }]"
		);
	}

	#[tokio::test]
	async fn assert_and_coerce_errors_redact_values_when_select_permission_restricted() {
		let setup = "
			DEFINE TABLE user SCHEMALESS;
			CREATE user:alice;
			DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM type::record('user', $id))
				DURATION FOR TOKEN 1h, FOR SESSION 1h;
			DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD secret_assert ON doc TYPE option<int> ASSERT $value > 100
				PERMISSIONS FOR select NONE, FOR create, update FULL;
			DEFINE FIELD secret_coerce ON doc TYPE int
				PERMISSIONS FOR select NONE, FOR create, update FULL;
			CREATE doc:1 SET secret_assert = 200, secret_coerce = 10;
		";
		let db = TestDb::new(setup).await;
		let alice = Session::for_record(
			"test",
			"test",
			"user",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "alice")),
		);
		let sql = "
			UPDATE doc:1 SET secret_assert = 42;
			UPDATE doc:1 SET secret_coerce = 'not-an-int';
			UPDATE doc:1 SET secret_coerce = secret_coerce;
		";
		let held = answers(&db, &alice, sql).await;
		assert!(
			held[0].contains("[REDACTED]") && !held[0].contains("42"),
			"expected assert failure value to be redacted, got: {}",
			held[0]
		);
		assert!(
			held[1].contains("[REDACTED]") && !held[1].contains("not-an-int"),
			"expected coerce failure value to be redacted, got: {}",
			held[1]
		);
		assert!(
			held[2].contains("Expected `int` but found `NONE`"),
			"expected NONE coerce failure not to be redacted, got: {}",
			held[2]
		);
	}

	#[tokio::test]
	async fn assert_and_coerce_errors_redact_values_when_table_select_is_restricted() {
		let setup = "
			DEFINE TABLE user SCHEMALESS;
			CREATE user:alice;
			DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM type::record('user', $id))
				DURATION FOR TOKEN 1h, FOR SESSION 1h;
			DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select NONE, FOR create, update FULL;
			DEFINE FIELD guarded ON doc TYPE option<int> ASSERT $value > 100;
			DEFINE FIELD typed ON doc TYPE array<int>;
			DEFINE TABLE open SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD guarded ON open TYPE option<int> ASSERT $value > 100;
			CREATE doc:1 SET guarded = 200, typed = [1, 2, 3];
			CREATE open:1 SET guarded = 200;
		";
		let db = TestDb::new(setup).await;
		let alice = Session::for_record(
			"test",
			"test",
			"user",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "alice")),
		);
		let sql = "
			UPDATE doc:1 SET guarded = 42;
			UPDATE doc:1 SET typed += 'hidden-element';
			UPDATE open:1 SET guarded = 42;
		";
		let held = answers(&db, &alice, sql).await;
		assert!(
			held[0].contains("[REDACTED]") && !held[0].contains("42"),
			"expected assert failure under a table SELECT NONE to be redacted, got: {}",
			held[0]
		);
		assert!(
			held[1].contains("[REDACTED]")
				&& !held[1].contains("hidden-element")
				&& !held[1].contains("index"),
			"expected coerce failure under a table SELECT NONE to be redacted without its position, got: {}",
			held[1]
		);
		assert!(
			held[2].contains("42") && !held[2].contains("[REDACTED]"),
			"expected assert failure on a readable table to show its value, got: {}",
			held[2]
		);
	}

	#[tokio::test]
	async fn remove_expunge_requires_owner_role() {
		use crate::iam::{Level, Role};

		let setup = "
			DEFINE TABLE t1 SCHEMALESS;
			DEFINE TABLE t2 SCHEMALESS;
			DEFINE TABLE t3 SCHEMALESS;
		";
		let db = TestDb::new(setup).await;
		let db_editor =
			Session::for_level(Level::Database("test".into(), "test".into()), Role::Editor);
		let db_owner =
			Session::for_level(Level::Database("test".into(), "test".into()), Role::Owner);
		let ns_editor = Session::for_level(Level::Namespace("test".into()), Role::Editor);
		let ns_owner = Session::for_level(Level::Namespace("test".into()), Role::Owner);
		let root_editor = Session::for_level(Level::Root, Role::Editor);
		let root_owner = Session::for_level(Level::Root, Role::Owner);

		let editor_table =
			answers(&db, &db_editor, "REMOVE TABLE t1; REMOVE TABLE AND EXPUNGE t2;").await;
		assert_eq!(editor_table[0], "NONE");
		assert!(
			editor_table[1].contains("Not enough permissions to perform this action"),
			"expected Editor to be denied REMOVE TABLE AND EXPUNGE ..., got: {}",
			editor_table[1]
		);

		let owner_table = answers(&db, &db_owner, "REMOVE TABLE AND EXPUNGE t2;").await;
		assert_eq!(owner_table[0], "NONE");

		let editor_db = answers(&db, &ns_editor, "REMOVE DATABASE AND EXPUNGE test;").await;
		assert!(
			editor_db[0].contains("Not enough permissions to perform this action"),
			"expected Editor to be denied REMOVE DATABASE AND EXPUNGE ..., got: {}",
			editor_db[0]
		);
		let owner_db = answers(&db, &ns_owner, "REMOVE DATABASE AND EXPUNGE test;").await;
		assert_eq!(owner_db[0], "NONE");

		let editor_ns = answers(&db, &root_editor, "REMOVE NAMESPACE AND EXPUNGE test;").await;
		assert!(
			editor_ns[0].contains("Not enough permissions to perform this action"),
			"expected Editor to be denied REMOVE NAMESPACE AND EXPUNGE ..., got: {}",
			editor_ns[0]
		);
		let owner_ns = answers(&db, &root_owner, "REMOVE NAMESPACE AND EXPUNGE test;").await;
		assert_eq!(owner_ns[0], "NONE");
	}

	#[tokio::test]
	async fn count_index_with_time_now_guard_does_not_use_stale_write_time_deltas() {
		let setup = "
			DEFINE TABLE doc SCHEMALESS;
			DEFINE INDEX idx_past ON doc COUNT WHERE ts <= time::now();
			CREATE doc:1 SET ts = time::now() + 20ms;
		";
		let db = TestDb::new(setup).await;
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
		let held = answers(
			&db,
			&TestDb::owner(),
			"SELECT count() FROM doc WHERE ts <= time::now() GROUP ALL;",
		)
		.await;
		assert_eq!(held, ["[{ count: 1 }]"]);
	}

	#[tokio::test]
	async fn remove_table_purges_reference_keys_and_caches() {
		let setup = "
			DEFINE TABLE parent SCHEMALESS;
			DEFINE TABLE child SCHEMALESS;
			DEFINE TABLE sibling SCHEMALESS;
			DEFINE FIELD ref ON child TYPE record<parent> REFERENCE ON DELETE REJECT;
			DEFINE FIELD ref ON sibling TYPE option<record<parent>> REFERENCE ON DELETE REJECT;
			CREATE parent:1;
			CREATE child:1 SET ref = parent:1;
		";
		let db = TestDb::new(setup).await;
		let held = answers(
			&db,
			&TestDb::owner(),
			"
				SELECT <~child FROM parent:1;
				REMOVE TABLE child;
				SELECT <~child FROM parent:1;
				DELETE parent:1 RETURN BEFORE;
			",
		)
		.await;
		assert_eq!(
			held,
			["[{ \"<~child\": [child:1] }]", "NONE", "[{ \"<~child\": [] }]", "[{ id: parent:1 }]",]
		);
	}
}
