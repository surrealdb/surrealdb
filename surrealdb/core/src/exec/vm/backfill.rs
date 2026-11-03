//! The host's side of the instructions that fill a view from the tables it
//! reads.
//!
//! A view's backfill is a program: a select over the view's sources computes
//! the rows (or the groups) the view holds, and these instructions turn each
//! into the view table's record. A view's rows carry state no statement can
//! express — an aggregated group's stats — so they are written to storage
//! directly, and then run through the view table's own order (its indexes, its
//! second-order views, events, live queries and change feed), as the
//! incremental path runs the order for a created row. A materialized row is
//! keyed as the incremental path keys it: by its source record's key, or by
//! the key its projected `id` gives. A row whose key the backfill already
//! wrote, because two source records make the same key, replaces that row as
//! the incremental path does, and runs the order for an updated row. The rows
//! the view table held before are retired first, each deleted and run through
//! the order for a deleted row, so a view reading the view table takes the old
//! rows' removal and the new rows' arrival as it takes any other write; where
//! that order writes nothing for the table, they are deleted as one range. The
//! statement that defines the view changes the catalog the order is read from,
//! so the order is the one of the view table's catalog version when the
//! statement's retirement or fill meets the table, served from the compile
//! cache ([`super::fault::view_rows`]).

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::anyhow;
use surrealdb_kvs::key::KVKey as _;

use super::host::run_value_row;
use super::interp::State;
use super::program::{Backfill, Program, StoreInstr};
use super::scalar::Scalar;
use super::store::{read, table_name_of, write};
use super::write::{WriteState, action_pipeline, view_row_exec, view_row_key};
use crate::catalog::aggregation::{self, backfill_stats};
use crate::catalog::{Record, encode_whole_record_with};
use crate::doc::Action;
use crate::exec::operators::write::ctx::{RecordFormatSlot, ResolvedFormat, TableExistence};
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::scope::{WritePurpose, WriteScope};
use crate::exec::operators::write::stages::observe::view_row_reaches_nothing;
use crate::exec::operators::write::stages::view::{ViewBatch, row_of};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::key::schema::{RecordKey, RecordPrefix};
use crate::key::{KVKeyDecode as _, KVSubspace as _};
use crate::kvs::NORMAL_BATCH_SIZE;
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
			// The row is held under the key its `id` gives, as maintaining the view holds it:
			// the source record's, unless the projection names the `id`.
			let id = RecordId {
				key: view_row_key(object.remove("id").unwrap_or(Value::None), &table)?,
				table: table.clone(),
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
			// Source records that make the same key make one row. The view table was emptied
			// before the backfill, so a row already under the key is one this backfill wrote, and
			// it is replaced as maintaining the view replaces it: the source read last holds the
			// row.
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
			if before.is_none() {
				super::write::note_created(&txn, ns, db, &table, &id.key)?;
			}
			let action = match before {
				Some(_) => Action::Update,
				None => Action::Create,
			};
			maintain(state, ctx, &table, id.key, action, before.as_ref(), Some(&record)).await?;
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
			let key = RecordIdKey::Array(Array(group));
			let (ns, db) = ids(ctx).await?;
			let txn = ctx.txn();
			// Each group is written as the view's maintenance writes it: its
			// statistics at their own key where the store keeps them there. Nothing
			// is read first: the statement cleared the view table's whole root, so
			// no group has a row or a state yet.
			let mut batch = ViewBatch::of(ns, db, txn.keyed_view_state());
			let slots = batch.want_group(&table, &key)?;
			let record = row_of(batch.keyed(slots), data, &stats);
			batch.write_group(&txn, slots, &table, &key, &record, &stats, true).await?;
			batch.flush_to(&txn).await?;
			maintain(state, ctx, &table, key, Action::Create, None, Some(&record)).await?;
		}
		StoreInstr::RetireViewRows {
			table,
		} => {
			let table = table_name_of(&read(regs, *table))?;
			retire(state, ctx, &table).await?;
		}
		StoreInstr::RefuseNoWrite
		| StoreInstr::Allow {
			..
		}
		| StoreInstr::Get {
			..
		}
		| StoreInstr::StampCatalog {
			..
		}
		| StoreInstr::Put {
			..
		}
		| StoreInstr::Replace {
			..
		}
		| StoreInstr::GetField {
			..
		}
		| StoreInstr::NextId {
			..
		}
		| StoreInstr::SessionDb {
			..
		}
		| StoreInstr::RealRunOnly
		| StoreInstr::ShowGrants {
			..
		}
		| StoreInstr::IssueGrant {
			..
		}
		| StoreInstr::RevokeGrants {
			..
		}
		| StoreInstr::PurgeGrants {
			..
		}
		| StoreInstr::NewUuid {
			..
		}
		| StoreInstr::SessionAuthLimit {
			..
		}
		| StoreInstr::Lock {
			..
		}
		| StoreInstr::Delete {
			..
		}
		| StoreInstr::DeleteModelArtifact {
			..
		}
		| StoreInstr::DeleteRange {
			..
		}
		| StoreInstr::DiscardTableMigration {
			..
		}
		| StoreInstr::HoldTableForRemoval {
			..
		}
		| StoreInstr::BufferTableChange {
			..
		}
		| StoreInstr::SessionNs {
			..
		}
		| StoreInstr::BumpChangefeedFence {
			..
		}
		| StoreInstr::ClearDatastoreCache
		| StoreInstr::JumpIfImport {
			..
		}
		| StoreInstr::Raise {
			..
		}
		| StoreInstr::RefuseRedefiningTable {
			..
		}
		| StoreInstr::CheckTableChange {
			..
		}
		| StoreInstr::RefuseNamedIdConversion {
			..
		}
		| StoreInstr::ValidateRecordKeys {
			..
		}
		| StoreInstr::RefuseRemovingViewed {
			..
		}
		| StoreInstr::RefuseRemovingLightweight {
			..
		}
		| StoreInstr::PurgeEdgePointers {
			..
		}
		| StoreInstr::ClearViewTable {
			..
		}
		| StoreInstr::RefuseViewCycle {
			..
		}
		| StoreInstr::RefuseClauseEffects {
			..
		}
		| StoreInstr::ViewSources {
			..
		}
		| StoreInstr::Refuse {
			..
		}
		| StoreInstr::ClearCache
		| StoreInstr::Scan {
			..
		}
		| StoreInstr::ScanAt {
			..
		}
		| StoreInstr::VersionStamp {
			..
		}
		| StoreInstr::Clear {
			..
		}
		| StoreInstr::ClearRange {
			..
		}
		| StoreInstr::Compact {
			..
		}
		| StoreInstr::CompactAll
		| StoreInstr::SetQueryTimeout {
			..
		}
		| StoreInstr::NewDatabaseCompatibility {
			..
		}
		| StoreInstr::SetDefaultCompatibility {
			..
		}
		| StoreInstr::VerifyCompatibility {
			..
		}
		| StoreInstr::DropMethods {
			..
		}
		| StoreInstr::Push {
			..
		}
		| StoreInstr::Render {
			..
		}
		| StoreInstr::RenderOne {
			..
		}
		| StoreInstr::CoerceString {
			..
		}
		| StoreInstr::CoerceName {
			..
		}
		| StoreInstr::Convert {
			..
		}
		| StoreInstr::AccessCheck {
			..
		}
		| StoreInstr::ParseApiPath {
			..
		}
		| StoreInstr::CastComment {
			..
		}
		| StoreInstr::ProcessInfo {
			..
		}
		| StoreInstr::InfoIndex {
			..
		}
		| StoreInstr::JumpIfNoService {
			..
		}
		| StoreInstr::RefuseField {
			..
		}
		| StoreInstr::FieldCheck {
			..
		}
		| StoreInstr::PurgeDroppedReferences {
			..
		}
		| StoreInstr::RefuseLightweight {
			..
		}
		| StoreInstr::ClaimDocIdsReclaim {
			..
		}
		| StoreInstr::AdmitIndexFormat {
			..
		}
		| StoreInstr::NextIndexId {
			..
		}
		| StoreInstr::IndexReplayReuses {
			..
		}
		| StoreInstr::FieldAdmitsSubFields {
			..
		}
		| StoreInstr::RefuseBlockingBuild {
			..
		}
		| StoreInstr::BuildIndex {
			..
		}
		| StoreInstr::RetireIndexLocally {
			..
		}
		| StoreInstr::KillOnCommit {
			..
		}
		| StoreInstr::RealtimeChecks
		| StoreInstr::Live {
			..
		}
		| StoreInstr::Kill {
			..
		}
		| StoreInstr::SessionOption {
			..
		}
		| StoreInstr::SessionUseName {
			..
		}
		| StoreInstr::SessionUse {
			..
		}
		| StoreInstr::SessionLetChecks {
			..
		}
		| StoreInstr::SessionLet {
			..
		}
		| StoreInstr::SharesDocIds {
			..
		}
		| StoreInstr::LoadAnalyzer {
			..
		}
		| StoreInstr::CleanupMappers {
			..
		}
		| StoreInstr::EvictModule {
			..
		}
		| StoreInstr::VerifyModule {
			..
		}
		| StoreInstr::LoadModule {
			..
		}
		| StoreInstr::ConnectBucket {
			..
		}
		| StoreInstr::CheckDepth {
			..
		}
		| StoreInstr::SequenceDefinedHere {
			..
		}
		| StoreInstr::EvictSequence {
			..
		}
		| StoreInstr::JumpIfSpeculating {
			..
		}
		| StoreInstr::JumpIfSessionBase {
			..
		}
		| StoreInstr::ResolvePath {
			..
		}
		| StoreInstr::ResolveColumns {
			..
		}
		| StoreInstr::DefineField {
			..
		}
		| StoreInstr::RemoveFieldDefinition {
			..
		}
		| StoreInstr::InfoMigrations {
			..
		}
		| StoreInstr::Migrate {
			..
		}
		| StoreInstr::QueueVerification {
			..
		}
		| StoreInstr::CheckFieldChange {
			..
		}
		| StoreInstr::RemoveFieldStep {
			..
		}
		| StoreInstr::MigrateTable {
			..
		}
		| StoreInstr::CheckIndexMigrations {
			..
		}
		| StoreInstr::AlterFieldDefinition {
			..
		}
		| StoreInstr::AlterFieldClauses {
			..
		}
		| StoreInstr::CheckIndexColumns {
			..
		}
		| StoreInstr::AlterType {
			..
		}
		| StoreInstr::RewriteEnumLabels {
			..
		}
		| StoreInstr::CheckTypeObject {
			..
		}
		| StoreInstr::CheckTypeUnreferenced {
			..
		}
		| StoreInstr::CheckTypesDefined {
			..
		}
		| StoreInstr::CheckTypeDefinition {
			..
		}
		| StoreInstr::CheckTableAlter {
			..
		} => {
			return Err(internal("a store instruction taken as a backfill instruction"));
		}
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
	backfill_stats(&backfill.aggregations, &backfill.values, computed, backfill.groups == 0)
		.map_err(ControlFlow::Err)
}

/// Delete each record of view table `table` and run it through the table's
/// order for a deleted row ([`maintain`]), a page of records at a time: the
/// page is read before any of its records is deleted, so no cursor is open
/// across the writes the order makes. A record is deleted before the order runs
/// over it, as a removed view row is, so a view reading the table that
/// recalculates an extreme over the rows left does not count it. The pages are
/// sized to the fuel left, a unit charged per record.
///
/// A table whose view rows that order writes nothing for
/// ([`view_row_reaches_nothing`]) has its records deleted as one range. A
/// speculative run deletes the records and runs no order: the views reading
/// the table are maintained by the statement's real run.
async fn retire(state: &mut State, ctx: &ExecutionContext, table: &TableName) -> FlowResult<()> {
	let (ns, db) = ids(ctx).await?;
	let txn = ctx.txn();
	let records = RecordPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
	};
	if txn.speculating() {
		txn.note_speculative_skip();
		let range = records.range().map_err(ControlFlow::Err)?;
		return txn.delr(range).await.map_err(ControlFlow::Err);
	}
	let write_ctx = scope(state, ctx).write_ctx_for(table, WritePurpose::ViewMaintenance).await?;
	if view_row_reaches_nothing(&write_ctx) {
		let range = records.range().map_err(ControlFlow::Err)?;
		return txn.delr(range).await.map_err(ControlFlow::Err);
	}
	let mut next = Some(records.range().map_err(ControlFlow::Err)?);
	while let Some(range) = next {
		let size = super::fuel::batch(NORMAL_BATCH_SIZE);
		let page = txn.batch_keys_vals_raw(range, size, None).await.map_err(ControlFlow::Err)?;
		next = page.next.map(|range| records.raw(range));
		if page.result.is_empty() {
			break;
		}
		for (key, bytes) in &page.result {
			let id = RecordKey::decode_key(key).map_err(ControlFlow::Err)?.id.into_owned();
			let rid = RecordId {
				table: table.clone(),
				key: id.clone(),
			};
			let record = txn
				.decode_record_as_written(ns, db, table, bytes, rid, None)
				.await
				.map_err(ControlFlow::Err)?;
			txn.del(key.clone().into()).await.map_err(ControlFlow::Err)?;
			txn.forget_record(ns, db, table, &id);
			maintain(state, ctx, table, id, Action::Delete, Some(&record), None).await?;
		}
		super::fuel::tick(ctx, super::fuel::units(page.result.len())).await?;
	}
	Ok(())
}

/// The write scope the statement's fill and retirement of view rows share,
/// stacked on the statement's scope the first time either meets a row.
fn scope(state: &mut State, ctx: &ExecutionContext) -> Arc<WriteScope> {
	Arc::clone(
		state
			.backfill
			.get_or_insert_with(|| WriteScope::new(ctx.clone(), TableExistence::DefinedOnDemand)),
	)
}

/// Run the order of view table `table` for `action` over the row under `key`,
/// whose image before the write is `initial` and after it `current`: a write of
/// its own over a write state stacked on the statement's scope, in the view
/// table's view-maintenance context, with the row's table the one view in
/// flight beneath it ([`view_row_exec`]). The fill runs a row it creates for a
/// created row, and one that replaced a row it wrote under the same key for an
/// updated row from that row. The order is the one of the table's catalog
/// version, taken the first time the statement's fill or retirement of the
/// view's rows meets the table ([`super::fault::view_rows`]) and held for the
/// rest of it.
async fn maintain(
	state: &mut State,
	ctx: &ExecutionContext,
	table: &TableName,
	key: RecordIdKey,
	action: Action,
	initial: Option<&Record>,
	current: Option<&Record>,
) -> FlowResult<()> {
	let rows = match &state.backfill_order {
		Some((held, rows)) if held == table => Arc::clone(rows),
		_ => {
			let rows = super::fault::view_rows(table, ctx).await?;
			state.backfill_order = Some((table.clone(), Arc::clone(&rows)));
			rows
		}
	};
	let scope = scope(state, ctx);
	let id = RecordId {
		table: table.clone(),
		key,
	};
	let write_ctx = scope.write_ctx_for(&id.table, WritePurpose::ViewMaintenance).await?;
	let exec = view_row_exec(write_ctx.exec().clone(), 1)?;
	let row = WriteRow::for_view_row(id, initial, current, false);
	let pipeline = rows
		.pipelines
		.get(action_pipeline(action))
		.ok_or_else(|| internal("a view table's order the program does not hold"))?;
	let mut nested = State::new(&rows, Vec::new(), exec.function_registry())?;
	nested.frames = rows.permissions.iter().map(|spec| spec.context(&exec)).collect();
	nested.permissions_bypassed = state.permissions_bypassed;
	nested.rowless = true;
	nested.write =
		Some(Box::new(WriteState::view_row(scope, row, write_ctx, Arc::from([table.clone()]))));
	match Box::pin(run_value_row(&rows, pipeline, Value::None, &mut nested, &exec)).await {
		Ok(_) => Ok(()),
		Err(ControlFlow::Err(error)) => Err(ControlFlow::Err(error)),
		// Nothing a schema statement runs on its own behalf is inside a loop
		// or a function body, so a `BREAK`, `CONTINUE` or `RETURN` reaching
		// here escaped the construct that gives it meaning.
		Err(_) => Err(ControlFlow::Err(anyhow!(crate::exec::Error::InvalidControlFlow))),
	}
}
